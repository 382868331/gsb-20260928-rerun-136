package offlinequeue

import (
	"errors"
	"sort"
	"sync"
)

type RemoteService interface {
	Submit(op Operation) error
	Cancel(opID string) error
}

type Runtime struct {
	mu       sync.Mutex
	clientID string
	store    *FileStore
	state    *State
	remote   RemoteService
}

func NewRuntime(clientID string, store *FileStore, remote RemoteService) (*Runtime, error) {
	state, err := store.Load(clientID)
	if err != nil {
		return nil, err
	}
	return &Runtime{clientID: clientID, store: store, state: state, remote: remote}, nil
}

func (r *Runtime) Add(id, kind, payload, dependsOn string) (*Operation, bool, error) {
	r.mu.Lock()
	if err := r.validateDependency(id, dependsOn); err != nil {
		r.mu.Unlock()
		return nil, false, err
	}
	if existing := r.state.Operations[id]; existing != nil {
		copied := *existing
		r.mu.Unlock()
		return &copied, false, nil
	}
	op := &Operation{ID: id, Kind: kind, Payload: payload, DependsOn: dependsOn, Status: StatusPending}
	r.state.Operations[id] = op
	r.state.Order = append(r.state.Order, id)
	err := r.store.Save(r.state)
	copied := *op
	r.mu.Unlock()
	return &copied, true, err
}

func (r *Runtime) validateDependency(id, dependsOn string) error {
	if dependsOn == "" {
		return nil
	}
	if dependsOn == id {
		return ErrDependencyCycle
	}
	if _, ok := r.state.Operations[dependsOn]; !ok {
		return ErrDependencyMissing
	}
	seen := map[string]bool{id: true}
	current := dependsOn
	for current != "" {
		if seen[current] {
			return ErrDependencyCycle
		}
		seen[current] = true
		dep := r.state.Operations[current]
		if dep == nil {
			return ErrDependencyMissing
		}
		current = dep.DependsOn
	}
	return nil
}

func (r *Runtime) CancelUnsent(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	op := r.state.Operations[id]
	if op == nil {
		return ErrNotFound
	}
	if op.Status != StatusPending {
		return ErrNotCancelable
	}
	op.Status = StatusCanceled
	return r.store.Save(r.state)
}

func (r *Runtime) RequestSubmittedCancel(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	op := r.state.Operations[id]
	if op == nil {
		return ErrNotFound
	}
	if op.Status != StatusSending && op.Status != StatusCancelPending {
		if op.Status == StatusAcknowledged || op.Status == StatusCancelDenied {
			return ErrAlreadyCommitted
		}
		return ErrNotCancelable
	}
	if op.Status != StatusCancelPending {
		op.Status = StatusCancelPending
		if err := r.store.Save(r.state); err != nil {
			return err
		}
	}
	err := r.remote.Cancel(id)
	if err != nil {
		if errors.Is(err, ErrAlreadyCommitted) {
			op.Status = StatusCancelDenied
			_ = r.store.Save(r.state)
		}
		return err
	}
	op.Status = StatusCanceled
	return r.store.Save(r.state)
}

func (r *Runtime) Flush() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for {
		next := r.nextSendable()
		if next == nil {
			return nil
		}
		next.Status = StatusSending
		if err := r.store.Save(r.state); err != nil {
			return err
		}
		next.Attempts++
		err := r.remote.Submit(*next)
		if err != nil {
			if errors.Is(err, ErrAcknowledgementLost) {
				next.Status = StatusPending
				if saveErr := r.store.Save(r.state); saveErr != nil {
					return saveErr
				}
				continue
			}
			if errors.Is(err, ErrRejectedByService) {
				next.Status = StatusRejected
				next.RejectReason = err.Error()
				r.blockDependents(next.ID)
				if saveErr := r.store.Save(r.state); saveErr != nil {
					return saveErr
				}
				continue
			}
			if saveErr := r.store.Save(r.state); saveErr != nil {
				return saveErr
			}
			return err
		}
		next.Status = StatusAcknowledged
		if err := r.store.Save(r.state); err != nil {
			return err
		}
	}
}

func (r *Runtime) nextSendable() *Operation {
	ids := append([]string(nil), r.state.Order...)
	sort.Strings(ids)
	for _, id := range ids {
		op := r.state.Operations[id]
		if op.Status != StatusPending && op.Status != StatusSending {
			continue
		}
		if op.DependsOn == "" {
			return op
		}
		dep := r.state.Operations[op.DependsOn]
		switch {
		case dep == nil:
			op.Status = StatusBlocked
			op.RejectReason = "missing dependency"
		case dep.Status == StatusAcknowledged:
			return op
		case dep.Status == StatusRejected, dep.Status == StatusBlocked, dep.Status == StatusCanceled:
			op.Status = StatusBlocked
			op.RejectReason = "dependency " + string(dep.Status)
		default:
		}
	}
	return nil
}

func (r *Runtime) MarkRejected(id, reason string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	op := r.state.Operations[id]
	if op == nil {
		return ErrNotFound
	}
	op.Status = StatusRejected
	op.RejectReason = reason
	r.blockDependents(id)
	return r.store.Save(r.state)
}

func (r *Runtime) blockDependents(rootID string) {
	for {
		changed := false
		for _, op := range r.state.Operations {
			if op.Status == StatusBlocked || op.DependsOn == "" {
				continue
			}
			dep := r.state.Operations[op.DependsOn]
			if dep != nil && (dep.Status == StatusBlocked || dep.Status == StatusRejected || dep.Status == StatusCanceled) {
				op.Status = StatusBlocked
				op.RejectReason = "dependency " + string(dep.Status)
				changed = true
			}
		}
		if !changed {
			return
		}
	}
}

func (r *Runtime) Status(id string) (Operation, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	op := r.state.Operations[id]
	if op == nil {
		return Operation{}, false
	}
	return *op, true
}
