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
	draft := Operation{ID: id, Kind: kind, Payload: payload, DependsOn: dependsOn, OriginID: id}
	if err := r.validateDependency(draft.ID, draft.DependsOn); err != nil {
		r.mu.Unlock()
		return nil, false, err
	}
	if existing := r.state.Operations[id]; existing != nil {
		copied := *existing
		r.mu.Unlock()
		return &copied, false, nil
	}
	op := draft
	op.Status = StatusPending
	r.state.Operations[id] = &op
	r.state.Order = append(r.state.Order, id)
	err := r.store.Save(r.state)
	copied := op
	r.mu.Unlock()
	return &copied, true, err
}

// AddWithPrecondition enqueues an offline edit that assumes the target object
// is still at baseVersion. When another client advances the object first, the
// server reports a stale-precondition conflict instead of silently overwriting.
func (r *Runtime) AddWithPrecondition(id, kind, payload, objectKey string, baseVersion int64, dependsOn string) (*Operation, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	draft := Operation{
		ID:          id,
		Kind:        kind,
		Payload:     payload,
		DependsOn:   dependsOn,
		ObjectKey:   objectKey,
		BaseVersion: baseVersion,
		OriginID:    id,
	}
	if err := r.validateDependency(draft.ID, draft.DependsOn); err != nil {
		return nil, false, err
	}
	if existing := r.state.Operations[id]; existing != nil {
		copied := *existing
		return &copied, false, nil
	}
	op := draft
	op.Status = StatusPending
	r.state.Operations[id] = &op
	r.state.Order = append(r.state.Order, id)
	if err := r.store.Save(r.state); err != nil {
		return nil, false, err
	}
	copied := op
	return &copied, true, nil
}

// Revise keeps the original conflicted/rejected operation untouched (its intent
// and rejection reason stay on record) and submits a brand new operation that
// carries the revised payload and a fresh precondition. The new identity keeps
// lineage back to the original via RevisionOf/OriginID.
func (r *Runtime) Revise(newID, kind, payload, objectKey string, baseVersion int64, originalID string) (*Operation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	original := r.state.Operations[originalID]
	if original == nil {
		return nil, ErrNotFound
	}
	if original.Status != StatusConflict && original.Status != StatusRejected {
		return nil, ErrNotRevisable
	}
	if r.state.Operations[newID] != nil {
		return nil, ErrAlreadyExists
	}
	originID := original.OriginID
	if originID == "" {
		originID = original.ID
	}
	revised := &Operation{
		ID:          newID,
		Kind:        kind,
		Payload:     payload,
		ObjectKey:   objectKey,
		BaseVersion: baseVersion,
		RevisionOf:  originalID,
		OriginID:    originID,
		DependsOn:   original.DependsOn,
		Status:      StatusPending,
	}
	if revised.DependsOn != "" {
		dep := r.state.Operations[revised.DependsOn]
		if dep == nil || dep.Status != StatusAcknowledged {
			revised.DependsOn = ""
		}
	}
	r.state.Operations[newID] = revised
	r.state.Order = append(r.state.Order, newID)
	if err := r.store.Save(r.state); err != nil {
		return nil, err
	}
	copied := *revised
	return &copied, nil
}

// Abandon records that the user gave up on a conflicted/rejected operation.
// Its original payload and rejection reason are preserved on the record.
func (r *Runtime) Abandon(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	op := r.state.Operations[id]
	if op == nil {
		return ErrNotFound
	}
	if op.Status != StatusConflict && op.Status != StatusRejected {
		return ErrNotRevisable
	}
	op.Status = StatusAbandoned
	r.blockDependents(id)
	return r.store.Save(r.state)
}

// RebaseDependent re-points an operation blocked on a failed prerequisite to a
// confirmed revision of that prerequisite. The caller explicitly confirms the
// new prerequisite; dependents never inherit a replacement automatically.
func (r *Runtime) RebaseDependent(id, newDependsOn string, baseVersion int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	op := r.state.Operations[id]
	if op == nil {
		return ErrNotFound
	}
	if op.Status != StatusBlocked {
		return ErrNotBlocked
	}
	if newDependsOn != "" {
		dep := r.state.Operations[newDependsOn]
		if dep == nil || dep.Status != StatusAcknowledged {
			return ErrDependencyMissing
		}
	}
	if err := r.validateDependency(id, newDependsOn); err != nil {
		return err
	}
	op.DependsOn = newDependsOn
	op.BaseVersion = baseVersion
	op.Status = StatusPending
	op.RejectReason = ""
	return r.store.Save(r.state)
}

// Undo turns the reversal of an already-committed operation into its own
// independent business operation. Canceling a committed edit is not allowed;
// an undo is submitted (and idempotently applied) like any other operation.
func (r *Runtime) Undo(newID, kind, targetID string) (*Operation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	target := r.state.Operations[targetID]
	if target == nil {
		return nil, ErrNotFound
	}
	if target.Status != StatusAcknowledged {
		return nil, errors.New("undo target is not committed")
	}
	if target.UndoOf != "" {
		return nil, errors.New("undoing an undo is not supported")
	}
	if r.state.Operations[newID] != nil {
		return nil, ErrAlreadyExists
	}
	undo := &Operation{
		ID:        newID,
		Kind:      kind,
		Payload:   "undo:" + target.ID,
		UndoOf:    target.ID,
		ObjectKey: target.ObjectKey,
		OriginID:  newID,
		Status:    StatusPending,
	}
	r.state.Operations[newID] = undo
	r.state.Order = append(r.state.Order, newID)
	if err := r.store.Save(r.state); err != nil {
		return nil, err
	}
	copied := *undo
	return &copied, nil
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
			if errors.Is(err, ErrStalePrecondition) {
				next.Status = StatusConflict
				next.RejectReason = err.Error()
				r.blockDependents(next.ID)
				if saveErr := r.store.Save(r.state); saveErr != nil {
					return saveErr
				}
				continue
			}
			if errors.Is(err, ErrAlreadyUndone) {
				next.Status = StatusRejected
				next.RejectReason = err.Error()
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
		case dep.Status == StatusRejected, dep.Status == StatusBlocked, dep.Status == StatusCanceled,
			dep.Status == StatusConflict, dep.Status == StatusAbandoned:
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
			if dep != nil && (dep.Status == StatusBlocked || dep.Status == StatusRejected || dep.Status == StatusCanceled ||
				dep.Status == StatusConflict || dep.Status == StatusAbandoned) {
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
