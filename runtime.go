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
	return r.AddOp(Operation{ID: id, Kind: kind, Payload: payload, DependsOn: dependsOn})
}

func (r *Runtime) AddOp(op Operation) (*Operation, bool, error) {
	r.mu.Lock()
	// An archived summary keeps its identity: a new record may not reuse
	// the same ID and silently replace the confirmed summary as a premise.
	if _, archived := r.state.archived(op.ID); archived {
		r.mu.Unlock()
		return nil, false, ErrArchivedIdentity
	}
	missingPremise := ""
	if op.DependsOn != "" {
		if _, ok := r.state.Operations[op.DependsOn]; !ok {
			if _, archived := r.state.archived(op.DependsOn); !archived {
				missingPremise = op.DependsOn
			}
		}
	}
	if missingPremise == "" {
		if err := r.validateDependency(op.ID, op.DependsOn); err != nil {
			r.mu.Unlock()
			return nil, false, err
		}
	} else if op.DependsOn == op.ID {
		r.mu.Unlock()
		return nil, false, ErrDependencyCycle
	}
	if existing := r.state.Operations[op.ID]; existing != nil {
		copied := *existing
		r.mu.Unlock()
		return &copied, false, nil
	}
	op.RejectReason = ""
	op.Attempts = 0
	if missingPremise != "" {
		// Late resubmission whose premise was removed from history
		// (e.g. deleted by an adopted consolidation). Keep the original
		// intent as a blocked, processable gap instead of failing the
		// submission; the user confirms a new premise before it sends.
		op.Status = StatusBlocked
		op.MissingPremise = missingPremise
		op.ConsolidationScope = r.state.consolidationScopeOf(missingPremise)
		op.RejectReason = "missing dependency " + missingPremise
		if op.ConsolidationScope != "" {
			op.RejectReason += " (removed by " + op.ConsolidationScope + ")"
		}
	} else {
		op.Status = StatusPending
	}
	stored := op
	r.state.Operations[op.ID] = &stored
	r.state.Order = append(r.state.Order, op.ID)
	err := r.store.Save(r.state)
	copied := stored
	r.mu.Unlock()
	return &copied, true, err
}

// Revise creates a new operation with a new identity that carries the
// rejected operation's intent. The rejected record is kept untouched.
func (r *Runtime) Revise(rejectedID, newID, newPayload string, newBaseVersion int) (*Operation, error) {
	r.mu.Lock()
	old := r.state.Operations[rejectedID]
	if old == nil {
		r.mu.Unlock()
		return nil, ErrNotFound
	}
	if old.Status != StatusRejected {
		r.mu.Unlock()
		return nil, ErrNotRejected
	}
	if r.state.Operations[newID] != nil {
		r.mu.Unlock()
		return nil, ErrAlreadyExists
	}
	if err := r.validateDependency(newID, old.DependsOn); err != nil {
		r.mu.Unlock()
		return nil, err
	}
	revision := &Operation{
		ID:          newID,
		Kind:        old.Kind,
		Payload:     newPayload,
		ObjectID:    old.ObjectID,
		BaseVersion: newBaseVersion,
		DependsOn:   old.DependsOn,
		Revises:     rejectedID,
		Status:      StatusPending,
	}
	r.state.Operations[newID] = revision
	r.state.Order = append(r.state.Order, newID)
	err := r.store.Save(r.state)
	copied := *revision
	r.mu.Unlock()
	return &copied, err
}

// Abandon marks a rejected operation as canceled, keeping its record.
func (r *Runtime) Abandon(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	op := r.state.Operations[id]
	if op == nil {
		return ErrNotFound
	}
	if op.Status != StatusRejected {
		return ErrNotRejected
	}
	op.Status = StatusCanceled
	return r.store.Save(r.state)
}

// ConfirmSuccessor explicitly re-points a blocked successor at a confirmed
// dependency (typically a revision of its original predecessor). Successors
// never silently follow a revision without this confirmation.
func (r *Runtime) ConfirmSuccessor(successorID, newDependsOn string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	op := r.state.Operations[successorID]
	if op == nil {
		return ErrNotFound
	}
	if op.Status != StatusBlocked {
		return ErrNotBlocked
	}
	if err := r.validateDependency(successorID, newDependsOn); err != nil {
		return err
	}
	op.DependsOn = newDependsOn
	op.Status = StatusPending
	op.RejectReason = ""
	op.MissingPremise = ""
	op.ConsolidationScope = ""
	return r.store.Save(r.state)
}

// UndoCommitted creates a new independent business operation that undoes a
// committed operation. The original operation stays acknowledged.
func (r *Runtime) UndoCommitted(committedID string, undo Operation) (*Operation, error) {
	r.mu.Lock()
	committed := r.state.Operations[committedID]
	if committed == nil {
		r.mu.Unlock()
		return nil, ErrNotFound
	}
	if committed.Status != StatusAcknowledged {
		r.mu.Unlock()
		return nil, ErrNotCommitted
	}
	if r.state.Operations[undo.ID] != nil {
		r.mu.Unlock()
		return nil, ErrAlreadyExists
	}
	if err := r.validateDependency(undo.ID, undo.DependsOn); err != nil {
		r.mu.Unlock()
		return nil, err
	}
	undo.Kind = "undo:" + committed.Kind
	undo.ObjectID = committed.ObjectID
	undo.UndoOf = committedID
	undo.Status = StatusPending
	r.state.Operations[undo.ID] = &undo
	r.state.Order = append(r.state.Order, undo.ID)
	err := r.store.Save(r.state)
	copied := undo
	r.mu.Unlock()
	return &copied, err
}

func (r *Runtime) validateDependency(id, dependsOn string) error {
	if dependsOn == "" {
		return nil
	}
	if dependsOn == id {
		return ErrDependencyCycle
	}
	if _, ok := r.state.Operations[dependsOn]; !ok {
		// Late submissions may reference facts that were already
		// consolidated into the archive; they can still join their
		// original dependency chain.
		if _, archived := r.state.archived(dependsOn); archived {
			return nil
		}
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
			if summary, archived := r.state.archived(op.DependsOn); archived {
				if summary.Status == StatusAcknowledged && !summary.Corrected {
					return op
				}
				op.Status = StatusBlocked
				op.RejectReason = "dependency archived as " + string(summary.Status)
			} else {
				op.Status = StatusBlocked
				op.RejectReason = "missing dependency"
				op.MissingPremise = op.DependsOn
				op.ConsolidationScope = r.state.consolidationScopeOf(op.DependsOn)
			}
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

// Unresolved lists operations that are blocked on a failed or canceled
// dependency and therefore need manual review before they can be
// resubmitted. The result is stable across restarts because it is derived
// from the persisted state. Unrelated operations are never included.
func (r *Runtime) Unresolved() []Operation {
	r.mu.Lock()
	defer r.mu.Unlock()
	var result []Operation
	for _, id := range r.state.Order {
		op := r.state.Operations[id]
		if op.Status == StatusBlocked {
			result = append(result, *op)
		}
	}
	return result
}
