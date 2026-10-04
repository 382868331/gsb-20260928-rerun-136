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
	missingPremise, premisePlan, depErr := r.classifyPremise(op.ID, op.DependsOn)
	if depErr != nil {
		r.mu.Unlock()
		return nil, false, depErr
	}
	if existing := r.state.Operations[op.ID]; existing != nil {
		copied := *existing
		r.mu.Unlock()
		return &copied, false, nil
	}
	op.Status = StatusPending
	op.RejectReason = ""
	op.Attempts = 0
	op.MissingPremise = ""
	op.PremisePlan = ""
	if missingPremise != "" {
		// The premise was already removed (or archived as a non-fact) by a
		// known consolidation. Preserve the late submission's intent as a
		// processable gap instead of rejecting it outright.
		op.Status = StatusBlocked
		op.MissingPremise = missingPremise
		op.PremisePlan = premisePlan
		if summary, ok := r.state.archived(missingPremise); ok {
			op.RejectReason = "premise " + missingPremise + " archived as " + string(summary.Status)
		} else {
			op.RejectReason = "premise " + missingPremise + " removed by consolidation " + premisePlan
		}
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
	op.PremisePlan = ""
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
	_, _, err := r.classifyPremise(id, dependsOn)
	return err
}

// classifyPremise validates a dependency and, when the premise is no
// longer a live confirmed fact, returns the missing premise ID together
// with the consolidation scope (plan ID) responsible for it. The caller
// then keeps the operation as a blocked, explainable gap rather than
// dropping it. An archived acknowledged and uncorrected fact still
// supports late operations and returns no missing premise.
func (r *Runtime) classifyPremise(id, dependsOn string) (string, string, error) {
	if dependsOn == "" {
		return "", "", nil
	}
	if dependsOn == id {
		return "", "", ErrDependencyCycle
	}
	seen := map[string]bool{id: true}
	current := dependsOn
	for current != "" {
		if seen[current] {
			return "", "", ErrDependencyCycle
		}
		seen[current] = true
		if dep := r.state.Operations[current]; dep != nil {
			current = dep.DependsOn
			continue
		}
		if summary, ok := r.state.archived(current); ok {
			if summary.Status == StatusAcknowledged && !summary.Corrected {
				return "", "", nil
			}
			return current, "", nil
		}
		if plan := r.state.adoptedRemovalPlan(current); plan != nil {
			return current, plan.ID, nil
		}
		return "", "", ErrDependencyMissing
	}
	return "", "", nil
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
		if changed := r.resolveWaitingStates(); changed {
			if err := r.store.Save(r.state); err != nil {
				return err
			}
		}
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
				r.markGap(op, op.DependsOn, "premise "+op.DependsOn+
					" archived as "+string(summary.Status)+" by consolidation")
			} else if plan := r.state.adoptedRemovalPlan(op.DependsOn); plan != nil {
				r.markGap(op, op.DependsOn, "premise "+op.DependsOn+
					" removed by consolidation "+plan.ID)
			} else {
				r.markGap(op, op.DependsOn, "missing dependency "+op.DependsOn)
			}
		case dep.Status == StatusAcknowledged:
			return op
		case dep.Status == StatusRejected, dep.Status == StatusBlocked, dep.Status == StatusCanceled:
			if op.MissingPremise == "" {
				op.RejectReason = "dependency " + string(dep.Status)
			}
			op.Status = StatusBlocked
		default:
		}
	}
	return nil
}

// markGap turns an operation into a processable blocked gap, recording
// the missing premise and the consolidation scope that removed it.
func (r *Runtime) markGap(op *Operation, premise, reason string) {
	op.Status = StatusBlocked
	op.RejectReason = reason
	if op.MissingPremise == "" {
		op.MissingPremise = premise
	}
	if op.PremisePlan == "" {
		if plan := r.state.adoptedRemovalPlan(premise); plan != nil {
			op.PremisePlan = plan.ID
		} else if plan := r.state.adoptedPlanFor(premise, ClassSummaryOnly); plan != nil {
			op.PremisePlan = plan.ID
		}
	}
}

// resolveWaitingStates re-evaluates pending operations whose premises are
// live but already failed, so gaps become visible in Unresolved even
// without calling Add against a missing premise. Blocked operations are
// never auto-resolved.
func (r *Runtime) resolveWaitingStates() bool {
	changed := false
	for _, id := range r.state.Order {
		op := r.state.Operations[id]
		if op == nil || op.Status != StatusPending || op.DependsOn == "" {
			continue
		}
		dep := r.state.Operations[op.DependsOn]
		if dep != nil && (dep.Status == StatusRejected || dep.Status == StatusBlocked || dep.Status == StatusCanceled) {
			op.Status = StatusBlocked
			op.RejectReason = "dependency " + string(dep.Status)
			changed = true
		}
	}
	return changed
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
