package offlinequeue

import (
	"errors"
	"sort"
	"strings"
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
	if op.OriginClient == "" {
		op.OriginClient = r.clientID
	}
	if err := r.validateDependency(op.ID, op.DependsOn); err != nil {
		r.mu.Unlock()
		return nil, false, err
	}
	if existing := r.state.Operations[op.ID]; existing != nil {
		copied := *existing
		r.mu.Unlock()
		return &copied, false, nil
	}
	op.Status = StatusPending
	op.RejectReason = ""
	op.Attempts = 0
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
	// A qualified prerequisite belongs to another client and may arrive in a
	// later sync; it cannot be validated against the local store.
	if strings.Contains(dependsOn, qualifiedSeparator) {
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
	r.refreshBlocked()
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
		if op.OriginClient != "" && op.OriginClient != r.clientID {
			continue
		}
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

// UnresolvedDependency describes an operation whose prerequisite is not yet
// acknowledged, so the operation cannot be sent. It powers the user-facing
// view of dependencies that still need attention after a restart.
type UnresolvedDependency struct {
	OperationID      string `json:"operation_id"`
	OriginClient     string `json:"origin_client"`
	Status           Status `json:"status"`
	DependsOn        string `json:"depends_on"`
	DependencyOf     string `json:"dependency_origin_client"`
	DependencyStatus Status `json:"dependency_status"`
	Reason           string `json:"reason"`
}

// UnresolvedDependencies returns every operation whose prerequisite is absent
// or not acknowledged. Independent operations are never listed, even while
// other groups are blocked.
func (r *Runtime) UnresolvedDependencies() []UnresolvedDependency {
	r.mu.Lock()
	defer r.mu.Unlock()
	ids := append([]string(nil), r.state.Order...)
	sort.Strings(ids)
	var result []UnresolvedDependency
	for _, id := range ids {
		op := r.state.Operations[id]
		if op == nil || op.DependsOn == "" {
			continue
		}
		if op.Status == StatusAcknowledged || op.Status == StatusCanceled {
			continue
		}
		dep := r.state.Operations[op.DependsOn]
		if dep != nil && dep.Status == StatusAcknowledged {
			continue
		}
		entry := UnresolvedDependency{
			OperationID:  op.ID,
			OriginClient: op.OriginClient,
			Status:       op.Status,
			DependsOn:    op.DependsOn,
			Reason:       op.RejectReason,
		}
		if dep != nil {
			entry.DependencyOf = dep.OriginClient
			entry.DependencyStatus = dep.Status
			if entry.Reason == "" {
				entry.Reason = "dependency " + string(dep.Status)
			}
		} else if entry.Reason == "" {
			entry.Reason = "missing dependency"
		}
		result = append(result, entry)
	}
	return result
}

// PendingReviewItem describes work a user must look at after a prerequisite
// was rejected, revised, or abandoned.
type PendingReviewItem struct {
	ID           string `json:"id"`
	OriginClient string `json:"origin_client"`
	Status       Status `json:"status"`
	DependsOn    string `json:"depends_on"`
	Reason       string `json:"reason"`
}

// PendingReview returns rejected operations (to revise or abandon) followed
// by the successors that are blocked on them and must not be committed on a
// stale premise. Independent operations are excluded.
func (r *Runtime) PendingReview() []PendingReviewItem {
	r.mu.Lock()
	defer r.mu.Unlock()
	ids := append([]string(nil), r.state.Order...)
	sort.Strings(ids)
	var result []PendingReviewItem
	for _, id := range ids {
		op := r.state.Operations[id]
		if op == nil {
			continue
		}
		switch op.Status {
		case StatusRejected, StatusBlocked:
			result = append(result, PendingReviewItem{
				ID:           op.ID,
				OriginClient: op.OriginClient,
				Status:       op.Status,
				DependsOn:    op.DependsOn,
				Reason:       op.RejectReason,
			})
		}
	}
	return result
}

// DependentsOf returns the transitive successors of an operation. After a
// prerequisite is rejected, revised, or abandoned, these are exactly the
// operations that have to be re-reviewed before they can be committed again.
func (r *Runtime) DependentsOf(predecessorID string) []Operation {
	r.mu.Lock()
	defer r.mu.Unlock()
	ids := append([]string(nil), r.state.Order...)
	sort.Strings(ids)
	children := map[string][]string{}
	for _, id := range ids {
		op := r.state.Operations[id]
		if op != nil && op.DependsOn != "" {
			children[op.DependsOn] = append(children[op.DependsOn], id)
		}
	}
	var result []Operation
	seen := map[string]bool{}
	queue := append([]string(nil), children[predecessorID]...)
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if seen[id] {
			continue
		}
		seen[id] = true
		if op := r.state.Operations[id]; op != nil {
			result = append(result, *op)
		}
		queue = append(queue, children[id]...)
	}
	return result
}

const qualifiedSeparator = "/"

func qualifyID(origin, id string) string {
	if origin == "" || strings.Contains(id, qualifiedSeparator) {
		return id
	}
	return origin + qualifiedSeparator + id
}

// Snapshot exports this client's operations for exchange with another client.
// Identities are qualified with the originating client so two clients can
// reuse the same local id without colliding; provenance is preserved.
func (r *Runtime) Snapshot() []Operation {
	r.mu.Lock()
	defer r.mu.Unlock()
	ids := append([]string(nil), r.state.Order...)
	sort.Strings(ids)
	result := make([]Operation, 0, len(ids))
	for _, id := range ids {
		op := r.state.Operations[id]
		if op == nil {
			continue
		}
		copied := *op
		origin := op.OriginClient
		if origin == "" {
			origin = r.clientID
		}
		copied.OriginClient = origin
		copied.ID = qualifyID(origin, op.ID)
		if op.DependsOn != "" {
			copied.DependsOn = qualifyID(origin, op.DependsOn)
		}
		if op.Revises != "" {
			copied.Revises = qualifyID(origin, op.Revises)
		}
		if op.UndoOf != "" {
			copied.UndoOf = qualifyID(origin, op.UndoOf)
		}
		result = append(result, copied)
	}
	return result
}

// statusRank orders how converged knowledge progresses. Terminal outcomes
// outrank in-flight states, which outrank pending work.
func statusRank(s Status) int {
	switch s {
	case StatusPending:
		return 1
	case StatusSending, StatusCancelPending:
		return 2
	case StatusBlocked:
		return 3
	case StatusRejected:
		return 4
	case StatusAcknowledged:
		return 5
	case StatusCanceled:
		return 6
	case StatusCancelDenied:
		return 7
	default:
		return 0
	}
}

// Sync merges another client's operation group into the saved state. Merging
// is order-independent: syncing A then B and B then A converges to the same
// state, and exchanging snapshots repeatedly is idempotent. Foreign
// operations keep their identity and origin and are never submitted by this
// client; this client only submits its own operations.
func (r *Runtime) Sync(incoming []Operation) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	changed := false
	for _, in := range incoming {
		op := in
		if op.OriginClient == "" {
			return ErrDependencyMissing
		}
		op.ID = qualifyID(op.OriginClient, in.ID)
		if op.DependsOn != "" {
			op.DependsOn = qualifyID(op.OriginClient, in.DependsOn)
		}
		if op.Revises != "" {
			op.Revises = qualifyID(op.OriginClient, in.Revises)
		}
		if op.UndoOf != "" {
			op.UndoOf = qualifyID(op.OriginClient, in.UndoOf)
		}
		existing := r.state.Operations[op.ID]
		if existing == nil {
			stored := op
			r.state.Operations[op.ID] = &stored
			r.state.Order = append(r.state.Order, op.ID)
			changed = true
			continue
		}
		if statusRank(op.Status) > statusRank(existing.Status) {
			existing.Status = op.Status
			if op.RejectReason != "" {
				existing.RejectReason = op.RejectReason
			}
			changed = true
		}
		if op.Attempts > existing.Attempts {
			existing.Attempts = op.Attempts
		}
		if existing.OriginClient == "" {
			existing.OriginClient = op.OriginClient
		}
		if op.Kind != "" {
			existing.Kind = op.Kind
		}
		if op.Payload != "" {
			existing.Payload = op.Payload
		}
		if op.ObjectID != "" {
			existing.ObjectID = op.ObjectID
		}
		if op.Revises != "" {
			existing.Revises = op.Revises
		}
		if op.UndoOf != "" {
			existing.UndoOf = op.UndoOf
		}
	}
	if changed {
		r.refreshBlocked()
		return r.store.Save(r.state)
	}
	return nil
}

// refreshBlocked recomputes blocked successors based on current dependency
// statuses, including dependencies learned through a later sync. A blocked
// successor whose prerequisite is now acknowledged becomes sendable again
// only when it belongs to this client; foreign operations stay recorded.
func (r *Runtime) refreshBlocked() {
	for {
		changed := false
		for _, op := range r.state.Operations {
			if op.DependsOn == "" {
				continue
			}
			dep := r.state.Operations[op.DependsOn]
			switch {
			case dep == nil:
				if op.Status != StatusBlocked {
					op.Status = StatusBlocked
					op.RejectReason = "missing dependency"
					changed = true
				}
			case dep.Status == StatusAcknowledged:
				if op.Status == StatusBlocked {
					// Explicit re-confirmation is still required for rejected
					// premises; missing-dependency entries created by partial
					// sync can resume automatically once knowledge arrives.
					if op.RejectReason == "missing dependency" {
						op.Status = StatusPending
						op.RejectReason = ""
						changed = true
					}
				}
			case dep.Status == StatusRejected, dep.Status == StatusBlocked, dep.Status == StatusCanceled:
				if op.Status != StatusBlocked && op.Status != StatusAcknowledged && op.Status != StatusCanceled {
					op.Status = StatusBlocked
					op.RejectReason = "dependency " + string(dep.Status)
					changed = true
				}
			}
		}
		if !changed {
			return
		}
	}
}
