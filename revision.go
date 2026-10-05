package offlinequeue

import (
	"errors"
	"fmt"
	"sort"
)

// DependencyConflict explains why one requested premise reassignment was
// rejected during pre-commit analysis. Chain is the minimal conflict
// chain: for a cycle it lists the operations forming the loop, ending
// where it started (e.g. [x y z x]); for a contradictory premise it is
// the operation and the conflicting premise.
type DependencyConflict struct {
	OpID   string   `json:"op_id"`
	Chain  []string `json:"chain"`
	Reason string   `json:"reason"`
}

// DependencyRevisionPlan is the pre-commit analysis of a batch of premise
// reassignments for blocked operations. Nothing is applied at plan time;
// the caller explicitly adopts the valid subset (or part of it) with
// AdoptDependencyRevision. Unadopted operations stay blocked.
type DependencyRevisionPlan struct {
	ID          string               `json:"id"`
	Assignments map[string]string    `json:"assignments"`
	Valid       []string             `json:"valid"`
	Conflicts   []DependencyConflict `json:"conflicts"`
	Adopted     []string             `json:"adopted,omitempty"`
}

// PlanDependencyRevision analyses a batch of premise reassignments for
// blocked operations without changing any record. Every assignment is
// checked against the full transitive dependency graph that includes all
// other assignments in the batch, so cycles spanning multiple records and
// contradictory premises are caught before anything is committed, and the
// minimal conflict chain is reported per rejected assignment.
func (r *Runtime) PlanDependencyRevision(assignments map[string]string) (*DependencyRevisionPlan, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(assignments) == 0 {
		return nil, errors.New("no assignments given")
	}
	plan := &DependencyRevisionPlan{Assignments: map[string]string{}}
	// Tentative dependency view: current graph overlaid with the batch.
	tentative := map[string]string{}
	for id, op := range r.state.Operations {
		tentative[id] = op.DependsOn
	}
	ids := make([]string, 0, len(assignments))
	for id := range assignments {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	conflicted := map[string]DependencyConflict{}
	for _, id := range ids {
		premise := assignments[id]
		plan.Assignments[id] = premise
		op := r.state.Operations[id]
		switch {
		case op == nil:
			conflicted[id] = DependencyConflict{OpID: id, Chain: []string{id}, Reason: "operation not found"}
			continue
		case op.Status != StatusBlocked:
			conflicted[id] = DependencyConflict{OpID: id, Chain: []string{id}, Reason: "operation is not blocked (" + string(op.Status) + ")"}
			continue
		case premise == "":
			conflicted[id] = DependencyConflict{OpID: id, Chain: []string{id}, Reason: "premise is empty"}
			continue
		}
		if _, live := r.state.Operations[premise]; !live {
			summary, archived := r.state.archived(premise)
			if !archived {
				conflicted[id] = DependencyConflict{OpID: id, Chain: []string{id, premise}, Reason: "premise is missing from history"}
				continue
			}
			if summary.Status != StatusAcknowledged || summary.Corrected {
				conflicted[id] = DependencyConflict{OpID: id, Chain: []string{id, premise}, Reason: "contradictory premise: archived as " + string(summary.Status)}
				continue
			}
		} else {
			st := r.state.Operations[premise].Status
			if st == StatusRejected || st == StatusCanceled || st == StatusCancelDenied {
				conflicted[id] = DependencyConflict{OpID: id, Chain: []string{id, premise}, Reason: "contradictory premise: premise is " + string(st)}
				continue
			}
		}
		tentative[id] = premise
	}
	// Cycle detection over the tentative graph. Each node has a single
	// premise, so walking from an operation deterministically reaches the
	// unique cycle it would close; that walk is the minimal conflict chain.
	for _, id := range ids {
		if _, bad := conflicted[id]; bad {
			continue
		}
		path := []string{id}
		seen := map[string]int{id: 0}
		current := tentative[id]
		for current != "" {
			if at, ok := seen[current]; ok {
				chain := append(append([]string(nil), path[at:]...), current)
				conflicted[id] = DependencyConflict{OpID: id, Chain: chain, Reason: "dependency cycle"}
				break
			}
			seen[current] = len(path)
			path = append(path, current)
			next, known := tentative[current]
			if !known {
				// Premise resolved by an archived summary: terminal, no cycle.
				break
			}
			current = next
		}
	}
	for _, id := range ids {
		if _, bad := conflicted[id]; !bad {
			plan.Valid = append(plan.Valid, id)
		}
	}
	for _, id := range ids {
		if c, bad := conflicted[id]; bad {
			plan.Conflicts = append(plan.Conflicts, c)
		}
	}
	r.state.RevisionSeq++
	plan.ID = fmt.Sprintf("revision-%d", r.state.RevisionSeq)
	r.state.Revisions = append(r.state.Revisions, plan)
	if err := r.store.Save(r.state); err != nil {
		return nil, err
	}
	copied := *plan
	return &copied, nil
}

// AdoptDependencyRevision applies a chosen subset of a plan's valid
// assignments. Operations not adopted keep their blocked status and
// original premise. Adopting a reassignment never re-executes anything:
// only the premise pointer and blocked status change.
func (r *Runtime) AdoptDependencyRevision(planID string, adopt []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var plan *DependencyRevisionPlan
	for _, p := range r.state.Revisions {
		if p.ID == planID {
			plan = p
			break
		}
	}
	if plan == nil {
		return ErrRevisionNotFound
	}
	valid := map[string]bool{}
	for _, id := range plan.Valid {
		valid[id] = true
	}
	targets := adopt
	if targets == nil {
		targets = plan.Valid
	}
	for _, id := range targets {
		if !valid[id] {
			return ErrRevisionConflict
		}
	}
	for _, id := range targets {
		op := r.state.Operations[id]
		if op == nil || op.Status != StatusBlocked {
			continue
		}
		op.DependsOn = plan.Assignments[id]
		op.Status = StatusPending
		op.RejectReason = ""
		op.MissingPremise = ""
		op.ConsolidationScope = ""
		plan.Adopted = append(plan.Adopted, id)
	}
	sort.Strings(plan.Adopted)
	return r.store.Save(r.state)
}

// DependencyRevisions returns all revision plans, including adopted and
// still pending ones, so past premise decisions stay inspectable.
func (r *Runtime) DependencyRevisions() []DependencyRevisionPlan {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]DependencyRevisionPlan, len(r.state.Revisions))
	for i, p := range r.state.Revisions {
		result[i] = *p
	}
	return result
}

// FlushReport summarizes one fair flush round.
type FlushReport struct {
	Sent     []string `json:"sent"`
	Deferred []string `json:"deferred"`
}

// EffectVerifier is an optional RemoteService capability used to confirm
// whether an in-flight operation actually took effect remotely before
// deciding to resubmit it. Rebinding a premise is never a redo license:
// operations with unknown effects are verified first.
type EffectVerifier interface {
	Verify(opID string) (bool, error)
}

// FlushFair executes ready operations with at most maxSlots submission
// attempts per round. A branch whose premise stays unknown (transient
// failure) is deferred instead of occupying every slot, so independent
// branches keep progressing. Deferred branches can be retried by calling
// FlushFair again. Operations with confirmed effects are never
// resubmitted; in-flight ones are verified before any resubmission.
func (r *Runtime) FlushFair(maxSlots int) (*FlushReport, error) {
	if maxSlots < 1 {
		maxSlots = 1
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	report := &FlushReport{}
	deferred := map[string]bool{}
	verifier, _ := r.remote.(EffectVerifier)
	for {
		ready := r.sendableOps(deferred)
		if len(ready) == 0 {
			sort.Strings(report.Deferred)
			return report, nil
		}
		more := false
		if len(ready) > maxSlots {
			// More ready operations than slots: keep rotating so a
			// deferred branch cannot starve the untried ones.
			more = true
			ready = ready[:maxSlots]
		}
		progressed := false
		for _, op := range ready {
			sent, err := r.submitFair(op, verifier)
			if err != nil {
				if errors.Is(err, errDeferBranch) {
					deferred[op.ID] = true
					continue
				}
				sort.Strings(report.Deferred)
				return report, err
			}
			progressed = true
			if sent {
				report.Sent = append(report.Sent, op.ID)
			}
		}
		if !progressed && !more {
			for id := range deferred {
				report.Deferred = append(report.Deferred, id)
			}
			sort.Strings(report.Deferred)
			return report, nil
		}
	}
}

var errDeferBranch = errors.New("branch deferred for fairness")

// submitFair submits one operation. It reports whether the operation was
// (re)sent in this call. In-flight operations are verified first so an
// already applied effect is never executed twice.
func (r *Runtime) submitFair(op *Operation, verifier EffectVerifier) (bool, error) {
	if op.Status == StatusSending && verifier != nil {
		applied, err := verifier.Verify(op.ID)
		if err != nil {
			return false, errDeferBranch
		}
		if applied {
			op.Status = StatusAcknowledged
			return false, r.store.Save(r.state)
		}
	}
	op.Status = StatusSending
	if err := r.store.Save(r.state); err != nil {
		return false, err
	}
	op.Attempts++
	err := r.remote.Submit(*op)
	switch {
	case err == nil:
		op.Status = StatusAcknowledged
		return true, r.store.Save(r.state)
	case errors.Is(err, ErrAcknowledgementLost), errors.Is(err, ErrRemoteUnavailable):
		op.Status = StatusPending
		if saveErr := r.store.Save(r.state); saveErr != nil {
			return false, saveErr
		}
		return false, errDeferBranch
	case errors.Is(err, ErrRejectedByService):
		op.Status = StatusRejected
		op.RejectReason = err.Error()
		r.blockDependents(op.ID)
		return true, r.store.Save(r.state)
	default:
		if saveErr := r.store.Save(r.state); saveErr != nil {
			return false, saveErr
		}
		return false, err
	}
}

// sendableOps lists operations ready to send, skipping deferred branches:
// an operation is excluded when it or any transitive premise is deferred.
// Missing or failed premises are still converted into accurate blocked
// states as a side effect, matching Flush.
func (r *Runtime) sendableOps(skip map[string]bool) []*Operation {
	ids := append([]string(nil), r.state.Order...)
	sort.Strings(ids)
	var ready []*Operation
	for _, id := range ids {
		op := r.state.Operations[id]
		if op.Status != StatusPending && op.Status != StatusSending {
			continue
		}
		if op.DependsOn == "" {
			if !skip[id] {
				ready = append(ready, op)
			}
			continue
		}
		// Walk the premise chain: any deferred ancestor stalls this branch.
		deferredBranch := skip[id]
		seen := map[string]bool{id: true}
		current := op.DependsOn
		chainReady := false
		for !deferredBranch {
			if skip[current] {
				deferredBranch = true
				break
			}
			dep := r.state.Operations[current]
			if dep == nil {
				if summary, archived := r.state.archived(current); archived {
					if summary.Status == StatusAcknowledged && !summary.Corrected {
						chainReady = true
					} else {
						op.Status = StatusBlocked
						op.RejectReason = "dependency archived as " + string(summary.Status)
					}
				} else {
					op.Status = StatusBlocked
					op.RejectReason = "missing dependency"
					op.MissingPremise = current
					op.ConsolidationScope = r.state.consolidationScopeOf(current)
				}
				break
			}
			switch dep.Status {
			case StatusAcknowledged:
				chainReady = true
			case StatusRejected, StatusBlocked, StatusCanceled:
				op.Status = StatusBlocked
				op.RejectReason = "dependency " + string(dep.Status)
			default:
				if seen[dep.ID] {
					chainReady = false
					break
				}
				seen[dep.ID] = true
				if dep.DependsOn == "" {
					break
				}
				current = dep.DependsOn
				continue
			}
			break
		}
		if chainReady && !deferredBranch {
			ready = append(ready, op)
		}
	}
	return ready
}
