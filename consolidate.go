package offlinequeue

import (
	"errors"
	"fmt"
	"sort"
)

// ConsolidationClass describes how a history record is treated by a
// consolidation candidate.
type ConsolidationClass string

const (
	// ClassKeepReferenced is a confirmed fact still referenced by waiting
	// tasks; the full record must be kept.
	ClassKeepReferenced ConsolidationClass = "keep_referenced"
	// ClassSummaryOnly is a finished record that only needs a summary kept
	// so dependency chains and provenance stay explainable.
	ClassSummaryOnly ConsolidationClass = "summary_only"
	// ClassDeletable is a finished, unreferenced, undisputed record that
	// can be removed entirely.
	ClassDeletable ConsolidationClass = "deletable"
	// ClassProtected covers records with unknown effects (in flight or
	// cancel pending) or still disputed (rejected, blocked, cancel
	// denied). They are never removed based on age alone.
	ClassProtected ConsolidationClass = "protected"
)

var (
	ErrPlanNotFound  = errors.New("consolidation plan not found")
	ErrPlanAdopted   = errors.New("consolidation plan already adopted")
	ErrPlanWithdrawn = errors.New("consolidation plan already withdrawn")
	ErrNotArchived   = errors.New("operation is not in the archive")
)

// ConsolidationEntry explains the classification of one record.
type ConsolidationEntry struct {
	ID           string             `json:"id"`
	Class        ConsolidationClass `json:"class"`
	Reason       string             `json:"reason"`
	ReferencedBy []string           `json:"referenced_by,omitempty"`
	// Outcome records what actually happened at adoption. Records that
	// gained dependencies, became disputed, or entered an unknown-effect
	// state between preview and adoption are kept instead of cleaned.
	Outcome       string `json:"outcome,omitempty"`
	OutcomeReason string `json:"outcome_reason,omitempty"`
}

const (
	outcomeDeleted  = "deleted"
	outcomeArchived = "archived"
	outcomeKept     = "kept"
)

// ConsolidationPlan is a previewable history-consolidation candidate.
// It can be withdrawn any time before adoption; once adopted the plan
// (including the scope it was based on) is kept for audit.
type ConsolidationPlan struct {
	ID        string               `json:"id"`
	Scope     []string             `json:"scope"`
	Entries   []ConsolidationEntry `json:"entries"`
	Adopted   bool                 `json:"adopted"`
	Withdrawn bool                 `json:"withdrawn"`
}

// ArchivedSummary is the retained summary of a consolidated record.
type ArchivedSummary struct {
	ID           string `json:"id"`
	Kind         string `json:"kind"`
	ObjectID     string `json:"object_id,omitempty"`
	DependsOn    string `json:"depends_on,omitempty"`
	Revises      string `json:"revises,omitempty"`
	UndoOf       string `json:"undo_of,omitempty"`
	Status       Status `json:"status"`
	RejectReason string `json:"reject_reason,omitempty"`
	Corrected    bool   `json:"corrected,omitempty"`
	CorrectNote  string `json:"correct_note,omitempty"`
}

func isTerminal(s Status) bool {
	return s == StatusAcknowledged || s == StatusCanceled
}

// cleanupObjection returns a non-empty reason when a record classified
// deletable or summary-only at preview time must be kept at adoption:
// its effect has since become unknown, it became disputed, or a waiting
// task started depending on it between preview and adoption.
func (r *Runtime) cleanupObjection(op *Operation, referencedBy []string) string {
	switch op.Status {
	case StatusSending, StatusCancelPending:
		return "effect now unknown: operation entered flight after preview"
	case StatusPending:
		return "operation still waiting to be sent at adoption"
	case StatusRejected, StatusBlocked, StatusCancelDenied:
		return "record became disputed after preview (" + string(op.Status) + ")"
	}
	for _, from := range referencedBy {
		dep := r.state.Operations[from]
		if dep != nil && !isTerminal(dep.Status) {
			return "new dependency from waiting task " + from + " appeared after preview"
		}
	}
	return ""
}

// referencedBy maps each operation ID to the IDs of operations that point
// at it via DependsOn, Revises, or UndoOf.
func (s *State) referencedBy() map[string][]string {
	refs := map[string][]string{}
	add := func(target, from string) {
		if target != "" {
			refs[target] = append(refs[target], from)
		}
	}
	for _, id := range s.Order {
		op := s.Operations[id]
		if op == nil {
			continue
		}
		add(op.DependsOn, id)
		add(op.Revises, id)
		add(op.UndoOf, id)
	}
	return refs
}

// PreviewConsolidation builds a consolidation candidate over the current
// history without changing any record. Records with unknown effects or
// still disputed are always classified protected, never deletable.
func (r *Runtime) PreviewConsolidation() (*ConsolidationPlan, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	refs := r.state.referencedBy()
	plan := &ConsolidationPlan{}
	for _, id := range r.state.Order {
		op := r.state.Operations[id]
		if op == nil {
			continue
		}
		entry := ConsolidationEntry{ID: id, ReferencedBy: refs[id]}
		switch {
		case op.Status == StatusSending || op.Status == StatusCancelPending:
			entry.Class = ClassProtected
			entry.Reason = "effect unknown: operation is in flight"
		case op.Status == StatusPending:
			entry.Class = ClassProtected
			entry.Reason = "operation is still waiting to be sent"
		case op.Status == StatusRejected || op.Status == StatusBlocked || op.Status == StatusCancelDenied:
			entry.Class = ClassProtected
			entry.Reason = "record is still disputed or awaiting review (" + string(op.Status) + ")"
		case isTerminal(op.Status):
			waiting := false
			for _, from := range refs[id] {
				if dep := r.state.Operations[from]; dep != nil && !isTerminal(dep.Status) {
					waiting = true
					break
				}
			}
			switch {
			case waiting:
				entry.Class = ClassKeepReferenced
				entry.Reason = "confirmed fact still referenced by waiting tasks"
			case op.Status == StatusAcknowledged:
				entry.Class = ClassSummaryOnly
				entry.Reason = "confirmed fact; summary kept for chain explainability"
			case len(refs[id]) > 0:
				entry.Class = ClassSummaryOnly
				entry.Reason = "referenced by finished records; summary kept"
			default:
				entry.Class = ClassDeletable
				entry.Reason = "canceled, unreferenced and undisputed"
			}
		}
		plan.Entries = append(plan.Entries, entry)
		plan.Scope = append(plan.Scope, id)
	}
	r.state.PlanSeq++
	plan.ID = fmt.Sprintf("consolidation-%d", r.state.PlanSeq)
	r.state.Plans = append(r.state.Plans, plan)
	if err := r.store.Save(r.state); err != nil {
		return nil, err
	}
	copied := *plan
	return &copied, nil
}

func (r *Runtime) findPlan(planID string) *ConsolidationPlan {
	for _, p := range r.state.Plans {
		if p.ID == planID {
			return p
		}
	}
	return nil
}

// WithdrawConsolidation discards a candidate that has not been adopted.
func (r *Runtime) WithdrawConsolidation(planID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	plan := r.findPlan(planID)
	if plan == nil {
		return ErrPlanNotFound
	}
	if plan.Adopted {
		return ErrPlanAdopted
	}
	if plan.Withdrawn {
		return ErrPlanWithdrawn
	}
	plan.Withdrawn = true
	return r.store.Save(r.state)
}

// AdoptConsolidation applies a candidate: deletable records are removed,
// summary-only records are moved to the archive, and referenced or
// protected records are kept untouched. No remote effect is ever
// re-executed to fill history. The adopted plan and its scope are kept.
func (r *Runtime) AdoptConsolidation(planID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	plan := r.findPlan(planID)
	if plan == nil {
		return ErrPlanNotFound
	}
	if plan.Adopted {
		return ErrPlanAdopted
	}
	if plan.Withdrawn {
		return ErrPlanWithdrawn
	}
	refs := r.state.referencedBy()
	remove := map[string]bool{}
	for i := range plan.Entries {
		entry := &plan.Entries[i]
		op := r.state.Operations[entry.ID]
		if op == nil {
			entry.Outcome = outcomeKept
			entry.OutcomeReason = "record no longer present at adoption"
			continue
		}
		if entry.Class == ClassDeletable || entry.Class == ClassSummaryOnly {
			if reason := r.cleanupObjection(op, refs[entry.ID]); reason != "" {
				entry.Outcome = outcomeKept
				entry.OutcomeReason = reason
				continue
			}
		}
		switch entry.Class {
		case ClassDeletable:
			remove[entry.ID] = true
			entry.Outcome = outcomeDeleted
			entry.OutcomeReason = "removed at adoption"
		case ClassSummaryOnly:
			r.state.Archive = append(r.state.Archive, ArchivedSummary{
				ID:           op.ID,
				Kind:         op.Kind,
				ObjectID:     op.ObjectID,
				DependsOn:    op.DependsOn,
				Revises:      op.Revises,
				UndoOf:       op.UndoOf,
				Status:       op.Status,
				RejectReason: op.RejectReason,
			})
			remove[entry.ID] = true
			entry.Outcome = outcomeArchived
			entry.OutcomeReason = "summary archived at adoption"
		default:
			entry.Outcome = outcomeKept
			entry.OutcomeReason = "protected by preview"
		}
	}
	if len(remove) > 0 {
		order := r.state.Order[:0]
		for _, id := range r.state.Order {
			if remove[id] {
				delete(r.state.Operations, id)
			} else {
				order = append(order, id)
			}
		}
		r.state.Order = order
	}
	plan.Adopted = true
	return r.store.Save(r.state)
}

// ConsolidationPlans returns all candidates, including withdrawn and
// adopted ones, so the basis of past consolidations stays inspectable.
func (r *Runtime) ConsolidationPlans() []ConsolidationPlan {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]ConsolidationPlan, len(r.state.Plans))
	for i, p := range r.state.Plans {
		result[i] = *p
	}
	return result
}

// Archived returns the retained summary of a consolidated record.
func (r *Runtime) Archived(id string) (ArchivedSummary, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	summary, ok := r.state.archived(id)
	if !ok {
		return ArchivedSummary{}, false
	}
	return *summary, true
}

func (s *State) archived(id string) (*ArchivedSummary, bool) {
	for i := range s.Archive {
		if s.Archive[i].ID == id {
			return &s.Archive[i], true
		}
	}
	return nil, false
}

// adoptedRemovalPlan returns the adopted plan that removed id, i.e. a
// deletable record that is neither live nor archived. Withdrawn plans and
// plans that ultimately kept the record never qualify.
func (s *State) adoptedRemovalPlan(id string) *ConsolidationPlan {
	for i := len(s.Plans) - 1; i >= 0; i-- {
		plan := s.Plans[i]
		if !plan.Adopted || plan.Withdrawn {
			continue
		}
		for _, entry := range plan.Entries {
			if entry.ID == id && entry.Class == ClassDeletable &&
				(entry.Outcome == "" || entry.Outcome == outcomeDeleted) {
				return plan
			}
		}
	}
	return nil
}

// adoptedPlanFor finds the adopted plan that classified id with class.
func (s *State) adoptedPlanFor(id string, class ConsolidationClass) *ConsolidationPlan {
	for i := len(s.Plans) - 1; i >= 0; i-- {
		plan := s.Plans[i]
		if !plan.Adopted || plan.Withdrawn {
			continue
		}
		for _, entry := range plan.Entries {
			if entry.ID == id && entry.Class == class {
				return plan
			}
		}
	}
	return nil
}

// CorrectArchived applies a late correction to a previously consolidated
// result. The summary is marked corrected (never re-executed), live
// dependents of the corrected fact are blocked so they become processable
// gaps in Unresolved, and the affected IDs are returned. Already finished
// successors are left alone; their release basis remains the recorded
// dependency chain.
func (r *Runtime) CorrectArchived(id, note string) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	summary, ok := r.state.archived(id)
	if !ok {
		if r.state.Operations[id] != nil {
			return nil, ErrNotArchived
		}
		return nil, ErrNotFound
	}
	summary.Corrected = true
	summary.CorrectNote = note
	var affected []string
	for _, opID := range r.state.Order {
		op := r.state.Operations[opID]
		if op == nil || isTerminal(op.Status) || op.Status == StatusBlocked {
			continue
		}
		if op.DependsOn == id || op.Revises == id || op.UndoOf == id {
			op.Status = StatusBlocked
			op.RejectReason = "dependency " + id + " corrected after consolidation"
			affected = append(affected, opID)
		}
	}
	sort.Strings(affected)
	if err := r.store.Save(r.state); err != nil {
		return nil, err
	}
	return affected, nil
}
