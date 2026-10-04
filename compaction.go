package offlinequeue

import (
	"fmt"
	"sort"
)

// Compaction actions decided by a preview.
const (
	ActionKeep      = "keep"      // confirmed fact still referenced by waiting work
	ActionProtected = "protected" // unknown effect or disputed record, never dropped by age
	ActionSummarize = "summarize" // acknowledged, unreferenced: keep a summary only
	ActionDelete    = "delete"    // canceled, unreferenced: safe to drop
)

// Compaction plan lifecycle.
const (
	PlanPreview   = "preview"
	PlanAdopted   = "adopted"
	PlanWithdrawn = "withdrawn"
)

// ArchivedRecord is the summary kept for a compacted operation. It never
// re-executes anything; it only preserves what the local log already knew.
type ArchivedRecord struct {
	ID       string `json:"id"`
	Kind     string `json:"kind"`
	ObjectID string `json:"object_id,omitempty"`
	Status   Status `json:"status"`
	Revises  string `json:"revises,omitempty"`
	UndoOf   string `json:"undo_of,omitempty"`
	Summary  string `json:"summary"`
	PlanID   string `json:"plan_id"`
}

// CompactionEntry describes the planned fate of one operation together
// with the reason, so the preview can be reviewed before adoption.
type CompactionEntry struct {
	OpID    string `json:"op_id"`
	Action  string `json:"action"`
	Reason  string `json:"reason"`
	Applied bool   `json:"applied,omitempty"`
	Note    string `json:"note,omitempty"`
}

// CompactionPlan is a withdrawable preview until AdoptCompaction applies
// it. Adopted plans keep the scope they were based on for audit.
type CompactionPlan struct {
	ID      string            `json:"id"`
	Status  string            `json:"status"`
	Scope   []string          `json:"scope"`
	Entries []CompactionEntry `json:"entries"`
}

// ChainLink is one hop of a dependency explanation. Source is "live",
// "archived", "gap" (deleted by compaction) or "missing".
type ChainLink struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Source string `json:"source"`
	Note   string `json:"note,omitempty"`
}

// terminalForCompaction reports statuses that never change on their own,
// making them eligible for summarize/delete classification.
func terminalForCompaction(s Status) bool {
	return s == StatusAcknowledged || s == StatusCanceled
}

// neededByWaiting computes the set of operation ids transitively referenced
// by any operation that is still waiting (pending, sending, cancel_pending)
// or blocked. Confirmed facts in this set must be kept in full.
func (r *Runtime) neededByWaiting() map[string][]string {
	needed := map[string][]string{}
	for _, op := range r.state.Operations {
		waiting := op.Status == StatusPending || op.Status == StatusSending ||
			op.Status == StatusCancelPending || op.Status == StatusBlocked
		if !waiting {
			continue
		}
		for cur := op.DependsOn; cur != ""; {
			dep := r.state.Operations[cur]
			needed[cur] = append(needed[cur], op.ID)
			if dep == nil {
				break
			}
			cur = dep.DependsOn
		}
	}
	return needed
}

func (r *Runtime) classifyLocked(op *Operation, needed map[string][]string) CompactionEntry {
	entry := CompactionEntry{OpID: op.ID}
	if waiters, ok := needed[op.ID]; ok {
		entry.Action = ActionKeep
		entry.Reason = fmt.Sprintf("confirmed fact still referenced by waiting task(s) %v", waiters)
		return entry
	}
	switch op.Status {
	case StatusAcknowledged:
		entry.Action = ActionSummarize
		entry.Reason = "acknowledged and unreferenced; summary is sufficient"
	case StatusCanceled:
		entry.Action = ActionDelete
		entry.Reason = "canceled before commit and unreferenced"
	case StatusRejected, StatusCancelDenied:
		entry.Action = ActionProtected
		entry.Reason = "disputed record (" + string(op.Status) + "); not dropped by age"
	default:
		entry.Action = ActionProtected
		entry.Reason = "effect not yet known (" + string(op.Status) + "); not dropped by age"
	}
	return entry
}

// PreviewCompaction builds a withdrawable candidate plan over the current
// history. Nothing is changed by previewing; independent tasks keep running.
func (r *Runtime) PreviewCompaction() (*CompactionPlan, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	needed := r.neededByWaiting()
	scope := append([]string(nil), r.state.Order...)
	sort.Strings(scope)
	plan := &CompactionPlan{Status: PlanPreview, Scope: scope}
	for _, id := range scope {
		op := r.state.Operations[id]
		if op == nil {
			continue
		}
		plan.Entries = append(plan.Entries, r.classifyLocked(op, needed))
	}
	r.state.NextPlan++
	plan.ID = fmt.Sprintf("compact:%d", r.state.NextPlan)
	r.state.Plans[plan.ID] = plan
	if err := r.store.Save(r.state); err != nil {
		return nil, err
	}
	copied := *plan
	return &copied, nil
}

// AdoptCompaction applies a preview: summarize/delete entries are
// re-validated against the live state, so late submissions or corrections
// that arrived during the preview are honored (an entry that no longer
// qualifies is skipped with a note instead of being applied blindly).
// The adopted plan keeps the scope it was based on.
func (r *Runtime) AdoptCompaction(planID string) (*CompactionPlan, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	plan := r.state.Plans[planID]
	if plan == nil {
		return nil, ErrNotFound
	}
	if plan.Status != PlanPreview {
		return nil, ErrPlanNotPreview
	}
	needed := r.neededByWaiting()
	for i := range plan.Entries {
		entry := &plan.Entries[i]
		if entry.Action != ActionSummarize && entry.Action != ActionDelete {
			continue
		}
		op := r.state.Operations[entry.OpID]
		if op == nil {
			entry.Note = "skipped: operation no longer present"
			continue
		}
		if waiters, ok := needed[entry.OpID]; ok {
			entry.Note = fmt.Sprintf("skipped: now referenced by waiting task(s) %v", waiters)
			continue
		}
		if !terminalForCompaction(op.Status) {
			entry.Note = "skipped: status changed to " + string(op.Status) + " during preview"
			continue
		}
		switch entry.Action {
		case ActionSummarize:
			if op.Status != StatusAcknowledged {
				entry.Note = "skipped: status changed to " + string(op.Status) + " during preview"
				continue
			}
			r.state.Archived[op.ID] = &ArchivedRecord{
				ID:       op.ID,
				Kind:     op.Kind,
				ObjectID: op.ObjectID,
				Status:   op.Status,
				Revises:  op.Revises,
				UndoOf:   op.UndoOf,
				Summary: fmt.Sprintf("kind=%s status=%s attempts=%d payload_len=%d",
					op.Kind, op.Status, op.Attempts, len(op.Payload)),
				PlanID: plan.ID,
			}
			r.removeLocked(op.ID)
			entry.Applied = true
		case ActionDelete:
			if op.Status != StatusCanceled {
				entry.Note = "skipped: status changed to " + string(op.Status) + " during preview"
				continue
			}
			r.removeLocked(op.ID)
			r.state.Gaps[op.ID] = plan.ID
			entry.Applied = true
		}
	}
	plan.Status = PlanAdopted
	if err := r.store.Save(r.state); err != nil {
		return nil, err
	}
	copied := *plan
	return &copied, nil
}

// WithdrawCompaction discards a preview that was never adopted. Adopted
// plans are kept for audit and cannot be withdrawn.
func (r *Runtime) WithdrawCompaction(planID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	plan := r.state.Plans[planID]
	if plan == nil {
		return ErrNotFound
	}
	if plan.Status != PlanPreview {
		return ErrPlanNotPreview
	}
	plan.Status = PlanWithdrawn
	return r.store.Save(r.state)
}

// CompactionPlans lists all known plans (preview, adopted, withdrawn) in
// stable id order.
func (r *Runtime) CompactionPlans() []CompactionPlan {
	r.mu.Lock()
	defer r.mu.Unlock()
	ids := make([]string, 0, len(r.state.Plans))
	for id := range r.state.Plans {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	result := make([]CompactionPlan, 0, len(ids))
	for _, id := range ids {
		result = append(result, *r.state.Plans[id])
	}
	return result
}

// ArchivedSummary returns the kept summary for a compacted operation.
func (r *Runtime) ArchivedSummary(id string) (ArchivedRecord, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec := r.state.Archived[id]
	if rec == nil {
		return ArchivedRecord{}, false
	}
	return *rec, true
}

// ExplainChain walks the dependency basis of an operation across live
// records, archived summaries and compaction gaps, so the release basis of
// waiting tasks stays explainable even over long or cross-linked chains.
func (r *Runtime) ExplainChain(id string) []ChainLink {
	r.mu.Lock()
	defer r.mu.Unlock()
	var chain []ChainLink
	seen := map[string]bool{}
	for cur := id; cur != "" && !seen[cur]; {
		seen[cur] = true
		if op := r.state.Operations[cur]; op != nil {
			chain = append(chain, ChainLink{ID: cur, Status: string(op.Status), Source: "live"})
			cur = op.DependsOn
			continue
		}
		if rec := r.state.Archived[cur]; rec != nil {
			chain = append(chain, ChainLink{
				ID: cur, Status: string(rec.Status), Source: "archived",
				Note: "summary kept by " + rec.PlanID,
			})
			break
		}
		if planID, ok := r.state.Gaps[cur]; ok {
			chain = append(chain, ChainLink{
				ID: cur, Status: "deleted", Source: "gap",
				Note: "deleted by " + planID + "; confirm a new basis with ConfirmSuccessor",
			})
			break
		}
		chain = append(chain, ChainLink{ID: cur, Status: "unknown", Source: "missing"})
		break
	}
	return chain
}

func (r *Runtime) removeLocked(id string) {
	delete(r.state.Operations, id)
	for i, oid := range r.state.Order {
		if oid == id {
			r.state.Order = append(r.state.Order[:i], r.state.Order[i+1:]...)
			return
		}
	}
}
