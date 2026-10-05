package offlinequeue

import (
	"errors"
	"fmt"
	"sort"
)

var (
	ErrRebindNotAdoptable = errors.New("rebind entry is not adoptable")
	ErrRebindEntryMissing = errors.New("rebind plan has no entry for the operation")
	ErrRebindEntryAdopted = errors.New("rebind entry already adopted")
)

// RebindEntry is the analysis result for one requested premise revision.
// Only entries marked Adoptable can be adopted; conflicting entries carry
// the minimal conflict chain (e.g. the cycle a -> b -> c -> a) so the
// operator can see exactly which premises contradict each other.
type RebindEntry struct {
	OpID         string   `json:"op_id"`
	NewDependsOn string   `json:"new_depends_on"`
	Adoptable    bool     `json:"adoptable"`
	Adopted      bool     `json:"adopted"`
	Reason       string   `json:"reason,omitempty"`
	Conflict     []string `json:"conflict,omitempty"`
}

// RebindPlan is a previewable batch of premise revisions for blocked
// operations. The plan is persisted so the basis of later adoptions stays
// inspectable after a restart.
type RebindPlan struct {
	ID      string        `json:"id"`
	Entries []RebindEntry `json:"entries"`
}

// PreviewRebind analyzes a batch of premise revisions for blocked
// operations against the full transitive dependency graph, without
// changing any record. All assignments are evaluated together, so cycles
// and contradictory premises that only appear across several operations
// (e.g. a->b, b->c, c->a) are detected, and each conflicting entry
// reports the minimal conflict chain. Valid entries can later be adopted
// individually with AdoptRebind; everything not adopted stays blocked.
func (r *Runtime) PreviewRebind(assignments map[string]string) (*RebindPlan, error) {
	r.mu.Lock()
	plan := &RebindPlan{Entries: r.analyzeRebind(assignments)}
	r.state.RebindSeq++
	plan.ID = fmt.Sprintf("rebind-%d", r.state.RebindSeq)
	r.state.RebindPlans = append(r.state.RebindPlans, plan)
	if err := r.store.Save(r.state); err != nil {
		r.mu.Unlock()
		return nil, err
	}
	copied := *plan
	r.mu.Unlock()
	return &copied, nil
}

// analyzeRebind evaluates every requested assignment against the
// hypothetical graph in which all assignments are applied at once.
func (r *Runtime) analyzeRebind(assignments map[string]string) []RebindEntry {
	ids := make([]string, 0, len(assignments))
	for id := range assignments {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	entries := make([]RebindEntry, 0, len(ids))
	for _, id := range ids {
		entries = append(entries, r.analyzeRebindEntry(id, assignments[id], assignments))
	}
	return entries
}

func (r *Runtime) analyzeRebindEntry(opID, newDependsOn string, assignments map[string]string) RebindEntry {
	entry := RebindEntry{OpID: opID, NewDependsOn: newDependsOn}
	op := r.state.Operations[opID]
	if op == nil {
		entry.Reason = "operation not found"
		return entry
	}
	if op.Status != StatusBlocked {
		entry.Reason = "operation is not blocked (" + string(op.Status) + ")"
		return entry
	}
	if reason, ok := r.premiseUsable(newDependsOn, assignments); !ok {
		entry.Reason = reason
		return entry
	}
	if cycle := r.rebindCycle(opID, assignments); cycle != nil {
		entry.Reason = "dependency cycle"
		entry.Conflict = cycle
		return entry
	}
	entry.Adoptable = true
	return entry
}

// premiseUsable checks whether a proposed premise can support a blocked
// operation. A premise that is itself being revised inside the same batch
// is judged by its own entry, so chained revisions within one plan stay
// adoptable; cycle detection still catches contradictions between them.
func (r *Runtime) premiseUsable(premise string, assignments map[string]string) (string, bool) {
	if premise == "" {
		return "", true
	}
	if dep := r.state.Operations[premise]; dep != nil {
		switch dep.Status {
		case StatusRejected, StatusCanceled, StatusCancelDenied:
			if _, revised := assignments[premise]; !revised {
				return "premise " + premise + " is " + string(dep.Status), false
			}
		case StatusBlocked:
			if _, revised := assignments[premise]; !revised {
				return "premise " + premise + " is blocked and not revised in this batch", false
			}
		}
		return "", true
	}
	if summary, archived := r.state.archived(premise); archived {
		if summary.Status == StatusAcknowledged && !summary.Corrected {
			return "", true
		}
		return "archived premise " + premise + " is " + string(summary.Status), false
	}
	return "premise " + premise + " is unknown", false
}

// rebindCycle walks the effective dependency chain (current dependencies
// overridden by the batch assignments) starting at opID and returns the
// minimal cycle it runs into, or nil when the chain is acyclic.
func (r *Runtime) rebindCycle(opID string, assignments map[string]string) []string {
	dep := func(id string) string {
		if next, ok := assignments[id]; ok {
			return next
		}
		if op := r.state.Operations[id]; op != nil {
			return op.DependsOn
		}
		return ""
	}
	path := []string{}
	index := map[string]int{}
	current := opID
	for current != "" {
		if i, seen := index[current]; seen {
			cycle := append([]string(nil), path[i:]...)
			return append(cycle, current)
		}
		index[current] = len(path)
		path = append(path, current)
		current = dep(current)
	}
	return nil
}

// AdoptRebind applies the listed entries of a previewed plan. Each entry
// is re-validated against the live graph before it is applied, so
// dependencies added after the preview cannot smuggle in a cycle.
// Entries that are not adopted leave their operations blocked.
func (r *Runtime) AdoptRebind(planID string, opIDs ...string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var plan *RebindPlan
	for _, p := range r.state.RebindPlans {
		if p.ID == planID {
			plan = p
			break
		}
	}
	if plan == nil {
		return ErrPlanNotFound
	}
	for _, opID := range opIDs {
		var entry *RebindEntry
		for i := range plan.Entries {
			if plan.Entries[i].OpID == opID {
				entry = &plan.Entries[i]
				break
			}
		}
		if entry == nil {
			return ErrRebindEntryMissing
		}
		if entry.Adopted {
			return ErrRebindEntryAdopted
		}
		if !entry.Adoptable {
			return ErrRebindNotAdoptable
		}
		op := r.state.Operations[opID]
		if op == nil {
			return ErrNotFound
		}
		if op.Status != StatusBlocked {
			return ErrNotBlocked
		}
		// Re-check against the live graph: the single assignment applied
		// on top of the current state must not close a cycle, and the
		// premise must still resolve to something known.
		single := map[string]string{opID: entry.NewDependsOn}
		if cycle := r.rebindCycle(opID, single); cycle != nil {
			return ErrDependencyCycle
		}
		if entry.NewDependsOn != "" {
			if _, ok := r.state.Operations[entry.NewDependsOn]; !ok {
				if _, archived := r.state.archived(entry.NewDependsOn); !archived {
					return ErrDependencyMissing
				}
			}
		}
		op.DependsOn = entry.NewDependsOn
		op.Status = StatusPending
		op.RejectReason = ""
		op.MissingPremise = ""
		op.ConsolidationScope = ""
		entry.Adopted = true
	}
	return r.store.Save(r.state)
}

// RebindPlans returns all previewed rebind plans so past premise
// revisions stay explainable.
func (r *Runtime) RebindPlans() []RebindPlan {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]RebindPlan, len(r.state.RebindPlans))
	for i, p := range r.state.RebindPlans {
		result[i] = *p
	}
	return result
}
