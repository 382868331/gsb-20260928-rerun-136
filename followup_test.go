package offlinequeue

import (
	"errors"
	"testing"
)

// A late submission that depends on a record deleted by consolidation is
// kept as a processable blocked gap: its intent is preserved, the missing
// premise and responsible consolidation scope are recorded, and it shows
// up in Unresolved. After confirming a new valid premise it can be sent.
func TestLateSubmissionOnDeletedPremiseBecomesProcessableGap(t *testing.T) {
	rt, service, dir := setupHistory(t)
	plan, err := rt.PreviewConsolidation()
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.AdoptConsolidation(plan.ID); err != nil {
		t.Fatal(err)
	}
	if _, ok := rt.Status("h3"); ok {
		t.Fatal("h3 should have been deleted")
	}

	late, created, err := rt.Add("late-del", "demo", "late-intent", "h3")
	if err != nil {
		t.Fatalf("late submission rejected: %v", err)
	}
	if !created || late.Payload != "late-intent" {
		t.Fatalf("late op intent lost: %#v created=%v", late, created)
	}
	if late.Status != StatusBlocked || late.MissingPremise != "h3" || late.PremisePlan != plan.ID {
		t.Fatalf("late op = %#v, want blocked gap naming h3 and %s", late, plan.ID)
	}
	if late.RejectReason == "" {
		t.Fatal("gap must explain the missing premise and scope")
	}

	unresolved := rt.Unresolved()
	found := false
	for _, op := range unresolved {
		if op.ID == "late-del" {
			found = true
		}
	}
	if !found {
		t.Fatalf("late gap missing from Unresolved: %v", unresolved)
	}

	effectsBefore := service.EffectCount()
	if err := rt.Flush(); err != nil {
		t.Fatal(err)
	}
	// The deleted premise must never be re-executed to fill history, but
	// the unrelated waiting task h7 still goes out.
	if got := service.EffectCount(); got != effectsBefore+1 {
		t.Fatalf("effects after flush = %d, want %d (only independent h7)", got, effectsBefore+1)
	}
	if op, _ := rt.Status("late-del"); op.Status != StatusBlocked {
		t.Fatalf("late-del status = %s, want blocked", op.Status)
	}

	// Repeated late submission of the same ID returns the same gap record
	// without executing anything.
	dup, created, err := rt.Add("late-del", "demo", "changed", "h3")
	if err != nil || created {
		t.Fatalf("duplicate late add = (%v, %v), want (false, nil)", created, err)
	}
	if dup.Payload != "late-intent" {
		t.Fatalf("original intent overwritten: %q", dup.Payload)
	}

	// Restart: the gap is recovered with premise and scope intact.
	rt, err = NewRuntime("c", mustStore(t, dir, "c"), service)
	if err != nil {
		t.Fatal(err)
	}
	restartGaps := rt.Unresolved()
	foundRestart := false
	for _, op := range restartGaps {
		if op.ID == "late-del" {
			foundRestart = true
		}
	}
	if !foundRestart {
		t.Fatalf("late gap missing from unresolved after restart = %v", restartGaps)
	}
	if op, _ := rt.Status("late-del"); op.MissingPremise != "h3" || op.PremisePlan != plan.ID {
		t.Fatalf("gap context lost across restart: %#v", op)
	}

	// User confirms a new valid premise, then the late op sends once.
	if _, _, err := rt.Add("new-base", "demo", "new premise", ""); err != nil {
		t.Fatal(err)
	}
	if err := rt.ConfirmSuccessor("late-del", "new-base"); err != nil {
		t.Fatal(err)
	}
	if err := rt.Flush(); err != nil {
		t.Fatal(err)
	}
	if op, _ := rt.Status("late-del"); op.Status != StatusAcknowledged {
		t.Fatalf("late-del = %s, want acknowledged", op.Status)
	}
	if err := rt.ConfirmSuccessor("late-del", "new-base"); !errors.Is(err, ErrNotBlocked) {
		t.Fatalf("repeat confirm = %v, want ErrNotBlocked", err)
	}
	if err := rt.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := service.EffectCount(); got != effectsBefore+3 {
		t.Fatalf("effects = %d, want %d (h7 + new-base + late-del only)", got, effectsBefore+3)
	}
}

// A genuinely unknown premise is still rejected.
func TestLateSubmissionOnUnknownPremiseRejected(t *testing.T) {
	rt, _, _ := setupHistory(t)
	if _, _, err := rt.Add("late-x", "demo", "x", "never-existed"); !errors.Is(err, ErrDependencyMissing) {
		t.Fatalf("unknown premise add = %v, want ErrDependencyMissing", err)
	}
}

// Dependencies and disputes appearing between preview and adoption keep
// the affected records live; only still-clean records are removed.
func TestAdoptionRechecksDependenciesGainedAfterPreview(t *testing.T) {
	rt, _, _ := setupHistory(t)
	plan, err := rt.PreviewConsolidation()
	if err != nil {
		t.Fatal(err)
	}
	if got := entryClass(t, plan, "h3"); got != ClassDeletable {
		t.Fatalf("h3 preview class = %s", got)
	}

	// After the preview h3 gains a waiting dependent and h2 (classified
	// summary-only) becomes disputed. Neither may be cleaned.
	if _, _, err := rt.Add("late-h3", "demo", "depends on h3", "h3"); err != nil {
		t.Fatal(err)
	}
	if err := rt.MarkRejected("h2", "late dispute after preview"); err != nil {
		t.Fatal(err)
	}

	if err := rt.AdoptConsolidation(plan.ID); err != nil {
		t.Fatal(err)
	}
	if _, ok := rt.Status("h3"); !ok {
		t.Fatal("h3 must be kept: it gained a waiting dependent after preview")
	}
	if err := rt.Flush(); err != nil {
		t.Fatal(err)
	}
	if op, _ := rt.Status("late-h3"); op.Status != StatusBlocked {
		t.Fatalf("late-h3 = %s, want blocked gap on canceled h3", op.Status)
	}
	if _, ok := rt.Status("h2"); !ok {
		t.Fatal("h2 must be kept: it became disputed after preview")
	}
	// Adopted plan explains why h3 deviated from the preview.
	adopted := rt.ConsolidationPlans()[len(rt.ConsolidationPlans())-1]
	var h3Entry *ConsolidationEntry
	for i := range adopted.Entries {
		if adopted.Entries[i].ID == "h3" {
			h3Entry = &adopted.Entries[i]
		}
	}
	if h3Entry == nil || h3Entry.Outcome != outcomeKept || h3Entry.OutcomeReason == "" {
		t.Fatalf("h3 entry = %#v, want kept with reason", h3Entry)
	}
}
