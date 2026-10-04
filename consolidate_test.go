package offlinequeue

import (
	"errors"
	"testing"
)

func entryClass(t *testing.T, plan *ConsolidationPlan, id string) ConsolidationClass {
	t.Helper()
	for _, e := range plan.Entries {
		if e.ID == id {
			return e.Class
		}
	}
	t.Fatalf("plan %s has no entry for %s", plan.ID, id)
	return ""
}

// Builds a history: h1(acked) <- h2(acked, depends h1), h3 canceled alone,
// h4 acked but referenced by waiting h5, h6 rejected (disputed).
func setupHistory(t *testing.T) (*Runtime, *MockService, string) {
	t.Helper()
	dir := t.TempDir()
	service := NewMockService()
	rt, err := NewRuntime("c", mustStore(t, dir, "c"), service)
	if err != nil {
		t.Fatal(err)
	}
	add := func(id, dep string) {
		t.Helper()
		if _, _, err := rt.Add(id, "demo", "p-"+id, dep); err != nil {
			t.Fatalf("add %s: %v", id, err)
		}
	}
	add("h1", "")
	add("h2", "h1")
	add("h3", "")
	add("h4", "")
	add("h6", "")
	add("h5", "h6")
	if err := rt.CancelUnsent("h3"); err != nil {
		t.Fatal(err)
	}
	service.Reject["h6"] = "disputed"
	if err := rt.Flush(); err != nil {
		t.Fatal(err)
	}
	// h7 is still waiting on the confirmed h4, keeping h4 referenced.
	add("h7", "h4")
	return rt, service, dir
}

func TestConsolidationPreviewClassifiesHistory(t *testing.T) {
	rt, _, _ := setupHistory(t)
	plan, err := rt.PreviewConsolidation()
	if err != nil {
		t.Fatal(err)
	}
	if got := entryClass(t, plan, "h1"); got != ClassSummaryOnly {
		t.Fatalf("h1 class = %s, want summary_only", got)
	}
	if got := entryClass(t, plan, "h2"); got != ClassSummaryOnly {
		t.Fatalf("h2 class = %s, want summary_only", got)
	}
	if got := entryClass(t, plan, "h3"); got != ClassDeletable {
		t.Fatalf("h3 class = %s, want deletable", got)
	}
	if got := entryClass(t, plan, "h4"); got != ClassKeepReferenced {
		t.Fatalf("h4 class = %s, want keep_referenced", got)
	}
	if got := entryClass(t, plan, "h5"); got != ClassProtected {
		t.Fatalf("h5 class = %s, want protected", got)
	}
	if got := entryClass(t, plan, "h6"); got != ClassProtected {
		t.Fatalf("h6 class = %s, want protected (disputed)", got)
	}
	if got := entryClass(t, plan, "h7"); got != ClassProtected {
		t.Fatalf("h7 class = %s, want protected (waiting)", got)
	}
}

func TestConsolidationWithdrawAndAdoptKeepsScope(t *testing.T) {
	rt, service, _ := setupHistory(t)
	first, err := rt.PreviewConsolidation()
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.WithdrawConsolidation(first.ID); err != nil {
		t.Fatal(err)
	}
	if err := rt.AdoptConsolidation(first.ID); !errors.Is(err, ErrPlanWithdrawn) {
		t.Fatalf("adopt withdrawn = %v, want ErrPlanWithdrawn", err)
	}
	second, err := rt.PreviewConsolidation()
	if err != nil {
		t.Fatal(err)
	}
	effectsBefore := service.EffectCount()
	if err := rt.AdoptConsolidation(second.ID); err != nil {
		t.Fatal(err)
	}
	if err := rt.WithdrawConsolidation(second.ID); !errors.Is(err, ErrPlanAdopted) {
		t.Fatalf("withdraw adopted = %v, want ErrPlanAdopted", err)
	}
	if got := service.EffectCount(); got != effectsBefore {
		t.Fatalf("consolidation re-executed effects: %d -> %d", effectsBefore, got)
	}
	if _, ok := rt.Status("h3"); ok {
		t.Fatal("h3 should be deleted")
	}
	if _, ok := rt.Status("h1"); ok {
		t.Fatal("h1 should be archived, not live")
	}
	summary, ok := rt.Archived("h1")
	if !ok || summary.Status != StatusAcknowledged {
		t.Fatalf("h1 archive = %#v, %v", summary, ok)
	}
	if op, ok := rt.Status("h4"); !ok || op.Status != StatusAcknowledged {
		t.Fatalf("h4 must be kept referenced, got %#v", op)
	}
	// Adopted plan keeps the scope it was based on.
	plans := rt.ConsolidationPlans()
	if len(plans) != 2 || !plans[1].Adopted || len(plans[1].Scope) != 7 {
		t.Fatalf("plans = %#v", plans)
	}
}

func TestConsolidationCandidateSurvivesRestart(t *testing.T) {
	rt, _, dir := setupHistory(t)
	plan, err := rt.PreviewConsolidation()
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := NewRuntime("c", mustStore(t, dir, "c"), NewMockService())
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.AdoptConsolidation(plan.ID); err != nil {
		t.Fatalf("adopt after restart: %v", err)
	}
	if _, ok := restarted.Status("h3"); ok {
		t.Fatal("h3 should be deleted after restart adoption")
	}
}

func TestLateSubmissionJoinsArchivedDependency(t *testing.T) {
	rt, service, _ := setupHistory(t)
	plan, err := rt.PreviewConsolidation()
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.AdoptConsolidation(plan.ID); err != nil {
		t.Fatal(err)
	}
	// Late old operation referencing an archived acknowledged fact.
	if _, _, err := rt.Add("late-1", "demo", "late", "h1"); err != nil {
		t.Fatalf("late add on archived acked dep: %v", err)
	}
	if err := rt.Flush(); err != nil {
		t.Fatal(err)
	}
	if op, _ := rt.Status("late-1"); op.Status != StatusAcknowledged {
		t.Fatalf("late-1 status = %s", op.Status)
	}
	// Independent tasks were never halted by consolidation.
	if _, _, err := rt.Add("indep", "demo", "independent", ""); err != nil {
		t.Fatal(err)
	}
	if err := rt.Flush(); err != nil {
		t.Fatal(err)
	}
	if op, _ := rt.Status("indep"); op.Status != StatusAcknowledged {
		t.Fatalf("indep status = %s", op.Status)
	}
	_ = service
}

func TestLateCorrectionCreatesProcessableGap(t *testing.T) {
	rt, service, _ := setupHistory(t)
	plan, err := rt.PreviewConsolidation()
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.AdoptConsolidation(plan.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := rt.Add("late-2", "demo", "late", "h1"); err != nil {
		t.Fatal(err)
	}
	affected, err := rt.CorrectArchived("h1", "client reports wrong result")
	if err != nil {
		t.Fatal(err)
	}
	if len(affected) != 1 || affected[0] != "late-2" {
		t.Fatalf("affected = %v", affected)
	}
	if op, _ := rt.Status("late-2"); op.Status != StatusBlocked {
		t.Fatalf("late-2 status = %s, want blocked gap", op.Status)
	}
	unresolved := rt.Unresolved()
	found := false
	for _, op := range unresolved {
		if op.ID == "late-2" {
			found = true
		}
	}
	if !found {
		t.Fatalf("unresolved = %v, want late-2 gap listed", unresolved)
	}
	effectsBefore := service.EffectCount()
	if err := rt.Flush(); err != nil {
		t.Fatal(err)
	}
	// Only the independent h7 may be sent; blocked late-2 and archived
	// records are never re-executed to fill history.
	if got := service.EffectCount(); got != effectsBefore+1 {
		t.Fatalf("effects after correction flush = %d, want %d", got, effectsBefore+1)
	}
	summary, _ := rt.Archived("h1")
	if !summary.Corrected || summary.CorrectNote == "" {
		t.Fatalf("summary = %#v", summary)
	}
	// Gap is processable: confirm a new premise and flush.
	if _, _, err := rt.Add("fix", "demo", "fixed", ""); err != nil {
		t.Fatal(err)
	}
	if err := rt.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := rt.ConfirmSuccessor("late-2", "fix"); err != nil {
		t.Fatal(err)
	}
	if err := rt.Flush(); err != nil {
		t.Fatal(err)
	}
	if op, _ := rt.Status("late-2"); op.Status != StatusAcknowledged {
		t.Fatalf("late-2 after gap resolution = %s", op.Status)
	}
}

func TestLateSubmissionOnDeletedRecordBecomesProcessableGap(t *testing.T) {
	rt, service, dir := setupHistory(t)
	plan, err := rt.PreviewConsolidation()
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.AdoptConsolidation(plan.ID); err != nil {
		t.Fatal(err)
	}
	if _, ok := rt.Status("h3"); ok {
		t.Fatal("h3 should be deleted by consolidation")
	}

	// Late resubmission depending on the deleted record is kept as a
	// blocked gap, not rejected at the door.
	gap, created, err := rt.Add("late-del", "demo", "original-intent", "h3")
	if err != nil {
		t.Fatalf("late add on deleted dep: %v", err)
	}
	if !created || gap.Status != StatusBlocked {
		t.Fatalf("late-del = %#v, want blocked gap", gap)
	}
	if gap.MissingPremise != "h3" || gap.ConsolidationScope != plan.ID {
		t.Fatalf("gap markers = (%q, %q), want (h3, %s)",
			gap.MissingPremise, gap.ConsolidationScope, plan.ID)
	}
	if gap.Payload != "original-intent" {
		t.Fatalf("gap lost original intent: %#v", gap)
	}

	// Duplicate resubmission returns the same record, never a second one.
	again, created, err := rt.Add("late-del", "demo", "changed", "h3")
	if err != nil || created {
		t.Fatalf("duplicate late add = (%v, %v), want (false, nil)", created, err)
	}
	if again.Payload != "original-intent" {
		t.Fatalf("duplicate add overwrote intent: %#v", again)
	}

	// The gap is visible in the unresolved list and survives a restart.
	found := false
	for _, op := range rt.Unresolved() {
		if op.ID == "late-del" {
			found = true
		}
	}
	if !found {
		t.Fatalf("unresolved = %v, want late-del listed", rt.Unresolved())
	}
	restarted, err := NewRuntime("c", mustStore(t, dir, "c"), service)
	if err != nil {
		t.Fatal(err)
	}
	found = false
	for _, op := range restarted.Unresolved() {
		if op.ID == "late-del" {
			found = true
		}
	}
	if !found {
		t.Fatalf("unresolved after restart = %#v, want late-del listed", restarted.Unresolved())
	}

	// Flushing never re-executes the deleted record to fill the gap.
	effectsBefore := service.EffectCount()
	if err := restarted.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := service.EffectCount(); got != effectsBefore+1 {
		t.Fatalf("effects = %d, want %d (only independent h7)", got, effectsBefore+1)
	}
	if op, _ := restarted.Status("late-del"); op.Status != StatusBlocked {
		t.Fatalf("late-del after flush = %#v, want still blocked", op)
	}

	// The user confirms a new valid premise, then the gap sends.
	if _, _, err := restarted.Add("fix", "demo", "fixed", ""); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := restarted.ConfirmSuccessor("late-del", "fix"); err != nil {
		t.Fatal(err)
	}
	// Duplicate confirmation does not apply twice.
	if err := restarted.ConfirmSuccessor("late-del", "fix"); !errors.Is(err, ErrNotBlocked) {
		t.Fatalf("duplicate confirm = %v, want ErrNotBlocked", err)
	}
	if err := restarted.Flush(); err != nil {
		t.Fatal(err)
	}
	op, _ := restarted.Status("late-del")
	if op.Status != StatusAcknowledged || op.MissingPremise != "" || op.ConsolidationScope != "" {
		t.Fatalf("late-del after resolution = %#v", op)
	}
	count := 0
	for _, e := range service.EffectsSnapshot() {
		if e.ID == "late-del" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("late-del applied %d times, want 1", count)
	}
}

func TestAdoptionRechecksDependenciesAddedAfterPreview(t *testing.T) {
	rt, _, _ := setupHistory(t)
	// An unrelated canceled record is deletable and stays so.
	if _, _, err := rt.Add("h9", "demo", "p-h9", ""); err != nil {
		t.Fatal(err)
	}
	if err := rt.CancelUnsent("h9"); err != nil {
		t.Fatal(err)
	}
	plan, err := rt.PreviewConsolidation()
	if err != nil {
		t.Fatal(err)
	}
	if got := entryClass(t, plan, "h3"); got != ClassDeletable {
		t.Fatalf("h3 class = %s, want deletable", got)
	}
	// Between preview and adoption a new waiting task starts depending
	// on the deletable record, and a summary-only record becomes
	// disputed. Neither may be cleaned by the stale candidate.
	if _, _, err := rt.Add("h8", "demo", "p-h8", "h3"); err != nil {
		t.Fatal(err)
	}
	if err := rt.MarkRejected("h2", "late dispute"); err != nil {
		t.Fatal(err)
	}
	if err := rt.AdoptConsolidation(plan.ID); err != nil {
		t.Fatal(err)
	}
	if op, ok := rt.Status("h3"); !ok || op.Status != StatusCanceled {
		t.Fatalf("h3 must be kept for new dependent, got %#v", op)
	}
	if op, ok := rt.Status("h2"); !ok || op.Status != StatusRejected {
		t.Fatalf("h2 must be kept as disputed, got %#v", op)
	}
	// h1 is now referenced by the disputed h2, so it is kept as well.
	if _, ok := rt.Status("h1"); !ok {
		t.Fatal("h1 must be kept while its dependent h2 is disputed")
	}
	// The unaffected deletable record added before adoption is cleaned.
	if _, ok := rt.Status("h9"); ok {
		t.Fatal("h9 should be deleted by adoption")
	}
	// The confirmed summary still supports the late dependent chain:
	// h8 was blocked on canceled h3 and stays a processable gap.
	if op, _ := rt.Status("h8"); op.Status != StatusBlocked {
		t.Fatalf("h8 = %#v, want blocked on canceled h3", op)
	}
}
