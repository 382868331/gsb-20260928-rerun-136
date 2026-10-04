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
