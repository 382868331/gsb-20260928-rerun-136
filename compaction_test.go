package offlinequeue

import (
	"errors"
	"strings"
	"testing"
)

func buildCompactionRuntime(t *testing.T) (*Runtime, *MockService, string) {
	t.Helper()
	dir := t.TempDir()
	service := NewMockService()
	rt, err := NewRuntime("client-c", mustStore(t, dir, "client-c"), service)
	if err != nil {
		t.Fatal(err)
	}
	add := func(id, dep string) {
		t.Helper()
		if _, _, err := rt.Add(id, "demo", "payload-"+id, dep); err != nil {
			t.Fatalf("add %s: %v", id, err)
		}
	}
	add("done", "")
	add("kept", "")
	add("cancel-me", "")
	add("disputed", "")
	if err := rt.CancelUnsent("cancel-me"); err != nil {
		t.Fatal(err)
	}
	service.Reject["disputed"] = "still argued about"
	if err := rt.Flush(); err != nil {
		t.Fatal(err)
	}
	// Added after the flush so their effect is still unknown.
	add("unknown", "")
	add("waiting", "kept")
	return rt, service, dir
}

func entryFor(t *testing.T, plan *CompactionPlan, id string) CompactionEntry {
	t.Helper()
	for _, e := range plan.Entries {
		if e.OpID == id {
			return e
		}
	}
	t.Fatalf("no entry for %s", id)
	return CompactionEntry{}
}

func TestCompactionPreviewClassifiesWithoutTouchingHistory(t *testing.T) {
	rt, service, _ := buildCompactionRuntime(t)

	plan, err := rt.PreviewCompaction()
	if err != nil {
		t.Fatal(err)
	}
	if plan.Status != PlanPreview {
		t.Fatalf("plan status = %s", plan.Status)
	}
	cases := map[string]string{
		"done":      ActionSummarize,
		"kept":      ActionKeep,
		"waiting":   ActionProtected, // effect unknown
		"cancel-me": ActionDelete,
		"disputed":  ActionProtected, // disputed, not dropped by age
		"unknown":   ActionProtected, // unknown effect
	}
	for id, want := range cases {
		if got := entryFor(t, plan, id).Action; got != want {
			t.Errorf("%s action = %s, want %s", id, got, want)
		}
	}
	if !strings.Contains(entryFor(t, plan, "kept").Reason, "waiting") {
		t.Errorf("keep reason should name the waiting task: %s", entryFor(t, plan, "kept").Reason)
	}
	// Preview alone changes nothing.
	if got := service.EffectCount(); got != 2 {
		t.Fatalf("effects after preview = %d, want 2", got)
	}
	if _, ok := rt.Status("done"); !ok {
		t.Fatal("preview must not remove records")
	}
}

func TestAdoptedCompactionKeepsScopeAndReleasesViaSummary(t *testing.T) {
	rt, service, dir := buildCompactionRuntime(t)

	plan, err := rt.PreviewCompaction()
	if err != nil {
		t.Fatal(err)
	}
	// A late submission arrives during the preview and re-references a
	// record that was classified as summarize-only.
	if _, _, err := rt.Add("late-joiner", "demo", "late", "done"); err != nil {
		t.Fatal(err)
	}

	adopted, err := rt.AdoptCompaction(plan.ID)
	if err != nil {
		t.Fatal(err)
	}
	if adopted.Status != PlanAdopted {
		t.Fatalf("status = %s", adopted.Status)
	}
	if len(adopted.Scope) == 0 {
		t.Fatal("adopted plan must keep the scope it was based on")
	}
	// "done" gained a waiting dependent during preview: skipped, kept live.
	e := entryFor(t, adopted, "done")
	if e.Applied || !strings.Contains(e.Note, "late-joiner") {
		t.Fatalf("done entry = %+v, want skipped because of late-joiner", e)
	}
	// "cancel-me" was deleted and recorded as a gap.
	if _, ok := rt.Status("cancel-me"); ok {
		t.Fatal("cancel-me should be deleted")
	}
	if _, ok := rt.ArchivedSummary("cancel-me"); ok {
		t.Fatal("deleted records keep no summary")
	}
	// Comp the plan is auditable after restart.
	rt2, err := NewRuntime("client-c", mustStore(t, dir, "client-c"), service)
	if err != nil {
		t.Fatal(err)
	}
	plans := rt2.CompactionPlans()
	if len(plans) != 1 || plans[0].Status != PlanAdopted || len(plans[0].Scope) == 0 {
		t.Fatalf("plans after restart = %+v", plans)
	}
	// Compaction never re-executes old effects.
	if got := service.EffectCount(); got != 2 {
		t.Fatalf("effects after adopt = %d, want 2", got)
	}
	// Independent waiting work is not halted by the compaction.
	if err := rt2.Flush(); err != nil {
		t.Fatal(err)
	}
	if op, _ := rt2.Status("late-joiner"); op.Status != StatusAcknowledged {
		t.Fatalf("late-joiner = %s, want acknowledged", op.Status)
	}
	if op, _ := rt2.Status("done"); op.Status != StatusAcknowledged {
		t.Fatalf("done = %s, want acknowledged (kept live)", op.Status)
	}
}

func TestSummarizedFactStillReleasesLateDependents(t *testing.T) {
	rt, service, _ := buildCompactionRuntime(t)
	plan, err := rt.PreviewCompaction()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.AdoptCompaction(plan.ID); err != nil {
		t.Fatal(err)
	}
	if _, ok := rt.ArchivedSummary("done"); !ok {
		t.Fatal("done should be archived as a summary")
	}
	// Late correction referencing the summarized fact can still attach.
	if _, _, err := rt.Add("correction", "demo", "fix", "done"); err != nil {
		t.Fatalf("late attach to summarized fact: %v", err)
	}
	if err := rt.Flush(); err != nil {
		t.Fatal(err)
	}
	if op, _ := rt.Status("correction"); op.Status != StatusAcknowledged {
		t.Fatalf("correction = %s, want acknowledged", op.Status)
	}
	chain := rt.ExplainChain("correction")
	if len(chain) != 2 || chain[1].Source != "archived" {
		t.Fatalf("chain = %+v, want live -> archived", chain)
	}
	_ = service
}

func TestDeletedDependencyLeavesProcessableGap(t *testing.T) {
	rt, service, _ := buildCompactionRuntime(t)
	plan, err := rt.PreviewCompaction()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.AdoptCompaction(plan.ID); err != nil {
		t.Fatal(err)
	}
	// Settle unrelated pending work; compaction must not hold it back.
	if err := rt.Flush(); err != nil {
		t.Fatal(err)
	}
	before := service.EffectCount()
	// A very late operation references the deleted record.
	if _, _, err := rt.Add("orphan", "demo", "late", "cancel-me"); err != nil {
		t.Fatalf("gap add should be accepted, got %v", err)
	}
	op, _ := rt.Status("orphan")
	if op.Status != StatusBlocked || !strings.Contains(op.RejectReason, plan.ID) {
		t.Fatalf("orphan = %#v, want blocked gap naming %s", op, plan.ID)
	}
	// The gap is processable: confirm a new basis, then it flushes.
	if err := rt.ConfirmSuccessor("orphan", "kept"); err != nil {
		t.Fatal(err)
	}
	if err := rt.Flush(); err != nil {
		t.Fatal(err)
	}
	if op, _ := rt.Status("orphan"); op.Status != StatusAcknowledged {
		t.Fatalf("orphan = %s, want acknowledged", op.Status)
	}
	// History was not filled by re-executing old effects.
	if got := service.EffectCount(); got != before+1 { // orphan only
		t.Fatalf("effects = %d, want %d", got, before+2)
	}
	chain := rt.ExplainChain("orphan")
	if len(chain) < 2 || chain[1].ID != "kept" {
		t.Fatalf("chain after re-point = %+v", chain)
	}
}

func TestWithdrawOnlyUnadoptedPreviews(t *testing.T) {
	rt, _, _ := buildCompactionRuntime(t)
	plan, err := rt.PreviewCompaction()
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.WithdrawCompaction(plan.ID); err != nil {
		t.Fatal(err)
	}
	if err := rt.WithdrawCompaction(plan.ID); !errors.Is(err, ErrPlanNotPreview) {
		t.Fatalf("double withdraw = %v", err)
	}
	plan2, err := rt.PreviewCompaction()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.AdoptCompaction(plan2.ID); err != nil {
		t.Fatal(err)
	}
	if err := rt.WithdrawCompaction(plan2.ID); !errors.Is(err, ErrPlanNotPreview) {
		t.Fatalf("withdraw adopted = %v, want ErrPlanNotPreview", err)
	}
	if err := rt.WithdrawCompaction("compact:999"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("withdraw unknown = %v", err)
	}
}
