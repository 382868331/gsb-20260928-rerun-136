package offlinequeue

import (
	"errors"
	"strings"
	"testing"
)

func rebindEntry(t *testing.T, plan *RebindPlan, opID string) RebindEntry {
	t.Helper()
	for _, e := range plan.Entries {
		if e.OpID == opID {
			return e
		}
	}
	t.Fatalf("plan %s has no entry for %s", plan.ID, opID)
	return RebindEntry{}
}

func statusOf(t *testing.T, rt *Runtime, id string) Operation {
	t.Helper()
	op, ok := rt.Status(id)
	if !ok {
		t.Fatalf("operation %s not found", id)
	}
	return op
}

// Diamond: a shared premise fails, both branches and their join block;
// one batch rebinds the whole diamond onto the revision.
func TestRebindDiamondSharedPremise(t *testing.T) {
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
	add("root", "")
	add("left", "root")
	add("right", "root")
	add("join", "left")

	service.Reject["root"] = "root failed"
	if err := rt.Flush(); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"left", "right", "join"} {
		if got := statusOf(t, rt, id).Status; got != StatusBlocked {
			t.Fatalf("%s = %s, want blocked", id, got)
		}
	}
	if _, err := rt.Revise("root", "root-v2", "p-root-v2", 0); err != nil {
		t.Fatal(err)
	}

	plan, err := rt.PreviewRebind(map[string]string{
		"left":  "root-v2",
		"right": "root-v2",
		"join":  "left",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"left", "right", "join"} {
		if e := rebindEntry(t, plan, id); !e.Adoptable {
			t.Fatalf("entry %s not adoptable: %s", id, e.Reason)
		}
	}
	if err := rt.AdoptRebind(plan.ID, "left", "right", "join"); err != nil {
		t.Fatal(err)
	}
	if err := rt.FlushFair(2); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"root-v2", "left", "right", "join"} {
		if got := statusOf(t, rt, id).Status; got != StatusAcknowledged {
			t.Fatalf("%s = %s, want acknowledged", id, got)
		}
	}
	if got := service.EffectCount(); got != 4 {
		t.Fatalf("effects = %d, want 4 (each exactly once)", got)
	}
}

// A cycle spanning three operations must be detected with the minimal
// conflict chain, not just direct self references.
func TestRebindDetectsThreeOpCycle(t *testing.T) {
	dir := t.TempDir()
	service := NewMockService()
	rt, err := NewRuntime("c", mustStore(t, dir, "c"), service)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := rt.Add("base", "demo", "p", ""); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"x", "y", "z"} {
		if _, _, err := rt.Add(id, "demo", "p-"+id, "base"); err != nil {
			t.Fatal(err)
		}
	}
	service.Reject["base"] = "no"
	if err := rt.Flush(); err != nil {
		t.Fatal(err)
	}

	plan, err := rt.PreviewRebind(map[string]string{"x": "y", "y": "z", "z": "x"})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"x", "y", "z"} {
		e := rebindEntry(t, plan, id)
		if e.Adoptable {
			t.Fatalf("entry %s must not be adoptable inside a cycle", id)
		}
		if len(e.Conflict) != 4 {
			t.Fatalf("entry %s conflict = %v, want minimal 3-cycle chain", id, e.Conflict)
		}
		if e.Conflict[0] != e.Conflict[3] || e.Conflict[0] != id {
			t.Fatalf("entry %s conflict = %v, want chain starting and ending at %s", id, e.Conflict, id)
		}
	}
	if err := rt.AdoptRebind(plan.ID, "x"); !errors.Is(err, ErrRebindNotAdoptable) {
		t.Fatalf("adopting cyclic entry = %v, want ErrRebindNotAdoptable", err)
	}

	// Direct self reference is still rejected, with a length-1 chain.
	self, err := rt.PreviewRebind(map[string]string{"x": "x"})
	if err != nil {
		t.Fatal(err)
	}
	if e := rebindEntry(t, self, "x"); e.Adoptable || len(e.Conflict) != 2 {
		t.Fatalf("self reference entry = %+v, want conflict [x x]", e)
	}
	for _, id := range []string{"x", "y", "z"} {
		if got := statusOf(t, rt, id).Status; got != StatusBlocked {
			t.Fatalf("%s = %s, want still blocked", id, got)
		}
	}
}

// The valid part of a mixed batch can be adopted alone; the rest stays
// blocked with its original reason.
func TestRebindPartialAdoption(t *testing.T) {
	dir := t.TempDir()
	service := NewMockService()
	rt, err := NewRuntime("c", mustStore(t, dir, "c"), service)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := rt.Add("base", "demo", "p", ""); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"good", "c1", "c2"} {
		if _, _, err := rt.Add(id, "demo", "p-"+id, "base"); err != nil {
			t.Fatal(err)
		}
	}
	service.Reject["base"] = "no"
	if err := rt.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Revise("base", "base-v2", "p2", 0); err != nil {
		t.Fatal(err)
	}

	plan, err := rt.PreviewRebind(map[string]string{
		"good": "base-v2",
		"c1":   "c2",
		"c2":   "c1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if e := rebindEntry(t, plan, "good"); !e.Adoptable {
		t.Fatalf("good entry not adoptable: %s", e.Reason)
	}
	for _, id := range []string{"c1", "c2"} {
		if e := rebindEntry(t, plan, id); e.Adoptable || len(e.Conflict) != 3 {
			t.Fatalf("%s entry = %+v, want non-adoptable 2-cycle", id, e)
		}
	}
	if err := rt.AdoptRebind(plan.ID, "good"); err != nil {
		t.Fatal(err)
	}
	if err := rt.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := statusOf(t, rt, "good").Status; got != StatusAcknowledged {
		t.Fatalf("good = %s, want acknowledged", got)
	}
	for _, id := range []string{"c1", "c2"} {
		op := statusOf(t, rt, id)
		if op.Status != StatusBlocked || !strings.Contains(op.RejectReason, "dependency rejected") {
			t.Fatalf("%s = %#v, want still blocked on rejected base", id, op)
		}
	}
	if got := service.EffectCount(); got != 2 {
		t.Fatalf("effects = %d, want 2 (base-v2, good)", got)
	}
}

// A premise that stays unknown must not occupy execution slots:
// independent branches keep progressing under a tight slot limit, and
// duplicate resubmissions plus reconnects never duplicate effects.
func TestFlushFairNoStarvationAcrossReconnects(t *testing.T) {
	dir := t.TempDir()
	service := NewMockService()
	rt, err := NewRuntime("c", mustStore(t, dir, "c"), service)
	if err != nil {
		t.Fatal(err)
	}
	// s1 chains behind held, which itself waits on a premise that does
	// not exist yet; both are kept as blocked gaps.
	if _, _, err := rt.Add("s1", "demo", "p-s1", "held"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := rt.Add("held", "demo", "p-held", "ghost"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"i1", "i2", "i3", "i4", "i5"} {
		if _, _, err := rt.Add(id, "demo", "p-"+id, ""); err != nil {
			t.Fatal(err)
		}
	}

	// Interleave a duplicate resubmission and a reconnect: no effect may
	// happen twice and the blocked branch must not starve the others.
	if _, created, err := rt.Add("i1", "demo", "changed", ""); err != nil || created {
		t.Fatalf("duplicate add = (%v, %v)", created, err)
	}
	service.SetOffline(true)
	if err := rt.FlushFair(2); !errors.Is(err, ErrRemoteUnavailable) {
		t.Fatalf("offline flush = %v", err)
	}
	service.SetOffline(false)
	if err := rt.FlushFair(2); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"i1", "i2", "i3", "i4", "i5"} {
		if got := statusOf(t, rt, id).Status; got != StatusAcknowledged {
			t.Fatalf("%s = %s, want acknowledged (independent branch starved)", id, got)
		}
	}
	held := statusOf(t, rt, "held")
	if held.Status != StatusBlocked || held.MissingPremise != "ghost" {
		t.Fatalf("held = %#v, want blocked with missing premise ghost", held)
	}
	if got := statusOf(t, rt, "s1").Status; got != StatusBlocked {
		t.Fatalf("s1 = %s, want blocked", got)
	}
	if got := service.EffectCount(); got != 5 {
		t.Fatalf("effects = %d, want 5", got)
	}

	// The premise appears later as a new dependency during execution;
	// rebind the blocked chain and finish without re-running anything.
	if _, _, err := rt.Add("ghost", "demo", "p-ghost", ""); err != nil {
		t.Fatal(err)
	}
	plan, err := rt.PreviewRebind(map[string]string{"held": "ghost", "s1": "held"})
	if err != nil {
		t.Fatal(err)
	}
	if e := rebindEntry(t, plan, "held"); !e.Adoptable {
		t.Fatalf("held entry not adoptable: %s", e.Reason)
	}
	if e := rebindEntry(t, plan, "s1"); !e.Adoptable {
		t.Fatalf("s1 entry not adoptable: %s", e.Reason)
	}
	if err := rt.AdoptRebind(plan.ID, "held", "s1"); err != nil {
		t.Fatal(err)
	}
	if err := rt.FlushFair(1); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"ghost", "held", "s1"} {
		if got := statusOf(t, rt, id).Status; got != StatusAcknowledged {
			t.Fatalf("%s = %s, want acknowledged", id, got)
		}
	}
	if got := service.EffectCount(); got != 8 {
		t.Fatalf("effects = %d, want 8 (every operation exactly once)", got)
	}
	seen := map[string]int{}
	for _, op := range service.EffectsSnapshot() {
		seen[op.ID]++
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("effect %s applied %d times", id, n)
		}
	}
}

// Committed operations are never re-executed because of graph changes,
// and a non-blocked operation cannot be a rebind target.
func TestRebindNeverReexecutesCommitted(t *testing.T) {
	dir := t.TempDir()
	service := NewMockService()
	rt, err := NewRuntime("c", mustStore(t, dir, "c"), service)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := rt.Add("done", "demo", "p-done", ""); err != nil {
		t.Fatal(err)
	}
	if _, _, err := rt.Add("bad", "demo", "p-bad", ""); err != nil {
		t.Fatal(err)
	}
	if _, _, err := rt.Add("child", "demo", "p-child", "bad"); err != nil {
		t.Fatal(err)
	}
	service.Reject["bad"] = "no"
	if err := rt.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := statusOf(t, rt, "done").Status; got != StatusAcknowledged {
		t.Fatalf("done = %s", got)
	}

	// A committed operation is not a valid rebind target.
	plan, err := rt.PreviewRebind(map[string]string{"done": "bad"})
	if err != nil {
		t.Fatal(err)
	}
	if e := rebindEntry(t, plan, "done"); e.Adoptable {
		t.Fatalf("committed operation must not be rebindable")
	}

	// Rebinding a blocked child onto the committed fact sends the child
	// once and leaves the committed operation untouched.
	plan2, err := rt.PreviewRebind(map[string]string{"child": "done"})
	if err != nil {
		t.Fatal(err)
	}
	if e := rebindEntry(t, plan2, "child"); !e.Adoptable {
		t.Fatalf("child entry not adoptable: %s", e.Reason)
	}
	if err := rt.AdoptRebind(plan2.ID, "child"); err != nil {
		t.Fatal(err)
	}
	if err := rt.FlushFair(3); err != nil {
		t.Fatal(err)
	}
	if got := statusOf(t, rt, "child").Status; got != StatusAcknowledged {
		t.Fatalf("child = %s", got)
	}
	done := statusOf(t, rt, "done")
	if done.Attempts != 1 {
		t.Fatalf("done attempts = %d, want 1 (no re-execution)", done.Attempts)
	}
	count := 0
	for _, op := range service.EffectsSnapshot() {
		if op.ID == "done" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("done effect applied %d times", count)
	}
}

// An archived confirmed summary remains a legal premise, but its identity
// cannot be reused by a new record with the same name.
func TestArchivedSummaryIdentityProtected(t *testing.T) {
	rt, service, _ := setupHistory(t)
	plan, err := rt.PreviewConsolidation()
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.AdoptConsolidation(plan.ID); err != nil {
		t.Fatal(err)
	}
	// h1 is now an archived acknowledged summary; h3 was deleted.

	// Late operation depending on the deleted record becomes a blocked gap.
	if _, _, err := rt.Add("late", "demo", "p-late", "h3"); err != nil {
		t.Fatal(err)
	}
	late := statusOf(t, rt, "late")
	if late.Status != StatusBlocked || late.MissingPremise != "h3" {
		t.Fatalf("late = %#v, want blocked gap on h3", late)
	}

	// The archived confirmed summary is a legal rebind premise.
	rplan, err := rt.PreviewRebind(map[string]string{"late": "h1"})
	if err != nil {
		t.Fatal(err)
	}
	if e := rebindEntry(t, rplan, "late"); !e.Adoptable {
		t.Fatalf("late entry not adoptable: %s", e.Reason)
	}
	if err := rt.AdoptRebind(rplan.ID, "late"); err != nil {
		t.Fatal(err)
	}
	if err := rt.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := statusOf(t, rt, "late").Status; got != StatusAcknowledged {
		t.Fatalf("late = %s, want acknowledged via archived summary", got)
	}

	// A new record must not take over the archived summary's identity.
	if _, _, err := rt.Add("h1", "demo", "impostor", ""); !errors.Is(err, ErrArchivedIdentity) {
		t.Fatalf("add over archived id = %v, want ErrArchivedIdentity", err)
	}
	if got := service.EffectCount(); got != 5 {
		t.Fatalf("effects = %d, want 5 (no re-execution of archived facts)", got)
	}
}
