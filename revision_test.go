package offlinequeue

import (
	"errors"
	"strings"
	"testing"
)

func mustPlan(t *testing.T, rt *Runtime, assignments map[string]string) *DependencyRevisionPlan {
	t.Helper()
	plan, err := rt.PlanDependencyRevision(assignments)
	if err != nil {
		t.Fatalf("plan revision: %v", err)
	}
	return plan
}

func conflictFor(t *testing.T, plan *DependencyRevisionPlan, opID string) DependencyConflict {
	t.Helper()
	for _, c := range plan.Conflicts {
		if c.OpID == opID {
			return c
		}
	}
	t.Fatalf("no conflict recorded for %s (plan %+v)", opID, plan)
	return DependencyConflict{}
}

func TestDependencyRevisionDiamondPartialAdoption(t *testing.T) {
	dir := t.TempDir()
	service := NewMockService()
	rt, err := NewRuntime("c1", mustStore(t, dir, "c1"), service)
	if err != nil {
		t.Fatal(err)
	}
	add := func(id, dep string) {
		t.Helper()
		if _, _, err := rt.Add(id, "demo", "payload-"+id, dep); err != nil {
			t.Fatalf("add %s: %v", id, err)
		}
	}
	// Diamond: b and c share premise a; d depends on b.
	add("a", "")
	add("b", "a")
	add("c", "a")
	add("d", "b")

	service.Reject["a"] = "stale premise"
	if err := rt.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	for _, id := range []string{"b", "c"} {
		op, _ := rt.Status(id)
		if op.Status != StatusBlocked || op.RejectReason != "dependency rejected" {
			t.Fatalf("%s = (%s, %q), want blocked dependency rejected", id, op.Status, op.RejectReason)
		}
	}
	if op, _ := rt.Status("d"); op.Status != StatusBlocked || op.RejectReason != "dependency blocked" {
		t.Fatalf("d = (%s, %q), want blocked dependency blocked", op.Status, op.RejectReason)
	}

	if _, err := rt.Revise("a", "a2", "payload-a2", 0); err != nil {
		t.Fatalf("revise: %v", err)
	}
	if err := rt.Flush(); err != nil {
		t.Fatalf("flush revision: %v", err)
	}

	plan := mustPlan(t, rt, map[string]string{"b": "a2", "c": "a2", "d": "b"})
	if len(plan.Conflicts) != 0 || len(plan.Valid) != 3 {
		t.Fatalf("plan = %+v, want 3 valid and no conflicts", plan)
	}

	// Partial adoption: only b and c are adopted; d stays blocked.
	if err := rt.AdoptDependencyRevision(plan.ID, []string{"b", "c"}); err != nil {
		t.Fatalf("adopt partial: %v", err)
	}
	if op, _ := rt.Status("d"); op.Status != StatusBlocked || op.DependsOn != "b" {
		t.Fatalf("d after partial adopt = (%s, dep %s), want blocked on b", op.Status, op.DependsOn)
	}
	report, err := rt.FlushFair(1)
	if err != nil {
		t.Fatalf("flush fair: %v", err)
	}
	if len(report.Sent) != 2 {
		t.Fatalf("sent = %v, want [b c]", report.Sent)
	}
	if op, _ := rt.Status("d"); op.Status != StatusBlocked {
		t.Fatalf("d adopted? status = %s, want blocked", op.Status)
	}

	// Adopt the rest; d now unblocks and sends after its premise b.
	if err := rt.AdoptDependencyRevision(plan.ID, []string{"d"}); err != nil {
		t.Fatalf("adopt d: %v", err)
	}
	// A new dependency added mid-execution joins the graph normally.
	add("e", "d")
	if _, err := rt.FlushFair(2); err != nil {
		t.Fatalf("flush fair 2: %v", err)
	}

	var order []string
	for _, op := range service.EffectsSnapshot() {
		order = append(order, op.ID)
	}
	want := []string{"a2", "b", "c", "d", "e"}
	if strings.Join(order, ",") != strings.Join(want, ",") {
		t.Fatalf("effects = %v, want %v", order, want)
	}
	if got := service.EffectCount(); got != len(want) {
		t.Fatalf("effect count = %d, want %d (each exactly once)", got, len(want))
	}
	for _, id := range want {
		if op, _ := rt.Status(id); op.Status != StatusAcknowledged {
			t.Fatalf("%s status = %s, want acknowledged", id, op.Status)
		}
	}
}

func TestDependencyRevisionDetectsThreeWayCycle(t *testing.T) {
	dir := t.TempDir()
	service := NewMockService()
	rt, err := NewRuntime("c2", mustStore(t, dir, "c2"), service)
	if err != nil {
		t.Fatal(err)
	}
	// Blocked gaps whose premises were removed from history.
	for _, id := range []string{"x", "y", "z"} {
		op, _, err := rt.Add(id, "demo", "payload-"+id, "gone-"+id)
		if err != nil {
			t.Fatalf("add %s: %v", id, err)
		}
		if op.Status != StatusBlocked || op.MissingPremise != "gone-"+id {
			t.Fatalf("%s = (%s, missing %q)", id, op.Status, op.MissingPremise)
		}
	}

	plan := mustPlan(t, rt, map[string]string{"x": "y", "y": "z", "z": "x"})
	if len(plan.Valid) != 0 || len(plan.Conflicts) != 3 {
		t.Fatalf("plan = %+v, want 3 conflicts and no valid", plan)
	}
	c := conflictFor(t, plan, "x")
	if strings.Join(c.Chain, ",") != "x,y,z,x" {
		t.Fatalf("minimal chain = %v, want [x y z x]", c.Chain)
	}
	if !strings.Contains(c.Reason, "cycle") {
		t.Fatalf("reason = %q, want cycle", c.Reason)
	}
	if err := rt.AdoptDependencyRevision(plan.ID, []string{"x"}); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("adopting conflicted assignment = %v, want ErrRevisionConflict", err)
	}
	for _, id := range []string{"x", "y", "z"} {
		if op, _ := rt.Status(id); op.Status != StatusBlocked {
			t.Fatalf("%s status = %s, want still blocked", id, op.Status)
		}
	}
	if got := service.EffectCount(); got != 0 {
		t.Fatalf("effects = %d, want 0", got)
	}

	// A self-reference is caught by the same transitive analysis.
	self := mustPlan(t, rt, map[string]string{"x": "x"})
	sc := conflictFor(t, self, "x")
	if strings.Join(sc.Chain, ",") != "x,x" {
		t.Fatalf("self chain = %v, want [x x]", sc.Chain)
	}
}

func TestDependencyRevisionRejectsContradictoryPremise(t *testing.T) {
	dir := t.TempDir()
	service := NewMockService()
	rt, err := NewRuntime("c3", mustStore(t, dir, "c3"), service)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := rt.Add("bad", "demo", "p", ""); err != nil {
		t.Fatal(err)
	}
	if _, _, err := rt.Add("w", "demo", "p", "gone"); err != nil {
		t.Fatal(err)
	}
	service.Reject["bad"] = "no"
	if err := rt.Flush(); err != nil {
		t.Fatal(err)
	}
	plan := mustPlan(t, rt, map[string]string{"w": "bad"})
	c := conflictFor(t, plan, "w")
	if !strings.Contains(c.Reason, "contradictory premise") {
		t.Fatalf("reason = %q, want contradictory premise", c.Reason)
	}
	if op, _ := rt.Status("w"); op.Status != StatusBlocked {
		t.Fatalf("w status = %s, want blocked", op.Status)
	}
}

// flakyRemote fails selected operations with a transient error.
type flakyRemote struct {
	inner *MockService
	fail  map[string]bool
}

func (f *flakyRemote) Submit(op Operation) error {
	if f.fail[op.ID] {
		return ErrRemoteUnavailable
	}
	return f.inner.Submit(op)
}

func (f *flakyRemote) Cancel(id string) error { return f.inner.Cancel(id) }

func (f *flakyRemote) Verify(id string) (bool, error) { return f.inner.Verify(id) }

func TestFlushFairDefersStuckBranchWithoutStarvingOthers(t *testing.T) {
	dir := t.TempDir()
	service := NewMockService()
	remote := &flakyRemote{inner: service, fail: map[string]bool{"a1": true}}
	rt, err := NewRuntime("c4", mustStore(t, dir, "c4"), remote)
	if err != nil {
		t.Fatal(err)
	}
	add := func(id, dep string) {
		t.Helper()
		if _, _, err := rt.Add(id, "demo", "payload-"+id, dep); err != nil {
			t.Fatalf("add %s: %v", id, err)
		}
	}
	add("a1", "")
	add("a2", "a1")
	add("b1", "")
	add("b2", "b1")
	// A lost acknowledgement forces a resubmission; the effect must
	// still happen exactly once.
	service.DropAcks["b1"] = 1

	report, err := rt.FlushFair(1)
	if err != nil {
		t.Fatalf("flush fair: %v", err)
	}
	if strings.Join(report.Deferred, ",") != "a1,b1" {
		t.Fatalf("deferred = %v, want [a1 b1]", report.Deferred)
	}
	// The independent b branch is not starved by the stuck a branch,
	// even with a single execution slot: b1 was attempted, lost its
	// ack, and both branches can be retried on the next call.
	report, err = rt.FlushFair(1)
	if err != nil {
		t.Fatalf("flush fair retry: %v", err)
	}
	if strings.Join(report.Sent, ",") != "b1,b2" {
		t.Fatalf("sent = %v, want [b1 b2]", report.Sent)
	}
	if op, _ := rt.Status("b2"); op.Status != StatusAcknowledged {
		t.Fatalf("b2 = %s, want acknowledged (independent branch progressed)", op.Status)
	}
	if op, _ := rt.Status("a2"); op.Status != StatusPending {
		t.Fatalf("a2 = %s, want pending behind deferred a1", op.Status)
	}

	// The stuck branch recovers once its premise is known again.
	remote.fail["a1"] = false
	if _, err := rt.FlushFair(1); err != nil {
		t.Fatal(err)
	}
	if _, err := rt.FlushFair(1); err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, op := range service.EffectsSnapshot() {
		counts[op.ID]++
	}
	for _, id := range []string{"a1", "a2", "b1", "b2"} {
		if counts[id] != 1 {
			t.Fatalf("%s effects = %d, want exactly 1", id, counts[id])
		}
	}
}

// crashRemote applies the effect remotely, then reports a failure so the
// operation is left in flight with an unknown effect.
type crashRemote struct {
	inner   *MockService
	crashed map[string]bool
}

func (c *crashRemote) Submit(op Operation) error {
	err := c.inner.Submit(op)
	if err == nil && !c.crashed[op.ID] {
		c.crashed[op.ID] = true
		return errors.New("connection lost after apply")
	}
	return err
}

func (c *crashRemote) Cancel(id string) error { return c.inner.Cancel(id) }

func (c *crashRemote) Verify(id string) (bool, error) { return c.inner.Verify(id) }

func TestFlushFairVerifiesUnknownEffectBeforeResubmit(t *testing.T) {
	dir := t.TempDir()
	service := NewMockService()
	remote := &crashRemote{inner: service, crashed: map[string]bool{}}
	rt, err := NewRuntime("c5", mustStore(t, dir, "c5"), remote)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := rt.Add("op", "demo", "payload", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := rt.FlushFair(1); err == nil {
		t.Fatal("expected crash error from first flush")
	}
	if op, _ := rt.Status("op"); op.Status != StatusSending {
		t.Fatalf("op = %s, want sending (effect unknown)", op.Status)
	}
	// Recovery: the effect is verified as already applied, so the
	// operation is acknowledged without any re-execution.
	report, err := rt.FlushFair(1)
	if err != nil {
		t.Fatalf("recovery flush: %v", err)
	}
	if len(report.Sent) != 0 {
		t.Fatalf("resent %v, want verification without resubmission", report.Sent)
	}
	if op, _ := rt.Status("op"); op.Status != StatusAcknowledged {
		t.Fatalf("op = %s, want acknowledged", op.Status)
	}
	if got := service.EffectCount(); got != 1 {
		t.Fatalf("effects = %d, want exactly 1", got)
	}
}

func TestArchivedSummaryIdentityCannotBeReplaced(t *testing.T) {
	dir := t.TempDir()
	service := NewMockService()
	rt, err := NewRuntime("c6", mustStore(t, dir, "c6"), service)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := rt.Add("s1", "demo", "payload", ""); err != nil {
		t.Fatal(err)
	}
	if err := rt.Flush(); err != nil {
		t.Fatal(err)
	}
	plan, err := rt.PreviewConsolidation()
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.AdoptConsolidation(plan.ID); err != nil {
		t.Fatal(err)
	}
	if _, ok := rt.Archived("s1"); !ok {
		t.Fatal("s1 should be archived as a summary")
	}
	// A same-named new record must not replace the confirmed summary.
	if _, _, err := rt.Add("s1", "demo", "impostor", ""); !errors.Is(err, ErrArchivedIdentity) {
		t.Fatalf("reusing archived id = %v, want ErrArchivedIdentity", err)
	}
	// The confirmed summary remains a legitimate premise for late work.
	if _, _, err := rt.Add("late", "demo", "payload", "s1"); err != nil {
		t.Fatalf("late op on archived premise: %v", err)
	}
	if err := rt.Flush(); err != nil {
		t.Fatal(err)
	}
	if op, _ := rt.Status("late"); op.Status != StatusAcknowledged {
		t.Fatalf("late = %s, want acknowledged", op.Status)
	}
	if got := service.EffectCount(); got != 2 {
		t.Fatalf("effects = %d, want 2 (s1 once, late once)", got)
	}
}
