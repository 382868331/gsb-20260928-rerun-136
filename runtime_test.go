package offlinequeue

import (
	"errors"
	"strings"
	"testing"
)

func TestOfflineQueueLifecycleDeduplicatesAndPreservesDependencies(t *testing.T) {
	dir := t.TempDir()
	service := NewMockService()
	service.SetOffline(true)

	rt, err := NewRuntime("client-a", mustStore(t, dir, "client-a"), service)
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
	add("a2", "")
	add("a3", "a2")
	add("a4", "")
	add("a5", "")
	add("a6", "a5")

	if err := rt.Flush(); !errors.Is(err, ErrRemoteUnavailable) {
		t.Fatalf("offline flush error = %v", err)
	}
	if got := service.EffectCount(); got != 0 {
		t.Fatalf("offline effects = %d, want 0", got)
	}

	if _, created, err := rt.Add("a1", "demo", "changed", ""); err != nil || created {
		t.Fatalf("duplicate add = (%v, %v), want (false, nil)", created, err)
	}
	if err := rt.CancelUnsent("a5"); err != nil {
		t.Fatal(err)
	}

	service.Reject["a2"] = "business rule failed"
	service.DropAcks["a4"] = 1
	service.SetOffline(false)
	if err := rt.Flush(); err != nil {
		t.Fatalf("first online flush: %v", err)
	}

	assertStatus := func(id string, want Status) {
		t.Helper()
		got, ok := rt.Status(id)
		if !ok || got.Status != want {
			t.Fatalf("%s status = %#v, want %s", id, got, want)
		}
	}
	assertStatus("a1", StatusAcknowledged)
	assertStatus("a2", StatusRejected)
	assertStatus("a3", StatusBlocked)
	assertStatus("a4", StatusAcknowledged)
	assertStatus("a5", StatusCanceled)
	assertStatus("a6", StatusBlocked)

	if err := rt.RequestSubmittedCancel("a1"); !errors.Is(err, ErrAlreadyCommitted) {
		t.Fatalf("cancel acknowledged = %v, want ErrAlreadyCommitted", err)
	}
	if err := rt.CancelUnsent("a1"); !errors.Is(err, ErrNotCancelable) {
		t.Fatalf("cancel acknowledged as unsent = %v, want ErrNotCancelable", err)
	}
	if got := service.EffectCount(); got != 2 {
		t.Fatalf("effects after flush = %d, want 2", got)
	}

	restarted, err := NewRuntime("client-a", mustStore(t, dir, "client-a"), service)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Flush(); err != nil {
		t.Fatalf("restart flush: %v", err)
	}
	if got := service.EffectCount(); got != 2 {
		t.Fatalf("effects after restart = %d, want 2", got)
	}
}

func TestLostAcknowledgementRetriesWithoutDuplicateEffect(t *testing.T) {
	dir := t.TempDir()
	service := NewMockService()
	service.SetOffline(true)
	rt, err := NewRuntime("client-lost", mustStore(t, dir, "client-lost"), service)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := rt.Add("x1", "demo", "one", ""); err != nil {
		t.Fatal(err)
	}
	if err := rt.Flush(); !errors.Is(err, ErrRemoteUnavailable) {
		t.Fatal(err)
	}

	service.DropAcks["x1"] = 1
	service.SetOffline(false)
	if err := rt.Flush(); err != nil {
		t.Fatalf("lost ack flush = %v", err)
	}
	if got, ok := rt.Status("x1"); !ok || got.Status != StatusAcknowledged {
		t.Fatalf("after lost ack status = %#v, want acknowledged", got)
	}
	if got := service.EffectCount(); got != 1 {
		t.Fatalf("effects = %d, want 1", got)
	}
}

func TestClientsKeepIndependentLocalState(t *testing.T) {
	dir := t.TempDir()
	service := NewMockService()
	storeA := mustStore(t, dir, "client-a")
	storeB := mustStore(t, dir, "client-b")
	ra, err := NewRuntime("client-a", storeA, service)
	if err != nil {
		t.Fatal(err)
	}
	rb, err := NewRuntime("client-b", storeB, service)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := ra.Add("shared-id", "a", "", ""); err != nil {
		t.Fatal(err)
	}
	if _, _, err := rb.Add("shared-id", "b", "", ""); err != nil {
		t.Fatal(err)
	}
	if err := ra.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := rb.Flush(); err != nil {
		t.Fatal(err)
	}
	if a, ok := ra.Status("shared-id"); !ok || a.Kind != "a" || a.Status != StatusAcknowledged {
		t.Fatalf("client-a state = %#v", a)
	}
	if b, ok := rb.Status("shared-id"); !ok || b.Kind != "b" || b.Status != StatusAcknowledged {
		t.Fatalf("client-b state = %#v", b)
	}
}

func mustStore(t *testing.T, dir, clientID string) *FileStore {
	t.Helper()
	store, err := NewFileStore(dir, clientID)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func TestStalePreconditionRevisionKeepsIdentityAndProvenance(t *testing.T) {
	dir := t.TempDir()
	service := NewMockService()
	service.SetOffline(true)

	ra, err := NewRuntime("client-a", mustStore(t, dir, "client-a"), service)
	if err != nil {
		t.Fatal(err)
	}
	rb, err := NewRuntime("client-b", mustStore(t, dir, "client-b"), service)
	if err != nil {
		t.Fatal(err)
	}

	edit := Operation{ID: "edit", Kind: "rename", ObjectID: "doc-1", BaseVersion: 0}
	aEdit := edit
	aEdit.ID = "a-edit"
	aEdit.Payload = "title-from-a"
	if _, _, err := ra.AddOp(aEdit); err != nil {
		t.Fatal(err)
	}
	bEdit := edit
	bEdit.ID = "b-edit"
	bEdit.Payload = "title-from-b"
	if _, _, err := rb.AddOp(bEdit); err != nil {
		t.Fatal(err)
	}
	if _, _, err := rb.Add("b-follow", "annotate", "note", "b-edit"); err != nil {
		t.Fatal(err)
	}

	service.SetOffline(false)
	if err := ra.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := rb.Flush(); err != nil {
		t.Fatal(err)
	}

	rejected, ok := rb.Status("b-edit")
	if !ok || rejected.Status != StatusRejected {
		t.Fatalf("b-edit status = %#v, want rejected", rejected)
	}
	if !strings.Contains(rejected.RejectReason, ErrPreconditionFailed.Error()) {
		t.Fatalf("reject reason = %q, want precondition detail", rejected.RejectReason)
	}
	if rejected.Payload != "title-from-b" {
		t.Fatalf("rejected payload overwritten: %q", rejected.Payload)
	}
	if follow, _ := rb.Status("b-follow"); follow.Status != StatusBlocked {
		t.Fatalf("b-follow status = %#v, want blocked", follow)
	}

	// Restart: rejected record, reason, and blocked chain survive recovery.
	rb, err = NewRuntime("client-b", mustStore(t, dir, "client-b"), service)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := rb.Status("b-edit"); got.Status != StatusRejected || got.RejectReason == "" {
		t.Fatalf("after restart b-edit = %#v", got)
	}
	if got, _ := rb.Status("b-follow"); got.Status != StatusBlocked {
		t.Fatalf("after restart b-follow = %#v", got)
	}

	// User revises with a new identity based on the current server version.
	revision, err := rb.Revise("b-edit", "b-edit-v2", "title-from-b-rebased", service.Version("doc-1"))
	if err != nil {
		t.Fatal(err)
	}
	if revision.Revises != "b-edit" || revision.ID != "b-edit-v2" {
		t.Fatalf("revision provenance = %#v", revision)
	}
	if got, _ := rb.Status("b-edit"); got.Status != StatusRejected || got.Payload != "title-from-b" {
		t.Fatalf("original op changed after revise: %#v", got)
	}

	// Successor must not follow the revision without explicit confirmation.
	if err := rb.Flush(); err != nil {
		t.Fatal(err)
	}
	if got, _ := rb.Status("b-follow"); got.Status != StatusBlocked {
		t.Fatalf("b-follow auto-unblocked: %#v", got)
	}
	if err := rb.ConfirmSuccessor("b-follow", "b-edit-v2"); err != nil {
		t.Fatal(err)
	}
	if err := rb.Flush(); err != nil {
		t.Fatal(err)
	}
	if got, _ := rb.Status("b-follow"); got.Status != StatusAcknowledged {
		t.Fatalf("b-follow after confirm = %#v", got)
	}

	effects := service.EffectsSnapshot()
	if len(effects) != 3 {
		t.Fatalf("effects = %d, want 3", len(effects))
	}
	if effects[1].ID != "b-edit-v2" || effects[1].Revises != "b-edit" {
		t.Fatalf("revision effect = %#v", effects[1])
	}
}

func TestAbandonRejectedKeepsRecord(t *testing.T) {
	dir := t.TempDir()
	service := NewMockService()
	rt, err := NewRuntime("client-c", mustStore(t, dir, "client-c"), service)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := rt.AddOp(Operation{ID: "c1", Kind: "demo", Payload: "keep-me", ObjectID: "doc-9", BaseVersion: 3}); err != nil {
		t.Fatal(err)
	}
	if err := rt.Flush(); err != nil {
		t.Fatal(err)
	}
	if got, _ := rt.Status("c1"); got.Status != StatusRejected {
		t.Fatalf("c1 = %#v, want rejected", got)
	}
	if err := rt.Abandon("c1"); err != nil {
		t.Fatal(err)
	}
	got, _ := rt.Status("c1")
	if got.Status != StatusCanceled || got.Payload != "keep-me" || got.RejectReason == "" {
		t.Fatalf("abandoned op lost intent or reason: %#v", got)
	}
	if _, err := rt.Revise("c1", "c1-v2", "x", 0); !errors.Is(err, ErrNotRejected) {
		t.Fatalf("revise abandoned = %v, want ErrNotRejected", err)
	}
}

func TestUndoCommittedIsIndependentOperation(t *testing.T) {
	dir := t.TempDir()
	service := NewMockService()
	rt, err := NewRuntime("client-u", mustStore(t, dir, "client-u"), service)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := rt.AddOp(Operation{ID: "u1", Kind: "rename", Payload: "new-name", ObjectID: "doc-7"}); err != nil {
		t.Fatal(err)
	}
	if err := rt.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := rt.RequestSubmittedCancel("u1"); !errors.Is(err, ErrAlreadyCommitted) {
		t.Fatalf("cancel committed = %v, want ErrAlreadyCommitted", err)
	}

	undo, err := rt.UndoCommitted("u1", Operation{ID: "u1-undo", Payload: "old-name", BaseVersion: service.Version("doc-7")})
	if err != nil {
		t.Fatal(err)
	}
	if undo.UndoOf != "u1" || undo.Kind != "undo:rename" || undo.ObjectID != "doc-7" {
		t.Fatalf("undo op = %#v", undo)
	}
	if err := rt.Flush(); err != nil {
		t.Fatal(err)
	}
	if got, _ := rt.Status("u1"); got.Status != StatusAcknowledged {
		t.Fatalf("original op changed by undo: %#v", got)
	}
	if got, _ := rt.Status("u1-undo"); got.Status != StatusAcknowledged {
		t.Fatalf("undo status = %#v", got)
	}
	effects := service.EffectsSnapshot()
	if len(effects) != 2 || effects[1].ID != "u1-undo" || effects[1].UndoOf != "u1" {
		t.Fatalf("effects = %#v", effects)
	}
}

func TestUnresolvedDependenciesAndDependentsSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	service := NewMockService()
	service.SetOffline(true)
	rt, err := NewRuntime("client-a", mustStore(t, dir, "client-a"), service)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := rt.Add("p1", "demo", "p1", ""); err != nil {
		t.Fatal(err)
	}
	if _, _, err := rt.Add("p2", "demo", "p2", "p1"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := rt.Add("p3", "demo", "p3", "p2"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := rt.Add("solo", "demo", "solo", ""); err != nil {
		t.Fatal(err)
	}
	if err := rt.Flush(); !errors.Is(err, ErrRemoteUnavailable) {
		t.Fatal(err)
	}

	service.Reject["p1"] = "nope"
	service.SetOffline(false)
	if err := rt.Flush(); err != nil {
		t.Fatal(err)
	}

	dependents := rt.DependentsOf("p1")
	if len(dependents) != 2 || dependents[0].ID != "p2" || dependents[1].ID != "p3" {
		t.Fatalf("dependents = %#v", dependents)
	}

	review := rt.PendingReview()
	if len(review) != 3 {
		t.Fatalf("pending review = %#v, want p1,p2,p3", review)
	}
	if review[0].ID != "p1" || review[0].Status != StatusRejected {
		t.Fatalf("first review item = %#v", review[0])
	}

	unresolved := rt.UnresolvedDependencies()
	if len(unresolved) != 2 {
		t.Fatalf("unresolved = %#v, want p2,p3", unresolved)
	}
	if unresolved[0].OperationID != "p2" || unresolved[0].DependsOn != "p1" ||
		unresolved[0].DependencyStatus != StatusRejected || unresolved[0].Reason == "" {
		t.Fatalf("unresolved entry = %#v", unresolved[0])
	}

	restarted, err := NewRuntime("client-a", mustStore(t, dir, "client-a"), service)
	if err != nil {
		t.Fatal(err)
	}
	if got := restarted.UnresolvedDependencies(); len(got) != 2 {
		t.Fatalf("after restart unresolved = %#v", got)
	}
	if got := restarted.PendingReview(); len(got) != 3 {
		t.Fatalf("after restart review = %#v", got)
	}
	if got := restarted.DependentsOf("p1"); len(got) != 2 {
		t.Fatalf("after restart dependents = %#v", got)
	}
}

func TestSyncConvergesRegardlessOfOrderWithProvenance(t *testing.T) {
	dir := t.TempDir()
	service := NewMockService()
	service.SetOffline(true)

	ra, err := NewRuntime("client-a", mustStore(t, dir, "a"), service)
	if err != nil {
		t.Fatal(err)
	}
	rb, err := NewRuntime("client-b", mustStore(t, dir, "b"), service)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := ra.Add("op1", "demo", "a1", ""); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ra.Add("op2", "demo", "a2", "op1"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := rb.Add("op1", "demo", "b1", ""); err != nil {
		t.Fatal(err)
	}

	service.SetOffline(false)
	if err := ra.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := rb.Flush(); err != nil {
		t.Fatal(err)
	}

	snapA := ra.Snapshot()
	snapB := rb.Snapshot()
	if len(snapA) != 2 || snapA[0].ID != "client-a/op1" || snapA[0].OriginClient != "client-a" {
		t.Fatalf("snapshot a = %#v", snapA)
	}
	if snapA[1].DependsOn != "client-a/op1" {
		t.Fatalf("qualified dependency = %#v", snapA[1])
	}
	if snapB[0].ID != "client-b/op1" {
		t.Fatalf("snapshot b = %#v", snapB)
	}

	m1, err := NewRuntime("merge-ab", mustStore(t, dir, "m1"), service)
	if err != nil {
		t.Fatal(err)
	}
	if err := m1.Sync(snapA); err != nil {
		t.Fatal(err)
	}
	if err := m1.Sync(snapB); err != nil {
		t.Fatal(err)
	}
	m2, err := NewRuntime("merge-ba", mustStore(t, dir, "m2"), service)
	if err != nil {
		t.Fatal(err)
	}
	if err := m2.Sync(snapB); err != nil {
		t.Fatal(err)
	}
	if err := m2.Sync(snapA); err != nil {
		t.Fatal(err)
	}

	view := func(rt *Runtime) map[string]Status {
		out := map[string]Status{}
		for _, op := range rt.Snapshot() {
			out[op.ID] = op.Status
		}
		return out
	}
	v1, v2 := view(m1), view(m2)
	if len(v1) != 3 || len(v2) != 3 {
		t.Fatalf("merged views = %#v / %#v", v1, v2)
	}
	for id, status := range v1 {
		if v2[id] != status {
			t.Fatalf("diverged at %s: %s vs %s", id, status, v2[id])
		}
		if status != StatusAcknowledged {
			t.Fatalf("merged %s = %s", id, status)
		}
	}

	before := service.EffectCount()
	if err := m1.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := service.EffectCount(); got != before {
		t.Fatalf("foreign operations submitted: %d vs %d", before, got)
	}

	if err := m1.Sync(snapA); err != nil {
		t.Fatal(err)
	}
	if err := m1.Sync(snapB); err != nil {
		t.Fatal(err)
	}
	if got := len(m1.Snapshot()); got != 3 {
		t.Fatalf("re-sync changed state: %d entries", got)
	}
}

func TestSyncInAnyOrderResolvesForeignDependencies(t *testing.T) {
	dir := t.TempDir()
	service := NewMockService()
	ra, err := NewRuntime("client-a", mustStore(t, dir, "a2"), service)
	if err != nil {
		t.Fatal(err)
	}
	rb, err := NewRuntime("client-b", mustStore(t, dir, "b2"), service)
	if err != nil {
		t.Fatal(err)
	}
	service.SetOffline(true)
	if _, _, err := ra.Add("a-root", "demo", "root", ""); err != nil {
		t.Fatal(err)
	}
	if _, _, err := rb.AddOp(Operation{ID: "b-child", Kind: "demo", Payload: "child", DependsOn: "client-a/a-root"}); err != nil {
		t.Fatal(err)
	}
	service.SetOffline(false)
	if err := ra.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := rb.Flush(); err != nil {
		t.Fatal(err)
	}
	if got, _ := rb.Status("b-child"); got.Status != StatusBlocked {
		t.Fatalf("b-child waiting on foreign prerequisite = %#v", got)
	}

	m, err := NewRuntime("merge-x", mustStore(t, dir, "mx"), service)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Sync(rb.Snapshot()); err != nil {
		t.Fatal(err)
	}
	if got := m.UnresolvedDependencies(); len(got) != 1 {
		t.Fatalf("partial sync unresolved = %#v", got)
	}
	if err := m.Sync(ra.Snapshot()); err != nil {
		t.Fatal(err)
	}
	if got := m.UnresolvedDependencies(); len(got) != 0 {
		t.Fatalf("full sync unresolved = %#v, want none", got)
	}
}
