package offlinequeue

import (
	"errors"
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

func TestTwoClientsStalePreconditionReviseRebaseAndRestart(t *testing.T) {
	dir := t.TempDir()
	service := NewMockService()
	service.SeedObject("doc", 3)
	service.SetOffline(true)

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

	// Both clients compose offline edits against the version they last saw.
	if _, _, err := ra.AddWithPrecondition("a-edit", "edit", "a-title", "doc", 3, ""); err != nil {
		t.Fatal(err)
	}
	if _, _, err := rb.AddWithPrecondition("b-edit", "edit", "b-title", "doc", 3, ""); err != nil {
		t.Fatal(err)
	}
	// A follow-up on client-b assumes b-edit lands first; it must not be sent
	// before that prerequisite is acknowledged.
	if _, _, err := rb.AddWithPrecondition("b-follow", "edit", "b-body", "doc", 4, "b-edit"); err != nil {
		t.Fatal(err)
	}

	// Both flush while sharing the same mock server; the order across separate
	// runtimes is driven explicitly by the test.
	service.SetOffline(false)
	if err := ra.Flush(); err != nil {
		t.Fatalf("client-a flush: %v", err)
	}
	if got := service.ObjectVersion("doc"); got != 4 {
		t.Fatalf("doc version after client-a = %d, want 4", got)
	}
	if err := rb.Flush(); err != nil {
		t.Fatalf("client-b flush: %v", err)
	}

	conflicted, ok := rb.Status("b-edit")
	if !ok || conflicted.Status != StatusConflict {
		t.Fatalf("b-edit = %#v, want conflict", conflicted)
	}
	if conflicted.RejectReason == "" || conflicted.Payload != "b-title" {
		t.Fatalf("conflicted record lost intent or reason: %#v", conflicted)
	}
	follow, ok := rb.Status("b-follow")
	if !ok || follow.Status != StatusBlocked {
		t.Fatalf("b-follow = %#v, want blocked", follow)
	}

	// The dependent must not silently adopt a replacement prerequisite.
	if err := rb.Flush(); err != nil {
		t.Fatalf("flush while follow blocked: %v", err)
	}
	if got, _ := rb.Status("b-follow"); got.Status != StatusBlocked {
		t.Fatalf("b-follow auto-sent as %s", got.Status)
	}

	// User revises against the new server version: new identity, old record
	// preserved, lineage recorded.
	if _, err := rb.Revise("b-edit-v2", "edit", "b-title-rebased", "doc", service.ObjectVersion("doc"), "b-edit"); err != nil {
		t.Fatalf("revise: %v", err)
	}
	revised, _ := rb.Status("b-edit-v2")
	if revised.RevisionOf != "b-edit" || revised.OriginID != "b-edit" || revised.ID == "b-edit" {
		t.Fatalf("revision lineage wrong: %#v", revised)
	}
	original, _ := rb.Status("b-edit")
	if original.Status != StatusConflict || original.Payload != "b-title" {
		t.Fatalf("original record overwritten: %#v", original)
	}
	if err := rb.Flush(); err != nil {
		t.Fatalf("flush revision: %v", err)
	}
	if got, _ := rb.Status("b-edit-v2"); got.Status != StatusAcknowledged {
		t.Fatalf("revision status = %s", got.Status)
	}

	// Explicit confirmation re-points the dependent at the new prerequisite.
	if err := rb.RebaseDependent("b-follow", "b-edit-v2", service.ObjectVersion("doc")); err != nil {
		t.Fatalf("rebase: %v", err)
	}
	if _, err := rb.Revise("should-not-exist", "x", "x", "doc", 5, "b-edit-v2"); !errors.Is(err, ErrNotRevisable) {
		t.Fatalf("revise acknowledged = %v, want ErrNotRevisable", err)
	}
	if err := rb.Flush(); err != nil {
		t.Fatalf("flush rebased follow-up: %v", err)
	}
	if got, _ := rb.Status("b-follow"); got.Status != StatusAcknowledged {
		t.Fatalf("b-follow after rebase = %s", got.Status)
	}

	effects := service.EffectsSnapshot()
	ids := make([]string, 0, len(effects))
	for _, e := range effects {
		ids = append(ids, e.ID)
	}
	wantIDs := map[string]bool{"a-edit": true, "b-edit-v2": true, "b-follow": true}
	if len(ids) != 3 {
		t.Fatalf("server effects = %v, want 3 unique effects", ids)
	}
	for _, id := range ids {
		if !wantIDs[id] {
			t.Fatalf("unexpected server effect %q", id)
		}
	}

	// Restart recovery re-reads the dependency chain from disk.
	restarted, err := NewRuntime("client-b", mustStore(t, dir, "client-b"), service)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Flush(); err != nil {
		t.Fatalf("restart flush: %v", err)
	}
	if service.EffectCount() != 3 {
		t.Fatalf("effects after restart = %d, want 3", service.EffectCount())
	}
	if got, _ := restarted.Status("b-edit"); got.Status != StatusConflict {
		t.Fatalf("conflicted history lost across restart: %#v", got)
	}
	if got, _ := restarted.Status("b-edit-v2"); got.RevisionOf != "b-edit" {
		t.Fatalf("revision source lost across restart: %#v", got)
	}
}

func TestAbandonKeepsRecordAndBlocksDependents(t *testing.T) {
	dir := t.TempDir()
	service := NewMockService()
	service.SeedObject("doc", 9)
	rt, err := NewRuntime("client-z", mustStore(t, dir, "client-z"), service)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := rt.AddWithPrecondition("z1", "edit", "v1", "doc", 1, ""); err != nil {
		t.Fatal(err)
	}
	if _, _, err := rt.Add("z2", "edit", "child", "z1"); err != nil {
		t.Fatal(err)
	}
	if err := rt.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := rt.Abandon("z1"); err != nil {
		t.Fatalf("abandon: %v", err)
	}
	if got, _ := rt.Status("z1"); got.Status != StatusAbandoned || got.Payload != "v1" || got.RejectReason == "" {
		t.Fatalf("abandoned record lost intent/reason: %#v", got)
	}
	if got, _ := rt.Status("z2"); got.Status != StatusBlocked {
		t.Fatalf("z2 = %s, want blocked", got.Status)
	}
}

func TestCommittedUndoIsIndependentIdempotentOperation(t *testing.T) {
	dir := t.TempDir()
	service := NewMockService()
	rt, err := NewRuntime("client-u", mustStore(t, dir, "client-u"), service)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := rt.AddWithPrecondition("u1", "edit", "data", "doc", 0, ""); err != nil {
		t.Fatal(err)
	}
	if err := rt.Flush(); err != nil {
		t.Fatal(err)
	}

	// Cancel of an already-committed edit stays denied; reversal is an undo.
	if err := rt.RequestSubmittedCancel("u1"); !errors.Is(err, ErrAlreadyCommitted) {
		t.Fatalf("cancel committed = %v, want ErrAlreadyCommitted", err)
	}
	undo, err := rt.Undo("u1-undo", "undo", "u1")
	if err != nil {
		t.Fatalf("undo create: %v", err)
	}
	if undo.UndoOf != "u1" || undo.ID == "u1" {
		t.Fatalf("undo not independent: %#v", undo)
	}

	// The confirmation for the undo is lost once; resend applies only once.
	service.DropAcks["u1-undo"] = 1
	if err := rt.Flush(); err != nil {
		t.Fatalf("undo flush: %v", err)
	}
	if got, _ := rt.Status("u1-undo"); got.Status != StatusAcknowledged {
		t.Fatalf("undo status = %s", got.Status)
	}
	if !service.IsUndone("u1") {
		t.Fatal("undo was not recorded server-side")
	}
	if got := service.EffectCount(); got != 2 {
		t.Fatalf("effects = %d, want edit+undo (2)", got)
	}

	// A second undo of the same target is rejected, and a restart adds nothing.
	if _, err := rt.Undo("u1-undo-2", "undo", "u1"); err != nil {
		t.Fatalf("second undo create: %v", err)
	}
	if err := rt.Flush(); err != nil {
		t.Fatalf("second undo flush: %v", err)
	}
	if got, _ := rt.Status("u1-undo-2"); got.Status != StatusRejected {
		t.Fatalf("second undo status = %s, want rejected", got.Status)
	}
	restarted, err := NewRuntime("client-u", mustStore(t, dir, "client-u"), service)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Flush(); err != nil {
		t.Fatalf("restart flush: %v", err)
	}
	if service.EffectCount() != 2 {
		t.Fatalf("effects after restart = %d, want 2", service.EffectCount())
	}
}
