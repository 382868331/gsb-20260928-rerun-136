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

func TestUnresolvedDependenciesVisibleAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	service := NewMockService()
	rt, err := NewRuntime("client-r", mustStore(t, dir, "client-r"), service)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := rt.Add("r1", "demo", "root", ""); err != nil {
		t.Fatal(err)
	}
	if _, _, err := rt.Add("r2", "demo", "child", "r1"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := rt.Add("r3", "demo", "grandchild", "r2"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := rt.Add("r4", "demo", "unrelated", ""); err != nil {
		t.Fatal(err)
	}

	service.Reject["r1"] = "manual review failed"
	if err := rt.Flush(); err != nil {
		t.Fatal(err)
	}

	unresolved := rt.Unresolved()
	if len(unresolved) != 2 || unresolved[0].ID != "r2" || unresolved[1].ID != "r3" {
		t.Fatalf("unresolved = %#v, want r2 and r3", unresolved)
	}
	if unresolved[0].DependsOn != "r1" || unresolved[0].RejectReason == "" {
		t.Fatalf("unresolved entry lost dependency context: %#v", unresolved[0])
	}
	if got, _ := rt.Status("r4"); got.Status != StatusAcknowledged {
		t.Fatalf("unrelated op affected: %#v", got)
	}

	// Restart: the same unresolved list is recovered from saved state.
	rt, err = NewRuntime("client-r", mustStore(t, dir, "client-r"), service)
	if err != nil {
		t.Fatal(err)
	}
	if got := rt.Unresolved(); len(got) != 2 || got[0].ID != "r2" || got[1].ID != "r3" {
		t.Fatalf("unresolved after restart = %#v", got)
	}

	// Continue from saved state: revise the root, re-confirm the chain.
	if _, err := rt.Revise("r1", "r1-v2", "root-fixed", 0); err != nil {
		t.Fatal(err)
	}
	if err := rt.ConfirmSuccessor("r2", "r1-v2"); err != nil {
		t.Fatal(err)
	}
	if err := rt.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := rt.Unresolved(); len(got) != 1 || got[0].ID != "r3" {
		t.Fatalf("unresolved after partial fix = %#v, want r3", got)
	}
	if err := rt.ConfirmSuccessor("r3", "r2"); err != nil {
		t.Fatal(err)
	}
	if err := rt.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := rt.Unresolved(); len(got) != 0 {
		t.Fatalf("unresolved after full fix = %#v, want none", got)
	}
	if got, _ := rt.Status("r3"); got.Status != StatusAcknowledged {
		t.Fatalf("r3 = %#v, want acknowledged", got)
	}
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
