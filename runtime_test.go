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
