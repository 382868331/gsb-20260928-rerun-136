package offlinequeue

import (
	"path/filepath"
	"testing"
	"time"
)

const testTimeout = 3 * time.Second

func waitFor(t *testing.T, q *Queue, id string, want Status) {
	t.Helper()
	got, final := q.WaitFor(id, testTimeout)
	if !final || got != want {
		op, _ := q.store.Get(id)
		t.Fatalf("op %s: want status %s, got %s (final=%v, attempts=%d, err=%q)",
			id, want, got, final, op.Attempts, op.LastError)
	}
}

// 断网期间持续产生操作，恢复后按依赖顺序全部提交确认。
func TestOfflineEnqueueThenSubmitByDependency(t *testing.T) {
	srv := NewMockService()
	srv.SetOffline(true)
	store := mustStore(t, "client-a")
	q := New(store, srv, WithTick(5*time.Millisecond))
	defer q.Close()

	mustEnqueue(t, q, EnqueueInput{ID: "a1", Kind: "create", Payload: "x"})
	mustEnqueue(t, q, EnqueueInput{ID: "a2", Kind: "update", Payload: "y", DependsOn: []string{"a1"}})
	mustEnqueue(t, q, EnqueueInput{ID: "a3", Kind: "update", Payload: "z", DependsOn: []string{"a2"}})

	// 断网 150ms：操作必须全部保留为 pending，且服务端零生效。
	time.Sleep(150 * time.Millisecond)
	for _, id := range []string{"a1", "a2", "a3"} {
		if st, _ := q.Status(id); st != StatusPending {
			t.Fatalf("op %s should stay pending offline, got %s", id, st)
		}
	}
	if srv.ApplyCountOf("a1") != 0 {
		t.Fatalf("nothing should apply offline, got %d", srv.ApplyCountOf("a1"))
	}

	srv.SetOffline(false)
	waitFor(t, q, "a3", StatusConfirmed)
	if srv.ApplyCountOf("a1") != 1 || srv.ApplyCountOf("a2") != 1 || srv.ApplyCountOf("a3") != 1 {
		t.Fatalf("each op should apply exactly once: %+v", srv.ApplyCount)
	}
}

// 有前置依赖的操作不能抢跑：前置未确认前，后继一次都不能发往服务端。
func TestDependencyDoesNotJumpAhead(t *testing.T) {
	srv := NewMockService()
	store := mustStore(t, "client-dep")
	q := New(store, srv, WithTick(5*time.Millisecond))
	defer q.Close()

	// a1 生效后持续丢一段确认，制造"前置仍在途"的稳定窗口。
	srv.DelayAck["a1"] = 100
	mustEnqueue(t, q, EnqueueInput{ID: "a1", Kind: "create"})
	mustEnqueue(t, q, EnqueueInput{ID: "a2", Kind: "update", DependsOn: []string{"a1"}})

	time.Sleep(100 * time.Millisecond)
	if srv.Applied("a2") {
		t.Fatal("a2 must not be sent before a1 is confirmed")
	}
	waitFor(t, q, "a1", StatusConfirmed)
	waitFor(t, q, "a2", StatusConfirmed)
}

// 同一操作重复发送只确认一次：丢确认后重试，服务端只生效一次。
func TestDuplicateSendConfirmedOnce(t *testing.T) {
	srv := NewMockService()
	srv.DropAckFor["d1"] = true
	store := mustStore(t, "client-dup")
	q := New(store, srv, WithTick(5*time.Millisecond))
	defer q.Close()

	mustEnqueue(t, q, EnqueueInput{ID: "d1", Kind: "create"})
	waitFor(t, q, "d1", StatusConfirmed)
	if got := srv.ApplyCountOf("d1"); got != 1 {
		t.Fatalf("op d1 must apply exactly once despite retry, got %d", got)
	}
	op, _ := q.store.Get("d1")
	if op.Attempts < 2 {
		t.Fatalf("expected at least 2 attempts (send + retry), got %d", op.Attempts)
	}
}

// 服务拒绝后，后继操作保留为 blocked。
func TestRejectBlocksSuccessors(t *testing.T) {
	srv := NewMockService()
	srv.Reject["r1"] = true
	store := mustStore(t, "client-reject")
	q := New(store, srv, WithTick(5*time.Millisecond))
	defer q.Close()

	mustEnqueue(t, q, EnqueueInput{ID: "r1", Kind: "create"})
	mustEnqueue(t, q, EnqueueInput{ID: "r2", Kind: "update", DependsOn: []string{"r1"}})
	mustEnqueue(t, q, EnqueueInput{ID: "r3", Kind: "update", DependsOn: []string{"r2"}})
	waitFor(t, q, "r1", StatusRejected)
	waitFor(t, q, "r2", StatusBlocked)
	waitFor(t, q, "r3", StatusBlocked)
	if srv.Applied("r2") || srv.Applied("r3") {
		t.Fatal("successors of rejected op must never be sent")
	}
}

// 取消尚未发送：本地撤销，永不联系服务端。
func TestCancelBeforeSend(t *testing.T) {
	srv := NewMockService()
	srv.SetOffline(true)
	store := mustStore(t, "client-cancel1")
	q := New(store, srv, WithTick(5*time.Millisecond))
	defer q.Close()

	mustEnqueue(t, q, EnqueueInput{ID: "c1", Kind: "create"})
	time.Sleep(50 * time.Millisecond)
	if err := q.Cancel("c1"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, q, "c1", StatusCancelled)
	srv.SetOffline(false)
	time.Sleep(100 * time.Millisecond)
	if srv.Applied("c1") {
		t.Fatal("cancelled-before-send op must never reach service")
	}
}

// 取消已经提交：走服务端取消路径，与未发送取消是两个状态。
func TestCancelAfterSubmit(t *testing.T) {
	srv := NewMockService()
	store := mustStore(t, "client-cancel2")
	q := New(store, srv, WithTick(5*time.Millisecond))
	defer q.Close()

	mustEnqueue(t, q, EnqueueInput{ID: "c2", Kind: "create"})
	waitFor(t, q, "c2", StatusConfirmed)
	if err := q.Cancel("c2"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, q, "c2", StatusCancelledRemote)
	if !srv.Cancelled("c2") || srv.CancelCountOf("c2") != 1 {
		t.Fatalf("remote cancel expected once, count=%d", srv.CancelCountOf("c2"))
	}

	// 重复取消已取消的操作应当被拒绝。
	if err := q.Cancel("c2"); err == nil {
		t.Fatal("cancelling a final op must error")
	}
}

// 已提交操作的取消在断网时保留意图，恢复后完成取消。
func TestCancelSubmittedWhileOfflineRetries(t *testing.T) {
	srv := NewMockService()
	store := mustStore(t, "client-cancel3")
	q := New(store, srv, WithTick(5*time.Millisecond))
	defer q.Close()

	mustEnqueue(t, q, EnqueueInput{ID: "c3", Kind: "create"})
	waitFor(t, q, "c3", StatusConfirmed)
	srv.SetOffline(true)
	if err := q.Cancel("c3"); err == nil {
		t.Fatal("cancel should fail while offline and stay pending")
	}
	time.Sleep(60 * time.Millisecond)
	if st, _ := q.Status("c3"); st != StatusCancelPending {
		t.Fatalf("want cancel_pending offline, got %s", st)
	}
	srv.SetOffline(false)
	waitFor(t, q, "c3", StatusCancelledRemote)
}

// 保存重启：丢确认后关闭进程再重开，重放仍只生效一次。
func TestRestartNoDuplicateEffect(t *testing.T) {
	path := filepath.Join(t.TempDir(), "client-restart.json")
	srv := NewMockService()
	srv.DropAckFor["k1"] = true

	store := openStore(t, path)
	q := New(store, srv, WithTick(5*time.Millisecond))
	if err := q.Enqueue(EnqueueInput{ID: "k1", Kind: "create", Payload: "v"}); err != nil {
		t.Fatal(err)
	}
	// 等到服务端已生效但确认丢失（本地仍在重试）。
	deadline := time.Now().Add(testTimeout)
	for time.Now().Before(deadline) && srv.ApplyCountOf("k1") == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if srv.ApplyCountOf("k1") != 1 {
		t.Fatal("service should have applied k1 once before restart")
	}
	q.Close()

	// 模拟进程重启：重新打开同一状态文件，服务仍是同一个模拟实例。
	store2 := openStore(t, path)
	q2 := New(store2, srv, WithTick(5*time.Millisecond))
	defer q2.Close()
	waitFor(t, q2, "k1", StatusConfirmed)
	if got := srv.ApplyCountOf("k1"); got != 1 {
		t.Fatalf("after restart op must still apply exactly once, got %d", got)
	}
	op, _ := q2.store.Get("k1")
	if op.Status != StatusConfirmed {
		t.Fatalf("restarted op should reconcile to confirmed, got %s", op.Status)
	}
}

// 不同客户端复用同一套状态结构，但各自独立文件，互不影响。
func TestMultipleClientsIndependentState(t *testing.T) {
	srv := NewMockService()
	storeA := mustStoreIn(t, filepath.Join(t.TempDir(), "a.json"))
	storeB := mustStoreIn(t, filepath.Join(t.TempDir(), "b.json"))
	qA := New(storeA, srv, WithTick(5*time.Millisecond))
	qB := New(storeB, srv, WithTick(5*time.Millisecond))
	defer qA.Close()
	defer qB.Close()

	mustEnqueue(t, qA, EnqueueInput{ID: "shared-id", Kind: "create", Payload: "A"})
	mustEnqueue(t, qB, EnqueueInput{ID: "shared-id", Kind: "create", Payload: "B"})
	waitFor(t, qA, "shared-id", StatusConfirmed)
	waitFor(t, qB, "shared-id", StatusConfirmed)
	if len(qA.Ops()) != 1 || len(qB.Ops()) != 1 {
		t.Fatal("each client keeps its own queue")
	}
}

// 同一客户端重复入队同一 ID 只保留一条记录。
func TestDuplicateEnqueueRejected(t *testing.T) {
	srv := NewMockService()
	store := mustStore(t, "client-dup-enq")
	q := New(store, srv, WithTick(5*time.Millisecond))
	defer q.Close()
	mustEnqueue(t, q, EnqueueInput{ID: "u1", Kind: "create"})
	if err := q.Enqueue(EnqueueInput{ID: "u1", Kind: "create"}); err == nil {
		t.Fatal("duplicate enqueue must return error")
	}
	if len(q.Ops()) != 1 {
		t.Fatalf("duplicate op must not create a second record, got %d", len(q.Ops()))
	}
}

func openStore(t *testing.T, path string) *Store {
	t.Helper()
	s, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func mustStore(t *testing.T, name string) *Store {
	t.Helper()
	return mustStoreIn(t, filepath.Join(t.TempDir(), name+".json"))
}

func mustStoreIn(t *testing.T, path string) *Store {
	t.Helper()
	return openStore(t, path)
}

func mustEnqueue(t *testing.T, q *Queue, in EnqueueInput) {
	t.Helper()
	if err := q.Enqueue(in); err != nil {
		t.Fatal(err)
	}
}
