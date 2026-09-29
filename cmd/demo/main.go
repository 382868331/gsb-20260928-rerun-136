// 演示：断网产生操作 → 恢复后按依赖提交 → 丢确认重试不重复生效
// → 拒绝使后继受阻 → 两种取消路径 → 重启后状态恢复。
//
// 运行：go run ./cmd/demo
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"offlinequeue"
)

func main() {
	dir, err := os.MkdirTemp("", "offlinequeue-demo-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	srv := offlinequeue.NewMockService()
	srv.SetOffline(true) // 应用启动时处于断网状态

	store, err := offlinequeue.OpenStore(filepath.Join(dir, "client-demo.json"))
	if err != nil {
		panic(err)
	}
	q := offlinequeue.New(store, srv, offlinequeue.WithTick(10*time.Millisecond))

	must := func(err error) {
		if err != nil {
			panic(err)
		}
	}

	// 1) 断网期间继续产生操作，且有依赖链。
	must(q.Enqueue(offlinequeue.EnqueueInput{ID: "op-1", Kind: "doc.create", Payload: `{"title":"x"}`}))
	must(q.Enqueue(offlinequeue.EnqueueInput{ID: "op-2", Kind: "doc.edit", Payload: `{"v":2}`, DependsOn: []string{"op-1"}}))

	// 2) 一条会丢确认的操作，验证重发只生效一次。
	srv.DropAckFor["op-3"] = true
	must(q.Enqueue(offlinequeue.EnqueueInput{ID: "op-3", Kind: "doc.tag", DependsOn: []string{"op-2"}}))

	// 3) 一条会被服务拒绝的操作及其后继。
	srv.Reject["op-4"] = true
	must(q.Enqueue(offlinequeue.EnqueueInput{ID: "op-4", Kind: "doc.bad"}))
	must(q.Enqueue(offlinequeue.EnqueueInput{ID: "op-5", Kind: "doc.after", DependsOn: []string{"op-4"}}))

	// 4) 断网时取消尚未发送的操作。
	must(q.Enqueue(offlinequeue.EnqueueInput{ID: "op-6", Kind: "doc.cancel-before"}))
	time.Sleep(50 * time.Millisecond)
	must(q.Cancel("op-6"))

	fmt.Println("== 断网 300ms 后恢复 ==")
	time.Sleep(300 * time.Millisecond)
	srv.SetOffline(false)

	for _, id := range []string{"op-1", "op-2", "op-3", "op-4", "op-5"} {
		q.WaitFor(id, 2*time.Second)
	}

	// 5) 取消一条已经提交生效的操作（走服务端取消）。
	q.WaitFor("op-2", 2*time.Second)
	must(q.Cancel("op-2"))
	time.Sleep(100 * time.Millisecond)

	for _, op := range q.Ops() {
		fmt.Printf("%-5s %-18s attempts=%d status=%s\n", op.ID, op.Kind, op.Attempts, op.Status)
	}
	fmt.Printf("服务端生效次数: op-3=%d（丢确认重发仍只一次）\n", srv.ApplyCountOf("op-3"))
	fmt.Printf("服务端取消次数: op-2=%d\n", srv.CancelCountOf("op-2"))

	// 6) 模拟保存后重启：重新打开状态文件，终态保留，不会重放。
	q.Close()
	store2, err := offlinequeue.OpenStore(filepath.Join(dir, "client-demo.json"))
	if err != nil {
		panic(err)
	}
	q2 := offlinequeue.New(store2, srv, offlinequeue.WithTick(10*time.Millisecond))
	time.Sleep(200 * time.Millisecond)
	st, _ := q2.Status("op-3")
	fmt.Printf("重启后 op-3 状态=%s，服务端累计生效=%d\n", st, srv.ApplyCountOf("op-3"))
	q2.Close()
}
