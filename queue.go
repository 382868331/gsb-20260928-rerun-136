package offlinequeue

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// Queue 是一个客户端本地的离线操作队列。
//
// 设计要点：
//   - 断网期间 Enqueue 持续可用，操作累积为 pending；
//   - 网络恢复后由后台 pump 按入队顺序与依赖关系提交；
//   - 同一操作靠稳定的 opID 在服务端幂等，重发只确认一次；
//   - 前置依赖被拒绝/取消时，后继保留为 blocked；
//   - 取消区分"尚未发送"（本地撤销）与"已经提交"（发取消请求）。
type Queue struct {
	store   *Store
	service Service
	tick    time.Duration

	stop chan struct{}
	done chan struct{}
	wg   sync.WaitGroup
}

// Option 配置 Queue。
type Option func(*Queue)

// WithTick 设置 pump 的轮询间隔，默认为 20ms。
func WithTick(d time.Duration) Option {
	return func(q *Queue) { q.tick = d }
}

// New 创建队列并启动后台提交 pump。
func New(store *Store, service Service, opts ...Option) *Queue {
	q := &Queue{store: store, service: service, tick: 20 * time.Millisecond,
		stop: make(chan struct{}), done: make(chan struct{})}
	for _, o := range opts {
		o(q)
	}
	q.wg.Add(1)
	go q.pump()
	return q
}

// Close 停止后台 pump。
func (q *Queue) Close() {
	close(q.stop)
	q.wg.Wait()
}

// EnqueueInput 描述一条新操作。
type EnqueueInput struct {
	ID        string
	Kind      string
	Payload   string
	DependsOn []string
}

// Enqueue 产生一条操作。重复 ID 返回错误且不会产生第二条记录，
// 这是"同一操作只确认一次"在入口处的保证。
func (q *Queue) Enqueue(in EnqueueInput) error {
	if in.ID == "" {
		return errors.New("op id is required")
	}
	now := time.Now()
	op := &Op{
		ID: in.ID, Kind: in.Kind, Payload: in.Payload,
		DependsOn: append([]string(nil), in.DependsOn...),
		Status:    StatusPending, CreatedAt: now, UpdatedAt: now,
	}
	unlock := q.store.Lock()
	defer unlock()
	for _, dep := range op.DependsOn {
		if _, ok := q.store.ops[dep]; !ok {
			return fmt.Errorf("op %q depends on unknown op %q", in.ID, dep)
		}
	}
	return q.store.add(op)
}

// Status 查询操作当前状态。
func (q *Queue) Status(id string) (Status, bool) {
	op, ok := q.store.Get(id)
	return op.Status, ok
}

// Ops 返回按入队顺序排列的全部操作。
func (q *Queue) Ops() []Op { return q.store.List() }

// Cancel 取消操作，分两条路径：
//
//   - 尚未发送（pending）：标记为 cancelled 并立即落盘，永不联系服务端；
//   - 在途（sending）/已确认（confirmed）：说明可能已经提交，
//     走"取消已提交"路径，向服务端发 Cancel，状态经 cancel_pending
//     最终到 cancelled_remote。
//
// 已经终结（blocked/rejected/cancelled*）的取消请求返回错误。
func (q *Queue) Cancel(id string) error {
	unlock := q.store.Lock()
	op, ok := q.store.ops[id]
	if !ok {
		unlock()
		return fmt.Errorf("op %q not found", id)
	}
	switch op.Status {
	case StatusPending:
		op.Status = StatusCancelled
		op.touch(time.Now())
		err := q.store.saveLocked()
		unlock()
		return err
	case StatusSending:
		op.Status = StatusCancelPending
		op.touch(time.Now())
		err := q.store.saveLocked()
		unlock()
		return err
	case StatusConfirmed:
		op.Status = StatusCancelPending
		op.touch(time.Now())
		err := q.store.saveLocked()
		unlock()
		if err != nil {
			return err
		}
		return q.sendCancel(id)
	case StatusCancelPending:
		unlock()
		return q.sendCancel(id)
	default:
		unlock()
		return fmt.Errorf("op %q in status %s cannot be cancelled", id, op.Status)
	}
}

// sendCancel 对已提交操作调用服务端取消；失败保留 cancel_pending 等重试。
func (q *Queue) sendCancel(id string) error {
	if err := q.service.Cancel(id); err != nil {
		if errors.Is(err, ErrRejected) {
			// 服务端从未生效（提交在途时取消且并未到达），
			// 无需远程取消，按本地撤销落定。
			unlock := q.store.Lock()
			defer unlock()
			_, err = q.store.update(id, func(o *Op) {
				o.Status = StatusCancelled
				o.touch(time.Now())
			})
			return err
		}
		return err
	}
	unlock := q.store.Lock()
	defer unlock()
	_, err := q.store.update(id, func(o *Op) {
		o.Status = StatusCancelledRemote
		o.touch(time.Now())
	})
	return err
}

// pump 按固定节拍推进所有可推进的操作。
func (q *Queue) pump() {
	defer q.wg.Done()
	t := time.NewTicker(q.tick)
	defer t.Stop()
	for {
		select {
		case <-q.stop:
			return
		case <-t.C:
			q.pass()
		}
	}
}

// pass 执行一轮调度。每轮持锁只做状态读取与轻量更新，
// 网络调用在锁外进行，避免慢请求阻塞 Enqueue/Cancel。
func (q *Queue) pass() {
	// 先处理依赖传播：被拒绝/取消的前置使其后继受阻保留。
	q.propagateBlocked()
	q.processCancels()

	ids := q.readyPendingIDs()
	for _, id := range ids {
		q.submitOne(id)
	}
}

// dependencySettled 判断依赖是否已允许提交：
// confirmed 与 cancelled_remote 都表示前置已在服务端落定。
func dependencySettled(status Status) bool {
	return status == StatusConfirmed || status == StatusCancelledRemote
}

// dependencyFailed 判断前置是否走到了后继必须受阻的状态。
func dependencyFailed(status Status) bool {
	switch status {
	case StatusRejected, StatusBlocked, StatusCancelled:
		return true
	}
	return false
}

// propagateBlocked 把"前置失败"传播给后继操作。
func (q *Queue) propagateBlocked() {
	unlock := q.store.Lock()
	defer unlock()
	changed := false
	for _, id := range q.store.order {
		op := q.store.ops[id]
		if op.Status != StatusPending {
			continue
		}
		for _, dep := range op.DependsOn {
			if dependencyFailed(q.store.ops[dep].Status) {
				op.Status = StatusBlocked
				op.LastError = "blocked by failed dependency " + dep
				op.touch(time.Now())
				changed = true
				break
			}
		}
	}
	if changed {
		_ = q.store.saveLocked()
	}
}

// readyPendingIDs 返回当前可以发送的 pending 操作：
// 自身 pending，且每个前置都已在服务端落定（不能抢跑）。
func (q *Queue) readyPendingIDs() []string {
	unlock := q.store.Lock()
	defer unlock()
	var ids []string
	for _, id := range q.store.order {
		op := q.store.ops[id]
		if op.Status != StatusPending {
			continue
		}
		ready := true
		for _, dep := range op.DependsOn {
			if !dependencySettled(q.store.ops[dep].Status) {
				ready = false
				break
			}
		}
		if ready {
			ids = append(ids, id)
		}
	}
	return ids
}

// submitOne 发送一个操作并根据结果更新确认记录。
func (q *Queue) submitOne(id string) {
	unlock := q.store.Lock()
	op, ok := q.store.ops[id]
	if !ok || op.Status != StatusPending {
		unlock()
		return
	}
	op.Status = StatusSending
	op.Attempts++
	op.touch(time.Now())
	kind, payload := op.Kind, op.Payload
	if err := q.store.saveLocked(); err != nil {
		unlock()
		return
	}
	unlock()

	// 锁外调用服务，断网时这里只表现为错误，不影响本地继续入队。
	res, err := q.service.Submit(id, kind, payload)

	unlock = q.store.Lock()
	defer unlock()
	cur, ok := q.store.ops[id]
	if !ok {
		return
	}
	now := time.Now()
	switch {
	case err == nil:
		if cur.Status == StatusCancelPending {
			// 在途期间被取消：已提交，保留 cancel_pending 由 pump 发取消请求。
			cur.RemoteKey = res.RemoteKey
			cur.LastError = ""
			cur.touch(now)
		} else {
			cur.Status = StatusConfirmed
			cur.RemoteKey = res.RemoteKey
			cur.LastError = ""
			cur.touch(now)
		}
	case errors.Is(err, ErrRejected):
		if cur.Status == StatusCancelPending {
			cur.Status = StatusCancelled
		} else {
			cur.Status = StatusRejected
		}
		cur.LastError = err.Error()
		cur.touch(now)
	default:
		if cur.Status == StatusCancelPending {
			switch {
			case errors.Is(err, ErrAckLost):
				// 提交可能已生效，保留取消意图，pump 会发 Cancel。
			default:
				// 断网且未生效：无需远程取消，本地撤销即可。
				cur.Status = StatusCancelled
			}
			cur.LastError = err.Error()
			cur.touch(now)
		} else {
			// ErrUnreachable / ErrAckLost：结果未知或未生效，回退 pending 等下轮重试。
			// 丢确认时服务端其实已生效，重试靠服务端幂等，不会二次生效。
			cur.Status = StatusPending
			cur.LastError = err.Error()
			cur.touch(now)
		}
	}
	_ = q.store.saveLocked()
}

// processCancels 重试所有 cancel_pending 的取消请求。
func (q *Queue) processCancels() {
	unlock := q.store.Lock()
	var pending []string
	for _, id := range q.store.order {
		if q.store.ops[id].Status == StatusCancelPending {
			pending = append(pending, id)
		}
	}
	unlock()
	for _, id := range pending {
		_ = q.sendCancel(id)
	}
}

// WaitFor 阻塞等待某操作进入终结状态或超时；主要给测试/demo 使用。
func (q *Queue) WaitFor(id string, timeout time.Duration) (Status, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if op, ok := q.store.Get(id); ok && op.Status.IsFinal() {
			return op.Status, true
		}
		time.Sleep(q.tick)
	}
	op, ok := q.store.Get(id)
	return op.Status, ok && op.Status.IsFinal()
}
