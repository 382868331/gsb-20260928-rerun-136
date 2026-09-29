package offlinequeue

import (
	"errors"
	"sync"
)

var (
	// ErrUnreachable 模拟连接断开：请求根本没到服务端（或响应丢失）。
	ErrUnreachable = errors.New("service unreachable")
	// ErrAckLost 模拟丢确认：服务端已生效，但确认没有回到客户端。
	// 重试时服务端必须识别为同一操作，绝不重复生效。
	ErrAckLost = errors.New("ack lost")
	// ErrRejected 表示服务端永久拒绝该操作。
	ErrRejected = errors.New("operation rejected by service")
)

// SubmitResult 是提交成功（已生效）的确认。
type SubmitResult struct {
	RemoteKey int64
}

// Service 是操作提交通道的抽象，真实环境可替换为 HTTP/RPC 客户端。
type Service interface {
	// Submit 提交一个操作。opID 由客户端生成并在重试时保持不变，
	// 用于服务端幂等：同一 opID 只生效一次。
	Submit(opID, kind, payload string) (SubmitResult, error)
	// Cancel 取消一个已经提交生效的操作；opID 同样用于幂等。
	Cancel(opID string) error
}

// appliedEntry 记录服务端一次真正生效。
type appliedEntry struct {
	kind    string
	payload string
}

// MockService 是本地模拟服务。
//
// 它在内存中维护"已生效"表，从而可以验证：
//   - 断网（Offline）：提交返回 ErrUnreachable，服务端不生效；
//   - 丢确认（DropAck）：提交先生效再返回 ErrAckLost，重试不会二次生效；
//   - 拒绝（reject 集合）：返回永久错误，后继操作应受阻保留；
//   - 幂等：同一 opID 无论重发多少次，ApplyCount 恒为 1。
type MockService struct {
	mu sync.Mutex

	// Offline 为 true 时服务断开，Submit/Cancel 返回 ErrUnreachable。
	Offline bool
	// DropAckFor 中的 opID 第一次提交时生效但返回 ErrAckLost。
	DropAckFor map[string]bool
	// DelayAck 中的 opID 在已生效后的前 N 次重发继续返回 ErrAckLost，
	// 用来稳定模拟"前置长时间在途、后继不能抢跑"的窗口。
	DelayAck map[string]int
	// Reject 中的 opID 会被服务端永久拒绝。
	Reject map[string]bool

	applied   map[string]*appliedEntry
	cancelled map[string]bool
	rejected  map[string]bool
	// ApplyCount 记录每个 opID 真正执行业务的次数，用于断言无重复生效。
	ApplyCount map[string]int
	// CancelCount 记录取消业务的执行次数。
	CancelCount map[string]int
}

// NewMockService 创建空的模拟服务。
func NewMockService() *MockService {
	return &MockService{
		DropAckFor:  map[string]bool{},
		DelayAck:    map[string]int{},
		Reject:      map[string]bool{},
		applied:     map[string]*appliedEntry{},
		cancelled:   map[string]bool{},
		rejected:    map[string]bool{},
		ApplyCount:  map[string]int{},
		CancelCount: map[string]int{},
	}
}

// SetOffline 切换模拟服务的连通状态。
func (m *MockService) SetOffline(off bool) {
	m.mu.Lock()
	m.Offline = off
	m.mu.Unlock()
}

// Applied 返回某 opID 是否在服务端真正生效过。
func (m *MockService) Applied(opID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.applied[opID]
	return ok
}

// Cancelled 返回某 opID 是否在服务端真正取消过。
func (m *MockService) Cancelled(opID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cancelled[opID]
}

// ApplyCountOf 返回某 opID 真正生效的次数（并发安全）。
func (m *MockService) ApplyCountOf(opID string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ApplyCount[opID]
}

// CancelCountOf 返回某 opID 远程取消执行次数（并发安全）。
func (m *MockService) CancelCountOf(opID string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.CancelCount[opID]
}

// Submit 实现 Service。
func (m *MockService) Submit(opID, kind, payload string) (SubmitResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Offline {
		return SubmitResult{}, ErrUnreachable
	}
	// 幂等：已经生效过（含上次丢确认）的操作直接返回原结果，不再执行业务。
	if _, ok := m.applied[opID]; ok {
		if m.DropAckFor[opID] {
			// 丢确认只模拟第一次：首次生效但丢包，重试正常确认，
			// ApplyCount 恒为 1，证明重复发送没有重复生效。
			delete(m.DropAckFor, opID)
			return SubmitResult{}, ErrAckLost
		}
		if m.DelayAck[opID] > 0 {
			m.DelayAck[opID]--
			return SubmitResult{}, ErrAckLost
		}
		return SubmitResult{RemoteKey: int64(len(opID))}, nil
	}
	if m.Reject[opID] || m.rejected[opID] {
		m.rejected[opID] = true
		return SubmitResult{}, ErrRejected
	}
	m.applied[opID] = &appliedEntry{kind: kind, payload: payload}
	m.ApplyCount[opID]++
	if m.DropAckFor[opID] {
		delete(m.DropAckFor, opID)
		return SubmitResult{}, ErrAckLost
	}
	if m.DelayAck[opID] > 0 {
		return SubmitResult{}, ErrAckLost
	}
	return SubmitResult{RemoteKey: int64(len(opID))}, nil
}

// Cancel 实现 Service：对已经提交生效的操作执行取消。
func (m *MockService) Cancel(opID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Offline {
		return ErrUnreachable
	}
	if _, ok := m.applied[opID]; !ok {
		return ErrRejected
	}
	if !m.cancelled[opID] {
		m.cancelled[opID] = true
		m.CancelCount[opID]++
	}
	return nil
}
