package offlinequeue

import "time"

// Status 表示操作在本地生命周期中的状态。
type Status string

const (
	// StatusPending 已入队，尚未成功发送。
	StatusPending Status = "pending"
	// StatusSending 已经在途发送，结果（确认/失败）未知。
	StatusSending Status = "sending"
	// StatusConfirmed 服务端已确认生效；同一操作只会确认一次。
	StatusConfirmed Status = "confirmed"
	// StatusBlocked 前置依赖被拒绝/取消，本操作无法再提交。
	StatusBlocked Status = "blocked"
	// StatusRejected 服务端明确拒绝（永久失败，后继受阻）。
	StatusRejected Status = "rejected"
	// StatusCancelled 取消时尚未发送，直接本地撤销，不会联系服务端。
	StatusCancelled Status = "cancelled"
	// StatusCancelPending 已提交后请求取消，取消请求在途。
	StatusCancelPending Status = "cancel_pending"
	// StatusCancelledRemote 服务端已确认取消（已提交操作的取消路径）。
	StatusCancelledRemote Status = "cancelled_remote"
)

// IsFinal 表示状态已终结，队列调度不会再改变它。
func (s Status) IsFinal() bool {
	switch s {
	case StatusConfirmed, StatusBlocked, StatusRejected,
		StatusCancelled, StatusCancelledRemote:
		return true
	}
	return false
}

// Op 是一条本地操作及其确认记录。
type Op struct {
	ID        string    `json:"id"`
	Kind      string    `json:"kind"`
	Payload   string    `json:"payload"`
	DependsOn []string  `json:"depends_on"`
	Status    Status    `json:"status"`
	Attempts  int       `json:"attempts"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	RemoteKey int64     `json:"remote_key,omitempty"`
	LastError string    `json:"last_error,omitempty"`
}

func (o *Op) touch(now time.Time) {
	o.UpdatedAt = now
}
