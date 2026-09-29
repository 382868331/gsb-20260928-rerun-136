# 客户端离线操作运行库（Go）

纯 Go、无第三方依赖的本地离线操作队列。应用断网时继续产生操作，
网络恢复后按依赖顺序提交；本地持久化操作队列与确认记录，
进程重启后不会让同一操作在服务端重复生效。

## 能力与对应实现

- **断网继续产生操作**：Enqueue 只写本地状态，后台 pump 发不出去就保留 pending 重试（queue.go）。
- **按依赖提交、不能抢跑**：readyPendingIDs 要求每个前置都达到 confirmed / cancelled_remote 才发送后继。
- **同一操作只确认一次**：客户端使用稳定 opID，入队重复 ID 直接拒绝；发送重试时服务端以 opID 幂等，ApplyCount 恒为 1。
- **丢确认**：模拟服务支持 DropAckFor（首次生效丢确认）与 DelayAck（持续丢确认窗口），本地回退 pending 重试，服务端不二次生效。
- **拒绝后后继受阻**：前置变为 rejected 后，propagateBlocked 把后继保留为 blocked，不再发送。
- **取消尚未发送**：pending 直接变 cancelled，纯本地撤销，永不联系服务端。
- **取消已经提交**：sending/confirmed 先变 cancel_pending，再对服务端发 Cancel，最终 cancelled_remote；断网时保留取消意图并重试。
- **保存重启无重复生效**：状态以 JSON 快照原子落盘（临时文件 + rename），重启后 sending/pending 操作靠服务端幂等重放。
- **多客户端复用、不做文件同步**：不同客户端各自 OpenStore(自己的路径)，共用同一套状态结构与库代码，文件完全独立。

## 状态机

    pending --发送--> sending --确认--> confirmed --取消已提交--> cancel_pending --> cancelled_remote
       |                  |
       |                  +-- 断网/丢确认 --> pending（重试，服务端幂等）
       |                  +-- 拒绝 -------> rejected
       | 取消未发送        +-- 在途取消，结果回来后保留取消意图
       v
    cancelled

    pending（前置 rejected/cancelled/blocked）--> blocked

## 接入真实服务

实现两个方法的 Service 接口即可替换模拟服务（见 server.go）：

    type Service interface {
        Submit(opID, kind, payload string) (SubmitResult, error)
        Cancel(opID string) error
    }

约定：Submit 必须对相同 opID 幂等（重发返回同一结果，不重复执行业务）；
网络类错误返回非 ErrRejected 错误以便重试，永久拒绝用
errors.Is(err, offlinequeue.ErrRejected) 判定。

## 验证

    go test -race ./... -count=5   # 全部场景，含重启无重复生效
    go run ./cmd/demo              # 端到端演示

测试覆盖：断网累积/恢复提交、依赖不抢跑、丢确认只生效一次、
拒绝阻断后继、未发送取消、已提交取消、断网取消重试、重启幂等、
多客户端独立状态、重复入队拒绝。
