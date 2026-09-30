# 客户端离线操作运行库

这是一个 Go 本地操作队列示例。状态按 `clientID` 分文件保存，不做客户端之间的文件同步。

能力：

- 离线时继续创建操作，恢复后调用 `Flush` 提交。
- 同一个操作 ID 在本客户端重复入队只保留原记录。
- `DependsOn` 未确认前，后继操作不会发送；前置取消、拒绝或阻断时，后继保持 `blocked`。
- `CancelUnsent` 只取消尚未发送的 `pending` 操作。
- `RequestSubmittedCancel` 单独处理已尝试发送的操作；服务端已经生效时返回 `ErrAlreadyCommitted` 并记录为 `cancel_denied_committed`。
- 发送前先持久化为 `sending`，重启后可重试。
- 本地模拟服务按操作 ID 幂等，丢确认后重试不会重复生效。
- 带对象版本前提的编辑（`AddWithPrecondition`）：服务端已被其他客户端推进时返回前置失效冲突，记录为 `conflict` 并保留原意图与拒绝原因；后继操作保持 `blocked`，不会自动沿用旧前提。
- 冲突/被拒操作只能由用户选择：`Revise` 以新 ID 基于新版本重提（通过 `revision_of`/`origin_id` 记录来源，旧记录不被覆盖），或 `Abandon` 放弃（记录为 `abandoned`，内容仍保留）。
- 后继操作必须经 `RebaseDependent` 显式确认新的前置操作并刷新版本前提后才会重新发送。
- 已生效操作不能再取消；`Undo` 创建独立的撤销业务操作，按自身 ID 幂等提交，丢确认重发只生效一次，重复撤销被服务端拒绝。
- 状态按客户端分文件持久化，重启后恢复依赖链与冲突/修订来源。

运行验证：

```bash
go test ./...
go vet ./...
```
