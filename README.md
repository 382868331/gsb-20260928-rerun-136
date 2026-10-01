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
- 操作可携带 `ObjectID` 与 `BaseVersion` 前置版本；服务端版本已变化时按业务冲突拒绝，原操作保留意图与拒绝原因。
- 被拒操作可 `Revise` 生成带新身份与 `Revises` 来源的修订重新提交，或 `Abandon` 放弃；旧记录不被覆盖。
- 后继操作在前置被拒后保持 `blocked`，必须 `ConfirmSuccessor` 显式确认新前提才会重发，不会自动沿用旧前提。
- 已生效操作的撤销通过 `UndoCommitted` 生成独立的 `undo:*` 业务操作（记录 `UndoOf` 来源），原操作保持已确认。
- 前置被拒或放弃后，可用 `DependentsOf` 列出必须重审的全部后继操作，`PendingReview` 汇总待人工处理项；无关的独立操作仍正常发送，不会被一起丢掉。
- `UnresolvedDependencies` 列出所有前提尚未确认的操作、前提状态与原因；状态保存在各客户端文件中，重启后可直接从保存状态继续查看与处理。
- 每个操作记录 `OriginClient` 来源；`Snapshot` 导出操作组，`Sync` 合入其他客户端的操作组。身份用 `clientID/id` 限定，两个客户端使用相同本地 ID 不会冲突。
- 合并不依赖同步顺序：先 A 后 B 与先 B 后 A 收敛到同一状态，重复合入幂等；分批合入时先到达的后继先记录为未解析依赖，前提到达后自动恢复。
- 合入的外部操作只保留身份与来源，不会由本客户端重复提交；已生效结果的撤销在任何客户端上仍以新的 `undo:*` 业务操作表达。

运行验证：

```bash
go test ./...
go vet ./...
```
