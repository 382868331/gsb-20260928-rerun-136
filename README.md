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
- `Unresolved` 列出所有因前置失败而被阻断、需要人工重审的操作（含依赖与原因），重启后从保存状态恢复同一清单，可据此 `Revise`/`ConfirmSuccessor` 继续处理；无关操作不受影响。
- `PreviewCompaction` 生成可预览、可撤回的历史整理候选：仍被等待任务引用的已确认事实完整保留（keep），无引用的已确认记录只留摘要（summarize），无引用的已取消记录可删除（delete）；效果未知（pending/sending 等）与仍有争议（rejected/cancel_denied）的记录一律 protected，不按年龄清除。
- `AdoptCompaction` 采用候选时按当时状态逐条复核：整理期间补交的旧操作若重新引用了待摘要记录，该条目跳过并注明原因；采用后的计划保留所依据的范围（scope）供审计，已采用的计划不可撤回。
- 整理期间及之后的迟到操作仍可接续原依赖：前置已被摘要为已确认事实的，后继照常放行；前置已被删除的，操作以 `blocked` 留下可处理的缺口（注明来源计划），用 `ConfirmSuccessor` 确认新前提即可继续，系统不会重新执行旧效果来填补历史。
- `ExplainChain` 跨活记录、归档摘要与删除缺口展示长链与交叉依赖的放行依据；`CompactionPlans` 列出全部候选（preview/adopted/withdrawn），重启后保持不变；历史整理不阻塞无关任务的正常 `Flush`。

运行验证：

```bash
go test ./...
go vet ./...
```
