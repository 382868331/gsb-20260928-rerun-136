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
- `PreviewConsolidation` 生成可预览的历史整理候选：已确认且仍被等待任务引用的记录标记为 `keep_referenced`，已终结但需保留链条可解释性的标记为 `summary_only`，已取消且无引用无争议的标记为 `deletable`；在途（效果未知）与仍有争议的记录一律 `protected`，绝不按年龄直接清除。
- 候选可 `WithdrawConsolidation` 撤回（仅限未采用前）；`AdoptConsolidation` 采用后删除可删记录、把仅需摘要的记录移入归档，并保留候选及其所依据的范围供审计。整理从不重新执行旧效果来填补历史，独立任务照常放行。
- 整理期间客户端可补交旧操作：依赖已归档为已确认的事实时仍能接续原依赖链；依赖已被删除、归档为取消/被拒或资料不足时，操作不再直接失败，而是以 `blocked` 保留原始意图，记录缺失前提（`MissingPremise`）与负责的整理范围（`PremisePlan`），出现在 `Unresolved` 中，可 `ConfirmSuccessor` 确认新的有效前提后继续发送；历史效果从不重新执行。
- 正式采用整理候选时会复核预览之后的变化：新出现的等待依赖、在途（效果未知）与争议记录一律保留为 `kept` 并在候选条目的 `Outcome`/`OutcomeReason` 中说明，只有仍满足条件的记录才会删除或归档。
- 保存重开后缺失前提、整理范围与缺口清单原样恢复；同一旧操作重复补交只返回原缺口记录，重复确认或重复发送均不会重复执行效果。
- `CorrectArchived` 处理对已归档结果的迟到更正：摘要标记更正、仍存活的依赖者转为阻断缺口，已终结后继的放行依据保持原记录可解释。

运行验证：

```bash
go test ./...
go vet ./...
```
