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

运行验证：

```bash
go test ./...
go vet ./...
```
