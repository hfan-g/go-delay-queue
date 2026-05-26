# Delay Queue

多层时间轮延迟队列，任务到期后通过 HTTP 回调通知业务方。

## 快速开始

编辑 `conf.yaml` 中的 Redis 连接信息，然后启动：

```bash
go run cmd/server/main.go
```

服务监听 `:8088`。

### 添加任务

`execute_at` 为 Unix 秒级时间戳：

```bash
curl -X POST "http://localhost:8088/task/add" \
  -d "id=order-cancel-123" \
  -d "callback_url=http://api.example.com/order/cancel" \
  -d "payload={\"order_id\":123}" \
  -d "execute_at=$(( $(date +%s) + 120 ))"
```

| 参数 | 必填 | 说明 |
| --- | --- | --- |
| id | 是 | 任务唯一标识 |
| callback_url | 是 | 回调地址（HTTP POST） |
| payload | 是 | 透传参数 |
| execute_at | 是 | 期望执行时刻的 Unix 秒时间戳 |

默认配置下延迟超过约 24 小时的任务会被拒绝。

### 查询任务

```
GET /task/{id}
```

## 架构

四个核心组件：

- **Scheduler** — 接收任务，写入 Redis 持久化，加入时间轮
- **TimingWheel** — 三层时间轮（1s/60 槽 → 1m/60 槽 → 1h/24 槽），到期后通知 Scheduler
- **Executor** — 固定数量 worker 池，并发执行 HTTP 回调
- **RedisStore** — 基于 Hash + Set 存储任务数据和状态索引，Lua 脚本保证状态原子性

执行失败会自动重试，重试耗尽后标记 Dead。

## 配置

见 `conf.yaml`，yaml 注释中有各字段说明。

## 运行测试

```bash
go test ./internal/... -v
```

## License

MIT
