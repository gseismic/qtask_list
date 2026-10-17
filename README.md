# qtask_list - 分布式任务队列

基于 Redis List 的分布式任务队列，核心机制是 `BRPOPLPUSH` 可靠消费 + 多子队列状态管理。仅依赖 Redis，无需 RabbitMQ/Kafka。

内置 Web Dashboard（`qtask web`）用于可视化盯盘与运维：

![Web Dashboard 总览页：KPI 卡片、告警与队列运行矩阵](docs/images/dashboard-overview-light.png)

## 特性

- **可靠消费**：`BRPOPLPUSH` 原子操作，Worker 崩溃不丢任务
- **重试退避**：失败按指数退避（30s 起步，±10% 抖动）写入延迟队列，不再瞬时烧光重试次数
- **业务去重**：`logical_key` 由哈希化 live owner 持有；在途任务不靠普通 TTL，终态按 `dedup_until` 精确保留
- **执行截止**：`expire_seconds` 写入消息信封，pop 时强制检查，过期任务跳过不执行（历史标记 `skipped`）
- **延迟任务**：基于 Redis Sorted Set 的定时任务，Lua 脚本原子迁移（单次批量上限 500）
- **Crash Recovery**：Worker 意外退出后自动恢复 processing 中的任务；维护线程周期性接手失联 Worker 的任务
- **多级流水线**：通过 `result_queue` 串联多个 Worker，构建多阶段处理管道
- **大 payload 外存**：超过阈值自动走 RemoteStorage，避免撑爆 Redis；瞬时拉取失败延后重试不进 DLQ
- **信号量背压**：线程池 Semaphore 防止任务排队无限增长
- **批量 Pipeline**：push_batch、历史查询、归档均使用 Redis Pipeline 减少 RTT
- **历史归档**：仅 terminal 状态（completed/failed/skipped）的任务归档到 SQLite，live 任务历史保留 Redis

## 安装

```bash
pip install -e .                  # 基础安装
pip install -e ".[dev]"           # 开发依赖 (pytest, ruff, mypy)
pip install -e ".[dashboard]"     # Dashboard 支持 (fastapi, uvicorn, jinja2)
pip install -e ".[storage]"       # RemoteStorage 服务端支持
pip install -e ".[all]"           # 全部可选依赖，包含开发工具
```

## 架构概览

### 整体架构

```
┌──────────────┐     ┌──────────────────────┐     ┌──────────────┐
│  Producer    │────▶│   Redis List Queue    │────▶│   Worker     │
│ (SmartQueue) │     │   ns:queue_name       │     │  (消费者)    │
│  push 任务   │     │                       │     │              │
└──────────────┘     │  :processing (处理中)  │     │   handler →  │
                     │  :retry      (重试)    │     │   ack/fail   │
                     │  :dlq        (死信)    │     │              │
                     │  :delay      (延迟)    │     └──────┬───────┘
                     └──────────────────────┘            │
                                                          ▼
                                                  ┌──────────────┐
                                                  │ result_queue │
                                                  │ (下一级队列) │
                                                  └──────────────┘
```

### 核心模块

| 文件 | 类 | 职责 |
|------|-----|------|
| `queue.py` | `SmartQueue` | 队列 CRUD，ready/processing/dlq/delay 管理，兼容旧 retry List |
| `worker.py` | `Worker` | 三线程模型（主循环 + 维护线程 + 线程池），handler 路由，并发处理 |
| `history.py` | `TaskHistory` | Redis ZSET + Hash 存储任务生命周期，TTL 自动过期清理 |
| `storage.py` | `RemoteStorage` | HTTP 客户端，大于 50KB 的 payload 自动外存 |
| `archiver.py` | `ArchiveManager` | Redis 任务历史 → SQLite 归档 |
| `archiver.py` | `Monitor` | Redis `INFO MEMORY` 内存监控 |
| `cli/__main__.py`（`qtask_list/cli/`） | Typer CLI | 任务生命周期控制台：push/peek/status/requeue/retry/recover/history/task/worker/archive/monitor/dashboard |

### 队列结构

每条逻辑队列在 Redis 中对应主队列和若干状态队列：

| 子队列 Key | 数据结构 | 用途 |
|-----------|---------|------|
| `{ns}:{name}` | List | 主队列，待消费任务 |
| `{ns}:{name}:processing` | List | 默认 processing 队列（SmartQueue 直接使用 / 兼容旧版本） |
| `{ns}:{name}:processing:{worker_id}` | List | Worker 专属 processing 队列，避免多 Worker 启动时误恢复活跃任务 |
| `{ns}:{name}:retry` | List | V1 兼容重试队列；V2 新重试统一写入 delay ZSET |
| `{ns}:{name}:dlq` | List | 死信队列（重试耗尽） |
| `{ns}:{name}:delay` | Sorted Set | 延迟与 `retry_wait` 队列（按时间戳排序，Lua 脚本原子迁移） |
| `{ns}:{name}:worker:{worker_id}` | String + TTL | Worker heartbeat，用于判断 processing 是否已失联 |

队列名称可以包含多级 namespace，但不能使用内部前缀 `qtask:`、空路径段或 Redis glob 字符 `*?[]`。两个队列不能同时占用对方的状态路径，例如 `jobs` 与 `jobs:delay`、`jobs:processing:other`；库会在写入前拒绝实际冲突。单独使用 `testns:dedup` 等名称仍然有效。

### 任务生命周期

```
push() ──▶ [主队列] ──▶ pop(BRPOPLPUSH) ──▶ [processing]
               │                                    │
               │                            ┌───────┴───────┐
               │                            ▼               ▼
               │                         ack()           fail()
               │                      (删除+记完成)   (删除+判断重试)
               │                                            │
               │                                  ┌─────────┴─────────┐
               │                                  ▼                   ▼
               │                         [delay/retry_wait]       [dlq 队列]
               │                          到点 move_delay()       retry >= max_retry
               │                                  │
               │                                  └────────────────────┘
               │
               └─ V1 retry List 由 move_retry() 迁移（仅兼容旧消息）
```

## 核心模块详解

### SmartQueue — 队列核心

```python
from qtask_list import SmartQueue

q = SmartQueue(
    redis_url="redis://localhost:6379/0",
    queue_name="fetch",
    namespace="stockev",
    max_retry=3,                # 最大重试次数
    large_threshold=50 * 1024,  # 大 payload 阈值 (50KB)
    ttl_days=15,                # 历史记录保留天数
)
```

**生产端 API：**

```python
# 单条任务
task_id = q.push({"action": "fetch_stock", "symbol": "AAPL"})

# 延迟任务 (60 秒后执行)
task_id = q.push({"action": "fetch_stock", "symbol": "AAPL"}, delay_seconds=60)

# 业务身份键去重：同键任务存在时跳过投递并返回 None
task_id = q.push(
    {"action": "fetch_kline", "symbol": "AAPL"},
    logical_key="kline:AAPL:1d:2026-09-19",  # 身份键：类型:主体:时间桶
    expire_seconds=7 * 86400,                  # 执行截止：超期未执行则 pop 时跳过
)
task_id = q.push(..., logical_key="kline:AAPL:1d:2026-09-19", force=True)  # 显式绕过去重

# 批量推送 (Redis Pipeline 优化)；支持 delay/expire/logical_keys，被去重的项返回 None
task_ids = q.push_batch(
    [
        {"action": "fetch_news", "url": "https://.../a"},
        {"action": "fetch_news", "url": "https://.../b"},
    ],
    logical_keys=["news:sha(a)", "news:sha(b)"],
)
```

新代码建议使用结构化的 `TaskSpec`，将业务 payload 与调度/身份元数据分开：

```python
from datetime import datetime, timedelta, timezone
from qtask_list import TaskSpec

now = datetime.now(timezone.utc)
result = q.enqueue(
    TaskSpec(
        action="fetch_quote",
        payload={"symbol": "AAPL"},
        logical_key="quote:AAPL:20260920T1310",
        scheduled_for=now,
        start_deadline_at=now + timedelta(minutes=7),
        dedup_until=now + timedelta(days=2),
    )
)
if result.accepted:
    print(result.task_id)
```

`TaskSpec` 的 `scheduled_for`、`not_before_at`、`start_deadline_at` 和 `dedup_until` 必须使用带时区的 `datetime`。旧 `push()`/`push_batch()` 仍可使用，但会发出兼容层弃用提醒。

`enqueue_many()` 按顺序逐项提交；中途异常会抛出 `BatchEnqueueError`，其 `index` 是失败项下标，`results` 是此前已经完成的结果前缀。批次不是跨任务事务。

**消费端 API：**

```python
# 阻塞获取 (BRPOPLPUSH)
payload, raw_msg = q.pop(timeout=10)

# 非阻塞获取
payload, raw_msg = q.pop_no_wait()

# 确认 / 失败
q.ack(raw_msg)                    # 标记完成
q.fail(raw_msg, "error reason")   # 标记失败，自动判断重试或入 DLQ
```

**管理操作：**

```python
q.recover()        # Crash recovery: processing → 主队列
q.move_retry()     # 迁移 V1 retry List；V2 retry_wait 由 move_delay() 到点迁移
q.move_delay()     # delay 到期 → 主队列 (Lua 原子操作，单次最多 500 条)
q.requeue_dlq()    # DLQ replay 为新 task_id；V1 无 V2 描述时才原地兼容迁移
q.clear()          # 清空 ready/retry/delay/dlq；默认保留 processing
q.get_stats()      # 返回 queue/processing/retry/retry_wait/dlq/delay 等计数
```

**关键设计决策：**

- `pop()` 使用 `BRPOPLPUSH`（非 `BLPOP`），取任务的同时推入 `processing` 队列。Worker 使用带 heartbeat 的专属 processing key，只自动恢复已失联 Worker 的任务。直接使用 `SmartQueue(processing_key=...)` 时，领取路径会登记该 key，管理统计与队列级恢复可找到它；这种消费者没有 Worker heartbeat，执行恢复前应先确认消费者已停止。
- Lua 只填充运行元数据，业务 JSON 由 Python 保真序列化；inline、压缩和外存模式均保留空数组、64 位整数与浮点值。
- 领取阶段 Redis 响应丢失后，Worker 会恢复本地未登记消息；handler 尚未准入的 begin 不占用执行预算。handler 完成后的结算暂时失败会持续重试结算并保留续租，不重复运行 handler。已经发生的 payload 读取失败仍消耗 attempt。
- 重试退避：`fail()` 未耗尽重试次数时按 `retry_backoff_base * 2^(attempt-1)`（±10% 抖动，上限 `retry_backoff_max`）写入 delay ZSET，到点由 `move_delay()` 迁回主队列。`QueueAdmin.queue_stats()` 从状态转换计数读取 `retry_wait`；该值是 `delay` 的子集。V1 retry List 仅由 `move_retry()` 兼容迁移。
- 执行截止：`TaskSpec.start_deadline_at`（兼容 `push(expire_seconds=...)`）写入任务记录并同步写入 V2 信封；pop/begin-attempt 强制检查，过期任务标记 `skipped`/`deadline_missed`，不进入 DLQ。旧 V1 `expires_at` 仍可读取。
- `move_delay()` 使用 Redis **Lua 脚本**，原子地将到期任务从 ZSET 迁移到主队列，单次调用最多迁移 500 条，避免同一秒大量任务到期时阻塞 Redis。
- 大 payload 自动外存：push 时超过 `large_threshold` 则上传到 `RemoteStorage`，队列中仅存引用。消费端未配置 storage 时显式报错进 DLQ（配置错误）；storage 瞬时不可用时按退避重试，耗尽 `max_attempts` 后进入 DLQ。旧 V1 外存引用也在读取前迁移到统一状态机，受同样的预算和 deadline 约束。

### Worker — 任务处理器

**执行模型：**

- **主线程 (`_worker_loop`)**：循环 pop 任务 → 提交到线程池
- **线程池 (`ThreadPoolExecutor`)**：并发执行 handler
- **心跳线程 (`_heartbeat_loop`)**：刷新 Worker 心跳，并续租已经领取及等待线程池执行的任务
- **维护线程 (`_maintenance_loop`)**：定时健康检查 + 历史归档

```python
from qtask_list import Worker, SmartQueue

# 创建下游队列 (可选)
store_q = SmartQueue("redis://localhost:6379/0", "store", namespace="stockev")

worker = Worker(
    redis_url="redis://localhost:6379/0",
    queue_name="fetch",
    namespace="stockev",
    result_queue=store_q,    # 处理结果自动推送到下游队列
    max_workers=4,           # 并发数
    max_retry=3,             # 最大重试
)

# 注册 handler — 按 payload["action"] 字段路由
@worker.on("fetch_stock")
def handle_fetch(task: dict):
    symbol = task["symbol"]
    price = fetch_price(symbol)
    # 返回 dict → 自动 push 到 result_queue
    return {"action": "store_price", "symbol": symbol, "price": price}

# 启动 (自动 crash recovery + 注册信号)
worker.run()
```

**关键设计决策：**

- **信号量背压**：`Semaphore(max_workers * 2)` 限制线程池排队深度，防止任务无限积压。
- **启动自动 recovery**：`run()` 只恢复 heartbeat 已过期的 `processing:{worker_id}`，避免抢回其他活跃 Worker 正在处理的任务。
- **优雅停止**：`SIGINT/SIGTERM` 信号 → `stop()`；已准入 handler 的任务在 drain 期间继续刷新 heartbeat，尚未准入的已领取任务退回 ready 并恢复 attempt，最后再清理 Worker 状态。
- **maintenance_interval**：默认 30 分钟执行一次归档和内存检查。

### QueueAdmin — 管理接口

`QueueAdmin` 是给 Dashboard、CLI、Agent 和运维脚本复用的任务生命周期控制接口。它使用用户能理解的状态和动作，不要求调用方直接拼 Redis Key。

```python
from qtask_list import QueueAdmin, QueueState

admin = QueueAdmin("redis://localhost:6379/0")

# 查看队列和任务
queues = admin.list_queues()
tasks = admin.list_tasks("stockev:day-kline:fetch", state="dlq", limit=50)
history = admin.list_tasks("stockev:day-kline:fetch", state=QueueState.history, limit=50)
completed = admin.list_tasks("stockev:day-kline:fetch", state=QueueState.completed, limit=50)
failed = admin.list_tasks("stockev:day-kline:fetch", state=QueueState.failed, limit=50)
expired = admin.list_tasks("stockev:day-kline:fetch", state=QueueState.expired, limit=50)
recent_done = admin.list_tasks(
    "stockev:day-kline:fetch",
    state=QueueState.completed,
    completed_after=1717200000,
)
detail = admin.get_task("<task_id>")

# 手动控制
admin.push_task("stockev:day-kline:fetch", {"action": "test"})
admin.push_task("stockev:day-kline:fetch", {"action": "test"}, expire_seconds=3600)
admin.requeue_task("stockev:day-kline:fetch", "<task_id>", from_state="dlq")
admin.requeue_dlq("stockev:day-kline:fetch")
admin.list_expired("stockev:day-kline:fetch", limit=100)
admin.requeue_expired(
    "stockev:day-kline:fetch",
    start_deadline_at="2026-09-21T16:00:00+00:00",  # 必须是未来时刻
)
admin.move_retry("stockev:day-kline:fetch")
admin.recover("stockev:day-kline:fetch")  # 默认只恢复 stale worker
admin.recover("stockev:day-kline:fetch", worker_id="worker-01")  # 仅恢复指定失联 Worker

# 清理、删除和诊断
admin.clear_queue("stockev:day-kline:fetch")  # 默认保留 processing 与历史
admin.clear_queue("stockev:day-kline:fetch", include_history=True)  # 无剩余 processing 时可清历史
admin.clean_history("stockev:day-kline:fetch", ttl_days=15)
admin.delete_task("<task_id>", queue_name="stockev:day-kline:fetch")
admin.delete_queue("stockev:day-kline:fetch")  # 删除整条队列及历史，谨慎使用
admin.diagnose("stockev:day-kline:fetch")
```

`QueueState` 用户可见状态：`ready`, `processing`, `retry`, `retry_wait`, `dlq`, `delay`, `history`, `completed`, `failed`, `skipped`, `cancelled`, `deadline_missed`, `expired`, `all`。其中 `retry` 是旧 List 兼容视图，V2 正在等待退避的任务位于 `retry_wait`。

`expired` 是 `deadline_missed` 的兼容别名：投递时通过 `TaskSpec.start_deadline_at`（或兼容参数 `expire_seconds`）设定最晚开始时间，任务未完成且超过该时间后会出现在过期视图中；`clean-history` / `clean_expired()` 表示历史记录 TTL 清理，二者不是同一件事。

`skipped` 表示任务在 begin-attempt 时已过 `start_deadline_at`，被跳过未执行；`cancelled` 表示被 supersede 或管理操作取消。

`QueueAdmin.recover(..., worker_id=...)` 只处理该 Worker 的 processing，仍在迁移时原子复核心跳；其他失联 Worker 和 legacy processing 不在该范围内。省略该参数保持队列级恢复行为。请使用 `list_workers()` 返回的标识；指定标识不得为空、包含冒号、Redis glob 字符或控制字符，最长 256 个 UTF-8 字节。

队列与 Worker 的自动发现统一使用注册表：`queue_names()` 仅读取 `qtask:queues`，`list_workers()` 仅读取相应 `qtask:workers:<queue>`，不扫描 Redis 键空间，也不缓存发现结果。投递任务及 Worker 心跳会登记队列；空闲 Worker 也可立即显示。Worker 退出后如有残留 processing，会保留注册项供诊断和恢复。未登记的旧状态键、心跳或历史不会自动出现在全局列表。

清空消息会保留已登记的逻辑队列；删除队列会移除注册项。外部删除注册项后，现有 Dashboard/CLI 的下一次查询立即反映变化；仍在运行的 Worker 会在下一次心跳重新登记自己的队列。

删除队列在同一次 Redis Lua 中复核 Worker 心跳、验证数据归属和类型、删除所属消息与记录、安排专属外存回收并注销队列。有活跃 Worker 时拒绝删除；并发投递只会发生在整个删除操作之前或之后。该管理操作会同步遍历队列数据和相关命名空间，大队列应在维护窗口执行。

跨队列搜索使用 `QueueAdmin.search_tasks_page(queues=None, search=..., limit=50, cursor=...)`；`queues=None` 在第一页固定当前队列集合，后续游标沿该快照翻页。也可显式传入队列列表。响应包含 `tasks`、`next_cursor`、`has_more`、`scan_limited`；默认每页扫描预算为 5000 条，达到上限时使用游标继续检索。Dashboard 固定使用这个预算。`all` 包含 Redis 中仍保留的终态历史，SQLite 归档不在该搜索范围。`QueueAdmin.list_tasks()` 的筛选扫描最多遍历 5000 条，超过后报错并提示改用分页接口。

历史分页使用创建时间和任务 ID 作为稳定边界，删除已读记录不会跳过下一页。各原始容器状态也支持继续翻页，并补充没有历史索引的消息；这类诊断续页期间容器发生变化时要求刷新第一页。旧格式游标同样需要刷新，不再以 500 条作为列表终点。

总览的 `completed_total` 等字段是状态转换累计数，`history` 是当前 Redis 历史索引条数。旧队列需要在暂停投递和消费的维护窗口运行 `qtask rebuild-observation <queue>` 回填截止时间及等待重试索引；界面会标记未回填队列。

DLQ 重放（`admin.requeue_dlq` / `qtask requeue`）创建新的 `task_id`，原失败记录保持不变，并通过 `replay_of` / `replayed_by` 保留血缘；新实例从 `attempt=0` 开始。`reset_retry` 与 CLI `--keep-retry` 仅为兼容参数，V2 replay 不会复活原终态。从活跃 Worker 的 processing 重放会被拒绝，需先执行 recover。

Dashboard、CLI 和 `QueueAdmin.replay_task()` 默认继承原记录的 `max_attempts`；可用 `max_attempts` 显式覆盖。`SmartQueue.replay_task()` 未传该参数时仍使用当前队列配置。重复重放会追加全部新实例的血缘。批量 DLQ 重放返回 `moved`、`skipped`、`attempted`、`not_attempted` 和最多 20 项失败原因；截止已过或缺少 V2 审计记录的消息保留，不能降级成普通 payload 投递。界面保留部分完成的确认框，CLI 对部分完成以非零状态退出。

DLQ 入口要求原消息仍在 DLQ 中，并在同一 Lua 中成功移除后才生成实例；批量快照中的消息若已被另一操作移走，会计入跳过。库调用 `replay_task(..., require_dlq=True)` 可显式要求这一条件；默认 False 保留从历史记录主动重放的行为。

### TaskHistory — 任务历史

记录每个任务从创建到完成/失败的全生命周期状态：

```
Redis Key 结构:
  qtask:task:{task_id}     → Hash   (action, status, created_at, payload, ...)
  qtask:hist:{queue_name}  → ZSET   (时间戳索引 task_id)
```

- 历史索引 ZSET 不设置 TTL；任务记录只在进入终态且不再有 operational message 后按 `ttl_days` 设置 TTL。`clean_expired()` 会跳过仍在 ready/processing/delay/DLQ 中的任务。
- 同时兼容 Hash 和 String 两种存储格式，保证向后兼容。

### RemoteStorage — 大文件外存

解决 Redis 不适合存储大 payload 的问题。新对象使用每任务独占的随机 key：

```
push: payload > large_threshold (50KB)
  → POST /api/storage/upload → 返回独占 key 与 storage_id
  → 队列中只保存外存描述符

pop: 读取外存描述符
  → GET /api/storage/download/{key} → 校验并还原 payload
```

这是一个 HTTP 客户端，需外部存储服务配合。项目内置了一个轻量服务端，可通过 CLI 启动：

```bash
qtask storage --port 8096 --data-dir ~/.qtask-storage --gc-redis redis://localhost:6379/0
```

服务端依赖安装：

```bash
pip install -e ".[storage]"
```

服务端默认只监听 `127.0.0.1`；远程访问必须设置 `QTASK_STORAGE_TOKEN`，客户端会自动从同名环境变量读取 Bearer token。通过本机反向代理对外发布时也应设置 token。上传上限由 `QTASK_STORAGE_MAX_BYTES` 配置，默认 64 MiB。队列托管的新对象有 24 小时待入队宽限期，成功入队后在 live 和 DLQ 期间持续保留；终态历史保留期结束后，服务端从任务 Redis 的回收日志删除对象。到期回收及手动删除都会先在 Redis 撤销待入队资格，再删除文件；删除失败时由回收日志重试。手动删除需要回收 Redis 可用。存储服务必须连接到任务使用的同一个 Redis。旧内容寻址对象仍可读取，但不会按单任务删除；既有对象的旧 TTL 边界仍适用。

### ArchiveManager + Monitor — 归档与监控

**ArchiveManager**：将 Redis 历史任务按日期归档到 SQLite：

```
归档流程:
  1. ZSET 取过期 task_id → 批量读取详情 (Pipeline)
  2. 按 created_at 日期分库 → archive_data/qtask_hist_{YYYYMMDD}.db
  3. 写入成功后从 Redis 删除
```

SQLite 表结构：`task_history(task_id, queue_name, action, status, payload, result, created_at, updated_at, raw_data)`

**Monitor**：`Redis INFO MEMORY` 监控，超阈值告警。

## 任务身份与去重（logical_key）

动态增长的抓取列表（新闻等）和定时抓取场景需要"同一业务对象不重复投递"。规则：

```
logical_key = 任务类型 : 业务主体 : 数据时间桶
```

- **显式声明**：身份键由调用方传入，库不做 payload 自动哈希（URL 带时间戳/query 参数会让自动哈希失效）。
- **时间桶编进键**：跨期天然是新键，同桶重推被去重。live owner 不设置普通 TTL；终态 owner 按 `dedup_until` 精确保留，过期后 compare-and-delete。
- **失败自愈**：进 DLQ 的任务是 failed outcome + dlq location；重放会创建新 task_id，原任务不会被重新打开。下一个时间桶仍是新身份，调度下一轮不会被卡住。

新版 owner 键使用独立前缀 `qtask:owner:v2:{sha256(queue)}:{sha256(logical_key)}`，避免旧明文身份与哈希身份互相碰撞。旧 String owner 与旧哈希 owner 在访问时核验身份并迁移，保留终态和保留期。升级时应停止旧生产者与消费者，再统一启动新版；旧版本不能识别新的 owner 键。已有冲突队列应先在维护窗口处理命名，不会自动改名或删除数据。

各场景键模板与执行截止策略：

| 任务类型 | logical_key 示例 | 时间桶 | start_deadline_at（执行截止） |
|---|---|---|---|
| 日 K 线 | `kline:AAPL:1d:2026-09-19` | 交易日 | 无或 7 天（回补合法） |
| 5min 行情快照 | `quote:AAPL:20260920T1310` | 采样周期 | 桶结束 + 容忍度（如 420s） |
| 股票列表发现 | `universe:20260920-am` | 固定窗口（如 4h） | 下个桶开始前 |
| 新闻抓取 | `news:sha256(url)` | 条目即身份 | 无或数天 |

三个生命周期不要混淆：live identity（在途所有权）、`start_deadline_at`/`expire_seconds`（最晚开始时间）、`dedup_until`（终态身份保留）、`ttl_days`（历史记录保留）。

同一 `logical_key` 建议同时作为数据库唯一约束（如 kline 表 symbol+period+trade_date）：队列挡投递、唯一键挡落库，两层一个身份。

## 使用约束（重要）

1. **at-least-once 交付**：crash recovery 会重跑"已执行但未 ack"的任务；handler 先推 result_queue 后 ack 的窗口也会造成下游重复。**落库逻辑必须幂等（upsert / 唯一约束）**。
2. **handler 必须自带超时**：库没有任务级超时，线程无法强杀。所有外部 HTTP 请求必须设置 timeout，否则线程永久占用并发额度，且 heartbeat 持续刷新导致 stale recovery 不会接管。
3. **重试语义**：`max_retry=3` 表示**总共执行 3 次**（首次 + 2 次重试），不是首次之外再重试 3 次。
4. **并发注意**：`logical_key` 只表达身份去重；同一业务对象需要串行时，可使用独立的 `TaskSpec.concurrency_key`。Worker 按 heartbeat 与 lease 两者较短的周期续租；租约配置须为有限数值且至少 1 毫秒，业务应按实际网络与调度延迟留足余量。

## 定时抓取部署模式（场景示例）

定时抓取推荐**外置 cron + logical_key 去重**，而不是在 handler 里自我续期（失败进 DLQ 会断链，re-push 与 ack 之间的崩溃会分裂周期链）。可直接运行的确定性 5 分钟 slot 示例见 `examples/07_advanced/scheduler.py`：

```python
# scheduler.py — 由 crontab / systemd timer 每 5 分钟调用
from datetime import datetime, timedelta, timezone
from qtask_list import SmartQueue, TaskSpec

q = SmartQueue("redis://localhost:6379/0", "quote", namespace="stockev")

now = datetime.now(timezone.utc)
slot = now.replace(minute=(now.minute // 5) * 5, second=0, microsecond=0)
bucket = slot.strftime("%Y%m%dT%H%M")  # 确定性 5min 时间桶
for symbol in watchlist():
    q.enqueue(
        TaskSpec(
            action="fetch_quote",
            payload={"symbol": symbol},
            scheduled_for=slot,
            start_deadline_at=slot + timedelta(minutes=7),
            logical_key=f"quote:{symbol}:{bucket}",
        )
    )
```

```bash
# crontab：每 5 分钟投一轮
*/5 * * * * cd /path/to/app && python scheduler.py
```

- 上一轮未消费完时，下一轮的同键任务被去重，不会堆积。
- 单只股票抓取失败进 DLQ 不影响下一轮（新桶新键），DLQ 只需定期巡检（`qtask status` / Dashboard）。
- 回补（批量下载）与定时任务**分队列部署**：单队列 FIFO 下大批量回补会让时效敏感的定时任务排队过期。

## 快速开始

### 1. 启动顺序 (重要!)

多级流水线场景必须先启动下游 Worker，再启动上游。完整三级流水线示例见 `examples/06_pipeline/`：

```
终端 1: python examples/06_pipeline/stockev/store_worker.py     (末级，先启动!)
终端 2: python examples/06_pipeline/finance/calculate_worker.py (中级)
终端 3: python examples/06_pipeline/stockev/fetch_worker.py     (首级)
终端 4: python examples/06_pipeline/generator.py                (生产者)
```

原因：上游 Worker 通过 `TaskResult.emissions` 把结果投递到下游队列，若无下游 Worker 消费则任务堆积。

### 2. Python SDK 示例

```python
from qtask_list import SmartQueue

q = SmartQueue(
    redis_url="redis://localhost:6379/0",
    queue_name="fetch",
    namespace="stockev_list"
)

# 单条任务
q.push({"action": "fetch_stock", "symbol": "AAPL", "url": "https://api.example.com/AAPL"})

# 批量任务
q.push_batch([
    {"action": "fetch_stock", "symbol": "AAPL"},
    {"action": "fetch_stock", "symbol": "TSLA"},
])

# 延迟任务 (60 秒后执行)
q.push({"action": "fetch_stock", "symbol": "AAPL"}, delay_seconds=60)
```

```python
from qtask_list import Worker, SmartQueue

store_q = SmartQueue("redis://localhost:6379/0", "store", namespace="stockev_list")

worker = Worker(
    redis_url="redis://localhost:6379/0",
    queue_name="fetch",
    namespace="stockev_list",
    result_queue=store_q,
    max_workers=4,
)

@worker.on("fetch_stock")
def handle_fetch(task):
    symbol = task["symbol"]
    price = fetch_price(symbol)
    return {"action": "store_price", "symbol": symbol, "price": price}

worker.run()
```

### 3. CLI 命令

```bash
# 查看所有队列状态
qtask status
qtask status --namespace stockev --problems-only
qtask status --namespace stockev --problems-only --json  # 脚本消费完整状态

# 查看指定队列
qtask status stockev_list:fetch

# 投递测试任务
qtask push stockev_list:fetch '{"action":"fetch_stock","symbol":"AAPL"}'
qtask push stockev_list:fetch --file task.json --delay 60

# 查看队列中的实际消息
qtask peek stockev_list:fetch --state ready -l 20
qtask peek stockev_list:fetch --state dlq --json

# 实时监控 (2 秒刷新)
qtask watch stockev_list:fetch -i 2

# 清空 ready/retry/delay/dlq；默认保留 processing 与历史
qtask clear stockev_list:fetch --force
# 无活跃 Worker 时可显式清理失联 processing
qtask clear stockev_list:fetch --include-processing --force
# 需要同时删除已终结历史时显式加 --include-history
qtask clear stockev_list:fetch --include-history --force

# DLQ 重新入队
qtask requeue stockev_list:fetch --force
qtask requeue stockev_list:fetch --task-id <task_id> --force

# V1 retry List → 主队列（V2 retry_wait 由维护线程自动到点迁移）
qtask retry stockev_list:fetch

# Crash recovery：默认只恢复 stale worker 的 processing，避免抢活跃 Worker 任务
qtask recover stockev_list:fetch
qtask recover stockev_list:fetch --force-active --yes

# 查看历史
qtask history stockev_list:fetch
qtask history stockev_list:fetch -l 50
qtask history -t <task_id>

# 单任务查看、重放、删除
qtask task get <task_id>
qtask task requeue <task_id> --queue stockev_list:fetch --from dlq --force
qtask task delete <task_id> --queue stockev_list:fetch --force

# 启动用户代码中已注册 handler 的 Worker
qtask worker --module myapp.workers:worker

# 清理过期历史
qtask clean-history stockev_list:fetch -t 15
qtask clean-history

# 归档到 SQLite
qtask archive stockev_list:fetch -d 1

# Redis 内存监控
qtask monitor

# 启动 RemoteStorage 服务端（大 payload 外存）
qtask storage --port 8096 --data-dir ~/.qtask-storage --gc-redis redis://localhost:6379/0

# 启动 Web Dashboard（旧命令 qtask dashboard 仍兼容）
qtask web
```

Dashboard 是 React 单页应用（构建产物已随包发布，无需 Node 环境）。启动后打开 `http://localhost:8765`，信息架构面向"盯盘 → 排查 → 处理"的运维动线（总览页见文首截图）：

| 页面 | 路由 | 用途 |
|------|------|------|
| 总览 | `/` | namespace/名称筛选、当前范围 KPI、分组运行矩阵与告警摘要；色条展示当前排队/执行/延迟/旧重试构成，浏览器观测完成量单列展示；筛选与折叠记忆在本浏览器 |
| 队列 | `/queues` | 队列卡片、筛选与搜索；进入详情做操作 |
| 队列详情 | `/queues/:name` | 完整状态 Tabs（ready/processing/retry/retry_wait/delay/dlq/completed/failed/skipped/cancelled/deadline_missed/history）、任务搜索与时间筛选、诊断、手工投递、队列级操作（drain retry/重放 DLQ/恢复 stale/清理历史/清空/删除） |
| 任务 | `/tasks` | 跨队列搜索 task_id/action/payload，并可按 action 与发布时间/完成时间筛选；task_id 精确命中直接打开详情抽屉 |
| 任务详情抽屉 | 表格内点击 | 最新状态、失败原因、重试时间、payload/result 懒加载、可跳转血缘；failed/skipped/cancelled 可更新已过期截止后重放，成功可直接查看新任务；completed 不提供重放 |
| Worker | `/workers` | 心跳与失联标记、单个 Worker 的失联任务恢复、Redis 内存水位 |
| 告警 | `/alerts` | DLQ 堆积、ready 无 Worker、累计失败率偏高、stale worker、ready 积压、任务过期、Worker 失联、Redis 内存超限的聚合列表；可定位跳转、标记已处理 |
| 教程 | `/guide` | 说人话的概念手册（状态流转、关键数字、去重、操作速查表） |

其他能力：白天/黑夜双主题（记忆在浏览器）、自动刷新（5s/15s/30s/关，出错自动降频 30s）、危险操作（清空/删除）需输入队列名二次确认、未登录自动跳转 `/login`。

总览的完成量和速率由本次访问的浏览器采样，在 SPA 页面间切换会保留，完整重新加载页面后重新采样。至少观测 30 秒后显示速率；没有调度器提供的“本轮期望量”时无法计算批次完成百分比。只有持续 ready 积压、在线 Worker 且连续 2 分钟无完成时才显示“疑似停滞”；空闲、无 Worker、完成进展或超过 60 秒的观测中断会重置停滞计时。未知旧指标显示为未知。

顶栏刷新作用于全局监控和当前任务视图。任务列表默认按顶栏间隔更新第一页，加载更多时自动取消该列表的自动刷新以保留当前位置；手动刷新第一页或重新勾选后可查看最新结果。列表和详情显示各自最近成功更新时间，请求失败时保留当前查询的旧数据并标明过时。全局与单队列任务均支持精确 action 和发布/完成时间筛选。

队列与任务的管理 API 提供查询参数入口，如 `GET /api/queues/detail?name=...`、`GET /api/queues/tasks?name=...`、`POST /api/queues/requeue-dlq?name=...`、`GET /api/tasks/detail?task_id=...`、`POST /api/tasks/replay?task_id=...`。名称和 ID 应按查询参数编码，支持 `/`、`%`、中文等字符；原有单数路径入口仍可使用。

Dashboard 内存告警默认使用 Redis `maxmemory`；可设置 `QTASK_DASHBOARD_MEMORY_THRESHOLD_MB` 为有限正数（MB）。两者同时存在时取较小值，均未配置时明确显示“未启用”，不判断为正常。该配置只影响 Dashboard，Worker 的 `monitor_threshold_mb` 配置保持独立。

告警“已处理”只保存在本浏览器，确认后立即更新；完整重新加载页面后持续故障的确认仍有效。监测到故障恢复后再复发时重新提示。未打开后台期间发生的恢复与复发无法由浏览器单独判断，多人共享或后台持久化告警仍需要服务端模型。

### 运行 Dashboard

**本机使用**：默认监听 `127.0.0.1`，无需认证，启动后自动打开浏览器：

```bash
qtask web               # 默认 8765 端口，旧命令 qtask dashboard 仍兼容
qtask web --port 8900   # 指定端口
```

**远程访问**：必须启用登录，并显式监听远程地址（CLI 参数方式）：

```bash
qtask web \
  --host 0.0.0.0 \
  --port 8765 \
  --user admin \
  --password '<strong-password>' \
  --no-open
```

或通过环境变量部署（适合容器编排）：

```bash
export QTASK_DASHBOARD_USER=admin
export QTASK_DASHBOARD_PASSWORD='<strong-password>'
export QTASK_DASHBOARD_SECRET='<random-secret>'
qtask web --host 0.0.0.0 --no-open
```

**不用 CLI，直接用 uvicorn**（适合 systemd/容器；不经过 CLI 的认证保护检查，需自行配置认证）：

```bash
pip install "qtask_list[dashboard]"

export QTASK_DASHBOARD_USER=admin
export QTASK_DASHBOARD_PASSWORD='<strong-password>'
export QTASK_DASHBOARD_SECRET='<random-secret>'
uvicorn qtask_list.dashboard.main:app --host 0.0.0.0 --port 8765
```

设置 `QTASK_DASHBOARD_PASSWORD` 后，访问 `/` 会先跳转到 `/login`，所有 `/api/*` 管理接口以及 SPA 客户端路由（如 `/queues`）也会校验登录会话。不设置密码时认证默认关闭，服务可直接访问。公网部署建议放在 HTTPS 反向代理后，并设置 `QTASK_DASHBOARD_SECURE_COOKIE=1` 或 CLI 参数 `--secure-cookie`。
CLI 绑定非 loopback 地址且未配置认证时会拒绝启动；确需在可信网络无认证运行时，必须显式传 `--allow-unauthenticated`。

### 修改前端（仅开发 Dashboard 时需要）

前端源码在 `qtask_list/frontend/`（Vite + React + TypeScript + pnpm），构建产物提交在 `qtask_list/dashboard/static/spa/`，因此使用与部署都不依赖 Node。只有改前端时才需要：

```bash
cd qtask_list/frontend
pnpm install
pnpm dev         # 开发模式，代理 /api 到本机 8765 的后端
pnpm test        # 状态、重放条件、采样与告警的前端逻辑回归
pnpm build       # 构建并输出到 qtask_list/dashboard/static/spa/，随包提交
```

## 配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `redis_url` | `redis://localhost:6379/0` | Redis 连接地址 |
| `namespace` | `""` | 命名空间，多项目隔离 |
| `max_retry` | `3` | 最大执行次数（总共），超限进入 DLQ |
| `retry_backoff_base` | `30` | 重试退避基数（秒），指数退避；0 = 旧的立即重试 |
| `retry_backoff_max` | `3600` | 单次重试退避上限（秒） |
| `large_threshold` | `50KB` | 大 payload 阈值，超限走 RemoteStorage |
| `ttl_days` | `15` | 历史记录保留天数 |
| `record_history` | `True` | 兼容参数；`False` 映射为 `history_mode=minimal`，仍保留状态机所需的最小任务记录 |
| `history_mode` | `full` | `full` 保留完整审计字段，`minimal` 仍保留 outcome/location/attempt/deadline 等正确性字段 |
| `max_workers` | `1` | Worker 线程池并发数 |
| `maintenance_interval` | `1800` (30min) | 维护线程执行间隔（归档+健康检查） |
| `stale_recover_interval` | `300` | 维护线程接手失联 Worker 任务的间隔（秒） |
| `archive_dir` | `$QTASK_ARCHIVE_DIR` 或 `./archive_data` 绝对路径 | SQLite 归档目录；多 Worker 必须共享同一目录 |
| `monitor_threshold_mb` | `None` | Redis 内存告警阈值（MB），不设则不检查 |

## 环境变量

```bash
export REDIS_URL=redis://localhost:6379/0
export QTASK_DASHBOARD_USER=admin
export QTASK_DASHBOARD_PASSWORD=<strong-password>
export QTASK_DASHBOARD_SECRET=<random-secret>
export QTASK_DASHBOARD_SESSION_TTL=86400
export QTASK_DASHBOARD_SECURE_COOKIE=1
```

## 项目结构

```
qtask_list/
├── qtask_list/              # 包根
│   ├── queue.py             # SmartQueue — 5 子队列管理
│   ├── worker.py            # Worker — 三线程任务处理器
│   ├── history.py           # TaskHistory — Redis 任务历史
│   ├── storage.py           # RemoteStorage — 大文件 HTTP 外存
│   ├── archiver.py          # ArchiveManager (SQLite 归档) + Monitor (内存监控)
│   ├── cli/                 # Typer CLI (qtask / qtask_list 命令)
│   ├── dashboard/           # FastAPI 后端（API + SPA 托管）+ templates/static
│   │   └── static/spa/      # React Dashboard 构建产物（frontend/ 构建输出）
│   ├── remote_storage/      # 外存服务端（python -m qtask_list.remote_storage.server）
│   └── frontend/            # Dashboard 前端源码（Vite + React + TS + pnpm，改前端才需要）
├── examples/                # 使用示例
├── tests/                   # 测试用例
├── pyproject.toml           # 项目配置与依赖
└── README.md
```

## 开发验证

安装开发依赖及本机 `redis-server` 后运行 `python -m pytest tests/ -q`。测试会自动启动仅监听私有 Unix socket 的临时 Redis，并把归档与外存文件写入临时目录，不使用开发或业务 Redis。结束后自动停止实例并清理临时文件。

`SmartQueue.check_consistency(limit=...)` 是抽样诊断；返回 `message_scan_complete` 和 `unverified_tasks` 表示覆盖程度。`repair_safe=True` 修改终态 operational 标记前会额外核验真实容器，不把抽样缺失当作消息已不存在。
