---
name: qtask-list-usage
description: qtask_list 分布式任务队列使用指南。当用户需要构建 Redis 任务队列、编写 Worker 消费者、创建多级流水线、使用 CLI 运维命令（push/status/watch/clear/recover/requeue）、启动 Dashboard、或参考本项目的 examples/ 示例时使用。覆盖 SmartQueue、Worker、QueueAdmin、TaskHistory、RemoteStorage、ArchiveManager、Monitor 全部核心类及完整使用模式。
---

# qtask_list 使用指南

## 概述

qtask_list 是基于 Redis List 的分布式任务队列，核心机制为 `BRPOPLPUSH` 可靠消费 + 多子队列状态管理。仅依赖 Redis，无需 RabbitMQ/Kafka。

关键特性：可靠消费、V2 退避重试、DLQ 死信队列、延迟任务、Crash Recovery、多级流水线、大 payload 外存、信号量背压、历史归档。新代码优先使用 `TaskSpec`/`enqueue()`；`push()` 是兼容接口。

项目为单包结构：核心库与附属模块（CLI/Dashboard/外存服务/前端源码）都在 `qtask_list/` 包内，示例在 `examples/`。

## 安装

```bash
pip install -e .                  # 基础安装
pip install -e ".[dev]"           # 开发依赖 (pytest, ruff, mypy)
pip install -e ".[dashboard]"     # Dashboard (fastapi, uvicorn)
pip install -e ".[storage]"       # RemoteStorage 服务端 (fastapi, uvicorn, python-multipart)
```

安装后可用 `qtask` 或 `qtask_list` 命令。

## 核心 API

项目中 `qtask_list/__init__.py` 导出：

```python
from qtask_list import SmartQueue, Worker, RemoteStorage, QueueAdmin, QueueState, start_dashboard
```

### SmartQueue（生产/消费核心）

```python
from qtask_list import SmartQueue

q = SmartQueue(
    redis_url="redis://localhost:6379/0",
    queue_name="fetch",
    namespace="stockev",
    max_retry=3,                # 默认 3
    large_threshold=50 * 1024,  # 大 payload 阈值 (50KB)
    ttl_days=15,                # 历史保留天数
)
```

**生产端**：
```python
# 单条
task_id = q.push({"action": "fetch_stock", "symbol": "AAPL"})
# 批量 (Pipeline 优化)
task_ids = q.push_batch([{"action": "fetch_stock", "symbol": "AAPL"}, ...])
# 延迟 (60 秒后执行)
task_id = q.push({"action": "send_email"}, delay_seconds=60)
# 业务截止 (1 小时后仍未开始则进入 deadline_missed/expired 视图)
task_id = q.push({"action": "expire_if_slow"}, expire_seconds=3600)
```

V2 结构化投递：

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
```

身份、调度、截止时间均在信封和任务记录中维护，不要把 `_retry`、`expires_at` 等私有运行字段写进业务 payload。

**消费端**：
```python
payload, raw_msg = q.pop(timeout=10)   # 阻塞，BRPOPLPUSH
payload, raw_msg = q.pop_no_wait()     # 非阻塞
q.ack(raw_msg)                          # 成功
q.fail(raw_msg, "error reason")         # 失败，自动判断重试或入 DLQ
```

**管理**：
```python
q.recover()        # Crash recovery: processing → 主队列
q.move_retry()     # 迁移 V1 retry List；V2 retry_wait 由 move_delay() 到点迁移
q.move_delay()     # delay 到期 → 主队列 (Lua 原子操作)
q.requeue_dlq()    # V2 DLQ replay 创建新 task_id
q.clear()          # 清空所有子队列
q.get_stats()      # 包含 queue/processing/retry/retry_wait/dlq/delay
```

### Worker（三线程任务处理器）

- **主线程**：循环 pop 任务 → 提交到线程池
- **线程池**：并发执行 handler
- **维护线程**：定时健康检查 + 历史归档（默认 30 分钟间隔）

```python
from qtask_list import Worker, SmartQueue

store_q = SmartQueue("redis://localhost:6379/0", "store", namespace="stockev")

worker = Worker(
    redis_url="redis://localhost:6379/0",
    queue_name="fetch",
    namespace="stockev",
    result_queue=store_q,    # handler 返回值自动 push 到此队列
    max_workers=4,           # 并发数
    max_retry=3,
    maintenance_interval=1800,  # 维护间隔（秒），默认 30 分钟
    heartbeat_ttl=120,          # heartbeat TTL（秒）
)

@worker.on("fetch_stock")
def handle_fetch(task: dict):
    symbol = task["symbol"]
    price = fetch_price(symbol)
    return {"action": "store_price", "symbol": symbol, "price": price}

worker.run()  # 阻塞运行，自动 crash recovery + 注册 SIGINT/SIGTERM
```

**关键设计**：
- `Semaphore(max_workers * 2)` 限制线程池排队深度
- `run()` 启动时自动 recover 失联 Worker 的 processing 任务
- 优雅停止：信号触发 → `stop()`；已准入 handler 的任务在 drain 期间刷新 heartbeat，尚未准入的已领取任务退回 ready 且不消耗 attempt

### QueueAdmin（管理接口）

面向 Dashboard、CLI、Agent 的统一管理 API：

```python
from qtask_list import QueueAdmin, QueueState

admin = QueueAdmin("redis://localhost:6379/0")

# 发现
admin.list_queues()          # 返回队列深度、retry_wait、outcome、deadline 和 Worker 统计
admin.list_workers("stockev:fetch")  # Worker 信息列表

# 读取任务
admin.list_tasks("stockev:fetch", state=QueueState.dlq, limit=50)
admin.list_tasks("stockev:fetch", state=QueueState.history, limit=50)
admin.list_tasks("stockev:fetch", state=QueueState.completed, limit=50)
admin.list_tasks("stockev:fetch", state=QueueState.failed, limit=50)
admin.list_tasks("stockev:fetch", state=QueueState.expired, limit=50)
admin.list_tasks("stockev:fetch", state=QueueState.all, search="AAPL")
page = admin.search_tasks_page(None, search="AAPL", limit=50)  # 固定本次全部队列快照
if page["next_cursor"]:
    next_page = admin.search_tasks_page(
        None, search="AAPL", limit=50, cursor=page["next_cursor"]
    )
admin.list_tasks(
    "stockev:fetch",
    state=QueueState.completed,
    completed_after=1717200000,
)
admin.get_task("<task_id>")

# 诊断
admin.diagnose("stockev:fetch")  # 返回 stats + suggestions

# 控制
admin.push_task("stockev:fetch", {"action": "test", "data": 1})
admin.push_task("stockev:fetch", {"action": "test"}, delay_seconds=60)
admin.push_task("stockev:fetch", {"action": "test"}, expire_seconds=3600)
admin.move_retry("stockev:fetch")         # 迁移旧 retry List → ready
admin.requeue_dlq("stockev:fetch")        # dlq → ready（全部）
admin.requeue_task("stockev:fetch", "<id>", from_state=QueueState.dlq)  # 单条
admin.list_expired("stockev:fetch", limit=100)
admin.requeue_expired(
    "stockev:fetch",
    start_deadline_at="2026-09-21T16:00:00+00:00",
)  # deadline_missed → 新实例（必须显式提供未来截止时间）
admin.requeue_expired(
    "stockev:fetch", task_id="<id>", start_deadline_at="2026-09-21T16:00:00+00:00"
)
admin.recover("stockev:fetch")            # 安全恢复（仅 stale worker）
admin.recover("stockev:fetch", worker_id="worker-01")  # 只恢复该失联 Worker，仍复核心跳
admin.recover("stockev:fetch", include_active=True)  # 强制恢复活跃 Worker
admin.delete_task("<task_id>")
admin.delete_task("<task_id>", queue_name="stockev:fetch")
admin.delete_queue("stockev:fetch")  # 删除整条队列及历史，谨慎使用
admin.clear_queue("stockev:fetch")
admin.clear_queue("stockev:fetch", include_history=True)  # 连带历史
admin.clean_history("stockev:fetch", ttl_days=15)
admin.clean_history(ttl_days=15)  # 全部队列
```

**QueueState 枚举**：`ready`, `processing`, `retry`, `retry_wait`, `dlq`, `delay`, `history`, `completed`, `failed`, `skipped`, `cancelled`, `deadline_missed`, `expired`, `all`

`expired` 是 `deadline_missed` 的兼容别名：`TaskSpec.start_deadline_at`（兼容 `expire_seconds`）控制最晚开始时间；`clean-history` / `clean_expired()` 只负责历史记录清理，二者不是同一件事。DLQ replay 创建新 task_id，原终态不会被重新打开。

`search_tasks_page()` 默认每页最多扫描 5000 条，并通过 `next_cursor` 继续；`list_tasks()` 的筛选扫描达到 5000 条时会提示改用分页接口。Dashboard 固定采用默认扫描预算。

### TaskHistory（Redis 任务历史）

记录每任务从创建到完成/失败的全生命周期。Key 结构：
- `qtask:task:{task_id}` — Hash 存储任务详情
- `qtask:hist:{queue_name}` — ZSET 时间戳索引

历史索引 ZSET 不设置 TTL；任务记录在终态且没有 operational message 后按 `ttl_days` 保留，`clean_expired()` 会跳过仍在运行中的任务。同时兼容 Hash 和 String 格式。

### RemoteStorage（大文件外存）

push 时 payload 超过 `large_threshold`（默认 50KB）自动外存：

```
push: data > 50KB → POST /api/storage/upload → 队列仅存 {"_large": true, "key": "xxx"}
pop: 检测 _large=true → GET /api/storage/download/{key} → 还原完整 payload
```

需外部 HTTP 存储服务配合。

### ArchiveManager + Monitor

```python
from qtask_list.archiver import ArchiveManager, Monitor

# 内存监控
monitor = Monitor(redis_client, threshold_mb=512)
monitor.check_health()
monitor.get_memory_info()

# SQLite 归档
archiver = ArchiveManager(redis_url)
archiver.archive_to_sqlite("stockev:fetch", days_ago=1)  # 归档 1 天前数据
```

归档输出：`archive_data/qtask_hist_{YYYYMMDD}.db`

## CLI 命令

安装后可通过 `qtask` 或 `qtask_list` 调用。源码在 `qtask_list/cli/__main__.py`。

```bash
# 设置 Redis 连接
export REDIS_URL=redis://localhost:6379/0

# 队列状态
qtask status                             # 所有队列
qtask status stockev:fetch               # 指定队列

# 投递任务
qtask push stockev:fetch '{"action":"fetch_stock","symbol":"AAPL"}'
qtask push stockev:fetch --file task.json
qtask push stockev:fetch --file task.json --delay 60

# 查看消息
qtask peek stockev:fetch --state ready -l 20
qtask peek stockev:fetch --state dlq --json

# 实时监控
qtask watch stockev:fetch -i 2           # 2 秒刷新

# 清空队列
qtask clear stockev:fetch --force
qtask clear stockev:fetch --include-history --force

# DLQ 重放
qtask requeue stockev:fetch --force              # 全部
qtask requeue stockev:fetch --task-id <id> --force  # 单条

# V1 retry List → ready（V2 retry_wait 自动到点迁移）
qtask retry stockev:fetch

# Crash recovery
qtask recover stockev:fetch                     # 仅 stale worker
qtask recover stockev:fetch --force-active --yes # 强制恢复活跃 Worker（需要确认）

# 历史
qtask history stockev:fetch -l 50
qtask history -t <task_id>                      # 单任务详情

# 单任务操作
qtask task get <task_id>
qtask task requeue <task_id> --queue stockev:fetch --from dlq --force
qtask task delete <task_id> --queue stockev:fetch --force

# 清理过期历史
qtask clean-history stockev:fetch -t 15
qtask clean-history

# 归档到 SQLite
qtask archive stockev:fetch -d 1

# 内存监控
qtask monitor

# 启动 RemoteStorage 服务端（大 payload 外存）
qtask storage --port 8096 --data-dir ~/.qtask-storage --ttl-days 7

# 启动 Worker（加载用户模块中已注册 handler 的 worker 实例）
qtask worker --module myapp.workers:worker

# 启动 Dashboard
qtask dashboard
```

## Dashboard

基于 FastAPI + React，`qtask_list/dashboard/main.py`。

```python
# Python API
from qtask_list import start_dashboard
start_dashboard(port=8765, redis_url="redis://localhost:6379/0")
```

访问 `http://localhost:8765`，功能：
- 队列状态一览（ready/processing/retry/retry_wait/dlq/delay/completed/failed/skipped/cancelled/deadline_missed/history）
- 搜索 task_id、action、payload
- 按创建时间和完成时间筛选任务
- 任务详情和原始 JSON
- 单任务重试、删除
- 批量 drain 旧 retry List、重放 DLQ、按新截止时间 replay 错过截止的任务
- 安全恢复 stale processing
- 投递测试任务，支持 delay 和 expire_seconds
- 删除队列及关联历史记录

## 多级流水线模式

### 典型架构

```
stockev_list:fetch → finance:calculate → stockev_list:store
```

**启动顺序（关键！）**：必须先启动下游 Worker，再启动上游，防止任务堆积。

```bash
# 终端 1: store worker (下游)
python examples/stockev/store_worker.py

# 终端 2: calculate worker (中间)
python examples/finance/calculate_worker.py

# 终端 3: fetch worker (上游)
python examples/stockev/fetch_worker.py

# 终端 4: 生产任务
python examples/generator.py
```

### 代码模式

```python
# 跨 namespace 链式 Worker
next_q = SmartQueue(redis_url, "calculate", namespace="finance")

worker = Worker(
    redis_url,
    "fetch",
    namespace="stockev_list",
    result_queue=next_q,   # handler return value → push 到 next_q
    max_workers=4,
)

@worker.on("fetch_stock")
def handle_fetch(task):
    return {"action": "calculate_ma", "symbol": task["symbol"], "price": result}
```

## 队列 Key 结构

每条逻辑队列在 Redis 中对应：

| Key | 类型 | 说明 |
|-----|------|------|
| `{ns}:{name}` | List | 主队列（ready 任务） |
| `{ns}:{name}:processing` | List | 默认 processing（兼容旧版） |
| `{ns}:{name}:processing:{worker_id}` | List | Worker 专属 processing |
| `{ns}:{name}:retry` | List | V1 兼容重试队列；V2 重试进入 delay ZSET |
| `{ns}:{name}:dlq` | List | 死信队列 |
| `{ns}:{name}:delay` | Sorted Set | 延迟与 retry_wait（时间戳为 score） |
| `{ns}:{name}:worker:{worker_id}` | String+TTL | Worker heartbeat |
| `qtask:hist:{queue}` | ZSET | 历史索引 |
| `qtask:task:{task_id}` | Hash | 任务详情 |

### 任务生命周期

```
push() → [主队列] → pop(BRPOPLPUSH) → [processing]
                       ├── ack() → 删除 + 记录完成
                       └── fail() → retry < max → [delay/retry_wait] → move_delay() → [主队列]
                                  └── retry >= max → [dlq] → replay 新 task_id
```

## 配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `redis_url` | `redis://localhost:6379/0` | Redis 连接 |
| `namespace` | `""` | 命名空间，多项目隔离 |
| `max_retry` | `3` | 兼容别名；最大总执行次数（含首次执行） |
| `large_threshold` | `50KB` | 大 payload 阈值 |
| `ttl_days` | `15` | 历史保留天数 |
| `history_mode` | `full` | `full` 或 `minimal`；minimal 仍保留状态机正确性字段 |
| `max_workers` | `1` | Worker 线程池并发数 |
| `maintenance_interval` | `1800` (30min) | 维护线程间隔 |
| `heartbeat_ttl` | `120` (2min) | Worker heartbeat TTL |

## 项目结构

```
qtask_list/
├── qtask_list/           # 核心库
│   ├── __init__.py       # 公开 API
│   ├── queue.py          # SmartQueue
│   ├── worker.py         # Worker
│   ├── history.py        # TaskHistory
│   ├── storage.py        # RemoteStorage
│   ├── archiver.py       # ArchiveManager + Monitor
│   └── admin.py          # QueueAdmin + QueueState
│   ├── cli/              # Typer CLI（qtask / qtask_list）
│   │   └── __main__.py
│   ├── dashboard/        # FastAPI（main.py）+ templates + static（含 SPA 构建产物）
│   ├── remote_storage/   # RemoteStorage 服务端
│   │   └── server.py
│   └── frontend/         # Dashboard 前端源码（Vite + React + pnpm）
├── examples/
│   ├── generator.py
│   ├── stockev/          # fetch_worker.py, store_worker.py
│   └── finance/          # calculate_worker.py
├── tests/
└── pyproject.toml
```

## 常见模式与最佳实践

### 使用 redis_client 共享连接

多 Worker 场景建议共享 Redis 连接：

```python
import redis
r = redis.from_url("redis://localhost:6379/0", decode_responses=True)

q = SmartQueue(redis_client=r, queue_name="fetch")
worker = Worker(redis_client=r, queue_name="fetch", ...)
```

### 错误处理

```python
@worker.on("fetch_stock")
def handle_fetch(task):
    try:
        result = fetch_data(task["symbol"])
        return {"action": "next_step", "data": result}
    except ValueError as e:
        logger.error(f"Invalid: {e}")
        raise  # 自动进入 retry
    except Exception as e:
        logger.warning(f"Transient: {e}")
        raise  # 超 max_retry 后进入 DLQ
```

### 批量操作走 Pipeline

```python
pipe = r.pipeline()
pipe.lpush("key", "val")
pipe.hset("hash_key", mapping=data)
pipe.execute()
```

### 延迟任务到期迁移用 Lua 原子操作

`move_delay()` 内部使用 Lua 脚本保证原子性。

## 常见陷阱

1. **Worker 启动顺序**：多级流水线必须先启动下游 Worker，否则中间队列堆积
2. **CLI 命令名**：安装后为 `qtask` 或 `qtask_list`，源码调试用 `python -m qtask_list.cli`
3. **`recover()` 默认安全**：只恢复失联 Worker 的 processing，不会抢活跃 Worker 的任务。强制恢复需 `include_active=True`
4. **`clear` 不含 history**：默认清队列不删历史，删历史需显式 `--include-history`
5. **V2 重试在 delay ZSET**：`retry_wait` 由 `delay_reason=retry` 派生；`retry` List 只代表待迁移的旧消息，使用 `move_retry()` 兼容。
6. **heartbeat TTL**：Worker 崩溃后需等 heartbeat 过期，`recover()` 才会处理
7. **大 payload**：超过 50KB 自动走 RemoteStorage 客户端；服务端需安装 `qtask_list[storage]` 并启动 `qtask storage`
8. **deadline 与历史 TTL 不同**：`start_deadline_at`/`expire_seconds` 控制任务是否进入 `deadline_missed`/`expired` 视图；`clean-history` 清理的是历史记录 TTL，不会代替过期任务 replay
