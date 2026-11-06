"""Fetch Worker：消费行情任务并以 TaskSpec 发往计算阶段。

Pipeline 第 1 级：消费 ``stockev_list:fetch``，把结果 fan-out 到 ``finance:calculate``。
Worker 先投递 ``TaskResult.emissions`` 再 ack 上游任务；若投递后 ack 前崩溃，重跑会
再次投递下游；下游 ``logical_key`` 在设定的保留期内抑制重复。
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path
import random
import sys
import time

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from qtask_list import SmartQueue, TaskContext, TaskResult, TaskSpec, Worker  # noqa: E402

REDIS_URL = "redis://localhost:6379/0"
STOCKEV_NS = "stockev_list"
FINANCE_NS = "finance"

# 下游队列；emissions 会被 Worker 自动 enqueue 到这里（跨 namespace）
calculate_q = SmartQueue(REDIS_URL, "calculate", namespace=FINANCE_NS)

worker = Worker(
    REDIS_URL,
    "fetch",                     # 消费 stockev_list:fetch
    namespace=STOCKEV_NS,
    result_queue=calculate_q,    # 未配置时返回 emissions 会报 PermanentTaskError
    max_workers=4,               # 线程池并发数；>1 时同标的任务可能并发
)


@worker.on("fetch_stock")  # 按 payload["action"] 路由到本 handler
def fetch_stock(task: dict, context: TaskContext) -> TaskResult:
    # task 是业务 payload；context 带 task_id/attempt/trace_id 等只读运行元数据。
    # 库通过检查 handler 签名决定是否传入 context（双参数 handler 自动获得）。
    symbol = task["symbol"]
    url = task["url"]

    print(f"[fetch] Fetching {symbol} from {url}")

    # 用 sleep 模拟网络请求。真实 handler 必须给外部调用设 timeout：库无法
    # 强杀线程，卡死的线程会永久占用并发额度（Semaphore 排队深度 = max_workers*2）。
    time.sleep(random.uniform(0.05, 0.2))

    price = round(random.uniform(50, 500), 2)
    volume = random.randint(1000000, 100000000)

    timestamp = datetime.now(timezone.utc).isoformat()
    return TaskResult(
        # value 仅作审计写入历史 result 字段，不影响下游
        value={"symbol": symbol, "price": price, "volume": volume},
        # emissions 先投递成功、再 ack 本任务（Worker 内固定顺序）。
        # 若"投递后 ack 前"崩溃，任务仍在 processing，recover 后重跑会再次
        # 投递下游 —— at-least-once 语义，保留期内由下游 logical_key 抑制重复。
        # 投递时下游拒收（非重复类）会抛 RetryableTaskError，本任务自动重试。
        emissions=(
            TaskSpec(
                action="calculate_ma",
                payload={
                    "symbol": symbol,
                    "price": price,
                    "volume": volume,
                    "timestamp": timestamp,
                },
                # 身份由父 task_id 派生：本任务重跑时下游重复投递会被去重
                logical_key=f"calculate:{context.task_id}",
                # 覆盖预期的重跑窗口；永久幂等仍由业务存储保证。
                dedup_until=datetime.now(timezone.utc) + timedelta(days=2),
                # 血缘与追踪：下游可回溯到本任务
                parent_task_id=context.task_id,
                trace_id=context.trace_id,
            ),
        ),
    )


if __name__ == "__main__":
    # run() 阻塞运行：启动时自动 recover 失联 Worker 的 processing 任务，
    # 注册 SIGINT/SIGTERM 优雅停机（drain 期间刷新 heartbeat，不再取新任务）。
    print(f"Starting fetch worker for {STOCKEV_NS}:fetch -> {FINANCE_NS}:calculate")
    worker.run()
