"""Calculate Worker：计算移动平均线并以 TaskSpec 发往存储阶段。

Pipeline 第 2 级：消费 ``finance:calculate``（由 fetch_worker 投递），结果 fan-out 到
``stockev_list:store``，用于演示跨 namespace 的 result_queue。
"""

from pathlib import Path
from datetime import datetime, timedelta, timezone
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

# 下游队列回到 stockev_list namespace：结果最终落到 stockev_list:store
store_q = SmartQueue(REDIS_URL, "store", namespace=STOCKEV_NS)

worker = Worker(
    REDIS_URL,
    "calculate",             # 消费 finance:calculate
    namespace=FINANCE_NS,
    result_queue=store_q,    # emissions 自动投递到 stockev_list:store
    max_workers=4,
)


@worker.on("calculate_ma")
def calculate_ma(task: dict, context: TaskContext) -> TaskResult:
    # 输入 payload 即上游 fetch_stock 的 emissions 内容；
    # context 可拿到 attempt（第几次重试）、trace_id 等运行元数据
    symbol = task["symbol"]
    price = task["price"]
    volume = task["volume"]

    print(f"[calculate] Computing MA for {symbol} @ ${price}")

    # 模拟指标计算；真实实现应基于行情数据计算，并给取数调用设置 timeout
    time.sleep(random.uniform(0.05, 0.15))

    ma5 = round(price * random.uniform(0.98, 1.02), 2)
    ma10 = round(price * random.uniform(0.96, 1.04), 2)
    ma20 = round(price * random.uniform(0.94, 1.06), 2)

    return TaskResult(
        # 审计结果写入 finance:calculate 历史
        value={"symbol": symbol, "price": price, "ma5": ma5, "ma10": ma10, "ma20": ma20},
        # 与 fetch 阶段同理：先投递下游、再 ack 本任务。
        # 注意 logical_key 从 finance namespace 任务的 task_id 派生，而
        # emissions 投递到 stockev_list:store —— 跨 namespace 时身份仍唯一。
        emissions=(
            TaskSpec(
                action="store_result",
                payload={
                    "symbol": symbol,
                    "price": price,
                    "volume": volume,
                    "ma5": ma5,
                    "ma10": ma10,
                    "ma20": ma20,
                    "timestamp": task["timestamp"],
                },
                # 父任务 id 派生身份：保留期内重跑不会重复生成下游存库任务
                logical_key=f"store:{context.task_id}",
                dedup_until=datetime.now(timezone.utc) + timedelta(days=2),
                parent_task_id=context.task_id,
                trace_id=context.trace_id,
            ),
        ),
    )


if __name__ == "__main__":
    print(f"Starting calculate worker for {FINANCE_NS}:calculate -> {STOCKEV_NS}:store")
    worker.run()
