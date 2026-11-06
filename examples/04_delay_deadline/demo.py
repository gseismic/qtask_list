"""延迟任务与业务截止时间（deadline）演示。

一条命令运行（需要本地 Redis）::

    python examples/04_delay_deadline/demo.py

两个概念的区别：
- not_before_at（兼容 delay_seconds）：任务最早何时**可以被消费**，到点前放在
  delay ZSET，由 move_delay()（或 Worker 维护线程）搬回主队列；
- start_deadline_at（兼容 expire_seconds）：任务最晚何时**必须开始执行**。
  pop 时已过截止 → 拒绝执行并记录 outcome=skipped；滞留在队列里未消费的
  过期任务则出现在 deadline_missed（expired 视图）。

注意：本脚本假设 demo:timing-demo 队列为空；若上次运行中断，先清理：
    python -m qtask_list.cli clear demo:timing-demo --include-history --force
"""

from pathlib import Path
from datetime import datetime, timedelta, timezone
import sys
import time

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from qtask_list import QueueAdmin, SmartQueue, TaskSpec  # noqa: E402

REDIS_URL = "redis://localhost:6379/0"
NAMESPACE = "demo"
QUEUE_NAME = "timing-demo"

queue = SmartQueue(REDIS_URL, QUEUE_NAME, namespace=NAMESPACE)
admin = QueueAdmin(REDIS_URL)


def main() -> None:
    now = datetime.now(timezone.utc)

    print("== 1. 延迟任务：5 秒后才可消费 ==")
    queue.enqueue(
        TaskSpec(
            action="send_reminder",
            payload={"user": "alice"},
            not_before_at=now + timedelta(seconds=5),   # 最早可执行时刻
        )
    )
    payload, raw_msg = queue.pop_no_wait()
    print(f"  立即 pop: {'取不到（正确，任务在 delay ZSET）' if raw_msg is None else '意外取到'}")
    print("  等待 5 秒……")
    time.sleep(5)
    # 到期任务需要搬回主队列（Worker 维护线程自动执行；这里手动触发）
    moved = queue.move_delay()
    print(f"  move_delay 搬回 {moved} 个任务")
    payload, raw_msg = queue.pop_no_wait()
    if raw_msg:
        print(f"  现在可以消费: {payload}")
        queue.ack(raw_msg)

    print("\n== 2. 业务截止：3 秒后 pop 将被拒绝 ==")
    queue.enqueue(
        TaskSpec(
            action="price_check",
            payload={"symbol": "AAPL"},
            start_deadline_at=datetime.now(timezone.utc) + timedelta(seconds=3),
        )
    )
    payload, raw_msg = queue.pop_no_wait()
    if raw_msg:
        print(f"  截止前正常消费: {payload}")
        queue.ack(raw_msg)
    expired_id = queue.enqueue(TaskSpec(
        action="price_check",
        payload={"symbol": "TSLA"},
        start_deadline_at=datetime.now(timezone.utc) + timedelta(seconds=2),
    )).task_id
    time.sleep(3)
    payload, raw_msg = queue.pop_no_wait()
    outcome = queue.history.get(expired_id)["outcome"]
    print(f"  截止后 pop: {'拒绝（正确，任务已跳过）' if raw_msg is None and outcome == 'skipped' else '结果异常'}")

    print("\n== 3. 滞留任务过期 -> deadline_missed 视图 ==")
    # deadline_missed 不是独立存储状态，而是从主队列/重试/延迟容器里
    # 「已过截止仍未执行」的任务派生出的视图
    queue.enqueue(
        TaskSpec(
            action="snapshot_report",
            payload={"date": "yesterday"},
            # 让截止时间直接落在过去，模拟「排队太久错过窗口」
            start_deadline_at=datetime.now(timezone.utc) - timedelta(seconds=1),
        )
    )
    expired = admin.list_expired(f"{NAMESPACE}:{QUEUE_NAME}", limit=10)
    for task in expired:
        print(f"  expired: {task['task_id'][:8]}… action={task.get('action')}")
    if not expired:
        print("  (无)")

    print("\n== 4. replay 错过截止的任务（必须提供新截止时间）==")
    if expired:
        new_deadline = datetime.now(timezone.utc) + timedelta(minutes=10)
        result = admin.requeue_expired(
            f"{NAMESPACE}:{QUEUE_NAME}",
            start_deadline_at=new_deadline,
        )
        print(f"  已按新截止时间重放: {result}")
        # 清理演示残留，避免队列遗留任务
        payload, raw_msg = queue.pop_no_wait()
        if raw_msg:
            queue.ack(raw_msg)


if __name__ == "__main__":
    main()
