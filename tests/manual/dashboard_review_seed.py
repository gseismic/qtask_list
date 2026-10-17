"""只向 handoff_dashboard_server.py 创建的私有 Redis 写入审查回归样本。"""

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import redis

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from qtask_list import QueueAdmin, SmartQueue, TaskSpec, Worker


folder = Path(sys.argv[1]).resolve()
runtime = json.loads((folder / "browser-runtime.json").read_text())
assert runtime["redis_socket"].startswith("/tmp/qtask-handoff-browser-")
connection = redis.Redis(unix_socket_path=runtime["redis_socket"], decode_responses=True)
admin = QueueAdmin(redis_client=connection)
for name in admin.queue_names():
    if name.startswith("review") or name == "中文/队列":
        admin.delete_queue(name)
now = time.time()
ids = {}


def queue(name, **options):
    return SmartQueue(redis_client=connection, queue_name=name, **options)


def failed(target, deadline=None):
    task = target.enqueue(TaskSpec("review_action", {"value": 1}, start_deadline_at=deadline))
    claim = target.pop_claim_no_wait()
    assert claim is not None
    assert target.fail(claim.raw_message, "审查回归失败样本", permanent=True)
    return task.task_id


large = queue("review:large")
for number in range(501):
    large.enqueue(TaskSpec("review_action", {"number": number}))
ids["metadata"] = queue("review:table").enqueue(TaskSpec("review_action", {})).task_id
deadline = datetime.fromtimestamp(time.time() + 0.8, timezone.utc)
ids["overdue"] = queue("review:overdue").enqueue(TaskSpec(
    "review_action", {}, start_deadline_at=deadline,
)).task_id
retry = queue("review:retry", max_attempts=3)
ids["retry"] = retry.enqueue(TaskSpec("review_action", {})).task_id
claim = retry.pop_claim_no_wait()
assert claim is not None
retry.fail(claim.raw_message, "暂时失败，等待自动重试")
ids["replay"] = failed(queue("review:replay", max_attempts=7))
bulk = queue("review:bulk", max_attempts=1)
ids["bulk_expired"] = failed(bulk, deadline)
ids["bulk_good"] = failed(bulk)
for name in ["review/slash", "中文/队列", "review%2Fname"]:
    ids[name] = queue(name).enqueue(TaskSpec("review_action", {"queue": name})).task_id
pipe = connection.pipeline()
pipe.zadd("qtask:queues", {"review:sparse": now})
for number in range(5001):
    task_id = f"review-sparse-{number}"
    pipe.hset(f"qtask:task:{task_id}", mapping={
        "task_id": task_id, "_queue": "review:sparse", "action": "rare" if number == 5000 else "common",
        "created_at": now - number, "updated_at": now, "status": "completed",
        "outcome": "completed", "location": "history", "operational_message": 0,
    })
    pipe.zadd("qtask:hist:review:sparse", {task_id: now - number})
pipe.execute()
for worker_id in ("review-stale-a", "review-stale-b"):
    worker = Worker("redis://isolated/0", "review:workers", redis_client=connection, worker_id=worker_id)
    ids[worker_id] = worker.queue.enqueue(TaskSpec("review_action", {})).task_id
    worker._refresh_heartbeat()
    assert worker.queue.pop_claim_no_wait()
    connection.delete(worker._heartbeat_key)
time.sleep(0.9)
(folder / "review-seed.json").write_text(json.dumps(ids, ensure_ascii=False, indent=2) + "\n")
connection.close()
print(json.dumps(ids, ensure_ascii=False, indent=2))
