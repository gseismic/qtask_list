"""只为本次独立浏览器实例添加审查样本。"""

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import redis


ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from qtask_list.models import TaskSpec
from qtask_list.queue import SmartQueue


folder = Path(__file__).parent
runtime = json.loads((folder / "browser/browser-runtime.json").read_text())
assert runtime["redis_socket"].startswith("/tmp/qtask-handoff-browser-")
connection = redis.Redis(unix_socket_path=runtime["redis_socket"], decode_responses=True)


def queue(name, **options):
    return SmartQueue(redis_client=connection, queue_name=name, **options)


ids = {}
large = queue("review:large")
for number in range(501):
    large.enqueue(TaskSpec(action="review_action", payload={"number": number}))
metadata = queue("review:table")
ids["metadata"] = metadata.enqueue(TaskSpec(action="review_action", payload={})).task_id
overdue = queue("review:overdue")
ids["overdue"] = overdue.enqueue(TaskSpec(
    action="review_action", payload={},
    start_deadline_at=datetime.fromtimestamp(time.time() + 0.5, timezone.utc),
)).task_id
retry = queue("review:retry", max_attempts=3)
ids["retry"] = retry.enqueue(TaskSpec(action="review_action", payload={})).task_id
claim = retry.pop_claim_no_wait()
assert claim is not None
retry.fail(claim.raw_message, reason="review transient failure")
bulk = queue("review:bulk", max_attempts=1)
ids["bulk"] = bulk.enqueue(TaskSpec(
    action="review_action", payload={},
    start_deadline_at=datetime.fromtimestamp(time.time() + 0.5, timezone.utc),
)).task_id
claim = bulk.pop_claim_no_wait()
assert claim is not None
bulk.fail(claim.raw_message, reason="review permanent failure")
slash = queue("review/slash")
slash.enqueue(TaskSpec(action="review_action", payload={}))
now = time.time()
pipe = connection.pipeline()
pipe.zadd("qtask:queues", {"review:sparse": now})
for number in range(5001):
    task_id = f"review-sparse-{number}"
    pipe.hset(f"qtask:task:{task_id}", mapping={
        "task_id": task_id, "_queue": "review:sparse", "action": "review_action",
        "created_at": now - number, "updated_at": now, "status": "completed",
        "outcome": "completed", "location": "history", "operational_message": 0,
    })
    pipe.zadd("qtask:hist:review:sparse", {task_id: now - number})
pipe.execute()
time.sleep(0.6)
(folder / "browser/review-seed.json").write_text(json.dumps(ids, indent=2) + "\n")
connection.close()
print(json.dumps(ids, indent=2))
