"""在独立 Redis 上验证 1 万条队列记录、1 万个无关键和自定义 processing。"""

import json
import sys
import time
from pathlib import Path

import redis

root = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(root))
from qtask_list import QueueAdmin, SmartQueue

runtime = json.loads((Path(__file__).parent / "browser-3/browser-runtime.json").read_text())
assert runtime["redis_socket"].startswith("/tmp/qtask-handoff-browser-")
r = redis.Redis(unix_socket_path=runtime["redis_socket"], decode_responses=True)
admin = QueueAdmin(redis_client=r)
queue = SmartQueue(redis_client=r, queue_name="review:delete-scale")
prefix = "review-benchmark-noise:"
custom = "review-delete-custom-inflight"
now = time.time()
pipe = r.pipeline(transaction=False)
pipe.zadd("qtask:queues", {queue.base: now})
pipe.zadd(queue.processing_registry_key, {custom: now})
for index in range(10000):
    task_id = f"scale-{index}"
    raw = json.dumps({"task_id": task_id, "payload": {"action": "test"}})
    pipe.lpush(queue.queue, raw)
    pipe.hset(f"qtask:task:{task_id}", mapping={"task_id": task_id, "_queue": queue.base})
    pipe.zadd(queue.history.idx_key, {task_id: now + index / 100000})
    pipe.set(f"{prefix}{index}", "preserve")
pipe.lpush(custom, json.dumps({"task_id": "scale-unindexed", "payload": {}}))
pipe.hset("qtask:task:scale-unindexed", mapping={"task_id": "scale-unindexed", "_queue": queue.base})
pipe.execute()
started = time.perf_counter()
result = admin.delete_queue(queue.base)
elapsed = time.perf_counter() - started
assert result["history_records"] == 10001, result
assert not r.exists(queue.queue, custom, queue.processing_registry_key, queue.history.idx_key)
assert not any(r.scan_iter("qtask:task:scale-*"))
noise = list(r.scan_iter(f"{prefix}*"))
assert len(noise) == 10000
for start in range(0, len(noise), 500):
    r.delete(*noise[start:start + 500])
print(json.dumps({"messages": 10001, "unrelated_keys_preserved": len(noise),
                  "elapsed_ms": round(elapsed * 1000, 2), "result": result}, indent=2))
r.close()
