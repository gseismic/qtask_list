"""在私有 Redis 中复核任务状态机的关键不变式。"""

import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from _isolated_redis import isolated_redis  # noqa: E402
from qtask_list import SmartQueue, TaskSpec  # noqa: E402
from qtask_list.archiver import ArchiveManager  # noqa: E402


def main() -> None:
    with isolated_redis() as (url, root):
        queue = SmartQueue(url, "live", namespace="review")
        task_id = queue.enqueue(TaskSpec("work", {"value": 1})).task_id
        queue.r.zadd(queue.history.idx_key, {task_id: time.time() - 2 * 86400})
        manager = ArchiveManager(url, db_dir=str(root / "archives"))
        assert manager.archive_to_sqlite(queue.base, days_ago=1) == 0
        assert queue.size() == 1 and queue.history.get(task_id) is not None

        retry = SmartQueue(url, "retry", namespace="review", max_attempts=3, retry_backoff_base=0)
        retry_id = retry.enqueue(TaskSpec("unstable", {})).task_id
        attempts = 0
        while retry.dlq_size() == 0:
            claim = retry.pop_claim_no_wait()
            if claim is None:
                retry.move_delay()
                continue
            attempts += 1
            assert retry.fail(claim.raw_message, "模拟失败")
        assert attempts == 3 and retry.history.get(retry_id)["outcome"] == "failed"

        replay = retry.replay_task(retry_id)
        claim = retry.pop_claim_no_wait()
        assert replay.accepted and claim is not None and replay.task_id != retry_id
        assert "_retry" not in claim.payload
        assert retry.history.get(replay.task_id)["replay_of"] == retry_id
        assert retry.ack(claim.raw_message)

        try:
            retry.push([{"action": "invalid"}])
        except (TypeError, ValueError):
            pass
        else:
            raise AssertionError("非法 payload 未被拒绝")
        print(json.dumps({"live_history_retained": True, "attempts": attempts,
                          "replay_uses_new_id": True, "invalid_payload_rejected": True},
                         ensure_ascii=False))


if __name__ == "__main__":
    main()
