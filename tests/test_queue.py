import pytest
import redis
import json
import time
from qtask_list import SmartQueue


@pytest.fixture
def redis_url():
    return "redis://localhost:6379/0"


@pytest.fixture
def r(redis_url):
    client = redis.from_url(redis_url, decode_responses=True)
    yield client
    for k in client.scan_iter("testns:*"):
        client.delete(k)
    for k in client.scan_iter("qtask:task:*"):
        client.delete(k)
    for k in client.scan_iter("qtask:hist:testns:*"):
        client.delete(k)


class TestSmartQueue:
    def test_push_and_pop(self, redis_url, r):
        q = SmartQueue(redis_url, "test", namespace="testns")
        task_id = q.push({"action": "test", "data": "hello"})
        
        assert task_id is not None
        assert r.llen("testns:test") == 1
        
        payload, raw = q.pop()
        assert payload["action"] == "test"
        assert payload["data"] == "hello"
        
        q.ack(raw)
        assert r.llen("testns:test:processing") == 0

    def test_batch_push(self, redis_url, r):
        q = SmartQueue(redis_url, "batch_test", namespace="testns")
        task_ids = q.push_batch([
            {"action": "test", "i": 1},
            {"action": "test", "i": 2},
            {"action": "test", "i": 3},
        ])
        
        assert len(task_ids) == 3
        assert r.llen("testns:batch_test") == 3

    def test_fail_and_retry(self, redis_url, r):
        # V2 自动重试统一进入 delay ZSET；base=0 只表示立即到期，不再写入 retry List。
        q = SmartQueue(redis_url, "retry_test", namespace="testns", max_retry=2, retry_backoff_base=0)
        q.push({"action": "test"})

        payload, raw = q.pop()
        assert payload["action"] == "test"
        q.fail(raw, "test error")

        # 失败后进入 retry_wait（delay ZSET），运行元数据不污染业务 payload。
        assert r.zcard("testns:retry_test:delay") == 1
        assert r.llen("testns:retry_test:retry") == 0
        retry_msg = r.zrange("testns:retry_test:delay", 0, -1)[0]
        retry_envelope = json.loads(retry_msg)
        assert retry_envelope["attempt"] == 1
        assert retry_envelope["delay_reason"] == "retry"
        assert "_retry" not in retry_envelope["payload"]["data"]
        assert q.get_stats()["retry_wait"] == 1

        # move_delay 移回主队列
        q.move_delay()
        assert r.llen("testns:retry_test") == 1

    def test_dlq(self, redis_url, r):
        q = SmartQueue(redis_url, "dlq_test", namespace="testns", max_retry=1)
        q.push({"action": "test"})

        # 第一次失败，进入 retry
        payload, raw = q.pop()
        assert payload["action"] == "test"
        q.fail(raw, "test error")

        assert r.llen("testns:dlq_test:dlq") == 1

    def test_delay(self, redis_url, r):
        q = SmartQueue(redis_url, "delay_test", namespace="testns")
        # 创建一个延迟任务
        import time
        now = time.time()
        msg = '{"task_id":"test","payload":"{}"}'
        r.zadd("testns:delay_test:delay", {msg: now - 1})  # 已过期
        
        assert r.zcard("testns:delay_test:delay") == 1
        
        # move_delay 移到主队列
        count = q.move_delay()
        assert count == 1
        assert r.llen("testns:delay_test") == 1

    def test_ttl(self, redis_url, r):
        q = SmartQueue(redis_url, "ttl_test", namespace="testns", ttl_days=15)
        q.push({"action": "test"})
        
        # V2 历史索引持久化，终态且无 operational message 后才清理。
        ttl = r.ttl("qtask:hist:testns:ttl_test")
        assert ttl == -1

    def test_history(self, redis_url, r):
        q = SmartQueue(redis_url, "history_test", namespace="testns")
        q.push({"action": "test_action", "data": "hello"})

        history = q.history.list(limit=10)
        assert len(history) >= 1
        assert history[0]["action"] == "test_action"
        assert history[0]["status"] == "pending"

    def test_clean_expired(self, redis_url, r):
        q = SmartQueue(redis_url, "clean_test", namespace="testns")
        done_id = q.push({"action": "done"})
        live_id = q.push({"action": "live"})
        _payload, raw = q.pop(timeout=1)
        assert q.ack(raw) is True
        old = time.time() - 2 * 86400
        r.zadd(q.history.idx_key, {live_id: old, done_id: old})
        r.hset(f"qtask:task:{done_id}", "finished_at", old)

        count = q.history.clean_expired(ttl_seconds=0)
        assert count == 1

        history = q.history.list(limit=10)
        assert [item["task_id"] for item in history] == [live_id]

    def test_ack_non_processing_message_does_not_create_history(self, redis_url, r):
        q = SmartQueue(redis_url, "ack_missing", namespace="testns")
        fake_msg = '{"task_id":"missing","payload":"{}"}'

        assert q.ack(fake_msg) is False
        assert r.exists("qtask:task:missing") == 0

    def test_fail_non_processing_message_does_not_retry_or_create_history(self, redis_url, r):
        q = SmartQueue(redis_url, "fail_missing", namespace="testns")
        fake_msg = '{"task_id":"missing","payload":"{}"}'

        assert q.fail(fake_msg, "missing") is False
        assert r.llen("testns:fail_missing:retry") == 0
        assert r.llen("testns:fail_missing:dlq") == 0
        assert r.exists("qtask:task:missing") == 0

    def test_pop_moves_malformed_message_to_dlq(self, redis_url, r):
        q = SmartQueue(redis_url, "poison", namespace="testns")
        r.lpush("testns:poison", "not-json")

        payload, raw = q.pop(timeout=1)

        assert payload is None
        assert raw is None
        assert r.llen("testns:poison:processing") == 0
        assert r.lrange("testns:poison:dlq", 0, -1) == ["not-json"]

    def test_pop_moves_message_without_task_id_to_dlq(self, redis_url, r):
        q = SmartQueue(redis_url, "missing_task_id", namespace="testns")
        raw_msg = json.dumps({"payload": json.dumps({"action": "test"})})
        r.lpush("testns:missing_task_id", raw_msg)

        payload, raw = q.pop(timeout=1)

        assert payload is None
        assert raw is None
        assert r.llen("testns:missing_task_id:processing") == 0
        assert r.lrange("testns:missing_task_id:dlq", 0, -1) == [raw_msg]

    def test_recover_stale_processing_skips_active_worker(self, redis_url, r):
        active_q = SmartQueue(
            redis_url,
            "worker_recovery",
            namespace="testns",
            processing_key="testns:worker_recovery:processing:active",
        )
        recovery_q = SmartQueue(
            redis_url,
            "worker_recovery",
            namespace="testns",
            processing_key="testns:worker_recovery:processing:recovery",
        )
        active_q.push({"action": "test"})
        active_q.pop(timeout=1)
        r.set("testns:worker_recovery:worker:active", "1", ex=60)

        recovered = recovery_q.recover_stale_processing("testns:worker_recovery:worker:")

        assert recovered == 0
        assert r.llen("testns:worker_recovery") == 0
        assert r.llen("testns:worker_recovery:processing:active") == 1

    def test_recover_stale_processing_recovers_missing_heartbeat(self, redis_url, r):
        stale_q = SmartQueue(
            redis_url,
            "stale_recovery",
            namespace="testns",
            processing_key="testns:stale_recovery:processing:stale",
        )
        recovery_q = SmartQueue(
            redis_url,
            "stale_recovery",
            namespace="testns",
            processing_key="testns:stale_recovery:processing:recovery",
        )
        stale_q.push({"action": "test"})
        stale_q.pop(timeout=1)

        recovered = recovery_q.recover_stale_processing(
            "testns:stale_recovery:worker:", include_unmanaged=True
        )

        assert recovered == 1
        assert r.llen("testns:stale_recovery") == 1
        assert r.llen("testns:stale_recovery:processing:stale") == 0
