"""注册表是队列和 Worker 自动发现的唯一来源。"""

import json
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest
import redis

from qtask_list import FrozenClock, QueueAdmin, SmartQueue, TaskSpec, Worker


def test_discovery_reads_only_registries_without_scanning(isolated_redis, monkeypatch):
    r = isolated_redis
    r.zadd("qtask:queues", {"registered:z": 1, "registered:a": 2})
    r.zadd("qtask:workers:registered:a", {"online": 1})
    r.set("registered:a:worker:online", "1", ex=60)
    raw = json.dumps({"task_id": "unregistered", "payload": "{}"})
    r.lpush("unregistered", raw)
    r.lpush("unregistered:processing:worker", raw)
    r.set("unregistered:worker:worker", "1", ex=60)
    r.zadd("qtask:hist:unregistered", {"unregistered": 1})
    r.zadd("qtask:workers:unregistered", {"worker": 1})

    def unexpected_scan(*args, **kwargs):
        pytest.fail("自动发现不应扫描 Redis 键空间")

    monkeypatch.setattr(r, "scan_iter", unexpected_scan)
    monkeypatch.setattr(r, "scan", unexpected_scan)
    admin = QueueAdmin(redis_client=r)
    assert admin.queue_names() == ["registered:a", "registered:z"]
    assert [row["name"] for row in admin.list_queues()] == ["registered:a", "registered:z"]
    assert [(row["queue"], row["worker_id"]) for row in admin.list_workers()] == [
        ("registered:a", "online"),
    ]


def test_existing_admin_immediately_observes_registry_changes(isolated_redis):
    r = isolated_redis
    admin = QueueAdmin(redis_client=r)
    assert admin.queue_names() == []
    with r.client() as external:
        external.zadd("qtask:queues", {"jobs": 1})
        assert admin.queue_names() == ["jobs"]
        external.lpush("jobs", json.dumps({"task_id": "remaining", "payload": "{}"}))
        external.zadd("qtask:hist:jobs", {"remaining": 1})
        external.zrem("qtask:queues", "jobs")
        assert admin.queue_names() == []
    SmartQueue(redis_client=r, queue_name="new-jobs").enqueue(TaskSpec("x", {}))
    assert admin.queue_names() == ["new-jobs"]
    r.flushdb()
    assert admin.list_queues() == []
    assert admin.list_workers() == []


def test_registered_workers_report_stale_processing_and_remove_empty_entries(isolated_redis):
    r = isolated_redis
    r.zadd("qtask:queues", {"jobs": 1})
    r.zadd("qtask:workers:jobs", {"online": 1, "stale": 1, "finished": 1})
    r.set("jobs:worker:online", "1", ex=60)
    r.lpush("jobs:processing:stale", "pending task")
    r.set("jobs:worker:unregistered", "1", ex=60)
    admin = QueueAdmin(redis_client=r)
    workers = admin.list_workers()
    assert [(row["worker_id"], row["active"], row["processing"]) for row in workers] == [
        ("online", True, 0), ("stale", False, 1),
    ]
    assert r.zscore("qtask:workers:jobs", "finished") is None
    r.delete("jobs:processing:stale")
    assert [row["worker_id"] for row in admin.list_workers("jobs")] == ["online"]
    assert r.zscore("qtask:workers:jobs", "stale") is None


def test_idle_worker_heartbeat_registers_queue_and_recovers_after_external_clear(isolated_redis):
    r = isolated_redis
    worker = Worker("redis://isolated/0", "jobs", redis_client=r,
                    worker_id="idle", clock=FrozenClock(1000))
    admin = QueueAdmin(redis_client=r)
    worker._refresh_heartbeat()
    assert admin.queue_names() == ["jobs"]
    assert r.zscore("qtask:queues", "jobs") == 1000
    assert r.zscore("qtask:workers:jobs", "idle") == 1000
    assert 0 < r.ttl("jobs:worker:idle") <= worker.heartbeat_ttl
    assert admin.queue_stats("jobs")["active_workers"] == 1
    assert r.exists("jobs", "qtask:hist:jobs") == 0
    r.flushdb()
    assert admin.list_queues() == []
    worker._refresh_heartbeat()
    assert admin.queue_names() == ["jobs"]
    assert admin.list_workers()[0]["worker_id"] == "idle"


def test_worker_pruning_rechecks_concurrent_heartbeat(isolated_redis, monkeypatch):
    r = isolated_redis
    worker = Worker("redis://isolated/0", "jobs", redis_client=r, worker_id="renewing")
    worker._refresh_heartbeat()
    r.delete(worker._heartbeat_key)
    original_llen = r.llen
    renewed = False

    def renew_before_processing_read(key):
        nonlocal renewed
        if key == worker.queue.processing and not renewed:
            renewed = True
            worker._refresh_heartbeat()
        return original_llen(key)

    monkeypatch.setattr(r, "llen", renew_before_processing_read)
    admin = QueueAdmin(redis_client=r)
    admin.list_workers()
    assert renewed
    assert r.zscore(worker._worker_registry_key, worker.worker_id) is not None
    assert admin.list_workers()[0]["active"] is True


@pytest.mark.parametrize("pending", [False, True])
def test_worker_exit_keeps_registry_until_processing_is_recovered(isolated_redis, pending):
    r = isolated_redis
    worker = Worker("redis://isolated/0", "jobs", redis_client=r, worker_id="leaving")
    worker._refresh_heartbeat()
    if pending:
        r.lpush(worker.queue.processing, json.dumps({"task_id": "orphan", "payload": "{}"}))
    worker._cleanup_worker_state()
    admin = QueueAdmin(redis_client=r)
    assert r.exists(worker._heartbeat_key) == 0
    assert admin.queue_names() == ["jobs"]
    if pending:
        assert admin.list_workers()[0]["active"] is False
        assert admin.queue_stats("jobs")["processing"] == 1
        assert admin.recover("jobs", worker_id="leaving")["recovered"] == 1
        assert r.llen("jobs") == 1
    assert admin.list_workers() == []
    assert r.zscore("qtask:workers:jobs", "leaving") is None


@pytest.mark.parametrize("conflict", ["queues", "workers", "heartbeat"])
def test_worker_registration_type_error_leaves_no_partial_state(isolated_redis, conflict):
    r = isolated_redis
    worker = Worker("redis://isolated/0", "jobs", redis_client=r, worker_id="new")
    key = {"queues": "qtask:queues", "workers": "qtask:workers:jobs",
           "heartbeat": worker._heartbeat_key}[conflict]
    if conflict == "heartbeat":
        r.lpush(key, "wrong type")
    else:
        r.set(key, "wrong type")
    with pytest.raises(redis.ResponseError):
        worker._refresh_heartbeat()
    assert r.keys("*") == [key]


def test_concurrent_worker_registration_rejects_overlapping_queue_names(isolated_redis):
    r = isolated_redis
    workers = [Worker("redis://isolated/0", name, redis_client=r, worker_id="new")
               for name in ("jobs", "jobs:delay")]
    ready = Barrier(2)

    def register(worker):
        ready.wait(timeout=5)
        try:
            worker._refresh_heartbeat()
            return True
        except ValueError as error:
            assert "namespace conflict" in str(error)
            return False

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(register, workers))
    assert sorted(results) == [False, True]
    winner = workers[results.index(True)]
    loser = workers[results.index(False)]
    assert r.zrange("qtask:queues", 0, -1) == [winner.queue.base]
    assert r.exists(loser._heartbeat_key, loser._worker_registry_key) == 0
