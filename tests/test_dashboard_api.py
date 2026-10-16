import json
import time
from datetime import datetime, timedelta, timezone

import pytest
import redis
from fastapi.testclient import TestClient

from qtask_list import SmartQueue, TaskSpec
from qtask_list.admin import QueueAdmin
from qtask_list.dashboard import main as dashboard_main
from qtask_list.dashboard.main import app


@pytest.fixture(autouse=True)
def dashboard_auth_env(monkeypatch):
    for name in [
        "QTASK_DASHBOARD_AUTH",
        "QTASK_DASHBOARD_USER",
        "QTASK_DASHBOARD_PASSWORD",
        "QTASK_DASHBOARD_SECRET",
        "QTASK_DASHBOARD_SESSION_TTL",
        "QTASK_DASHBOARD_SECURE_COOKIE",
    ]:
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def r():
    client = redis.from_url("redis://localhost:6379/0", decode_responses=True)
    cleanup(client)
    yield client
    cleanup(client)


def cleanup(client):
    for hist_key in client.scan_iter("qtask:hist:qtask_dash_test:*"):
        for task_id in client.zrange(hist_key, 0, -1):
            client.delete(f"qtask:task:{task_id}")
        client.delete(hist_key)
    for task_id in ["retry-with-history", "retry-missing-history"]:
        client.delete(f"qtask:task:{task_id}")
    for key in client.scan_iter("qtask_dash_test:*"):
        client.delete(key)
    for pattern in ("qtask:metrics:qtask_dash_test:*", "qtask:deadline:qtask_dash_test:*",
                    "qtask:workers:qtask_dash_test:*"):
        for key in client.scan_iter(pattern):
            client.delete(key)
    for queue in client.zrange("qtask:queues", 0, -1):
        if queue.startswith("qtask_dash_test:"):
            client.zrem("qtask:queues", queue)
    dashboard_main.admin._legacy_queue_cache.clear()
    dashboard_main.admin._legacy_queue_discovered_at = float("-inf")
    dashboard_main.admin._legacy_worker_cache.clear()
    dashboard_main.admin._legacy_worker_discovered_at = float("-inf")


def make_msg(task_id: str, payload: dict | None = None) -> str:
    return json.dumps({"task_id": task_id, "payload": json.dumps(payload or {})})


def seed_expired_tasks(client, queue: str, count: int, prefix: str, status: str = "pending") -> list[str]:
    """构造带 operational message 的 V2 deadline_missed 样本。"""
    now = time.time()
    task_ids = []
    pipe = client.pipeline()
    for index in range(count):
        task_id = f"{prefix}-{index:03d}"
        task_ids.append(task_id)
        payload = {"action": "expired_action", "index": index}
        descriptor = {"kind": "inline", "data": payload}
        message = {
            "version": 2,
            "task_id": task_id,
            "action": "expired_action",
            "attempt": 0,
            "max_attempts": 3,
            "created_at": now - index,
            "available_at": now,
            "start_deadline_at": now - 10,
            "payload": descriptor,
        }
        pipe.hset(
            f"qtask:task:{task_id}",
            mapping={
                "task_id": task_id,
                "status": status,
                "outcome": "",
                "action": "expired_action",
                "expires_at": str(now - 10),
                "start_deadline_at": str(now - 10),
                "created_at": str(now - index),
                "updated_at": str(now),
                "operational_message": "1",
                "payload_kind": "inline",
                "payload_descriptor": json.dumps(descriptor),
                "max_attempts": "3",
                "_queue": queue,
            },
        )
        pipe.lpush(queue, json.dumps(message))
        pipe.zadd(f"qtask:hist:{queue}", {task_id: now - index})
    pipe.execute()
    return task_ids


def message_ids(client, key: str) -> list[str]:
    return [json.loads(raw)["task_id"] for raw in client.lrange(key, 0, -1)]


def test_dashboard_lists_queues_and_state_tasks(client, r):
    queue = "qtask_dash_test:day-kline:fetch"
    r.lpush(queue, make_msg("ready-1", {"action": "scrape_day_kline", "symbol": "sh600000"}))
    r.lpush(f"{queue}:dlq", make_msg("dlq-1", {"action": "scrape_day_kline"}))

    queues = client.get("/api/queues")
    assert queues.status_code == 200
    data = queues.json()
    item = next(q for q in data if q["name"] == queue)
    assert item["queue"] == 1
    assert item["dlq"] == 1

    ready = client.get(f"/api/queue/{queue}/tasks", params={"state": "ready"})
    assert ready.status_code == 200
    assert ready.json()["tasks"][0]["task_id"] == "ready-1"

    dlq = client.get(f"/api/queue/{queue}/tasks", params={"state": "dlq"})
    assert dlq.status_code == 200
    assert dlq.json()["tasks"][0]["task_id"] == "dlq-1"


def test_dashboard_ignores_foreign_redis_lists(client, r):
    foreign_key = "foreign:list:not-qtask"
    r.delete(foreign_key)
    r.lpush(foreign_key, "not a qtask message")

    try:
        response = client.get("/api/queues")
        assert response.status_code == 200
        assert not any(item["name"] == foreign_key for item in response.json())
    finally:
        r.delete(foreign_key)


def test_dashboard_requeues_single_dlq_task(client, r):
    queue = "qtask_dash_test:sector-em:fetch"
    r.hset("qtask:task:dlq-move", mapping={"task_id": "dlq-move", "status": "failed"})
    r.zadd(f"qtask:hist:{queue}", {"dlq-move": 1})
    r.lpush(f"{queue}:dlq", make_msg("dlq-keep"))
    r.lpush(f"{queue}:dlq", make_msg("dlq-move"))

    response = client.post(
        "/api/task/dlq-move/requeue",
        json={"queue": queue, "from_state": "dlq"},
    )

    assert response.status_code == 200
    assert response.json()["moved"] == 1
    assert message_ids(r, queue) == ["dlq-move"]
    assert message_ids(r, f"{queue}:dlq") == ["dlq-keep"]
    assert r.hget("qtask:task:dlq-move", "status") == "pending"


def test_dashboard_bulk_retry_and_requeue_dlq(client, r):
    queue = "qtask_dash_test:sector-sina:fetch"
    r.lpush(f"{queue}:retry", make_msg("retry-1"))
    r.lpush(f"{queue}:dlq", make_msg("dlq-1"))

    retry = client.post(f"/api/queue/{queue}/retry")
    dlq = client.post(
        f"/api/queue/{queue}/requeue-dlq",
        json={"task_id": None, "confirm_bulk": True},
    )

    assert retry.status_code == 200
    assert retry.json()["moved"] == 1
    assert dlq.status_code == 200
    assert dlq.json()["moved"] == 1
    assert r.llen(queue) == 2
    assert r.llen(f"{queue}:retry") == 0
    assert r.llen(f"{queue}:dlq") == 0


def test_dashboard_bulk_retry_updates_existing_history_without_orphans(client, r):
    queue = "qtask_dash_test:sector-sina:store"
    r.hset(
        "qtask:task:retry-with-history",
        mapping={"task_id": "retry-with-history", "status": "retry"},
    )
    r.expire("qtask:task:retry-with-history", 60)
    r.zadd(f"qtask:hist:{queue}", {"retry-with-history": 1})
    r.lpush(f"{queue}:retry", make_msg("retry-with-history"))
    r.lpush(f"{queue}:retry", make_msg("retry-missing-history"))

    response = client.post(f"/api/queue/{queue}/retry")

    assert response.status_code == 200
    assert response.json()["moved"] == 2
    assert r.hget("qtask:task:retry-with-history", "status") == "pending"
    assert r.ttl("qtask:task:retry-with-history") == -1
    assert r.zscore(f"qtask:hist:{queue}", "retry-with-history") is not None
    assert r.exists("qtask:task:retry-missing-history") == 0


def test_dashboard_recover_skips_active_worker(client, r):
    queue = "qtask_dash_test:day-kline:store"
    r.lpush(f"{queue}:processing", make_msg("legacy"))
    r.lpush(f"{queue}:processing:active", make_msg("active"))
    r.lpush(f"{queue}:processing:stale", make_msg("stale"))
    r.set(f"{queue}:worker:active", "1", ex=60)

    response = client.post(f"/api/queue/{queue}/recover", json={"include_active": False})

    assert response.status_code == 200
    assert response.json() == {"recovered": 1, "skipped_active": 1, "skipped_legacy": 1}
    assert r.llen(queue) == 1
    assert r.llen(f"{queue}:processing") == 1
    assert r.llen(f"{queue}:processing:active") == 1


def test_dashboard_recover_one_worker_keeps_other_workers_and_legacy(client, r):
    """行级恢复只影响用户确认的 Worker，不能顺带恢复同队列其他孤儿。"""
    queue = "qtask_dash_test:precise-recovery"
    for worker in ("selected", "other", "online"):
        r.lpush(f"{queue}:processing:{worker}", make_msg(f"precise-{worker}"))
    r.lpush(f"{queue}:processing", make_msg("precise-legacy"))
    r.set(f"{queue}:worker:online", "1", ex=60)

    response = client.post(f"/api/queue/{queue}/recover", json={"worker_id": "selected"})

    assert response.status_code == 200
    assert response.json() == {"recovered": 1, "skipped_active": 0, "skipped_legacy": 0}
    assert r.llen(queue) == 1
    assert r.llen(f"{queue}:processing:selected") == 0
    for suffix in ("processing", "processing:other", "processing:online"):
        assert r.llen(f"{queue}:{suffix}") == 1


def test_dashboard_precise_recovery_rechecks_resumed_heartbeat(client, r, monkeypatch):
    """失联 Worker 在点击恢复后恢复心跳时，原子迁移仍必须保护它的任务。"""
    queue = "qtask_dash_test:precise-heartbeat-race"
    r.lpush(f"{queue}:processing:selected", make_msg("precise-racing"))
    original = dashboard_main.admin._drain_list_to_ready

    def resume_before_move(source, destination, **kwargs):
        r.set(f"{queue}:worker:selected", "1", ex=60)
        return original(source, destination, **kwargs)

    monkeypatch.setattr(dashboard_main.admin, "_drain_list_to_ready", resume_before_move)
    response = client.post(f"/api/queue/{queue}/recover", json={"worker_id": "selected"})

    assert response.status_code == 200
    assert response.json()["recovered"] == 0
    assert r.llen(f"{queue}:processing:selected") == 1
    assert r.llen(queue) == 0


@pytest.mark.parametrize("worker_id", ["", "bad:worker", "*", "w?", "w[0]", " w", "w\n"])
def test_dashboard_precise_recovery_rejects_ambiguous_worker_id(client, r, worker_id):
    """拒绝可扩大 key 范围或无法明确确认的 Worker 标识。"""
    response = client.post(
        "/api/queue/qtask_dash_test:invalid-worker/recover", json={"worker_id": worker_id}
    )
    assert response.status_code == 422


def test_dashboard_precise_recovery_protects_active_worker(client, r):
    """指定 Worker 不代表允许抢回在线任务，强制恢复仍要求二次确认。"""
    queue = "qtask_dash_test:precise-active"
    r.lpush(f"{queue}:processing:online", make_msg("precise-online"))
    r.set(f"{queue}:worker:online", "1", ex=60)
    response = client.post(f"/api/queue/{queue}/recover", json={"worker_id": "online"})
    assert response.status_code == 200
    assert response.json()["skipped_active"] == 1
    response = client.post(
        f"/api/queue/{queue}/recover", json={"worker_id": "online", "include_active": True}
    )
    assert response.status_code == 400
    assert r.llen(f"{queue}:processing:online") == 1


def test_dashboard_queue_action_filter_matches_global_semantics(client, r):
    """从全局切到单队列时，精确 action 筛选仍然生效。"""
    queue = SmartQueue(redis_client=r, namespace="qtask_dash_test", queue_name="action-filter")
    accepted = queue.enqueue(TaskSpec(action="fetch_quote", payload={"symbol": "AAPL"}))
    queue.enqueue(TaskSpec(action="fetch_news", payload={"symbol": "AAPL"}))
    response = client.get(
        f"/api/queue/{queue.base}/tasks", params={"state": "ready", "action": "fetch_quote"}
    )
    assert response.status_code == 200
    assert [task["task_id"] for task in response.json()["tasks"]] == [accepted.task_id]


def test_dashboard_deletes_task_from_queue_and_history(client, r):
    queue = "qtask_dash_test:fin-sheet-em:fetch"
    r.lpush(queue, make_msg("delete-me", {"action": "scrape_fin_sheet"}))
    r.hset("qtask:task:delete-me", mapping={"task_id": "delete-me", "status": "pending"})
    r.zadd(f"qtask:hist:{queue}", {"delete-me": 1})

    response = client.delete("/api/task/delete-me", params={"queue": queue})

    assert response.status_code == 200
    assert response.json()["queue_messages"] == 1
    assert r.llen(queue) == 0
    assert r.exists("qtask:task:delete-me") == 0
    assert r.zscore(f"qtask:hist:{queue}", "delete-me") is None


def test_dashboard_push_task(client, r):
    queue = "qtask_dash_test:day-kline-xq:fetch"
    response = client.post(
        f"/api/queue/{queue}/tasks",
        json={"payload": {"action": "scrape_day_kline", "symbol": "SZ000001"}},
    )

    assert response.status_code == 200
    task_id = response.json()["task_id"]
    assert r.llen(queue) == 1
    assert r.exists(f"qtask:task:{task_id}") == 1


def test_dashboard_retry_wait_stats_are_reported_from_injected_redis(client, r):
    """V2 retry_wait 位于 delay ZSET，Dashboard 统计必须与注入客户端同库。"""
    from qtask_list import SmartQueue

    queue = "qtask_dash_test:retry-wait:fetch"
    q = SmartQueue(
        redis_client=r,
        queue_name="retry-wait:fetch",
        namespace="qtask_dash_test",
        retry_backoff_base=0,
    )
    q.push({"action": "retryable"})
    _payload, raw = q.pop(timeout=1)
    assert q.fail(raw, "transient") is True

    response = client.get(f"/api/queue/{queue}")
    assert response.status_code == 200
    stats = response.json()["stats"]
    assert stats["retry_wait"] == 1
    assert stats["retry"] == 0
    assert stats["delay"] == 1


def test_dashboard_push_invalid_task_returns_4xx(client, r):
    """缺少 action 的用户输入应返回 422，而不是未处理异常导致 500。"""
    queue = "qtask_dash_test:invalid:fetch"
    response = client.post(f"/api/queue/{queue}/tasks", json={"payload": {}})
    assert response.status_code == 422
    assert "action" in response.json()["detail"]


def test_dashboard_push_naive_datetime_returns_4xx(client, r):
    """naive datetime 违反 TaskSpec 契约时由 REST 层返回可修复错误。"""
    queue = "qtask_dash_test:naive-date:fetch"
    response = client.post(
        f"/api/queue/{queue}/tasks",
        json={
            "payload": {"action": "fetch"},
            "scheduled_for": "2026-09-20T10:00:00",
        },
    )
    assert response.status_code == 422
    assert "timezone-aware" in response.json()["detail"]


def test_dashboard_auth_disabled_by_default(client):
    response = client.get("/api/auth")
    assert response.status_code == 200
    assert response.json()["enabled"] is False

    queues = client.get("/api/queues")
    assert queues.status_code == 200


def test_dashboard_auth_requires_login(monkeypatch):
    monkeypatch.setenv("QTASK_DASHBOARD_USER", "ops")
    monkeypatch.setenv("QTASK_DASHBOARD_PASSWORD", "secret")
    monkeypatch.setenv("QTASK_DASHBOARD_SECRET", "test-secret")
    client = TestClient(app)

    index = client.get("/", follow_redirects=False)
    assert index.status_code in {302, 307}
    assert index.headers["location"] == "/login"

    queues = client.get("/api/queues")
    assert queues.status_code == 401

    failed = client.post("/api/login", json={"username": "ops", "password": "bad"})
    assert failed.status_code == 401
    assert "qtask_dashboard_session" not in client.cookies

    logged_in = client.post("/api/login", json={"username": "ops", "password": "secret"})
    assert logged_in.status_code == 200
    assert logged_in.json()["authenticated"] is True
    assert "qtask_dashboard_session" in client.cookies

    auth = client.get("/api/auth")
    assert auth.status_code == 200
    assert auth.json()["authenticated"] is True

    queues = client.get("/api/queues")
    assert queues.status_code == 200

    logout = client.post("/api/logout")
    assert logout.status_code == 200
    assert "qtask_dashboard_session" not in client.cookies

    queues = client.get("/api/queues")
    assert queues.status_code == 401


def test_dashboard_auth_guards_spa_fallback(monkeypatch):
    """认证开启时，SPA 客户端路由与静态资源回退也必须登录后才能访问。"""
    monkeypatch.setenv("QTASK_DASHBOARD_USER", "ops")
    monkeypatch.setenv("QTASK_DASHBOARD_PASSWORD", "secret")
    monkeypatch.setenv("QTASK_DASHBOARD_SECRET", "test-secret")
    client = TestClient(app)

    spa_route = client.get("/queues", follow_redirects=False)
    assert spa_route.status_code in {302, 307}
    assert spa_route.headers["location"] == "/login"

    logged_in = client.post("/api/login", json={"username": "ops", "password": "secret"})
    assert logged_in.status_code == 200
    spa_route = client.get("/queues", follow_redirects=False)
    assert spa_route.status_code == 200


def test_dashboard_queue_stats_includes_completed_failed(client, r):
    queue = "qtask_dash_test:perf-sina:fetch"
    r.lpush(queue, make_msg("perf-1", {"action": "scrape_perf"}))

    r.hset("qtask:task:hist-ok", mapping={
        "task_id": "hist-ok", "status": "completed", "action": "scrape_perf",
    })
    r.hset("qtask:task:hist-fail", mapping={
        "task_id": "hist-fail", "status": "failed", "action": "scrape_perf",
    })
    r.hset("qtask:task:hist-pending", mapping={
        "task_id": "hist-pending", "status": "pending", "action": "scrape_perf",
    })
    r.zadd(f"qtask:hist:{queue}", {"hist-ok": 1, "hist-fail": 2, "hist-pending": 3})

    # 旧历史需显式回填后，累计计数才能用于总览。
    dashboard_main.admin.rebuild_observation_indexes(queue)
    response = client.get(f"/api/queue/{queue}")
    assert response.status_code == 200
    stats = response.json()["stats"]
    assert stats["history"] == 3
    assert stats["completed"] == 1
    assert stats["failed"] == 1
    # pending should not be counted as completed or failed
    assert stats["completed"] + stats["failed"] <= stats["history"]


def test_dashboard_queue_stats_supports_legacy_string_history(client, r):
    queue = "qtask_dash_test:legacy-string:fetch"
    r.set(
        "qtask:task:legacy-string-task",
        json.dumps({
            "task_id": "legacy-string-task",
            "status": "completed",
            "action": "legacy_action",
        }),
    )
    r.zadd(f"qtask:hist:{queue}", {"legacy-string-task": time.time()})

    dashboard_main.admin.rebuild_observation_indexes(queue)
    response = client.get(f"/api/queue/{queue}")

    assert response.status_code == 200
    stats = response.json()["stats"]
    assert stats["history"] == 1
    assert stats["completed"] == 1
    assert stats["failed"] == 0


def test_dashboard_delete_queue(client, r):
    queue = "qtask_dash_test:delete-me:fetch"
    r.lpush(queue, make_msg("del-1"))
    r.lpush(f"{queue}:dlq", make_msg("del-dlq"))
    r.hset("qtask:task:del-hist", mapping={
        "task_id": "del-hist", "status": "completed",
    })
    r.zadd(f"qtask:hist:{queue}", {"del-hist": 1})
    r.zadd("qtask:queues", {queue: time.time()})

    queues_before = client.get("/api/queues").json()
    assert any(q["name"] == queue for q in queues_before)

    response = client.delete(f"/api/queue/{queue}", params={"confirm": "true"})
    assert response.status_code == 200
    result = response.json()
    assert result["history_records"] == 1
    assert result["deleted_keys"] > 0

    queues_after = client.get("/api/queues").json()
    assert not any(q["name"] == queue for q in queues_after)
    assert r.exists(queue) == 0
    assert r.exists(f"{queue}:dlq") == 0
    assert r.exists("qtask:task:del-hist") == 0


def test_dashboard_supplements_action_from_history_for_large_payload(client, r):
    queue = "qtask_dash_test:large-payload:fetch"
    large_payload = {"_large": True, "key": "abc123"}
    msg = json.dumps({"task_id": "large-1", "payload": json.dumps(large_payload)})
    r.lpush(queue, msg)
    r.hset("qtask:task:large-1", mapping={
        "task_id": "large-1", "status": "pending", "action": "scrape_day_kline",
    })
    r.zadd(f"qtask:hist:{queue}", {"large-1": 1})

    response = client.get(f"/api/queue/{queue}/tasks", params={"state": "ready"})
    assert response.status_code == 200
    task = response.json()["tasks"][0]
    assert task["task_id"] == "large-1"
    assert task["action"] == "scrape_day_kline"


def test_dashboard_resolves_compressed_payload(client, r):
    import base64
    import zstandard

    queue = "qtask_dash_test:compressed:fetch"
    original = {"action": "scrape_day_kline", "symbol": "sh600000"}
    data = json.dumps(original).encode()
    compressed = zstandard.ZstdCompressor().compress(data)
    payload = {"_compressed": True, "data": base64.b64encode(compressed).decode()}
    msg = json.dumps({"task_id": "comp-1", "payload": json.dumps(payload)})
    r.lpush(queue, msg)

    response = client.get("/api/task/comp-1/payload", params={"queue": queue, "state": "ready"})
    assert response.status_code == 200
    result = response.json()
    assert result["action"] == "scrape_day_kline"
    assert result["payload"]["symbol"] == "sh600000"


def test_dashboard_payload_endpoint_for_large_without_storage(client, r):
    queue = "qtask_dash_test:large-nostorage:fetch"
    payload = {"_large": True, "key": "abc123"}
    msg = json.dumps({"task_id": "large-ns-1", "payload": json.dumps(payload)})
    r.lpush(queue, msg)
    r.hset("qtask:task:large-ns-1", mapping={
        "task_id": "large-ns-1", "status": "pending", "action": "scrape_fin_sheet",
    })
    r.zadd(f"qtask:hist:{queue}", {"large-ns-1": 1})

    response = client.get("/api/task/large-ns-1/payload", params={"queue": queue, "state": "ready"})
    assert response.status_code == 200
    result = response.json()
    assert result["action"] == "scrape_fin_sheet"
    assert result["payload"]["_large"] is True
    assert "未配置" in result["_note"]


# ── 过期任务测试 ──

def test_dashboard_push_with_expire(client, r):
    """推送带过期时间的任务。"""
    import time
    queue = "qtask_dash_test:expire:fetch"
    r.delete(queue)
    r.delete(f"qtask:hist:{queue}")

    response = client.post(
        f"/api/queue/{queue}/tasks",
        json={"payload": {"action": "test_expire"}, "expire_seconds": 1},
    )
    assert response.status_code == 200
    task_id = response.json()["task_id"]

    record = r.hgetall(f"qtask:task:{task_id}")
    assert "expires_at" in record
    assert float(record["expires_at"]) > time.time()

    r.delete(f"qtask:task:{task_id}")


def test_dashboard_list_expired(client, r):
    """列出过期任务。"""
    queue = "qtask_dash_test:expired:fetch"
    r.delete(queue)
    r.delete(f"qtask:hist:{queue}")

    task_id = seed_expired_tasks(r, queue, 1, "exp-task")[0]

    response = client.get(f"/api/queue/{queue}/expired")
    assert response.status_code == 200
    assert response.json()["count"] >= 1
    tasks = response.json()["tasks"]
    assert any(t["task_id"] == task_id for t in tasks)

    r.delete(f"qtask:task:{task_id}")
    r.delete(f"qtask:hist:{queue}")


def test_dashboard_list_expired_respects_limit_above_fifty(client, r):
    """过期任务列表应尊重 API limit，而不是固定截断到 50 条。"""
    queue = "qtask_dash_test:expired:limit"
    seed_expired_tasks(r, queue, 60, "exp-limit")

    response = client.get(f"/api/queue/{queue}/expired", params={"limit": 60})

    assert response.status_code == 200
    result = response.json()
    assert result["count"] == 60
    assert len(result["tasks"]) == 60

    state_response = client.get(
        f"/api/queue/{queue}/tasks",
        params={"state": "expired", "limit": 60},
    )
    assert state_response.status_code == 200
    state_result = state_response.json()
    assert state_result["count"] == 60
    assert len(state_result["tasks"]) == 60


def test_dashboard_requeue_expired(client, r):
    """放回过期任务。"""
    queue = "qtask_dash_test:expired:requeue"
    r.delete(queue)
    r.delete(f"qtask:hist:{queue}")

    task_id = seed_expired_tasks(r, queue, 1, "exp-task")[0]
    deadline = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()

    response = client.post(
        f"/api/queue/{queue}/requeue-expired",
        json={"task_id": task_id, "start_deadline_at": deadline},
    )
    assert response.status_code == 200
    assert response.json()["moved"] == 1

    record = r.hgetall(f"qtask:task:{task_id}")
    assert record["outcome"] == "cancelled"
    assert response.json()["new_task_id"] != task_id

    r.delete(queue)
    r.delete(f"qtask:task:{task_id}")
    r.delete(f"qtask:hist:{queue}")


def test_dashboard_requeue_expired_ready_task_does_not_duplicate_or_lose_payload(client, r):
    """ready 中的过期任务通过 cancel + replay 保留 payload 且只产生一个新实例。"""
    queue = "qtask_dash_test:expired:ready"
    task_id = seed_expired_tasks(r, queue, 1, "exp-ready")[0]
    deadline = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()

    response = client.post(
        f"/api/queue/{queue}/requeue-expired",
        json={"task_id": task_id, "start_deadline_at": deadline},
    )

    assert response.status_code == 200
    result = response.json()
    assert result["moved"] == 1
    assert result["new_task_id"] != task_id
    assert r.llen(queue) == 1
    raw = json.loads(r.lindex(queue, 0))
    assert raw["payload"]["data"]["action"] == "expired_action"
    assert r.hget(f"qtask:task:{task_id}", "outcome") == "cancelled"


def test_dashboard_requeue_expired_rejects_past_new_deadline_without_cancelling(client, r):
    """无效的新截止时间不能取消仍待处理的旧任务。"""
    queue = "qtask_dash_test:expired:past-deadline"
    task_id = seed_expired_tasks(r, queue, 1, "exp-past")[0]
    past_deadline = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()

    response = client.post(
        f"/api/queue/{queue}/requeue-expired",
        json={"task_id": task_id, "start_deadline_at": past_deadline},
    )

    assert response.status_code == 200
    assert response.json()["moved"] == 0
    assert "必须晚于当前时间" in response.json()["note"]
    assert r.llen(queue) == 1
    assert r.hget(f"qtask:task:{task_id}", "outcome") != "cancelled"


def test_dashboard_requeue_expired_without_payload_does_not_rebuild_lossy_task(client, r):
    """历史记录缺完整 payload 时不能仅凭 action 重建任务。"""
    queue = "qtask_dash_test:expired:no-payload"
    task_id = "exp-no-payload"
    now = time.time()
    r.hset(f"qtask:task:{task_id}", mapping={
        "task_id": task_id,
        "status": "pending",
        "outcome": "",
        "action": "expired_action",
        "expires_at": str(now - 10),
        "start_deadline_at": str(now - 10),
        "operational_message": "1",
        "_queue": queue,
    })
    r.zadd(f"qtask:hist:{queue}", {task_id: now - 10})
    deadline = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()

    response = client.post(
        f"/api/queue/{queue}/requeue-expired",
        json={"task_id": task_id, "start_deadline_at": deadline},
    )

    assert response.status_code == 200
    result = response.json()
    assert result["moved"] == 0
    assert "operational" in result["note"]
    assert r.llen(queue) == 0
    assert r.hget(f"qtask:task:{task_id}", "expires_at") != ""


def test_dashboard_requeue_expired_bulk_handles_more_than_fifty(client, r):
    """批量放回过期任务不应被内部 50 条常量截断。"""
    queue = "qtask_dash_test:expired:bulk-requeue"
    task_ids = seed_expired_tasks(r, queue, 60, "exp-bulk", status="expired")

    deadline = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    response = client.post(
        f"/api/queue/{queue}/requeue-expired",
        json={"task_id": None, "start_deadline_at": deadline},
    )

    assert response.status_code == 200
    assert response.json()["moved"] == 60
    assert r.llen(queue) == 60
    assert all(r.hget(f"qtask:task:{task_id}", "outcome") == "cancelled" for task_id in task_ids)

    expired_again = client.get(f"/api/queue/{queue}/expired", params={"limit": 60})
    assert expired_again.status_code == 200
    assert expired_again.json()["count"] == 0
    assert all(r.hget(f"qtask:task:{task_id}", "start_deadline_at") != "" for task_id in task_ids)


def test_dashboard_queue_stats_includes_expired(client, r):
    """queue_stats 包含 expired 计数。"""
    queue = "qtask_dash_test:stats:expired"
    r.delete(queue)
    r.delete(f"qtask:hist:{queue}")

    task_id = seed_expired_tasks(r, queue, 1, "exp-stats")[0]
    dashboard_main.admin.rebuild_observation_indexes(queue)

    response = client.get("/api/queues")
    assert response.status_code == 200
    stats = next((q for q in response.json() if q["name"] == queue), None)
    assert stats is not None
    assert "expired" in stats
    assert stats["expired"] >= 1

    r.delete(f"qtask:task:{task_id}")
    r.delete(f"qtask:hist:{queue}")


def test_global_search_cursor_keeps_initial_queue_snapshot(client, r):
    # 翻页之间有新队列入库，旧游标仍沿原查询快照继续。
    redis_url = "redis://localhost:6379/0"
    first_queue = SmartQueue(redis_url, "cursor-first", namespace="qtask_dash_test", redis_client=r)
    first_ids = [
        first_queue.enqueue(TaskSpec(action="search", payload={"index": index})).task_id
        for index in range(2)
    ]
    page1 = client.get("/api/tasks", params={"limit": 1})
    assert page1.status_code == 200
    cursor = page1.json()["next_cursor"]
    assert cursor

    second_queue = SmartQueue(redis_url, "cursor-second", namespace="qtask_dash_test", redis_client=r)
    second_queue.enqueue(TaskSpec(action="search", payload={"index": 99}))
    page2 = client.get("/api/tasks", params={"limit": 1, "cursor": cursor})
    assert page2.status_code == 200
    assert page2.json()["tasks"][0]["task_id"] in first_ids
    assert page2.json()["tasks"][0]["_queue"] == first_queue.base


def test_global_search_filters_action_and_created_time(client, r):
    # 跨队列搜索在大量历史中应可用 action 与时间共同缩小范围，且游标绑定筛选条件。
    queue = SmartQueue("redis://localhost:6379/0", "global-filter", namespace="qtask_dash_test", redis_client=r)
    old = queue.enqueue(TaskSpec(action="target_for_filter", payload={"batch": "old"})).task_id
    current = queue.enqueue(TaskSpec(action="target_for_filter", payload={"batch": "new"})).task_id
    queue.enqueue(TaskSpec(action="other_for_filter", payload={"batch": "new"}))
    cutoff = time.time() - 60
    r.hset(f"qtask:task:{old}", "created_at", str(cutoff - 3600))
    r.zadd(f"qtask:hist:{queue.base}", {old: cutoff - 3600})

    response = client.get("/api/tasks", params={
        "action": "target_for_filter", "created_after": cutoff, "limit": 1,
    })
    assert response.status_code == 200
    assert [row["task_id"] for row in response.json()["tasks"]] == [current]
    cursor = response.json()["next_cursor"]
    if cursor:
        changed = client.get("/api/tasks", params={
            "action": "target_for_filter", "created_after": cutoff - 1,
            "limit": 1, "cursor": cursor,
        })
        assert changed.status_code == 400


def test_filtered_operational_views_preserve_actionable_display_state(client, r):
    # 过期与等待重试是派生视图，表格行必须传递展示状态给任务抽屉。
    expired_queue = "qtask_dash_test:display-expired"
    seed_expired_tasks(r, expired_queue, 1, "display-expired")
    expired = client.get(f"/api/queue/{expired_queue}/tasks", params={"state": "deadline_missed"})
    assert expired.status_code == 200
    assert expired.json()["tasks"][0]["_state"] == "deadline_missed"

    retry_queue = SmartQueue(
        "redis://localhost:6379/0", "display-retry", namespace="qtask_dash_test", redis_client=r
    )
    retry_queue.push({"action": "retry_action"})
    item = retry_queue.pop_no_wait()
    assert item is not None
    retry_queue.fail(item[1], "temporary failure")
    waiting = client.get(f"/api/queue/{retry_queue.base}/tasks", params={"state": "retry_wait"})
    assert waiting.status_code == 200
    assert waiting.json()["tasks"][0]["_state"] == "retry_wait"


def test_search_rejects_malformed_cursor_as_client_error(client, r):
    # 不可信游标即使能解码为 JSON，也不能触发服务端异常。
    assert client.get("/api/tasks", params={"cursor": "W10"}).status_code == 400


def test_sparse_queue_search_is_bounded_and_resumable(client, r):
    # retry_wait 稀疏筛选每次只扫描上限内的记录，下一页继续剩余历史。
    queue = "qtask_dash_test:sparse-search"
    pipe = r.pipeline(transaction=False)
    for index in range(5001):
        task_id = f"sparse-{index}"
        pipe.hset(f"qtask:task:{task_id}", mapping={
            "task_id": task_id, "action": "test", "outcome": "completed",
            "created_at": str(1000 + index), "location": "history",
        })
        pipe.zadd(f"qtask:hist:{queue}", {task_id: 1000 + index})
    pipe.execute()

    first = client.get(f"/api/queue/{queue}/tasks", params={"state": "retry_wait", "limit": 1})
    assert first.status_code == 200
    assert first.json()["scanned"] == 5000
    assert first.json()["scan_limited"] is True
    assert first.json()["next_cursor"]
    second = client.get(f"/api/queue/{queue}/tasks", params={
        "state": "retry_wait", "limit": 1, "cursor": first.json()["next_cursor"],
    })
    assert second.status_code == 200
    assert second.json()["scanned"] == 1
    assert second.json()["has_more"] is False
    with pytest.raises(ValueError, match="search_tasks_page"):
        QueueAdmin(redis_url="redis://localhost:6379/0", redis_client=r).list_tasks(
            queue, state="retry_wait", limit=1
        )


def test_dashboard_list_completed_tasks(client, r):
    """按已完成状态过滤任务。"""
    queue = "qtask_dash_test:completed:tasks"
    r.delete(queue)
    r.delete(f"qtask:hist:{queue}")

    task_id = "comp-task-01"
    now = time.time()
    r.hset(f"qtask:task:{task_id}", mapping={
        "task_id": task_id, "status": "completed", "action": "done_action",
        "created_at": str(now - 100), "updated_at": str(now - 50),
    })
    r.zadd(f"qtask:hist:{queue}", {task_id: now - 50})

    response = client.get(f"/api/queue/{queue}/tasks", params={"state": "completed"})
    assert response.status_code == 200
    tasks = response.json()["tasks"]
    assert any(t["task_id"] == task_id and t["_state"] == "completed" for t in tasks)

    r.delete(f"qtask:task:{task_id}")
    r.delete(f"qtask:hist:{queue}")


def test_dashboard_completed_tasks_scan_beyond_recent_pending(client, r):
    """completed 视图不应被最近 pending 任务遮挡。"""
    queue = "qtask_dash_test:completed:sparse"
    now = time.time()
    completed_id = "sparse-completed"
    r.hset(
        f"qtask:task:{completed_id}",
        mapping={
            "task_id": completed_id,
            "status": "completed",
            "action": "done_action",
            "created_at": str(now - 1000),
            "updated_at": str(now - 900),
        },
    )
    r.zadd(f"qtask:hist:{queue}", {completed_id: now - 900})

    pipe = r.pipeline()
    for index in range(320):
        task_id = f"sparse-pending-{index:03d}"
        pipe.hset(
            f"qtask:task:{task_id}",
            mapping={
                "task_id": task_id,
                "status": "pending",
                "action": "pending_action",
                "created_at": str(now - index),
            },
        )
        pipe.zadd(f"qtask:hist:{queue}", {task_id: now - index})
    pipe.execute()

    response = client.get(f"/api/queue/{queue}/tasks", params={"state": "completed", "limit": 1})

    assert response.status_code == 200
    assert response.json()["tasks"][0]["task_id"] == completed_id


def test_dashboard_completed_time_filter_scans_beyond_recent_completed(client, r):
    """completed 时间筛选不应先被较新的 completed 截断。"""
    queue = "qtask_dash_test:completed:time-sparse"
    now = time.time()
    target_id = "time-sparse-completed"
    r.hset(
        f"qtask:task:{target_id}",
        mapping={
            "task_id": target_id,
            "status": "completed",
            "action": "target_action",
            "created_at": str(now - 1000),
            "updated_at": str(now - 900),
        },
    )
    r.zadd(f"qtask:hist:{queue}", {target_id: now - 900})

    pipe = r.pipeline()
    for index in range(320):
        task_id = f"time-sparse-recent-{index:03d}"
        pipe.hset(
            f"qtask:task:{task_id}",
            mapping={
                "task_id": task_id,
                "status": "completed",
                "action": "recent_action",
                "created_at": str(now - index),
                "updated_at": str(now - index),
            },
        )
        pipe.zadd(f"qtask:hist:{queue}", {task_id: now - index})
    pipe.execute()

    response = client.get(
        f"/api/queue/{queue}/tasks",
        params={"state": "completed", "limit": 1, "created_before": now - 900},
    )

    assert response.status_code == 200
    assert response.json()["tasks"][0]["task_id"] == target_id


def test_dashboard_time_filter(client, r):
    """时间范围筛选任务。"""
    queue = "qtask_dash_test:timefilter"
    r.delete(queue)
    r.delete(f"qtask:hist:{queue}")

    task_id = "time-task-01"
    now = time.time()
    r.hset(f"qtask:task:{task_id}", mapping={
        "task_id": task_id, "status": "completed", "action": "time_action",
        "created_at": str(now - 200), "updated_at": str(now - 50),
    })
    r.zadd(f"qtask:hist:{queue}", {task_id: now - 50})

    response = client.get(
        f"/api/queue/{queue}/tasks",
        params={"state": "completed", "created_after": now - 300, "created_before": now - 100},
    )
    assert response.status_code == 200
    tasks = response.json()["tasks"]
    assert any(t["task_id"] == task_id for t in tasks)

    response = client.get(
        f"/api/queue/{queue}/tasks",
        params={"state": "completed", "created_after": now - 50},
    )
    assert response.status_code == 200
    tasks = response.json()["tasks"]
    assert not any(t["task_id"] == task_id for t in tasks)

    r.delete(f"qtask:task:{task_id}")
    r.delete(f"qtask:hist:{queue}")
