"""完整管理后台审查的行为回归；使用 conftest 中的私有 Redis。"""

import json
import time
from datetime import datetime, timedelta, timezone

import pytest
import redis
from fastapi.testclient import TestClient

from qtask_list import FrozenClock, QueueAdmin, SmartQueue, TaskSpec, Worker
from qtask_list.archiver import Monitor
from qtask_list.dashboard import auth
from qtask_list.dashboard import main as dashboard


@pytest.fixture
def management(isolated_redis, monkeypatch):
    for key in ("QTASK_DASHBOARD_AUTH", "QTASK_DASHBOARD_USER", "QTASK_DASHBOARD_PASSWORD",
                "QTASK_DASHBOARD_SECRET", "QTASK_DASHBOARD_SESSION_TTL",
                "QTASK_DASHBOARD_MEMORY_THRESHOLD_MB"):
        monkeypatch.delenv(key, raising=False)
    admin = QueueAdmin(redis_client=isolated_redis)
    monkeypatch.setattr(dashboard, "admin", admin)
    monkeypatch.setattr(dashboard, "redis_client", isolated_redis)
    monkeypatch.setattr(dashboard, "monitor", Monitor(isolated_redis, use_maxmemory=True))
    with TestClient(dashboard.app) as client:
        yield client, admin, isolated_redis


@pytest.mark.parametrize("username,password", [("admin", "中文🔑密码"), ("管理员", "ascii")])
def test_unicode_credentials_login_and_reject_wrong_password(management, monkeypatch, username, password):
    client, _admin, _r = management
    monkeypatch.setenv("QTASK_DASHBOARD_USER", username)
    monkeypatch.setenv("QTASK_DASHBOARD_PASSWORD", password)
    page = client.get("/login")
    assert page.status_code == 200 and username in page.text
    assert client.post("/api/login", json={"username": username, "password": "错误"}).status_code == 401
    assert client.post("/api/login", json={"username": username, "password": password}).status_code == 200
    assert client.get("/api/queues").status_code == 200


def test_invalid_unicode_cookie_is_unauthenticated(monkeypatch):
    monkeypatch.setenv("QTASK_DASHBOARD_PASSWORD", "secret")
    assert auth.validate_session_token("bad.无效签名") is False


@pytest.mark.parametrize("path", ["/", "/queues/review%3Atable", "/index.html"])
def test_spa_html_revalidates_so_replaced_assets_are_not_cached(management, path):
    client, _admin, _r = management
    response = client.get(path)
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-cache"


def test_session_expires_at_ttl_boundary_and_bad_ttl_is_a_configuration_error(management, monkeypatch):
    client, _admin, _r = management
    monkeypatch.setenv("QTASK_DASHBOARD_PASSWORD", "secret")
    monkeypatch.setenv("QTASK_DASHBOARD_SESSION_TTL", "10")
    token = auth.create_session_token("admin", now=100)
    assert auth.validate_session_token(token, now=109)
    assert not auth.validate_session_token(token, now=110)
    monkeypatch.setenv("QTASK_DASHBOARD_SESSION_TTL", "invalid")
    result = client.get("/api/auth")
    assert result.status_code == 200
    assert "QTASK_DASHBOARD_SESSION_TTL" in result.json()["error"]
    assert client.post("/api/login", json={"username": "admin", "password": "secret"}).status_code == 500


def test_queue_deletion_protects_worker_registered_before_atomic_delete(management, monkeypatch):
    _client, admin, r = management
    from qtask_list.state import DELETE_QUEUE_LUA
    worker = Worker("redis://isolated/0", "review-delete", redis_client=r, worker_id="new-worker")
    task = worker.queue.enqueue(TaskSpec("test", {}))
    original = r.eval
    admitted = []

    def register_before_delete(script, *args):
        if script == DELETE_QUEUE_LUA and not admitted:
            admitted.append(True)
            worker._refresh_heartbeat()
            admitted.append(worker.queue.pop_claim_no_wait())
        return original(script, *args)

    monkeypatch.setattr(r, "eval", register_before_delete)
    with pytest.raises(ValueError, match="active workers"):
        admin.delete_queue(worker.queue.base)
    assert admitted[1] is not None
    assert r.exists(worker._heartbeat_key)
    assert r.llen(worker.queue.processing) == 1
    assert admin.get_task(task.task_id)["location"] == "processing"
    assert worker.queue.ack(admitted[1].raw_message)


def test_enqueue_after_atomic_delete_keeps_complete_new_queue(management, monkeypatch):
    _client, admin, r = management
    from qtask_list.state import DELETE_QUEUE_LUA
    queue = SmartQueue(redis_client=r, queue_name="review-delete")
    queue.enqueue(TaskSpec("old", {}))
    original = r.eval
    fresh = []

    def enqueue_after_delete(script, *args):
        result = original(script, *args)
        if script == DELETE_QUEUE_LUA:
            fresh.append(queue.enqueue(TaskSpec("new", {})))
        return result

    monkeypatch.setattr(r, "eval", enqueue_after_delete)
    admin.delete_queue(queue.base)
    assert fresh[0].accepted
    assert queue.size() == 1
    assert admin.get_task(fresh[0].task_id)["action"] == "new"
    assert queue.base in admin.queue_names()


def test_atomic_delete_validates_before_removing_any_data(management):
    _client, admin, r = management
    queue = SmartQueue(redis_client=r, queue_name="review-delete")
    task = queue.enqueue(TaskSpec("test", {}))
    r.set(queue.processing_registry_key, "wrong type")
    with pytest.raises(redis.ResponseError, match="type conflict"):
        admin.delete_queue(queue.base)
    assert queue.size() == 1
    assert admin.get_task(task.task_id)
    assert queue.base in admin.queue_names()


def test_atomic_delete_preserves_other_queue_and_literal_backslash_namespace(management):
    _client, admin, r = management
    other = SmartQueue(redis_client=r, queue_name="reviewname")
    selected = SmartQueue(redis_client=r, queue_name="review\\name")
    task = other.enqueue(TaskSpec("test", {}, logical_key="keep"))
    selected.enqueue(TaskSpec("test", {}))
    r.set(f"{other.base}:supersede:key", "i:8")
    r.set(f"{selected.base}:supersede:key", "i:9")
    admin.delete_queue(selected.base)
    assert r.get(f"{other.base}:supersede:key") == "i:8"
    assert r.get(f"{selected.base}:supersede:key") is None
    assert admin.get_task(task.task_id)
    assert other.size() == 1


def test_atomic_delete_schedules_only_dedicated_storage_and_validates_gc_first(management):
    _client, admin, r = management
    queue = SmartQueue(redis_client=r, queue_name="review-delete")
    task = queue.enqueue(TaskSpec("test", {}))
    record_key = f"qtask:task:{task.task_id}"
    r.hset(record_key, mapping={"payload_dedicated": "1", "payload_storage_id": "test-storage",
                                "payload_ref": "dedicated-object"})
    gc_key = f"{queue.storage_gc_key}:test-storage"
    r.set(gc_key, "wrong type")
    with pytest.raises(redis.ResponseError, match="type conflict"):
        admin.delete_queue(queue.base)
    assert queue.size() == 1 and admin.get_task(task.task_id)
    r.delete(gc_key)
    before = time.time()
    admin.delete_queue(queue.base)
    assert before <= r.zscore(gc_key, "dedicated-object") <= time.time()
    assert not r.exists(record_key)


def test_atomic_delete_rejects_foreign_audit_record_without_partial_changes(management):
    _client, admin, r = management
    selected = SmartQueue(redis_client=r, queue_name="review-delete")
    other = SmartQueue(redis_client=r, queue_name="other")
    own_id = selected.enqueue(TaskSpec("test", {})).task_id
    foreign_id = other.enqueue(TaskSpec("test", {})).task_id
    r.zadd(selected.history.idx_key, {foreign_id: time.time()})
    with pytest.raises(ValueError, match="ownership conflict"):
        admin.delete_queue(selected.base)
    assert selected.size() == other.size() == 1
    assert admin.get_task(own_id) and admin.get_task(foreign_id)


def test_history_cursor_survives_deleted_anchor_and_tied_unicode_ids(management):
    _client, admin, r = management
    name = "review-paging"
    ids = ["a", "aa", "a\x00", "á", "中", "🙂", "z"]
    for task_id in ids:
        r.hset(f"qtask:task:{task_id}", mapping={
            "task_id": task_id, "_queue": name, "created_at": 1000,
            "outcome": "completed", "location": "history",
        })
        r.zadd(f"qtask:hist:{name}", {task_id: 1000})
    expected = r.zrevrange(f"qtask:hist:{name}", 0, -1)
    first = admin.search_tasks_page([name], limit=2)
    seen = [row["task_id"] for row in first["tasks"]]
    assert seen == expected[:2]
    for task_id in seen:
        r.zrem(f"qtask:hist:{name}", task_id)
        r.delete(f"qtask:task:{task_id}")
    r.zadd(f"qtask:hist:{name}", {"future": time.time() + 3600})
    r.hset("qtask:task:future", mapping={"task_id": "future", "outcome": "completed"})
    cursor = first["next_cursor"]
    while cursor:
        page = admin.search_tasks_page([name], limit=2, cursor=cursor)
        seen.extend(row["task_id"] for row in page["tasks"])
        cursor = page["next_cursor"]
    assert seen == expected


def test_history_cursor_resumes_sparse_filter_without_offset_drift(management):
    _client, admin, r = management
    name = "review-paging"
    for index in range(10):
        task_id = f"task-{index}"
        r.hset(f"qtask:task:{task_id}", mapping={
            "task_id": task_id, "action": "match" if index == 0 else "other", "created_at": 1000 + index,
        })
        r.zadd(f"qtask:hist:{name}", {task_id: 1000 + index})
    page = admin.search_tasks_page([name], action="match", limit=1, scan_limit=3)
    assert page["scan_limited"] and page["scanned"] == 3
    r.zrem(f"qtask:hist:{name}", "task-9")
    found = []
    for _ in range(5):
        if not page["next_cursor"]:
            break
        page = admin.search_tasks_page([name], action="match", limit=1,
                                       scan_limit=3, cursor=page["next_cursor"])
        found.extend(row["task_id"] for row in page["tasks"])
    assert found == ["task-0"]


def test_history_cursor_supports_epoch_zero(management):
    _client, _admin, r = management
    clock = FrozenClock(0)
    queue = SmartQueue(redis_client=r, queue_name="review-paging", clock=clock)
    for _ in range(3):
        queue.enqueue(TaskSpec("test", {}))
    admin = QueueAdmin(redis_client=r, clock=clock)
    page = admin.search_tasks_page([queue.base], limit=1)
    ids = [page["tasks"][0]["task_id"]]
    while page["next_cursor"]:
        page = admin.search_tasks_page([queue.base], limit=1, cursor=page["next_cursor"])
        ids.extend(row["task_id"] for row in page["tasks"])
    assert len(set(ids)) == 3


@pytest.mark.parametrize("state", ["ready", "processing", "retry", "delay", "dlq"])
def test_every_container_page_reaches_unindexed_message_after_five_hundred(management, state):
    client, _admin, r = management
    name = f"review-pages:{state}"
    key = name if state == "ready" else f"{name}:{state}"
    for index in range(501):
        raw = json.dumps({"task_id": f"orphan-{index}", "payload": {"action": "legacy"}})
        if state == "delay":
            r.zadd(key, {raw: time.time() + index})
        else:
            r.lpush(key, raw)
    cursor = None
    ids = []
    for _ in range(10):
        params = {"name": name, "state": state, "limit": 100}
        if cursor:
            params["cursor"] = cursor
        response = client.get("/api/queues/tasks", params=params)
        assert response.status_code == 200, response.text
        page = response.json()
        ids.extend(row["task_id"] for row in page["tasks"])
        cursor = page["next_cursor"]
        if not cursor:
            break
    assert cursor is None
    assert len(ids) == len(set(ids)) == 501


def test_raw_container_cursor_requests_refresh_when_contents_change(management):
    client, _admin, r = management
    name = "review-pages"
    for index in range(3):
        r.lpush(name, json.dumps({"task_id": f"legacy-{index}", "payload": {}}))
    page = client.get("/api/queues/tasks", params={"name": name, "state": "ready", "limit": 1}).json()
    r.rpop(name)
    response = client.get("/api/queues/tasks", params={"name": name, "state": "ready", "limit": 1,
                                                      "cursor": page["next_cursor"]})
    assert response.status_code == 400 and "刷新第一页" in response.json()["detail"]


def test_mixed_indexed_and_unindexed_tasks_are_listed_once_with_current_metadata(management):
    client, admin, r = management
    queue = SmartQueue(redis_client=r, queue_name="review-pages")
    task = queue.enqueue(TaskSpec("current", {}))
    r.lpush(queue.queue, json.dumps({"task_id": "legacy", "payload": {"action": "old"}}))
    r.hset("qtask:task:legacy", mapping={"task_id": "legacy", "action": "old", "status": "pending"})
    r.zadd(queue.history.idx_key, {"legacy": 1000})
    response = client.get("/api/queues/tasks", params={"name": queue.base, "state": "ready"})
    assert response.status_code == 200
    rows = response.json()["tasks"]
    assert {row["task_id"] for row in rows} == {task.task_id, "legacy"}
    assert len(rows) == 2
    row = next(row for row in rows if row["task_id"] == task.task_id)
    assert row["created_at"] == admin.get_task(task.task_id)["created_at"]
    assert row["updated_at"] == admin.get_task(task.task_id)["updated_at"]
    assert row["location"] == "ready" and row["max_attempts"] == 3


def test_legacy_record_without_location_uses_its_actual_dlq_container(management):
    client, _admin, r = management
    name = "review-pages"
    r.lpush(f"{name}:dlq", json.dumps({"task_id": "legacy", "payload": {"action": "old"}}))
    r.hset("qtask:task:legacy", mapping={"task_id": "legacy", "status": "failed", "action": "old"})
    r.zadd(f"qtask:hist:{name}", {"legacy": 1000})
    response = client.get("/api/queues/tasks", params={"name": name, "state": "dlq"})
    assert response.json()["tasks"][0]["location"] == "dlq"


@pytest.mark.parametrize("name", ["review/slash", "review%2Fname", "中文/队列", "review\\name"])
def test_queue_query_endpoints_preserve_names_for_read_write_delete(management, name):
    client, _admin, _r = management
    query = {"name": name}
    posted = client.post("/api/queues/tasks", params=query, json={"payload": {"action": "test"}})
    assert posted.status_code == 200 and posted.json()["accepted"]
    assert posted.json()["queue"] == name
    assert client.get("/api/queues/detail", params=query).json()["name"] == name
    assert client.get("/api/queues/tasks", params=query).json()["count"] == 1
    assert client.get("/api/queues/diagnose", params=query).json()["queue"] == name
    assert client.post("/api/queues/recover", params=query, json={}).status_code == 200
    assert client.delete("/api/queues/detail", params={**query, "confirm": True}).status_code == 200


def test_task_query_endpoints_preserve_arbitrary_legacy_id(management):
    client, _admin, r = management
    task_id = "旧任务/with%2F?&+"
    r.hset(f"qtask:task:{task_id}", mapping={"task_id": task_id, "status": "completed", "outcome": "completed"})
    query = {"task_id": task_id}
    assert client.get("/api/tasks/detail", params=query).json()["task_id"] == task_id
    assert client.delete("/api/tasks/detail", params=query).status_code == 200
    assert client.get("/api/tasks/detail", params=query).status_code == 404


def failed_task(queue, deadline=None):
    task = queue.enqueue(TaskSpec("test", {"value": 1}, start_deadline_at=deadline))
    claim = queue.pop_claim_no_wait()
    assert claim is not None
    assert queue.fail(claim.raw_message, "review failure", permanent=True)
    return task.task_id


@pytest.mark.parametrize("budget", [1, 7])
def test_dashboard_replay_inherits_source_budget_and_retains_all_lineage(management, budget):
    client, admin, r = management
    queue = SmartQueue(redis_client=r, queue_name="review-replay", max_attempts=budget)
    task_id = failed_task(queue)
    replay_ids = []
    for _ in range(2):
        response = client.post("/api/tasks/replay", params={"task_id": task_id}, json={"queue": queue.base})
        assert response.status_code == 200, response.text
        replay_ids.append(response.json()["task_id"])
        record = admin.get_task(replay_ids[-1])
        assert record["max_attempts"] == budget
        assert record["attempt"] == 0 and record["replay_of"] == task_id
    assert admin.get_task(task_id)["replayed_by"].split(",") == replay_ids
    assert admin.get_task(task_id)["outcome"] == "failed"


def test_replay_explicit_budget_and_legacy_json_lineage(management):
    client, admin, r = management
    queue = SmartQueue(redis_client=r, queue_name="review-replay", max_attempts=1)
    task_id = failed_task(queue)
    r.hset(f"qtask:task:{task_id}", "replayed_by", json.dumps(["older-a", "older-b"]))
    response = client.post("/api/tasks/replay", params={"task_id": task_id},
                           json={"queue": queue.base, "max_attempts": 5})
    assert response.status_code == 200
    new_id = response.json()["task_id"]
    assert admin.get_task(new_id)["max_attempts"] == 5
    assert json.loads(admin.get_task(task_id)["replayed_by"]) == ["older-a", "older-b", new_id]
    assert client.post("/api/tasks/replay", params={"task_id": task_id},
                       json={"max_attempts": True}).status_code == 422


def test_bulk_replay_reports_expired_skip_and_keeps_original_message(management):
    client, admin, r = management
    queue = SmartQueue(redis_client=r, queue_name="review-replay", max_attempts=1)
    future = datetime.now(timezone.utc) + timedelta(seconds=0.5)
    expired_id = failed_task(queue, deadline=future)
    good_id = failed_task(queue)
    time.sleep(0.6)
    response = client.post("/api/queues/requeue-dlq", params={"name": queue.base}, json={"confirm_bulk": True})
    assert response.status_code == 200
    result = response.json()
    assert result["moved"] == 1 and result["skipped"] == 1 and result["not_attempted"] == 0
    assert result["errors"][0]["task_id"] == expired_id
    assert "start_deadline_at" in result["errors"][0]["reason"]
    assert queue.dlq_size() == 1
    assert admin.get_task(expired_id)["operational_message"] == 1
    assert admin.get_task(good_id)["replayed_by"]
    assert admin.get_task(admin.get_task(good_id)["replayed_by"])["max_attempts"] == 1


def test_bulk_replay_does_not_downgrade_orphan_v2_payload_descriptor(management):
    client, _admin, r = management
    queue = SmartQueue(redis_client=r, queue_name="review-replay", max_attempts=1)
    task_id = failed_task(queue)
    raw = r.lindex(queue.dlq, 0)
    r.delete(f"qtask:task:{task_id}")
    result = client.post("/api/queues/requeue-dlq", params={"name": queue.base},
                         json={"confirm_bulk": True}).json()
    assert result["moved"] == 0 and result["skipped"] == 1
    assert r.lindex(queue.dlq, 0) == raw
    assert queue.size() == 0


@pytest.mark.parametrize("entrypoint", ["admin", "sdk"])
def test_dlq_snapshot_cannot_replay_message_already_moved_by_another_admin(management, monkeypatch, entrypoint):
    _client, admin, r = management
    queue = SmartQueue(redis_client=r, queue_name="review-replay")
    task_id = failed_task(queue)
    other_replays = []
    if entrypoint == "admin":
        original = admin.requeue_task

        def concurrently_move(*args, **kwargs):
            other_replays.append(admin.replay_task(task_id, queue_name=queue.base)["task_id"])
            return original(*args, **kwargs)

        monkeypatch.setattr(admin, "requeue_task", concurrently_move)
        result = admin.requeue_dlq(queue.base)
        assert result["moved"] == 0 and result["skipped"] == 1
    else:
        original_get = queue.history.get

        def move_before_record_read(identifier):
            if not other_replays:
                other_replays.append(admin.replay_task(task_id, queue_name=queue.base)["task_id"])
            return original_get(identifier)

        monkeypatch.setattr(queue.history, "get", move_before_record_read)
        assert queue.requeue_dlq() == 0
    assert queue.size() == 1 and queue.dlq_size() == 0
    assert admin.get_task(task_id)["replayed_by"] == other_replays[0]


def test_dlq_replay_rechecks_source_in_the_atomic_lua(management, monkeypatch):
    from qtask_list.state import REPLAY_TASK_LUA
    _client, admin, r = management
    queue = SmartQueue(redis_client=r, queue_name="review-replay")
    task_id = failed_task(queue)
    original = r.eval
    concurrent = []

    def move_before_lua(script, *args):
        if script == REPLAY_TASK_LUA and not concurrent:
            concurrent.append(None)
            concurrent[0] = admin.replay_task(task_id, queue_name=queue.base)["task_id"]
        return original(script, *args)

    monkeypatch.setattr(r, "eval", move_before_lua)
    result = admin.requeue_dlq(queue.base)
    assert result["moved"] == 0 and result["skipped"] == 1
    assert queue.size() == 1 and queue.dlq_size() == 0
    assert admin.get_task(task_id)["replayed_by"] == concurrent[0]


def test_removed_dlq_message_is_conflict_but_history_replay_remains_available(management):
    client, admin, r = management
    queue = SmartQueue(redis_client=r, queue_name="review-replay")
    task_id = failed_task(queue)
    first = admin.replay_task(task_id, queue_name=queue.base)["task_id"]
    single = client.post("/api/queues/requeue-dlq", params={"name": queue.base}, json={"task_id": task_id})
    assert single.status_code == 409 and "不在 DLQ" in single.json()["detail"]
    moved = client.post("/api/tasks/requeue", params={"task_id": task_id}, json={"queue": queue.base, "from_state": "dlq"})
    assert moved.status_code == 409
    assert queue.size() == 1
    history_replay = client.post("/api/tasks/replay", params={"task_id": task_id}, json={"queue": queue.base})
    assert history_replay.status_code == 200 and history_replay.json()["accepted"]
    assert admin.get_task(task_id)["replayed_by"] == f"{first},{history_replay.json()['task_id']}"


def test_bulk_replay_storage_error_keeps_message_and_redacts_connection_details(management, monkeypatch):
    from qtask_list.errors import RemoteStorageTransientError
    client, admin, r = management
    queue = SmartQueue(redis_client=r, queue_name="review-replay")
    failed_task(queue)

    def unavailable(*_args, **_kwargs):
        raise RemoteStorageTransientError("storage_timeout", "https://user:secret@private.example")

    monkeypatch.setattr(admin, "requeue_task", unavailable)
    response = client.post("/api/queues/requeue-dlq", params={"name": queue.base}, json={"confirm_bulk": True})
    assert response.status_code == 200
    result = response.json()
    assert result["moved"] == 0 and result["skipped"] == 1
    assert "secret" not in response.text and "storage_timeout" in result["errors"][0]["reason"]
    assert queue.dlq_size() == 1


def test_cli_partial_requeue_is_nonzero_and_reports_retained_message(management, monkeypatch):
    from typer.testing import CliRunner
    from qtask_list.cli import __main__ as cli
    _client, admin, r = management
    queue = SmartQueue(redis_client=r, queue_name="review-replay")
    r.lpush(queue.dlq, json.dumps({"task_id": "legacy-good", "payload": {"action": "test"}}), "invalid-json")
    monkeypatch.setattr(cli, "admin_from_url", lambda _url: admin)
    result = CliRunner().invoke(cli.app, ["requeue", queue.base, "--force"])
    assert result.exit_code == 1
    assert "已处理 1 条，跳过 1 条" in result.stdout and "不含 task_id" in result.stdout
    assert queue.size() == queue.dlq_size() == 1
    assert r.lindex(queue.dlq, 0) == "invalid-json"


def test_health_uses_maxmemory_and_distinguishes_unconfigured_monitor(management):
    client, _admin, r = management
    assert client.get("/api/health").json()["memory"]["status"] == "unconfigured"
    r.config_set("maxmemory", 1)
    try:
        memory = client.get("/api/health").json()["memory"]
        assert memory["status"] == "warning"
        assert memory["threshold_source"] == "maxmemory"
        assert memory["used_memory"] > memory["maxmemory"]
    finally:
        r.config_set("maxmemory", 0)


def test_memory_configuration_uses_one_snapshot_and_rejects_invalid_limit(management, monkeypatch):
    _client, _admin, r = management
    monkeypatch.setenv("QTASK_DASHBOARD_MEMORY_THRESHOLD_MB", "0.01")
    memory = dashboard._create_monitor(r).get_memory_info()
    assert memory["status"] == "warning" and memory["threshold_source"] == "configured"
    for value in ("nan", "inf", "-1", "0", "invalid"):
        monkeypatch.setenv("QTASK_DASHBOARD_MEMORY_THRESHOLD_MB", value)
        with pytest.raises(ValueError, match="QTASK_DASHBOARD_MEMORY_THRESHOLD_MB"):
            dashboard._create_monitor(r)


def test_api_dependency_failures_are_actionable_and_do_not_expose_credentials(management, monkeypatch):
    client, admin, _r = management

    def unavailable():
        raise redis.ConnectionError("redis://user:secret@private.example/0")

    monkeypatch.setattr(admin, "list_queues", unavailable)
    result = client.get("/api/queues")
    assert result.status_code == 503
    assert "secret" not in result.text and "核对" in result.json()["detail"]
    from qtask_list.errors import RemoteStorageTransientError

    def missing_storage(*_args, **_kwargs):
        raise RemoteStorageTransientError("storage_timeout", "https://user:secret@private.example")

    monkeypatch.setattr(admin, "replay_task", missing_storage)
    result = client.post("/api/tasks/replay", params={"task_id": "any"}, json={})
    assert result.status_code == 503 and "secret" not in result.text
    assert "storage_timeout" in result.json()["detail"]
