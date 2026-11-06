"""全量 Python Review 中的并发、安全和管理行为回归。"""

import asyncio
import json
import os
import runpy
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
import redis
from fastapi.testclient import TestClient

from qtask_list import QueueAdmin, SmartQueue, TaskSpec, Worker, start_dashboard
from qtask_list.archiver import ArchiveManager
from qtask_list.errors import (
    RemoteStorageConfigurationError, RemoteStorageTransientError, RetryableTaskError,
    WorkerIdentityConflictError,
)
from qtask_list.archiver import DELETE_ARCHIVED_SNAPSHOT_LUA


def test_worker_auto_recovery_does_not_claim_unmanaged_consumer(isolated_redis):
    direct = SmartQueue(
        redis_client=isolated_redis,
        queue_name="review-custom",
        processing_key="review-custom:processing:direct",
    )
    task_id = direct.enqueue(TaskSpec("work", {})).task_id
    claim = direct.pop_claim_no_wait()
    assert claim is not None
    worker = Worker("redis://isolated/0", "review-custom", redis_client=isolated_redis)

    assert worker.queue.recover_stale_processing(worker._heartbeat_prefix) == 0
    assert isolated_redis.llen(direct.processing) == 1
    assert direct.ack(claim.raw_message)
    assert direct.history.get(task_id)["outcome"] == "completed"


def test_duplicate_worker_id_cannot_acquire_live_identity(isolated_redis):
    first = Worker("redis://isolated/0", "review-worker", redis_client=isolated_redis, worker_id="w")
    second = Worker("redis://isolated/0", "review-worker", redis_client=isolated_redis, worker_id="w")
    first._refresh_heartbeat()

    with pytest.raises(RuntimeError, match="worker.*(identity|id|ID)"):
        second._refresh_heartbeat()
    assert isolated_redis.exists(first._heartbeat_key)


def test_old_worker_cleanup_cannot_remove_new_owner_or_processing(isolated_redis):
    first = Worker("redis://isolated/0", "review-worker", redis_client=isolated_redis, worker_id="w")
    second = Worker("redis://isolated/0", "review-worker", redis_client=isolated_redis, worker_id="w")
    first._refresh_heartbeat()
    isolated_redis.delete(first._heartbeat_key, first._worker_owner_key)
    second._refresh_heartbeat()
    second.queue.enqueue(TaskSpec("work", {}))
    claim = second.queue.pop_claim_no_wait()
    assert claim is not None

    first._cleanup_worker_state()
    assert isolated_redis.get(second._worker_owner_key) == second._worker_owner_token
    assert isolated_redis.exists(second._heartbeat_key)
    assert isolated_redis.llen(second.queue.processing) == 1
    assert second.queue.ack(claim.raw_message)


def test_duplicate_worker_run_does_not_recover_an_active_claim(isolated_redis):
    first = Worker("redis://isolated/0", "review-worker", redis_client=isolated_redis, worker_id="w")
    first._refresh_heartbeat()
    first.queue.enqueue(TaskSpec("work", {}))
    claim = first.queue.pop_claim_no_wait()
    assert claim is not None
    second = Worker("redis://isolated/0", "review-worker", redis_client=isolated_redis, worker_id="w")

    with pytest.raises(RuntimeError, match="worker.*(identity|id|ID)"):
        second.run()
    assert not second.running
    assert isolated_redis.llen(first.queue.processing) == 1
    assert first.queue.ack(claim.raw_message)


def test_worker_queue_refreshes_identity_before_each_direct_pop(isolated_redis):
    worker = Worker("redis://isolated/0", "review-refresh", redis_client=isolated_redis, worker_id="w")
    worker.queue.enqueue(TaskSpec("work", {}))
    claim = worker.queue.pop_claim_no_wait()
    assert claim and worker.queue.ack(claim.raw_message)
    isolated_redis.delete(worker._heartbeat_key, worker._worker_owner_key)
    worker.queue.enqueue(TaskSpec("work", {}))
    next_claim = worker.queue.pop_claim_no_wait()
    assert next_claim and worker.queue.ack(next_claim.raw_message)


def test_expired_worker_cannot_start_a_returned_blocking_pop(isolated_redis, monkeypatch):
    first = Worker("redis://isolated/0", "review-fence", redis_client=isolated_redis, worker_id="w")
    second = Worker("redis://isolated/0", "review-fence", redis_client=isolated_redis, worker_id="w")
    first._refresh_heartbeat()
    task = first.queue.enqueue(TaskSpec("work", {}))
    raw = isolated_redis.rpoplpush(first.queue.queue, first.queue.processing)
    original = first.queue._ensure_task_record

    def lose_owner_after_record(header):
        original(header)
        isolated_redis.delete(first._heartbeat_key, first._worker_owner_key)
        second._refresh_heartbeat()

    monkeypatch.setattr(first.queue, "_ensure_task_record", lose_owner_after_record)
    with pytest.raises(WorkerIdentityConflictError):
        first.queue._claim_popped(raw)
    assert isolated_redis.llen(first.queue.processing) == 0
    assert isolated_redis.llen(first.queue.queue) == 1
    assert int(first.queue.history.get(task.task_id)["attempt"]) == 0
    claim = second.queue.pop_claim_no_wait()
    assert claim and second.queue.ack(claim.raw_message)


@pytest.mark.parametrize("message_kind", ["legacy", "poison"])
def test_expired_worker_does_not_mutate_legacy_or_poison_claim(isolated_redis, monkeypatch, message_kind):
    first = Worker("redis://isolated/0", "review-fence", redis_client=isolated_redis, worker_id="w")
    second = Worker("redis://isolated/0", "review-fence", redis_client=isolated_redis, worker_id="w")
    first._refresh_heartbeat()
    raw = (first.queue._build_envelope("legacy-fence", {"action": "work"})
           if message_kind == "legacy" else "invalid-json")
    isolated_redis.lpush(first.queue.processing, raw)
    if message_kind == "legacy":
        isolated_redis.set("qtask:task:legacy-fence", json.dumps({"action": "work", "status": "pending"}))
    name = "_migrate_v1_message" if message_kind == "legacy" else "_move_poison_to_dlq"
    original = getattr(first.queue, name)

    def lose_owner_before_mutation(*args):
        isolated_redis.delete(first._heartbeat_key, first._worker_owner_key)
        second._refresh_heartbeat()
        return original(*args)

    monkeypatch.setattr(first.queue, name, lose_owner_before_mutation)
    with pytest.raises(WorkerIdentityConflictError):
        first.queue._claim_popped(raw)
    assert isolated_redis.lrange(first.queue.queue, 0, -1) == [raw]
    assert isolated_redis.llen(first.queue.dlq) == 0
    if message_kind == "legacy":
        assert isolated_redis.type("qtask:task:legacy-fence") == "string"


@pytest.mark.parametrize("poison", [False, True])
def test_expired_worker_cannot_recover_new_owners_claim(isolated_redis, poison):
    first = Worker("redis://isolated/0", "review-fence", redis_client=isolated_redis, worker_id="w")
    second = Worker("redis://isolated/0", "review-fence", redis_client=isolated_redis, worker_id="w")
    first._refresh_heartbeat()
    isolated_redis.delete(first._heartbeat_key, first._worker_owner_key)
    second._refresh_heartbeat()
    if poison:
        raw = "invalid-json"
        isolated_redis.lpush(second.queue.processing, raw)
    else:
        second.queue.enqueue(TaskSpec("work", {}))
        claim = second.queue.pop_claim_no_wait()
        assert claim
        raw = claim.raw_message
    with pytest.raises(WorkerIdentityConflictError):
        first.queue.recover_abandoned_claims(set(), worker_owner_key=first._worker_owner_key,
                                             worker_owner_token=first._worker_owner_token)
    assert isolated_redis.lrange(second.queue.processing, 0, -1) == [raw]
    assert isolated_redis.llen(second.queue.dlq) == 0


def test_startup_recovery_stops_when_worker_identity_is_lost(isolated_redis):
    first = Worker("redis://isolated/0", "review-fence", redis_client=isolated_redis, worker_id="w")
    second = Worker("redis://isolated/0", "review-fence", redis_client=isolated_redis, worker_id="w")
    first._refresh_heartbeat()
    isolated_redis.delete(first._heartbeat_key, first._worker_owner_key)
    second._refresh_heartbeat()
    second.queue.enqueue(TaskSpec("work", {}))
    claim = second.queue.pop_claim_no_wait()
    assert claim
    with pytest.raises(WorkerIdentityConflictError):
        first.queue.recover_processing_key(first.queue.processing, required_owner_key=first._worker_owner_key,
                                           required_owner_token=first._worker_owner_token)
    assert isolated_redis.lrange(second.queue.processing, 0, -1) == [claim.raw_message]
    assert second.queue.ack(claim.raw_message)


def test_legacy_custom_processing_prevents_history_removal(isolated_redis, tmp_path):
    queue = SmartQueue(redis_client=isolated_redis, queue_name="review-custom-legacy",
                       processing_key="outside-processing-key")
    assert queue.pop_claim_no_wait() is None
    task_id = "legacy-in-custom"
    raw = queue._build_envelope(task_id, {"action": "work"})
    isolated_redis.lpush(queue.processing, raw)
    old = time.time() - 2 * 86400
    isolated_redis.set(f"qtask:task:{task_id}", json.dumps({"status": "failed", "created_at": old}))
    isolated_redis.zadd(queue.history.idx_key, {task_id: old})
    manager = ArchiveManager("redis://isolated/0", db_dir=str(tmp_path), redis_client=isolated_redis)
    assert manager.archive_to_sqlite(queue.base) == 0
    assert queue.history.clean_expired() == 0
    assert queue.history.clear_terminal() == 0
    assert queue.history.get(task_id)


def test_archive_checks_index_type_before_deleting_record(isolated_redis):
    isolated_redis.hset("qtask:task:archive-fence", mapping={"task_id": "archive-fence"})
    isolated_redis.set("bad-index", "not-a-zset")
    with pytest.raises(redis.ResponseError, match="history index"):
        isolated_redis.eval(DELETE_ARCHIVED_SNAPSHOT_LUA, 2, "qtask:task:archive-fence", "bad-index",
                            "archive-fence", "hash", "task_id", "archive-fence")
    assert isolated_redis.exists("qtask:task:archive-fence")


def test_custom_processing_is_protected_from_management_by_default(isolated_redis):
    queue = SmartQueue(redis_client=isolated_redis, queue_name="review-managed",
                       processing_key="review-managed:processing:direct")
    task_id = queue.enqueue(TaskSpec("work", {})).task_id
    claim = queue.pop_claim_no_wait()
    admin = QueueAdmin(redis_client=isolated_redis)
    assert claim is not None
    assert admin.recover(queue.base)["skipped_unmanaged"] == 1
    with pytest.raises(ValueError, match="custom processing|自定义"):
        admin.delete_task(task_id, queue.base)
    with pytest.raises(ValueError, match="custom processing"):
        admin.clear_queue(queue.base, include_processing=True)
    with pytest.raises(ValueError, match="custom processing"):
        admin.delete_queue(queue.base)
    assert isolated_redis.llen(queue.processing) == 1
    assert queue.ack(claim.raw_message)
    admin.delete_queue(queue.base, force_unmanaged=True)


def test_custom_processing_cannot_overlap_another_queue(isolated_redis):
    other = SmartQueue(redis_client=isolated_redis, queue_name="other")
    task = other.enqueue(TaskSpec("work", {}))
    direct = SmartQueue(redis_client=isolated_redis, queue_name="review-custom", processing_key="other")
    direct.enqueue(TaskSpec("work", {}))
    with pytest.raises(redis.ResponseError, match="processing key conflict"):
        direct.pop_claim_no_wait()
    assert other.size() == 1 and other.history.get(task.task_id)


def test_new_queue_cannot_occupy_registered_custom_processing(isolated_redis):
    direct = SmartQueue(redis_client=isolated_redis, queue_name="review-custom", processing_key="other")
    assert direct.pop_claim_no_wait() is None
    other = SmartQueue(redis_client=isolated_redis, queue_name="other")
    with pytest.raises(ValueError, match="namespace conflict"):
        other.enqueue(TaskSpec("work", {}))


def test_dashboard_sdk_rejects_unauthenticated_remote_bind(monkeypatch):
    for key in ("QTASK_DASHBOARD_PASSWORD", "QTASK_DASHBOARD_AUTH"):
        monkeypatch.delenv(key, raising=False)
    launched = []
    monkeypatch.setattr("subprocess.run", lambda *args, **kwargs: launched.append(args))

    with pytest.raises(ValueError, match="认证"):
        start_dashboard(host="0.0.0.0")
    assert not launched


def test_dashboard_sdk_allows_explicit_remote_opt_in_or_password(monkeypatch):
    for key in ("QTASK_DASHBOARD_PASSWORD", "QTASK_DASHBOARD_AUTH"):
        monkeypatch.delenv(key, raising=False)
    launched = []
    monkeypatch.setattr("subprocess.run", lambda *args, **kwargs: launched.append(kwargs["env"]))
    start_dashboard(host="0.0.0.0", allow_unauthenticated=True)
    start_dashboard(host="0.0.0.0", password="test-password")
    assert len(launched) == 2
    assert launched[1]["QTASK_DASHBOARD_PASSWORD"] == "test-password"


def test_dashboard_rejects_untrusted_origin_writes(monkeypatch, isolated_redis):
    from qtask_list.dashboard import main as dashboard

    monkeypatch.delenv("QTASK_DASHBOARD_PASSWORD", raising=False)
    monkeypatch.delenv("QTASK_DASHBOARD_AUTH", raising=False)
    monkeypatch.setattr(dashboard, "admin", QueueAdmin(redis_client=isolated_redis))
    with TestClient(dashboard.app) as client:
        response = client.post(
            "/api/queues/tasks",
            params={"name": "review-origin"},
            headers={"Origin": "https://unrelated.example"},
            json={"payload": {"action": "work"}},
        )
        simple = client.post(
            "/api/queues/retry", params={"name": "review-origin"},
            headers={"Origin": "https://unrelated.example"},
        )
    assert response.status_code == 403
    assert simple.status_code == 403
    assert isolated_redis.llen("review-origin") == 0


def test_dashboard_origin_policy_accepts_same_origin_and_configured_origin(monkeypatch):
    from qtask_list.dashboard import main as dashboard

    monkeypatch.setattr(dashboard, "_allowed_origin_tuples", {("https", "trusted.example", 443)})
    with TestClient(dashboard.app) as client:
        same = client.post("/missing-test-path", headers={"Origin": "http://testserver"})
        trusted = client.post("/missing-test-path", headers={"Origin": "https://trusted.example"})
        no_origin = client.post("/missing-test-path")
        opaque = client.post("/missing-test-path", headers={"Origin": "null"})
    assert (same.status_code, trusted.status_code, no_origin.status_code) == (405, 405, 405)
    assert opaque.status_code == 403


def test_dashboard_configured_origin_is_in_cors_preflight(monkeypatch):
    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
    env["QTASK_DASHBOARD_ALLOWED_ORIGINS"] = "https://trusted.example:443"
    env.pop("QTASK_DASHBOARD_PASSWORD", None)
    env.pop("QTASK_DASHBOARD_AUTH", None)
    program = """
from fastapi.testclient import TestClient
from qtask_list.dashboard.main import app
with TestClient(app) as client:
    allowed = client.options('/api/tasks/replay', headers={
        'Origin': 'https://trusted.example', 'Access-Control-Request-Method': 'POST',
        'Access-Control-Request-Headers': 'content-type'})
    denied = client.options('/api/tasks/replay', headers={
        'Origin': 'https://evil.example', 'Access-Control-Request-Method': 'POST'})
    assert allowed.status_code == 200
    assert allowed.headers['access-control-allow-origin'] == 'https://trusted.example'
    assert denied.status_code == 400
"""
    result = subprocess.run([sys.executable, "-c", program], env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_dashboard_blocks_cross_origin_write_with_authenticated_cookie(monkeypatch):
    from qtask_list.dashboard import main as dashboard

    monkeypatch.setenv("QTASK_DASHBOARD_PASSWORD", "review-password")
    monkeypatch.setenv("QTASK_DASHBOARD_SECRET", "review-secret")
    with TestClient(dashboard.app) as client:
        login = client.post("/api/login", json={"username": "admin", "password": "review-password"})
        assert login.status_code == 200
        response = client.post("/api/queues/retry", params={"name": "review-origin"},
                               headers={"Origin": "https://unrelated.example"})
    assert response.status_code == 403


def test_payload_resolution_redacts_external_error(isolated_redis):
    class FailingStorage:
        def save_bytes(self, data, **kwargs):
            return "payload-ref"

        def load(self, key):
            raise RemoteStorageTransientError("storage_timeout", "https://user:secret@private.example")

    storage = FailingStorage()
    queue = SmartQueue(redis_client=isolated_redis, queue_name="review-storage",
                       storage=storage, large_threshold=0)
    task_id = queue.enqueue(TaskSpec("work", {"value": "large"})).task_id
    result = QueueAdmin(redis_client=isolated_redis, storage=storage).resolve_payload(
        task_id, queue.base, "history"
    )
    assert "secret" not in json.dumps(result, ensure_ascii=False)
    assert "storage_timeout" in result["_note"]


def test_storage_error_code_is_also_redacted(isolated_redis):
    error = RemoteStorageTransientError("https://user:secret@private.example", "secret")
    assert "secret" not in QueueAdmin(redis_client=isolated_redis)._safe_error_note(error)


@pytest.mark.parametrize("code", ["storage_timeout", "https://user:secret@private.example"])
def test_worker_payload_failure_does_not_persist_storage_secrets(isolated_redis, code):
    class FailingStorage:
        def save_bytes(self, data, **kwargs):
            return "payload-ref"

        def load(self, key):
            raise RemoteStorageTransientError(code, "https://user:secret@private.example")

    queue = SmartQueue(redis_client=isolated_redis, queue_name="review-worker-redaction",
                       storage=FailingStorage(), large_threshold=0, max_attempts=1)
    task_id = queue.enqueue(TaskSpec("work", {"value": "large"})).task_id
    assert queue.pop_claim_no_wait() is None
    record = queue.history.get(task_id)
    assert "secret" not in json.dumps(record, ensure_ascii=False)
    assert record["reason_code"] == ("storage_timeout" if code == "storage_timeout" else "storage_error")


def test_storage_legacy_signature_is_used_without_duplicate_upload(isolated_redis):
    class LegacyStorage:
        def __init__(self):
            self.calls = 0

        def save_bytes(self, data):
            self.calls += 1
            return "legacy-ref"

    storage = LegacyStorage()
    queue = SmartQueue(redis_client=isolated_redis, queue_name="review-old-storage",
                       storage=storage, large_threshold=0)
    assert queue.enqueue(TaskSpec("work", {"value": "large"})).accepted
    assert storage.calls == 1


def test_storage_internal_type_error_is_not_retried_as_old_signature(isolated_redis):
    class BrokenStorage:
        def __init__(self):
            self.calls = 0

        def save_bytes(self, data, retain_until=None):
            self.calls += 1
            raise TypeError("https://user:secret@private.example")

    storage = BrokenStorage()
    queue = SmartQueue(redis_client=isolated_redis, queue_name="review-broken-storage",
                       storage=storage, large_threshold=0)
    with pytest.raises(RemoteStorageConfigurationError) as failure:
        queue.enqueue(TaskSpec("work", {"value": "large"}))
    assert "secret" not in str(failure.value)
    assert storage.calls == 1 and queue.size() == 0


def test_storage_rejects_cross_origin_multipart_upload(tmp_path, monkeypatch):
    from qtask_list.remote_storage import server

    monkeypatch.delenv("QTASK_STORAGE_TOKEN", raising=False)
    monkeypatch.setattr(server, "DATA_DIR", tmp_path)
    with TestClient(server.app, client=("127.0.0.1", 50000)) as client:
        response = client.post("/api/storage/upload", headers={"Origin": "https://unrelated.example"},
                               files={"file": ("payload.bin", b"payload")})
    assert response.status_code == 403
    assert not list(tmp_path.rglob("*.meta.json"))


def test_storage_allows_same_origin_and_server_clients(tmp_path, monkeypatch):
    from qtask_list.remote_storage import server

    monkeypatch.delenv("QTASK_STORAGE_TOKEN", raising=False)
    monkeypatch.setattr(server, "DATA_DIR", tmp_path)
    with TestClient(server.app, client=("127.0.0.1", 50000)) as client:
        same = client.post("/api/storage/upload", headers={"Origin": "http://testserver:80"},
                           files={"file": ("payload.bin", b"same")})
        direct = client.post("/api/storage/upload", files={"file": ("payload.bin", b"direct")})
        opaque = client.post("/api/storage/upload", headers={"Origin": "null"},
                             files={"file": ("payload.bin", b"opaque")})
    assert (same.status_code, direct.status_code, opaque.status_code) == (200, 200, 403)


def test_requeue_retry_wait_uses_delay_container(isolated_redis):
    queue = SmartQueue(redis_client=isolated_redis, queue_name="review-retry", retry_backoff_base=60)
    task_id = queue.enqueue(TaskSpec("work", {})).task_id
    assert queue.fail(queue.pop_claim_no_wait().raw_message, "temporary")

    result = QueueAdmin(redis_client=isolated_redis).requeue_task(queue.base, task_id, "retry_wait")
    assert result["moved"] == 1
    assert queue.size() == 1 and queue.delay_size() == 0
    assert int(isolated_redis.hget(queue.metrics_key, "retry_wait.current")) == 0


def test_requeue_retry_wait_does_not_move_scheduled_task(isolated_redis):
    queue = SmartQueue(redis_client=isolated_redis, queue_name="review-scheduled")
    task_id = queue.enqueue(TaskSpec("work", {}, not_before_at=datetime.now(timezone.utc) + timedelta(hours=1))).task_id
    result = QueueAdmin(redis_client=isolated_redis).requeue_task(queue.base, task_id, "retry_wait")
    assert result["moved"] == 0 and queue.delay_size() == 1 and queue.size() == 0


@pytest.mark.parametrize("argument", ["delay_seconds", "expire_seconds", "dedup_ttl"])
def test_batch_rejects_negative_relative_time_before_writes(isolated_redis, argument):
    queue = SmartQueue(redis_client=isolated_redis, queue_name="review-batch")
    with pytest.raises(ValueError, match=">= 0"):
        queue.push_batch([{"action": "work"}], logical_keys=["item"], **{argument: -1})
    assert queue.size() == 0 and queue.delay_size() == 0


def test_archive_preserves_a_concurrent_replay_update(isolated_redis, tmp_path, monkeypatch):
    queue = SmartQueue(redis_client=isolated_redis, queue_name="review-archive")
    task_id = queue.enqueue(TaskSpec("work", {})).task_id
    queue.clear()
    old = time.time() - 2 * 86400
    isolated_redis.hset(f"qtask:task:{task_id}", "finished_at", old)
    isolated_redis.zadd(queue.history.idx_key, {task_id: old})
    manager = ArchiveManager("redis://isolated/0", db_dir=str(tmp_path))
    manager.r = isolated_redis
    replayed = []

    def replay_after_snapshot(_queue_name, identifier):
        if not replayed:
            replayed.append(queue.replay_task(identifier).task_id)
        return False

    monkeypatch.setattr(manager, "_has_operational_message", replay_after_snapshot)
    manager.archive_to_sqlite(queue.base)
    record = queue.history.get(task_id)
    archive = {}
    for path in tmp_path.glob("*.db"):
        with sqlite3.connect(path) as connection:
            row = connection.execute("SELECT raw_data FROM task_history WHERE task_id=?", (task_id,)).fetchone()
        if row:
            archive = json.loads(row[0])
    assert replayed[0] in str((record or archive).get("replayed_by", ""))


def test_worker_maintenance_injects_its_redis_client(isolated_redis, tmp_path, monkeypatch):
    import qtask_list.archiver as archiver

    worker = Worker("redis://wrong-destination/0", "review-archive-injection",
                    redis_client=isolated_redis, archive_dir=str(tmp_path))
    received = []

    def capture(_url, *, db_dir, redis_client):
        received.append((db_dir, redis_client))
        return None

    monkeypatch.setattr(archiver, "ArchiveManager", capture)
    worker._maintenance_loop()
    assert received == [(str(tmp_path), isolated_redis)]


def test_two_archivers_cannot_replace_newer_snapshot_with_older(isolated_redis, tmp_path, monkeypatch):
    queue = SmartQueue(redis_client=isolated_redis, queue_name="review-concurrent-archive")
    task_id = queue.enqueue(TaskSpec("work", {})).task_id
    queue.clear()
    old = time.time() - 2 * 86400
    isolated_redis.hset(f"qtask:task:{task_id}", "finished_at", old)
    isolated_redis.zadd(queue.history.idx_key, {task_id: old})
    first = ArchiveManager("redis://isolated/0", db_dir=str(tmp_path), redis_client=isolated_redis)
    second = ArchiveManager("redis://isolated/0", db_dir=str(tmp_path), redis_client=isolated_redis)
    snapped = threading.Event()
    resume = threading.Event()
    failures = []
    counts = []

    def pause_after_snapshot(_queue, _identifier):
        snapped.set()
        if not resume.wait(5):
            raise RuntimeError("archive test timed out")
        return False

    monkeypatch.setattr(first, "_has_operational_message", pause_after_snapshot)

    def archive(manager):
        try:
            counts.append(manager.archive_to_sqlite(queue.base))
        except Exception as exc:
            failures.append(exc)

    thread_one = threading.Thread(target=archive, args=(first,))
    thread_one.start()
    assert snapped.wait(5)
    replay_id = queue.replay_task(task_id).task_id
    thread_two = threading.Thread(target=archive, args=(second,))
    thread_two.start()
    resume.set()
    thread_one.join(10)
    thread_two.join(10)
    assert not thread_one.is_alive() and not thread_two.is_alive() and not failures
    assert sorted(counts) == [0, 1]
    with sqlite3.connect(next(tmp_path.glob("*.db"))) as connection:
        row = connection.execute("SELECT raw_data FROM task_history WHERE task_id=?", (task_id,)).fetchone()
    assert replay_id in str(json.loads(row[0])["replayed_by"])


def test_task_budget_stays_with_the_producer_instance(isolated_redis):
    producer = SmartQueue(redis_client=isolated_redis, queue_name="review-budget", max_attempts=1)
    worker = Worker("redis://isolated/0", "review-budget", redis_client=isolated_redis, max_attempts=7)
    task_id = producer.enqueue(TaskSpec("work", {})).task_id
    claim = worker.queue.pop_claim_no_wait()
    assert claim is not None and claim.context.max_attempts == 1
    assert worker.queue.fail(claim.raw_message, "failure")
    assert producer.history.get(task_id)["outcome"] == "failed"


def test_cli_replay_file_and_confirmation_have_distinct_short_options(isolated_redis, tmp_path):
    from typer.testing import CliRunner
    from qtask_list.cli.__main__ import app

    queue = SmartQueue(redis_client=isolated_redis, queue_name="review-cli", max_attempts=1)
    task_id = queue.enqueue(TaskSpec("work", {"value": "old"})).task_id
    claim = queue.pop_claim_no_wait()
    assert claim and queue.fail(claim.raw_message, "failed", permanent=True)
    payload_file = tmp_path / "payload.json"
    payload_file.write_text(json.dumps({"value": "new"}), encoding="utf-8")
    result = CliRunner().invoke(app, ["task", "replay", task_id, "--queue", queue.base,
                                     "-f", str(payload_file), "-y", "--json"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["accepted"]
    replay = queue.pop_claim_no_wait()
    assert replay and replay.payload == {"value": "new"}


def test_pipeline_examples_retain_downstream_identities(isolated_redis, monkeypatch):
    root = Path(__file__).resolve().parents[1]
    fetch = runpy.run_path(str(root / "examples/06_pipeline/stockev/fetch_worker.py"))
    calculate = runpy.run_path(str(root / "examples/06_pipeline/finance/calculate_worker.py"))
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)
    context = SimpleNamespace(task_id="parent-id", trace_id="trace-id")
    task = {"symbol": "AAPL", "url": "https://example.com"}
    for stage, payload, target in (
        (fetch["fetch_stock"], task, "review-calculate"),
        (calculate["calculate_ma"], {"symbol": "AAPL", "price": 1, "volume": 10,
                                     "timestamp": "2026-10-28T00:00:00Z"}, "review-store"),
    ):
        queue = SmartQueue(redis_client=isolated_redis, queue_name=target)
        original = stage(payload, context).emissions[0]
        assert queue.enqueue(original).accepted
        claim = queue.pop_claim_no_wait()
        assert claim and queue.ack(claim.raw_message)
        duplicate = queue.enqueue(stage(payload, context).emissions[0])
        assert not duplicate.accepted and duplicate.reason == "duplicate_retained"


def test_news_example_retries_on_stop_and_labels_planned_emissions():
    root = Path(__file__).resolve().parents[1]
    example = runpy.run_path(str(root / "examples/07_advanced/news_discover.py"))
    context = SimpleNamespace(task_id="parent", trace_id="trace", stop_requested=True)
    with pytest.raises(RetryableTaskError, match="尚未投递"):
        example["discover_news"]({"urls": ["https://example.com/a"]}, context)
    context.stop_requested = False
    result = example["discover_news"]({"urls": ["https://example.com/a"]}, context)
    assert result.value["planned_emissions"] == 1
    assert result.emissions[0].dedup_until > datetime.now(timezone.utc) + timedelta(days=6)


def test_scheduler_example_uses_utc_and_valid_slot_sizes():
    root = Path(__file__).resolve().parents[1]
    example = runpy.run_path(str(root / "examples/07_advanced/scheduler.py"))
    now = datetime(2026, 10, 28, 17, 8, tzinfo=timezone(timedelta(hours=8)))
    assert example["floor_to_slot"](now) == datetime(2026, 10, 28, 9, 5, tzinfo=timezone.utc)
    for minutes in (0, -1, 7, 61, True):
        with pytest.raises(ValueError, match="divisor of 60"):
            example["floor_to_slot"](now, minutes)


def test_deadline_example_demonstrates_skipped_task(isolated_redis, capsys):
    root = Path(__file__).resolve().parents[1]
    runpy.run_path(str(root / "examples/04_delay_deadline/demo.py"), run_name="__main__")
    output = capsys.readouterr().out
    assert "截止后 pop: 拒绝（正确，任务已跳过）" in output


def test_expired_cleanup_preserves_unreadable_metadata(tmp_path, monkeypatch):
    from qtask_list.remote_storage import server

    monkeypatch.setattr(server, "DATA_DIR", tmp_path)
    monkeypatch.setattr(server, "_ttl_seconds", 1)
    key = "a" * 32
    path = server._key_path(key)
    path.parent.mkdir()
    path.write_bytes(b"retained")
    server._meta_path(key).write_text('{"retain_until":', encoding="utf-8")
    old = time.time() - 10
    os.utime(path, (old, old))
    server._cleanup_expired()
    assert path.read_bytes() == b"retained"


def test_retention_extension_cannot_succeed_before_concurrent_ttl_deletion(tmp_path, monkeypatch):
    from fastapi import HTTPException
    from qtask_list.remote_storage import server

    monkeypatch.setattr(server, "DATA_DIR", tmp_path)
    monkeypatch.setattr(server, "_ttl_seconds", 1)
    key = "b" * 32
    path = server._key_path(key)
    path.parent.mkdir()
    path.write_bytes(b"payload")
    old = time.time() - 10
    os.utime(path, (old, old))
    entering_delete = threading.Event()
    release_delete = threading.Event()
    retain_finished = threading.Event()
    original_unlink = Path.unlink
    result = []

    def paused_unlink(item, *args, **kwargs):
        if item == path:
            entering_delete.set()
            if not release_delete.wait(5):
                raise RuntimeError("cleanup test timed out")
        return original_unlink(item, *args, **kwargs)

    def extend():
        try:
            result.append(asyncio.run(server.retain(key, server.RetentionRequest(retain_until=time.time() + 3600))))
        except HTTPException as exc:
            result.append(exc.status_code)
        finally:
            retain_finished.set()

    monkeypatch.setattr(Path, "unlink", paused_unlink)
    cleaner = threading.Thread(target=server._cleanup_expired)
    cleaner.start()
    assert entering_delete.wait(5)
    extender = threading.Thread(target=extend)
    extender.start()
    retain_finished.wait(0.3)
    release_delete.set()
    cleaner.join(5)
    extender.join(5)
    assert not cleaner.is_alive() and not extender.is_alive() and result
    assert result[0] == 404 or (isinstance(result[0], dict) and path.exists())


def test_expired_cleanup_respects_valid_retention_and_ignores_other_files(tmp_path, monkeypatch):
    from qtask_list.remote_storage import server

    monkeypatch.setattr(server, "DATA_DIR", tmp_path)
    monkeypatch.setattr(server, "_ttl_seconds", 1)
    old = time.time() - 10
    keys = ["a" * 32, "b" * 32, "c" * 32, "d" * 32]
    for key in keys:
        path = server._key_path(key)
        path.parent.mkdir(exist_ok=True)
        path.write_bytes(b"x")
        os.utime(path, (old, old))
    for key, retention in zip(keys[:3], [0, time.time() + 3600, old]):
        server._meta_path(key).write_text(json.dumps({"retain_until": retention}), encoding="utf-8")
    unrelated = tmp_path / "important.txt"
    unrelated.write_text("preserve", encoding="utf-8")
    os.utime(unrelated, (old, old))
    server._cleanup_expired()
    assert [server._key_path(key).exists() for key in keys] == [True, True, False, False]
    assert unrelated.read_text(encoding="utf-8") == "preserve"
    assert asyncio.run(server.health())["files"] == 2


def test_retention_metadata_replacement_is_atomic_on_error(tmp_path, monkeypatch):
    from qtask_list.remote_storage import server

    monkeypatch.setattr(server, "DATA_DIR", tmp_path)
    key = "e" * 32
    path = server._key_path(key)
    path.parent.mkdir()
    path.write_bytes(b"payload")
    original = json.dumps({"retain_until": time.time() + 60})
    server._meta_path(key).write_text(original, encoding="utf-8")

    def fail_replace(_source, _destination):
        raise OSError("simulated replacement failure")

    monkeypatch.setattr(server.os, "replace", fail_replace)
    with pytest.raises(OSError, match="simulated replacement failure"):
        server._extend_retention(key, time.time() + 3600)
    assert server._meta_path(key).read_text(encoding="utf-8") == original
    assert not list(path.parent.glob("*.tmp"))


def test_retention_lock_serializes_separate_processes(tmp_path, monkeypatch):
    from qtask_list.remote_storage import server

    monkeypatch.setattr(server, "DATA_DIR", tmp_path)
    marker = tmp_path / "acquired"
    program = """
from pathlib import Path
import sys
from qtask_list.remote_storage import server
server.DATA_DIR = Path(sys.argv[1])
print('ready', flush=True)
with server._retention_guard():
    Path(sys.argv[2]).write_text('acquired')
"""
    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
    with server._retention_guard():
        process = subprocess.Popen([sys.executable, "-c", program, str(tmp_path), str(marker)],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
        assert process.stdout.readline().strip() == "ready"
        time.sleep(0.3)
        assert not marker.exists()
    stdout, stderr = process.communicate(timeout=5)
    assert process.returncode == 0, (stdout, stderr)
    assert marker.read_text() == "acquired"
