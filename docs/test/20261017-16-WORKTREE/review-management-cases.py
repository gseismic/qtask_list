"""只在私有 Redis 中复现管理界面与 API 的审查边界。"""

import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import redis
from fastapi.testclient import TestClient


ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))


def run_cases(client, connection):
    from qtask_list.dashboard import main as dashboard
    from qtask_list.models import TaskSpec
    from qtask_list.queue import SmartQueue

    results = {}

    def queue(name, **options):
        return SmartQueue(redis_client=connection, queue_name=name, **options)

    def fail_once(instance, *, deadline=None):
        task = instance.enqueue(TaskSpec(action="review_action", payload={"value": 1},
                                         start_deadline_at=deadline))
        claim = instance.pop_claim_no_wait()
        assert claim is not None
        instance.fail(claim.raw_message, reason="review failure")
        return task.task_id

    connection.flushdb()
    instance = queue("review:metadata", max_attempts=1)
    task_id = instance.enqueue(TaskSpec(action="review_action", payload={})).task_id
    ready = client.get("/api/queue/review:metadata/tasks", params={"state": "ready"}).json()["tasks"][0]
    claim = instance.pop_claim_no_wait()
    assert claim is not None
    processing = client.get("/api/queue/review:metadata/tasks", params={"state": "processing"}).json()["tasks"][0]
    detail = client.get(f"/api/task/{task_id}").json()
    results["operational_metadata"] = {
        "ready": {key: ready.get(key) for key in ("created_at", "updated_at", "attempt", "max_attempts")},
        "processing": {key: processing.get(key) for key in ("created_at", "updated_at", "attempt", "max_attempts")},
        "detail": {key: detail.get(key) for key in ("created_at", "updated_at", "attempt", "max_attempts")},
    }

    connection.flushdb()
    instance = queue("review:paging")
    for number in range(501):
        instance.enqueue(TaskSpec(action="review_action", payload={"number": number}))
    raw = client.get("/api/queue/review:paging/tasks", params={"state": "ready", "limit": 500}).json()
    indexed = client.get("/api/tasks", params={"queue": instance.base, "status": "ready", "limit": 500}).json()
    results["raw_list_501"] = {
        "queue_count": instance.size(), "returned": len(raw["tasks"]),
        "next_cursor": raw.get("next_cursor"), "has_more": raw.get("has_more"),
        "indexed_next_cursor": bool(indexed.get("next_cursor")),
    }

    connection.flushdb()
    now = time.time()
    pipe = connection.pipeline()
    pipe.zadd("qtask:queues", {"review:sparse": now})
    for number in range(5001):
        identifier = f"sparse-{number}"
        pipe.hset(f"qtask:task:{identifier}", mapping={
            "task_id": identifier, "_queue": "review:sparse", "action": "review_action",
            "created_at": now - number, "updated_at": now, "status": "completed",
            "outcome": "completed", "location": "history", "operational_message": 0,
        })
        pipe.zadd("qtask:hist:review:sparse", {identifier: now - number})
    pipe.execute()
    sparse = client.get("/api/queue/review:sparse/tasks", params={"state": "ready", "created_after": 0})
    paged = client.get("/api/tasks", params={"queue": "review:sparse", "status": "ready", "created_after": 0})
    results["sparse_time_filter"] = {
        "queue_endpoint_status": sparse.status_code, "queue_endpoint_body": sparse.text,
        "global_endpoint_status": paged.status_code,
        "global_scan_limited": paged.json().get("scan_limited"),
        "global_has_more": paged.json().get("has_more"),
    }

    connection.flushdb()
    instance = queue("review:replay", max_attempts=1)
    task_id = fail_once(instance)
    original = client.get(f"/api/task/{task_id}").json()
    first = client.post(f"/api/task/{task_id}/replay", json={"queue": instance.base}).json()
    second = client.post(f"/api/task/{task_id}/replay", json={"queue": instance.base}).json()
    final_source = client.get(f"/api/task/{task_id}").json()
    replay = client.get(f"/api/task/{first['task_id']}").json()
    results["replay_policy_and_lineage"] = {
        "first_accepted": first["accepted"], "second_accepted": second["accepted"],
        "first_task_id": first["task_id"], "second_task_id": second["task_id"],
        "source_replayed_by": final_source.get("replayed_by"),
        "source_max_attempts": original["max_attempts"], "replay_max_attempts": replay["max_attempts"],
    }

    connection.flushdb()
    instance = queue("review:expired-dlq", max_attempts=1)
    task_id = fail_once(instance, deadline=datetime.fromtimestamp(time.time() + 0.5, timezone.utc))
    time.sleep(0.6)
    result = client.post("/api/queue/review:expired-dlq/requeue-dlq", json={"confirm_bulk": True})
    results["bulk_expired_dlq"] = {
        "http_status": result.status_code, "result": result.json(),
        "dlq_remaining": instance.dlq_size(), "original_task_id": task_id,
    }

    connection.flushdb()
    instance = queue("review/valid-name")
    instance.enqueue(TaskSpec(action="review_action", payload={}))
    name = quote(instance.base, safe="")
    get_queue = client.get(f"/api/queue/{name}")
    get_tasks = client.get(f"/api/queue/{name}/tasks")
    results["slash_queue_name"] = {
        "registered": instance.base in dashboard.admin.queue_names(),
        "queue_api_status": get_queue.status_code, "tasks_api_status": get_tasks.status_code,
        "global_search_count": client.get("/api/tasks", params={"queue": instance.base}).json()["count"],
    }

    for username, password in [("admin", "中文密码"), ("管理员", "ascii-password")]:
        os.environ["QTASK_DASHBOARD_USER"] = username
        os.environ["QTASK_DASHBOARD_PASSWORD"] = password
        with TestClient(dashboard.app, raise_server_exceptions=False) as auth_client:
            login = auth_client.post("/api/login", json={"username": username, "password": password})
            results[f"unicode_login_{'password' if username == 'admin' else 'username'}"] = {
                "status": login.status_code, "body": login.text,
            }
    for name in ("QTASK_DASHBOARD_USER", "QTASK_DASHBOARD_PASSWORD"):
        os.environ.pop(name, None)

    connection.flushdb()
    instance = queue("review:delete-race")
    deletion_client = dashboard.admin.r
    original_delete = deletion_client.delete
    injected = []

    def delete_then_enqueue(*keys):
        response = original_delete(*keys)
        if instance.base in keys and not injected:
            injected.append(instance.enqueue(TaskSpec(action="review_action", payload={})).task_id)
        return response

    deletion_client.delete = delete_then_enqueue
    try:
        deleted = client.delete("/api/queue/review:delete-race", params={"confirm": True})
    finally:
        deletion_client.delete = original_delete
    results["delete_concurrent_enqueue"] = {
        "delete_status": deleted.status_code, "accepted_task_id": injected[0],
        "message_remaining": instance.size(),
        "history_remaining": dashboard.admin.get_task(injected[0]) is not None,
        "registered": instance.base in dashboard.admin.queue_names(),
    }

    connection.flushdb()
    from qtask_list.worker import Worker
    worker = Worker("redis://private-review/0", "delete-active", namespace="review",
                    redis_client=connection, worker_id="racing-worker")
    original_clear = SmartQueue.clear
    active = {}

    def clear_then_start_worker(current, *args, **kwargs):
        response = original_clear(current, *args, **kwargs)
        worker._refresh_heartbeat()
        active["task_id"] = worker.queue.enqueue(TaskSpec(action="review_action", payload={})).task_id
        active["claim"] = worker.queue.pop_claim_no_wait()
        assert active["claim"] is not None
        active["heartbeat_before_delete"] = bool(connection.exists(worker._heartbeat_key))
        return response

    SmartQueue.clear = clear_then_start_worker
    try:
        deleted = client.delete("/api/queue/review:delete-active", params={"confirm": True})
    finally:
        SmartQueue.clear = original_clear
    results["delete_concurrent_active_worker"] = {
        "delete_status": deleted.status_code,
        "active_before_final_delete": active["heartbeat_before_delete"],
        "processing_remaining": connection.llen(worker.queue.processing),
        "heartbeat_remaining": bool(connection.exists(worker._heartbeat_key)),
        "history_remaining": dashboard.admin.get_task(active["task_id"]) is not None,
        "handler_ack_succeeds": worker.queue.ack(active["claim"].raw_message),
    }

    connection.flushdb()
    instance = queue("review:cursor-deletion")
    task_ids = [instance.enqueue(TaskSpec(action="review_action", payload={})).task_id for _ in range(3)]
    first_page = client.get("/api/tasks", params={"queue": instance.base, "limit": 1}).json()
    first_id = first_page["tasks"][0]["task_id"]
    removed = client.delete(f"/api/task/{first_id}", params={"queue": instance.base})
    assert removed.status_code == 200
    next_page = client.get("/api/tasks", params={"queue": instance.base, "limit": 1,
                                                 "cursor": first_page["next_cursor"]}).json()
    returned_ids = [item["task_id"] for item in next_page["tasks"]]
    results["cursor_after_prior_page_deletion"] = {
        "expected_remaining": [task_id for task_id in task_ids if task_id != first_id],
        "returned_ids": returned_ids, "has_more": next_page["has_more"],
        "missing_ids": [task_id for task_id in task_ids if task_id not in returned_ids and task_id != first_id],
    }

    origin = "https://unrelated.example"
    headers = {"Origin": origin, "Access-Control-Request-Method": "POST",
               "Access-Control-Request-Headers": "content-type"}
    preflight = client.options("/api/queue/review:cors/tasks", headers=headers)
    created = client.post("/api/queue/review:cors/tasks", headers={"Origin": origin},
                          json={"payload": {"action": "review_action"}})
    results["cross_origin_default_auth_disabled"] = {
        "preflight_status": preflight.status_code,
        "preflight_allow_origin": preflight.headers.get("access-control-allow-origin"),
        "post_status": created.status_code,
        "post_accepted": created.json().get("accepted"),
        "post_allow_origin": created.headers.get("access-control-allow-origin"),
    }

    connection.config_set("maxmemory", 512 * 1024)
    try:
        health = client.get("/api/health").json()
        results["redis_memory_over_limit"] = {
            "health_status": health["status"],
            "used_memory": health["memory"]["used_memory"],
            "maxmemory": health["memory"]["maxmemory"],
            "memory_status": health["memory"]["status"],
            "dashboard_threshold_mb": dashboard.monitor.threshold_mb,
        }
    finally:
        connection.config_set("maxmemory", 0)
    return results


def main():
    with tempfile.TemporaryDirectory(prefix="qtask-review-") as directory:
        socket = str(Path(directory) / "redis.sock")
        server = subprocess.Popen([
            "redis-server", "--port", "0", "--unixsocket", socket, "--unixsocketperm", "700",
            "--save", "", "--appendonly", "no", "--dir", directory,
        ], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        connection = redis.Redis(unix_socket_path=socket, decode_responses=True)
        try:
            for _ in range(200):
                try:
                    if connection.ping():
                        break
                except redis.RedisError:
                    time.sleep(0.01)
            else:
                raise RuntimeError("私有 Redis 启动失败")
            os.environ["REDIS_URL"] = f"unix://{socket}"
            for name in ("QTASK_DASHBOARD_AUTH", "QTASK_DASHBOARD_PASSWORD", "QTASK_DASHBOARD_USER",
                         "QTASK_DASHBOARD_SECRET", "QTASK_STORAGE_URL"):
                os.environ.pop(name, None)
            from qtask_list.dashboard.main import app
            with TestClient(app, raise_server_exceptions=False) as client:
                results = run_cases(client, connection)
            output = json.dumps(results, ensure_ascii=False, indent=2) + "\n"
            destination = Path(__file__).parent / "review-management-results.json"
            sequence = 2
            while destination.exists():
                destination = Path(__file__).parent / f"review-management-results-{sequence}.json"
                sequence += 1
            destination.write_text(output, encoding="utf-8")
            print(output)
        finally:
            connection.close()
            server.terminate()
            server.wait(timeout=5)


if __name__ == "__main__":
    main()
