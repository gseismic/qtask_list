import json
import time

import pytest
import redis
from typer.testing import CliRunner

from qtask_list.cli.__main__ import app


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def r():
    client = redis.from_url("redis://localhost:6379/0", decode_responses=True)
    cleanup_test_keys(client)
    yield client
    cleanup_test_keys(client)


def cleanup_test_keys(client):
    for hist_key in client.scan_iter("qtask:hist:stockev_list:*"):
        try:
            for task_id in client.zrange(hist_key, 0, -1):
                client.delete(f"qtask:task:{task_id}")
        finally:
            client.delete(hist_key)
    for hist_key in client.scan_iter("qtask:hist:testns:*"):
        try:
            for task_id in client.zrange(hist_key, 0, -1):
                client.delete(f"qtask:task:{task_id}")
        finally:
            client.delete(hist_key)
    for pattern in ("qtask:deadline:stockev_list:*", "qtask:metrics:stockev_list:*",
                    "qtask:workers:stockev_list:*", "qtask:deadline:testns:*",
                    "qtask:metrics:testns:*", "qtask:workers:testns:*"):
        for key in client.scan_iter(pattern):
            client.delete(key)
    for queue in client.zrange("qtask:queues", 0, -1):
        if queue.startswith(("stockev_list:", "testns:")):
            client.zrem("qtask:queues", queue)
    for task_id in ["abc123", "clean1", "test1"]:
        client.delete(f"qtask:task:{task_id}")
    for key in client.scan_iter("stockev_list:*"):
        client.delete(key)
    for key in client.scan_iter("testns:*"):
        client.delete(key)


def make_msg(task_id: str, payload: dict | None = None) -> str:
    return json.dumps({"task_id": task_id, "payload": json.dumps(payload or {})})


class TestCLI:
    def test_status_empty(self, runner, r):
        result = runner.invoke(app, ["status"])
        assert result.exit_code == 0

    def test_status_with_queue(self, runner, r):
        r.zadd("qtask:queues", {"stockev_list:test": time.time()})
        r.lpush("stockev_list:test", make_msg("status-1", {"action": "status"}))

        result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        assert "stockev_list:test" in result.stdout

    def test_status_specific_queue_with_namespace(self, runner, r):
        r.lpush("stockev_list:specific", "test")

        result = runner.invoke(app, ["status", "specific", "-n", "stockev_list"])
        assert result.exit_code == 0
        assert "stockev_list:specific" in result.stdout

    def test_status_namespace_problem_filter_and_json(self, runner, r):
        # 值班脚本应只接收目标 namespace 的可行动异常，JSON 不混入表格文本。
        r.zadd("qtask:queues", {name: time.time() for name in (
            "stockev_list:status_bad", "stockev_list:status_ok", "testns:status_other",
        )})
        r.zadd("qtask:workers:stockev_list:status_ok", {"live": time.time()})
        r.lpush("stockev_list:status_bad", make_msg("status-bad"))
        r.lpush("stockev_list:status_ok", make_msg("status-ok"))
        r.set("stockev_list:status_ok:worker:live", str(time.time()), ex=120)
        r.lpush("testns:status_other", make_msg("status-other"))

        result = runner.invoke(app, ["status", "--namespace", "stockev_list", "--problems-only", "--json"])
        assert result.exit_code == 0
        rows = json.loads(result.stdout)
        assert [row["name"] for row in rows] == ["stockev_list:status_bad"]
        assert rows[0]["queue"] == 1

        all_rows = runner.invoke(app, ["status", "--namespace", "stockev_list", "--json"])
        assert {row["name"] for row in json.loads(all_rows.stdout)} == {
            "stockev_list:status_bad", "stockev_list:status_ok"
        }

    def test_status_table_and_single_json_include_operational_fields(self, runner, r):
        # 人工巡检应直接看到重试等待、过期和 Worker 数，单队列 JSON 供脚本复用。
        queue = "stockev_list:status_fields"
        r.zadd("qtask:queues", {queue: time.time()})
        r.lpush(queue, make_msg("status-fields"))
        r.zadd(f"qtask:deadline:{queue}", {"status-fields": time.time() - 10})
        table = runner.invoke(app, ["status"])
        assert table.exit_code == 0
        assert "Rw=retry_wait" in table.stdout
        assert "Due=overdue" in table.stdout
        assert "W=online/stale" in table.stdout
        single = runner.invoke(app, ["status", queue, "--json"])
        assert json.loads(single.stdout)["deadline_missed"] == 1

    def test_push_and_peek_ready_task(self, runner, r):
        result = runner.invoke(
            app,
            ["push", "stockev_list:push_test", '{"action":"fetch","symbol":"AAPL"}'],
        )

        assert result.exit_code == 0
        assert r.llen("stockev_list:push_test") == 1

        raw = r.lindex("stockev_list:push_test", 0)
        msg = json.loads(raw)
        assert msg["version"] == 2
        assert msg["payload"]["kind"] == "inline"
        payload = msg["payload"]["data"]
        assert payload == {"action": "fetch", "symbol": "AAPL"}
        assert r.exists(f"qtask:task:{msg['task_id']}") == 1

        peek_result = runner.invoke(app, ["peek", "stockev_list:push_test", "--json"])
        assert peek_result.exit_code == 0
        assert "fetch" in peek_result.stdout
        assert "AAPL" in peek_result.stdout

    def test_push_with_namespace_uses_admin_queue_name(self, runner, r):
        result = runner.invoke(
            app,
            ["push", "push_ns", '{"action":"fetch"}', "-n", "testns", "--json"],
        )

        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert output["queue"] == "testns:push_ns"
        assert r.llen("testns:push_ns") == 1

    def test_peek_processing_reads_worker_specific_key(self, runner, r):
        queue = "stockev_list:processing_peek"
        r.zadd(f"qtask:workers:{queue}", {"worker-1": time.time()})
        r.lpush(f"{queue}:processing:worker-1", make_msg("proc-1", {"action": "work"}))

        result = runner.invoke(app, ["peek", queue, "--state", "processing", "--json"])

        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert output[0]["task_id"] == "proc-1"
        assert output[0]["state"] == "processing"
        assert output[0]["source"] == f"{queue}:processing:worker-1"

    def test_clear_queue_can_include_history(self, runner, r):
        push_result = runner.invoke(
            app,
            ["push", "stockev_list:clear_test", '{"action":"clear_me"}'],
        )
        assert push_result.exit_code == 0
        assert r.exists("qtask:hist:stockev_list:clear_test") == 1

        result = runner.invoke(
            app,
            ["clear", "stockev_list:clear_test", "--include-history", "--force"],
        )

        assert result.exit_code == 0
        assert "Cleared" in result.stdout
        assert r.exists("stockev_list:clear_test") == 0
        assert r.exists("qtask:hist:stockev_list:clear_test") == 0

    def test_clear_queue_can_preserve_dlq(self, runner, r):
        queue = "stockev_list:clear_keep_dlq"
        r.lpush(queue, make_msg("ready-1"))
        r.lpush(f"{queue}:dlq", make_msg("dlq-1"))

        result = runner.invoke(app, ["clear", queue, "--no-dlq", "--force"])

        assert result.exit_code == 0
        assert r.exists(queue) == 0
        assert message_ids(r, f"{queue}:dlq") == ["dlq-1"]

    def test_requeue_moves_dlq_to_ready(self, runner, r):
        r.lpush("stockev_list:dlq_test:dlq", make_msg("dlq-1"))
        r.lpush("stockev_list:dlq_test:dlq", make_msg("dlq-2"))
        r.hset("qtask:task:dlq-1", mapping={"task_id": "dlq-1", "status": "failed"})
        r.hset("qtask:task:dlq-2", mapping={"task_id": "dlq-2", "status": "failed"})
        r.zadd("qtask:hist:stockev_list:dlq_test", {"dlq-1": 1, "dlq-2": 2})

        result = runner.invoke(app, ["requeue", "stockev_list:dlq_test", "--force"])

        assert result.exit_code == 0
        assert r.llen("stockev_list:dlq_test:dlq") == 0
        assert r.llen("stockev_list:dlq_test") == 2
        assert r.hget("qtask:task:dlq-1", "status") == "pending"
        assert r.hget("qtask:task:dlq-2", "status") == "pending"

    def test_requeue_single_task_from_dlq(self, runner, r):
        r.lpush("stockev_list:single_dlq:dlq", make_msg("dlq-keep"))
        r.lpush("stockev_list:single_dlq:dlq", make_msg("dlq-move"))

        result = runner.invoke(
            app,
            ["requeue", "stockev_list:single_dlq", "--task-id", "dlq-move", "--force"],
        )

        assert result.exit_code == 0
        assert r.llen("stockev_list:single_dlq") == 1
        assert r.llen("stockev_list:single_dlq:dlq") == 1
        assert message_ids(r, "stockev_list:single_dlq") == ["dlq-move"]
        assert message_ids(r, "stockev_list:single_dlq:dlq") == ["dlq-keep"]

    def test_retry_moves_retry_to_ready(self, runner, r):
        r.lpush("stockev_list:retry_test:retry", make_msg("retry-1"))
        r.hset("qtask:task:retry-1", mapping={"task_id": "retry-1", "status": "failed"})
        r.zadd("qtask:hist:stockev_list:retry_test", {"retry-1": 1})

        result = runner.invoke(app, ["retry", "stockev_list:retry_test"])

        assert result.exit_code == 0
        assert r.llen("stockev_list:retry_test:retry") == 0
        assert r.llen("stockev_list:retry_test") == 1
        assert r.hget("qtask:task:retry-1", "status") == "pending"

    def test_recover_skips_active_worker_processing_by_default(self, runner, r):
        queue = "stockev_list:proc_test"
        r.lpush(f"{queue}:processing", make_msg("legacy"))
        r.lpush(f"{queue}:processing:active", make_msg("active"))
        r.lpush(f"{queue}:processing:stale", make_msg("stale"))
        r.zadd(f"qtask:workers:{queue}", {"active": time.time(), "stale": time.time()})
        r.set(f"{queue}:worker:active", "1", ex=60)

        result = runner.invoke(app, ["recover", queue])

        assert result.exit_code == 0
        assert "Skipped 1 active" in result.stdout
        assert "Skipped legacy processing" in result.stdout
        assert r.llen(queue) == 1
        assert r.llen(f"{queue}:processing") == 1
        assert r.llen(f"{queue}:processing:stale") == 0
        assert r.llen(f"{queue}:processing:active") == 1

        forced = runner.invoke(app, ["recover", queue, "--force-active", "--yes"])

        assert forced.exit_code == 0
        assert r.llen(queue) == 3
        assert r.llen(f"{queue}:processing") == 0
        assert r.llen(f"{queue}:processing:active") == 0

    def test_history_can_get_task_without_queue_name(self, runner, r):
        r.hset("qtask:task:abc123", mapping={"task_id": "abc123", "action": "test"})

        result = runner.invoke(app, ["history", "-t", "abc123"])

        assert result.exit_code == 0
        assert "abc123" in result.stdout

    def test_history_lists_queue_history_via_admin(self, runner, r):
        r.hset(
            "qtask:task:hist-list-1",
            mapping={
                "task_id": "hist-list-1",
                "action": "listed_action",
                "status": "completed",
                "created_at": 1,
            },
        )
        r.zadd("qtask:hist:stockev_list:hist_list", {"hist-list-1": 1})

        result = runner.invoke(app, ["history", "stockev_list:hist_list"])

        assert result.exit_code == 0
        assert "listed_action" in result.stdout

    def test_task_get_and_delete(self, runner, r):
        push_result = runner.invoke(
            app,
            ["push", "stockev_list:task_delete", '{"action":"delete_me"}'],
        )
        assert push_result.exit_code == 0
        raw = r.lindex("stockev_list:task_delete", 0)
        task_id = json.loads(raw)["task_id"]

        get_result = runner.invoke(app, ["task", "get", task_id])
        assert get_result.exit_code == 0
        assert "delete_me" in get_result.stdout

        delete_result = runner.invoke(
            app,
            ["task", "delete", task_id, "--queue", "stockev_list:task_delete", "--force"],
        )

        assert delete_result.exit_code == 0
        assert r.llen("stockev_list:task_delete") == 0
        assert r.exists(f"qtask:task:{task_id}") == 0
        assert r.zscore("qtask:hist:stockev_list:task_delete", task_id) is None

    def test_task_requeue_moves_single_task_from_retry(self, runner, r):
        r.lpush("stockev_list:task_requeue:retry", make_msg("retry-keep"))
        r.lpush("stockev_list:task_requeue:retry", make_msg("retry-move"))

        result = runner.invoke(
            app,
            [
                "task",
                "requeue",
                "retry-move",
                "--queue",
                "stockev_list:task_requeue",
                "--from",
                "retry",
                "--force",
            ],
        )

        assert result.exit_code == 0
        assert message_ids(r, "stockev_list:task_requeue") == ["retry-move"]
        assert message_ids(r, "stockev_list:task_requeue:retry") == ["retry-keep"]


def message_ids(client, key: str) -> list[str]:
    return [json.loads(raw)["task_id"] for raw in client.lrange(key, 0, -1)]


class TestCLIWatch:
    def test_watch_command(self, runner, r, monkeypatch):
        r.lpush("stockev_list:watch_test", "test")

        def stop_after_first_sleep(_seconds):
            raise KeyboardInterrupt

        monkeypatch.setattr("qtask_list.cli.__main__.time.sleep", stop_after_first_sleep)
        result = runner.invoke(
            app,
            ["watch", "stockev_list:watch_test", "-i", "1"],
            catch_exceptions=False,
        )

        assert result.exit_code == 0
        assert "Watching" in result.stdout

    def test_watch_rejects_zero_interval(self, runner):
        # 零间隔会形成 Redis 忙轮询，CLI 参数校验应在连接前阻止。
        result = runner.invoke(app, ["watch", "stockev_list:watch_test", "-i", "0"])
        assert result.exit_code != 0


class TestCLIWorker:
    def test_worker_missing_qtask_list(self, runner, r, monkeypatch):
        import qtask_list.cli.__main__ as cli_module

        monkeypatch.setattr(cli_module, "QTASK_LIST_AVAILABLE", False)

        result = runner.invoke(app, ["worker", "--module", "unused:worker"])
        assert result.exit_code != 0
        assert "not installed" in result.stdout or "Error" in result.stdout

    def test_worker_requires_module_and_rejects_unused_options(self, runner):
        # 旧参数不会配置业务 Worker，必须从帮助和调用入口移除以免误用。
        result = runner.invoke(app, ["worker"])

        assert result.exit_code != 0
        assert "--module" in result.stdout
        old_option = runner.invoke(app, ["worker", "--module", "unused:worker", "--queue", "x"])
        assert old_option.exit_code != 0
        assert "--queue" in old_option.stdout


def test_dashboard_rejects_remote_bind_without_auth(runner, monkeypatch):
    # 管理后台含写操作，绑定非本地网卡时默认必须有认证配置。
    monkeypatch.delenv("QTASK_DASHBOARD_PASSWORD", raising=False)
    monkeypatch.delenv("QTASK_DASHBOARD_AUTH", raising=False)
    result = runner.invoke(app, ["dashboard", "--host", "0.0.0.0", "--no-open"])
    assert result.exit_code == 2
    assert "--allow-unauthenticated" in result.stdout


class TestCLICleanHistory:
    def test_clean_history(self, runner, r):
        old = time.time() - 16 * 86400
        r.hset("qtask:task:clean1", mapping={
            "task_id": "clean1",
            "action": "test",
            "outcome": "completed",
            "status": "completed",
            "operational_message": "0",
            "finished_at": str(old),
        })
        r.zadd("qtask:hist:stockev_list:clean_test", {"clean1": 1})

        result = runner.invoke(app, ["clean-history", "stockev_list:clean_test"])
        assert result.exit_code == 0
        assert r.exists("qtask:task:clean1") == 0
        assert r.zcard("qtask:hist:stockev_list:clean_test") == 0

    def test_clean_history_all_queues(self, runner, r):
        old = time.time() - 16 * 86400
        r.zadd("qtask:queues", {"stockev_list:clean_all_a": old, "testns:clean_all_b": old})
        for task_id in ("clean-all-1", "clean-all-2"):
            r.hset(f"qtask:task:{task_id}", mapping={
                "task_id": task_id,
                "action": "test",
                "outcome": "completed",
                "status": "completed",
                "operational_message": "0",
                "finished_at": str(old),
            })
        r.zadd("qtask:hist:stockev_list:clean_all_a", {"clean-all-1": 1})
        r.zadd("qtask:hist:testns:clean_all_b", {"clean-all-2": 1})

        result = runner.invoke(app, ["clean-history"])

        assert result.exit_code == 0
        assert r.exists("qtask:task:clean-all-1") == 0
        assert r.exists("qtask:task:clean-all-2") == 0
        assert r.zcard("qtask:hist:stockev_list:clean_all_a") == 0
        assert r.zcard("qtask:hist:testns:clean_all_b") == 0


class TestCLIStorage:
    def test_storage_passes_runtime_config(self, runner, tmp_path, monkeypatch):
        import uvicorn
        from qtask_list.remote_storage import server as storage_server

        calls = {}
        monkeypatch.setattr(storage_server, "DATA_DIR", tmp_path / "before")
        monkeypatch.setattr(storage_server, "_ttl_seconds", 123.0)
        monkeypatch.setattr(
            storage_server,
            "_start_cleanup_thread",
            lambda: calls.setdefault("cleanup_started", True),
        )

        def fake_run(app, host, port):
            calls.update({"app": app, "host": host, "port": port})

        monkeypatch.setattr(uvicorn, "run", fake_run)

        result = runner.invoke(
            app,
            [
                "storage",
                "--host",
                "127.0.0.1",
                "--port",
                "9010",
                "--data-dir",
                str(tmp_path),
                "--ttl-days",
                "0",
            ],
        )

        assert result.exit_code == 0
        assert storage_server.DATA_DIR == tmp_path
        assert storage_server._ttl_seconds == 0
        assert calls == {
            "cleanup_started": True,
            "app": storage_server.app,
            "host": "127.0.0.1",
            "port": 9010,
        }
