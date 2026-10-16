"""PLAN-024：核心数据保真、故障恢复和旧格式兼容的行为回归。"""

import base64
import hashlib
import json
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone

import pytest
import redis
import zstandard

from qtask_list import FrozenClock, QueueAdmin, SmartQueue, TaskSpec, Worker
from qtask_list.archiver import ArchiveManager
from qtask_list.errors import RemoteStorageTransientError
from qtask_list.state import BEGIN_ATTEMPT_LUA, COMPLETE_TASK_LUA, FAIL_TASK_LUA
from qtask_list.storage import StoredObject


class MemoryStorage:
    """替代 HTTP 传输，验证外存消息与 inline 消息具有一致的业务语义。"""

    def __init__(self, payload=None):
        self.payload = payload
        self.loads = 0

    def save_bytes(self, data, **kwargs):
        self.payload = data
        return "memory"

    def load(self, key):
        self.loads += 1
        if self.payload is None:
            raise RemoteStorageTransientError("storage_timeout", "模拟暂时故障")
        return self.payload


@pytest.mark.parametrize("mode", ["inline", "zstd", "external"])
def test_payload_is_exact_through_claim_retry_and_replay(isolated_redis, mode):
    """空数组、大整数和浮点数经过状态转换仍保持 JSON 值与类型。"""
    kwargs = {"compress_threshold": 0} if mode == "zstd" else {}
    if mode == "external":
        kwargs.update(storage=MemoryStorage(), large_threshold=0)
    q = SmartQueue(redis_client=isolated_redis, queue_name="review", retry_backoff_base=0, **kwargs)
    payload = {
        "empty": [],
        "nested": [{"items": []}],
        "id": 9007199254740993,
        "price": 0.12345678901234567,
    }
    first = q.enqueue(TaskSpec("x", payload))
    claim = q.pop_claim_no_wait()
    assert claim.payload == payload
    assert type(claim.payload["id"]) is int
    assert q.fail(claim.raw_message, "retry")
    q.move_delay()
    claim = q.pop_claim_no_wait()
    assert claim.payload == payload
    assert q.fail(claim.raw_message, "permanent", permanent=True)
    replay = q.replay_task(first.task_id)
    assert replay.accepted
    claim = q.pop_claim_no_wait()
    assert claim.payload == payload
    assert q.ack(claim.raw_message)


def test_concurrency_deferral_preserves_payload(isolated_redis):
    """并发等待模板不能重新编码业务数组或消耗执行预算。"""
    q = SmartQueue(redis_client=isolated_redis, queue_name="review")
    q.enqueue(TaskSpec("x", {}, concurrency_key="same"))
    payload = {"items": [], "id": 2**63 - 1}
    q.enqueue(TaskSpec("x", payload, concurrency_key="same"))
    first = q.pop_claim_no_wait()
    assert q.pop_claim_no_wait() is None
    assert q.ack(first.raw_message)
    raw = isolated_redis.zrange(q.delay, 0, -1)[0]
    isolated_redis.zadd(q.delay, {raw: 0})
    q.move_delay()
    claim = q.pop_claim_no_wait()
    assert claim.payload == payload
    assert claim.context.attempt == 1


@pytest.mark.parametrize("older,newer", [(2**53, 2**53 + 1), (-(2**63), -(2**63) + 1), (-1, 0)])
def test_supersede_compares_integer_versions_exactly(isolated_redis, older, newer):
    """整数版本不能经 double 比较而丢掉相邻版本。"""
    q = SmartQueue(redis_client=isolated_redis, queue_name="review")
    old = q.enqueue(TaskSpec("x", {}, supersede_key="latest", supersede_version=older))
    new = q.enqueue(TaskSpec("x", {}, supersede_key="latest", supersede_version=newer))
    claim = q.pop_claim_no_wait()
    assert claim.context.task_id == new.task_id
    assert q.history.get(old.task_id)["outcome"] == "cancelled"


@pytest.mark.parametrize("name", ["qtask:task:x", "jobs:*", "jobs::x"])
def test_conflicting_queue_names_fail_before_writes(isolated_redis, name):
    """命名冲突不能演变成跨队列消费或内部状态键覆盖。"""
    with pytest.raises(ValueError):
        SmartQueue(redis_client=isolated_redis, queue_name=name)
    assert isolated_redis.dbsize() == 0


@pytest.mark.parametrize("parent_first", [True, False])
def test_queue_namespace_overlap_is_rejected_in_both_directions(isolated_redis, parent_first):
    """原子投递拒绝实际父子冲突，同时保留单独使用状态名称的合法队列。"""
    names = ["jobs", "jobs:delay"] if parent_first else ["jobs:delay", "jobs"]
    first = SmartQueue(redis_client=isolated_redis, queue_name=names[0])
    first.enqueue(TaskSpec("x", {}))
    other = SmartQueue(redis_client=isolated_redis, queue_name=names[1])
    with pytest.raises(ValueError, match="namespace conflict"):
        other.enqueue(TaskSpec("x", {}))
    assert first.size() == 1
    assert len(list(isolated_redis.scan_iter("qtask:task:*"))) == 1


def test_enqueue_type_error_has_no_partial_identity(isolated_redis):
    """错误目标类型必须在 Lua 创建任务和 owner 之前拒绝。"""
    q = SmartQueue(redis_client=isolated_redis, queue_name="review")
    isolated_redis.set(q.delay, "wrong type")
    with pytest.raises(redis.ResponseError):
        q.enqueue(
            TaskSpec(
                "x",
                {},
                logical_key="key",
                not_before_at=datetime.now(timezone.utc) + timedelta(minutes=1),
            )
        )
    assert not list(isolated_redis.scan_iter("qtask:task:*"))
    assert not isolated_redis.exists(q._dedup_key("key"))
    assert q.size() == 0


def test_enqueue_recognizes_lost_success_response(isolated_redis, monkeypatch):
    """提交完成但响应丢失时，凭最终提交标记确认成功。"""
    q = SmartQueue(redis_client=isolated_redis, queue_name="review")
    original = isolated_redis.eval

    def lose_response(script, *args):
        result = original(script, *args)
        if "enqueue_confirmed" in script:
            raise redis.ConnectionError("模拟响应丢失")
        return result

    monkeypatch.setattr(isolated_redis, "eval", lose_response)
    result = q.enqueue(TaskSpec("x", {}))
    assert result.accepted and q.size() == 1


def test_hash_and_plain_logical_keys_are_independent(isolated_redis):
    """明文身份恰为另一身份的哈希时，也必须独立投递。"""
    q = SmartQueue(redis_client=isolated_redis, queue_name="review")
    first = q.enqueue(TaskSpec("x", {}, logical_key="alpha"))
    hashed = hashlib.sha256(b"alpha").hexdigest()
    second = q.enqueue(TaskSpec("x", {}, logical_key=hashed))
    assert first.accepted and second.accepted
    assert not q.enqueue(TaskSpec("x", {}, logical_key="alpha")).accepted


@pytest.mark.parametrize("kind", ["hash", "string"])
def test_legacy_identity_is_migrated_and_released(isolated_redis, kind):
    """旧 owner 的兼容迁移必须保留去重，并在当前任务终态释放。"""
    q = SmartQueue(redis_client=isolated_redis, queue_name="review")
    task_id = q.enqueue(TaskSpec("x", {}, logical_key="key")).task_id
    owner = isolated_redis.hgetall(q._dedup_key("key"))
    isolated_redis.delete(q._dedup_key("key"))
    old_key = q._legacy_dedup_key("key")
    if kind == "hash":
        old_key = q.dedup_prefix + q._hash_key("key")
        isolated_redis.hset(old_key, mapping=owner)
    else:
        isolated_redis.set(old_key, task_id, ex=60)
    assert not q.enqueue(TaskSpec("x", {}, logical_key="key")).accepted
    claim = q.pop_claim_no_wait()
    assert q.ack(claim.raw_message)
    if kind == "hash":
        assert not isolated_redis.exists(q._dedup_key("key"))
    else:
        assert isolated_redis.hget(q._dedup_key("key"), "outcome") == "completed"


def test_legacy_string_history_can_execute_and_clean(isolated_redis):
    """旧 String 记录既能消费，也能按终态保留规则清理。"""
    q = SmartQueue(redis_client=isolated_redis, queue_name="review")
    task_id = "legacy-string"
    record = {
        "task_id": task_id,
        "_queue": q.base,
        "action": "x",
        "status": "pending",
        "created_at": time.time(),
        "extra": {"id": 9007199254740993},
    }
    isolated_redis.set("qtask:task:" + task_id, json.dumps(record), ex=300)
    isolated_redis.lpush(q.queue, q._build_envelope(task_id, {"action": "x", "items": []}))
    claim = q.pop_claim_no_wait()
    assert claim.payload == {"action": "x", "items": []}
    assert q.history.get(task_id)["extra"] == record["extra"]
    assert q.ack(claim.raw_message)
    old = time.time() - 3 * 86400
    record.update(status="completed", finished_at=old, created_at=old, operational_message=0)
    isolated_redis.set("qtask:task:" + task_id, json.dumps(record))
    isolated_redis.zadd(q.history.idx_key, {task_id: old})
    assert q.history.clean_expired(ttl_seconds=86400) == 1


@pytest.mark.parametrize("kind", ["inline", "zstd", "external"])
def test_legacy_payload_migration_resolves_action_without_leaking_retry(isolated_redis, kind):
    """旧 action 隐藏在压缩/外存中时，迁移仍正确路由且剥离运行字段。"""
    payload = {"action": "x", "items": [], "id": 2**63 - 1, "_retry": 0}
    raw = json.dumps(payload).encode()
    storage = MemoryStorage(raw)
    q = SmartQueue(redis_client=isolated_redis, queue_name="review", storage=storage)
    wrapped = payload
    if kind == "zstd":
        wrapped = {
            "_compressed": True,
            "data": base64.b64encode(zstandard.ZstdCompressor().compress(raw)).decode(),
        }
    elif kind == "external":
        wrapped = {"_large": True, "key": "memory"}
    isolated_redis.lpush(q.queue, q._build_envelope("legacy", wrapped))
    claim = q.pop_claim_no_wait()
    assert claim.context.action == "x"
    assert claim.context.attempt == 1
    assert claim.payload == {key: value for key, value in payload.items() if key != "_retry"}
    assert q.ack(claim.raw_message)


def test_legacy_external_failure_has_budget_and_replay(isolated_redis):
    """旧外存暂时故障必须耗尽预算，并可用新 task_id 重放。"""
    storage = MemoryStorage()
    q = SmartQueue(
        redis_client=isolated_redis,
        queue_name="review",
        storage=storage,
        max_attempts=2,
        retry_backoff_base=0,
    )
    isolated_redis.lpush(q.queue, q._build_envelope("legacy", {"_large": True, "key": "memory"}))
    assert q.pop_claim_no_wait() is None
    q.move_delay()
    assert q.pop_claim_no_wait() is None
    assert storage.loads == 2
    assert q.dlq_size() == 1
    assert q.history.get("legacy")["attempt"] == 2
    storage.payload = b'{"action":"x","items":[],"_retry":1}'
    replay = q.replay_task("legacy")
    assert replay.task_id != "legacy"
    claim = q.pop_claim_no_wait()
    assert claim.context.action == "x" and claim.context.attempt == 1
    assert claim.payload == {"action": "x", "items": []}


def test_sampled_consistency_does_not_unprotect_dlq(isolated_redis):
    """抽样未覆盖 DLQ 时，安全修复与清历史不能删除其审计记录。"""
    q = SmartQueue(redis_client=isolated_redis, queue_name="review")
    q.enqueue(TaskSpec("older", {}))
    older = q.pop_claim_no_wait()
    failed = q.enqueue(TaskSpec("newer", {})).task_id
    claim = q.pop_claim_no_wait()
    q.fail(claim.raw_message, "permanent", permanent=True)
    q.return_unstarted_claim(older)
    report = q.check_consistency(limit=1, repair_safe=True)
    assert report["repaired"] == 0
    assert q.history.get(failed)["operational_message"] == 1
    assert q.history.clear_terminal() == 0
    assert q.dlq_size() == 1


@pytest.mark.parametrize("result", [{"count": 1}, [1, {"items": []}], True, 3])
def test_structured_results_are_archived_as_json(isolated_redis, tmp_path, result):
    """常见 handler 返回值可归档，并能从 SQLite 恢复结构化值。"""
    q = SmartQueue(redis_client=isolated_redis, queue_name="review")
    task_id = q.enqueue(TaskSpec("x", {})).task_id
    q.ack(q.pop_claim_no_wait().raw_message, result=result)
    old = time.time() - 2 * 86400
    isolated_redis.hset("qtask:task:" + task_id, "finished_at", old)
    isolated_redis.zadd(q.history.idx_key, {task_id: old})
    archiver = ArchiveManager("redis://isolated/0", db_dir=str(tmp_path))
    assert archiver.archive_to_sqlite(q.base) == 1
    with sqlite3.connect(next(tmp_path.glob("*.db"))) as connection:
        row = connection.execute(
            "SELECT result FROM task_history WHERE task_id=?", (task_id,)
        ).fetchone()
    assert json.loads(row[0]) == result


def test_worker_recovers_interrupted_begin_without_touching_active_claim(
    isolated_redis, monkeypatch
):
    """领取后断连可由当前 Worker 恢复，本地仍执行的 claim 不被抢走。"""
    worker = Worker("redis://isolated/0", "review", redis_client=isolated_redis)
    task_id = worker.queue.enqueue(TaskSpec("x", {})).task_id
    calls = []

    @worker.on("x")
    def handler(payload):
        calls.append(payload)

    original = isolated_redis.eval
    interrupted = False

    def fail_once(script, *args):
        nonlocal interrupted
        if script == BEGIN_ATTEMPT_LUA and not interrupted:
            interrupted = True
            raise redis.ConnectionError("模拟领取中断")
        return original(script, *args)

    monkeypatch.setattr(isolated_redis, "eval", fail_once)
    with pytest.raises(redis.ConnectionError):
        worker._poll_once()
    assert worker._poll_once()
    assert len(calls) == 1
    assert worker.queue.history.get(task_id)["outcome"] == "completed"
    worker.queue.enqueue(TaskSpec("x", {}))
    claim = worker.queue.pop_claim_no_wait()
    assert worker.queue.recover_abandoned_claims({claim.raw_message}) == 0
    assert isolated_redis.llen(worker.queue.processing) == 1


@pytest.mark.parametrize("commit_first", [False, True])
def test_worker_retries_settlement_without_reexecuting_handler(
    isolated_redis, monkeypatch, commit_first
):
    """ack 发送前或提交后断连，都只结算重试，不再次运行 handler。"""
    worker = Worker("redis://isolated/0", "review", redis_client=isolated_redis)
    task_id = worker.queue.enqueue(TaskSpec("x", {})).task_id
    claim = worker.queue.pop_claim_no_wait()
    calls = []

    @worker.on("x")
    def handler(payload):
        calls.append(1)
        return {"ok": True}

    original = isolated_redis.eval
    failures = 0

    def fail_once(script, *args):
        nonlocal failures
        if script == COMPLETE_TASK_LUA and failures == 0:
            failures += 1
            if commit_first:
                original(script, *args)
            raise redis.ConnectionError("模拟结算连接中断")
        return original(script, *args)

    monkeypatch.setattr(isolated_redis, "eval", fail_once)
    worker._process_task(claim)
    assert calls == [1]
    assert worker.queue.history.get(task_id)["outcome"] == "completed"
    assert isolated_redis.llen(worker.queue.processing) == 0
    assert worker.queue.dlq_size() == 0


def test_maintenance_survives_redis_and_metrics_outage(isolated_redis, monkeypatch):
    """维护失败计数也失败时，循环恢复后仍会再次尝试维护。"""
    worker = Worker("redis://isolated/0", "review", redis_client=isolated_redis)
    worker.running = True
    calls = []

    def leadership():
        calls.append(1)
        if len(calls) == 1:
            raise redis.ConnectionError("模拟 Redis 不可用")
        worker.running = False
        worker._maintenance_wakeup.set()
        return False

    def unavailable(*args, **kwargs):
        raise redis.ConnectionError("模拟指标也不可用")

    monkeypatch.setattr(worker, "_acquire_maintenance_leader", leadership)
    monkeypatch.setattr(isolated_redis, "hincrby", unavailable)
    worker.heartbeat_interval = 0.001
    worker._maintenance_loop()
    assert len(calls) == 2


def test_short_lease_is_renewed_while_handler_runs(isolated_redis, monkeypatch):
    """租约短于默认 heartbeat 周期时，同键 handler 仍保持串行。"""
    worker = Worker(
        "redis://isolated/0",
        "review",
        redis_client=isolated_redis,
        max_workers=2,
        concurrency_lease_seconds=0.25,
        concurrency_retry_seconds=0.05,
    )
    monkeypatch.setattr(worker, "_maintenance_loop", lambda: None)
    first_started, second_started, release = threading.Event(), threading.Event(), threading.Event()
    calls = []

    @worker.on("x")
    def handler(payload):
        calls.append(1)
        if len(calls) == 1:
            first_started.set()
            release.wait(5)
        else:
            second_started.set()

    for _ in range(2):
        worker.queue.enqueue(TaskSpec("x", {}, concurrency_key="same"))
    thread = threading.Thread(target=worker.run)
    thread.start()
    try:
        assert first_started.wait(2)
        assert not second_started.wait(0.7)
        assert isolated_redis.exists(worker.queue._lease_key("same"))
        release.set()
        assert second_started.wait(4)
    finally:
        worker.stop("regression_complete")
        release.set()
        thread.join(5)
    assert not thread.is_alive()
    assert len(calls) == 2


def test_admin_reads_migrated_legacy_history(isolated_redis):
    """管理入口仍能读取迁移后的记录及其完整业务数据。"""
    q = SmartQueue(redis_client=isolated_redis, queue_name="review")
    isolated_redis.lpush(q.queue, q._build_envelope("legacy", {"action": "x", "items": []}))
    claim = q.pop_claim_no_wait()
    q.ack(claim.raw_message)
    admin = QueueAdmin(redis_client=isolated_redis)
    assert admin.resolve_payload("legacy", q.base, "completed")["payload"] == {
        "action": "x",
        "items": [],
    }


def test_committed_begin_response_loss_restores_unadmitted_budget(isolated_redis, monkeypatch):
    """唯一执行机会不能被 handler 准入前丢失的 Redis 响应耗尽。"""
    worker = Worker("redis://isolated/0", "review", redis_client=isolated_redis, max_attempts=1)
    task_id = worker.queue.enqueue(TaskSpec("x", {})).task_id
    calls = []

    @worker.on("x")
    def handler(payload, context):
        calls.append(context.attempt)

    original = isolated_redis.eval
    interrupted = False

    def lose_begin_response(script, *args):
        nonlocal interrupted
        result = original(script, *args)
        if script == BEGIN_ATTEMPT_LUA and not interrupted:
            interrupted = True
            raise redis.ConnectionError("begin 已提交，但响应丢失")
        return result

    monkeypatch.setattr(isolated_redis, "eval", lose_begin_response)
    with pytest.raises(redis.ConnectionError):
        worker._poll_once()
    assert worker._poll_once()
    assert calls == [1]
    assert worker.queue.history.get(task_id)["outcome"] == "completed"


def test_pre_begin_outage_does_not_refund_an_old_retry(isolated_redis, monkeypatch):
    """领取重试消息但尚未 begin 时，不能返还上一次真正执行的预算。"""
    worker = Worker(
        "redis://isolated/0",
        "review",
        redis_client=isolated_redis,
        max_attempts=2,
        retry_backoff_base=0,
    )
    q = worker.queue
    q.enqueue(TaskSpec("x", {}))
    claim = q.pop_claim_no_wait()
    q.fail(claim.raw_message, "第一次执行失败")
    q.move_delay()
    original = isolated_redis.eval
    failures = []

    def fail_before_begin(script, *args):
        if script == BEGIN_ATTEMPT_LUA and not failures:
            failures.append(1)
            raise redis.ConnectionError("尚未执行 begin")
        return original(script, *args)

    monkeypatch.setattr(isolated_redis, "eval", fail_before_begin)
    attempts = []

    @worker.on("x")
    def handler(payload, context):
        attempts.append(context.attempt)

    with pytest.raises(redis.ConnectionError):
        worker._poll_once()
    assert worker._poll_once()
    assert attempts == [2]


@pytest.mark.parametrize("text", ["null", "false", "123", '"quoted"'])
def test_history_text_identity_is_not_json_coerced(isolated_redis, text):
    """看起来像 JSON 的合法文本身份在 History、Admin 和 replay 中保持原值。"""
    q = SmartQueue(redis_client=isolated_redis, queue_name="review")
    task_id = q.enqueue(TaskSpec(text, {}, logical_key=text, trace_id=text)).task_id
    claim = q.pop_claim_no_wait()
    q.fail(claim.raw_message, "failed", permanent=True)
    admin = QueueAdmin(redis_client=isolated_redis)
    for record in (
        q.history.get(task_id),
        admin.get_task(task_id),
        admin.list_tasks(q.base, "failed")[0],
    ):
        assert record["action"] == text
        assert record["logical_key"] == text
        assert record["trace_id"] == text
    replay = q.replay_task(task_id)
    replay_claim = q.pop_claim_no_wait()
    assert replay_claim.context.task_id == replay.task_id
    assert replay_claim.context.logical_key == text


def test_replay_recognizes_committed_response_loss(isolated_redis, monkeypatch):
    """重放响应丢失后返回实际新实例，不误报失败或重复创建。"""
    q = SmartQueue(redis_client=isolated_redis, queue_name="review")
    task_id = q.enqueue(TaskSpec("x", {}, logical_key="key")).task_id
    q.fail(q.pop_claim_no_wait().raw_message, "failed", permanent=True)
    original = isolated_redis.eval

    def lose_response(script, *args):
        result = original(script, *args)
        if "replay.accepted" in script:
            raise redis.ConnectionError("replay 响应丢失")
        return result

    monkeypatch.setattr(isolated_redis, "eval", lose_response)
    result = q.replay_task(task_id)
    assert result.accepted
    assert q.size() == 1 and q.dlq_size() == 0
    assert q.history.get(task_id)["replayed_by"] == result.task_id


def test_old_terminal_string_without_operational_flag_can_clear(isolated_redis):
    """旧终态 String 缺少运行标记时可清理，且不会删除旧 DLQ 的历史。"""
    q = SmartQueue(redis_client=isolated_redis, queue_name="review")
    for task_id in ("done", "dlq"):
        isolated_redis.set(
            "qtask:task:" + task_id,
            json.dumps(
                {
                    "task_id": task_id,
                    "status": "failed",
                    "created_at": time.time(),
                    "result": True,
                }
            ),
        )
        isolated_redis.zadd(q.history.idx_key, {task_id: time.time()})
    isolated_redis.lpush(q.dlq, q._build_envelope("dlq", {"action": "x"}))
    assert q.history.clear_terminal() == 1
    assert q.history.get("done") is None
    assert q.history.get("dlq")["result"] is True


def test_invalid_metric_counter_does_not_partially_enqueue(isolated_redis):
    """Hash 类型正确但计数值非法时，也必须先拒绝，再创建消息与身份。"""
    q = SmartQueue(redis_client=isolated_redis, queue_name="review")
    isolated_redis.hset(q.metrics_key, "enqueue.accepted", "invalid")
    with pytest.raises(redis.ResponseError):
        q.enqueue(TaskSpec("x", {}, logical_key="key"))
    assert q.size() == 0
    assert not list(isolated_redis.scan_iter("qtask:task:*"))
    assert not isolated_redis.exists(q._dedup_key("key"))


def test_legacy_raw_key_cannot_alias_versioned_owner(isolated_redis):
    """即使旧明文身份含版本前缀，新 owner 也处于完全独立的键空间。"""
    q = SmartQueue(redis_client=isolated_redis, queue_name="review")
    legacy_key = "v2:" + q._hash_key("alpha")
    isolated_redis.set(q._legacy_dedup_key(legacy_key), "old-task", ex=60)
    result = q.enqueue(TaskSpec("x", {}, logical_key="alpha"))
    assert result.accepted
    assert isolated_redis.get(q._legacy_dedup_key(legacy_key)) == "old-task"
    duplicate = q.enqueue(TaskSpec("x", {}, logical_key=legacy_key))
    assert not duplicate.accepted and duplicate.duplicate_of == "old-task"


def test_legacy_retained_owner_keeps_terminal_state_and_ttl(isolated_redis):
    """旧终态身份的迁移不能把有限保留变成永久活跃锁。"""
    q = SmartQueue(redis_client=isolated_redis, queue_name="review")
    isolated_redis.hset(
        "qtask:task:old-task", mapping={"task_id": "old-task", "status": "completed"}
    )
    isolated_redis.set(q._legacy_dedup_key("key"), "old-task", ex=60)
    result = q.enqueue(TaskSpec("x", {}, logical_key="key"))
    assert result.reason == "duplicate_retained"
    assert 0 < isolated_redis.pttl(q._dedup_key("key")) <= 60000


def test_queue_deletion_removes_versioned_retained_owner(isolated_redis):
    """整队删除清理新 owner，不能留下不再可见的身份抑制。"""
    q = SmartQueue(redis_client=isolated_redis, queue_name="review")
    q.enqueue(
        TaskSpec(
            "x", {}, logical_key="key", dedup_until=datetime.now(timezone.utc) + timedelta(days=1)
        )
    )
    q.ack(q.pop_claim_no_wait().raw_message)
    assert isolated_redis.exists(q._dedup_key("key"))
    QueueAdmin(redis_client=isolated_redis).delete_queue(q.base)
    assert not isolated_redis.exists(q._dedup_key("key"))


def test_skipping_poison_messages_observes_worker_stop(isolated_redis, monkeypatch):
    """内部连续跳过毒消息时，也须在停止请求后退出领取循环。"""
    worker = Worker("redis://isolated/0", "review", redis_client=isolated_redis)
    worker.running = True
    for i in range(5):
        isolated_redis.lpush(worker.queue.queue, f"invalid-{i}")
    original = worker.queue._claim_popped

    def stop_after_first(raw, **kwargs):
        result = original(raw, **kwargs)
        worker.stop("skip_loop_test")
        return result

    monkeypatch.setattr(worker.queue, "_claim_popped", stop_after_first)
    assert not worker._poll_once()
    assert worker.queue.size() == 4


def test_unexpected_handler_exit_keeps_spent_attempt(isolated_redis):
    """真正进入过 handler 的任务异常退出后，不应返还已用执行次数。"""
    worker = Worker("redis://isolated/0", "review", redis_client=isolated_redis, max_attempts=1)
    task_id = worker.queue.enqueue(TaskSpec("x", {})).task_id

    @worker.on("x")
    def handler(payload):
        raise SystemExit("模拟线程处理器异常退出")

    claim = worker.queue.pop_claim_no_wait()
    worker._register_claim(claim)
    with pytest.raises(SystemExit):
        worker._process_task_with_semaphore(claim)
    worker.queue.recover_abandoned_claims(set(), spent_attempts=worker._abandoned_attempts)
    assert worker.queue.pop_claim_no_wait() is None
    assert worker.queue.history.get(task_id)["outcome"] == "failed"
    assert worker.queue.history.get(task_id)["attempt"] == 1


def test_invalid_completed_counter_does_not_remove_processing(isolated_redis):
    """结算的计数错误不能先删除 processing 再中断终态写入。"""
    q = SmartQueue(redis_client=isolated_redis, queue_name="review")
    task_id = q.enqueue(TaskSpec("x", {})).task_id
    claim = q.pop_claim_no_wait()
    isolated_redis.hset(q.metrics_key, "outcome.completed", "invalid")
    with pytest.raises(redis.ResponseError):
        q.ack(claim.raw_message)
    assert isolated_redis.llen(q.processing) == 1
    assert q.history.get(task_id)["outcome"] == ""
    isolated_redis.hset(q.metrics_key, "outcome.completed", 0)
    assert q.ack(claim.raw_message)


@pytest.mark.parametrize("use_admin", [False, True])
def test_safe_recovery_rechecks_reappearing_heartbeat(isolated_redis, monkeypatch, use_admin):
    """发现 stale 后心跳恢复时，原子搬移仍拒绝抢走活跃任务。"""
    q = SmartQueue(
        redis_client=isolated_redis, queue_name="review", processing_key="review:processing:w"
    )
    q.enqueue(TaskSpec("x", {}))
    q.pop_claim_no_wait()
    heartbeat = "review:worker:w"
    original = isolated_redis.exists

    def heartbeat_reappears(key, *keys):
        if key == heartbeat and not original(heartbeat):
            isolated_redis.set(heartbeat, "alive", ex=60)
            return 0
        return original(key, *keys)

    monkeypatch.setattr(isolated_redis, "exists", heartbeat_reappears)
    if use_admin:
        recovered = QueueAdmin(redis_client=isolated_redis).recover(q.base)["recovered"]
    else:
        recovered = q.recover_stale_processing("review:worker:")
    assert recovered == 0
    assert isolated_redis.llen(q.processing) == 1


@pytest.mark.parametrize("use_admin", [False, True])
def test_recovery_restores_deadline_observation(isolated_redis, use_admin):
    """processing 恢复为 ready 后重新进入 deadline 索引。"""
    q = SmartQueue(
        redis_client=isolated_redis, queue_name="review", processing_key="review:processing:w"
    )
    task_id = q.enqueue(
        TaskSpec("x", {}, start_deadline_at=datetime.now(timezone.utc) + timedelta(minutes=1))
    ).task_id
    q.pop_claim_no_wait()
    assert isolated_redis.zscore(q.deadline_key, task_id) is None
    if use_admin:
        assert QueueAdmin(redis_client=isolated_redis).recover(q.base)["recovered"] == 1
    else:
        assert q.recover() == 1
    assert isolated_redis.zscore(q.deadline_key, task_id) is not None


def test_admin_resolves_retry_wait_and_completed_all_view(isolated_redis):
    """派生状态和已经离开容器的历史任务仍能还原 payload。"""
    q = SmartQueue(redis_client=isolated_redis, queue_name="review", retry_backoff_base=0)
    task_id = q.enqueue(TaskSpec("x", {"items": []})).task_id
    q.fail(q.pop_claim_no_wait().raw_message, "retry")
    admin = QueueAdmin(redis_client=isolated_redis)
    assert admin.resolve_payload(task_id, q.base, "retry_wait")["payload"] == {"items": []}
    q.move_delay()
    q.ack(q.pop_claim_no_wait().raw_message)
    assert admin.resolve_payload(task_id, q.base, "all")["payload"] == {"items": []}


@pytest.mark.parametrize("explicit_past", [False, True])
def test_client_clock_ahead_does_not_delay_unscheduled_enqueue_or_replay(
    isolated_redis, explicit_past
):
    """客户端时钟超前时，默认任务与已经到点的任务仍应立即进入 ready。"""
    q = SmartQueue(
        redis_client=isolated_redis, queue_name="review", clock=FrozenClock(time.time() + 3600)
    )
    not_before = datetime.now(timezone.utc) - timedelta(minutes=1) if explicit_past else None
    task_id = q.enqueue(TaskSpec("x", {}, not_before_at=not_before)).task_id
    assert q.size() == 1 and q.delay_size() == 0
    q.fail(q.pop_claim_no_wait().raw_message, "failed", permanent=True)
    replay = q.replay_task(task_id)
    assert replay.accepted and q.size() == 1 and q.delay_size() == 0


def test_payload_fetch_outage_does_not_refund_real_attempt(isolated_redis, monkeypatch):
    """外存下载失败后结算再断连，恢复不能退款并多下载一次。"""
    storage = MemoryStorage()
    worker = Worker(
        "redis://isolated/0", "review", redis_client=isolated_redis, max_attempts=1, storage=storage
    )
    isolated_redis.lpush(
        worker.queue.queue,
        worker.queue._build_envelope(
            "legacy",
            {"_large": True, "key": "memory"},
        ),
    )
    original = isolated_redis.eval
    failures = []

    def fail_settlement(script, *args):
        if script == FAIL_TASK_LUA and not failures:
            failures.append(1)
            raise redis.ConnectionError("外存失败后的结算中断")
        return original(script, *args)

    monkeypatch.setattr(isolated_redis, "eval", fail_settlement)
    with pytest.raises(redis.ConnectionError):
        worker._poll_once()
    assert storage.loads == 1
    assert not worker._poll_once()
    assert storage.loads == 1
    assert worker.queue.dlq_size() == 1
    assert worker.queue.history.get("legacy")["attempt"] == 1


def test_legacy_invalid_action_is_dead_lettered(isolated_redis):
    """旧版允许但 V2 不接受的 action 应进入 DLQ，而非循环卡住领取。"""
    q = SmartQueue(redis_client=isolated_redis, queue_name="review")
    isolated_redis.lpush(q.queue, q._build_envelope("invalid-action", {"action": " "}))
    assert q.pop_claim_no_wait() is None
    assert q.processing_size() == 0 and q.dlq_size() == 1


@pytest.mark.parametrize(
    "kwargs",
    [
        {"concurrency_lease_seconds": float("nan")},
        {"concurrency_lease_seconds": float("inf")},
        {"concurrency_lease_seconds": 0.0001},
        {"concurrency_retry_seconds": float("inf")},
        {"retry_backoff_base": float("nan")},
        {"retry_backoff_max": float("inf")},
        {"ttl_days": 0.1},
    ],
)
def test_invalid_timing_configuration_fails_before_writes(isolated_redis, kwargs):
    """不能传给 Redis 的时间配置在构造阶段拒绝，避免半写终态。"""
    with pytest.raises(ValueError):
        SmartQueue(redis_client=isolated_redis, queue_name="review", **kwargs)
    assert isolated_redis.dbsize() == 0


def test_external_gc_type_error_cannot_partially_complete(isolated_redis):
    """动态外存 GC 键的类型也应提前检查，避免终态写入后才回收失败。"""

    class DedicatedStorage(MemoryStorage):
        """模拟任务独占对象，覆盖终态回收日志的动态 storage_id。"""

        def save(self, data, retain_until=None):
            self.payload = data
            return StoredObject("memory", created=True, dedicated=True, storage_id="test-store")

    q = SmartQueue(
        redis_client=isolated_redis,
        queue_name="review",
        storage=DedicatedStorage(),
        large_threshold=0,
    )
    task_id = q.enqueue(TaskSpec("x", {"data": "payload"})).task_id
    claim = q.pop_claim_no_wait()
    gc_key = q.storage_gc_key + ":test-store"
    isolated_redis.set(gc_key, "invalid")
    with pytest.raises(redis.ResponseError):
        q.ack(claim.raw_message)
    assert isolated_redis.llen(q.processing) == 1
    assert q.history.get(task_id)["outcome"] == ""
    isolated_redis.delete(gc_key)
    assert q.ack(claim.raw_message)
    assert isolated_redis.zscore(gc_key, "memory") is not None


def test_legacy_expired_external_task_never_downloads(isolated_redis):
    """V1 迁移后仍在外存访问前检查截止时间，过期不消耗 attempt。"""
    storage = MemoryStorage()
    q = SmartQueue(redis_client=isolated_redis, queue_name="review", storage=storage)
    isolated_redis.lpush(
        q.queue,
        q._build_envelope(
            "expired-legacy",
            {"_large": True, "key": "memory"},
            expires_at=time.time() - 1,
        ),
    )
    assert q.pop_claim_no_wait() is None
    assert storage.loads == 0
    record = q.history.get("expired-legacy")
    assert record["outcome"] == "skipped" and record["attempt"] == 0
    assert q.dlq_size() == 0


@pytest.mark.parametrize("reader_kind", ["history", "admin", "batch", "archive"])
def test_history_read_is_coherent_during_format_conversion(
    isolated_redis, monkeypatch, tmp_path, reader_kind
):
    """旧 TYPE/GET 窗口中并发转 Hash，单条、批量与归档读取仍返回完整快照。"""
    q = SmartQueue(redis_client=isolated_redis, queue_name="review")
    task_id = "format-race"
    key = "qtask:task:" + task_id
    old = time.time() - 2 * 86400
    expected = {
        "task_id": task_id,
        "_queue": q.base,
        "action": "x",
        "status": "completed",
        "operational_message": 0,
        "created_at": old,
        "finished_at": old,
        "result": {"id": 9007199254740993, "items": []},
    }
    isolated_redis.set(key, json.dumps(expected))
    isolated_redis.zadd(q.history.idx_key, {task_id: old})
    typed, converted = threading.Event(), threading.Event()
    original_type, original_pipeline = isolated_redis.type, isolated_redis.pipeline
    values, errors = [], []

    def type_with_conversion_window(name):
        kind = original_type(name)
        if threading.current_thread() is reader and name == key and kind == "string":
            typed.set()
            converted.wait(2)
        return kind

    def pipeline_with_conversion_window(*args, **kwargs):
        pipe = original_pipeline(*args, **kwargs)
        execute = pipe.execute

        def execute_with_window(*args, **kwargs):
            commands = list(pipe.command_stack)
            result = execute(*args, **kwargs)
            if threading.current_thread() is reader and any(
                command[0][0] in {"TYPE", b"TYPE"} for command in commands
            ):
                typed.set()
                converted.wait(2)
            return result

        pipe.execute = execute_with_window
        return pipe

    monkeypatch.setattr(isolated_redis, "type", type_with_conversion_window)
    monkeypatch.setattr(isolated_redis, "pipeline", pipeline_with_conversion_window)

    def read_record():
        try:
            admin = QueueAdmin(redis_client=isolated_redis)
            if reader_kind == "history":
                values.append(q.history.get(task_id))
            elif reader_kind == "admin":
                values.append(admin.get_task(task_id))
            elif reader_kind == "batch":
                values.append(admin._read_history_records([task_id])[0])
            else:
                manager = ArchiveManager("redis://isolated/0", db_dir=str(tmp_path))
                manager.r = isolated_redis
                assert manager.archive_to_sqlite(q.base) == 1
                with sqlite3.connect(next(tmp_path.glob("*.db"))) as connection:
                    raw = connection.execute("SELECT raw_data FROM task_history").fetchone()[0]
                values.append(json.loads(raw))
        except BaseException as exc:
            errors.append(exc)

    reader = threading.Thread(target=read_record)
    reader.start()
    try:
        typed.wait(0.15)
        q.history.ensure_hash(task_id)
    finally:
        converted.set()
        reader.join(3)
    assert not reader.is_alive() and not errors
    assert values[0]["result"] == expected["result"]
