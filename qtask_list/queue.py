"""qtask_list V2 队列核心。"""

from __future__ import annotations

import hashlib
import json
import math
import random
import uuid
import warnings
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from itertools import chain
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, cast

import orjson
import redis
from loguru import logger

from .clock import Clock, SystemClock, datetime_to_epoch, epoch_to_datetime
from .envelope import EnvelopeCodec, EnvelopeHeader, PreparedPayload, prepared_from_descriptor
from .errors import BatchEnqueueError, PayloadDecodeError, RemoteStorageError, RemoteStorageTransientError
from .history import TaskHistory
from .models import (
    DuplicateAction,
    EnqueueResult,
    HistoryMode,
    IdentityPolicy,
    JsonValue,
    TaskContext,
    TaskSpec,
)
from .state import (
    ADMIN_MOVE_LUA,
    BEGIN_ATTEMPT_LUA,
    CANCEL_OR_PURGE_LUA,
    CHECK_QUEUE_NAMESPACE_LUA,
    COMPLETE_TASK_LUA,
    ENQUEUE_V2_LUA,
    FAIL_TASK_LUA,
    MOVE_DUE_DELAY_LUA,
    REGISTER_PROCESSING_KEY_LUA,
    RELEASE_LEASE_LUA,
    RENEW_LEASE_LUA,
    REPLAY_TASK_LUA,
    RETURN_UNSTARTED_CLAIM_LUA,
    RETRY_TASK_LUA,
)
from .storage import RemoteStorage

# 单次 Lua 迁移的批量上限，避免同一秒大量任务到期时长期阻塞 Redis。
MOVE_DELAY_BATCH = 500
MAX_ENQUEUE_MANY = 1000
STORAGE_GC_KEY = "qtask:storage:gc"
STORAGE_PENDING_KEY = "qtask:storage:pending"
_UNSET = object()


class TransientPayloadError(RemoteStorageTransientError):
    """旧名称兼容；新代码应捕获 RemoteStorageTransientError。"""


@dataclass(frozen=True)
class TaskClaim:
    """Worker 已原子开始的一次执行尝试。"""

    payload: dict[str, JsonValue]
    raw_message: str
    context: TaskContext
    lease_key: str | None = None
    lease_token: str | None = None
    # payload 准备已发生、结算尚未确认时，恢复不能返还这次尝试的预算。
    settlement_pending: bool = False

    def with_stop_event(self, stop_event: Any) -> "TaskClaim":
        """把 Worker 的只读停止信号绑定到上下文。"""
        return replace(self, context=replace(self.context, _stop_event=stop_event))


class SmartQueue:
    """可靠执行队列。

    V2 把业务 payload 与运行元数据分离，并通过少量 Redis Lua 状态转换保证
    identity、消息位置和不可变 outcome 同步变化。旧 push/pop 调用方式继续可用。
    """

    def __init__(
        self,
        redis_url: Optional[str] = None,
        queue_name: str = "",
        namespace: Optional[str] = None,
        storage: Optional[RemoteStorage] = None,
        large_threshold: int = 50 * 1024,
        compress_threshold: int = 50 * 1024,
        max_retry: Optional[int] = None,
        ttl_days: int = 15,
        redis_client: Optional[Any] = None,
        processing_key: Optional[str] = None,
        retry_backoff_base: float = 30,
        retry_backoff_max: float = 3600,
        record_history: bool = True,
        *,
        max_attempts: Optional[int] = None,
        history_mode: HistoryMode | str | None = None,
        clock: Clock | None = None,
        worker_id: str | None = None,
        concurrency_lease_seconds: float = 120,
        concurrency_retry_seconds: float = 1,
        external_safety_margin: float = 86400,
    ):
        self.r: Any
        if redis_client is not None:
            self.r = redis_client
        elif redis_url:
            self.r = redis.from_url(redis_url, decode_responses=True)
        else:
            raise ValueError("Either redis_url or redis_client must be provided")
        if not queue_name:
            raise ValueError("queue_name must not be empty")

        self.namespace = namespace or ""
        self.base = f"{self.namespace}:{queue_name}" if self.namespace else queue_name
        if (self.base.startswith("qtask:")
                or any(not part for part in self.base.split(":"))
                or any(char in self.base for char in "*?[]")):
            raise ValueError("queue name uses an internal prefix, empty segment or Redis glob character")
        self.queue = self.base
        self.default_processing = f"{self.base}:processing"
        self.processing = processing_key or self.default_processing
        self.retry = f"{self.base}:retry"  # 仅兼容读取 V1 retry List
        self.dlq = f"{self.base}:dlq"
        self.delay = f"{self.base}:delay"
        self.dedup_prefix = f"{self.base}:dedup:"
        self.owner_prefix = f"qtask:owner:v2:{self._hash_key(self.base)}:"
        self.supersede_prefix = f"{self.base}:supersede:"
        self.lease_prefix = f"{self.base}:lease:"
        self.registry_key = "qtask:queues"
        self.worker_registry_key = f"qtask:workers:{self.base}"
        self.processing_registry_key = f"qtask:processing:{self.base}"
        self._processing_key_managed_by_worker = False
        self.metrics_key = f"qtask:metrics:{self.base}"
        self.deadline_key = f"qtask:deadline:{self.base}"
        self.storage_gc_key = STORAGE_GC_KEY
        self.storage_pending_key = STORAGE_PENDING_KEY

        if max_attempts is not None and max_retry is not None and max_attempts != max_retry:
            raise ValueError("max_attempts and deprecated max_retry disagree")
        resolved_attempts = max_attempts if max_attempts is not None else max_retry
        if resolved_attempts is None:
            resolved_attempts = 3
        # 旧版 max_retry=0 表示首次失败后直接 DLQ；V2 等价于 max_attempts=1。
        self.max_attempts = max(1, int(resolved_attempts))
        self.max_retry = self.max_attempts

        if (not math.isfinite(retry_backoff_base) or not math.isfinite(retry_backoff_max)
                or retry_backoff_base < 0 or retry_backoff_max < 0):
            raise ValueError("retry backoff values must be >= 0")
        if retry_backoff_max and retry_backoff_base > retry_backoff_max:
            raise ValueError("retry_backoff_base must not exceed retry_backoff_max")
        if (not math.isfinite(concurrency_lease_seconds) or concurrency_lease_seconds < 0.001
                or not math.isfinite(concurrency_retry_seconds) or concurrency_retry_seconds <= 0):
            raise ValueError("concurrency lease must be finite and >= 0.001s; retry must be finite and > 0")
        self.retry_backoff_base = float(retry_backoff_base)
        self.retry_backoff_max = float(retry_backoff_max)
        self.concurrency_lease_seconds = float(concurrency_lease_seconds)
        self.concurrency_retry_seconds = float(concurrency_retry_seconds)
        self.external_safety_margin = float(external_safety_margin)

        self.storage = storage
        self.large_threshold = large_threshold
        self.compress_threshold = compress_threshold
        self.codec = EnvelopeCodec(
            storage=storage,
            large_threshold=large_threshold,
            compress_threshold=compress_threshold,
        )
        self.clock = clock or SystemClock()
        self.ttl_days = ttl_days
        self.ttl_seconds = max(ttl_days * 86400, 1)
        self.record_history = record_history
        self.history_mode = HistoryMode(
            history_mode or (HistoryMode.FULL if record_history else HistoryMode.MINIMAL)
        )
        self.worker_id = worker_id or self._worker_id_from_processing(self.processing)
        self.history = TaskHistory(
            redis_client=self.r,
            queue_name=self.base,
            ttl_days=ttl_days,
            clock=self.clock,
        )

    # ==================== Identity / key helpers ====================

    @staticmethod
    def _hash_key(value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    def _dedup_key(self, logical_key: str) -> str:
        return f"{self.owner_prefix}{self._hash_key(logical_key)}"

    def _legacy_dedup_key(self, logical_key: str) -> str:
        return f"{self.dedup_prefix}{logical_key}"

    def _migrate_identity(self, logical_key: str) -> None:
        """按类型与原身份核验迁移旧 owner，避免哈希名与旧明文名碰撞。"""
        script = r"""
        if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
        local kind = redis.call('TYPE', KEYS[2])['ok']
        local source = ''
        if kind == 'hash' and redis.call('HGET', KEYS[2], 'logical_key') == ARGV[1] then
            source = KEYS[2]
        elseif redis.call('TYPE', KEYS[3])['ok'] == 'string' then
            source = KEYS[3]
        end
        if source == '' then return 0 end
        local ttl = redis.call('PTTL', source)
        if redis.call('TYPE', source)['ok'] == 'hash' then
            redis.call('HSET', KEYS[1], unpack(redis.call('HGETALL', source)))
            local outcome = redis.call('HGET', KEYS[1], 'outcome') or 'none'
            if outcome ~= '' and outcome ~= 'none' and ttl >= 0 then
                redis.call('PEXPIRE', KEYS[1], math.max(ttl, 1))
            end
        else
            local retain_until = ''
            local owner_id = redis.call('GET', source)
            local outcome = 'none'
            local record_key = 'qtask:task:' .. owner_id
            local record_kind = redis.call('TYPE', record_key)['ok']
            if record_kind == 'hash' then
                outcome = redis.call('HGET', record_key, 'outcome') or ''
                if outcome == '' then outcome = redis.call('HGET', record_key, 'status') or 'none' end
            elseif record_kind == 'string' then
                local ok, record = pcall(cjson.decode, redis.call('GET', record_key))
                if ok and type(record) == 'table' then outcome = record['outcome'] or record['status'] or 'none' end
            end
            local terminal = outcome == 'completed' or outcome == 'failed' or
                             outcome == 'skipped' or outcome == 'cancelled'
            if ttl >= 0 then
                local now = redis.call('TIME')
                retain_until = tostring(tonumber(now[1]) + tonumber(now[2]) / 1000000 + math.max(ttl, 1) / 1000)
            end
            redis.call('HSET', KEYS[1], 'task_id', owner_id,
                       'logical_key', ARGV[1], 'generation', '1', 'outcome', terminal and outcome or 'none',
                       'dedup_until', retain_until)
            if terminal and ttl >= 0 then redis.call('PEXPIRE', KEYS[1], math.max(ttl, 1)) end
        end
        redis.call('DEL', source)
        return 1
        """
        self.r.eval(script, 3, self._dedup_key(logical_key),
                    f"{self.dedup_prefix}{self._hash_key(logical_key)}",
                    self._legacy_dedup_key(logical_key), logical_key)

    def _supersede_key(self, supersede_key: str) -> str:
        return f"{self.supersede_prefix}{self._hash_key(supersede_key)}"

    def _lease_key(self, concurrency_key: str) -> str:
        return f"{self.lease_prefix}{self._hash_key(concurrency_key)}"

    def _dummy_key(self, kind: str, task_id: str) -> str:
        return f"{self.base}:__qtask_dummy__:{kind}:{task_id}"

    @staticmethod
    def _worker_id_from_processing(processing_key: str) -> str | None:
        marker = ":processing:"
        if marker not in processing_key:
            return None
        return processing_key.rsplit(marker, 1)[1]

    @staticmethod
    def _supersede_value(version: int | str | None) -> str:
        if version is None:
            return ""
        return f"i:{version}" if isinstance(version, int) else f"s:{version}"

    def _redis_now(self) -> float:
        try:
            seconds, microseconds = self.r.time()
            return float(seconds) + float(microseconds) / 1_000_000
        except Exception:
            return self.clock.now()

    def _assert_queue_namespace(self) -> None:
        """管理与领取前核验真实队列边界，避免状态键被当作另一队列。"""
        try:
            self.r.eval(CHECK_QUEUE_NAMESPACE_LUA, 0, self.base)
        except redis.ResponseError as exc:
            if "qtask queue namespace conflict:" in str(exc):
                raise ValueError(str(exc)) from exc
            raise

    def _register_processing_key(self) -> None:
        """领取前登记自定义 processing key；Worker key 由 Worker 注册表覆盖。"""
        if self.processing == self.default_processing or self._processing_key_managed_by_worker:
            return
        self.r.eval(
            REGISTER_PROCESSING_KEY_LUA,
            1,
            self.processing_registry_key,
            self.base,
            self.clock.now(),
            self.processing,
        )

    # ==================== Enqueue ====================

    def enqueue(
        self,
        spec: TaskSpec,
        *,
        on_duplicate: DuplicateAction | str = DuplicateAction.REJECT,
    ) -> EnqueueResult:
        """原子投递 TaskSpec，并返回明确的重复/接受结果。"""
        if not isinstance(spec, TaskSpec):
            raise TypeError("spec must be TaskSpec")
        duplicate_action = DuplicateAction(on_duplicate)
        self._assert_queue_namespace()
        if spec.logical_key:
            self._migrate_identity(spec.logical_key)

        if spec.logical_key and duplicate_action == DuplicateAction.REJECT:
            duplicate = self._dedup_preflight(spec.logical_key)
            if duplicate is not None:
                self.r.hincrby(self.metrics_key, f"enqueue.{duplicate.reason}", 1)
                return duplicate

        created_at = self.clock.now()
        not_before = datetime_to_epoch(spec.not_before_at)
        redis_now = self._redis_now()
        available_at = max(redis_now, not_before) if not_before is not None else redis_now
        # 新任务对象必须覆盖无界 live 与 DLQ；终态之后由 Redis 清理日志回收。
        retain_until = None
        # JSON 校验和 RemoteStorage 上传均先于 identity 占用。
        prepared = self.codec.prepare(spec.payload, retain_until=retain_until)
        task_id = str(uuid.uuid4())
        envelope = self.codec.build(
            task_id=task_id,
            spec=spec,
            prepared=prepared,
            created_at=created_at,
            available_at=available_at,
            max_attempts=self.max_attempts,
        )
        raw_message = self.codec.encode(envelope)
        record = self._task_record(
            task_id=task_id,
            spec=spec,
            prepared=prepared,
            created_at=created_at,
            available_at=available_at,
        )

        owner_key = (
            self._dedup_key(spec.logical_key)
            if spec.logical_key
            else self._dummy_key("owner", task_id)
        )
        legacy_owner_key = (
            self._legacy_dedup_key(spec.logical_key)
            if spec.logical_key
            else self._dummy_key("legacy-owner", task_id)
        )
        supersede_key = (
            self._supersede_key(spec.supersede_key)
            if spec.supersede_key
            else self._dummy_key("supersede", task_id)
        )

        try:
            result = self.r.eval(
                ENQUEUE_V2_LUA,
                11,
                self.queue,
                self.delay,
                owner_key,
                legacy_owner_key,
                f"qtask:task:{task_id}",
                self.history.idx_key,
                self.registry_key,
                supersede_key,
                self.metrics_key,
                self.deadline_key,
                self.storage_pending_key,
                "1" if spec.logical_key else "0",
                duplicate_action.value,
                task_id,
                spec.logical_key or "",
                self._epoch_string(datetime_to_epoch(spec.dedup_until)),
                raw_message,
                available_at,
                orjson.dumps(record).decode("utf-8"),
                self.base,
                "1" if spec.supersede_key else "0",
                self._supersede_value(spec.supersede_version),
                created_at,
            )
        except Exception as exc:
            # 网络结果不确定时按 task_id 查证，不盲目删除外存对象或 owner。
            committed = self.r.hget(f"qtask:task:{task_id}", "enqueue_confirmed") == "1"
            if committed and not isinstance(exc, redis.ResponseError):
                return EnqueueResult(True, task_id, spec.logical_key, None, "enqueued")
            if not self.r.exists(f"qtask:task:{task_id}"):
                self._cleanup_prepared_if_unreferenced(prepared)
            if "qtask queue namespace conflict:" in str(exc):
                raise ValueError(str(exc)) from exc
            raise

        code = str(result[0])
        if code in {"duplicate_active", "duplicate_retained"}:
            self._cleanup_prepared_if_unreferenced(prepared)
            return EnqueueResult(
                accepted=False,
                task_id=None,
                logical_key=spec.logical_key,
                duplicate_of=str(result[1]) or None,
                reason=cast(Any, code),
            )
        if code == "task_id_collision":
            self._cleanup_prepared_if_unreferenced(prepared)
            raise RuntimeError(f"generated task_id collision: {task_id}")
        if code != "enqueued":
            self._cleanup_prepared_if_unreferenced(prepared)
            raise RuntimeError(f"unexpected enqueue result: {result!r}")
        return EnqueueResult(True, task_id, spec.logical_key, None, "enqueued")

    def enqueue_many(
        self,
        specs: Sequence[TaskSpec],
        *,
        on_duplicate: DuplicateAction | str = DuplicateAction.REJECT,
    ) -> list[EnqueueResult]:
        """按输入顺序投递 TaskSpec；每项可拥有独立身份和时间边界。"""
        if len(specs) > MAX_ENQUEUE_MANY:
            raise ValueError(f"enqueue_many accepts at most {MAX_ENQUEUE_MANY} specs")
        results: list[EnqueueResult] = []
        for index, spec in enumerate(specs):
            try:
                results.append(self.enqueue(spec, on_duplicate=on_duplicate))
            except Exception as exc:
                raise BatchEnqueueError(index, results) from exc
        return results

    def push(
        self,
        payload: Dict[str, Any],
        delay_seconds: int | float = 0,
        expire_seconds: int | float = 0,
        logical_key: Optional[str] = None,
        dedup_ttl: Optional[int | float] = None,
        force: bool = False,
    ) -> Optional[str]:
        """兼容便捷接口；新代码应使用 enqueue(TaskSpec)。"""
        warnings.warn(
            "SmartQueue.push() is a compatibility API; prefer enqueue(TaskSpec)",
            DeprecationWarning,
            stacklevel=2,
        )
        if not isinstance(payload, dict):
            raise TypeError("payload must be a dict")
        action = payload.get("action")
        if not isinstance(action, str) or not action:
            raise ValueError("payload['action'] is required")
        if delay_seconds < 0 or expire_seconds < 0 or (dedup_ttl is not None and dedup_ttl < 0):
            raise ValueError("delay_seconds, expire_seconds and dedup_ttl must be >= 0")
        now = datetime.fromtimestamp(self.clock.now(), tz=timezone.utc)
        spec = TaskSpec(
            action=action,
            payload=cast(Mapping[str, JsonValue], payload),
            logical_key=logical_key,
            not_before_at=now + timedelta(seconds=float(delay_seconds)) if delay_seconds else None,
            start_deadline_at=(
                now + timedelta(seconds=float(expire_seconds)) if expire_seconds else None
            ),
            dedup_until=now + timedelta(seconds=float(dedup_ttl)) if dedup_ttl else None,
        )
        result = self.enqueue(
            spec,
            on_duplicate=DuplicateAction.ALLOW_NEW if force else DuplicateAction.REJECT,
        )
        return result.task_id if result.accepted else None

    def push_batch(
        self,
        payloads: List[Dict[str, Any]],
        delay_seconds: int | float = 0,
        expire_seconds: int | float = 0,
        logical_keys: Optional[List[Optional[str]]] = None,
        dedup_ttl: Optional[int | float] = None,
        force: bool = False,
    ) -> List[Optional[str]]:
        """兼容批量接口；内部转成 TaskSpec 列表，避免新增平行数组。"""
        warnings.warn(
            "SmartQueue.push_batch() is deprecated; prefer enqueue_many()",
            DeprecationWarning,
            stacklevel=2,
        )
        if logical_keys is not None and len(logical_keys) != len(payloads):
            raise ValueError("logical_keys length must match payloads")
        now = datetime.fromtimestamp(self.clock.now(), tz=timezone.utc)
        specs: list[TaskSpec] = []
        for index, payload in enumerate(payloads):
            action = payload.get("action") if isinstance(payload, dict) else None
            if not isinstance(action, str) or not action:
                raise ValueError(f"payloads[{index}]['action'] is required")
            specs.append(
                TaskSpec(
                    action=action,
                    payload=cast(Mapping[str, JsonValue], payload),
                    logical_key=logical_keys[index] if logical_keys else None,
                    not_before_at=(
                        now + timedelta(seconds=float(delay_seconds)) if delay_seconds else None
                    ),
                    start_deadline_at=(
                        now + timedelta(seconds=float(expire_seconds)) if expire_seconds else None
                    ),
                    dedup_until=(
                        now + timedelta(seconds=float(dedup_ttl)) if dedup_ttl else None
                    ),
                )
            )
        results = self.enqueue_many(
            specs,
            on_duplicate=DuplicateAction.ALLOW_NEW if force else DuplicateAction.REJECT,
        )
        return [result.task_id if result.accepted else None for result in results]

    @staticmethod
    def _build_envelope(
        task_id: str,
        payload: Dict[str, Any],
        expires_at: Optional[float] = None,
    ) -> str:
        """构造 V1 消息，仅供迁移测试和旧管理工具兼容。"""
        envelope: Dict[str, Any] = {"task_id": task_id, "payload": orjson.dumps(payload).decode()}
        if expires_at:
            envelope["expires_at"] = expires_at
        return orjson.dumps(envelope).decode()

    def _task_record(
        self,
        *,
        task_id: str,
        spec: TaskSpec,
        prepared: PreparedPayload,
        created_at: float,
        available_at: float,
        replay_of: str | None = None,
    ) -> dict[str, str]:
        deadline = datetime_to_epoch(spec.start_deadline_at)
        payload_ref = prepared.external_key or ""
        return {
            "task_id": task_id,
            "_queue": self.base,
            "action": spec.action,
            "logical_key": spec.logical_key or "",
            "created_at": str(created_at),
            "updated_at": str(created_at),
            "scheduled_for": self._epoch_string(datetime_to_epoch(spec.scheduled_for)),
            "not_before_at": self._epoch_string(datetime_to_epoch(spec.not_before_at)),
            "start_deadline_at": self._epoch_string(deadline),
            # expires_at 作为一个小版本的只读兼容字段。
            "expires_at": self._epoch_string(deadline),
            "dedup_until": self._epoch_string(datetime_to_epoch(spec.dedup_until)),
            "trace_id": spec.trace_id or "",
            "parent_task_id": spec.parent_task_id or "",
            "replay_of": replay_of or "",
            "concurrency_key": spec.concurrency_key or "",
            "supersede_key": spec.supersede_key or "",
            "supersede_version": self._json_scalar(spec.supersede_version),
            "supersede_value": self._supersede_value(spec.supersede_version),
            "attempt": "0",
            "max_attempts": str(self.max_attempts),
            "outcome": "",
            "status": "pending",
            "operational_message": "1",
            "available_at": str(available_at),
            "delay_reason": "schedule" if spec.not_before_at is not None else "enqueue",
            "payload_kind": prepared.kind,
            "payload_ref": payload_ref,
            "payload_dedicated": "1" if prepared.external_dedicated else "0",
            "payload_storage_id": str(prepared.descriptor.get("storage_id") or ""),
            "payload_pending": "1" if prepared.descriptor.get("pending") else "0",
            "payload_size": str(prepared.size),
            "payload_checksum": prepared.sha256,
            "payload_descriptor": orjson.dumps(prepared.descriptor).decode("utf-8"),
            "history_mode": self.history_mode.value,
        }

    def _dedup_preflight(self, logical_key: str) -> EnqueueResult | None:
        for key in (self._dedup_key(logical_key), self._legacy_dedup_key(logical_key)):
            key_type = self.r.type(key)
            if key_type == "hash" and key == self._dedup_key(logical_key):
                task_id = self.r.hget(key, "task_id")
                outcome = self.r.hget(key, "outcome") or "none"
            elif key_type == "string":
                task_id = self.r.get(key)
                outcome = "none"
                if task_id:
                    record = self.history.get(str(task_id))
                    if record:
                        outcome = str(record.get("outcome") or record.get("status") or "none")
            else:
                continue
            if task_id:
                reason = (
                    "duplicate_retained"
                    if outcome not in {"", "none", "pending", "retry", "processing"}
                    else "duplicate_active"
                )
                return EnqueueResult(False, None, logical_key, str(task_id), cast(Any, reason))
        return None

    def _cleanup_prepared_if_unreferenced(self, prepared: PreparedPayload) -> None:
        if (
            not prepared.external_created
            or not prepared.external_dedicated
            or not prepared.external_key
            or self.storage is None
        ):
            return
        self.storage.delete(prepared.external_key)

    # ==================== Claim / pop ====================

    def pop_claim(
        self,
        timeout: int = 10,
        *,
        _on_started: Callable[[TaskClaim], None] | None = None,
        _on_discarded: Callable[[TaskClaim], None] | None = None,
        _stop_requested: Callable[[], bool] | None = None,
    ) -> TaskClaim | None:
        """阻塞领取并原子开始一次 attempt。"""
        self._assert_queue_namespace()
        self._register_processing_key()
        while True:
            if _stop_requested is not None and _stop_requested():
                return None
            try:
                raw_message = self.r.brpoplpush(self.queue, self.processing, timeout)
            except Exception as exc:
                logger.error(f"Pop failed: {exc}")
                return None
            if not raw_message:
                return None
            claim = self._claim_popped(
                str(raw_message), _on_started=_on_started, _on_discarded=_on_discarded
            )
            if claim is not None:
                return claim

    def pop_claim_no_wait(self) -> TaskClaim | None:
        """非阻塞领取并原子开始一次 attempt。"""
        self._assert_queue_namespace()
        self._register_processing_key()
        while True:
            try:
                raw_message = self.r.rpoplpush(self.queue, self.processing)
            except Exception as exc:
                logger.error(f"Pop no wait failed: {exc}")
                return None
            if not raw_message:
                return None
            claim = self._claim_popped(str(raw_message))
            if claim is not None:
                return claim

    def pop(self, timeout: int = 10) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
        """兼容接口：返回业务 payload 与已开始 attempt 的 V2 raw message。"""
        claim = self.pop_claim(timeout=timeout)
        if claim is None:
            return None, None
        return cast(Dict[str, Any], claim.payload), claim.raw_message

    def pop_no_wait(self) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
        """兼容非阻塞 pop。"""
        claim = self.pop_claim_no_wait()
        if claim is None:
            return None, None
        return cast(Dict[str, Any], claim.payload), claim.raw_message

    def _claim_popped(
        self,
        raw_message: str,
        *,
        _on_started: Callable[[TaskClaim], None] | None = None,
        _on_discarded: Callable[[TaskClaim], None] | None = None,
    ) -> TaskClaim | None:
        try:
            header = self.codec.header(raw_message)
        except Exception as exc:
            self._move_poison_to_dlq(raw_message, "invalid_envelope", str(exc))
            return None

        if header.version == 1:
            migrated = self._migrate_v1_message(raw_message, header)
            if migrated is None:
                return None
            raw_message = migrated
            header = self.codec.header(raw_message)

        self._ensure_task_record(header)
        if header.logical_key:
            self._migrate_identity(header.logical_key)
        owner_key = (
            self._dedup_key(header.logical_key)
            if header.logical_key
            else self._dummy_key("owner", header.task_id)
        )
        supersede_key = (
            self._supersede_key(header.supersede_key)
            if header.supersede_key
            else self._dummy_key("supersede", header.task_id)
        )
        lease_key = (
            self._lease_key(header.concurrency_key)
            if header.concurrency_key
            else self._dummy_key("lease", header.task_id)
        )
        lease_token = uuid.uuid4().hex if header.concurrency_key else ""
        started_data = dict(header.raw)
        claim_token = uuid.uuid4().hex
        started_data["claim_token"] = claim_token
        started_data.pop("lease_token", None)
        if lease_token:
            started_data["lease_token"] = lease_token
        deferred_data = dict(header.raw)
        deferred_data.pop("lease_token", None)

        result = self.r.eval(
            BEGIN_ATTEMPT_LUA,
            10,
            self.processing,
            f"qtask:task:{header.task_id}",
            self.dlq,
            owner_key,
            supersede_key,
            lease_key,
            self.delay,
            self.metrics_key,
            self.storage_gc_key,
            self.deadline_key,
            raw_message,
            header.task_id,
            "1" if header.logical_key else "0",
            "1" if header.supersede_key else "0",
            self._supersede_value(header.supersede_version),
            "1" if header.concurrency_key else "0",
            lease_token,
            int(self.concurrency_lease_seconds * 1000),
            self.concurrency_retry_seconds,
            self.ttl_seconds,
            self.codec.runtime_tail(started_data, "attempt"),
            self.codec.runtime_tail(deferred_data, "available_at", delay_reason="concurrency"),
            claim_token,
        )
        code = str(result[0])
        if code != "started":
            if code not in {"skipped", "cancelled", "deferred", "failed_budget", "not_found"}:
                logger.warning(f"begin_attempt ignored task={header.task_id} result={result!r}")
            return None

        started_message = str(result[1])
        started_header = self.codec.header(started_message)
        context = TaskContext(
            task_id=started_header.task_id,
            action=started_header.action,
            logical_key=started_header.logical_key,
            attempt=started_header.attempt,
            max_attempts=started_header.max_attempts,
            scheduled_for=epoch_to_datetime(started_header.scheduled_for),
            start_deadline_at=epoch_to_datetime(started_header.start_deadline_at),
            trace_id=started_header.trace_id,
            parent_task_id=started_header.parent_task_id,
            replay_of=started_header.replay_of,
            worker_id=self.worker_id,
        )
        provisional = TaskClaim(
            payload={},
            raw_message=started_message,
            context=context,
            lease_key=lease_key if started_header.concurrency_key else None,
            lease_token=lease_token or None,
        )
        if _on_started:
            _on_started(provisional)
        try:
            payload = self.codec.decode_payload(started_header)
            if started_header.raw.get("legacy_payload"):
                action = payload.get("action")
                if not isinstance(action, str) or not action:
                    raise PayloadDecodeError("configuration", "V1 payload action is required")
                if action != started_header.action:
                    updated = dict(started_header.raw, action=action)
                    resolved_message = self.codec.encode(updated)
                    script = r"""
                    if redis.call('LREM', KEYS[1], 1, ARGV[1]) == 0 then return 0 end
                    redis.call('LPUSH', KEYS[1], ARGV[2])
                    redis.call('HSET', KEYS[2], 'action', ARGV[3])
                    return 1
                    """
                    if not self.r.eval(script, 2, self.processing,
                                       f"qtask:task:{header.task_id}", started_message,
                                       resolved_message, action):
                        if _on_discarded:
                            _on_discarded(provisional)
                        return None
                    started_message = resolved_message
                    provisional = replace(provisional, raw_message=started_message,
                                          context=replace(context, action=action))
                    if _on_started:
                        _on_started(provisional)
        except redis.RedisError:
            if _on_discarded:
                _on_discarded(replace(provisional, settlement_pending=True))
            raise
        except RemoteStorageTransientError as exc:
            settled = False
            try:
                self.fail(
                    started_message,
                    str(exc),
                    code=exc.code,
                    retry_after=exc.retry_after,
                )
                settled = True
            finally:
                if _on_discarded:
                    _on_discarded(replace(provisional, settlement_pending=not settled))
            return None
        except RemoteStorageError as exc:
            settled = False
            try:
                self.fail(started_message, str(exc), code=exc.code, permanent=True)
                settled = True
            finally:
                if _on_discarded:
                    _on_discarded(replace(provisional, settlement_pending=not settled))
            return None
        except Exception as exc:
            settled = False
            try:
                self.fail(
                    started_message,
                    str(exc),
                    code=str(getattr(exc, "code", "payload_decode")),
                    permanent=True,
                )
                settled = True
            finally:
                if _on_discarded:
                    _on_discarded(replace(provisional, settlement_pending=not settled))
            return None
        return replace(provisional, payload=payload)

    def _ensure_task_record(self, header: EnvelopeHeader) -> None:
        key = f"qtask:task:{header.task_id}"
        if self.r.exists(key):
            self.history.ensure_hash(header.task_id)
            return
        descriptor = header.raw.get("payload", {})
        now = float(header.raw.get("created_at") or self.clock.now())
        self.history.record(
            header.task_id,
            {
                "_queue": self.base,
                "action": header.action,
                "logical_key": header.logical_key or "",
                "attempt": header.attempt,
                "max_attempts": header.max_attempts,
                "start_deadline_at": header.start_deadline_at or "",
                "expires_at": header.start_deadline_at or "",
                "created_at": now,
                "payload_descriptor": descriptor,
                "payload_kind": descriptor.get("kind", "") if isinstance(descriptor, dict) else "",
                "history_mode": self.history_mode.value,
            },
        )

    def _migrate_v1_message(
        self,
        raw_message: str,
        header: EnvelopeHeader,
    ) -> str | None:
        """读取外存前将旧引用迁入 V2，复用 deadline、attempt 与终态脚本。"""
        self.history.ensure_hash(header.task_id)
        existing = self.history.get(header.task_id) or {}
        try:
            payload_raw = header.raw.get("payload", {})
            payload = json.loads(payload_raw) if isinstance(payload_raw, str) else payload_raw
            if not isinstance(payload, dict):
                raise ValueError("V1 payload must be an object")
            descriptor: dict[str, Any]
            if payload.get("_large"):
                descriptor = {"kind": "external", "key": str(payload.get("key") or ""), "legacy": True}
            elif payload.get("_compressed"):
                descriptor = {"kind": "zstd", "data": payload["data"], "legacy": True}
            else:
                descriptor = {"kind": "inline", "data": payload, "legacy": True}
            previous_attempt = max(0, int(payload.get("_retry", existing.get("attempt") or existing.get("retry") or 0)))
            action = payload.get("action") or existing.get("action") or "__qtask_legacy__"
            if not isinstance(action, str):
                raise ValueError("V1 action must be a string")
        except (ValueError, TypeError, KeyError) as exc:
            self._move_poison_to_dlq(raw_message, "invalid_payload", str(exc))
            return None

        logical_key = self._optional_str(existing.get("logical_key"))
        if logical_key:
            self._migrate_identity(logical_key)
        try:
            spec = TaskSpec(action=action, payload={}, logical_key=logical_key,
                            start_deadline_at=epoch_to_datetime(header.start_deadline_at))
        except (ValueError, TypeError) as exc:
            self._move_poison_to_dlq(raw_message, "configuration", str(exc))
            return None
        prepared = PreparedPayload(descriptor=descriptor, kind=str(descriptor["kind"]),
                                   size=0, sha256="", external_key=descriptor.get("key"))
        created_at = float(existing.get("created_at") or self.clock.now())
        available_at = self._redis_now()
        envelope = self.codec.build(task_id=header.task_id, spec=spec, prepared=prepared,
                                    created_at=created_at, available_at=available_at,
                                    max_attempts=self.max_attempts)
        envelope["attempt"] = previous_attempt
        envelope["legacy_payload"] = True
        migrated = self.codec.encode(envelope)
        record = self._task_record(task_id=header.task_id, spec=spec, prepared=prepared,
                                   created_at=created_at, available_at=available_at)
        record.update(attempt=str(previous_attempt), location="processing", legacy_payload="1")
        # 消息与必要记录同一脚本转换，中断不会留下半迁移任务。
        script = r"""
        for index, expected in ipairs({'list', 'hash', 'zset', 'zset'}) do
            local kind = redis.call('TYPE', KEYS[index])['ok']
            if kind ~= 'none' and kind ~= expected then error('legacy migration key type conflict') end
        end
        local outcome = redis.call('HGET', KEYS[2], 'outcome') or ''
        if outcome ~= '' and outcome ~= 'none' then return 0 end
        local record = cjson.decode(ARGV[3])
        if redis.call('LREM', KEYS[1], 1, ARGV[1]) == 0 then return 0 end
        redis.call('LPUSH', KEYS[1], ARGV[2])
        for field, value in pairs(record) do redis.call('HSET', KEYS[2], field, value) end
        redis.call('PERSIST', KEYS[2])
        redis.call('ZADD', KEYS[3], 'NX', ARGV[4], ARGV[5])
        redis.call('ZADD', KEYS[4], ARGV[6], ARGV[7])
        return 1
        """
        moved = self.r.eval(script, 4, self.processing, f"qtask:task:{header.task_id}",
                            self.history.idx_key, self.registry_key, raw_message, migrated,
                            orjson.dumps(record).decode(), created_at, header.task_id,
                            available_at, self.base)
        return migrated if moved else None

    # ==================== Complete / fail ====================

    def ack(self, raw_msg: str, result: Any = None) -> bool:
        """原子完成任务；重复 ack 不产生第二次历史写入。"""
        try:
            header = self.codec.header(raw_msg)
        except Exception as exc:
            logger.error(f"Ack decode failed: {exc}")
            return False
        if header.logical_key:
            self._migrate_identity(header.logical_key)
        owner_key = (
            self._dedup_key(header.logical_key)
            if header.logical_key
            else self._dummy_key("owner", header.task_id)
        )
        lease_key = (
            self._lease_key(header.concurrency_key)
            if header.concurrency_key
            else self._dummy_key("lease", header.task_id)
        )
        result_json = ""
        if result is not None:
            try:
                result_json = orjson.dumps(result).decode("utf-8")
            except (TypeError, ValueError):
                result_json = orjson.dumps(str(result)).decode("utf-8")
        response = self.r.eval(
            COMPLETE_TASK_LUA,
            7,
            self.processing,
            f"qtask:task:{header.task_id}",
            owner_key,
            lease_key,
            self.metrics_key,
            self.storage_gc_key,
            self.deadline_key,
            raw_msg,
            header.task_id,
            "1" if header.logical_key else "0",
            header.lease_token or "",
            self.ttl_seconds,
            result_json,
            self.history_mode.value,
        )
        return str(response[0]) == "completed"

    def fail(
        self,
        raw_msg: str,
        reason: str = "",
        *,
        code: str = "handler_error",
        retry_after: float | None = None,
        permanent: bool = False,
    ) -> bool:
        """原子失败转换；实际 attempt 已在 begin_attempt 消耗。"""
        try:
            header = self.codec.header(raw_msg)
        except Exception as exc:
            logger.error(f"Fail decode failed: {exc}")
            self._move_poison_to_dlq(raw_msg, "invalid_envelope", str(exc))
            return False
        if header.logical_key:
            self._migrate_identity(header.logical_key)
        owner_key = (
            self._dedup_key(header.logical_key)
            if header.logical_key
            else self._dummy_key("owner", header.task_id)
        )
        lease_key = (
            self._lease_key(header.concurrency_key)
            if header.concurrency_key
            else self._dummy_key("lease", header.task_id)
        )

        if permanent or header.attempt >= header.max_attempts:
            data = dict(header.raw)
            data["last_error"] = {"code": code, "reason": reason}
            dlq_message = self.codec.encode(data)
            response = self.r.eval(
                FAIL_TASK_LUA,
                7,
                self.processing,
                f"qtask:task:{header.task_id}",
                self.dlq,
                owner_key,
                lease_key,
                self.metrics_key,
                self.deadline_key,
                raw_msg,
                dlq_message,
                header.task_id,
                code,
                reason,
                "1" if header.logical_key else "0",
                header.lease_token or "",
            )
            return str(response[0]) == "failed"

        delay = min(
            self.retry_backoff_base * (2 ** max(header.attempt - 1, 0)),
            self.retry_backoff_max,
        )
        if delay > 0:
            delay *= random.uniform(0.9, 1.1)
        if retry_after is not None:
            delay = max(delay, float(retry_after))
        run_at = self._redis_now() + delay
        data = dict(header.raw)
        data["available_at"] = run_at
        data["delay_reason"] = "retry"
        data["last_error"] = {"code": code, "reason": reason}
        next_message = self.codec.encode(data)
        response = self.r.eval(
            RETRY_TASK_LUA,
            8,
            self.processing,
            f"qtask:task:{header.task_id}",
            self.delay,
            owner_key,
            lease_key,
            self.metrics_key,
            self.storage_gc_key,
            self.deadline_key,
            raw_msg,
            next_message,
            header.task_id,
            run_at,
            code,
            reason,
            self.ttl_seconds,
            "1" if header.logical_key else "0",
            header.lease_token or "",
        )
        return str(response[0]) in {"retry", "skipped"}

    def renew_claim_lease(self, claim: TaskClaim) -> bool:
        """续租 keyed concurrency lease；token 防止旧 Worker 续租新锁。"""
        if not claim.lease_key or not claim.lease_token:
            return True
        return bool(
            self.r.eval(
                RENEW_LEASE_LUA,
                1,
                claim.lease_key,
                claim.lease_token,
                int(self.concurrency_lease_seconds * 1000),
            )
        )

    def return_unstarted_claim(self, claim: TaskClaim) -> bool:
        """停机时原子退回尚未进入 handler 的 claim，并恢复执行预算。"""
        response = self.r.eval(
            RETURN_UNSTARTED_CLAIM_LUA,
            6,
            self.processing,
            self.queue,
            f"qtask:task:{claim.context.task_id}",
            self.deadline_key,
            claim.lease_key or self._dummy_key("lease", claim.context.task_id),
            self.metrics_key,
            claim.raw_message,
            claim.context.task_id,
            claim.context.attempt,
            claim.lease_token or "",
        )
        return str(response[0]) == "returned"

    def release_claim_lease(self, claim: TaskClaim) -> bool:
        """显式释放 lease；正常 ack/fail 已在状态脚本内完成。"""
        if not claim.lease_key or not claim.lease_token:
            return True
        return bool(
            self.r.eval(RELEASE_LEASE_LUA, 1, claim.lease_key, claim.lease_token)
        )

    def _move_poison_to_dlq(self, raw_msg: str, code: str, reason: str) -> None:
        """把无法进入 V2 状态机的 poison message 原子移入 DLQ。"""
        move_script = r"""
        local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
        if removed == 0 then return 0 end
        redis.call('LPUSH', KEYS[2], ARGV[1])
        return 1
        """
        moved = self.r.eval(move_script, 2, self.processing, self.dlq, raw_msg)
        if not moved:
            return
        task_id = self._message_task_id(raw_msg)
        if task_id and self.r.exists(f"qtask:task:{task_id}"):
            self.history.update(
                task_id,
                {
                    "outcome": "failed",
                    "status": "failed",
                    "reason_code": code,
                    "reason": reason,
                    "operational_message": 1,
                    "location": "dlq",
                },
            )
        self.r.hincrby(self.metrics_key, f"failure.code.{code}", 1)
        logger.error(f"Poison task moved to DLQ task_id={task_id or '?'} reason={reason}")

    # ==================== Delay / recovery ====================

    def move_retry(self) -> int:
        """迁移 V1 retry List，并同时迁移已经到期的 V2 retry_wait。"""
        self._assert_queue_namespace()
        count = 0
        while True:
            message = self.r.rpoplpush(self.retry, self.queue)
            if not message:
                break
            count += 1
            task_id = self._message_task_id(str(message))
            if task_id:
                record = self.history.get(task_id)
                if record and not record.get("outcome"):
                    # V1 retry List 没有统一状态脚本，兼容迁移时同步回 ready。
                    self.history.update(task_id, {
                        "status": "pending", "operational_message": 1, "location": "ready"
                    })
        return count + self.move_delay()

    def move_delay(self) -> int:
        """原子迁移已经到期的 schedule/retry/concurrency delay。"""
        self._assert_queue_namespace()
        result = self.r.eval(
            MOVE_DUE_DELAY_LUA,
            3,
            self.delay,
            self.queue,
            self.metrics_key,
            MOVE_DELAY_BATCH,
        )
        return int(result or 0)

    def recover(self) -> int:
        """恢复当前 processing key；不改变 task_id、attempt 或 outcome。"""
        return self.recover_processing_key(self.processing)

    def recover_processing_key(self, processing_key: str, *, heartbeat_key: str | None = None) -> int:
        self._assert_queue_namespace()
        script = r"""
        local deadline_kind = redis.call('TYPE', KEYS[3])['ok']
        if deadline_kind ~= 'none' and deadline_kind ~= 'zset' then error('deadline index type conflict') end
        if ARGV[1] == '1' and redis.call('EXISTS', KEYS[4]) == 1 then return false end
        local message = redis.call('RPOPLPUSH', KEYS[1], KEYS[2])
        if not message then return false end
        local ok, envelope = pcall(cjson.decode, message)
        if ok and type(envelope) == 'table' and envelope['task_id'] then
            local record_key = 'qtask:task:' .. tostring(envelope['task_id'])
            local kind = redis.call('TYPE', record_key)
            if type(kind) == 'table' then kind = kind['ok'] end
            if kind == 'hash' then
                redis.call('HSET', record_key, 'location', 'ready')
                local deadline = redis.call('HGET', record_key, 'start_deadline_at') or ''
                if deadline ~= '' then redis.call('ZADD', KEYS[3], deadline, tostring(envelope['task_id'])) end
            end
        end
        return message
        """
        count = 0
        while True:
            message = self.r.eval(script, 4, processing_key, self.queue, self.deadline_key,
                                  heartbeat_key or self._dummy_key("heartbeat", "recovery"),
                                  "1" if heartbeat_key else "0")
            if not message:
                break
            count += 1
        if count:
            self.r.hincrby(self.metrics_key, "recovery.stale", count)
        return count

    def recover_abandoned_claims(
        self, protected_messages: set[str], *, spent_attempts: set[str] | None = None,
    ) -> int:
        """恢复当前 Worker 未登记的消息；调用方须保证没有并发领取过程。"""
        count = 0
        spent_attempts = spent_attempts or set()
        for raw_message in self.r.lrange(self.processing, 0, -1):
            if raw_message in protected_messages:
                continue
            try:
                header = self.codec.header(raw_message)
            except RemoteStorageError:
                self._move_poison_to_dlq(raw_message, "invalid_envelope", "abandoned invalid claim")
                continue
            lease_key = (self._lease_key(header.concurrency_key) if header.concurrency_key
                         else self._dummy_key("lease", header.task_id))
            script = r"""
            for index, expected in ipairs({'list', 'list', 'hash,string', 'zset', 'string', 'list'}) do
                local kind = redis.call('TYPE', KEYS[index])['ok']
                if kind ~= 'none' and not string.find(expected, kind, 1, true) then
                    error('recovery key type conflict')
                end
            end
            if redis.call('LREM', KEYS[1], 1, ARGV[1]) == 0 then return 0 end
            local destination = KEYS[2]
            if redis.call('TYPE', KEYS[3])['ok'] == 'hash' then
                local outcome = redis.call('HGET', KEYS[3], 'outcome') or ''
                if outcome == 'failed' then
                    destination = KEYS[6]
                    redis.call('HSET', KEYS[3], 'location', 'dlq', 'operational_message', '1')
                elseif outcome ~= '' and outcome ~= 'none' then
                    destination = ''
                else
                    local location = redis.call('HGET', KEYS[3], 'location') or ''
                    redis.call('HSET', KEYS[3], 'location', 'ready')
                    if ARGV[4] == '1' and location == 'processing' and ARGV[6] ~= '' and
                       redis.call('HGET', KEYS[3], 'claim_token') == ARGV[6] then
                        local attempt = tonumber(redis.call('HGET', KEYS[3], 'attempt') or '0')
                        if attempt > 0 and attempt == tonumber(ARGV[5]) then
                            redis.call('HSET', KEYS[3], 'attempt', tostring(attempt - 1))
                        end
                    end
                    local deadline = redis.call('HGET', KEYS[3], 'start_deadline_at') or ''
                    if deadline ~= '' then redis.call('ZADD', KEYS[4], deadline, ARGV[2]) end
                end
            end
            if destination ~= '' then redis.call('LPUSH', destination, ARGV[1]) end
            if ARGV[3] ~= '' and redis.call('GET', KEYS[5]) == ARGV[3] then
                redis.call('DEL', KEYS[5])
            end
            return 1
            """
            count += int(self.r.eval(script, 6, self.processing, self.queue,
                                     f"qtask:task:{header.task_id}", self.deadline_key,
                                     lease_key, self.dlq, raw_message, header.task_id,
                                     header.lease_token or "",
                                     "0" if header.task_id in spent_attempts else "1",
                                     header.attempt, header.raw.get("claim_token") or "") or 0)
        return count

    def processing_keys(self, include_legacy: bool = True) -> List[str]:
        keys = set(self.r.zrange(self.processing_registry_key, 0, -1))
        keys.update(
            f"{self.base}:processing:{worker_id}"
            for worker_id in self.r.zrange(self.worker_registry_key, 0, -1)
        )
        if self.processing != self.default_processing:
            keys.add(self.processing)
        if include_legacy:
            keys.add(self.default_processing)
        return sorted(str(key) for key in keys)

    def recover_stale_processing(self, heartbeat_prefix: str) -> int:
        count = 0
        for key in self.processing_keys(include_legacy=False):
            worker_id = key.rsplit(":", 1)[-1]
            if self.r.exists(f"{heartbeat_prefix}{worker_id}"):
                continue
            count += self.recover_processing_key(key, heartbeat_key=f"{heartbeat_prefix}{worker_id}")
        return count

    # ==================== Replay ====================

    def replay_task(
        self,
        task_id: str,
        *,
        start_deadline_at: datetime | None = None,
        payload: Mapping[str, JsonValue] | object = _UNSET,
        logical_key: str | None | object = _UNSET,
        action: str | None = None,
        max_attempts: int | None = None,
        require_dlq: bool = False,
    ) -> EnqueueResult:
        """创建新实例；require_dlq=True 时必须原子移除原 DLQ 消息才能创建。"""
        self._assert_queue_namespace()
        replay_budget = self.max_attempts if max_attempts is None else max_attempts
        if isinstance(replay_budget, bool) or not isinstance(replay_budget, int) or replay_budget < 1:
            raise ValueError("max_attempts must be a positive integer")
        source = self.history.get(task_id)
        if not source:
            raise KeyError(f"task not found: {task_id}")
        self.history.ensure_hash(task_id)
        outcome = str(source.get("outcome") or source.get("status") or "")
        if outcome not in {"failed", "skipped", "cancelled"}:
            raise ValueError(f"task outcome is not replayable: {outcome or 'none'}")
        source_raw = self._find_task_message(self.dlq, task_id)
        require_source = require_dlq or str(source.get("operational_message", "0")) in {"1", "true", "True"}
        if require_source and source_raw is None:
            raise RuntimeError("任务已不在 DLQ 中，请刷新核对状态")

        old_deadline = self._float_or_none(
            source.get("start_deadline_at") or source.get("expires_at")
        )
        chosen_deadline = datetime_to_epoch(start_deadline_at)
        if chosen_deadline is None:
            chosen_deadline = old_deadline
        if chosen_deadline is not None and chosen_deadline <= self._redis_now():
            raise ValueError("replay requires a future start_deadline_at")

        chosen_logical = (
            self._optional_str(source.get("logical_key"))
            if logical_key is _UNSET
            else cast(str | None, logical_key)
        )
        chosen_action = action or str(source.get("action") or "")
        if not chosen_action:
            raise ValueError("replay source has no action; provide action explicitly")
        if chosen_logical:
            self._migrate_identity(chosen_logical)

        scheduled_for = epoch_to_datetime(self._float_or_none(source.get("scheduled_for")))
        dedup_until = epoch_to_datetime(self._float_or_none(source.get("dedup_until")))
        supersede_version = source.get("supersede_version")
        if supersede_version == "":
            supersede_version = None
        spec = TaskSpec(
            action=chosen_action,
            payload={} if payload is _UNSET else cast(Mapping[str, JsonValue], payload),
            logical_key=chosen_logical,
            scheduled_for=scheduled_for,
            start_deadline_at=epoch_to_datetime(chosen_deadline),
            dedup_until=dedup_until,
            trace_id=self._optional_str(source.get("trace_id")),
            parent_task_id=self._optional_str(source.get("parent_task_id")),
            concurrency_key=self._optional_str(source.get("concurrency_key")),
            supersede_key=self._optional_str(source.get("supersede_key")),
            supersede_version=cast(int | str | None, supersede_version),
        )

        if payload is _UNSET:
            descriptor = source.get("payload_descriptor")
            if isinstance(descriptor, str):
                descriptor = orjson.loads(descriptor)
            if not isinstance(descriptor, dict):
                raise ValueError("replay source does not retain a payload descriptor")
            prepared = prepared_from_descriptor(descriptor)
            if prepared.kind == "external" or prepared.descriptor.get("legacy"):
                replay_header = self.codec.header(self.codec.encode({
                    "version": 2, "task_id": task_id, "action": chosen_action,
                    "payload": prepared.descriptor,
                }))
                replay_payload = self.codec.decode_payload(replay_header)
                if source.get("legacy_payload") and action is None:
                    legacy_action = replay_payload.get("action")
                    if isinstance(legacy_action, str) and legacy_action:
                        spec = replace(spec, action=legacy_action)
                prepared = self.codec.prepare(replay_payload, retain_until=None)
        else:
            prepared = self.codec.prepare(
                cast(Mapping[str, JsonValue], payload),
                retain_until=None,
            )

        created_at = self.clock.now()
        replay_available_at = self._redis_now()
        new_task_id = str(uuid.uuid4())
        envelope = self.codec.build(
            task_id=new_task_id,
            spec=spec,
            prepared=prepared,
            created_at=created_at,
            available_at=replay_available_at,
            max_attempts=replay_budget,
            replay_of=task_id,
        )
        raw_message = self.codec.encode(envelope)
        record = self._task_record(
            task_id=new_task_id,
            spec=spec,
            prepared=prepared,
            created_at=created_at,
            available_at=replay_available_at,
            replay_of=task_id,
        )
        record["max_attempts"] = str(replay_budget)
        owner_key = self._dedup_key(chosen_logical) if chosen_logical else self._dummy_key("owner", new_task_id)
        legacy_owner_key = (
            self._legacy_dedup_key(chosen_logical)
            if chosen_logical
            else self._dummy_key("legacy-owner", new_task_id)
        )
        supersede_key = (
            self._supersede_key(spec.supersede_key)
            if spec.supersede_key
            else self._dummy_key("supersede", new_task_id)
        )
        try:
            result = self.r.eval(
                REPLAY_TASK_LUA,
                14,
                self.dlq,
                self.queue,
                self.delay,
                owner_key,
                legacy_owner_key,
                f"qtask:task:{task_id}",
                f"qtask:task:{new_task_id}",
                self.history.idx_key,
                self.registry_key,
                supersede_key,
                self.metrics_key,
                self.storage_gc_key,
                self.deadline_key,
                self.storage_pending_key,
                source_raw or "",
                "1" if require_source else "0",
                task_id,
                new_task_id,
                "1" if chosen_logical else "0",
                chosen_logical or "",
                self._epoch_string(datetime_to_epoch(spec.dedup_until)),
                raw_message,
                replay_available_at,
                orjson.dumps(record).decode(),
                self.base,
                "1" if spec.supersede_key else "0",
                self._supersede_value(spec.supersede_version),
                created_at,
                self.ttl_seconds,
            )
        except Exception as exc:
            committed = self.r.hget(f"qtask:task:{new_task_id}", "enqueue_confirmed") == "1"
            if committed and not isinstance(exc, redis.ResponseError):
                return EnqueueResult(True, new_task_id, chosen_logical, None, "enqueued")
            if not self.r.exists(f"qtask:task:{new_task_id}"):
                self._cleanup_prepared_if_unreferenced(prepared)
            raise
        code = str(result[0])
        if code in {"duplicate_active", "duplicate_retained"}:
            self._cleanup_prepared_if_unreferenced(prepared)
            return EnqueueResult(False, None, chosen_logical, str(result[1]), cast(Any, code))
        if code != "enqueued":
            self._cleanup_prepared_if_unreferenced(prepared)
            raise RuntimeError(f"replay failed: {result!r}")
        return EnqueueResult(True, new_task_id, chosen_logical, None, "enqueued")

    def requeue_dlq(self, reset_retry: bool = True) -> int:
        """兼容接口：V2 任务均以新 task_id replay；V1 无历史消息按旧方式迁移。"""
        if not reset_retry:
            warnings.warn(
                "reset_retry=False is ignored for V2 replay; a new instance always starts at attempt 0",
                DeprecationWarning,
                stacklevel=2,
            )
        count = 0
        for raw_message in list(reversed(self.r.lrange(self.dlq, 0, -1))):
            task_id = self._message_task_id(raw_message)
            if not task_id:
                continue
            record = self.history.get(task_id)
            descriptor = record.get("payload_descriptor") if record else None
            if isinstance(descriptor, str):
                try:
                    descriptor = orjson.loads(descriptor)
                except (orjson.JSONDecodeError, TypeError):
                    descriptor = None
            replayable = (
                record is not None
                and bool(str(record.get("action") or ""))
                and isinstance(descriptor, dict)
            )
            if (
                record is not None
                and replayable
                and str(record.get("outcome") or record.get("status")) in {
                    "failed",
                    "skipped",
                    "cancelled",
                }
            ):
                try:
                    result = self.replay_task(task_id, require_dlq=True)
                except (ValueError, KeyError, RuntimeError):
                    result = None
                if result is not None:
                    if result.accepted:
                        count += 1
                    continue
            try:
                is_v2 = json.loads(raw_message).get("version") == 2
            except (ValueError, TypeError, AttributeError):
                is_v2 = False
            if is_v2:
                # 不把缺失审计/descriptor 的 V2 信封降级成 V1 业务参数。
                continue
            new_message = self._reset_retry_message(raw_message) if reset_retry else raw_message
            moved = self.r.eval(
                ADMIN_MOVE_LUA,
                3,
                self.dlq,
                self.queue,
                f"qtask:task:{task_id}",
                "list",
                raw_message,
                new_message,
                task_id,
            )
            if str(moved[0]) == "moved":
                count += 1
                if record and not record.get("outcome"):
                    # 旧 V1 记录没有不可变 outcome，可以恢复为 pending；
                    # V2 terminal 记录不会走此兼容分支。
                    self.history.update(task_id, {"status": "pending", "operational_message": 1})
        return count

    # ==================== Queue management ====================

    def size(self) -> int:
        return int(self.r.llen(self.queue))

    def processing_size(self) -> int:
        return sum(int(self.r.llen(key)) for key in self.processing_keys())

    def retry_size(self) -> int:
        return int(self.r.llen(self.retry))

    def dlq_size(self) -> int:
        return int(self.r.llen(self.dlq))

    def delay_size(self) -> int:
        return int(self.r.zcard(self.delay))

    def retry_wait_size(self) -> int:
        """统计 delay ZSET 中由自动失败重试产生的等待任务。"""
        count = 0
        for raw_message, _score in self.r.zscan_iter(self.delay):
            try:
                envelope = orjson.loads(raw_message)
            except (orjson.JSONDecodeError, TypeError):
                continue
            if isinstance(envelope, dict) and envelope.get("delay_reason") == "retry":
                count += 1
        return count

    def get_stats(self) -> dict[str, int | float]:
        """获取 operational 深度与 DLQ 最老年龄。"""
        return {
            "queue": self.size(),
            "ready": self.size(),
            "processing": self.processing_size(),
            "retry": self.retry_size(),
            "retry_wait": self.retry_wait_size(),
            "dlq": self.dlq_size(),
            "delay": self.delay_size(),
            "dlq_oldest_age": self.dlq_oldest_age(),
        }

    def metrics(self) -> dict[str, int | float]:
        """返回队列累计状态转换计数。"""
        result: dict[str, int | float] = {}
        for key, value in self.r.hgetall(self.metrics_key).items():
            try:
                result[str(key)] = int(value)
            except (TypeError, ValueError):
                try:
                    result[str(key)] = float(value)
                except (TypeError, ValueError):
                    continue
        return result

    def check_consistency(
        self,
        *,
        limit: int = 1000,
        repair_safe: bool = False,
    ) -> dict[str, Any]:
        """低频检查队列内部不变式。

        repair_safe 只修复所有权明确的 stale history index 与 operational 标记；
        orphan owner 等歧义问题只报告，不猜测性删除。
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")
        issues: list[dict[str, Any]] = []
        locations: dict[str, list[str]] = {}
        scanned_messages = 0

        list_sources = [
            (self.queue, "ready"),
            (self.retry, "retry"),
            (self.dlq, "dlq"),
            *[(key, "processing") for key in self.processing_keys()],
        ]
        for key, location in list_sources:
            for raw_message in self.r.lrange(key, 0, max(limit - scanned_messages - 1, -1)):
                task_id = self._message_task_id(raw_message)
                if task_id:
                    locations.setdefault(task_id, []).append(location)
                scanned_messages += 1
                if scanned_messages >= limit:
                    break
            if scanned_messages >= limit:
                break
        if scanned_messages < limit:
            for raw_message, _score in self.r.zscan_iter(self.delay):
                task_id = self._message_task_id(raw_message)
                if task_id:
                    locations.setdefault(task_id, []).append("delay")
                scanned_messages += 1
                if scanned_messages >= limit:
                    break

        message_scan_complete = scanned_messages < limit
        unverified_tasks = 0
        for processing_key in self.processing_keys(include_legacy=False):
            if not self.r.llen(processing_key):
                continue
            worker_id = processing_key.rsplit(":", 1)[-1]
            if not self.r.exists(f"{self.base}:worker:{worker_id}"):
                issues.append(
                    {
                        "type": "processing_without_heartbeat",
                        "processing_key": processing_key,
                        "worker_id": worker_id,
                    }
                )

        owner_count = 0
        for owner_key in chain(self.r.scan_iter(f"{self.owner_prefix}*"),
                               self.r.scan_iter(f"{self.dedup_prefix}*")):
            if owner_count >= limit:
                break
            owner_count += 1
            owner_type = self.r.type(owner_key)
            owner_task_id = (
                self.r.hget(owner_key, "task_id")
                if owner_type == "hash"
                else self.r.get(owner_key)
                if owner_type == "string"
                else None
            )
            retained = owner_type == "hash" and self.r.hget(owner_key, "outcome") in {
                "completed", "failed", "skipped", "cancelled",
            }
            if owner_task_id and not retained and not self.r.exists(f"qtask:task:{owner_task_id}"):
                issues.append(
                    {
                        "type": "orphan_dedup_owner",
                        "owner_key": str(owner_key),
                        "task_id": str(owner_task_id),
                    }
                )

        task_ids = self.r.zrevrange(self.history.idx_key, 0, limit - 1)
        now = self._redis_now()
        repaired = 0
        for task_id_value in task_ids:
            task_id = str(task_id_value)
            record = self.history.get(task_id)
            if record is None:
                issues.append({"type": "stale_history_index", "task_id": task_id})
                if repair_safe:
                    repaired += int(self.r.zrem(self.history.idx_key, task_id) or 0)
                continue
            outcome = str(record.get("outcome") or "")
            task_locations = locations.get(task_id, [])
            location_known = bool(task_locations) or message_scan_complete
            operational_flag = str(record.get("operational_message", "0")).lower() in {
                "1",
                "true",
            }
            if outcome in {"", "none"} and not task_locations and location_known:
                issues.append({"type": "live_task_without_message", "task_id": task_id})
            if outcome in {"completed", "skipped", "cancelled"} and task_locations:
                issues.append(
                    {
                        "type": "terminal_task_has_message",
                        "task_id": task_id,
                        "outcome": outcome,
                        "locations": task_locations,
                    }
                )
            if outcome == "failed" and any(location != "dlq" for location in task_locations):
                issues.append(
                    {
                        "type": "failed_task_outside_dlq",
                        "task_id": task_id,
                        "locations": task_locations,
                    }
                )
            actual_operational = bool(task_locations)
            if not location_known:
                unverified_tasks += 1
                if repair_safe and outcome in {"completed", "failed", "skipped", "cancelled"}:
                    # 修复不得把抽样未命中当作消息不存在，额外核验真实容器。
                    actual_operational = self.history._task_has_operational_message(task_id)
                    location_known = True
            if location_known and operational_flag != actual_operational:
                issues.append(
                    {
                        "type": "operational_flag_mismatch",
                        "task_id": task_id,
                        "recorded": operational_flag,
                        "actual": actual_operational,
                    }
                )
                if repair_safe and outcome in {"completed", "failed", "skipped", "cancelled"}:
                    self.r.hset(
                        f"qtask:task:{task_id}",
                        "operational_message",
                        "1" if actual_operational else "0",
                    )
                    repaired += 1

            if record.get("payload_kind") == "external" and operational_flag:
                descriptor = record.get("payload_descriptor")
                if isinstance(descriptor, dict):
                    retain_until = self._float_or_none(descriptor.get("retain_until"))
                    if (
                        isinstance(retain_until, float)
                        and retain_until > 0
                        and retain_until <= now + self.external_safety_margin
                    ):
                        issues.append(
                            {
                                "type": "external_retention_risk",
                                "task_id": task_id,
                                "payload_ref": record.get("payload_ref"),
                                "retain_until": retain_until,
                            }
                        )

        counts: dict[str, int] = {}
        for issue in issues:
            issue_type = str(issue["type"])
            counts[issue_type] = counts.get(issue_type, 0) + 1
        self.r.hset(self.metrics_key, "consistency.last_issue_count", len(issues))
        return {
            "queue": self.base,
            "checked_at": now,
            "scanned_messages": scanned_messages,
            "message_scan_complete": message_scan_complete,
            "unverified_tasks": unverified_tasks,
            "scanned_tasks": len(task_ids),
            "scanned_owners": owner_count,
            "issue_count": len(issues),
            "issue_counts": counts,
            "issues": issues,
            "repaired": repaired,
        }

    def dlq_oldest_age(self) -> float:
        oldest = self.r.lindex(self.dlq, -1)
        if not oldest:
            return 0.0
        try:
            header = self.codec.header(str(oldest))
            created_at = float(header.raw.get("created_at", self._redis_now()))
        except Exception:
            return 0.0
        return max(0.0, self._redis_now() - created_at)

    def clear(
        self,
        include_dlq: bool = True,
        *,
        identity_policy: IdentityPolicy | str = IdentityPolicy.KEEP,
        include_processing: bool = False,
        force_active: bool = False,
    ) -> dict[str, int]:
        """取消待执行消息；processing 需显式选择，活跃 Worker 默认受保护。"""
        self._assert_queue_namespace()
        policy = IdentityPolicy(identity_policy)
        if include_processing and not force_active:
            for key in self.processing_keys(include_legacy=False):
                worker_id = key.rsplit(":", 1)[-1]
                if self.r.llen(key) and self.r.exists(f"{self.base}:worker:{worker_id}"):
                    raise ValueError(f"active processing cannot be cleared: {worker_id}")
        removed = 0
        keys = [(self.queue, "list"), (self.retry, "list")]
        if include_processing:
            keys.extend((key, "list") for key in self.processing_keys())
        keys.append((self.delay, "zset"))
        if include_dlq:
            keys.append((self.dlq, "list"))

        for key, source_type in keys:
            messages = (
                list(self.r.zrange(key, 0, -1))
                if source_type == "zset"
                else list(self.r.lrange(key, 0, -1))
            )
            for raw_message in messages:
                removed += self._cancel_or_purge(
                    key,
                    source_type,
                    str(raw_message),
                    policy,
                    reason="queue cleared by administrator",
                    force_active=force_active,
                )
        return {"messages": removed, "identity_released": int(policy == IdentityPolicy.RELEASE)}

    def _cancel_or_purge(
        self,
        source_key: str,
        source_type: str,
        raw_message: str,
        policy: IdentityPolicy,
        *,
        reason: str,
        force_active: bool = False,
    ) -> int:
        task_id = self._message_task_id(raw_message)
        if not task_id:
            return int(
                self.r.zrem(source_key, raw_message)
                if source_type == "zset"
                else self.r.lrem(source_key, 1, raw_message)
            )
        record = self.history.get(task_id)
        if not record:
            return int(
                self.r.zrem(source_key, raw_message)
                if source_type == "zset"
                else self.r.lrem(source_key, 1, raw_message)
            )
        self.history.ensure_hash(task_id)
        logical_key = self._optional_str(record.get("logical_key"))
        if logical_key:
            self._migrate_identity(logical_key)
        concurrency_key = self._optional_str(record.get("concurrency_key"))
        lease_token = ""
        try:
            lease_token = self.codec.header(raw_message).lease_token or ""
        except Exception:
            pass
        worker_id = (
            self._worker_id_from_processing(source_key)
            if source_key.startswith(f"{self.base}:processing:")
            else None
        )
        heartbeat_key = (
            f"{self.base}:worker:{worker_id}"
            if worker_id
            else self._dummy_key("heartbeat", task_id)
        )
        response = self.r.eval(
            CANCEL_OR_PURGE_LUA,
            8,
            source_key,
            f"qtask:task:{task_id}",
            self._dedup_key(logical_key) if logical_key else self._dummy_key("owner", task_id),
            self._lease_key(concurrency_key) if concurrency_key else self._dummy_key("lease", task_id),
            self.metrics_key,
            self.storage_gc_key,
            heartbeat_key,
            self.deadline_key,
            source_type,
            raw_message,
            task_id,
            policy.value,
            "1" if logical_key else "0",
            self.ttl_seconds,
            "admin_clear",
            reason,
            lease_token,
            "1" if force_active else "0",
        )
        if str(response[0]) == "active_processing":
            raise ValueError(f"active processing cannot be deleted: {task_id}")
        return int(str(response[0]) == "removed")

    # ==================== Internal helpers ====================

    def _find_task_message(self, list_key: str, task_id: str) -> str | None:
        for raw_message in self.r.lrange(list_key, 0, -1):
            if self._message_task_id(raw_message) == task_id:
                return str(raw_message)
        return None

    @staticmethod
    def _message_task_id(raw_message: Any) -> str | None:
        try:
            data = json.loads(raw_message)
        except (json.JSONDecodeError, TypeError):
            return None
        if not isinstance(data, dict) or not data.get("task_id"):
            return None
        return str(data["task_id"])

    @staticmethod
    def _reset_retry_message(raw_message: str) -> str:
        """仅用于 V1 DLQ：移除 payload._retry。"""
        try:
            data = json.loads(raw_message)
            if not isinstance(data, dict):
                return raw_message
            payload_raw = data.get("payload", {})
            payload = json.loads(payload_raw) if isinstance(payload_raw, str) else payload_raw
            if not isinstance(payload, dict):
                return raw_message
            payload.pop("_retry", None)
            return SmartQueue._build_envelope(
                str(data["task_id"]),
                payload,
                SmartQueue._float_or_none(data.get("expires_at")),
            )
        except Exception:
            return raw_message

    @staticmethod
    def _epoch_string(value: float | None) -> str:
        return "" if value is None else str(value)

    @staticmethod
    def _json_scalar(value: int | str | None) -> str:
        return "" if value is None else orjson.dumps(value).decode("utf-8")

    @staticmethod
    def _optional_str(value: Any) -> str | None:
        return None if value in (None, "") else str(value)

    @staticmethod
    def _float_or_none(value: Any) -> float | None:
        try:
            return None if value in (None, "") else float(value)
        except (TypeError, ValueError):
            return None
