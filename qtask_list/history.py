"""任务审计记录。

V2 历史只保存身份、执行元数据与不可变 outcome；live location 以 Redis 容器为准。
live 任务和仍在 DLQ 的失败任务不设置 TTL，避免 operational message 失去审计记录。
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, cast

import redis
from loguru import logger

from .clock import Clock, SystemClock

TERMINAL_OUTCOMES = {"completed", "failed", "skipped", "cancelled"}

# 类型和内容必须在同一原子读取中获取，兼容在线 String→Hash 转换。
READ_TASK_RECORD_LUA = r"""
local kind = redis.call('TYPE', KEYS[1])['ok']
if kind == 'hash' then return {kind, redis.call('HGETALL', KEYS[1])} end
if kind == 'string' then return {kind, redis.call('GET', KEYS[1])} end
return {kind, false}
"""
_TEXT_FIELDS = {
    "task_id", "_queue", "action", "logical_key", "trace_id", "parent_task_id", "replay_of",
    "concurrency_key", "supersede_key", "supersede_value", "reason", "reason_code",
    "last_error", "last_error_code", "payload_ref", "payload_checksum", "payload_kind",
    "payload_storage_id", "history_mode", "status", "outcome", "location", "delay_reason",
    "replayed_by", "duplicate_override_of",
    "claim_token",
}


def decode_history_field(field: str, value: Any) -> Any:
    """文本身份不按 JSON 猜类型，结构字段和数值字段继续反序列化。"""
    if field in _TEXT_FIELDS:
        return value
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return value


def decode_task_record(task_id: str, snapshot: Any) -> dict[str, Any] | None:
    """解析原子读取结果，保持旧 String 和新 Hash 的公共返回形态一致。"""
    kind, raw = snapshot
    if kind == "none":
        return None
    if kind == "hash":
        record = {field: decode_history_field(field, value)
                  for field, value in zip(raw[::2], raw[1::2])}
        if not record:
            return None
    elif kind == "string":
        try:
            parsed = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            parsed = raw
        record = parsed if isinstance(parsed, dict) else {"_raw": parsed}
    else:
        raise ValueError(f"unsupported task record type: {kind}")
    record.setdefault("task_id", task_id)
    return cast(dict[str, Any], record)


def read_task_record(client: Any, task_id: str) -> dict[str, Any] | None:
    """以一次 Redis 调用读取完整历史，避免格式迁移期间类型失配。"""
    return decode_task_record(task_id, client.eval(READ_TASK_RECORD_LUA, 1, f"qtask:task:{task_id}"))


def _serialize_value(value: Any) -> Any:
    """历史字段序列化：None 转空串，容器转紧凑 JSON，其余原样。"""
    if value is None:
        return ""
    if isinstance(value, bool):
        return json.dumps(value)
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return value


def _serialize_field(field: str, value: Any) -> Any:
    """运行标记使用脚本约定的 0/1，其余布尔值保留 JSON 类型。"""
    if field == "operational_message" and isinstance(value, bool):
        return int(value)
    return _serialize_value(value)


class TaskHistory:
    """按队列维护任务 Hash 与创建时间索引。"""

    def __init__(
        self,
        redis_url: Optional[str] = None,
        queue_name: str = "",
        ttl_days: int = 15,
        redis_client: Optional[Any] = None,
        clock: Clock | None = None,
    ):
        self.r: Any
        if redis_client is not None:
            self.r = redis_client
        elif redis_url:
            self.r = redis.from_url(redis_url, decode_responses=True)
        else:
            raise ValueError("Either redis_url or redis_client must be provided")
        if not isinstance(ttl_days, int) or ttl_days < 0:
            raise ValueError("ttl_days must be an integer >= 0")
        self.queue_name = queue_name
        self.idx_key = f"qtask:hist:{queue_name}"
        self.task_key_prefix = "qtask:task:"
        self.ttl_seconds = ttl_days * 86400
        self.clock = clock or SystemClock()

    def ensure_hash(self, task_id: str) -> bool:
        """CAS 转换旧 String 历史，保留 TTL 和完整 JSON，避免覆盖并发更新。"""
        key = f"{self.task_key_prefix}{task_id}"
        for _ in range(8):
            kind, raw = self.r.eval(READ_TASK_RECORD_LUA, 1, key)
            if kind == "hash":
                return True
            if kind == "none":
                return False
            if kind != "string":
                raise ValueError(f"unsupported task record type: {kind}")
            if raw is None:
                continue
            parsed = json.loads(raw)
            if not isinstance(parsed, dict):
                raise ValueError("legacy task history must be a JSON object")
            parsed.setdefault("task_id", task_id)
            parsed.setdefault("_queue", self.queue_name)
            flat: list[Any] = []
            for field, value in parsed.items():
                flat.extend([field, _serialize_field(field, value)])
            script = r"""
            local kind = redis.call('TYPE', KEYS[1])['ok']
            if kind == 'hash' then return 1 end
            if kind ~= 'string' or redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
            local ttl = redis.call('PTTL', KEYS[1])
            redis.call('DEL', KEYS[1])
            redis.call('HSET', KEYS[1], unpack(ARGV, 2))
            if ttl >= 0 then redis.call('PEXPIRE', KEYS[1], math.max(ttl, 1)) end
            return 1
            """
            if self.r.eval(script, 1, key, raw, *flat):
                return True
        raise RuntimeError("legacy task record changed repeatedly during conversion")

    def record(self, task_id: str, data: dict[str, Any]) -> None:
        """记录新任务；新记录默认视为仍有 operational message。"""
        pipe = self.r.pipeline()
        self.record_pipeline(pipe, task_id, data)
        pipe.execute()

    def record_pipeline(self, pipe: Any, task_id: str, data: dict[str, Any]) -> None:
        """把任务记录加入调用方 pipeline；live 记录不设置 TTL。"""
        now = self.clock.now()
        record = dict(data)
        record.setdefault("task_id", task_id)
        record.setdefault("_queue", self.queue_name)
        record.setdefault("created_at", now)
        record.setdefault("updated_at", now)
        record.setdefault("outcome", "")
        record.setdefault("status", "pending")
        record.setdefault("operational_message", 1)
        task_key = f"{self.task_key_prefix}{task_id}"
        pipe.hset(task_key, mapping={key: _serialize_field(key, value) for key, value in record.items()})
        pipe.persist(task_key)
        pipe.zadd(self.idx_key, {task_id: float(record["created_at"])}, nx=True)

    def update(self, task_id: str, fields: dict[str, Any]) -> bool:
        """更新运行元数据；已经写入的 terminal outcome 不允许重新打开或改写。"""
        key = f"{self.task_key_prefix}{task_id}"
        if not self.ensure_hash(task_id):
            return False
        now = self.clock.now()
        mapping = {name: _serialize_field(name, value) for name, value in fields.items()}
        requested_outcome = str(mapping.get("outcome", ""))
        requested_status = str(mapping.get("status", ""))
        if not requested_outcome and requested_status in TERMINAL_OUTCOMES:
            requested_outcome = requested_status
            mapping["outcome"] = requested_outcome
        mapping["updated_at"] = now
        if requested_outcome in TERMINAL_OUTCOMES:
            mapping.setdefault("status", requested_outcome)
            mapping.setdefault("finished_at", now)

        flat: list[Any] = []
        for field, value in mapping.items():
            flat.extend([field, value])
        # 位置 6 同时被脚本作为 ttl；created score 单独从记录读取，避免更新排序。
        created_at = self.r.hget(key, "created_at") or now
        # 使用独立脚本版本，避免动态 argv 下标容易被调用方字段数影响。
        safe_script = r"""
        if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
        local current = redis.call('HGET', KEYS[1], 'outcome') or ''
        local requested = ARGV[1]
        local requested_status = ARGV[2]
        if current ~= '' and current ~= 'none' then
            if requested ~= '' and requested ~= current then return -1 end
            if requested == '' and requested_status ~= '' and requested_status ~= current then return -1 end
        end
        if #ARGV > 6 then redis.call('HSET', KEYS[1], unpack(ARGV, 7)) end
        redis.call('ZADD', KEYS[2], 'NX', tonumber(ARGV[3]), ARGV[4])
        local outcome = redis.call('HGET', KEYS[1], 'outcome') or ''
        local operational = redis.call('HGET', KEYS[1], 'operational_message') or '0'
        if outcome ~= '' and outcome ~= 'none' and operational == '0' then
            redis.call('EXPIRE', KEYS[1], tonumber(ARGV[5]))
        else
            redis.call('PERSIST', KEYS[1])
        end
        return 1
        """
        result = self.r.eval(
            safe_script,
            2,
            key,
            self.idx_key,
            requested_outcome,
            requested_status,
            created_at,
            task_id,
            max(self.ttl_seconds, 1),
            now,
            *flat,
        )
        return int(result or 0) == 1

    def get(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取任务详情，兼容旧版 String 记录。"""
        return read_task_record(self.r, task_id)

    def list(self, limit: int = 50) -> List[Dict[str, Any]]:
        """按创建时间倒序列出任务历史。"""
        task_ids = self.r.zrevrange(self.idx_key, 0, max(limit - 1, -1))
        return [record for task_id in task_ids if (record := self.get(str(task_id))) is not None]

    def clear(self) -> None:
        """彻底清空该队列历史；仅供已经显式确认的 destructive 管理操作。"""
        batch_size = 1000
        while True:
            task_ids = self.r.zrange(self.idx_key, 0, batch_size - 1)
            if not task_ids:
                self.r.delete(self.idx_key)
                break
            pipe = self.r.pipeline()
            for task_id in task_ids:
                pipe.delete(f"{self.task_key_prefix}{task_id}")
            pipe.zrem(self.idx_key, *task_ids)
            pipe.execute()

    def clear_terminal(self) -> int:
        """仅删除无 operational message 的终态记录，保留并发新投递的审计。"""
        script = r"""
        local kind = redis.call('TYPE', KEYS[1])
        if type(kind) == 'table' then kind = kind['ok'] end
        if kind == 'none' then return redis.call('ZREM', KEYS[2], ARGV[1]) end
        if kind == 'string' then
            local ok, record = pcall(cjson.decode, redis.call('GET', KEYS[1]))
            if ok and type(record) == 'table' then
                local status = record['outcome'] or ''
                if status == '' then status = record['status'] or '' end
                if status == 'completed' or status == 'failed' or
                   status == 'skipped' or status == 'cancelled' then
                    redis.call('DEL', KEYS[1])
                    return redis.call('ZREM', KEYS[2], ARGV[1])
                end
            end
            return 0
        end
        if kind ~= 'hash' then return 0 end
        local outcome = redis.call('HGET', KEYS[1], 'outcome') or ''
        if outcome == '' then outcome = redis.call('HGET', KEYS[1], 'status') or '' end
        local operational = redis.call('HGET', KEYS[1], 'operational_message') or '0'
        if (outcome == 'completed' or outcome == 'failed' or
            outcome == 'skipped' or outcome == 'cancelled') and operational == '0' then
            redis.call('DEL', KEYS[1])
            return redis.call('ZREM', KEYS[2], ARGV[1])
        end
        return 0
        """
        removed = 0
        offset = 0
        while True:
            task_ids = self.r.zrange(self.idx_key, offset, offset + 499)
            if not task_ids:
                break
            batch_removed = 0
            for task_id in task_ids:
                # 旧记录可能没有 operational 标记，转换前用真实容器确认。
                if self.r.type(f"{self.task_key_prefix}{task_id}") == "string":
                    if self._task_has_operational_message(str(task_id)):
                        self.ensure_hash(str(task_id))
                        self.r.hset(f"{self.task_key_prefix}{task_id}", "operational_message", "1")
                        continue
                    self.ensure_hash(str(task_id))
                batch_removed += int(self.r.eval(
                    script, 2, f"{self.task_key_prefix}{task_id}", self.idx_key, task_id
                ) or 0)
            removed += batch_removed
            offset += len(task_ids) - batch_removed
        return removed

    def clean_expired(self, ttl_seconds: Optional[int] = None) -> int:
        """只清理终态且不再有 operational message 的到期记录。"""
        retention = self.ttl_seconds if ttl_seconds is None else max(int(ttl_seconds), 0)
        cutoff = self.clock.now() - retention
        total_cleaned = 0
        offset = 0
        batch_size = 500

        while True:
            # 索引按 created_at 排序；长跑 live 任务会被安全跳过。
            task_ids = self.r.zrangebyscore(
                self.idx_key,
                "-inf",
                cutoff,
                start=offset,
                num=batch_size,
            )
            if not task_ids:
                break
            removed_this_batch = 0
            for raw_task_id in task_ids:
                task_id = str(raw_task_id)
                record = self.get(task_id)
                if record is None:
                    removed_this_batch += int(self.r.zrem(self.idx_key, task_id) or 0)
                    continue
                outcome = str(record.get("outcome") or record.get("status") or "")
                if outcome not in TERMINAL_OUTCOMES:
                    continue
                if self._task_has_operational_message(task_id):
                    # 修复旧记录缺失/错误的 operational 标记，但不删除。
                    self.ensure_hash(task_id)
                    self.r.hset(f"{self.task_key_prefix}{task_id}", "operational_message", "1")
                    continue
                finished_at = self._float_or_none(
                    record.get("finished_at") or record.get("updated_at") or record.get("created_at")
                )
                if finished_at is None or finished_at > cutoff:
                    continue
                removed = self._delete_if_still_archivable(task_id, cutoff)
                removed_this_batch += removed
                total_cleaned += removed

            if removed_this_batch:
                offset = 0
            else:
                offset += len(task_ids)
            if len(task_ids) < batch_size:
                break

        if total_cleaned:
            logger.info(f"[History] Cleaned {total_cleaned} terminal records for {self.queue_name}")
        return total_cleaned

    def _delete_if_still_archivable(self, task_id: str, cutoff: float) -> int:
        """再次在 Redis 内检查终态与 operational 标记，避免 moved=0 后误删。"""
        self.ensure_hash(task_id)
        script = r"""
        if redis.call('EXISTS', KEYS[1]) == 0 then
            return redis.call('ZREM', KEYS[2], ARGV[1])
        end
        local outcome = redis.call('HGET', KEYS[1], 'outcome') or ''
        if outcome == '' then outcome = redis.call('HGET', KEYS[1], 'status') or '' end
        if outcome ~= 'completed' and outcome ~= 'failed' and outcome ~= 'skipped' and outcome ~= 'cancelled' then
            return 0
        end
        local operational = redis.call('HGET', KEYS[1], 'operational_message') or '0'
        if operational ~= '0' then return 0 end
        local finished = tonumber(redis.call('HGET', KEYS[1], 'finished_at') or redis.call('HGET', KEYS[1], 'updated_at') or redis.call('HGET', KEYS[1], 'created_at') or '0')
        if finished > tonumber(ARGV[2]) then return 0 end
        redis.call('DEL', KEYS[1])
        redis.call('ZREM', KEYS[2], ARGV[1])
        return 1
        """
        return int(
            self.r.eval(
                script,
                2,
                f"{self.task_key_prefix}{task_id}",
                self.idx_key,
                task_id,
                cutoff,
            )
            or 0
        )

    def _task_has_operational_message(self, task_id: str) -> bool:
        """兼容旧记录：从容器确认任务是否仍可执行/重放。"""
        list_keys = [
            self.queue_name,
            f"{self.queue_name}:retry",
            f"{self.queue_name}:dlq",
            f"{self.queue_name}:processing",
            *self.r.scan_iter(f"{self.queue_name}:processing:*"),
        ]
        for key in list_keys:
            for raw in self.r.lrange(key, 0, -1):
                if self._message_task_id(raw) == task_id:
                    return True
        for raw, _score in self.r.zscan_iter(f"{self.queue_name}:delay"):
            if self._message_task_id(raw) == task_id:
                return True
        return False

    @staticmethod
    def _message_task_id(raw: Any) -> str | None:
        try:
            value = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return None
        if not isinstance(value, dict) or not value.get("task_id"):
            return None
        return str(value["task_id"])

    @staticmethod
    def _decode(value: Any) -> Any:
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value

    @staticmethod
    def _float_or_none(value: Any) -> float | None:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
