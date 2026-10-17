import base64
import binascii
import hashlib
import json
import math
import os
import time
import zlib
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, cast

import redis
import zstandard

from .clock import Clock, SystemClock
from .errors import RemoteStorageError
from .history import READ_HISTORY_PAGE_LUA, READ_TASK_RECORD_LUA, decode_task_record, read_task_record
from .models import (
    DuplicateAction,
    EnqueueResult,
    HistoryMode,
    IdentityPolicy,
    JsonValue,
    TaskSpec,
)
from .queue import SmartQueue
from .state import DELETE_QUEUE_LUA, PRUNE_WORKER_LUA
from .storage import RemoteStorage


class QueueState(str, Enum):
    """用户可见的任务状态。"""

    ready = "ready"
    processing = "processing"
    retry = "retry"
    retry_wait = "retry_wait"
    dlq = "dlq"
    delay = "delay"
    history = "history"
    completed = "completed"
    failed = "failed"
    skipped = "skipped"
    cancelled = "cancelled"
    deadline_missed = "deadline_missed"
    expired = "expired"
    all = "all"


class QueueAdmin:
    """面向 Dashboard、CLI 和 Agent 的队列管理接口。"""

    def __init__(
        self,
        redis_url: Optional[str] = None,
        redis_client: Optional[Any] = None,
        storage: Optional[RemoteStorage] = None,
        *,
        max_attempts: int = 3,
        retry_backoff_base: float = 30,
        retry_backoff_max: float = 3600,
        ttl_days: int = 15,
        history_mode: HistoryMode | str = HistoryMode.FULL,
        clock: Clock | None = None,
    ):
        self.redis_url = redis_url or os.environ.get("REDIS_URL", "redis://localhost:6379/0")
        self.r: Any = redis_client or redis.from_url(self.redis_url, decode_responses=True)
        self.storage = storage
        self.max_attempts = max_attempts
        self.retry_backoff_base = retry_backoff_base
        self.retry_backoff_max = retry_backoff_max
        self.ttl_days = ttl_days
        self.history_mode = HistoryMode(history_mode)
        self.clock = clock or SystemClock()

    # ==================== Queue Discovery ====================

    def list_queues(self) -> List[Dict[str, Any]]:
        workers = self.list_workers()
        by_queue: Dict[str, List[Dict[str, Any]]] = {}
        for worker in workers:
            by_queue.setdefault(worker["queue"], []).append(worker)
        return [
            {"name": queue, **self.queue_stats(queue, workers=by_queue.get(queue, []))}
            for queue in self.queue_names()
        ]

    def queue_names(self) -> List[str]:
        """实时读取已登记的逻辑队列；不根据状态键或历史推测队列存在。"""
        return sorted(self.r.zrange("qtask:queues", 0, -1))

    def queue_stats(
        self, queue_name: str, *, workers: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, int]:
        workers = self.list_workers(queue_name) if workers is None else workers
        processing_keys = {worker["processing_key"] for worker in workers}
        processing_keys.update(
            str(key) for key in self.r.zrange(f"qtask:processing:{queue_name}", 0, -1)
        )
        processing_keys.add(f"{queue_name}:processing")
        metric_values = self.r.hmget(
            f"qtask:metrics:{queue_name}",
            "outcome.completed",
            "outcome.failed",
            "outcome.skipped",
            "outcome.cancelled",
            "retry_wait.current",
            "observation.version",
        )
        completed, failed, skipped, cancelled, retry_wait, observation_version = [
            int(value or 0) for value in metric_values
        ]
        expired_count = int(self.r.zcount(f"qtask:deadline:{queue_name}", "-inf", self.clock.now()))
        return {
            "queue": int(self.r.llen(queue_name)),
            "processing": sum(int(self.r.llen(key)) for key in processing_keys),
            "retry": int(self.r.llen(f"{queue_name}:retry")),
            "retry_wait": retry_wait,
            "dlq": int(self.r.llen(f"{queue_name}:dlq")),
            "delay": int(self.r.zcard(f"{queue_name}:delay")),
            "history": int(self.r.zcard(f"qtask:hist:{queue_name}")),
            "completed": completed,
            "failed": failed,
            "skipped": skipped,
            "cancelled": cancelled,
            "completed_total": completed,
            "failed_total": failed,
            "skipped_total": skipped,
            "cancelled_total": cancelled,
            "deadline_missed": expired_count,
            "expired": expired_count,  # 一个小版本的兼容别名
            "active_workers": sum(1 for worker in workers if worker["active"]),
            "stale_workers": sum(1 for worker in workers if not worker["active"]),
            "observation_indexed": int(observation_version >= 2),
        }

    def rebuild_observation_indexes(self, queue_name: str) -> Dict[str, int]:
        """维护窗口内为旧队列回填总览索引；调用方应暂停投递与消费。"""
        if any(self.r.scan_iter(f"{queue_name}:worker:*")):
            raise ValueError("stop active workers before rebuilding observation indexes")
        outcome_counts = {name: 0 for name in ("completed", "failed", "skipped", "cancelled")}
        deadlines: Dict[str, float] = {}
        processing_ids: set[str] = set()
        for key in self.processing_keys(queue_name):
            for raw_message in self.r.lrange(key, 0, -1):
                task_id = self._message_task_id(raw_message)
                if task_id:
                    processing_ids.add(task_id)
        hist_key = f"qtask:hist:{queue_name}"
        cursor = 0
        scanned = 0
        while True:
            cursor, pairs = self.r.zscan(hist_key, cursor=cursor, count=500)
            task_ids = [str(task_id) for task_id, _score in pairs]
            for record in self._read_history_records(task_ids):
                scanned += 1
                outcome = str(record.get("outcome") or record.get("status") or "")
                if outcome in outcome_counts:
                    outcome_counts[outcome] += 1
                    continue
                if str(record.get("task_id") or "") in processing_ids or str(
                    record.get("location") or record.get("status") or ""
                ) == "processing":
                    continue
                deadline = self._float_or_none(record.get("start_deadline_at") or record.get("expires_at"))
                task_id = str(record.get("task_id") or "")
                if deadline is not None and task_id:
                    deadlines[task_id] = deadline
            if cursor == 0:
                break
        retry_wait = 0
        for raw_message, _score in self.r.zscan_iter(f"{queue_name}:delay"):
            try:
                envelope = json.loads(raw_message)
            except (ValueError, TypeError):
                continue
            if isinstance(envelope, dict) and envelope.get("delay_reason") == "retry":
                retry_wait += 1
        metric_key = f"qtask:metrics:{queue_name}"
        metric_names = [f"outcome.{name}" for name in outcome_counts]
        existing = self.r.hmget(metric_key, *metric_names)
        totals = {
            name: max(int(raw or 0), outcome_counts[name])
            for name, raw in zip(outcome_counts, existing)
        }
        deadline_key = f"qtask:deadline:{queue_name}"
        pipe = self.r.pipeline(transaction=True)
        pipe.delete(deadline_key)
        deadline_items = list(deadlines.items())
        for start in range(0, len(deadline_items), 500):
            pipe.zadd(deadline_key, dict(deadline_items[start : start + 500]))
        pipe.hset(metric_key, mapping={
            **{f"outcome.{name}": total for name, total in totals.items()},
            "retry_wait.current": retry_wait,
            "observation.version": 2,
        })
        pipe.zadd("qtask:queues", {queue_name: self.clock.now()})
        pipe.execute()
        return {"scanned": scanned, "deadlines": len(deadlines), "retry_wait": retry_wait}

    def processing_keys(self, queue_name: str, include_legacy: bool = True) -> List[str]:
        keys = {
            str(key) for key in self.r.zrange(f"qtask:processing:{queue_name}", 0, -1)
        }
        keys.update(
            f"{queue_name}:processing:{worker_id}"
            for worker_id in self.r.zrange(f"qtask:workers:{queue_name}", 0, -1)
        )
        if include_legacy:
            keys.add(f"{queue_name}:processing")
        return sorted(keys)

    def list_workers(self, queue_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """按注册表读取 Worker，移除既无心跳也无待恢复任务的失效注册项。"""
        names = [queue_name] if queue_name else self.queue_names()
        identifiers: set[tuple[str, str]] = set()
        for queue in names:
            for worker_id in self.r.zrange(f"qtask:workers:{queue}", 0, -1):
                identifiers.add((queue, str(worker_id)))

        workers: List[Dict[str, Any]] = []
        for queue, worker_id in sorted(identifiers):
            heartbeat_key = f"{queue}:worker:{worker_id}"
            processing_key = f"{queue}:processing:{worker_id}"
            raw_seen = self.r.get(heartbeat_key)
            processing = int(self.r.llen(processing_key))
            if raw_seen is None and processing == 0:
                self.r.eval(
                    PRUNE_WORKER_LUA, 3,
                    heartbeat_key, processing_key, f"qtask:workers:{queue}", worker_id,
                )
                continue
            workers.append({
                "queue": queue,
                "worker_id": worker_id,
                "active": raw_seen is not None,
                "heartbeat_key": heartbeat_key,
                "ttl": int(self.r.ttl(heartbeat_key)) if raw_seen is not None else -2,
                "last_seen": self._float_or_none(raw_seen),
                "processing_key": processing_key,
                "processing": processing,
            })
        return workers

    # ==================== Task Reading ====================

    def search_tasks_page(
        self,
        queues: Sequence[str] | None,
        *,
        state: QueueState | str = QueueState.all,
        action: Optional[str] = None,
        search: Optional[str] = None,
        created_after: Optional[float] = None,
        created_before: Optional[float] = None,
        completed_after: Optional[float] = None,
        completed_before: Optional[float] = None,
        limit: int = 50,
        cursor: Optional[str] = None,
        scan_limit: int = 5000,
    ) -> Dict[str, Any]:
        """按历史创建时间游标扫描；queues=None 固定查询开始时的全部队列。

        第一页的 exact_task_id 确认去除首尾空白后 search 对应的任务记录，
        用于独立于列表筛选的精确定位；支持旧字符串 ID，未命中或分页时为 None。
        """
        if limit < 1 or limit > 500 or scan_limit < limit:
            raise ValueError("invalid search limits")
        selected = QueueState(state)
        mode = "all" if queues is None else "explicit"
        container_states = {QueueState.ready, QueueState.processing, QueueState.retry,
                            QueueState.dlq, QueueState.delay}
        raw_position = None
        history_done = False
        raw_done = selected not in container_states
        if cursor:
            try:
                if len(cursor) > 32768:
                    raise ValueError("cursor is too long")
                encoded = cursor[2:] if cursor.startswith("z.") else cursor
                decoded = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
                if cursor.startswith("z."):
                    decompressor = zlib.decompressobj()
                    decoded = decompressor.decompress(decoded, 65537)
                    if len(decoded) > 65536 or not decompressor.eof or decompressor.unused_data:
                        raise ValueError("invalid compressed cursor")
                marker = json.loads(decoded)
                if not isinstance(marker, dict) or marker.get("version") != 2:
                    raise ValueError("invalid cursor structure")
                snapshot = marker.get("names")
                if (
                    not isinstance(snapshot, list)
                    or any(not isinstance(name, str) or not name for name in snapshot)
                    or snapshot != sorted(set(snapshot))
                    or marker.get("mode") != mode
                ):
                    raise ValueError("invalid cursor queues")
                names = snapshot
                if queues is not None and names != sorted(set(queues)):
                    raise ValueError("cursor queues changed")
                fingerprint = hashlib.sha256(json.dumps(
                    [names, selected.value, action, search, created_after, created_before,
                     completed_after, completed_before], ensure_ascii=False
                ).encode()).hexdigest()
                if marker["fingerprint"] != fingerprint:
                    raise ValueError("cursor filters changed")
                anchors = marker["anchors"]
                if not isinstance(anchors, list) or len(anchors) != len(names):
                    raise ValueError("invalid cursor anchors")
                for anchor in anchors:
                    if anchor is not None and (
                        not isinstance(anchor, list) or len(anchor) != 2
                        or not isinstance(anchor[0], (int, float)) or not math.isfinite(anchor[0])
                        or not isinstance(anchor[1], str)
                    ):
                        raise ValueError("invalid cursor anchor")
                cutoff = float(marker["cutoff"])
                if not math.isfinite(cutoff):
                    raise ValueError("invalid cursor timestamp")
                history_done = marker.get("history_done") is True
                raw_done = marker.get("raw_done") is True or selected not in container_states
                raw_position = marker.get("raw")
                if raw_position is not None and (
                    not isinstance(raw_position, dict)
                    or type(raw_position.get("queue")) is not int
                    or not 0 <= raw_position["queue"] < len(names)
                    or not isinstance(raw_position.get("key"), str)
                    or type(raw_position.get("offset")) is not int
                    or not 0 <= raw_position["offset"] <= 1_000_000_000
                    or not isinstance(raw_position.get("stamp", ""), str)
                ):
                    raise ValueError("invalid container cursor")
            except (KeyError, TypeError, ValueError, OverflowError, binascii.Error, zlib.error) as exc:
                raise ValueError("invalid search cursor; 请刷新第一页后重试") from exc
        else:
            names = sorted(set(self.queue_names() if queues is None else queues))
            fingerprint = hashlib.sha256(json.dumps(
                [names, selected.value, action, search, created_after, created_before,
                 completed_after, completed_before], ensure_ascii=False
            ).encode()).hexdigest()
            anchors = [None] * len(names)
            cutoff = self.clock.now()

        buffers: list[list[tuple[str, float, Optional[Dict[str, Any]]]]] = [[] for _ in names]
        positions = [0] * len(names)
        exhausted = [history_done] * len(names)
        fetch_sizes = [1] * len(names)

        def fill(index: int) -> None:
            if positions[index] < len(buffers[index]) or exhausted[index]:
                return
            anchor = anchors[index]
            snapshot = self.r.eval(
                READ_HISTORY_PAGE_LUA, 1, f"qtask:hist:{names[index]}", cutoff,
                anchor[0] if anchor is not None else "", anchor[1] if anchor is not None else "",
                fetch_sizes[index],
            )
            ids_scores = list(zip(snapshot[::2], snapshot[1::2]))
            fetch_sizes[index] = 50
            task_ids = [str(task_id) for task_id, _score in ids_scores]
            records = {str(row.get("task_id")): row for row in self._read_history_records(task_ids)}
            buffers[index] = [
                (task_id, float(score), records.get(task_id))
                for task_id, score in ids_scores
            ]
            positions[index] = 0
            exhausted[index] = not bool(ids_scores)

        rows: list[Dict[str, Any]] = []
        scanned = 0
        needle = search.lower() if search else None
        for index in range(len(names)):
            fill(index)
        while len(rows) < limit and scanned < scan_limit:
            available = [index for index in range(len(names)) if positions[index] < len(buffers[index])]
            if not available:
                break
            index = max(available, key=lambda item: (
                buffers[item][positions[item]][1], buffers[item][positions[item]][0]
            ))
            task_id, _score, record = buffers[index][positions[index]]
            positions[index] += 1
            anchors[index] = [_score, task_id]
            scanned += 1
            if positions[index] == len(buffers[index]):
                fill(index)
            if not record:
                continue
            if selected in container_states and not record.get("location"):
                # 缺少位置的旧记录由真实容器补充，不能按 pending 猜成 ready。
                continue
            outcome = str(record.get("outcome") or record.get("status") or "")
            location = str(record.get("location") or "")
            if not location:
                location = "history" if outcome in {"completed", "failed", "skipped", "cancelled"} else "ready"
            row_state = outcome if outcome in {"completed", "failed", "skipped", "cancelled"} else location
            if selected in {QueueState.expired, QueueState.deadline_missed}:
                deadline = self._float_or_none(record.get("start_deadline_at") or record.get("expires_at"))
                if deadline is None or deadline >= self.clock.now() or row_state in {
                    "completed", "failed", "skipped", "cancelled", "processing"
                }:
                    continue
            elif selected == QueueState.retry_wait:
                if location != "delay" or record.get("delay_reason") != "retry":
                    continue
            elif selected != QueueState.all and selected != QueueState.history:
                if selected.value != row_state and selected.value != location:
                    continue
            if action and record.get("action") != action:
                continue
            if not self._matches_time_filters(
                record, created_after, created_before, completed_after, completed_before
            ):
                continue
            record["_queue"] = names[index]
            record["_state"] = (
                QueueState.deadline_missed.value
                if selected in {QueueState.expired, QueueState.deadline_missed}
                else QueueState.retry_wait.value if selected == QueueState.retry_wait else row_state
            )
            record["_source"] = f"qtask:hist:{names[index]}"
            if needle and needle not in json.dumps(record, ensure_ascii=False, default=str).lower():
                continue
            rows.append(record)

        history_done = not any(positions[index] < len(buffers[index]) or not exhausted[index]
                               for index in range(len(names)))
        if history_done and not raw_done and len(rows) < limit and scanned < scan_limit:
            extra, raw_position, inspected = self._unindexed_tasks_page(
                names, selected, limit - len(rows), scan_limit - scanned, raw_position,
                action=action, search=search, cutoff=cutoff,
                created_after=created_after, created_before=created_before,
                completed_after=completed_after, completed_before=completed_before,
            )
            rows.extend(extra)
            scanned += inspected
            raw_done = raw_position is None
        if not names:
            raw_done = True
        has_more = not history_done or not raw_done
        next_cursor = None
        if has_more:
            payload = {
                "fingerprint": fingerprint, "names": names, "mode": mode,
                "version": 2, "anchors": anchors, "cutoff": cutoff,
                "history_done": history_done, "raw_done": raw_done, "raw": raw_position,
            }
            next_cursor = "z." + base64.urlsafe_b64encode(
                zlib.compress(json.dumps(payload, separators=(",", ":")).encode())
            ).decode().rstrip("=")
        # 与列表筛选独立的精确 ID 定位，兼容旧字符串 ID；分页无需重复定位。
        exact_id = search.strip() if search and not cursor else ""
        exact_record = self.get_task(exact_id) if exact_id else None
        return {
            "tasks": rows,
            "exact_task_id": exact_id if exact_record and exact_record.get("task_id") == exact_id else None,
            "count": len(rows),
            "next_cursor": next_cursor,
            "has_more": has_more,
            "scan_limited": scanned >= scan_limit and has_more,
            "scanned": scanned,
        }

    def _unindexed_tasks_page(
        self, names: list[str], state: QueueState, limit: int, budget: int,
        position: dict[str, Any] | None, *, action: str | None, search: str | None,
        cutoff: float, created_after: float | None, created_before: float | None,
        completed_after: float | None, completed_before: float | None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any] | None, int]:
        """补充没有可用审计索引的消息；原始容器变动时拒绝无声错页。"""
        rows: list[dict[str, Any]] = []
        scanned = 0
        first_queue = position["queue"] if position else 0
        for queue_index in range(first_queue, len(names)):
            name = names[queue_index]
            keys = self._state_keys(name, state)
            resume = position if queue_index == first_queue else None
            first_key = 0
            if resume and resume["key"]:
                if resume["key"] not in keys:
                    raise ValueError("队列内容已变化，请刷新第一页")
                first_key = keys.index(resume["key"])
            for key in keys[first_key:]:
                offset = resume["offset"] if resume and resume["key"] == key else 0
                is_delay = state == QueueState.delay
                pipe = self.r.pipeline(transaction=True)
                if is_delay:
                    pipe.zcard(key)
                    pipe.zrange(key, 0, 0, withscores=True)
                    pipe.zrange(key, -1, -1, withscores=True)
                else:
                    pipe.llen(key)
                    pipe.lindex(key, 0)
                    pipe.lindex(key, -1)
                summary = pipe.execute()
                total = int(summary[0])
                stamp = hashlib.sha256(json.dumps(summary, ensure_ascii=False).encode()).hexdigest()
                if resume and resume["key"] == key and resume.get("stamp") != stamp:
                    raise ValueError("队列内容已变化，请刷新第一页")
                resume = None
                while offset < total:
                    next_position = {"queue": queue_index, "key": key, "offset": offset, "stamp": stamp}
                    if len(rows) >= limit or scanned >= budget:
                        return rows, next_position, scanned
                    take = min(50, budget - scanned)
                    if is_delay:
                        messages = self.r.zrange(key, offset, offset + take - 1, withscores=True)
                    else:
                        messages = [(raw, None) for raw in reversed(
                            self.r.lrange(key, -offset - take, -offset - 1)
                        )]
                    if not messages:
                        break
                    decoded = [self._decode_message(raw, name, state.value, key) for raw, _ in messages]
                    pipe = self.r.pipeline(transaction=True)
                    for row in decoded:
                        task_id = str(row.get("task_id") or "")
                        pipe.zscore(f"qtask:hist:{name}", task_id)
                    membership = pipe.execute()
                    records = self._supplement_task_records(decoded)
                    for index, (row, (_raw, score)) in enumerate(zip(decoded, messages)):
                        if len(rows) >= limit or scanned >= budget:
                            return rows, {**next_position, "offset": offset}, scanned
                        offset += 1
                        scanned += 1
                        if membership[index] is not None and records.get(str(row.get("task_id")), {}).get("location"):
                            continue
                        if score is not None:
                            row["run_at"] = score
                        created_at = self._float_or_none(row.get("created_at"))
                        if created_at is not None and created_at > cutoff:
                            continue
                        if action and row.get("action") != action:
                            continue
                        if not self._matches_time_filters(row, created_after, created_before,
                                                         completed_after, completed_before):
                            continue
                        if search and search.lower() not in json.dumps(row, ensure_ascii=False, default=str).lower():
                            continue
                        rows.append(row)
        return rows, None, scanned

    def list_tasks(
        self,
        queue_name: str,
        state: QueueState | str = QueueState.all,
        limit: int = 50,
        search: Optional[str] = None,
        created_after: Optional[float] = None,
        created_before: Optional[float] = None,
        completed_after: Optional[float] = None,
        completed_before: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        if limit <= 0:
            return []
        selected_state = QueueState(state)
        indexed_states = {
            QueueState.all, QueueState.history, QueueState.completed, QueueState.failed,
            QueueState.skipped, QueueState.cancelled, QueueState.retry_wait,
            QueueState.deadline_missed, QueueState.expired,
        }
        has_time_filter = any(value is not None for value in (
            created_after, created_before, completed_after, completed_before
        ))
        if selected_state in indexed_states or search or has_time_filter:
            rows: List[Dict[str, Any]] = []
            cursor: Optional[str] = None
            scanned = 0
            scan_budget = 5000
            while len(rows) < limit:
                remaining = scan_budget - scanned
                if remaining <= 0:
                    raise ValueError("list_tasks 扫描已达 5000 条；请使用 search_tasks_page 继续翻页")
                page = self.search_tasks_page(
                    [queue_name], state=selected_state, search=search,
                    created_after=created_after, created_before=created_before,
                    completed_after=completed_after, completed_before=completed_before,
                    limit=min(limit - len(rows), 500, remaining), cursor=cursor,
                    scan_limit=remaining,
                )
                rows.extend(page["tasks"])
                scanned += page["scanned"]
                cursor = page["next_cursor"]
                if not cursor:
                    break
                if scanned >= scan_budget or page["scanned"] == 0:
                    raise ValueError("list_tasks 扫描已达 5000 条；请使用 search_tasks_page 继续翻页")
            return rows
        rows = self._read_state(queue_name, selected_state, limit)

        self._supplement_task_records(rows)

        return rows[:limit]

    @staticmethod
    def _apply_time_filters(
        rows: List[Dict[str, Any]],
        created_after: Optional[float] = None,
        created_before: Optional[float] = None,
        completed_after: Optional[float] = None,
        completed_before: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """对任务列表应用时间范围筛选。"""
        result = rows
        if created_after is not None:
            result = [r for r in result if r.get("created_at") and float(r["created_at"]) >= created_after]
        if created_before is not None:
            result = [r for r in result if r.get("created_at") and float(r["created_at"]) <= created_before]
        if completed_after is not None:
            result = [r for r in result if r.get("updated_at") and float(r["updated_at"]) >= completed_after]
        if completed_before is not None:
            result = [r for r in result if r.get("updated_at") and float(r["updated_at"]) <= completed_before]
        return result

    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        return read_task_record(self.r, task_id)

    def resolve_payload(self, task_id: str, queue_name: str, state: str) -> Dict[str, Any]:
        """从队列中查找任务消息并还原完整 payload（解压 / 拉取外存）。"""
        selected_state = QueueState(state) if state in QueueState._value2member_map_ else QueueState.all
        if selected_state == QueueState.retry_wait:
            selected_state = QueueState.delay
        elif selected_state in {QueueState.expired, QueueState.deadline_missed}:
            selected_state = QueueState.all

        if selected_state in (
            QueueState.history,
            QueueState.completed,
            QueueState.failed,
            QueueState.skipped,
            QueueState.cancelled,
        ):
            task = self.get_task(task_id)
            if not task:
                return {"task_id": task_id, "payload": None, "_note": "历史记录不含完整 payload"}
            descriptor = task.get("payload_descriptor")
            if isinstance(descriptor, dict):
                envelope = {
                    "version": 2,
                    "task_id": task_id,
                    "action": task.get("action") or "unknown",
                    "attempt": task.get("attempt", 0),
                    "max_attempts": task.get("max_attempts", self.max_attempts),
                    "payload": descriptor,
                }
                try:
                    queue = self._smart_queue(queue_name)
                    decoded_payload = queue.codec.decode_payload(
                        queue.codec.header(json.dumps(envelope))
                    )
                    return {
                        "task_id": task_id,
                        "payload": decoded_payload,
                        "action": task.get("action", ""),
                    }
                except Exception as exc:
                    return {
                        "task_id": task_id,
                        "payload": descriptor,
                        "action": task.get("action", ""),
                        "_note": f"payload 还原失败: {exc}",
                    }
            history_payload = task.get("payload")
            return {
                "task_id": task_id,
                "payload": history_payload,
                "action": task.get("action", ""),
                "_note": (
                    "历史记录不含 V2 payload descriptor" if history_payload is None else ""
                ),
            }

        for item_state in ([selected_state] if selected_state != QueueState.all else [
            QueueState.ready, QueueState.processing, QueueState.retry, QueueState.dlq, QueueState.delay,
        ]):
            raw_msg = self._find_raw_message(queue_name, task_id, item_state)
            if raw_msg:
                return self._resolve_payload_from_msg(raw_msg, task_id)

        if self.get_task(task_id):
            return self.resolve_payload(task_id, queue_name, QueueState.history.value)
        return {"task_id": task_id, "payload": None, "_note": "未在队列中找到此任务"}

    def _supplement_task_records(self, rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """批量补齐审计字段，容器位置保持本次真实读取的位置。"""
        missing = [row for row in rows if row.get("task_id")]
        if not missing:
            return {}

        records = {str(item.get("task_id")): item for item in self._read_history_records(
            [str(row["task_id"]) for row in missing]
        )}
        for row in missing:
            record = records.get(str(row["task_id"]), {})
            for field in ("action", "attempt", "max_attempts", "created_at", "updated_at",
                          "outcome", "status", "available_at", "delay_reason", "last_error",
                          "last_error_code", "reason", "reason_code", "logical_key",
                          "start_deadline_at", "expires_at", "replay_of"):
                if record.get(field) is not None and (field != "action" or record[field]):
                    row[field] = record[field]
        return records

    def _find_raw_message(self, queue_name: str, task_id: str, state: QueueState) -> Optional[str]:
        for key in self._state_keys(queue_name, state):
            if state == QueueState.delay:
                for raw_msg, _score in self.r.zscan_iter(key):
                    if self._message_task_id(raw_msg) == task_id:
                        return cast(str, raw_msg)
            else:
                for raw_msg in self.r.lrange(key, 0, -1):
                    if self._message_task_id(raw_msg) == task_id:
                        return cast(str, raw_msg)
        return None

    def _resolve_payload_from_msg(self, raw_msg: str, task_id: str) -> Dict[str, Any]:
        decode_error: Exception | None = None
        action_hint = ""
        try:
            queue_name = self._find_history_queue(task_id)
            queue = self._smart_queue(queue_name) if queue_name else None
            if queue is not None:
                header = queue.codec.header(raw_msg)
                payload = queue.codec.decode_payload(header)
                action = header.action
                payload_action = payload.get("action") if isinstance(payload, dict) else None
                if not action and isinstance(payload_action, str):
                    action = payload_action
                if not action:
                    history = self.get_task(task_id)
                    action = str(history.get("action") or "") if history else ""
                return {"task_id": task_id, "payload": payload, "action": action}
        except Exception as exc:
            # V1 的 header 不包含 action；解码失败后仍继续走下面的兼容
            # 解析，以便从 payload 或历史记录保留 action。
            decode_error = exc
            try:
                raw_data = json.loads(raw_msg)
                raw_payload = raw_data.get("payload", {}) if isinstance(raw_data, dict) else {}
                parsed_payload = (
                    self._parse_json(raw_payload) if isinstance(raw_payload, str) else raw_payload
                )
                if isinstance(parsed_payload, dict) and isinstance(parsed_payload.get("action"), str):
                    action_hint = parsed_payload["action"]
            except (json.JSONDecodeError, TypeError):
                pass

        # 无历史队列名时保留 V1 兼容解析。
        try:
            data = json.loads(raw_msg)
        except (json.JSONDecodeError, TypeError):
            note = f"payload 还原失败: {decode_error}" if decode_error else "消息解码失败"
            return {"task_id": task_id, "payload": None, "action": action_hint, "_note": note}

        payload_raw = data.get("payload", {})
        legacy_payload = self._parse_json(payload_raw) if isinstance(payload_raw, str) else payload_raw

        if isinstance(legacy_payload, dict):
            if legacy_payload.get("_compressed"):
                try:
                    encoded_data = legacy_payload.get("data")
                    if not isinstance(encoded_data, str):
                        raise ValueError("compressed payload data must be a string")
                    compressed = base64.b64decode(encoded_data)
                    # 每次新建解压上下文：dashboard 线程池并发调用，共享实例不安全
                    raw = zstandard.ZstdDecompressor().decompress(compressed)
                    legacy_payload = json.loads(raw)
                except Exception as e:
                    return {
                        "task_id": task_id,
                        "payload": legacy_payload,
                        "_note": f"解压失败: {e}",
                    }

            elif legacy_payload.get("_large"):
                if self.storage:
                    try:
                        storage_key = legacy_payload.get("key")
                        if not isinstance(storage_key, str):
                            raise ValueError("external payload key must be a string")
                        raw = self.storage.load(storage_key)
                        legacy_payload = json.loads(raw)
                    except Exception as e:
                        return {
                            "task_id": task_id,
                            "payload": legacy_payload,
                            "_note": f"外存拉取失败: {e}",
                        }
                else:
                    action = ""
                    hist = self.get_task(task_id)
                    if hist:
                        action = str(hist.get("action") or "")
                    return {
                        "task_id": task_id,
                        "payload": legacy_payload,
                        "action": action,
                        "_note": "未配置 RemoteStorage，无法还原大 payload",
                    }

        action_value = legacy_payload.get("action", "") if isinstance(legacy_payload, dict) else ""
        action = str(action_value) if isinstance(action_value, str) else ""
        if not action:
            hist = self.get_task(task_id)
            if hist:
                action = str(hist.get("action") or "")

        result: Dict[str, Any] = {"task_id": task_id, "payload": legacy_payload, "action": action}
        if decode_error and not action:
            result["_note"] = f"payload 还原失败: {decode_error}"
        return result

    def diagnose(self, queue_name: str) -> Dict[str, Any]:
        stats = self.queue_stats(queue_name)
        workers = self.list_workers(queue_name)
        suggestions = []

        if stats["dlq"] > 0:
            suggestions.append("DLQ 中有失败任务，可先查看详情，再选择单条或批量重放。")
        if stats["retry"] > 0:
            suggestions.append("retry 队列有待重试任务，可手动 drain 到 ready。")
        if stats["stale_workers"] > 0:
            suggestions.append("发现 stale worker processing，可执行安全恢复。")
        if stats["queue"] > 0 and stats["active_workers"] == 0:
            suggestions.append("ready 有积压但没有活跃 Worker，请启动对应消费者。")
        if stats["processing"] > 0 and stats["active_workers"] == 0:
            suggestions.append("processing 有任务但无活跃 Worker，请先执行 stale recover。")
        if not suggestions:
            suggestions.append("未发现需要立即处理的问题。")

        return {
            "queue": queue_name,
            "stats": stats,
            "workers": workers,
            "suggestions": suggestions,
        }

    # ==================== Task Control ====================

    def enqueue(
        self,
        queue_name: str,
        spec: TaskSpec,
        *,
        on_duplicate: DuplicateAction | str = DuplicateAction.REJECT,
    ) -> EnqueueResult:
        """以与 Python SDK 相同的 TaskSpec 语义投递。"""
        return self._smart_queue(queue_name).enqueue(spec, on_duplicate=on_duplicate)

    def enqueue_many(
        self,
        queue_name: str,
        specs: Sequence[TaskSpec],
        *,
        on_duplicate: DuplicateAction | str = DuplicateAction.REJECT,
    ) -> list[EnqueueResult]:
        """逐项投递；异常时 BatchEnqueueError.results 保存已完成前缀。"""
        return self._smart_queue(queue_name).enqueue_many(specs, on_duplicate=on_duplicate)

    def push_task(
        self,
        queue_name: str,
        payload: Dict[str, Any],
        delay_seconds: int = 0,
        expire_seconds: int = 0,
        *,
        action: str | None = None,
        logical_key: str | None = None,
        scheduled_for: datetime | str | float | None = None,
        not_before_at: datetime | str | float | None = None,
        start_deadline_at: datetime | str | float | None = None,
        dedup_until: datetime | str | float | None = None,
        dedup_ttl: int | float | None = None,
        trace_id: str | None = None,
        parent_task_id: str | None = None,
        concurrency_key: str | None = None,
        supersede_key: str | None = None,
        supersede_version: int | str | None = None,
        on_duplicate: DuplicateAction | str = DuplicateAction.REJECT,
    ) -> Dict[str, Any]:
        if delay_seconds < 0 or expire_seconds < 0 or (dedup_ttl is not None and dedup_ttl < 0):
            raise ValueError("relative time values must be >= 0")
        now = datetime.fromtimestamp(self.clock.now(), tz=timezone.utc)
        not_before = self._coerce_datetime(not_before_at)
        deadline = self._coerce_datetime(start_deadline_at)
        retained = self._coerce_datetime(dedup_until)
        if not_before is None and delay_seconds:
            not_before = now + timedelta(seconds=delay_seconds)
        if deadline is None and expire_seconds:
            deadline = now + timedelta(seconds=expire_seconds)
        if retained is None and dedup_ttl:
            retained = now + timedelta(seconds=float(dedup_ttl))
        selected_action = action or payload.get("action")
        if not isinstance(selected_action, str) or not selected_action:
            raise ValueError("action or payload['action'] is required")
        spec = TaskSpec(
            action=selected_action,
            payload=cast(Mapping[str, JsonValue], payload),
            logical_key=logical_key,
            scheduled_for=self._coerce_datetime(scheduled_for),
            not_before_at=not_before,
            start_deadline_at=deadline,
            dedup_until=retained,
            trace_id=trace_id,
            parent_task_id=parent_task_id,
            concurrency_key=concurrency_key,
            supersede_key=supersede_key,
            supersede_version=supersede_version,
        )
        result = self.enqueue(queue_name, spec, on_duplicate=on_duplicate)
        return {"queue": queue_name, **result.as_dict(), "delay_seconds": delay_seconds}

    def move_retry(self, queue_name: str) -> Dict[str, int]:
        count = self._smart_queue(queue_name).move_retry()
        return {"moved": count}

    def replay_task(
        self,
        task_id: str,
        *,
        queue_name: str | None = None,
        start_deadline_at: datetime | str | float | None = None,
        payload: Mapping[str, JsonValue] | None = None,
        logical_key: str | None = None,
        replace_payload: bool = False,
        replace_logical_key: bool = False,
        action: str | None = None,
        max_attempts: int | None = None,
        require_dlq: bool = False,
    ) -> Dict[str, Any]:
        """创建新实例 replay，返回新 task_id；原终态记录不变。"""
        record = self.get_task(task_id)
        selected_queue = queue_name or (str(record.get("_queue")) if record else None)
        selected_queue = selected_queue or self._find_history_queue(task_id)
        if not selected_queue:
            raise KeyError(f"cannot determine queue for task: {task_id}")
        queue = self._smart_queue(selected_queue)
        kwargs: dict[str, Any] = {
            "start_deadline_at": self._coerce_datetime(start_deadline_at),
            "action": action,
            "require_dlq": require_dlq,
        }
        budget = max_attempts
        if budget is None and record and record.get("max_attempts") not in (None, ""):
            budget = record["max_attempts"]
            if isinstance(budget, str) and budget.isdecimal():
                budget = int(budget)
        if budget is not None:
            if isinstance(budget, bool) or not isinstance(budget, int) or budget < 1:
                raise ValueError("max_attempts must be a positive integer")
            kwargs["max_attempts"] = budget
        if replace_payload:
            if payload is None:
                raise ValueError("replace_payload=True requires payload")
            kwargs["payload"] = payload
        if replace_logical_key:
            kwargs["logical_key"] = logical_key
        result = queue.replay_task(task_id, **kwargs)
        return {"queue": selected_queue, "replay_of": task_id, **result.as_dict()}

    def requeue_dlq(
        self,
        queue_name: str,
        task_id: Optional[str] = None,
        reset_retry: bool = True,
    ) -> Dict[str, Any]:
        if task_id:
            record = self.get_task(task_id)
            if record and self._has_v2_replay_source(record) and str(record.get("outcome") or record.get("status")) in {
                "failed",
                "skipped",
                "cancelled",
            }:
                result = self.replay_task(task_id, queue_name=queue_name, require_dlq=True)
                return {"moved": int(result["accepted"]), **result}
            moved = int(
                self.requeue_task(
                    queue_name,
                    task_id,
                    QueueState.dlq,
                    reset_retry=reset_retry,
                )["moved"]
            )
            return {"moved": moved}
        self._smart_queue(queue_name)._assert_queue_namespace()
        messages = list(reversed(self.r.lrange(f"{queue_name}:dlq", 0, -1)))
        moved = skipped = attempted = 0
        errors: list[dict[str, Any]] = []
        seen: set[str] = set()
        for raw_message in messages:
            attempted += 1
            identifier = self._message_task_id(raw_message)
            try:
                if not identifier:
                    raise ValueError("消息不含 task_id，未进行重放")
                if identifier in seen:
                    raise ValueError("同一 task_id 有重复消息，未重复生成任务")
                seen.add(identifier)
                result = self.requeue_task(queue_name, identifier, QueueState.dlq, reset_retry=reset_retry)
                if not result["moved"]:
                    raise ValueError(str(result.get("reason") or result.get("note") or "任务已变化或未被接受"))
                moved += int(result["moved"])
            except RemoteStorageError as exc:
                skipped += 1
                if len(errors) < 20:
                    errors.append({"task_id": identifier, "reason": f"外部存储操作失败（{exc.code}），请核对配置后重试"})
            except (ValueError, KeyError, RuntimeError) as exc:
                skipped += 1
                if len(errors) < 20:
                    errors.append({"task_id": identifier, "reason": str(exc)})
            except redis.RedisError:
                skipped += 1
                if len(errors) < 20:
                    errors.append({"task_id": identifier, "reason": "Redis 操作失败；已停止本次批量处理，请核对后重试"})
                break
        return {"moved": moved, "skipped": skipped, "attempted": attempted,
                "not_attempted": len(messages) - attempted, "errors": errors,
                "errors_truncated": skipped > len(errors)}

    def requeue_task(
        self,
        queue_name: str,
        task_id: str,
        from_state: QueueState | str,
        reset_retry: bool = True,
    ) -> Dict[str, Any]:
        state = QueueState(from_state)
        self._smart_queue(queue_name)._assert_queue_namespace()
        if state in {QueueState.ready, QueueState.history, QueueState.all}:
            return {"moved": 0, "task_id": task_id, "queue": queue_name, "from_state": state.value}

        if state == QueueState.processing:
            guard = self._check_active_processing(queue_name, task_id)
            if guard:
                return {
                    "moved": 0,
                    "task_id": task_id,
                    "queue": queue_name,
                    "from_state": state.value,
                    **guard,
                }

        record: Dict[str, Any] | None = None
        if state == QueueState.dlq:
            record = self.get_task(task_id)
            if record and self._has_v2_replay_source(record) and str(record.get("outcome") or record.get("status")) in {
                "failed",
                "skipped",
                "cancelled",
            }:
                replay = self.replay_task(task_id, queue_name=queue_name, require_dlq=True)
                return {
                    "moved": int(replay["accepted"]),
                    "task_id": task_id,
                    "new_task_id": replay.get("task_id"),
                    "queue": queue_name,
                    "from_state": state.value,
                    "reason": replay.get("reason"),
                }

        for key in self._state_keys(queue_name, state):
            if state == QueueState.delay:
                for raw_msg, _score in self.r.zscan_iter(key):
                    if self._message_task_id(raw_msg) == task_id:
                        moved = self._move_delay_message(key, queue_name, raw_msg)
                        if moved:
                            self._update_history(task_id, {"operational_message": 1})
                        return {
                            "moved": int(moved),
                            "task_id": task_id,
                            "queue": queue_name,
                            "from_state": state.value,
                        }
            else:
                for raw_msg in self.r.lrange(key, 0, -1):
                    if self._message_task_id(raw_msg) == task_id:
                        if state == QueueState.dlq:
                            data = self._parse_json(raw_msg)
                            if isinstance(data, dict) and data.get("version") == 2:
                                raise ValueError("V2 任务缺少可重放的审计记录或 payload descriptor，原消息已保留")
                        new_msg = (
                            self._reset_message_retry(raw_msg) if reset_retry and state == QueueState.dlq else raw_msg
                        )
                        heartbeat_key = None
                        if state == QueueState.processing and ":processing:" in key:
                            worker_id = key.rsplit(":processing:", 1)[1]
                            heartbeat_key = f"{queue_name}:worker:{worker_id}"
                        moved = self._move_list_message(
                            key, queue_name, raw_msg, new_msg, heartbeat_key=heartbeat_key,
                        )
                        if moved:
                            self._update_history(task_id, {"operational_message": 1})
                            # 只有没有不可变 V2 outcome 的旧记录才恢复为 pending；
                            # V2 terminal 记录必须保持不可变并走 replay 分支。
                            if (
                                state == QueueState.dlq
                                and record
                                and not record.get("outcome")
                                and str(record.get("status") or "") in {"failed", "retry"}
                            ):
                                self._update_history(task_id, {"status": "pending"})
                        return {
                            "moved": int(moved),
                            "task_id": task_id,
                            "queue": queue_name,
                            "from_state": state.value,
                        }

        return {"moved": 0, "task_id": task_id, "queue": queue_name, "from_state": state.value}

    def _check_active_processing(self, queue_name: str, task_id: str) -> Optional[Dict[str, Any]]:
        """拒绝从活跃 Worker 的 processing 重放，避免与正在执行的 handler 冲突。"""
        heartbeat_prefix = f"{queue_name}:worker:"
        for key in self.processing_keys(queue_name, include_legacy=False):
            worker_id = key.rsplit(":", 1)[-1]
            if not self.r.exists(f"{heartbeat_prefix}{worker_id}"):
                continue
            for raw_msg in self.r.lrange(key, 0, -1):
                if self._message_task_id(raw_msg) == task_id:
                    return {
                        "note": f"任务在活跃 worker {worker_id} 的 processing 中，拒绝重放；如确需强制请先 recover",
                    }
        return None

    def recover(
        self, queue_name: str, include_active: bool = False, *, worker_id: str | None = None
    ) -> Dict[str, int]:
        """恢复队列或指定 Worker；精确范围仍在原子迁移时复核心跳。"""
        if worker_id is not None and (
            not isinstance(worker_id, str)
            or not worker_id
            or worker_id.strip() != worker_id
            or len(worker_id.encode("utf-8")) > 256
            or any(char in worker_id for char in ":*?[]")
            or any(ord(char) < 32 for char in worker_id)
        ):
            raise ValueError("worker_id must be a non-empty identifier without colon or glob characters")
        self._smart_queue(queue_name)._assert_queue_namespace()
        legacy_processing = f"{queue_name}:processing"
        if include_active and worker_id is None:
            recovered = self._drain_list_to_ready(legacy_processing, queue_name)
            skipped_legacy = 0
        else:
            recovered = 0
            skipped_legacy = int(self.r.llen(legacy_processing)) if worker_id is None else 0

        skipped = 0
        heartbeat_prefix = f"{queue_name}:worker:"

        keys = (
            [f"{queue_name}:processing:{worker_id}"]
            if worker_id is not None else self.processing_keys(queue_name, include_legacy=False)
        )
        for key in keys:
            worker_id = key.rsplit(":", 1)[-1]
            if not include_active and self.r.exists(f"{heartbeat_prefix}{worker_id}"):
                skipped += int(self.r.llen(key))
                continue
            recovered += self._drain_list_to_ready(
                key, queue_name,
                heartbeat_key=None if include_active else f"{heartbeat_prefix}{worker_id}",
            )
        return {"recovered": recovered, "skipped_active": skipped, "skipped_legacy": skipped_legacy}

    def delete_task(
        self,
        task_id: str,
        queue_name: Optional[str] = None,
    ) -> Dict[str, int]:
        """原子取消消息后删除审计记录；活跃 handler 必须先完成或停止。"""
        record = self.get_task(task_id)
        record_queue = str(record.get("_queue") or "") if record else ""
        if queue_name and record_queue and queue_name != record_queue:
            raise ValueError(f"task {task_id} belongs to {record_queue}, not {queue_name}")
        queues = [queue_name or record_queue] if (queue_name or record_queue) else self.queue_names()

        # 先检查全部候选队列，避免已删部分消息后才发现活跃 handler。
        for queue in queues:
            if queue and self._check_active_processing(queue, task_id):
                raise ValueError(f"task {task_id} is being processed by an active worker")

        queue_removed = 0
        for queue in queues:
            if not queue:
                continue
            smart_queue = self._smart_queue(queue)
            for state in [
                QueueState.ready,
                QueueState.processing,
                QueueState.retry,
                QueueState.dlq,
            ]:
                for key in self._state_keys(queue, state):
                    for raw_msg in self.r.lrange(key, 0, -1):
                        if self._message_task_id(raw_msg) == task_id:
                            queue_removed += smart_queue._cancel_or_purge(
                                key,
                                "list",
                                str(raw_msg),
                                IdentityPolicy.RELEASE,
                                reason="task deleted by administrator",
                            )
            delay_key = f"{queue}:delay"
            for raw_msg, _score in self.r.zscan_iter(delay_key):
                if self._message_task_id(raw_msg) == task_id:
                    queue_removed += smart_queue._cancel_or_purge(
                        delay_key,
                        "zset",
                        str(raw_msg),
                        IdentityPolicy.RELEASE,
                        reason="task deleted by administrator",
                    )

        # 消息可能在扫描时从 ready 转入 processing；此时必须保留审计记录。
        fresh_record = self.get_task(task_id)
        if fresh_record and str(fresh_record.get("operational_message") or "0") not in {"0", "False", "false"}:
            raise ValueError(f"task {task_id} still has an operational message")
        if fresh_record and "operational_message" not in fresh_record:
            outcome = str(fresh_record.get("outcome") or fresh_record.get("status") or "")
            if outcome not in {"completed", "failed", "skipped", "cancelled"}:
                raise ValueError(f"task {task_id} has no atomic operational state")

        identities_released = 0
        if record:
            logical_key = record.get("logical_key")
            selected_queue = queue_name or record.get("_queue") or self._find_history_queue(task_id)
            if logical_key and selected_queue:
                smart_queue = self._smart_queue(str(selected_queue))
                release_script = r"""
                local kind = redis.call('TYPE', KEYS[1])
                if type(kind) == 'table' then kind = kind['ok'] end
                if kind == 'hash' and redis.call('HGET', KEYS[1], 'task_id') == ARGV[1] then
                    return redis.call('DEL', KEYS[1])
                end
                if kind == 'string' and redis.call('GET', KEYS[1]) == ARGV[1] then
                    return redis.call('DEL', KEYS[1])
                end
                return 0
                """
                identities_released += int(
                    self.r.eval(
                        release_script,
                        1,
                        smart_queue._dedup_key(str(logical_key)),
                        task_id,
                    )
                    or 0
                )
                identities_released += int(
                    self.r.eval(
                        release_script,
                        1,
                        smart_queue._legacy_dedup_key(str(logical_key)),
                        task_id,
                    )
                    or 0
                )

        history_records = int(self.r.delete(f"qtask:task:{task_id}") or 0)
        history_indexes = 0
        for hist_key in self.r.scan_iter("qtask:hist:*"):
            history_indexes += int(self.r.zrem(hist_key, task_id) or 0)

        return {
            "queue_messages": queue_removed,
            "history_records": history_records,
            "history_indexes": history_indexes,
            "identities_released": identities_released,
        }

    def clear_queue(
        self,
        queue_name: str,
        include_dlq: bool = True,
        include_history: bool = False,
        identity_policy: IdentityPolicy | str = IdentityPolicy.KEEP,
        include_processing: bool = False,
    ) -> Dict[str, int]:
        queue = self._smart_queue(queue_name)
        if include_history and not include_processing and queue.processing_size():
            raise ValueError("cannot clear history while processing tasks remain")
        if include_history and not include_dlq and queue.dlq_size():
            raise ValueError("cannot clear history while DLQ tasks remain")
        audit = queue.clear(
            include_dlq=include_dlq,
            identity_policy=identity_policy,
            include_processing=include_processing,
        )
        deleted_keys = int(audit["messages"])
        history_records = 0
        if include_history:
            history_records = queue.history.clear_terminal()

        return {
            "deleted_keys": deleted_keys,
            "history_records": history_records,
            "identity_released": int(IdentityPolicy(identity_policy) == IdentityPolicy.RELEASE),
        }

    def delete_queue(self, queue_name: str) -> Dict[str, int]:
        """原子复核心跳并删除所属数据；并发投递或登记只能发生在操作前后。"""
        queue = self._smart_queue(queue_name)
        try:
            deleted, records = self.r.eval(
                DELETE_QUEUE_LUA, 0, queue_name, queue.owner_prefix, queue.storage_gc_key,
            )
        except redis.ResponseError as exc:
            if "active workers" in str(exc) or "ownership conflict" in str(exc):
                raise ValueError(str(exc)) from exc
            raise
        return {"deleted_keys": int(deleted), "history_records": int(records)}

    def clean_history(
        self,
        queue_name: Optional[str] = None,
        ttl_days: int = 15,
    ) -> Dict[str, Any]:
        queues = [queue_name] if queue_name else self.queue_names()
        ttl_seconds = ttl_days * 86400
        per_queue: Dict[str, int] = {}
        total = 0

        for queue in queues:
            if not queue:
                continue
            count = self._smart_queue(queue).history.clean_expired(ttl_seconds=ttl_seconds)
            per_queue[queue] = count
            total += count

        return {"cleaned": total, "queues": per_queue}

    # ==================== Internal Helpers ====================

    def _read_state(
        self,
        queue_name: str,
        state: QueueState,
        limit: int,
    ) -> List[Dict[str, Any]]:
        if state == QueueState.history:
            return self._read_history(queue_name, limit)
        if state == QueueState.delay:
            return self._read_delay(queue_name, limit)

        rows: List[Dict[str, Any]] = []
        for key in self._state_keys(queue_name, state):
            remaining = max(limit - len(rows), 0)
            if remaining <= 0:
                break
            messages = list(reversed(self.r.lrange(key, -remaining, -1)))
            for raw_msg in messages:
                rows.append(self._decode_message(raw_msg, queue_name, state.value, key))
        return rows

    def _read_delay(self, queue_name: str, limit: int) -> List[Dict[str, Any]]:
        rows = []
        key = f"{queue_name}:delay"
        for raw_msg, score in self.r.zrange(key, 0, limit - 1, withscores=True):
            item = self._decode_message(raw_msg, queue_name, QueueState.delay.value, key)
            item["run_at"] = score
            item["run_at_text"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(score))
            rows.append(item)
        return rows

    def _read_history(self, queue_name: str, limit: int) -> List[Dict[str, Any]]:
        hist_key = f"qtask:hist:{queue_name}"
        task_ids = self.r.zrevrange(hist_key, 0, limit - 1)
        rows = []
        for data in self._read_history_records(task_ids):
            data["_queue"] = queue_name
            data["_state"] = QueueState.history.value
            data["_source"] = hist_key
            rows.append(data)
        return rows

    @staticmethod
    def _matches_time_filters(
        row: Dict[str, Any],
        created_after: Optional[float] = None,
        created_before: Optional[float] = None,
        completed_after: Optional[float] = None,
        completed_before: Optional[float] = None,
    ) -> bool:
        created_at = QueueAdmin._float_or_none(row.get("created_at"))
        outcome = str(row.get("outcome") or row.get("status") or "")
        completed_at = (
            QueueAdmin._float_or_none(row.get("finished_at") or row.get("updated_at"))
            if outcome in {"completed", "failed", "skipped", "cancelled"}
            else None
        )
        if created_after is not None and (created_at is None or created_at < created_after):
            return False
        if created_before is not None and (created_at is None or created_at > created_before):
            return False
        if completed_after is not None and (completed_at is None or completed_at < completed_after):
            return False
        if completed_before is not None and (completed_at is None or completed_at > completed_before):
            return False
        return True

    def _read_history_records(self, task_ids: List[str]) -> List[Dict[str, Any]]:
        if not task_ids:
            return []

        pipe = self.r.pipeline()
        for task_id in task_ids:
            pipe.eval(READ_TASK_RECORD_LUA, 1, f"qtask:task:{task_id}")
        snapshots = pipe.execute()

        records: List[Dict[str, Any]] = []
        for task_id, snapshot in zip(task_ids, snapshots):
            record = decode_task_record(task_id, snapshot)
            if record:
                records.append(record)
        return records

    def _read_deadline_missed(
        self,
        queue_name: str,
        limit: int = 50,
        scan_limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """从 operational 容器派生 deadline_missed，不把它写成 lifecycle 状态。"""
        if limit <= 0:
            return []
        effective_scan_limit = min(max(scan_limit or limit * 4, limit), 5000)
        now = self.clock.now()
        rows: list[dict[str, Any]] = []
        seen: set[str] = set()
        sources: list[tuple[str, str]] = [
            (queue_name, "list"),
            (f"{queue_name}:retry", "list"),
            (f"{queue_name}:delay", "zset"),
        ]
        for source_key, source_type in sources:
            if source_type == "zset":
                messages = [raw for raw, _score in self.r.zrange(
                    source_key,
                    0,
                    effective_scan_limit - 1,
                    withscores=True,
                )]
            else:
                messages = self.r.lrange(source_key, -effective_scan_limit, -1)
            for raw_message in messages:
                item = self._decode_message(
                    str(raw_message),
                    queue_name,
                    QueueState.deadline_missed.value,
                    source_key,
                )
                task_id = str(item.get("task_id") or "")
                if not task_id or task_id in seen:
                    continue
                raw_data = item.get("_raw")
                deadline: Any = None
                if isinstance(raw_data, dict):
                    deadline = raw_data.get("start_deadline_at") or raw_data.get("expires_at")
                deadline_value = self._float_or_none(deadline)
                if deadline_value is None or deadline_value >= now:
                    continue
                record = self.get_task(task_id)
                outcome = (record or {}).get("outcome") or (record or {}).get("status")
                if outcome in {"completed", "failed", "skipped", "cancelled"}:
                    continue
                item["deadline_missed"] = True
                item["start_deadline_at"] = deadline_value
                item["status"] = QueueState.deadline_missed.value
                rows.append(item)
                seen.add(task_id)
                if len(rows) >= limit:
                    return rows
        return rows

    def _read_expired(
        self,
        queue_name: str,
        limit: int = 50,
        scan_limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """deadline_missed 的兼容别名。"""
        return self._read_deadline_missed(queue_name, limit=limit, scan_limit=scan_limit)

    def list_expired(self, queue_name: str, limit: int = 50) -> List[Dict[str, Any]]:
        return self._read_deadline_missed(
            queue_name,
            limit=limit,
            scan_limit=max(limit * 3, 300),
        )

    def requeue_expired(
        self,
        queue_name: str,
        task_id: Optional[str] = None,
        limit: int = 500,
        start_deadline_at: datetime | str | float | None = None,
    ) -> Dict[str, Any]:
        deadline = self._coerce_datetime(start_deadline_at)
        if deadline is None:
            return {
                "moved": 0,
                "note": "deadline_missed replay 必须显式提供新的 start_deadline_at",
            }
        if deadline.timestamp() <= self.clock.now():
            return {
                "moved": 0,
                "note": "新的 start_deadline_at 必须晚于当前时间，旧任务未被修改",
            }
        if task_id:
            return self._requeue_single_expired(queue_name, task_id, deadline)

        expired = self._read_deadline_missed(
            queue_name,
            limit=limit,
            scan_limit=max(limit * 3, 500),
        )
        moved = 0
        for task in expired:
            tid = task.get("task_id")
            if not tid:
                continue
            result = self._requeue_single_expired(queue_name, tid, deadline)
            moved += int(result["moved"])
        return {"moved": moved, "start_deadline_at": deadline.isoformat()}

    def _requeue_single_expired(
        self,
        queue_name: str,
        task_id: str,
        start_deadline_at: datetime,
    ) -> Dict[str, Any]:
        data = self.get_task(task_id)
        if not data:
            return {"moved": 0, "task_id": task_id}

        if not self._is_expired_record(data):
            return {
                "moved": 0,
                "task_id": task_id,
                "queue": queue_name,
                "note": "任务未过期",
            }

        location = self._find_task_location(queue_name, task_id)
        if not location:
            return {
                "moved": 0,
                "task_id": task_id,
                "queue": queue_name,
                "note": "任务没有 operational message，无法安全取消后 replay",
            }
        state, source_key, raw_message = location
        if state == QueueState.processing:
            return {
                "moved": 0,
                "task_id": task_id,
                "queue": queue_name,
                "note": "processing 任务不属于 deadline_missed 派生视图",
            }
        queue = self._smart_queue(queue_name)
        source_type = "zset" if state == QueueState.delay else "list"
        removed = queue._cancel_or_purge(
            source_key,
            source_type,
            raw_message,
            IdentityPolicy.KEEP,
            reason="deadline_missed task replaced by explicit replay",
        )
        if not removed:
            return {"moved": 0, "task_id": task_id, "queue": queue_name}
        try:
            replay = self.replay_task(task_id, queue_name=queue_name, start_deadline_at=start_deadline_at)
        except Exception as exc:
            return {
                "moved": 0,
                "task_id": task_id,
                "queue": queue_name,
                "note": f"旧任务已取消，但 replay 失败: {exc}",
            }
        return {
            "moved": int(replay["accepted"]),
            "task_id": task_id,
            "new_task_id": replay["task_id"],
            "queue": queue_name,
            "from_state": state.value,
        }

    def _find_task_location(
        self,
        queue_name: str,
        task_id: str,
    ) -> Optional[Tuple[QueueState, str, str]]:
        for state in [
            QueueState.ready,
            QueueState.retry,
            QueueState.dlq,
            QueueState.delay,
            QueueState.processing,
        ]:
            for key in self._state_keys(queue_name, state):
                if state == QueueState.delay:
                    for raw_msg, _score in self.r.zscan_iter(key):
                        if self._message_task_id(raw_msg) == task_id:
                            return state, key, cast(str, raw_msg)
                else:
                    for raw_msg in self.r.lrange(key, 0, -1):
                        if self._message_task_id(raw_msg) == task_id:
                            return state, key, cast(str, raw_msg)
        return None

    def _decode_message(
        self,
        raw_msg: str,
        queue_name: str,
        state: str,
        source_key: str,
    ) -> Dict[str, Any]:
        item: Dict[str, Any] = {
            "task_id": "",
            "action": "",
            "payload": None,
            "_queue": queue_name,
            "_state": state,
            "_source": source_key,
            "_raw": raw_msg,
            "location": state,
        }
        try:
            data = json.loads(raw_msg)
        except (json.JSONDecodeError, TypeError) as exc:
            item["decode_error"] = str(exc)
            return item

        if not isinstance(data, dict):
            item["_raw"] = data
            return item

        try:
            version = int(data.get("version", 1) or 1)
            attempt = int(data.get("attempt", 0) or 0)
        except (ValueError, TypeError, OverflowError) as exc:
            item.update(task_id=str(data.get("task_id") or ""), decode_error=str(exc))
            return item
        if version == 2:
            descriptor = data.get("payload", {})
            payload = descriptor
            if isinstance(descriptor, dict) and descriptor.get("kind") == "inline":
                payload = descriptor.get("data")
            item.update(
                {
                    "task_id": data.get("task_id", ""),
                    "action": data.get("action", ""),
                    "payload": payload,
                    "payload_kind": descriptor.get("kind", "")
                    if isinstance(descriptor, dict)
                    else "",
                    "attempt": attempt,
                    "retry": max(attempt - 1, 0),
                    "max_attempts": data.get("max_attempts"),
                    "created_at": data.get("created_at"),
                    "updated_at": data.get("updated_at") or data.get("created_at"),
                    "logical_key": data.get("logical_key"),
                    "scheduled_for": data.get("scheduled_for"),
                    "start_deadline_at": data.get("start_deadline_at"),
                    "delay_reason": data.get("delay_reason", ""),
                    "available_at": data.get("available_at"),
                    "trace_id": data.get("trace_id"),
                    "parent_task_id": data.get("parent_task_id"),
                    "replay_of": data.get("replay_of"),
                    "status": state,
                    "_raw": data,
                }
            )
            return item

        payload_raw = data.get("payload", {})
        payload = self._parse_json(payload_raw) if isinstance(payload_raw, str) else payload_raw
        item["task_id"] = data.get("task_id", "")
        item["payload"] = payload
        item["_raw"] = data
        if isinstance(payload, dict):
            item["action"] = payload.get("action", "")
            item["retry"] = payload.get("_retry", 0)
            item["status"] = state
        return item

    def _state_keys(self, queue_name: str, state: QueueState) -> List[str]:
        if state == QueueState.ready:
            return [queue_name]
        if state == QueueState.processing:
            return self.processing_keys(queue_name)
        if state == QueueState.retry:
            return [f"{queue_name}:retry"]
        if state == QueueState.dlq:
            return [f"{queue_name}:dlq"]
        if state == QueueState.delay:
            return [f"{queue_name}:delay"]
        return []

    def _drain_list_to_ready(
        self,
        source: str,
        queue_name: str,
        update_status: bool = False,
        reset_retry: bool = False,
        heartbeat_key: str | None = None,
    ) -> int:
        count = 0
        collected_task_ids: List[str] = []
        while True:
            msg = self.r.lindex(source, -1)
            if not msg:
                break
            new_msg = self._reset_message_retry(msg) if reset_retry else msg
            moved = self._move_list_message(source, queue_name, msg, new_msg, heartbeat_key=heartbeat_key)
            if not moved:
                break
            count += 1
            if update_status:
                task_id = self._message_task_id(msg)
                if task_id:
                    collected_task_ids.append(task_id)
        if collected_task_ids:
            self._batch_update_status(collected_task_ids, queue_name)
        return count

    def _move_list_message(
        self, source: str, destination: str, raw_msg: str, new_msg: Optional[str] = None,
        *, heartbeat_key: str | None = None,
    ) -> bool:
        lua_script = """
        for _, key in ipairs({KEYS[1], KEYS[2]}) do
            local kind = redis.call('TYPE', key)['ok']
            if kind ~= 'none' and kind ~= 'list' then error('admin move key type conflict') end
        end
        local deadline_kind = redis.call('TYPE', KEYS[4])['ok']
        if deadline_kind ~= 'none' and deadline_kind ~= 'zset' then error('deadline index type conflict') end
        if ARGV[3] == '1' and redis.call('EXISTS', KEYS[3]) == 1 then return 0 end
        local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
        if removed == 0 then
            return 0
        end
        redis.call('LPUSH', KEYS[2], ARGV[2])
        local ok, envelope = pcall(cjson.decode, ARGV[2])
        if ok and type(envelope) == 'table' and envelope['task_id'] then
            local record_key = 'qtask:task:' .. tostring(envelope['task_id'])
            local kind = redis.call('TYPE', record_key)
            if type(kind) == 'table' then kind = kind['ok'] end
            if kind == 'hash' then
                redis.call('HSET', record_key, 'location', 'ready')
                local deadline = redis.call('HGET', record_key, 'start_deadline_at') or ''
                if deadline ~= '' then redis.call('ZADD', KEYS[4], deadline, tostring(envelope['task_id'])) end
            end
        end
        return removed
        """
        return bool(self.r.eval(
            lua_script, 4, source, destination, heartbeat_key or f"{destination}:__qtask_dummy__:heartbeat",
            f"qtask:deadline:{destination}",
            raw_msg, new_msg or raw_msg, "1" if heartbeat_key else "0",
        ))

    def _reset_message_retry(self, raw_msg: str) -> str:
        """剥离 DLQ 消息中的 _retry 计数（人工重放视为全新尝试）。解码失败原样返回。"""
        try:
            data = json.loads(raw_msg)
            if not isinstance(data, dict):
                return raw_msg
            payload_raw = data.get("payload", {})
            payload = json.loads(payload_raw) if isinstance(payload_raw, str) else payload_raw
            if not isinstance(payload, dict):
                return raw_msg
            payload.pop("_retry", None)
            return SmartQueue._build_envelope(str(data["task_id"]), payload, data.get("expires_at"))
        except Exception:
            return raw_msg

    def _move_delay_message(self, source: str, destination: str, raw_msg: str) -> bool:
        lua_script = """
        for index, expected in ipairs({'zset', 'list', 'hash'}) do
            local kind = redis.call('TYPE', KEYS[index])['ok']
            if kind ~= 'none' and kind ~= expected then error('admin delay move key type conflict') end
        end
        local raw_counter = redis.call('HGET', KEYS[3], 'retry_wait.current')
        if raw_counter and not string.match(raw_counter, '^%-?%d+$') then error('invalid retry counter') end
        local removed = redis.call('ZREM', KEYS[1], ARGV[1])
        if removed == 0 then
            return 0
        end
        local ok, envelope = pcall(cjson.decode, ARGV[1])
        if ok and type(envelope) == 'table' and envelope['delay_reason'] == 'retry' then
            local current = tonumber(redis.call('HGET', KEYS[3], 'retry_wait.current') or '0')
            if current > 0 then redis.call('HINCRBY', KEYS[3], 'retry_wait.current', -1) end
        end
        redis.call('LPUSH', KEYS[2], ARGV[1])
        if ok and type(envelope) == 'table' and envelope['task_id'] then
            local record_key = 'qtask:task:' .. tostring(envelope['task_id'])
            local kind = redis.call('TYPE', record_key)
            if type(kind) == 'table' then kind = kind['ok'] end
            if kind == 'hash' then redis.call('HSET', record_key, 'location', 'ready') end
        end
        return removed
        """
        queue_name = source[: -len(":delay")]
        return bool(self.r.eval(
            lua_script, 3, source, destination, f"qtask:metrics:{queue_name}", raw_msg
        ))

    def _message_task_id(self, raw_msg: str) -> Optional[str]:
        try:
            data = json.loads(raw_msg)
        except (json.JSONDecodeError, TypeError):
            return None
        if not isinstance(data, dict):
            return None
        task_id = data.get("task_id")
        return str(task_id) if task_id else None

    @staticmethod
    def _has_v2_replay_source(record: Dict[str, Any] | None) -> bool:
        """判断历史记录是否具备 V2 replay 所需的 action 和 payload descriptor。"""
        if not record or not str(record.get("action") or ""):
            return False
        descriptor = record.get("payload_descriptor")
        if isinstance(descriptor, str):
            try:
                descriptor = json.loads(descriptor)
            except (json.JSONDecodeError, TypeError):
                return False
        return isinstance(descriptor, dict)

    def _smart_queue(self, queue_name: str) -> SmartQueue:
        namespace, short_name = self._split_queue_name(queue_name)
        return SmartQueue(
            self.redis_url,
            short_name,
            namespace=namespace or None,
            redis_client=self.r,
            storage=self.storage,
            max_attempts=self.max_attempts,
            retry_backoff_base=self.retry_backoff_base,
            retry_backoff_max=self.retry_backoff_max,
            ttl_days=self.ttl_days,
            history_mode=self.history_mode,
            clock=self.clock,
        )

    def _update_history(self, task_id: str, fields: Dict[str, Any]) -> bool:
        record = self.get_task(task_id)
        if not record:
            return False
        queue_name = record.get("_queue") or self._find_history_queue(task_id)
        if not queue_name:
            return False
        queue = self._smart_queue(str(queue_name))
        return queue.history.update(task_id, fields)

    def _batch_update_status(self, task_ids: List[str], queue_name: str) -> int:
        if not task_ids:
            return 0
        now = time.time()
        queue = self._smart_queue(queue_name)
        idx_key = queue.history.idx_key
        lua_script = """
        if redis.call('EXISTS', KEYS[1]) == 0 then
            return 0
        end
        local outcome = redis.call('HGET', KEYS[1], 'outcome') or ''
        if outcome ~= '' and outcome ~= 'none' then return 0 end
        redis.call('HSET', KEYS[1], 'status', 'pending', 'operational_message', '1', 'updated_at', ARGV[1])
        redis.call('PERSIST', KEYS[1])
        redis.call('ZADD', KEYS[2], 'NX', ARGV[1], ARGV[2])
        return 1
        """
        pipe = self.r.pipeline()
        for task_id in task_ids:
            key = f"qtask:task:{task_id}"
            pipe.eval(lua_script, 2, key, idx_key, now, task_id)
        results = pipe.execute()
        return sum(int(result or 0) for result in results)

    def _find_history_queue(self, task_id: str) -> Optional[str]:
        for hist_key in self.r.scan_iter("qtask:hist:*"):
            if self.r.zscore(hist_key, task_id) is not None:
                return str(hist_key).replace("qtask:hist:", "")
        return None

    @staticmethod
    def _split_queue_name(queue_name: str) -> Tuple[str, str]:
        if ":" not in queue_name:
            return "", queue_name
        namespace, short_name = queue_name.rsplit(":", 1)
        return namespace, short_name

    @staticmethod
    def _parse_json(value: Any) -> Any:
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value

    @staticmethod
    def _is_expired_record(data: Dict[str, Any]) -> bool:
        expires_at = data.get("start_deadline_at") or data.get("expires_at")
        outcome = data.get("outcome") or data.get("status", "")
        if not expires_at or outcome in ("completed", "failed", "skipped", "cancelled"):
            return False
        try:
            return float(expires_at) < time.time()
        except (TypeError, ValueError):
            return False

    @staticmethod
    def _coerce_datetime(value: datetime | str | float | None) -> datetime | None:
        """管理入口接受 aware datetime、ISO 8601 或 epoch，统一成 aware datetime。"""
        if value is None or value == "":
            return None
        if isinstance(value, datetime):
            result = value
        elif isinstance(value, (int, float)):
            result = datetime.fromtimestamp(float(value), tz=timezone.utc)
        elif isinstance(value, str):
            normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
            result = datetime.fromisoformat(normalized)
        else:
            raise TypeError("datetime value must be datetime, ISO string, epoch or None")
        if result.tzinfo is None or result.utcoffset() is None:
            raise ValueError("datetime value must be timezone-aware")
        return result

    @staticmethod
    def _float_or_none(value: Any) -> Optional[float]:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None


def task_matches_payload(task: Dict[str, Any], pairs: Iterable[Tuple[str, Any]]) -> bool:
    payload = task.get("payload")
    if not isinstance(payload, dict):
        return False
    return all(payload.get(key) == value for key, value in pairs)
