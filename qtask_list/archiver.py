import sqlite3
import os
import time
import json
from datetime import datetime
from typing import Any, Dict, List, Optional

import redis
from loguru import logger

from .history import READ_TASK_RECORD_LUA, decode_task_record


class Monitor:
    """Redis 内存监控器"""

    def __init__(self, r: Any, threshold_mb: Optional[int] = None):
        self.r = r
        self.threshold_mb = threshold_mb

    def get_memory_info(self) -> Dict[str, Any]:
        """获取 Redis 内存统计信息"""
        info: Dict[str, Any] = self.r.info("memory")
        return {
            "used_memory_human": info.get("used_memory_human"),
            "used_memory_peak_human": info.get("used_memory_peak_human"),
            "used_memory_lua_human": info.get("used_memory_lua_human"),
            "maxmemory_human": info.get("maxmemory_human"),
            "used_memory": info.get("used_memory"),
            "maxmemory": info.get("maxmemory"),
            "status": "healthy" if self.check_health() else "warning",
        }

    def check_health(self) -> bool:
        """检查内存是否在健康阈值内"""
        if not self.threshold_mb:
            return True

        info: Dict[str, Any] = self.r.info("memory")
        used = info.get("used_memory", 0)

        if used > self.threshold_mb * 1024 * 1024:
            logger.warning(
                f"Redis memory usage ({info.get('used_memory_human')}) exceeds "
                f"threshold ({self.threshold_mb} MB)!"
            )
            return False
        return True


class ArchiveManager:
    """SQLite 归档管理器"""

    # 只归档 terminal 状态：仍在队列中的 live 任务（ready/processing/retry/delay）
    # 历史必须保留在 Redis，否则任务完成后 ack/fail 的历史更新会静默丢失
    TERMINAL_STATUSES = ("completed", "failed", "skipped", "cancelled")

    def __init__(self, redis_url: str, db_dir: str = "archive_data", prefix: str = "qtask_hist"):
        self.redis_url = redis_url
        self.r: Any = redis.from_url(redis_url, decode_responses=True)
        self.db_dir = db_dir
        self.prefix = prefix

        # exist_ok：多 Worker 同时启动时并发创建同一目录不报错
        os.makedirs(self.db_dir, exist_ok=True)

    def _get_db_path(self, date_str: str) -> str:
        """获取指定日期的 DB 文件路径"""
        return os.path.join(self.db_dir, f"{self.prefix}_{date_str}.db")

    def _init_db(self, db_path: str):
        """初始化 SQLite 表结构"""
        with sqlite3.connect(db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS task_history (
                    task_id TEXT PRIMARY KEY,
                    queue_name TEXT,
                    action TEXT,
                    status TEXT,
                    payload TEXT,
                    result TEXT,
                    created_at REAL,
                    updated_at REAL,
                    raw_data TEXT
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_queue ON task_history(queue_name)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON task_history(status)")

    def archive_to_sqlite(
        self, queue_full_name: str, days_ago: int = 1, batch_size: int = 500
    ) -> int:
        """将过期任务归档到 SQLite"""
        cutoff = time.time() - (days_ago * 86400)
        idx_key = f"qtask:hist:{queue_full_name}"
        task_key_prefix = "qtask:task:"

        total_archived = 0
        offset = 0

        while True:
            # 获取一批过期任务 ID（live 任务不删除，需用 offset 跳过已检查的窗口）
            task_ids = self.r.zrangebyscore(idx_key, "-inf", cutoff, start=offset, num=batch_size)
            if not task_ids:
                break

            # 分日期处理，因为可能跨天
            # 为简化，直接按当前归档执行日期分包，或根据任务 created_at 分包
            # 这里选择根据任务 created_at 时间戳决定写入哪个 SQLite 文件

            # 单键的类型与内容原子读取，批量 pipeline 不受在线格式转换影响。
            pipe = self.r.pipeline()
            for tid in task_ids:
                pipe.eval(READ_TASK_RECORD_LUA, 1, f"{task_key_prefix}{tid}")
            snapshots = pipe.execute()
            raw_tasks: List[Optional[Dict[str, Any]]] = []
            stale_task_ids = []

            for j, snapshot in enumerate(snapshots):
                data = decode_task_record(str(task_ids[j]), snapshot)
                if not data:
                    raw_tasks.append(None)
                    stale_task_ids.append(task_ids[j])
                    continue

                raw_tasks.append(data)

            # 合并写入
            db_sessions: Dict[str, list] = {}  # date_str -> rows

            for i, raw in enumerate(raw_tasks):
                if not raw:
                    continue
                # 只有终态且不再有 operational message 的任务可以归档。
                outcome = str(raw.get("outcome") or raw.get("status") or "")
                if outcome not in self.TERMINAL_STATUSES:
                    continue
                if str(raw.get("operational_message", "0")).lower() in {"1", "true"}:
                    continue
                tid = task_ids[i]
                if self._has_operational_message(queue_full_name, str(tid)):
                    continue
                finished_at = float(
                    raw.get("finished_at")
                    or raw.get("updated_at")
                    or raw.get("created_at", time.time())
                )
                if finished_at >= cutoff:
                    continue

                # 解析字段
                created_at = float(raw.get("created_at", time.time()))
                date_str = datetime.fromtimestamp(created_at).strftime("%Y%m%d")

                if date_str not in db_sessions:
                    db_sessions[date_str] = []

                db_sessions[date_str].append(
                    (
                        tid,
                        queue_full_name,
                        raw.get("action", ""),
                        outcome,
                        self._sqlite_json(raw.get("payload", "{}")),
                        self._sqlite_json(raw.get("result", "{}")),
                        created_at,
                        float(raw.get("updated_at", created_at)),
                        json.dumps(raw),
                    )
                )

            # 执行写入 SQLite 并从 Redis 删除
            for date_str, rows in db_sessions.items():
                db_path = self._get_db_path(date_str)
                self._init_db(db_path)

                with sqlite3.connect(db_path) as conn:
                    conn.executemany(
                        """
                        INSERT OR REPLACE INTO task_history 
                        (task_id, queue_name, action, status, payload, result, created_at, updated_at, raw_data)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                        rows,
                    )

                # 写入成功后，从 Redis 删除
                delete_pipe = self.r.pipeline()
                delete_pipe.zrem(idx_key, *[row[0] for row in rows])
                for row in rows:
                    delete_pipe.delete(f"{task_key_prefix}{row[0]}")
                delete_pipe.execute()

                total_archived += len(rows)

            if stale_task_ids:
                self.r.zrem(idx_key, *stale_task_ids)
                logger.info(
                    f"Removed {len(stale_task_ids)} stale history index entries for "
                    f"{queue_full_name}"
                )

            # 本批有删除（归档或 stale）时剩余条目会前移，从头继续扫描；
            # 全部是 live 任务时跳过该窗口，避免同一批反复扫描
            if db_sessions or stale_task_ids:
                offset = 0
            else:
                offset += len(task_ids)

            if len(task_ids) < batch_size:
                break

        return total_archived

    @staticmethod
    def _sqlite_json(value: Any) -> str | None:
        """将结构化历史转为 SQLite 可绑定的 JSON 文本，保留旧文本字段。"""
        if value is None or isinstance(value, str):
            return value
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))

    def _has_operational_message(self, queue_name: str, task_id: str) -> bool:
        """兼容旧记录：归档前以容器再次确认消息已不存在。"""
        list_keys = [
            queue_name,
            f"{queue_name}:retry",
            f"{queue_name}:dlq",
            f"{queue_name}:processing",
            *self.r.scan_iter(f"{queue_name}:processing:*"),
        ]
        for key in list_keys:
            for raw_message in self.r.lrange(key, 0, -1):
                try:
                    data = json.loads(raw_message)
                except (json.JSONDecodeError, TypeError):
                    continue
                if isinstance(data, dict) and str(data.get("task_id") or "") == task_id:
                    return True
        for raw_message, _score in self.r.zscan_iter(f"{queue_name}:delay"):
            try:
                data = json.loads(raw_message)
            except (json.JSONDecodeError, TypeError):
                continue
            if isinstance(data, dict) and str(data.get("task_id") or "") == task_id:
                return True
        return False
