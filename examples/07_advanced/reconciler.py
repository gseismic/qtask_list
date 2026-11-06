"""股票抓取 Reconciler 示例。

Reconciler 周期性读取期望任务清单，重复执行也只会得到结构化 duplicate 结果；它不
直接操作 Redis key，也不删除未知任务，从而把“补齐任务”和“破坏性清理”分开。

用法::

    python examples/07_advanced/reconciler.py                  # 使用内置样例清单
    python examples/07_advanced/reconciler.py --manifest tasks.jsonl

清单为 JSON Lines，每行至少包含 ``action``、``payload``、``logical_key``，可选
``start_deadline_at``（带时区 ISO 8601）与 ``trace_id``；``#`` 开头的行被忽略。
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
from typing import Any, Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from qtask_list import QueueAdmin, TaskSpec  # noqa: E402


REDIS_URL = "redis://localhost:6379/0"
QUEUE_NAME = "stockev_list:fetch"


def _deadline(value: str | None, now: datetime) -> datetime:
    """解析清单中的截止时间；缺省值给出一个短而明确的执行窗口。"""

    if value is None:
        # 缺省给 7 分钟窗口：明确且足够短，错过就等下轮 Reconciler
        return now + timedelta(minutes=7)
    parsed = datetime.fromisoformat(value)
    # 无时区的时间语义不明，直接拒绝而不是猜测
    if parsed.tzinfo is None:
        raise ValueError("start_deadline_at must include a timezone")
    return parsed


def load_specs(path: Path | None, *, now: datetime) -> list[TaskSpec]:
    """读取 JSONL 期望清单；没有文件时生成一个可直接试跑的样例。"""

    if path is None:
        # 内置样例：清单缺省时用来演示幂等补齐，不会破坏现有数据
        rows: Iterable[dict[str, Any]] = (
            {
                "action": "fetch_quote",
                "payload": {"symbol": symbol},
                "logical_key": f"reconcile:quote:{symbol}:{now.date().isoformat()}",
            }
            for symbol in ("AAPL", "TSLA", "NVDA")
        )
    else:
        rows = (
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        )

    specs: list[TaskSpec] = []
    for index, row in enumerate(rows, start=1):
        # 清单多由上游系统或人工生成，逐行校验而不是假设格式正确
        if not isinstance(row, dict):
            raise ValueError(f"manifest line {index} must be an object")
        action = row.get("action")
        payload = row.get("payload")
        logical_key = row.get("logical_key")
        if not isinstance(action, str) or not isinstance(payload, dict):
            raise ValueError(f"manifest line {index} needs action and object payload")
        if not isinstance(logical_key, str) or not logical_key:
            raise ValueError(f"manifest line {index} logical_key must be a non-empty string")
        specs.append(
            TaskSpec(
                action=action,
                payload=payload,
                logical_key=logical_key,
                scheduled_for=now,          # 期望清单表示「现在就该有」
                start_deadline_at=_deadline(row.get("start_deadline_at"), now),
                dedup_until=now + timedelta(days=2),
                trace_id=row.get("trace_id"),
            )
        )
    return specs


def reconcile(admin: QueueAdmin, queue_name: str, specs: list[TaskSpec]) -> None:
    """通过统一 Admin API 补齐清单，并输出 duplicate_of 供告警或审计使用。"""

    # enqueue_many 不会因重复而失败：结果里带 duplicate_of，供审计与告警判断
    results = admin.enqueue_many(queue_name, specs)
    stats = admin.queue_stats(queue_name)
    for result in results:
        print(result.as_dict())
    print({"queue": queue_name, "accepted": sum(r.accepted for r in results), "stats": stats})


def main() -> None:
    parser = argparse.ArgumentParser(description="按 JSONL 清单幂等补齐股票任务")
    parser.add_argument("--redis-url", default=REDIS_URL)
    parser.add_argument("--queue", default=QUEUE_NAME)
    parser.add_argument("--manifest", type=Path)
    args = parser.parse_args()

    now = datetime.now(timezone.utc)
    admin = QueueAdmin(args.redis_url)  # 管理接口：调用方无需拼 Redis key
    reconcile(admin, args.queue, load_specs(args.manifest, now=now))


if __name__ == "__main__":
    main()
