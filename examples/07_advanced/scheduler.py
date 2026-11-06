"""股票行情调度示例。

调度器由 cron/systemd timer 每 5 分钟运行一次。slot 由 UTC 时间向下取整得到，
因此同一时间窗口重复运行只会命中同一个 ``logical_key``，不会重复投递。

设计要点：
- 身份用「计划时刻」构造，不用进程启动的实际时间；cron 抖动或补跑都会命中同一身份；
- universe（列表发现）与 quote（行情采样）分属不同身份层级；
- 调度进程崩溃不会断链：下一轮 cron 重新投递同一时间桶，被去重即视为成功。
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
from typing import Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from qtask_list import EnqueueResult, SmartQueue, TaskSpec  # noqa: E402


REDIS_URL = "redis://localhost:6379/0"
NAMESPACE = "stockev_list"
WATCHLIST = ("AAPL", "TSLA", "NVDA", "MSFT", "GOOG")


def floor_to_slot(now: datetime, minutes: int = 5) -> datetime:
    """把带时区时间规整到固定分钟 slot，避免调度进程启动秒数影响身份键。"""

    # 身份里不允许出现本地时间：跨时区部署时必须显式带时区
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    if isinstance(minutes, bool) or not isinstance(minutes, int) or minutes <= 0 or 60 % minutes:
        raise ValueError("minutes must be a positive integer divisor of 60")
    now = now.astimezone(timezone.utc)
    return now.replace(
        minute=(now.minute // minutes) * minutes,  # 向下取整到 5 分钟边界
        second=0,
        microsecond=0,
    )


def build_specs(
    now: datetime,
    *,
    session: str,
    symbols: Iterable[str] = WATCHLIST,
) -> list[TaskSpec]:
    """为早盘/午盘窗口创建 universe 和行情任务。"""

    if session not in {"am", "pm"}:
        raise ValueError("session must be 'am' or 'pm'")
    slot = floor_to_slot(now)                   # 计划时刻（不是实际投递时刻）
    bucket = slot.strftime("%Y%m%dT%H%MZ")      # 编进身份的时间桶
    trade_date = slot.date().isoformat()
    deadline = slot + timedelta(minutes=7)      # 桶结束后 7 分钟内必须开始
    retained_until = slot + timedelta(days=2)   # 终态后 2 天内同键仍去重
    selected_symbols = tuple(symbols)

    specs = [
        # 列表发现任务：每半天一个身份，而不是每次 cron 一个
        TaskSpec(
            action="discover_universe",
            payload={
                "session": session,
                "trade_date": trade_date,
                "symbols": list(selected_symbols),
            },
            logical_key=f"universe:{trade_date}-{session}",
            scheduled_for=slot,
            start_deadline_at=deadline,
            dedup_until=retained_until,
        )
    ]
    specs.extend(
        # 行情采样任务：每只股票每个 5 分钟桶一个身份
        TaskSpec(
            action="fetch_quote",
            payload={
                "symbol": symbol,
                "trade_date": trade_date,
                "slot": bucket,
                "partition": f"quotes/trade_date={trade_date}",
                "session": session,
            },
            logical_key=f"quote:{symbol}:{bucket}",
            scheduled_for=slot,
            start_deadline_at=deadline,
            dedup_until=retained_until,
        )
        for symbol in selected_symbols
    )
    return specs


def main() -> None:
    parser = argparse.ArgumentParser(description="投递确定性 5 分钟股票行情任务")
    parser.add_argument("--redis-url", default=REDIS_URL)
    parser.add_argument("--namespace", default=NAMESPACE)
    parser.add_argument("--session", choices=("am", "pm"), default="am")
    args = parser.parse_args()

    queue = SmartQueue(args.redis_url, "fetch", namespace=args.namespace)
    specs = build_specs(datetime.now(timezone.utc), session=args.session)
    # duplicate 是正常输出而非错误：说明该时间桶已被本轮或上一轮 cron 投递过
    results: list[EnqueueResult] = queue.enqueue_many(specs)
    accepted = sum(result.accepted for result in results)
    duplicates = len(results) - accepted
    print(
        f"slot={specs[0].scheduled_for.isoformat()} session={args.session} "
        f"accepted={accepted} duplicates={duplicates}"
    )
    for result in results:
        print(result.as_dict())  # 结构化输出，便于日志采集与告警


if __name__ == "__main__":
    main()
