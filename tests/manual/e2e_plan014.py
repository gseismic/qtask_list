"""在私有 Redis 中验证 Worker 的重试、截止时间及 fan-out。"""

from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from _isolated_redis import isolated_redis  # noqa: E402
from qtask_list import SmartQueue, TaskContext, TaskSpec, Worker  # noqa: E402


def main() -> None:
    with isolated_redis() as (url, root):
        results: list[tuple[str, int]] = []
        news_results: list[str] = []
        failures = {"AAPL": 0}
        quote_queue = SmartQueue(url, "quote", namespace="e2e", max_attempts=3)
        news_queue = SmartQueue(url, "news", namespace="e2e")
        worker = Worker(url, "quote", namespace="e2e", max_workers=2,
                        retry_backoff_base=0.05, retry_backoff_max=0.1,
                        archive_dir=str(root / "archives"))
        news_worker = Worker(url, "news", namespace="e2e", archive_dir=str(root / "archives"))

        @worker.on("fetch_quote")
        def fetch_quote(task: dict, context: TaskContext) -> None:
            symbol = task["symbol"]
            if symbol == "AAPL" and failures["AAPL"] < 2:
                failures["AAPL"] += 1
                raise ConnectionError("模拟数据源故障")
            results.append((symbol, context.attempt))

        @worker.on("discover")
        def discover(_task: dict) -> None:
            news_queue.enqueue_many([
                TaskSpec("fetch_news", {"url": url}, logical_key=f"news:{url}")
                for url in ("https://x/a", "https://x/b")
            ])

        @news_worker.on("fetch_news")
        def fetch_news(task: dict) -> None:
            news_results.append(task["url"])

        # 先投递再启动 Worker，确保同键重复检查不受消费速度影响。
        first = quote_queue.enqueue(TaskSpec("fetch_quote", {"symbol": "TSLA"}, logical_key="TSLA"))
        duplicate = quote_queue.enqueue(TaskSpec("fetch_quote", {"symbol": "TSLA"}, logical_key="TSLA"))
        assert first.accepted and not duplicate.accepted
        quote_queue.enqueue(TaskSpec("fetch_quote", {"symbol": "AAPL"}, logical_key="AAPL"))
        expired_id = quote_queue.enqueue(TaskSpec(
            "fetch_quote", {"symbol": "MSFT"}, logical_key="MSFT",
            start_deadline_at=datetime.now(timezone.utc) - timedelta(seconds=1),
        )).task_id
        quote_queue.enqueue(TaskSpec("discover", {}))

        threads = [threading.Thread(target=item.run, daemon=True) for item in (worker, news_worker)]
        for thread in threads:
            thread.start()
        try:
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                record = quote_queue.history.get(expired_id)
                if (record and record.get("outcome") == "skipped" and
                        ("TSLA", 1) in results and ("AAPL", 3) in results and len(news_results) == 2):
                    break
                time.sleep(0.05)
        finally:
            worker.stop("manual_verification")
            news_worker.stop("manual_verification")
            for thread in threads:
                thread.join(timeout=5)

        assert all(not thread.is_alive() for thread in threads), "Worker 未正常退出"
        assert ("TSLA", 1) in results
        assert ("AAPL", 3) in results and failures["AAPL"] == 2
        assert not any(symbol == "MSFT" for symbol, _ in results)
        assert quote_queue.history.get(expired_id)["outcome"] == "skipped"
        assert sorted(news_results) == ["https://x/a", "https://x/b"]
        assert quote_queue.dlq_size() == 0 and news_queue.dlq_size() == 0
        print(f"端到端验证通过：quote={results} news={news_results}")


if __name__ == "__main__":
    main()
