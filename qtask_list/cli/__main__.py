from __future__ import annotations

import importlib
import ipaddress
import json
import os
import threading
import time
import webbrowser
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, cast

import redis
import typer
from rich.console import Console
from rich.table import Table

from qtask_list.security import redis_endpoint_label
from qtask_list.storage import RemoteStorage

if TYPE_CHECKING:
    from qtask_list import QueueAdmin, Worker

try:
    from qtask_list import QueueAdmin, Worker  # noqa: F811

    QTASK_LIST_AVAILABLE = True
except ImportError:
    QTASK_LIST_AVAILABLE = False


DEFAULT_REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")


class QueueState(str, Enum):
    ready = "ready"
    processing = "processing"
    retry = "retry"
    retry_wait = "retry_wait"
    dlq = "dlq"
    delay = "delay"
    completed = "completed"
    failed = "failed"
    skipped = "skipped"
    cancelled = "cancelled"
    deadline_missed = "deadline_missed"


app = typer.Typer()
task_app = typer.Typer(help="查看和操作单个任务")
app.add_typer(task_app, name="task")
console = Console()


def get_redis(redis_url: str = DEFAULT_REDIS_URL) -> Any:
    """获取 Redis 连接"""
    return redis.from_url(redis_url, decode_responses=True)


def normalize_queue_name(queue_name: str, namespace: Optional[str] = None) -> str:
    if namespace and ":" not in queue_name:
        return f"{namespace}:{queue_name}"
    return queue_name


def admin_from_url(redis_url: str) -> QueueAdmin:
    storage_url = os.environ.get("QTASK_STORAGE_URL", "")
    storage = RemoteStorage(storage_url) if storage_url else None
    return QueueAdmin(redis_url=redis_url, storage=storage)


def json_dumps(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2, default=str)


def payload_summary(payload: Any, max_length: int = 100) -> str:
    raw = json.dumps(payload, ensure_ascii=False, default=str)
    if len(raw) <= max_length:
        return raw
    return raw[: max_length - 3] + "..."


def cli_task_row(item: Dict[str, Any]) -> Dict[str, Any]:
    row = dict(item)
    row.setdefault("state", row.get("_state", ""))
    row.setdefault("source", row.get("_source", ""))
    if "_raw" in row:
        row.setdefault("raw", row["_raw"])
    payload = row.get("payload")
    if isinstance(payload, dict):
        row.setdefault("retry", payload.get("_retry", ""))
    return row


def queue_has_problem(stats: Dict[str, Any]) -> bool:
    """按值班可行动的信号判断队列是否需要查看。"""
    return any(int(stats.get(key, 0)) > 0 for key in ("dlq", "deadline_missed", "stale_workers")) or (
        int(stats.get("queue", 0)) > 0 and int(stats.get("active_workers", 0)) == 0
    )


def is_loopback_host(host: str) -> bool:
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def print_message_rows(rows: List[Dict[str, Any]], title: str):
    if not rows:
        console.print("[yellow]No tasks found[/yellow]")
        return

    table = Table(title=title)
    table.add_column("State", style="cyan")
    table.add_column("Source", style="blue")
    table.add_column("Task ID", style="green", no_wrap=True)
    table.add_column("Action", style="magenta")
    table.add_column("Retry", justify="right")
    table.add_column("Payload")

    for item in rows:
        task_id = str(item.get("task_id") or "")
        table.add_row(
            str(item.get("state", "")),
            str(item.get("source", "")),
            task_id[:12] + ("..." if len(task_id) > 12 else ""),
            str(item.get("action", "")),
            str(item.get("retry", "")),
            payload_summary(item.get("payload")),
        )
    console.print(table)


def load_payload(payload: Optional[str], payload_file: Optional[Path]) -> Dict[str, Any]:
    if payload and payload_file:
        raise typer.BadParameter("Use either PAYLOAD or --file, not both")
    if payload_file:
        raw = payload_file.read_text(encoding="utf-8")
    elif payload:
        raw = payload
    else:
        raise typer.BadParameter("A JSON payload or --file is required")

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"Payload must be valid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise typer.BadParameter("Payload must be a JSON object")
    return cast(Dict[str, Any], parsed)


def load_worker_from_module(module_spec: str) -> Worker:
    module_name, attr_name = (
        module_spec.split(":", 1) if ":" in module_spec else (module_spec, "worker")
    )
    module = importlib.import_module(module_name)
    loaded = getattr(module, attr_name)
    if not isinstance(loaded, Worker):
        raise typer.BadParameter(f"{module_spec} must resolve to a qtask_list.Worker instance")
    return loaded


@app.command("rebuild-observation")
def rebuild_observation(
    queue_name: str = typer.Argument(..., help="需要回填的旧队列名称"),
    redis_url: str = typer.Option(DEFAULT_REDIS_URL, "--redis", help="Redis URL"),
):
    """维护窗口内回填旧队列的截止时间和重试等待索引。"""
    try:
        result = admin_from_url(redis_url).rebuild_observation_indexes(queue_name)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print(json_dumps(result))


@app.command()
def status(
    queue_name: Optional[str] = typer.Argument(None, help="队列名称，如 stockev:fetch"),
    namespace: Optional[str] = typer.Option(None, "--namespace", "-n", help="指定队列补前缀，或筛选全部队列"),
    redis_url: str = typer.Option(DEFAULT_REDIS_URL, "--redis", help="Redis URL"),
    problems_only: bool = typer.Option(False, "--problems-only", help="仅列出需处理的队列（只用于全局状态）"),
    json_output: bool = typer.Option(False, "--json", help="输出完整状态 JSON"),
):
    """查看队列状态"""
    admin = admin_from_url(redis_url)

    if queue_name:
        if problems_only:
            raise typer.BadParameter("--problems-only 仅用于不指定队列的全局状态")
        queue_name = normalize_queue_name(queue_name, namespace)
        try:
            stats = admin.queue_stats(queue_name)
        except redis.RedisError as exc:
            console.print(f"[red]Redis 不可用：{redis_endpoint_label(redis_url)}[/red]")
            raise typer.Exit(1) from exc

        if json_output:
            console.print_json(json_dumps({"name": queue_name, **stats}))
            return

        table = Table(title=f"Queue: {queue_name}")
        table.add_column("Status", style="cyan")
        table.add_column("Count", style="green", justify="right")

        for key, value in stats.items():
            table.add_row(key, str(value))

        console.print(table)
        return

    try:
        queue_rows = admin.list_queues()
    except redis.RedisError as exc:
        console.print(f"[red]Redis 不可用：{redis_endpoint_label(redis_url)}[/red]")
        raise typer.Exit(1) from exc
    if namespace:
        queue_rows = [row for row in queue_rows if str(row["name"]).startswith(f"{namespace}:")]
    if problems_only:
        queue_rows = [row for row in queue_rows if queue_has_problem(row)]

    if json_output:
        console.print_json(json_dumps(queue_rows))
        return

    if not queue_rows:
        console.print("[yellow]No matching queues found[/yellow]")
        return

    console.print("R=ready P=processing Rt=legacy retry Rw=retry_wait D=delay; Due=overdue; W=online/stale")
    table = Table(title="All Queues", padding=(0, 1))
    table.add_column("Queue", style="cyan", no_wrap=True)
    table.add_column("R/P/Rt/Rw/D", justify="right", no_wrap=True)
    table.add_column("DLQ", style="red", justify="right")
    table.add_column("Due", style="red", justify="right")
    table.add_column("W", justify="right")

    total = {
        "queue": 0, "processing": 0, "retry": 0, "retry_wait": 0,
        "dlq": 0, "delay": 0, "deadline_missed": 0,
        "active_workers": 0, "stale_workers": 0,
    }

    for stats in queue_rows:
        table.add_row(
            str(stats["name"]),
            f"{stats['queue']}/{stats['processing']}/{stats['retry']}/{stats['retry_wait']}/{stats['delay']}",
            str(stats["dlq"]),
            str(stats["deadline_missed"]),
            f"{stats['active_workers']}/{stats['stale_workers']}",
        )
        for k in total:
            total[k] += stats[k]

    table.add_row(
        "[bold]Total[/bold]",
        f"[bold]{total['queue']}/{total['processing']}/{total['retry']}/{total['retry_wait']}/{total['delay']}[/bold]",
        f"[bold]{total['dlq']}[/bold]",
        f"[bold]{total['deadline_missed']}[/bold]",
        f"[bold]{total['active_workers']}/{total['stale_workers']}[/bold]",
    )

    console.print(table)


@app.command()
def push(
    queue_name: str = typer.Argument(..., help="队列名称"),
    payload: Optional[str] = typer.Argument(None, help='JSON payload，如 {"action":"fetch"}'),
    payload_file: Optional[Path] = typer.Option(None, "--file", "-f", help="从 JSON 文件读取 payload"),
    delay_seconds: int = typer.Option(0, "--delay", "-d", help="延迟执行秒数"),
    expire_seconds: int = typer.Option(0, "--expire", help="相对开始截止秒数"),
    action: Optional[str] = typer.Option(None, "--action", help="handler action，默认读取 payload.action"),
    logical_key: Optional[str] = typer.Option(None, "--logical-key", help="业务幂等身份"),
    scheduled_for: Optional[str] = typer.Option(None, "--scheduled-for", help="计划时刻 ISO 8601"),
    not_before_at: Optional[str] = typer.Option(None, "--not-before", help="最早开始时刻 ISO 8601"),
    start_deadline_at: Optional[str] = typer.Option(None, "--start-deadline", help="最晚开始时刻 ISO 8601"),
    dedup_until: Optional[str] = typer.Option(None, "--dedup-until", help="终态后重复抑制边界 ISO 8601"),
    trace_id: Optional[str] = typer.Option(None, "--trace-id", help="跨任务追踪 ID"),
    parent_task_id: Optional[str] = typer.Option(None, "--parent-task-id", help="fan-out 父任务 ID"),
    concurrency_key: Optional[str] = typer.Option(None, "--concurrency-key", help="同键串行 lease"),
    supersede_key: Optional[str] = typer.Option(None, "--supersede-key", help="latest-only 身份"),
    supersede_version: Optional[str] = typer.Option(None, "--supersede-version", help="latest-only 版本"),
    allow_new: bool = typer.Option(False, "--allow-new", help="允许覆盖现有 logical owner"),
    force_duplicate: bool = typer.Option(False, "--force-duplicate", help="确认危险的同键新实例"),
    namespace: Optional[str] = typer.Option(None, "--namespace", "-n", help="命名空间"),
    redis_url: str = typer.Option(DEFAULT_REDIS_URL, "--redis", help="Redis URL"),
    json_output: bool = typer.Option(False, "--json", help="输出 JSON"),
):
    """投递一个任务"""
    if not QTASK_LIST_AVAILABLE:
        console.print("[red]Error: qtask_list not installed. Run: pip install qtask_list[/red]")
        raise typer.Exit(1)

    payload_data = load_payload(payload, payload_file)
    queue_name = normalize_queue_name(queue_name, namespace)
    if allow_new and not force_duplicate:
        console.print("[red]--allow-new 会替换 logical owner；请同时传 --force-duplicate[/red]")
        raise typer.Exit(2)
    parsed_supersede_version: int | str | None = supersede_version
    if supersede_version is not None:
        try:
            parsed_supersede_version = int(supersede_version)
        except ValueError:
            parsed_supersede_version = supersede_version
    result = admin_from_url(redis_url).push_task(
        queue_name,
        payload_data,
        delay_seconds=delay_seconds,
        expire_seconds=expire_seconds,
        action=action,
        logical_key=logical_key,
        scheduled_for=scheduled_for,
        not_before_at=not_before_at,
        start_deadline_at=start_deadline_at,
        dedup_until=dedup_until,
        trace_id=trace_id,
        parent_task_id=parent_task_id,
        concurrency_key=concurrency_key,
        supersede_key=supersede_key,
        supersede_version=parsed_supersede_version,
        on_duplicate="allow_new" if allow_new else "reject",
    )
    task_id = result["task_id"]

    if json_output:
        console.print_json(json_dumps(result))
    else:
        if result["accepted"]:
            console.print(f"[green]Pushed task {task_id} to {result['queue']}[/green]")
        else:
            console.print(
                f"[yellow]Not enqueued: {result['reason']} duplicate_of={result['duplicate_of']}[/yellow]"
            )


@app.command()
def peek(
    queue_name: str = typer.Argument(..., help="队列名称"),
    state: QueueState = typer.Option(QueueState.ready, "--state", "-s", help="队列状态"),
    limit: int = typer.Option(20, "--limit", "-l", help="显示条数"),
    namespace: Optional[str] = typer.Option(None, "--namespace", "-n", help="命名空间"),
    redis_url: str = typer.Option(DEFAULT_REDIS_URL, "--redis", help="Redis URL"),
    json_output: bool = typer.Option(False, "--json", help="输出 JSON"),
):
    """查看队列中的实际消息，不移动任务"""
    queue_name = normalize_queue_name(queue_name, namespace)
    rows = [
        cli_task_row(item)
        for item in admin_from_url(redis_url).list_tasks(queue_name, state=state.value, limit=limit)
    ]

    if json_output:
        console.print_json(json_dumps(rows))
    else:
        print_message_rows(rows, f"{queue_name} [{state.value}]")


@app.command()
def clear(
    queue_name: str = typer.Argument(..., help="队列名称"),
    include_dlq: bool = typer.Option(True, "--include-dlq/--no-dlq", help="是否包含 DLQ"),
    include_history: bool = typer.Option(False, "--include-history", help="是否包含任务历史"),
    include_processing: bool = typer.Option(
        False, "--include-processing", help="同时清理失联 Worker 的 processing；活跃 Worker 受保护"
    ),
    release_identity: bool = typer.Option(
        False,
        "--release-identity",
        help="同时释放 logical identity（危险；默认保留终态抑制边界）",
    ),
    namespace: Optional[str] = typer.Option(None, "--namespace", "-n", help="命名空间"),
    redis_url: str = typer.Option(DEFAULT_REDIS_URL, "--redis", help="Redis URL"),
    force: bool = typer.Option(False, "--force", "-f", help="强制执行"),
):
    """清空队列"""
    queue_name = normalize_queue_name(queue_name, namespace)
    if not force:
        console.print(f"[red]WARNING: This will clear queue {queue_name}[/red]")
        if include_history:
            console.print("[red]History records for this queue will also be removed[/red]")
        if release_identity:
            console.print("[red]Logical identity owners will be released[/red]")
        if include_processing:
            console.print("[red]Stale processing messages will also be removed[/red]")
        if not typer.confirm("Continue?"):
            raise typer.Abort()

    try:
        result = admin_from_url(redis_url).clear_queue(
            queue_name,
            include_dlq=include_dlq,
            include_history=include_history,
            identity_policy="release" if release_identity else "keep",
            include_processing=include_processing,
        )
    except ValueError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(1) from exc
    history_count = result["history_records"]

    suffix = f" and {history_count} history records" if include_history else ""
    console.print(f"[green]Cleared {queue_name}{suffix}[/green]")


@app.command()
def requeue(
    queue_name: str = typer.Argument(..., help="队列名称"),
    task_id: Optional[str] = typer.Option(None, "--task-id", "-t", help="只重新入队一个任务"),
    namespace: Optional[str] = typer.Option(None, "--namespace", "-n", help="命名空间"),
    redis_url: str = typer.Option(DEFAULT_REDIS_URL, "--redis", help="Redis URL"),
    force: bool = typer.Option(False, "--force", "-f", help="强制执行"),
    keep_retry: bool = typer.Option(
        False, "--keep-retry", help="保留原有重试计数（默认重置，人工重放视为全新尝试）"
    ),
):
    """以新 task_id replay DLQ 任务；旧实例保持 failed。"""
    queue_name = normalize_queue_name(queue_name, namespace)
    if not force:
        target = f"task {task_id}" if task_id else "all tasks"
        console.print(f"[red]WARNING: This will requeue {target} from DLQ of {queue_name}[/red]")
        if not typer.confirm("Continue?"):
            raise typer.Abort()

    admin = admin_from_url(redis_url)
    if task_id:
        replay_result = admin.requeue_dlq(
            queue_name,
            task_id,
            reset_retry=not keep_retry,
        )
        moved = int(replay_result["moved"])
        if moved:
            new_task_id = replay_result.get("task_id")
            console.print(f"[green]Replayed task {task_id} as {new_task_id}[/green]")
        else:
            console.print(f"[yellow]Task {task_id} not found in DLQ[/yellow]")
            raise typer.Exit(1)
        return

    result = admin.requeue_dlq(queue_name, reset_retry=not keep_retry)
    count = result["moved"]
    if result.get("skipped") or result.get("not_attempted"):
        console.print(f"[yellow]已处理 {count} 条，跳过 {result.get('skipped', 0)} 条，"
                      f"未尝试 {result.get('not_attempted', 0)} 条[/yellow]")
        for error in result.get("errors", []):
            console.print(f"{error.get('task_id') or '未知任务'}：{error['reason']}", markup=False)
        raise typer.Exit(1)
    console.print(f"[green]Requeued {count} tasks from DLQ[/green]")


@app.command()
def retry(
    queue_name: str = typer.Argument(..., help="队列名称"),
    namespace: Optional[str] = typer.Option(None, "--namespace", "-n", help="命名空间"),
    redis_url: str = typer.Option(DEFAULT_REDIS_URL, "--redis", help="Redis URL"),
):
    """将 retry 队列中的任务移回主队列"""
    queue_name = normalize_queue_name(queue_name, namespace)
    count = admin_from_url(redis_url).move_retry(queue_name)["moved"]
    console.print(f"[green]Moved {count} tasks from retry to main queue[/green]")


@app.command()
def recover(
    queue_name: str = typer.Argument(..., help="队列名称"),
    namespace: Optional[str] = typer.Option(None, "--namespace", "-n", help="命名空间"),
    redis_url: str = typer.Option(DEFAULT_REDIS_URL, "--redis", help="Redis URL"),
    force_active: bool = typer.Option(
        False,
        "--force-active",
        help="也恢复仍有 heartbeat 的 active worker processing 队列",
    ),
    yes: bool = typer.Option(False, "--yes", "-y", help="确认强制恢复活跃 Worker"),
):
    """恢复 processing 队列中的任务，默认只恢复 stale worker"""
    queue_name = normalize_queue_name(queue_name, namespace)
    if force_active and not yes:
        console.print("[red]WARNING: active handler 可能仍在产生外部副作用[/red]")
        if not typer.confirm("Force recover active workers?"):
            raise typer.Abort()
    result = admin_from_url(redis_url).recover(queue_name, include_active=force_active)
    count = result["recovered"]
    skipped = result["skipped_active"]
    skipped_legacy = result.get("skipped_legacy", 0)

    if force_active:
        console.print(f"[green]Recovered {count} tasks from all processing queues[/green]")
        return

    console.print(f"[green]Recovered {count} tasks from stale processing queues[/green]")
    if skipped:
        console.print(f"[yellow]Skipped {skipped} active processing tasks[/yellow]")
    if skipped_legacy:
        console.print(
            "[yellow]Skipped legacy processing tasks; use --force-active only after "
            "confirming no direct SmartQueue consumer is still working[/yellow]"
        )


@app.command()
def history(
    queue_name: Optional[str] = typer.Argument(None, help="队列名称"),
    limit: int = typer.Option(20, "--limit", "-l", help="显示条数"),
    task_id: Optional[str] = typer.Option(None, "--task-id", "-t", help="查看特定任务"),
    namespace: Optional[str] = typer.Option(None, "--namespace", "-n", help="命名空间"),
    redis_url: str = typer.Option(DEFAULT_REDIS_URL, "--redis", help="Redis URL"),
):
    """查看任务历史记录"""
    if task_id:
        data = admin_from_url(redis_url).get_task(task_id)
        if data:
            console.print_json(json_dumps(data))
        else:
            console.print(f"[yellow]Task {task_id} not found[/yellow]")
        return

    if not queue_name:
        console.print("[red]QUEUE_NAME is required unless --task-id is provided[/red]")
        raise typer.Exit(1)

    queue_name = normalize_queue_name(queue_name, namespace)
    tasks = admin_from_url(redis_url).list_tasks(queue_name, state="history", limit=limit)

    if not tasks:
        console.print("[yellow]No history found[/yellow]")
        return

    table = Table(title=f"History: {queue_name}")
    table.add_column("Task ID", style="cyan", no_wrap=True)
    table.add_column("Action", style="green")
    table.add_column("Status", style="yellow")
    table.add_column("Created", style="blue")

    for data in tasks:
        tid = str(data.get("task_id", "unknown"))
        created = datetime.fromtimestamp(data.get("created_at", 0)).strftime("%H:%M:%S")
        status_style = {
            "pending": "yellow",
            "completed": "green",
            "failed": "red",
            "retry": "magenta",
        }.get(data.get("status", ""), "white")

        table.add_row(
            tid[:8] + "...",
            str(data.get("action", "")),
            f"[{status_style}]{data.get('status', '')}[/]",
            created,
        )

    console.print(table)


@app.command()
def watch(
    queue_name: str = typer.Argument(..., help="队列名称"),
    namespace: Optional[str] = typer.Option(None, "--namespace", "-n", help="命名空间"),
    interval: int = typer.Option(2, "--interval", "-i", min=1, help="刷新间隔(秒)，至少 1 秒"),
    redis_url: str = typer.Option(DEFAULT_REDIS_URL, "--redis", help="Redis URL"),
):
    """实时监控队列"""
    queue_name = normalize_queue_name(queue_name, namespace)

    admin = admin_from_url(redis_url)
    console.print(f"[green]Watching {queue_name} (Ctrl+C to exit)[/green]")

    try:
        while True:
            stats = admin.queue_stats(queue_name)
            console.clear()
            console.print(f"[cyan]Queue: {queue_name}[/cyan]")
            console.print(f"  Queue:        {stats['queue']}")
            console.print(f"  Processing:  {stats['processing']}")
            console.print(f"  Retry:        {stats['retry']}")
            console.print(f"  Retry wait:   {stats['retry_wait']}")
            console.print(f"  DLQ:          {stats['dlq']}")
            console.print(f"  Delay:        {stats['delay']}")
            console.print(f"  Overdue:      {stats['deadline_missed']}")
            console.print(f"  Workers:      {stats['active_workers']} online / {stats['stale_workers']} stale")
            time.sleep(interval)
    except KeyboardInterrupt:
        console.print("\n[yellow]Stopped[/yellow]")
    except redis.RedisError as exc:
        console.print(f"[red]Redis 不可用：{redis_endpoint_label(redis_url)}[/red]")
        raise typer.Exit(1) from exc


@app.command()
def worker(
    module: str = typer.Option(
        ...,
        "--module",
        "-m",
        help="导入一个 qtask_list.Worker 实例，如 myapp.workers:worker",
    ),
):
    """启动用户代码中注册好 handler 的 Worker"""
    if not QTASK_LIST_AVAILABLE:
        console.print("[red]Error: qtask_list not installed. Run: pip install qtask_list[/red]")
        raise typer.Exit(1)

    loaded_worker = load_worker_from_module(module)
    console.print(f"[green]Starting worker from {module}[/green]")
    loaded_worker.run()


@app.command()
def clean_history(
    queue_name: Optional[str] = typer.Argument(None, help="队列名称，如 stockev_list:fetch"),
    namespace: Optional[str] = typer.Option(None, "--namespace", "-n", help="命名空间"),
    ttl_days: int = typer.Option(15, "--ttl-days", "-t", help="过期天数"),
    redis_url: str = typer.Option(DEFAULT_REDIS_URL, "--redis", help="Redis URL"),
):
    """清理过期历史记录"""
    if not QTASK_LIST_AVAILABLE:
        console.print("[red]Error: qtask_list not installed. Run: pip install qtask_list[/red]")
        raise typer.Exit(1)

    if queue_name:
        queue_name = normalize_queue_name(queue_name, namespace)
        count = admin_from_url(redis_url).clean_history(queue_name, ttl_days=ttl_days)["cleaned"]
        console.print(f"[green]Cleaned {count} expired history records from {queue_name}[/green]")
        return

    total = admin_from_url(redis_url).clean_history(ttl_days=ttl_days)["cleaned"]
    console.print(f"[green]Cleaned {total} expired history records from all queues[/green]")


@app.command()
def archive(
    queue_name: Optional[str] = typer.Argument(None, help="队列名称"),
    namespace: Optional[str] = typer.Option(None, "--namespace", "-n", help="命名空间"),
    days: int = typer.Option(1, "--days", "-d", help="归档几天前的数据"),
    redis_url: str = typer.Option(DEFAULT_REDIS_URL, "--redis", help="Redis URL"),
):
    """将历史记录归档到 SQLite"""
    from qtask_list.archiver import ArchiveManager

    archiver = ArchiveManager(redis_url)

    if queue_name:
        queue_name = normalize_queue_name(queue_name, namespace)
        count = archiver.archive_to_sqlite(queue_name, days_ago=days)
        console.print(f"[green]Archived {count} tasks from {queue_name} to SQLite[/green]")
        return

    r = get_redis(redis_url)
    total = 0
    for hist_key in r.scan_iter("qtask:hist:*"):
        q_full = hist_key.replace("qtask:hist:", "")
        count = archiver.archive_to_sqlite(q_full, days_ago=days)
        total += count
    console.print(f"[green]Archived {total} tasks from all queues to SQLite[/green]")


@app.command()
def monitor(
    redis_url: str = typer.Option(DEFAULT_REDIS_URL, "--redis", help="Redis URL"),
):
    """查看 Redis 内存监控信息"""
    from qtask_list.archiver import Monitor

    r = get_redis(redis_url)
    m = Monitor(r)
    info = m.get_memory_info()

    table = Table(title="Redis Memory Monitor")
    table.add_column("Property", style="cyan")
    table.add_column("Value", style="green")

    for k, v in info.items():
        table.add_row(k, str(v))

    console.print(table)


@app.command()
def dashboard(
    host: str = typer.Option("127.0.0.1", "--host", help="Dashboard 监听地址，远程访问可用 0.0.0.0"),
    port: int = typer.Option(8765, "--port", "-p", help="Dashboard 端口"),
    redis_url: str = typer.Option(DEFAULT_REDIS_URL, "--redis", help="Redis URL"),
    user: Optional[str] = typer.Option(None, "--user", help="Dashboard 登录用户名"),
    password: Optional[str] = typer.Option(None, "--password", help="Dashboard 登录密码，设置后启用登录"),
    session_ttl: int = typer.Option(86400, "--session-ttl", help="登录会话有效期，秒"),
    secure_cookie: bool = typer.Option(False, "--secure-cookie", help="HTTPS 部署时启用 Secure Cookie"),
    allow_unauthenticated: bool = typer.Option(
        False, "--allow-unauthenticated", help="显式允许无认证的远程访问（危险）"
    ),
    open_browser: bool = typer.Option(True, "--open/--no-open", help="启动后打开浏览器"),
):
    """启动 Dashboard 面板"""
    os.environ["REDIS_URL"] = redis_url
    os.environ["QTASK_DASHBOARD_SESSION_TTL"] = str(session_ttl)
    if user is not None:
        os.environ["QTASK_DASHBOARD_USER"] = user
    if password is not None:
        os.environ["QTASK_DASHBOARD_PASSWORD"] = password
    if secure_cookie:
        os.environ["QTASK_DASHBOARD_SECURE_COOKIE"] = "1"

    display_host = "localhost" if host in {"0.0.0.0", "::", "127.0.0.1"} else host
    auth_enabled = bool(os.environ.get("QTASK_DASHBOARD_PASSWORD")) or (
        os.environ.get("QTASK_DASHBOARD_AUTH", "").strip().lower() in {"1", "true", "yes", "on"}
    )
    if not is_loopback_host(host) and not auth_enabled and not allow_unauthenticated:
        console.print(
            "[red]拒绝无认证的远程 Dashboard：设置 --password / QTASK_DASHBOARD_PASSWORD，"
            "或显式传 --allow-unauthenticated[/red]"
        )
        raise typer.Exit(2)

    try:
        import uvicorn
        from qtask_list.dashboard.main import app
    except ImportError as exc:
        console.print("[red]缺少 Dashboard 依赖；请安装 pip install 'qtask_list[dashboard]'[/red]")
        raise typer.Exit(1) from exc

    console.print(f"[green]Starting Dashboard on http://{display_host}:{port}[/green]")
    console.print(f"[cyan]Redis: {redis_endpoint_label(redis_url)}[/cyan]")
    console.print(f"[cyan]Auth: {'enabled' if auth_enabled else 'disabled'}[/cyan]")
    console.print("Press Ctrl+C to stop\n")

    if open_browser:

        def open_browser_delayed():
            time.sleep(2)
            webbrowser.open(f"http://{display_host}:{port}")

        threading.Thread(target=open_browser_delayed, daemon=True).start()

    uvicorn.run(app, host=host, port=port, log_level="info")


@task_app.command("get")
def task_get(
    task_id: str = typer.Argument(..., help="任务 ID"),
    redis_url: str = typer.Option(DEFAULT_REDIS_URL, "--redis", help="Redis URL"),
):
    """查看一个任务的历史详情"""
    data = admin_from_url(redis_url).get_task(task_id)
    if not data:
        console.print(f"[yellow]Task {task_id} not found[/yellow]")
        raise typer.Exit(1)
    console.print_json(json_dumps(data))


@task_app.command("requeue")
def task_requeue(
    task_id: str = typer.Argument(..., help="任务 ID"),
    queue_name: str = typer.Option(..., "--queue", "-q", help="队列名称"),
    source_state: QueueState = typer.Option(QueueState.dlq, "--from", help="来源状态"),
    namespace: Optional[str] = typer.Option(None, "--namespace", "-n", help="命名空间"),
    redis_url: str = typer.Option(DEFAULT_REDIS_URL, "--redis", help="Redis URL"),
    force: bool = typer.Option(False, "--force", "-f", help="强制执行"),
):
    """将一个任务从指定状态移回 ready 队列"""
    if source_state == QueueState.ready:
        console.print("[red]--from ready is not a requeue operation[/red]")
        raise typer.Exit(1)

    queue_name = normalize_queue_name(queue_name, namespace)
    if not force:
        console.print(
            f"[red]WARNING: This will move task {task_id} "
            f"from {source_state.value} to ready in {queue_name}[/red]"
        )
        if not typer.confirm("Continue?"):
            raise typer.Abort()

    moved = int(admin_from_url(redis_url).requeue_task(queue_name, task_id, source_state.value)["moved"])
    if not moved:
        console.print(f"[yellow]Task {task_id} not found in {source_state.value}[/yellow]")
        raise typer.Exit(1)

    console.print(f"[green]Requeued task {task_id} from {source_state.value}[/green]")


@task_app.command("delete")
def task_delete(
    task_id: str = typer.Argument(..., help="任务 ID"),
    queue_name: Optional[str] = typer.Option(None, "--queue", "-q", help="限制在某个队列删除"),
    namespace: Optional[str] = typer.Option(None, "--namespace", "-n", help="命名空间"),
    redis_url: str = typer.Option(DEFAULT_REDIS_URL, "--redis", help="Redis URL"),
    force: bool = typer.Option(False, "--force", "-f", help="强制执行"),
    json_output: bool = typer.Option(False, "--json", help="输出 JSON"),
):
    """删除一个任务的队列消息和历史记录"""
    normalized_queue = normalize_queue_name(queue_name, namespace) if queue_name else None
    if not force:
        scope = normalized_queue or "all queues"
        console.print(f"[red]WARNING: This will delete task {task_id} from {scope}[/red]")
        if not typer.confirm("Continue?"):
            raise typer.Abort()

    try:
        result = admin_from_url(redis_url).delete_task(task_id, normalized_queue)
    except ValueError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(1) from exc
    if json_output:
        console.print_json(json_dumps(result))
    else:
        console.print(
            "[green]Deleted "
            f"{result['queue_messages']} queue messages, "
            f"{result['history_records']} history records, "
            f"{result['history_indexes']} history index entries[/green]"
        )


@task_app.command("replay")
def task_replay(
    task_id: str = typer.Argument(..., help="原 failed/skipped/cancelled 任务 ID"),
    queue_name: Optional[str] = typer.Option(None, "--queue", "-q", help="队列名称；默认从历史推导"),
    start_deadline_at: Optional[str] = typer.Option(
        None,
        "--start-deadline",
        help="新的最晚开始时刻 ISO 8601；旧 deadline 已过时必填",
    ),
    payload: Optional[str] = typer.Option(None, "--payload", help="替换 JSON payload"),
    payload_file: Optional[Path] = typer.Option(None, "--file", "-f", help="从文件读取替换 payload"),
    logical_key: Optional[str] = typer.Option(None, "--logical-key", help="替换 logical key"),
    action: Optional[str] = typer.Option(None, "--action", help="替换 action"),
    namespace: Optional[str] = typer.Option(None, "--namespace", "-n", help="命名空间"),
    redis_url: str = typer.Option(DEFAULT_REDIS_URL, "--redis", help="Redis URL"),
    force: bool = typer.Option(False, "--force", "-f", help="确认创建新执行实例"),
    json_output: bool = typer.Option(False, "--json", help="输出 JSON"),
):
    """以新 task_id 重放终态任务，原记录保持不变。"""
    normalized_queue = normalize_queue_name(queue_name, namespace) if queue_name else None
    if not force:
        console.print(f"[red]WARNING: replay will create a new task instance for {task_id}[/red]")
        if not typer.confirm("Continue?"):
            raise typer.Abort()
    replace_payload = payload is not None or payload_file is not None
    payload_data = load_payload(payload, payload_file) if replace_payload else None
    result = admin_from_url(redis_url).replay_task(
        task_id,
        queue_name=normalized_queue,
        start_deadline_at=start_deadline_at,
        payload=payload_data,
        logical_key=logical_key,
        replace_payload=replace_payload,
        replace_logical_key=logical_key is not None,
        action=action,
    )
    if json_output:
        console.print_json(json_dumps(result))
    elif result["accepted"]:
        console.print(
            f"[green]Replayed {task_id} as {result['task_id']} in {result['queue']}[/green]"
        )
    else:
        console.print(
            f"[yellow]Replay rejected: {result['reason']} duplicate_of={result['duplicate_of']}[/yellow]"
        )


@app.command()
def storage(
    port: int = typer.Option(8096, "--port", "-p", help="监听端口"),
    data_dir: str = typer.Option("", "--data-dir", "-d", help="数据目录，默认 ~/.qtask-storage"),
    host: str = typer.Option("127.0.0.1", "--host", help="监听地址；远程监听需设置 QTASK_STORAGE_TOKEN"),
    ttl_days: float = typer.Option(7.0, "--ttl-days", help="文件保留天数，0=永不过期"),
    gc_redis: str = typer.Option(DEFAULT_REDIS_URL, "--gc-redis", help="外存回收日志所在 Redis"),
):
    """启动 RemoteStorage 服务（大 payload 外存）"""
    try:
        from qtask_list.remote_storage import server as storage_server
    except ImportError as exc:
        console.print("[red]RemoteStorage 依赖未安装，请执行: pip install qtask_list[storage][/red]")
        raise typer.Exit(1) from exc

    import uvicorn

    if host not in {"127.0.0.1", "::1", "localhost"} and not os.environ.get(
        "QTASK_STORAGE_TOKEN"
    ):
        console.print("[red]远程监听需要设置 QTASK_STORAGE_TOKEN[/red]")
        raise typer.Exit(2)

    data_path = Path(data_dir) if data_dir else storage_server.DEFAULT_DIR
    storage_server.configure(data_path, ttl_days, gc_redis)
    storage_server._start_cleanup_thread()

    console.print(f"[green]RemoteStorage 启动: port={port}, data={data_path}, ttl={ttl_days}天[/green]")
    uvicorn.run(storage_server.app, host=host, port=port)


if __name__ == "__main__":
    app()
