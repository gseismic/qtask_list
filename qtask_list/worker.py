"""qtask_list Worker。"""

from __future__ import annotations

import inspect
import os
import signal
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Any, Callable, Dict, Optional

import redis
from loguru import logger

from .clock import Clock
from .errors import PermanentTaskError, RetryableTaskError
from .models import HistoryMode, TaskResult
from .queue import SmartQueue, TaskClaim
from .storage import RemoteStorage


class Worker:
    """并发执行任务并维护 heartbeat、lease、恢复、归档和最小告警。

    handler 在注册时确定是一参数还是二参数形式，运行时不会通过捕获 TypeError 猜测签名。
    """

    def __init__(
        self,
        redis_url: str,
        queue_name: str,
        namespace: Optional[str] = None,
        result_queue: Optional[SmartQueue] = None,
        storage: Optional[RemoteStorage] = None,
        max_workers: int = 1,
        max_retry: Optional[int] = None,
        maintenance_interval: int = 1800,
        redis_client: Optional[Any] = None,
        worker_id: Optional[str] = None,
        heartbeat_ttl: int = 120,
        retry_backoff_base: float = 30,
        retry_backoff_max: float = 3600,
        record_history: bool = True,
        archive_dir: Optional[str] = None,
        monitor_threshold_mb: Optional[int] = None,
        stale_recover_interval: int = 300,
        *,
        max_attempts: Optional[int] = None,
        history_mode: HistoryMode | str | None = None,
        clock: Clock | None = None,
        concurrency_lease_seconds: float = 120,
        concurrency_retry_seconds: float = 1,
        dlq_alert_count: int = 1,
        dlq_alert_age: float = 3600,
        alert_callback: Callable[[dict[str, Any]], None] | None = None,
    ):
        if max_workers < 1:
            raise ValueError("max_workers must be >= 1")
        self.redis_url = redis_url
        self.worker_id = (worker_id or f"{os.getpid()}-{uuid.uuid4().hex}").replace(":", "_")
        self.heartbeat_ttl = max(heartbeat_ttl, 2)
        self.heartbeat_interval: float = max(1, min(30, self.heartbeat_ttl // 3))

        base = f"{namespace}:{queue_name}" if namespace else queue_name
        processing_key = f"{base}:processing:{self.worker_id}"
        self.queue = SmartQueue(
            redis_url,
            queue_name,
            namespace=namespace,
            storage=storage,
            max_retry=max_retry,
            max_attempts=max_attempts,
            redis_client=redis_client,
            processing_key=processing_key,
            retry_backoff_base=retry_backoff_base,
            retry_backoff_max=retry_backoff_max,
            record_history=record_history,
            history_mode=history_mode,
            clock=clock,
            worker_id=self.worker_id,
            concurrency_lease_seconds=concurrency_lease_seconds,
            concurrency_retry_seconds=concurrency_retry_seconds,
        )
        self.heartbeat_interval = min(self.heartbeat_interval, concurrency_lease_seconds / 3)
        self.result_queue = result_queue
        self._heartbeat_prefix = f"{self.queue.base}:worker:"
        self._heartbeat_key = f"{self._heartbeat_prefix}{self.worker_id}"
        self._worker_registry_key = f"qtask:workers:{self.queue.base}"
        self._leader_key = f"{self.queue.base}:maintenance:leader"
        self._leader_token = uuid.uuid4().hex

        self.handlers: Dict[str, Callable[..., Any]] = {}
        self._handler_accepts_context: dict[str, bool] = {}
        self.max_workers = max_workers
        self.running = False
        self.executor: Optional[ThreadPoolExecutor] = None
        # 限制线程池等待队列，避免 Redis 消息过早滞留 processing。
        self._semaphore = threading.Semaphore(max_workers * 2) if max_workers > 1 else None

        self._shutdown_event = threading.Event()
        # stop 与 handler 准入在此确定先后；不在持锁期间运行 handler。
        self._handler_start_lock = threading.Lock()
        # maintenance 线程只用该事件唤醒/退出；停止事件专门提供给 TaskContext，
        # 不能因为维护线程轮询而清除，否则 handler 会丢失 stop_requested 信号。
        self._maintenance_wakeup = threading.Event()
        # 心跳与租约必须独立于归档等慢维护操作。
        self._heartbeat_wakeup = threading.Event()
        self._draining = False
        self._active_claims: dict[str, TaskClaim] = {}
        # 异常退出的 handler 已用过预算，区别于领取响应丢失造成的未准入消息。
        self._abandoned_attempts: set[str] = set()
        self._active_claims_lock = threading.Lock()
        self.maintenance_interval = max(maintenance_interval, 1)
        self.archive_dir = archive_dir or os.environ.get("QTASK_ARCHIVE_DIR") or os.path.abspath(
            "archive_data"
        )
        self.monitor_threshold_mb = monitor_threshold_mb
        self.stale_recover_interval = max(stale_recover_interval, 30)
        self.dlq_alert_count = max(dlq_alert_count, 0)
        self.dlq_alert_age = max(dlq_alert_age, 0)
        self.alert_callback = alert_callback

    def _refresh_heartbeat(self) -> None:
        self.queue._assert_queue_namespace()
        now = self.queue.clock.now()
        pipe = self.queue.r.pipeline(transaction=True)
        pipe.set(self._heartbeat_key, str(now), ex=self.heartbeat_ttl)
        pipe.zadd(self._worker_registry_key, {self.worker_id: now})
        pipe.execute()
        self.queue._processing_key_managed_by_worker = True

    def _cleanup_worker_state(self) -> None:
        if self.queue.r.llen(self.queue.processing) == 0:
            self.queue.r.delete(self.queue.processing)
        self.queue.r.delete(self._heartbeat_key)
        self.queue.r.zrem(self._worker_registry_key, self.worker_id)
        release_script = r"""
        if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
        return redis.call('DEL', KEYS[1])
        """
        self.queue.r.eval(release_script, 1, self._leader_key, self._leader_token)

    def _signal_handler(self, signum: int, frame: Any) -> None:
        if not self.running:
            logger.warning("停止进行中再次收到信号，强制退出")
            raise KeyboardInterrupt
        sig_name = signal.Signals(signum).name
        logger.info(f"收到信号 {sig_name}({signum})，正在停止 worker {self.worker_id}...")
        self.stop(reason=f"signal:{sig_name}")

    def on(self, action: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """注册 handler，并立即验证 handler(payload[, context]) 契约。"""
        if not action:
            raise ValueError("action must not be empty")

        def decorator(function: Callable[..., Any]) -> Callable[..., Any]:
            signature = inspect.signature(function)
            positional = [
                parameter
                for parameter in signature.parameters.values()
                if parameter.kind
                in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
            ]
            variadic = any(
                parameter.kind == inspect.Parameter.VAR_POSITIONAL
                for parameter in signature.parameters.values()
            )
            required_keyword_only = [
                parameter
                for parameter in signature.parameters.values()
                if parameter.kind == inspect.Parameter.KEYWORD_ONLY
                and parameter.default is inspect.Parameter.empty
            ]
            if variadic or len(positional) not in {1, 2} or required_keyword_only:
                raise TypeError(
                    "handler must have signature handler(payload) or handler(payload, context)"
                )
            self.handlers[action] = function
            self._handler_accepts_context[action] = len(positional) == 2
            return function

        return decorator

    def _process_task_with_semaphore(self, claim: TaskClaim) -> None:
        interrupted = False
        try:
            self._process_task(claim)
        except BaseException:
            interrupted = True
            raise
        finally:
            with self._active_claims_lock:
                self._active_claims.pop(claim.context.task_id, None)
                if interrupted:
                    self._abandoned_attempts.add(claim.context.task_id)
            if self._semaphore:
                self._semaphore.release()

    def _process_task(self, claim: TaskClaim) -> None:
        """执行 handler，并把分类错误映射到统一状态转换。"""
        claim = claim.with_stop_event(self._shutdown_event)
        action = claim.context.action
        handler = self.handlers.get(action)
        with self._handler_start_lock:
            stop_requested = self._shutdown_event.is_set()
        if stop_requested:
            if not self.queue.return_unstarted_claim(claim):
                logger.warning(f"Stopped claim was already moved task={claim.context.task_id}")
            return
        started_monotonic = time.monotonic()
        if handler is None:
            logger.warning(f"No handler for action: {action}")
            self._settle_claim(partial(self.queue.fail,
                claim.raw_message, f"unknown action: {action}",
                code="unknown_action", permanent=True,
            ), claim)
            return

        try:
            if self._handler_accepts_context[action]:
                result = handler(claim.payload, claim.context)
            else:
                result = handler(claim.payload)

            audit_result: Any = result
            if isinstance(result, TaskResult):
                if result.emissions:
                    if self.result_queue is None:
                        raise PermanentTaskError(
                            "result_queue_missing",
                            "handler 返回了 emissions，但 Worker 未配置 result_queue",
                        )
                    enqueue_results = self.result_queue.enqueue_many(result.emissions)
                    unexpected = [
                        item
                        for item in enqueue_results
                        if not item.accepted
                        and item.reason not in {"duplicate_active", "duplicate_retained"}
                    ]
                    if unexpected:
                        raise RetryableTaskError(
                            "emission_failed",
                            f"下游任务投递未确认: {unexpected!r}",
                        )
                audit_result = result.value
            elif self.result_queue is not None and isinstance(result, dict) and result:
                # 兼容旧 handler：裸 dict 仍投递一个无自动 identity 的下游任务。
                self.result_queue.push(result)

            if not self._settle_claim(partial(self.queue.ack, claim.raw_message, result=audit_result), claim):
                logger.warning(f"Ack lost ownership task={claim.context.task_id}")
        except RetryableTaskError as exc:
            logger.warning(f"Retryable handler error action={action} code={exc.code}: {exc}")
            self._settle_claim(partial(self.queue.fail,
                claim.raw_message,
                str(exc),
                code=exc.code,
                retry_after=exc.retry_after,
            ), claim)
        except PermanentTaskError as exc:
            logger.error(f"Permanent handler error action={action} code={exc.code}: {exc}")
            self._settle_claim(partial(self.queue.fail,
                claim.raw_message,
                str(exc),
                code=exc.code,
                permanent=True,
            ), claim)
        except Exception as exc:
            logger.exception(f"Handler error for {action}: {exc}")
            self._settle_claim(partial(self.queue.fail,
                claim.raw_message,
                str(exc),
                code="handler_unclassified",
            ), claim)
        finally:
            duration = max(time.monotonic() - started_monotonic, 0.0)
            try:
                pipe = self.queue.r.pipeline()
                pipe.hincrbyfloat(self.queue.metrics_key, "handler_duration_seconds.total", duration)
                pipe.hincrby(self.queue.metrics_key, "handler_duration_seconds.count", 1)
                pipe.execute()
            except redis.RedisError as exc:
                logger.warning(f"Handler metrics unavailable: {exc}")

    def _settle_claim(self, operation: Callable[[], bool], claim: TaskClaim) -> bool:
        """Redis 暂时不可用时保留 claim 并重试结算，避免重复执行 handler。"""
        while True:
            try:
                return operation()
            except redis.RedisError as exc:
                logger.warning(f"Task settlement pending task={claim.context.task_id}: {exc}")
                if self._shutdown_event.wait(0.1):
                    # 停止后交由下一 Worker 的 stale recovery 接管未确认消息。
                    return False

    def _renew_active_leases(self) -> None:
        with self._active_claims_lock:
            claims = list(self._active_claims.values())
        for claim in claims:
            if not self.queue.renew_claim_lease(claim):
                logger.error(f"Concurrency lease lost task={claim.context.task_id}")

    def _register_claim(self, claim: TaskClaim) -> None:
        with self._active_claims_lock:
            self._active_claims[claim.context.task_id] = claim

    def _discard_claim(self, claim: TaskClaim) -> None:
        with self._active_claims_lock:
            self._active_claims.pop(claim.context.task_id, None)
            if claim.settlement_pending:
                self._abandoned_attempts.add(claim.context.task_id)

    def _heartbeat_loop(self) -> None:
        """独立续租所有已领取任务，包含等待线程池执行的 claim。"""
        while self.running or self._draining:
            try:
                self._refresh_heartbeat()
                self._renew_active_leases()
            except Exception as exc:
                logger.exception(f"Heartbeat renewal failed: {exc}")
            if self._heartbeat_wakeup.wait(self.heartbeat_interval):
                self._heartbeat_wakeup.clear()

    def _acquire_maintenance_leader(self) -> bool:
        ttl = max(self.heartbeat_ttl, 30)
        if self.queue.r.set(self._leader_key, self._leader_token, nx=True, ex=ttl):
            return True
        renew_script = r"""
        if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
        redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
        return 1
        """
        return bool(
            self.queue.r.eval(
                renew_script,
                1,
                self._leader_key,
                self._leader_token,
                ttl,
            )
        )

    def _emit_operational_alerts(self) -> None:
        if self.alert_callback is None:
            return
        stats = self.queue.get_stats()
        alerts: list[dict[str, Any]] = []
        if self.dlq_alert_count and int(stats["dlq"]) >= self.dlq_alert_count:
            alerts.append(
                {
                    "type": "dlq_count",
                    "queue": self.queue.base,
                    "value": int(stats["dlq"]),
                    "threshold": self.dlq_alert_count,
                }
            )
        if self.dlq_alert_age and float(stats["dlq_oldest_age"]) >= self.dlq_alert_age:
            alerts.append(
                {
                    "type": "dlq_oldest_age",
                    "queue": self.queue.base,
                    "value": float(stats["dlq_oldest_age"]),
                    "threshold": self.dlq_alert_age,
                }
            )
        if int(stats["ready"]) > 0 and not any(
            self.queue.r.scan_iter(f"{self.queue.base}:worker:*")
        ):
            alerts.append(
                {
                    "type": "workers_missing",
                    "queue": self.queue.base,
                    "value": int(stats["ready"]),
                }
            )
        for alert in alerts:
            try:
                self.alert_callback(alert)
            except Exception as exc:
                logger.error(f"Alert callback failed: {exc}")

    def _maintenance_loop(self) -> None:
        """leader 执行恢复、归档、诊断和告警；心跳由独立线程负责。"""
        from .archiver import ArchiveManager, Monitor

        archiver = None
        monitor = None
        try:
            archiver = ArchiveManager(self.redis_url, db_dir=self.archive_dir)
            monitor = Monitor(self.queue.r, threshold_mb=self.monitor_threshold_mb)
        except Exception as exc:
            logger.error(f"Maintenance init failed, archive/memory check disabled: {exc}")

        logger.info("Maintenance thread started")
        last_maintenance = 0.0
        last_stale_recover = 0.0
        while self.running or self._draining:
            try:
                now = self.queue.clock.now()
                is_leader = self._acquire_maintenance_leader()
                if self.running and is_leader:
                    if monitor:
                        monitor.check_health()
                    if now - last_stale_recover > self.stale_recover_interval:
                        recovered = self.queue.recover_stale_processing(self._heartbeat_prefix)
                        if recovered:
                            logger.info(f"Recovered {recovered} tasks from stale processing queues")
                        last_stale_recover = now
                    if now - last_maintenance > self.maintenance_interval:
                        consistency = self.queue.check_consistency(limit=1000)
                        if consistency["issue_count"]:
                            logger.warning(
                                f"Queue consistency issues queue={self.queue.base} "
                                f"counts={consistency['issue_counts']}"
                            )
                        if archiver:
                            count = archiver.archive_to_sqlite(self.queue.base, days_ago=1)
                            if count:
                                logger.info(f"Archived {count} tasks to SQLite")
                        self._emit_operational_alerts()
                        last_maintenance = now
            except Exception as exc:
                try:
                    self.queue.r.hincrby(self.queue.metrics_key, "maintenance.failure", 1)
                except redis.RedisError:
                    pass
                logger.exception(f"Maintenance error: {exc}")

            stop_wait = min(60, self.maintenance_interval, self.heartbeat_interval)
            if self._maintenance_wakeup.wait(stop_wait):
                self._maintenance_wakeup.clear()
                if not self.running and not self._draining:
                    break
        logger.info("Maintenance thread stopped")

    def _poll_once(self) -> bool:
        acquired = False
        if self.max_workers > 1:
            if self._semaphore is None or self.executor is None:
                raise RuntimeError("Thread pool is not initialized")
            while self.running:
                if self._semaphore.acquire(timeout=1):
                    acquired = True
                    break
            if not acquired:
                return False

        try:
            self._refresh_heartbeat()
            with self._active_claims_lock:
                protected = {item.raw_message for item in self._active_claims.values()}
                spent_attempts = set(self._abandoned_attempts)
            recovered = self.queue.recover_abandoned_claims(protected, spent_attempts=spent_attempts)
            with self._active_claims_lock:
                self._abandoned_attempts.difference_update(spent_attempts)
            if recovered:
                logger.warning(f"Recovered {recovered} abandoned local claims")
            self.queue.move_retry()
            claim = self.queue.pop_claim(
                timeout=2,
                _on_started=self._register_claim,
                _on_discarded=self._discard_claim,
                _stop_requested=self._shutdown_event.is_set,
            )
            if claim is None:
                return False
            self._register_claim(claim)
            if self.max_workers > 1:
                acquired = False
                assert self.executor is not None
                try:
                    self.executor.submit(self._process_task_with_semaphore, claim)
                except RuntimeError:
                    # 提交失败时直接处理已领取任务，避免消息留在活跃 processing。
                    self._process_task_with_semaphore(claim)
            else:
                self._process_task_with_semaphore(claim)
            return True
        finally:
            if acquired and self._semaphore:
                self._semaphore.release()

    def _worker_loop(self) -> None:
        logger.info(f"Worker started, listening on {self.queue.base} as {self.worker_id}")
        idle_count = 0
        while self.running:
            try:
                got_task = self._poll_once()
                if got_task:
                    idle_count = 0
                else:
                    idle_count += 1
                    if idle_count % 60 == 0:
                        logger.debug(
                            f"Worker 空闲中: {self.queue.base} "
                            f"(已空闲 {idle_count * 2}s, 约 {idle_count} 次轮询)"
                        )
            except Exception as exc:
                logger.exception(f"Worker loop error: {exc}")
                time.sleep(1)
        logger.info("Worker stopped")

    def run(self) -> None:
        """启动 Worker；优雅停止期间继续 heartbeat 和 lease 续租。"""
        with self._handler_start_lock:
            if self._shutdown_event.is_set():
                return
            self.running = True
            self._draining = False
        self._maintenance_wakeup.clear()
        self._heartbeat_wakeup.clear()
        if threading.current_thread() is threading.main_thread():
            try:
                signal.signal(signal.SIGINT, self._signal_handler)
                signal.signal(signal.SIGTERM, self._signal_handler)
            except ValueError:
                logger.warning("Failed to register signals (not in main thread?)")

        recovered = self.queue.recover_stale_processing(self._heartbeat_prefix)
        if recovered:
            logger.info(f"Recovered {recovered} tasks from stale processing queues")
        self._refresh_heartbeat()
        heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        heartbeat_thread.start()
        maintenance_thread = threading.Thread(target=self._maintenance_loop, daemon=True)
        maintenance_thread.start()
        if self.max_workers > 1:
            self.executor = ThreadPoolExecutor(max_workers=self.max_workers)

        try:
            self._worker_loop()
        finally:
            self.stop(reason="worker_loop_exit")
            if self.executor is not None:
                self.executor.shutdown(wait=True)
            self._draining = False
            self._heartbeat_wakeup.set()
            self._maintenance_wakeup.set()
            heartbeat_thread.join(timeout=5)
            maintenance_thread.join(timeout=5)
            self._cleanup_worker_state()

    def stop(self, reason: str = "unknown") -> None:
        """请求 Worker 停止；TaskContext 会立即观察到 stop_requested。"""
        with self._handler_start_lock:
            was_running = self.running
            if was_running:
                # 阻塞领取可能在 stop 之后返回；心跳覆盖该窗口和后续 drain。
                with self._active_claims_lock:
                    self._draining = True
                self.running = False
            self._shutdown_event.set()
        if was_running:
            logger.info(f"Worker 正在停止: {self.worker_id} reason={reason}")
        self._maintenance_wakeup.set()
        self._heartbeat_wakeup.set()
