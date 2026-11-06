"""qtask_list 的可程序化错误类型。"""

from __future__ import annotations

from typing import Any, Sequence

from .models import EnqueueResult


class BatchEnqueueError(Exception):
    """逐项投递中断；results 是已完成的输入前缀。"""

    def __init__(self, index: int, results: Sequence[EnqueueResult]):
        super().__init__(f"enqueue_many stopped at index {index}; {len(results)} preceding items committed")
        self.index = index
        self.results = list(results)


class WorkerIdentityConflictError(RuntimeError):
    """另一个活动实例持有该队列中的 Worker 标识。"""


class TaskError(Exception):
    """任务处理错误基类，稳定的 code 供 Worker 决定状态转换。"""

    def __init__(self, code: str, message: str):
        if not code:
            raise ValueError("error code must not be empty")
        super().__init__(message)
        self.code = code
        self.message = message

    def __str__(self) -> str:
        return self.message


class RetryableTaskError(TaskError):
    """可恢复错误；retry_after 是调用方建议的最早重试间隔。"""

    def __init__(self, code: str, message: str, retry_after: float | None = None):
        super().__init__(code, message)
        if retry_after is not None and retry_after < 0:
            raise ValueError("retry_after must be >= 0")
        self.retry_after = retry_after


class PermanentTaskError(TaskError):
    """不可恢复错误，Worker 不应消耗剩余自动重试次数。"""


class RemoteStorageError(Exception):
    """RemoteStorage 协议错误基类，携带稳定 code 与 HTTP 状态。"""

    retryable = False

    def __init__(
        self,
        code: str,
        message: str,
        *,
        status_code: int | None = None,
        retry_after: float | None = None,
    ):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.retry_after = retry_after

    def __str__(self) -> str:
        return self.message


class RemoteStorageTransientError(RemoteStorageError):
    """网络、408/429 或 5xx 等可恢复外存错误。"""

    retryable = True


class RemoteObjectNotFoundError(RemoteStorageError):
    """外存对象不存在或已经过期。"""


class RemoteStorageConfigurationError(RemoteStorageError):
    """外存未配置、认证失败或协议配置错误。"""


class PayloadChecksumError(RemoteStorageError):
    """外存内容长度或 SHA256 与信封不一致。"""


class PayloadDecodeError(RemoteStorageError):
    """payload 无法解压或不能解码为 JSON object。"""


def exception_details(exc: BaseException) -> dict[str, Any]:
    """把已分类异常转换为适合历史记录的稳定字段。"""
    return {
        "reason_code": str(getattr(exc, "code", "unclassified")),
        "reason": str(exc),
    }
