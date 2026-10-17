from .queue import SmartQueue
from .worker import Worker
from .storage import RemoteStorage
from .admin import QueueAdmin, QueueState
from .clock import Clock, FrozenClock, SystemClock
from .errors import BatchEnqueueError, PermanentTaskError, RetryableTaskError
from .models import (
    DuplicateAction,
    EnqueueResult,
    HistoryMode,
    IdentityPolicy,
    TaskContext,
    TaskResult,
    TaskSpec,
)
from .security import redis_endpoint_label

__all__ = [
    "BatchEnqueueError",
    "Clock",
    "DuplicateAction",
    "EnqueueResult",
    "FrozenClock",
    "HistoryMode",
    "IdentityPolicy",
    "PermanentTaskError",
    "QueueAdmin",
    "QueueState",
    "RemoteStorage",
    "RetryableTaskError",
    "SmartQueue",
    "SystemClock",
    "TaskContext",
    "TaskResult",
    "TaskSpec",
    "Worker",
    "start_dashboard",
]

__version__ = "0.3.0"


def start_dashboard(
    port: int = 8765,
    redis_url: str = "redis://localhost:6379/0",
    host: str = "127.0.0.1",
    user: str = "admin",
    password: str | None = None,
    session_ttl: int = 86400,
    secure_cookie: bool = False,
):
    """
    启动 Dashboard
    
    Args:
        port: Dashboard 端口 (默认 8765)
        redis_url: Redis 连接 URL
        host: Dashboard 监听地址，远程访问可用 0.0.0.0
        user: 登录用户名，设置 password 后生效
        password: 登录密码，设置后启用登录
        session_ttl: 登录会话有效期，秒
        secure_cookie: HTTPS 部署时启用 Secure Cookie
    
    Example:
        >>> from qtask_list import start_dashboard
        >>> start_dashboard(port=9000)
    """
    import importlib.util
    import os
    import subprocess
    import sys

    if importlib.util.find_spec("fastapi") is None or importlib.util.find_spec("uvicorn") is None:
        raise RuntimeError("Dashboard 依赖未安装，请执行 pip install qtask_list[dashboard]")
    
    # 设置环境变量
    env = os.environ.copy()
    env["REDIS_URL"] = redis_url
    env["PORT"] = str(port)
    env["QTASK_DASHBOARD_USER"] = user
    env["QTASK_DASHBOARD_SESSION_TTL"] = str(session_ttl)
    if password is not None:
        env["QTASK_DASHBOARD_PASSWORD"] = password
    if secure_cookie:
        env["QTASK_DASHBOARD_SECURE_COOKIE"] = "1"
    
    # 启动 dashboard
    display_host = "localhost" if host in {"0.0.0.0", "::", "127.0.0.1"} else host
    print(f"Starting qtask_list Dashboard on http://{display_host}:{port}")
    print(f"Redis: {redis_endpoint_label(redis_url)}")
    print(f"Auth: {'enabled' if password else 'disabled'}")
    print("\nPress Ctrl+C to stop\n")
    
    subprocess.run(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "qtask_list.dashboard.main:app",
            "--host",
            host,
            "--port",
            str(port),
        ],
        env=env,
    )
