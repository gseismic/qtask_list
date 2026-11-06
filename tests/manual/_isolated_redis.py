"""为手工验证启动私有 Redis，避免清理或连接本机共享数据库。"""

from contextlib import contextmanager
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
from typing import Iterator

import redis


@contextmanager
def isolated_redis() -> Iterator[tuple[str, Path]]:
    executable = shutil.which("redis-server")
    if executable is None:
        raise RuntimeError("手工验证需要 redis-server")
    with tempfile.TemporaryDirectory(prefix="qtask-manual-") as directory:
        root = Path(directory)
        socket_path = root / "redis.sock"
        server = subprocess.Popen(
            [executable, "--port", "0", "--unixsocket", str(socket_path),
             "--unixsocketperm", "700", "--save", "", "--appendonly", "no", "--dir", directory],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        probe = redis.Redis(unix_socket_path=str(socket_path), decode_responses=True)
        try:
            for _ in range(200):
                if server.poll() is not None:
                    raise RuntimeError("私有 Redis 启动失败")
                try:
                    if probe.ping():
                        break
                except redis.RedisError:
                    time.sleep(0.01)
            else:
                raise RuntimeError("私有 Redis 启动超时")
            yield f"unix://{socket_path}", root
        finally:
            probe.close()
            if server.poll() is None:
                server.terminate()
                try:
                    server.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait(timeout=5)
