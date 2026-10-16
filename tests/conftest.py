"""真实 Redis 测试隔离，防止测试中的清理操作触及开发或业务实例。"""

import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest
import redis


def pytest_configure(config):
    """收集测试模块前改用私有 Unix socket，覆盖模块级管理对象的连接。"""
    executable = shutil.which("redis-server")
    if executable is None:
        raise pytest.UsageError("核心测试需要 redis-server，以启动独立临时实例")
    directory = tempfile.TemporaryDirectory(prefix="qtask-tests-")
    root = Path(directory.name)
    original_cwd = Path.cwd()
    socket = str(root / "redis.sock")
    server = subprocess.Popen(
        [
            executable,
            "--port",
            "0",
            "--unixsocket",
            socket,
            "--unixsocketperm",
            "700",
            "--save",
            "",
            "--appendonly",
            "no",
            "--dir",
            str(root),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
    )
    original = redis.from_url
    env = {name: os.environ.get(name) for name in ("QTASK_ARCHIVE_DIR", "QTASK_STORAGE_DIR")}

    def cleanup():
        redis.from_url = original
        os.chdir(original_cwd)
        server.terminate()
        server.wait(timeout=5)
        for name, value in env.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        directory.cleanup()

    config.add_cleanup(cleanup)
    probe = redis.Redis(unix_socket_path=socket, decode_responses=True)
    for _ in range(200):
        try:
            if probe.ping():
                break
        except redis.RedisError:
            time.sleep(0.01)
    else:
        raise pytest.UsageError("独立 Redis 启动失败")
    probe.close()

    def isolated_from_url(url, **kwargs):
        return redis.Redis(unix_socket_path=socket, **kwargs)

    redis.from_url = isolated_from_url
    os.environ["QTASK_ARCHIVE_DIR"] = str(root / "archives")
    os.environ["QTASK_STORAGE_DIR"] = str(root / "storage")
    # 旧归档测试使用相对目录并会清理目录；文件输出也隔离到临时工作目录。
    if str(config.rootpath) not in sys.path:
        sys.path.insert(0, str(config.rootpath))
    os.chdir(root)


@pytest.fixture
def isolated_redis():
    """独立实例内为故障回归提供干净数据库，每个测试结束再清理。"""
    client = redis.from_url("redis://test-isolated/0", decode_responses=True)
    client.flushdb()
    yield client
    client.flushdb()
    client.close()
