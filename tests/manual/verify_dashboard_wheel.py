"""在仅安装 wheel[dashboard] 和测试客户端的隔离环境验证运行，不加入源码路径。"""

import importlib.metadata
import json
import os
import re
import socket
import subprocess
import sys
import time
from pathlib import Path


def verify() -> None:
    runtime = json.loads(Path(sys.argv[1]).read_text())
    assert runtime["redis_socket"].startswith("/tmp/qtask-handoff-browser-")
    os.environ["REDIS_URL"] = f"unix://{runtime['redis_socket']}"
    os.environ["QTASK_DASHBOARD_USER"] = "回归管理员"
    os.environ["QTASK_DASHBOARD_PASSWORD"] = "测试🔑密码"
    os.environ["QTASK_DASHBOARD_SECRET"] = "isolated-wheel-test"
    for key in ("QTASK_DASHBOARD_MEMORY_THRESHOLD_MB", "QTASK_DASHBOARD_SESSION_TTL",
                "QTASK_DASHBOARD_AUTH", "QTASK_DASHBOARD_SECURE_COOKIE"):
        os.environ.pop(key, None)

    from fastapi.testclient import TestClient
    import httpx
    from qtask_list.dashboard import main

    assert "site-packages" in str(Path(main.__file__)), main.__file__
    with TestClient(main.app) as client:
        login = client.get("/login")
        assert login.status_code == 200 and "回归管理员" in login.text
        assert "连接失败，请检查网络" in login.text
        assert client.post("/api/login", json={"username": "回归管理员", "password": "测试🔑密码"}).status_code == 200
        health = client.get("/api/health")
        assert health.status_code == 200 and health.json()["status"] == "ok"
        index = client.get("/")
        assert index.status_code == 200 and index.headers["cache-control"] == "no-cache"
        assets = re.findall(r'(?:src|href)="(/assets/[^" ]+)"', index.text)
        assert len(assets) == 2
        for asset in assets:
            assert client.get(asset).status_code == 200, asset
    requirements = importlib.metadata.requires("qtask_list") or []
    assert any("jinja2" in item.lower() and "dashboard" in item for item in requirements)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    server = subprocess.Popen([
        sys.executable, "-m", "uvicorn", "qtask_list.dashboard.main:app",
        "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning",
    ], stdout=sys.stderr, stderr=sys.stderr)
    try:
        with httpx.Client(base_url=f"http://127.0.0.1:{port}", trust_env=False) as remote:
            for _ in range(100):
                try:
                    info = remote.get("/api/auth")
                    if info.status_code == 200:
                        break
                except httpx.HTTPError:
                    pass
                if server.poll() is not None:
                    raise RuntimeError("wheel Uvicorn 启动失败")
                time.sleep(0.05)
            else:
                raise RuntimeError("wheel Uvicorn 未就绪")
            assert info.json()["username"] == "回归管理员"
            assert remote.post("/api/login", json={"username": "回归管理员", "password": "测试🔑密码"}).status_code == 200
            assert remote.get("/api/health").json()["status"] == "ok"
    finally:
        server.terminate()
        server.wait(timeout=5)
    print(json.dumps({"module": main.__file__, "assets": assets,
                      "fastapi": importlib.metadata.version("fastapi"),
                      "jinja2": importlib.metadata.version("jinja2"),
                      "health": health.json(), "uvicorn_startup": "passed",
                      "result": "passed"}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    verify()
