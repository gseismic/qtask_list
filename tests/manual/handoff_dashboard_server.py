"""为交接问题浏览器回归启动独立 Redis 和 Dashboard，不连接共享实例。"""

import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import redis
import uvicorn


def _stop(_signum: int, _frame: object) -> None:
    # Uvicorn 退出后会重新发出信号，确保仍能执行外层 Redis 清理。
    raise KeyboardInterrupt


def main() -> None:
    signal.signal(signal.SIGTERM, _stop)
    artifacts = Path(sys.argv[1]).resolve()
    artifacts.mkdir(parents=True, exist_ok=True)
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    with tempfile.TemporaryDirectory(prefix="qtask-handoff-browser-") as directory:
        redis_socket = str(Path(directory) / "redis.sock")
        server = subprocess.Popen(
            ["redis-server", "--port", "0", "--unixsocket", redis_socket,
             "--unixsocketperm", "700", "--save", "", "--appendonly", "no", "--dir", directory],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )
        try:
            client = redis.Redis(unix_socket_path=redis_socket, decode_responses=True)
            for _ in range(200):
                try:
                    if client.ping():
                        break
                except redis.RedisError:
                    time.sleep(0.01)
            else:
                raise RuntimeError("独立 Redis 启动失败")
            now = time.time()
            identifiers = ["4b701bde-5f35-4c8a-b05c-59b6e929f265", "legacy_order_42",
                           "v1", "旧任务-42", "unindexed-legacy-id"]
            queue = "handoff_browser:search"
            for index, task_id in enumerate(identifiers):
                client.hset(f"qtask:task:{task_id}", mapping={
                    "task_id": task_id, "action": "browser_test_dlq", "status": "pending",
                    "location": "ready", "_queue": queue,
                    "created_at": now - index, "updated_at": now - index,
                })
                if task_id != "unindexed-legacy-id":
                    client.zadd(f"qtask:hist:{queue}", {task_id: now - index})
                    client.lpush(queue, json.dumps({
                        "task_id": task_id, "payload": json.dumps({"action": "browser_test_dlq"}),
                    }))
            client.lpush("handoff_browser:external", json.dumps({
                "task_id": "external-only", "payload": "{}",
            }))
            client.zadd("qtask:queues", {queue: now, "handoff_browser:external": now,
                                        "handoff_browser:registered-empty": now})
            os.environ["REDIS_URL"] = f"unix://{redis_socket}"
            for name in ("QTASK_DASHBOARD_AUTH", "QTASK_DASHBOARD_USER", "QTASK_DASHBOARD_PASSWORD",
                         "QTASK_DASHBOARD_SECRET", "QTASK_DASHBOARD_SECURE_COOKIE",
                         "QTASK_DASHBOARD_SESSION_TTL", "QTASK_DASHBOARD_MEMORY_THRESHOLD_MB"):
                os.environ.pop(name, None)
            if "--auth" in sys.argv[2:]:
                # 仅供此独立实例回归，不使用用户凭据。
                os.environ["QTASK_DASHBOARD_USER"] = "回归管理员"
                os.environ["QTASK_DASHBOARD_PASSWORD"] = "测试🔑密码"
                os.environ["QTASK_DASHBOARD_SECRET"] = "isolated-dashboard-review"
            from qtask_list.dashboard.main import app

            with socket.socket() as listener:
                listener.bind(("127.0.0.1", 0))
                url = f"http://127.0.0.1:{listener.getsockname()[1]}"
                metadata = {"url": url, "redis_socket": redis_socket,
                            "task_ids": identifiers, "queue": queue}
                (artifacts / "browser-runtime.json").write_text(
                    json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8",
                )
                print(f"隔离浏览器测试地址：{url}", flush=True)
                uvicorn.Server(uvicorn.Config(app, log_level="info")).run(sockets=[listener])
            client.close()
        finally:
            server.terminate()
            server.wait(timeout=5)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
