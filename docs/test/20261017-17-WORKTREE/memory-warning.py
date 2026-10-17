"""只调整测试私有 Redis 的 maxmemory，结束时恢复配置。"""

import json
import subprocess
from pathlib import Path

import redis

folder = Path(__file__).parent
runtime = json.loads((folder / "browser-3/browser-runtime.json").read_text())
assert runtime["redis_socket"].startswith("/tmp/qtask-handoff-browser-")
r = redis.Redis(unix_socket_path=runtime["redis_socket"], decode_responses=True)
previous = r.config_get("maxmemory")["maxmemory"]
try:
    r.config_set("maxmemory", 1024 * 1024)
    subprocess.run(["playwright-cli", "--raw", "-s=dashboard-fix-review", "run-code",
                    f"--filename={folder / 'browser-memory-warning.js'}"], check=True, timeout=60)
finally:
    r.config_set("maxmemory", previous)
    r.close()
