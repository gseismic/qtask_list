"""独立重建 SPA，并逐字节核对前端产物及干净 wheel 中的全部源码/静态文件。"""

import hashlib
import json
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

root = Path(__file__).resolve().parents[3]
spa = root / "qtask_list/dashboard/static/spa"
folder = Path(__file__).parent
with tempfile.TemporaryDirectory(prefix="qtask-spa-verify-") as directory:
    output = Path(directory)
    subprocess.run(["pnpm", "exec", "vite", "build", "--outDir", str(output), "--emptyOutDir"],
                   cwd=root / "qtask_list/frontend", check=True, stdout=sys.stderr)
    built = {path.relative_to(output).as_posix(): path.read_bytes() for path in output.rglob("*") if path.is_file()}
    actual = {path.relative_to(spa).as_posix(): path.read_bytes() for path in spa.rglob("*") if path.is_file()}
    assert built == actual
javascript = next(value for key, value in actual.items() if key.endswith(".js"))
markers = ["exact_task_id", "overview-matrix", "kpi-action-hint", "/api/queues/", "/api/tasks/",
           "focusin", "stopImmediatePropagation"]
assert all(marker.encode() in javascript for marker in markers)
files = {}
for path in (root / "qtask_list").rglob("*"):
    if path.is_file() and "frontend" not in path.parts and "__pycache__" not in path.parts and path.suffix in {".py", ".html", ".css", ".js"}:
        files[path.relative_to(root).as_posix()] = path.read_bytes()
with zipfile.ZipFile(folder / "wheel-final-3/qtask_list-0.2.0-py3-none-any.whl") as wheel:
    packed = {name: wheel.read(name) for name in wheel.namelist() if name.startswith("qtask_list/") and name.endswith((".py", ".html", ".css", ".js"))}
    assert files == packed
print(json.dumps({"independent_spa_build": "identical", "markers": markers,
                  "sha256": {name: hashlib.sha256(data).hexdigest() for name, data in actual.items()},
                  "wheel_source_and_static_files": len(files), "wheel": "identical"}, indent=2))
