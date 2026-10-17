"""从最小的干净源码副本构建，避免本机 build/lib 中的旧 SPA 混入。"""

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

root = Path(__file__).resolve().parents[3]
output = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else Path(__file__).parent / "wheel-clean"
with tempfile.TemporaryDirectory(prefix="qtask-wheel-source-") as directory:
    source = Path(directory)
    for name in ("pyproject.toml", "README.md", "LICENSE"):
        shutil.copy2(root / name, source / name)
    shutil.copytree(root / "qtask_list", source / "qtask_list",
                    ignore=shutil.ignore_patterns("frontend", "__pycache__"))
    subprocess.run(["uv", "build", "--wheel", "--no-build-isolation", str(source),
                    "--out-dir", str(output)], check=True)
