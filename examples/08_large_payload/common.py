"""大 payload 外存（RemoteStorage）：超过阈值自动上传 HTTP 存储服务。

需要三个终端（需要本地 Redis）::

    # 终端 1：启动外存服务端（qtask_list[storage] 依赖）
    python -m qtask_list.remote_storage.server --port 8096

    # 终端 2：启动消费者
    python examples/08_large_payload/consumer.py

    # 终端 3：生产大任务
    python examples/08_large_payload/producer.py

机制：
- push 时 payload 序列化后的**原始大小**超过 large_threshold（默认 50KB，判断
  发生在压缩之前）→ 自动 POST 上传到 RemoteStorage 服务，队列里只存
  kind=external 的引用（key/size/sha256，不含数据本体）；
- pop 时检测到 external 引用 → 自动 GET 下载并校验 SHA256，还原完整 payload，
  消费方无感知；
- 队列托管对象在任务仍可执行或重放期间保留；终态后按 Redis 回收日志清理。
  独立上传的非托管对象按 retain_until 或服务端 TTL 清理。
"""

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from qtask_list import RemoteStorage, SmartQueue  # noqa: E402

REDIS_URL = "redis://localhost:6379/0"
NAMESPACE = "demo"
QUEUE_NAME = "big-payload"

# 客户端指向外存服务；生产环境应使用 HTTPS 与鉴权
storage = RemoteStorage("http://localhost:8096")

# 阈值调小到 10KB 方便演示（生产默认 50KB）
queue = SmartQueue(
    REDIS_URL,
    QUEUE_NAME,
    namespace=NAMESPACE,
    storage=storage,
    large_threshold=10 * 1024,
)
