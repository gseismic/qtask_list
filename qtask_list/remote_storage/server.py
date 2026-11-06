"""qtask_list RemoteStorage 服务端。

协议与客户端 qtask_list/storage.py 匹配：
  POST /api/storage/upload          → 上传 bytes，返回 {"key": "..."}
  GET  /api/storage/download/{key}  → 下载原始 bytes
  DELETE /api/storage/delete/{key}  → 删除

特性：
- 新任务使用独占随机键，旧内容寻址对象仍可读取
- 按 key 前两位分子目录，避免单目录文件过多
- 终态对象由 Redis 回收日志清理；旧缓存遵循服务端 TTL

启动：
  python -m qtask_list.remote_storage.server --port 8096
  uvicorn qtask_list.remote_storage.server:app --host 127.0.0.1 --port 8096
"""

import hmac
import ipaddress
import json
import math
import os
import sqlite3
import tempfile
import threading
import time
import uuid
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Annotated, Iterator

import uvicorn
import redis
from fastapi import FastAPI, File, Form, HTTPException, Request, Response, UploadFile
from fastapi.responses import JSONResponse
from loguru import logger
from pydantic import BaseModel

from qtask_list.security import origin_tuple

app = FastAPI(title="qtask RemoteStorage")


@app.middleware("http")
async def storage_access_control(request: Request, call_next):
    """本机可免 token；远程访问必须配置并提交 Bearer token。"""
    origin = request.headers.get("origin")
    if origin and request.method not in {"GET", "HEAD", "OPTIONS"}:
        actual = origin_tuple(origin)
        current = origin_tuple(f"{request.url.scheme}://{request.url.netloc}")
        if actual is None or actual != current:
            return JSONResponse({"detail": "不允许此来源执行写操作"}, status_code=403)
    token = os.environ.get("QTASK_STORAGE_TOKEN", "")
    if token:
        provided = request.headers.get("Authorization", "")
        if not hmac.compare_digest(provided, f"Bearer {token}"):
            return JSONResponse({"detail": "storage authentication required"}, status_code=401)
    else:
        host = request.client.host if request.client else ""
        try:
            local = ipaddress.ip_address(host).is_loopback
        except ValueError:
            local = False
        if not local:
            return JSONResponse(
                {"detail": "remote storage access requires QTASK_STORAGE_TOKEN"},
                status_code=403,
            )
    return await call_next(request)

DEFAULT_DIR = Path(os.environ.get("QTASK_STORAGE_DIR", Path.home() / ".qtask-storage"))
DATA_DIR = DEFAULT_DIR
DATA_DIR.mkdir(parents=True, exist_ok=True)


def _key_path(key: str) -> Path:
    if len(key) != 32 or any(char not in "0123456789abcdef" for char in key):
        raise ValueError("invalid storage key")
    return DATA_DIR / key[:2] / key


def _meta_path(key: str) -> Path:
    """外存保留元数据与内容分离，避免修改共享内容本身。"""
    return _key_path(key).with_name(f"{key}.meta.json")


@contextmanager
def _retention_guard() -> Iterator[None]:
    """在共享目录内串行化文件保留期与删除；锁不可用时不执行文件操作。"""
    lock_path = DATA_DIR / ".qtask-retention.lock"
    with closing(sqlite3.connect(lock_path, timeout=30)) as connection:
        connection.execute("BEGIN IMMEDIATE")
        try:
            yield
        finally:
            connection.rollback()


def _is_storage_file(path: Path) -> bool:
    """只识别服务的对象文件，忽略锁、临时文件及目录中的其他文件。"""
    name = path.name
    return (len(name) == 32 and all(char in "0123456789abcdef" for char in name)
            and path.parent == DATA_DIR / name[:2] and path.is_file())


def _generate_key(data: bytes) -> str:
    """新任务使用独占对象键；旧内容寻址键仍可照常读取。"""
    return uuid.uuid4().hex


_ttl_seconds: float = float(os.environ.get("QTASK_STORAGE_TTL", 7 * 86400))
_cleanup_interval: float = 60
MAX_UPLOAD_BYTES = int(os.environ.get("QTASK_STORAGE_MAX_BYTES", str(64 * 1024 * 1024)))
STORAGE_GC_KEY = "qtask:storage:gc"
STORAGE_PENDING_KEY = "qtask:storage:pending"
STORAGE_CLAIM_KEY = "qtask:storage:claim"
PENDING_GRACE_SECONDS = 86400
_gc_redis_url = os.environ.get("QTASK_STORAGE_GC_REDIS_URL") or os.environ.get(
    "REDIS_URL", "redis://localhost:6379/0"
)
PROMOTE_PENDING_LUA = r"""
local score = tonumber(redis.call('ZSCORE', KEYS[1], ARGV[1]) or '')
if not score or (ARGV[3] ~= '1' and score > tonumber(ARGV[2])) then return 0 end
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('ZADD', KEYS[2], ARGV[2], ARGV[1])
return 1
"""
_cleanup_thread_started = False
_cleanup_thread_lock = threading.Lock()


def configure(
    data_dir: str | Path | None = None,
    ttl_days: float | None = None,
    gc_redis_url: str | None = None,
) -> None:
    """配置服务端运行目录和 TTL。"""
    global DATA_DIR, _ttl_seconds, _gc_redis_url

    if data_dir is not None:
        DATA_DIR = Path(data_dir)
        DATA_DIR.mkdir(parents=True, exist_ok=True)
    if ttl_days is not None:
        _ttl_seconds = ttl_days * 86400 if ttl_days > 0 else 0
    if gc_redis_url is not None:
        _gc_redis_url = gc_redis_url


def _storage_id() -> str:
    """数据目录持久 ID，防止多个存储实例消费彼此的回收日志。"""
    id_path = DATA_DIR / ".storage-id"
    try:
        existing = id_path.read_text(encoding="ascii").strip()
        if existing:
            return existing
    except FileNotFoundError:
        pass
    value = uuid.uuid4().hex
    try:
        fd = os.open(id_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        # 另一进程可能刚创建文件，等待其写完 ID。
        for _ in range(20):
            existing = id_path.read_text(encoding="ascii").strip()
            if existing:
                return existing
            time.sleep(0.01)
        raise RuntimeError("storage ID initialization did not finish")
    with os.fdopen(fd, "w", encoding="ascii") as handle:
        handle.write(value)
    return value


def _promote_pending_gc(
    client: redis.Redis, storage_id: str, key: str, due_at: float, *, force: bool = False
) -> bool:
    """原子撤销待入队资格，并留下可重试的文件回收记录。"""
    return bool(client.eval(
        PROMOTE_PENDING_LUA,
        2,
        f"{STORAGE_PENDING_KEY}:{storage_id}",
        f"{STORAGE_GC_KEY}:{storage_id}",
        key,
        due_at,
        "1" if force else "0",
    ))


def _cleanup_storage_gc(batch_size: int = 1000) -> int:
    """先把到期待入队对象转入持久回收日志，再删除独占文件。"""
    client = redis.from_url(_gc_redis_url, decode_responses=True)
    removed = 0
    storage_id = _storage_id()
    try:
        redis_seconds, redis_microseconds = client.time()
        due_at = redis_seconds + redis_microseconds / 1_000_000
        pending_key = f"{STORAGE_PENDING_KEY}:{storage_id}"
        gc_key = f"{STORAGE_GC_KEY}:{storage_id}"
        due_pending = client.zrangebyscore(pending_key, "-inf", due_at, start=0, num=batch_size)
        for key in due_pending:
            # 转移一旦提交，入队脚本就再也看不到 pending；文件删除失败仍可重试。
            _promote_pending_gc(client, storage_id, key, due_at)

        due_keys = client.zrangebyscore(gc_key, "-inf", due_at, start=0, num=batch_size)
        for key in due_keys:
            try:
                with _retention_guard():
                    path = _key_path(key)
                    meta_path = _meta_path(key)
                    if not path.exists():
                        meta_path.unlink(missing_ok=True)
                        removed += 1
                    else:
                        metadata = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}
                        if not isinstance(metadata, dict):
                            raise ValueError("storage metadata must be an object")
                        if not metadata.get("dedicated"):
                            logger.warning(f"跳过非独占外存对象的自动删除 key={key}")
                        else:
                            path.unlink(missing_ok=True)
                            meta_path.unlink(missing_ok=True)
                            removed += 1
                client.zrem(gc_key, key)
            except (OSError, ValueError, TypeError) as exc:
                logger.warning(f"外存回收失败 key={key}: {exc}")
    finally:
        client.close()
    return removed


def _cleanup_expired():
    """删除超过 TTL 的缓存文件。"""
    if _ttl_seconds <= 0:
        return
    now = time.time()
    removed = 0
    for path in DATA_DIR.rglob("*"):
        if not _is_storage_file(path):
            continue
        try:
            with _retention_guard():
                if not path.exists():
                    continue
                meta_path = path.with_name(f"{path.name}.meta.json")
                retain_value = None
                if meta_path.exists():
                    try:
                        metadata = json.loads(meta_path.read_text(encoding="utf-8"))
                        if not isinstance(metadata, dict):
                            raise ValueError("storage metadata must be an object")
                        raw = metadata.get("retain_until")
                        retain_value = float(raw) if raw is not None else None
                        if retain_value is not None and (not math.isfinite(retain_value) or retain_value < 0):
                            raise ValueError("invalid retention")
                    except (OSError, ValueError, TypeError) as exc:
                        logger.warning(f"跳过保留期元数据不可读的外存对象 key={path.name}: {exc}")
                        continue
                # 0 表示显式持久保留；未来绝对时刻优先于全局 TTL。
                if retain_value == 0 or (retain_value and retain_value > now):
                    continue
                if retain_value or now - path.stat().st_mtime > _ttl_seconds:
                    path.unlink()
                    meta_path.unlink(missing_ok=True)
                    removed += 1
        except OSError:
            pass
    if removed:
        logger.info(f"TTL 清理完成，删除 {removed} 个过期文件")


def _start_cleanup_thread():
    global _cleanup_thread_started
    with _cleanup_thread_lock:
        if _cleanup_thread_started:
            return
        _cleanup_thread_started = True

    def _loop():
        last_ttl_cleanup = 0.0
        while True:
            time.sleep(_cleanup_interval)
            try:
                _cleanup_storage_gc()
                if time.time() - last_ttl_cleanup >= 3600:
                    _cleanup_expired()
                    last_ttl_cleanup = time.time()
            except Exception:
                logger.exception("TTL 清理异常")

    t = threading.Thread(target=_loop, daemon=True)
    t.start()


@app.on_event("startup")
def start_cleanup_on_app_startup():
    _start_cleanup_thread()


@app.post("/api/storage/upload")
async def upload(
    file: Annotated[UploadFile | None, File()] = None,
    retain_until: Annotated[str | None, Form()] = None,
    managed: Annotated[str, Form()] = "0",
):
    if file is None:
        raise HTTPException(400, "missing 'file' field")

    data = await file.read(MAX_UPLOAD_BYTES + 1)
    if not data:
        raise HTTPException(400, "empty payload")
    if len(data) > MAX_UPLOAD_BYTES:
        raise HTTPException(413, f"payload exceeds {MAX_UPLOAD_BYTES} bytes")
    if managed not in {"0", "1"}:
        raise HTTPException(400, "invalid managed flag")

    # 先验证请求，再创建独占对象，避免无效请求留下孤儿文件。
    requested_retain: float | None
    if retain_until == "":
        requested_retain = 0
    elif retain_until is None:
        requested_retain = None
    else:
        try:
            requested_retain = float(retain_until)
        except ValueError as exc:
            raise HTTPException(400, "invalid retain_until") from exc
        if not math.isfinite(requested_retain) or requested_retain < 0:
            raise HTTPException(400, "invalid retain_until")

    key = _generate_key(data)
    path = _key_path(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with _retention_guard():
        created = not path.exists()
        if created:
            size = path.write_bytes(data)
        else:
            size = path.stat().st_size

        # 托管对象先设置有限保留期，避免待入队日志写入前崩溃造成永久孤儿。
        provisional_until = time.time() + PENDING_GRACE_SECONDS + 3600
        effective_retain = _extend_retention_locked(
            key, provisional_until if managed == "1" else requested_retain, dedicated=True
        )
    storage_id = _storage_id()
    if managed == "1":
        client = None
        try:
            client = redis.from_url(_gc_redis_url, decode_responses=True)
            seconds, microseconds = client.time()
            client.zadd(
                f"{STORAGE_PENDING_KEY}:{storage_id}",
                {key: seconds + microseconds / 1_000_000 + PENDING_GRACE_SECONDS},
            )
        except (redis.RedisError, ValueError) as exc:
            with _retention_guard():
                path.unlink(missing_ok=True)
                _meta_path(key).unlink(missing_ok=True)
            raise HTTPException(503, "storage GC Redis unavailable") from exc
        finally:
            if client is not None:
                client.close()
        effective_retain = _extend_retention(key, 0, dedicated=True)
    logger.info(f"upload: key={key} size={size}")
    return {
        "key": key,
        "created": created,
        "dedicated": True,
        "storage_id": storage_id,
        "pending": managed == "1",
        "retain_until": effective_retain,
        "size": size,
    }


@app.get("/api/storage/download/{key}")
async def download(key: str):
    try:
        path = _key_path(key)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    with _retention_guard():
        try:
            handle = path.open("rb")
        except FileNotFoundError as exc:
            raise HTTPException(404, f"key not found: {key}") from exc
    with handle:
        data = handle.read()
    logger.debug(f"download: key={key} size={len(data)}")
    return Response(content=data, media_type="application/octet-stream")


@app.delete("/api/storage/delete/{key}")
async def delete(key: str):
    try:
        path = _key_path(key)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    client = redis.from_url(_gc_redis_url, decode_responses=True)
    try:
        storage_id = _storage_id()
        seconds, microseconds = client.time()
        _promote_pending_gc(
            client, storage_id, key, seconds + microseconds / 1_000_000, force=True
        )
        with _retention_guard():
            if not path.exists():
                _meta_path(key).unlink(missing_ok=True)
                missing = True
            else:
                path.unlink()
                _meta_path(key).unlink(missing_ok=True)
                missing = False
        pipe = client.pipeline(transaction=True)
        pipe.zrem(f"{STORAGE_GC_KEY}:{storage_id}", key)
        pipe.delete(f"{STORAGE_CLAIM_KEY}:{storage_id}:{key}")
        pipe.execute()
        if missing:
            raise HTTPException(404, f"key not found: {key}")
    except redis.RedisError as exc:
        raise HTTPException(503, "storage GC Redis unavailable") from exc
    finally:
        client.close()
    logger.info(f"delete: key={key}")
    return {"deleted": key}


class RetentionRequest(BaseModel):
    """共享内容对象的单调保留期延长请求。"""

    retain_until: float | None = None


def _extend_retention(
    key: str,
    requested: float | None,
    *,
    dedicated: bool | None = None,
) -> float | None:
    """单调延长 retain_until；0 表示持久保留，None 表示沿用服务端 TTL。"""
    with _retention_guard():
        return _extend_retention_locked(key, requested, dedicated=dedicated)


def _extend_retention_locked(
    key: str, requested: float | None, *, dedicated: bool | None = None,
) -> float | None:
    """调用方持有保留期锁；元数据不可读时禁止猜测和覆盖。"""
    meta_path = _meta_path(key)
    existing: float | None = None
    metadata: dict[str, object] = {}
    if meta_path.exists():
        parsed = json.loads(meta_path.read_text(encoding="utf-8"))
        if not isinstance(parsed, dict):
            raise ValueError("storage metadata must be an object")
        metadata = parsed
        raw_existing = metadata.get("retain_until")
        existing = float(str(raw_existing)) if raw_existing is not None else None
        if existing is not None and (not math.isfinite(existing) or existing < 0):
            raise ValueError("invalid storage retention")
    if existing == 0 or requested == 0:
        effective: float | None = 0
    elif existing is None:
        effective = requested
    elif requested is None:
        effective = existing
    else:
        effective = max(existing, requested)
    if dedicated is not None:
        metadata["dedicated"] = dedicated
    if effective is not None or dedicated is not None:
        metadata["retain_until"] = effective
        temporary: str | None = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=meta_path.parent,
                                             prefix=f".{key}.", suffix=".tmp", delete=False) as handle:
                temporary = handle.name
                json.dump(metadata, handle)
            os.replace(temporary, meta_path)
            temporary = None
        finally:
            if temporary is not None:
                Path(temporary).unlink(missing_ok=True)
    return effective


@app.post("/api/storage/retain/{key}")
async def retain(key: str, request: RetentionRequest):
    if request.retain_until is not None and (
        not math.isfinite(request.retain_until) or request.retain_until < 0
    ):
        raise HTTPException(400, "invalid retain_until")
    try:
        path = _key_path(key)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    with _retention_guard():
        if not path.exists():
            raise HTTPException(404, f"key not found: {key}")
        return {"key": key, "retain_until": _extend_retention_locked(key, request.retain_until or 0)}


@app.get("/api/storage/health")
async def health():
    count = 0
    total_size = 0
    for path in DATA_DIR.rglob("*"):
        try:
            if _is_storage_file(path):
                total_size += path.stat().st_size
                count += 1
        except FileNotFoundError:
            continue
    return {
        "status": "ok",
        "files": count,
        "total_size": total_size,
        "ttl_days": _ttl_seconds / 86400 if _ttl_seconds > 0 else None,
        "data_dir": str(DATA_DIR),
    }


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="qtask RemoteStorage server")
    p.add_argument("--port", type=int, default=8096)
    p.add_argument("--data-dir", type=str, default=str(DEFAULT_DIR))
    p.add_argument("--host", type=str, default="127.0.0.1")
    p.add_argument("--ttl-days", type=float, default=7.0, help="文件保留天数，0=永不过期")
    p.add_argument("--gc-redis", type=str, default=_gc_redis_url, help="外存回收日志所在 Redis")
    args = p.parse_args()

    if args.host not in {"127.0.0.1", "::1", "localhost"} and not os.environ.get(
        "QTASK_STORAGE_TOKEN"
    ):
        p.error("远程监听需要设置 QTASK_STORAGE_TOKEN")

    configure(args.data_dir, args.ttl_days, args.gc_redis)

    logger.info(f"RemoteStorage 启动: data_dir={DATA_DIR}, ttl={args.ttl_days}天, port={args.port}")
    _start_cleanup_thread()

    uvicorn.run(app, host=args.host, port=args.port)
