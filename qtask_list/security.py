"""运维界面可展示的连接信息，避免凭据进入日志和 HTTP 响应。"""

from __future__ import annotations

import ipaddress
from urllib.parse import urlsplit

_PUBLIC_STORAGE_ERROR_CODES = frozenset({
    "storage_protocol", "storage_timeout", "storage_connection", "storage_network",
    "storage_object_missing", "storage_auth", "storage_throttled", "storage_transient",
    "storage_request", "storage_not_configured", "invalid_payload_ref", "invalid_payload_kind",
    "invalid_payload", "payload_decompress", "payload_size_mismatch", "payload_checksum_mismatch",
    "configuration", "payload_decode", "payload_json", "invalid_envelope",
})


def safe_storage_error_code(code: object) -> str:
    """仅展示库定义的错误码，避免第三方异常将凭据写入错误码。"""
    return code if isinstance(code, str) and code in _PUBLIC_STORAGE_ERROR_CODES else "storage_error"


def safe_storage_error_note(code: object) -> str:
    return f"payload 或外部存储操作失败（{safe_storage_error_code(code)}），请核对配置及任务状态后重试"


def origin_tuple(value: str) -> tuple[str, str, int] | None:
    """解析 HTTP(S) 来源，按默认端口规范化后比较。"""
    try:
        parsed = urlsplit(value)
        if (parsed.scheme not in {"http", "https"} or not parsed.hostname
                or parsed.username is not None or parsed.password is not None
                or parsed.path not in {"", "/"} or parsed.query or parsed.fragment):
            return None
        port = parsed.port if parsed.port is not None else (443 if parsed.scheme == "https" else 80)
        return parsed.scheme, parsed.hostname.lower(), port
    except ValueError:
        return None


def canonical_origin(value: str) -> str | None:
    """生成浏览器使用的来源形式，去掉默认端口。"""
    parts = origin_tuple(value)
    if parts is None:
        return None
    scheme, host, port = parts
    host = f"[{host}]" if ":" in host else host
    suffix = "" if port == (443 if scheme == "https" else 80) else f":{port}"
    return f"{scheme}://{host}{suffix}"


def redis_endpoint_label(url: str) -> str:
    """只展示 Redis 主机与端口，不展示认证、路径及查询参数。"""
    try:
        parsed = urlsplit(url)
        if parsed.scheme not in {"redis", "rediss"} or not parsed.hostname:
            return "Redis（已配置）"
        host = parsed.hostname
        if ":" in host:
            host = f"[{host}]"
        port = f":{parsed.port}" if parsed.port is not None else ""
        return f"{parsed.scheme}://{host}{port}"
    except ValueError:
        return "Redis（已配置）"


def is_loopback_host(host: str) -> bool:
    """判断监听地址是否仅属于本机。"""
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False
