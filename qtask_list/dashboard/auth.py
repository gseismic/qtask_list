import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from dataclasses import dataclass
from typing import Any, Optional

from fastapi import HTTPException, Request, status


COOKIE_NAME = "qtask_dashboard_session"


@dataclass(frozen=True)
class AuthSettings:
    enabled: bool
    username: str
    password: str
    secret: str
    session_ttl: int
    secure_cookie: bool


def _truthy(value: Optional[str]) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def get_auth_settings() -> AuthSettings:
    password = os.environ.get("QTASK_DASHBOARD_PASSWORD", "")
    forced = _truthy(os.environ.get("QTASK_DASHBOARD_AUTH"))
    enabled = bool(password) or forced
    username = os.environ.get("QTASK_DASHBOARD_USER", "admin")
    secret = os.environ.get("QTASK_DASHBOARD_SECRET", "") or password
    try:
        session_ttl = int(os.environ.get("QTASK_DASHBOARD_SESSION_TTL", "86400"))
    except ValueError:
        session_ttl = 0
    secure_cookie = _truthy(os.environ.get("QTASK_DASHBOARD_SECURE_COOKIE"))

    return AuthSettings(
        enabled=enabled,
        username=username,
        password=password,
        secret=secret,
        session_ttl=session_ttl,
        secure_cookie=secure_cookie,
    )


def auth_config_error(settings: Optional[AuthSettings] = None) -> Optional[str]:
    settings = settings or get_auth_settings()
    if not settings.enabled:
        return None
    if not settings.password:
        return "QTASK_DASHBOARD_AUTH 已启用，但未设置 QTASK_DASHBOARD_PASSWORD"
    if settings.session_ttl <= 0:
        return "QTASK_DASHBOARD_SESSION_TTL 必须为正整数"
    return None


def verify_credentials(username: str, password: str) -> bool:
    settings = get_auth_settings()
    if not settings.enabled:
        return True
    if auth_config_error(settings):
        return False
    return hmac.compare_digest(username.encode("utf-8"), settings.username.encode("utf-8")) and hmac.compare_digest(
        password.encode("utf-8"),
        settings.password.encode("utf-8"),
    )


def create_session_token(username: str, now: Optional[int] = None) -> str:
    settings = get_auth_settings()
    issued_at = int(now if now is not None else time.time())
    payload = {
        "sub": username,
        "iat": issued_at,
        "exp": issued_at + settings.session_ttl,
        "nonce": secrets.token_urlsafe(12),
    }
    raw_payload = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    encoded = base64.urlsafe_b64encode(raw_payload).decode().rstrip("=")
    signature = _sign(encoded, settings.secret)
    return f"{encoded}.{signature}"


def validate_session_token(token: Optional[str], now: Optional[int] = None) -> bool:
    settings = get_auth_settings()
    if not settings.enabled:
        return True
    if not token or not token.isascii() or "." not in token or auth_config_error(settings):
        return False

    encoded, signature = token.rsplit(".", 1)
    expected = _sign(encoded, settings.secret)
    if not hmac.compare_digest(signature, expected):
        return False

    try:
        payload = _decode_payload(encoded)
    except (ValueError, json.JSONDecodeError):
        return False

    current_time = int(now if now is not None else time.time())
    return (
        payload.get("sub") == settings.username
        and isinstance(payload.get("exp"), int)
        and int(payload["exp"]) > current_time
    )


def is_request_authenticated(request: Request) -> bool:
    return validate_session_token(request.cookies.get(COOKIE_NAME))


def require_auth(request: Request) -> None:
    settings = get_auth_settings()
    config_error = auth_config_error(settings)
    if config_error:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=config_error)
    if settings.enabled and not is_request_authenticated(request):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")


def _sign(encoded_payload: str, secret: str) -> str:
    return hmac.new(
        secret.encode(),
        encoded_payload.encode(),
        hashlib.sha256,
    ).hexdigest()


def _decode_payload(encoded_payload: str) -> dict[str, Any]:
    padding = "=" * (-len(encoded_payload) % 4)
    raw = base64.urlsafe_b64decode(f"{encoded_payload}{padding}".encode())
    parsed = json.loads(raw.decode())
    if not isinstance(parsed, dict):
        raise ValueError("Invalid session payload")
    return parsed
