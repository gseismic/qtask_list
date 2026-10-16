"""V2 任务信封编解码与 V1 兼容读取。"""

from __future__ import annotations

import base64
import hashlib
import json
import threading
from dataclasses import dataclass
from typing import Any, Mapping, cast

import orjson
import zstandard

from .clock import datetime_to_epoch
from .errors import (
    PayloadChecksumError,
    PayloadDecodeError,
    RemoteStorageConfigurationError,
)
from .models import JsonValue, TaskSpec
from .storage import RemoteStorage, StoredObject


@dataclass(frozen=True)
class PreparedPayload:
    """已准备写入信封的 payload 描述及上传清理信息。"""

    descriptor: dict[str, Any]
    kind: str
    size: int
    sha256: str
    external_key: str | None = None
    external_created: bool = False
    external_dedicated: bool = False


@dataclass(frozen=True)
class EnvelopeHeader:
    """无需解压或访问外存即可读取的信封头。"""

    version: int
    task_id: str
    action: str
    attempt: int
    max_attempts: int
    logical_key: str | None
    scheduled_for: float | None
    start_deadline_at: float | None
    trace_id: str | None
    parent_task_id: str | None
    replay_of: str | None
    concurrency_key: str | None
    supersede_key: str | None
    supersede_version: int | str | None
    lease_token: str | None
    raw: dict[str, Any]


class EnvelopeCodec:
    """统一处理 inline、zstd、external payload，保证 handler 输入一致。"""

    def __init__(
        self,
        *,
        storage: RemoteStorage | None = None,
        large_threshold: int = 50 * 1024,
        compress_threshold: int = 50 * 1024,
    ):
        if large_threshold < 0 or compress_threshold < 0:
            raise ValueError("payload thresholds must be >= 0")
        self.storage = storage
        self.large_threshold = large_threshold
        self.compress_threshold = compress_threshold
        # zstandard 上下文不是线程安全的，每个 Worker 线程独立持有。
        self._tls = threading.local()

    def _compressor(self) -> zstandard.ZstdCompressor:
        compressor = getattr(self._tls, "compressor", None)
        if compressor is None:
            compressor = zstandard.ZstdCompressor()
            self._tls.compressor = compressor
        return cast(zstandard.ZstdCompressor, compressor)

    def _decompressor(self) -> zstandard.ZstdDecompressor:
        decompressor = getattr(self._tls, "decompressor", None)
        if decompressor is None:
            decompressor = zstandard.ZstdDecompressor()
            self._tls.decompressor = decompressor
        return cast(zstandard.ZstdDecompressor, decompressor)

    @staticmethod
    def _payload_bytes(payload: Mapping[str, JsonValue]) -> bytes:
        try:
            data = orjson.dumps(payload)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"payload must be a JSON-serializable object: {exc}") from exc
        decoded = orjson.loads(data)
        if not isinstance(decoded, dict):
            raise ValueError("payload must serialize to a JSON object")
        return data

    def prepare(
        self,
        payload: Mapping[str, JsonValue],
        *,
        retain_until: float | None,
    ) -> PreparedPayload:
        """序列化 payload；任何失败都发生在 identity 占用之前。"""
        data = self._payload_bytes(payload)
        digest = hashlib.sha256(data).hexdigest()

        if self.storage is not None and len(data) > self.large_threshold:
            if hasattr(self.storage, "save"):
                save_managed = getattr(self.storage, "save_managed", None)
                stored = (
                    save_managed(data, retain_until=retain_until)
                    if save_managed is not None
                    else self.storage.save(data, retain_until=retain_until)
                )
            else:
                # 兼容只实现旧 save_bytes/load 契约的自定义 storage。
                try:
                    key = self.storage.save_bytes(data, retain_until=retain_until)
                except TypeError:
                    key = self.storage.save_bytes(data)
                stored = StoredObject(key=str(key), created=False, retain_until=retain_until)
            return PreparedPayload(
                descriptor={
                    "kind": "external",
                    "key": stored.key,
                    "size": len(data),
                    "sha256": digest,
                    "retain_until": stored.retain_until,
                    "dedicated": bool(getattr(stored, "dedicated", False)),
                    "storage_id": getattr(stored, "storage_id", None),
                    "pending": bool(getattr(stored, "pending", False)),
                },
                kind="external",
                size=len(data),
                sha256=digest,
                external_key=stored.key,
                external_created=stored.created,
                external_dedicated=bool(getattr(stored, "dedicated", False)),
            )

        if len(data) > self.compress_threshold:
            compressed = self._compressor().compress(data)
            return PreparedPayload(
                descriptor={
                    "kind": "zstd",
                    "data": base64.b64encode(compressed).decode("ascii"),
                    "size": len(data),
                    "sha256": digest,
                },
                kind="zstd",
                size=len(data),
                sha256=digest,
            )

        return PreparedPayload(
            descriptor={"kind": "inline", "data": orjson.loads(data)},
            kind="inline",
            size=len(data),
            sha256=digest,
        )

    @staticmethod
    def encode(envelope: Mapping[str, Any]) -> str:
        """把信封编码成 Redis 消息。"""
        return orjson.dumps(envelope).decode("utf-8")

    @staticmethod
    def runtime_tail(envelope: Mapping[str, Any], field: str, **changes: Any) -> str:
        """由 Python 保真序列化 JSON，供 Lua 只在最前面填一个运行字段。"""
        data = dict(envelope)
        data.update(changes)
        data.pop(field, None)
        return "," + EnvelopeCodec.encode(data)[1:]

    def build(
        self,
        *,
        task_id: str,
        spec: TaskSpec,
        prepared: PreparedPayload,
        created_at: float,
        available_at: float,
        max_attempts: int,
        replay_of: str | None = None,
        duplicate_override_of: str | None = None,
    ) -> dict[str, Any]:
        """构造 V2 信封；仅写非空可选字段，减少高频消息体积。"""
        envelope: dict[str, Any] = {
            "version": 2,
            "task_id": task_id,
            "action": spec.action,
            "attempt": 0,
            "max_attempts": max_attempts,
            "created_at": created_at,
            "available_at": available_at,
            "delay_reason": "schedule" if spec.not_before_at is not None else "enqueue",
            "payload": prepared.descriptor,
        }
        optional = {
            "logical_key": spec.logical_key,
            "scheduled_for": datetime_to_epoch(spec.scheduled_for),
            "not_before_at": datetime_to_epoch(spec.not_before_at),
            "start_deadline_at": datetime_to_epoch(spec.start_deadline_at),
            "dedup_until": datetime_to_epoch(spec.dedup_until),
            "trace_id": spec.trace_id,
            "parent_task_id": spec.parent_task_id,
            "replay_of": replay_of,
            "concurrency_key": spec.concurrency_key,
            "supersede_key": spec.supersede_key,
            "supersede_version": spec.supersede_version,
            "duplicate_override_of": duplicate_override_of,
        }
        envelope.update({key: value for key, value in optional.items() if value is not None})
        return envelope

    @staticmethod
    def decode_raw(raw_message: str) -> dict[str, Any]:
        """只解析 JSON，不解压也不访问 RemoteStorage。"""
        try:
            value = orjson.loads(raw_message)
        except (orjson.JSONDecodeError, TypeError) as exc:
            raise PayloadDecodeError("invalid_envelope", f"消息不是合法 JSON: {exc}") from exc
        if not isinstance(value, dict):
            raise PayloadDecodeError("invalid_envelope", "消息信封必须是 JSON object")
        if not value.get("task_id"):
            raise PayloadDecodeError("invalid_envelope", "task_id is required")
        return cast(dict[str, Any], value)

    def header(self, raw_message: str) -> EnvelopeHeader:
        """读取 V2 或 V1 头部；V1 action 可能需要后续 payload 解码。"""
        data = self.decode_raw(raw_message)
        version = int(data.get("version", 1) or 1)
        if version == 2:
            action = data.get("action")
            if not isinstance(action, str) or not action:
                raise PayloadDecodeError("invalid_envelope", "V2 envelope action is required")
            deadline = self._float_or_none(data.get("start_deadline_at"))
            return EnvelopeHeader(
                version=2,
                task_id=str(data["task_id"]),
                action=action,
                attempt=int(data.get("attempt", 0) or 0),
                max_attempts=int(data.get("max_attempts", 1) or 1),
                logical_key=self._str_or_none(data.get("logical_key")),
                scheduled_for=self._float_or_none(data.get("scheduled_for")),
                start_deadline_at=deadline,
                trace_id=self._str_or_none(data.get("trace_id")),
                parent_task_id=self._str_or_none(data.get("parent_task_id")),
                replay_of=self._str_or_none(data.get("replay_of")),
                concurrency_key=self._str_or_none(data.get("concurrency_key")),
                supersede_key=self._str_or_none(data.get("supersede_key")),
                supersede_version=data.get("supersede_version"),
                lease_token=self._str_or_none(data.get("lease_token")),
                raw=data,
            )

        return EnvelopeHeader(
            version=1,
            task_id=str(data["task_id"]),
            action="",
            attempt=0,
            max_attempts=1,
            logical_key=None,
            scheduled_for=None,
            start_deadline_at=self._float_or_none(data.get("expires_at")),
            trace_id=None,
            parent_task_id=None,
            replay_of=None,
            concurrency_key=None,
            supersede_key=None,
            supersede_version=None,
            lease_token=None,
            raw=data,
        )

    def decode_payload(self, header: EnvelopeHeader) -> dict[str, JsonValue]:
        """在 begin_attempt 成功后还原业务 payload。"""
        if header.version == 1:
            return self._decode_v1_payload(header.raw.get("payload", {}))

        descriptor = header.raw.get("payload")
        if not isinstance(descriptor, dict):
            raise PayloadDecodeError("invalid_payload", "V2 payload descriptor must be an object")
        kind = descriptor.get("kind")
        if kind == "inline":
            payload = descriptor.get("data")
            if not isinstance(payload, dict):
                raise PayloadDecodeError("invalid_payload", "inline payload must be an object")
            return self._business_payload(cast(dict[str, JsonValue], payload), descriptor)

        if kind == "zstd":
            try:
                compressed = base64.b64decode(descriptor["data"], validate=True)
                raw = self._decompressor().decompress(compressed)
            except Exception as exc:
                raise PayloadDecodeError("payload_decompress", f"payload 解压失败: {exc}") from exc
            self._verify_payload(raw, descriptor)
            return self._business_payload(self._decode_json_object(raw), descriptor)

        if kind == "external":
            if self.storage is None:
                raise RemoteStorageConfigurationError(
                    "storage_not_configured",
                    "消息引用 RemoteStorage，但当前队列未配置 storage",
                )
            key = descriptor.get("key")
            if not isinstance(key, str) or not key:
                raise PayloadDecodeError("invalid_payload_ref", "external payload key is required")
            try:
                raw = self.storage.load(key)
            except RemoteStorageConfigurationError:
                raise
            except (ConnectionError, TimeoutError, OSError) as exc:
                from .errors import RemoteStorageTransientError

                raise RemoteStorageTransientError(
                    "storage_connection",
                    f"RemoteStorage 连接失败: {exc}",
                ) from exc
            self._verify_payload(raw, descriptor)
            return self._business_payload(self._decode_json_object(raw), descriptor)

        raise PayloadDecodeError("invalid_payload_kind", f"未知 payload kind: {kind!r}")

    @staticmethod
    def _business_payload(payload: dict[str, JsonValue], descriptor: Mapping[str, Any]) -> dict[str, JsonValue]:
        """仅旧格式迁移消息剥离运行字段，V2 业务同名字段保持原样。"""
        if descriptor.get("legacy"):
            payload = dict(payload)
            payload.pop("_retry", None)
        return payload

    def _decode_v1_payload(self, payload_raw: Any) -> dict[str, JsonValue]:
        payload = json.loads(payload_raw) if isinstance(payload_raw, str) else payload_raw
        if not isinstance(payload, dict):
            raise PayloadDecodeError("invalid_payload", "V1 payload must decode to an object")
        if payload.get("_large"):
            if self.storage is None:
                raise RemoteStorageConfigurationError(
                    "storage_not_configured",
                    "V1 消息引用 RemoteStorage，但当前队列未配置 storage",
                )
            try:
                raw = self.storage.load(str(payload.get("key", "")))
            except RemoteStorageConfigurationError:
                raise
            except (ConnectionError, TimeoutError, OSError) as exc:
                from .errors import RemoteStorageTransientError

                raise RemoteStorageTransientError(
                    "storage_connection",
                    f"RemoteStorage 连接失败: {exc}",
                ) from exc
            return self._decode_json_object(raw)
        if payload.get("_compressed"):
            try:
                compressed = base64.b64decode(payload["data"], validate=True)
                raw = self._decompressor().decompress(compressed)
            except Exception as exc:
                raise PayloadDecodeError("payload_decompress", f"V1 payload 解压失败: {exc}") from exc
            return self._decode_json_object(raw)
        return cast(dict[str, JsonValue], payload)

    @staticmethod
    def _verify_payload(raw: bytes, descriptor: Mapping[str, Any]) -> None:
        expected_size = descriptor.get("size")
        if expected_size is not None and int(expected_size) != len(raw):
            raise PayloadChecksumError(
                "payload_size_mismatch",
                f"payload size mismatch: expected={expected_size}, actual={len(raw)}",
            )
        expected_sha = descriptor.get("sha256")
        actual_sha = hashlib.sha256(raw).hexdigest()
        if expected_sha and str(expected_sha) != actual_sha:
            raise PayloadChecksumError(
                "payload_checksum_mismatch",
                f"payload sha256 mismatch: expected={expected_sha}, actual={actual_sha}",
            )

    @staticmethod
    def _decode_json_object(raw: bytes) -> dict[str, JsonValue]:
        try:
            value = orjson.loads(raw)
        except (orjson.JSONDecodeError, TypeError) as exc:
            raise PayloadDecodeError("payload_json", f"payload JSON 解码失败: {exc}") from exc
        if not isinstance(value, dict):
            raise PayloadDecodeError("payload_json", "payload JSON 必须是 object")
        return cast(dict[str, JsonValue], value)

    @staticmethod
    def _float_or_none(value: Any) -> float | None:
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError) as exc:
            raise PayloadDecodeError("invalid_envelope", f"invalid timestamp: {value!r}") from exc

    @staticmethod
    def _str_or_none(value: Any) -> str | None:
        return None if value in (None, "") else str(value)


def prepared_from_descriptor(descriptor: Mapping[str, Any]) -> PreparedPayload:
    """从历史记录恢复 payload descriptor；外存 replay 会下载并独立上传。"""
    kind = str(descriptor.get("kind", ""))
    if kind not in {"inline", "zstd", "external"}:
        raise PayloadDecodeError("invalid_payload_kind", f"未知 payload kind: {kind!r}")
    size = int(descriptor.get("size", 0) or 0)
    sha256 = str(descriptor.get("sha256", ""))
    if kind == "inline":
        raw = orjson.dumps(descriptor.get("data"))
        size = len(raw)
        sha256 = hashlib.sha256(raw).hexdigest()
    return PreparedPayload(
        descriptor=dict(descriptor),
        kind=kind,
        size=size,
        sha256=sha256,
        external_key=str(descriptor.get("key")) if kind == "external" else None,
        external_created=False,
        external_dedicated=bool(descriptor.get("dedicated", False)),
    )
