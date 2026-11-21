"""共用消息身份明细：事件去重与死信队列管理共享的消息来源身份模型。

背景
----
原先事件去重（dedup_filter）和死信登记（dlq_handler）各自维护一套消息身份
取值逻辑：

- 去重侧自己拼幂等键、算内容指纹；
- 死信侧自己对原始消息串做 sha256 截断指纹、只存截断后的消息体。

两边对“同一条消息”的身份认知互不相通，业务键相同但来自另一条队列或另一
接入通道的事件，容易被误当成同一条待重放记录。本模块把身份取值逻辑收拢为
一套 ``MessageIdentity`` 明细，归并消息来源身份时同时保留：

1. 去重字段：事件类型、业务键、去重键列、去重键取值、幂等键、内容指纹；
2. 死信原始消息数据：来源类型（队列/接入通道）、来源名称、队列偏移、
   通道编号、消费者凭证、投递序号、原始消息全文、消息指纹、来源坐标。

来源坐标（source_coordinate）把“来源 + 偏移/通道”纳入身份，业务键相同但
来源不同的两条消息会得到不同的来源坐标，从而不能互相替代。

本模块只做身份的构建、序列化与比对，不持有任何业务状态，事件去重与死信
队列管理都通过这里的工厂函数取得同构的身份明细。
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from typing import Any

# 消息来源类型：消息队列投递 / Webhook 等接入通道 / 未知
SOURCE_KIND_QUEUE = "queue"
SOURCE_KIND_CHANNEL = "channel"
SOURCE_KIND_UNKNOWN = "unknown"


def _safe_json_loads(raw_message: str) -> dict[str, Any]:
    """尽力把原始消息串解析成 payload dict，失败时返回空 dict。"""
    if not raw_message:
        return {}
    try:
        parsed = json.loads(raw_message)
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def extract_dedup_key_value(payload: dict[str, Any], key_fields: str) -> str:
    """按去重键列从 payload 中取出去重键取值（多键逗号分隔）。

    取值规则与历史 ``DedupFilterService._build_dedup_key`` 完全一致，保证
    已有去重规则仍按原字段判重：

    - ``id`` 键依次回退到 ``event_id``；
    - 命中顶层字段直接取值；
    - 兼容 ``data.<键>`` 形式的嵌套字段；
    - 其余情况在顶层字段里做同名兜底匹配。
    """
    if not key_fields:
        return ""

    parts: list[str] = []
    for key in key_fields.split(","):
        key = key.strip()
        if key == "id":
            val = payload.get("id") or payload.get("event_id", "")
            parts.append(str(val))
        elif key in payload:
            parts.append(str(payload[key]))
        elif f"data.{key}" in str(payload.get("data", {})):
            parts.append(str(payload.get("data", {}).get(key, "")))
        else:
            for dk, dv in payload.items():
                if dk == key:
                    parts.append(str(dv))
                    break
    return "|".join(parts)


def build_idempotency_key(event_type: str, payload: dict[str, Any], key_fields: str) -> str:
    """构建去重幂等键，保持历史拼接方式：事件类型前置，竖线连接。"""
    if not key_fields:
        return ""
    key_value = extract_dedup_key_value(payload, key_fields)
    return "|".join([event_type, key_value])


def compute_content_fingerprint(payload: dict[str, Any]) -> str:
    """计算 payload 的内容指纹（规范化 JSON 后 sha256）。"""
    content = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(content.encode()).hexdigest()


def compute_message_fingerprint(raw_message: str) -> str:
    """计算原始消息串的消息指纹（sha256 前 16 位，兼容死信历史口径）。"""
    return hashlib.sha256((raw_message or "").encode()).hexdigest()[:16]


def extract_business_key(payload: dict[str, Any], key_fields: str = "id") -> str:
    """提取业务键：优先去重键列取值，其次 id/event_id 兜底。"""
    key_value = extract_dedup_key_value(payload, key_fields or "id")
    if key_value:
        return key_value
    return str(payload.get("id") or payload.get("event_id") or "")


def build_source_coordinate(source_kind: str, source_name: str,
                            source_offset: Any = None, channel_id: Any = None) -> str:
    """构建来源坐标。

    - 队列消息：``queue:<队列名>#offset:<偏移>``
    - 通道事件：``channel:<通道编号或名称>``
    - 未知来源：``unknown:<名称>#offset:<偏移>``

    同一队列偏移的多次投递（含服务重启后重读）坐标相同；业务键相同但来自
    另一条队列或另一个通道时坐标不同，不能互相顶替。
    """
    kind = source_kind or SOURCE_KIND_UNKNOWN
    if kind == SOURCE_KIND_CHANNEL:
        channel = channel_id or source_name or "unknown"
        return f"channel:{channel}"
    name = source_name or "unknown"
    return f"{kind}:{name}#offset:{'' if source_offset is None else source_offset}"


@dataclass
class MessageIdentity:
    """共用消息身份明细。

    同时覆盖事件去重所需的判重字段和死信登记所需的原始消息数据，两套模块
    共用同一份结构，跨模块关联原队列消息与实际消费凭证。
    """

    # —— 来源身份 ——
    source_kind: str = SOURCE_KIND_UNKNOWN
    source_name: str = ""
    source_offset: str | None = None
    channel_id: str | None = None
    consumer_id: int | None = None
    delivery_attempt: int = 1
    source_coordinate: str = ""

    # —— 去重字段 ——
    event_type: str = ""
    business_key: str = ""
    dedup_key_fields: str = "id"
    dedup_key_value: str = ""
    idempotency_key: str = ""
    content_fingerprint: str = ""

    # —— 死信原始消息数据 ——
    raw_message: str = ""
    message_fingerprint: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MessageIdentity":
        fields = {f for f in cls.__dataclass_fields__}  # type: ignore[attr-defined]
        return cls(**{k: v for k, v in (data or {}).items() if k in fields})

    def same_source(self, other: "MessageIdentity") -> bool:
        """两条身份是否来自同一个来源坐标（队列+偏移或同一通道）。"""
        return bool(self.source_coordinate) and self.source_coordinate == other.source_coordinate


def _build_identity(
    *,
    source_kind: str,
    source_name: str,
    raw_message: str,
    payload: dict[str, Any] | None,
    event_type: str,
    dedup_key_fields: str,
    source_offset: Any,
    channel_id: Any,
    consumer_id: int | None,
    delivery_attempt: int,
) -> MessageIdentity:
    payload = dict(payload or {})
    if not payload and raw_message:
        payload = _safe_json_loads(raw_message)

    message_fingerprint = compute_message_fingerprint(raw_message)
    content_fingerprint = compute_content_fingerprint(payload) if payload else ""
    idempotency_key = build_idempotency_key(event_type, payload, dedup_key_fields)
    business_key = extract_business_key(payload, dedup_key_fields)
    dedup_key_value = extract_dedup_key_value(payload, dedup_key_fields)
    coordinate = build_source_coordinate(
        source_kind, source_name, source_offset=source_offset, channel_id=channel_id
    )

    return MessageIdentity(
        source_kind=source_kind,
        source_name=source_name,
        source_offset=None if source_offset is None else str(source_offset),
        channel_id=None if channel_id is None else str(channel_id),
        consumer_id=consumer_id,
        delivery_attempt=delivery_attempt,
        source_coordinate=coordinate,
        event_type=event_type or "",
        business_key=business_key,
        dedup_key_fields=dedup_key_fields or "id",
        dedup_key_value=dedup_key_value,
        idempotency_key=idempotency_key,
        content_fingerprint=content_fingerprint,
        raw_message=raw_message or "",
        message_fingerprint=message_fingerprint,
    )


def build_queue_identity(
    *,
    source_queue: str,
    original_message: str,
    payload: dict[str, Any] | None = None,
    event_type: str = "",
    dedup_key_fields: str = "id",
    source_offset: Any = None,
    consumer_id: int | None = None,
    delivery_attempt: int = 1,
) -> MessageIdentity:
    """构建队列投递消息的身份明细（死信登记/消息消费使用）。"""
    return _build_identity(
        source_kind=SOURCE_KIND_QUEUE,
        source_name=source_queue or "",
        raw_message=original_message or "",
        payload=payload,
        event_type=event_type,
        dedup_key_fields=dedup_key_fields,
        source_offset=source_offset,
        channel_id=None,
        consumer_id=consumer_id,
        delivery_attempt=delivery_attempt,
    )


def build_channel_identity(
    *,
    payload: dict[str, Any],
    event_type: str,
    channel_id: Any = None,
    channel_name: str = "",
    dedup_key_fields: str = "id",
    raw_message: str = "",
    delivery_attempt: int = 1,
) -> MessageIdentity:
    """构建接入通道（Webhook 等）事件的身份明细。"""
    if not raw_message:
        raw_message = json.dumps(payload or {}, sort_keys=True, ensure_ascii=False)
    return _build_identity(
        source_kind=SOURCE_KIND_CHANNEL,
        source_name=channel_name or (str(channel_id) if channel_id is not None else ""),
        raw_message=raw_message,
        payload=payload,
        event_type=event_type,
        dedup_key_fields=dedup_key_fields,
        source_offset=None,
        channel_id=channel_id,
        consumer_id=None,
        delivery_attempt=delivery_attempt,
    )


def build_event_identity(
    *,
    event_type: str,
    payload: dict[str, Any],
    dedup_key_fields: str = "id",
    source_kind: str = SOURCE_KIND_UNKNOWN,
    source_name: str = "",
    source_offset: Any = None,
    channel_id: Any = None,
    consumer_id: int | None = None,
    raw_message: str = "",
    delivery_attempt: int = 1,
) -> MessageIdentity:
    """事件去重通用入口：在判重字段之外补全来源身份。"""
    if not raw_message:
        raw_message = json.dumps(payload or {}, sort_keys=True, ensure_ascii=False)
    return _build_identity(
        source_kind=source_kind or SOURCE_KIND_UNKNOWN,
        source_name=source_name,
        raw_message=raw_message,
        payload=payload,
        event_type=event_type,
        dedup_key_fields=dedup_key_fields,
        source_offset=source_offset,
        channel_id=channel_id,
        consumer_id=consumer_id,
        delivery_attempt=delivery_attempt,
    )
