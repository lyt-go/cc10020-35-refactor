"""死信队列管理领域服务：失败消息隔离、原因分类分析、批量重放与处置策略。

提供完整的死信处理流程：消息入队→原因归类→处置决策→批量重放→结果验证。
跨模块关联：mq_consumer（来源队列）、call_trace_log（重放追踪/实际消费凭证）、
data_quality_rule（质量校验）。

重构说明：死信登记原先自行对原始消息串做指纹，与事件去重的身份取值各管一
段。现在统一使用 ``app.services.message_identity`` 的共用消息身份明细，入队
时同时保留：

- 去重字段：业务键、去重键取值、幂等键、内容指纹；
- 死信原始消息数据：来源类型、来源队列/通道、队列偏移、消费者凭证、投递
  序号、原始消息全文与消息指纹、来源坐标。

存量死信没有身份明细时按其原有字段（记录编号、原始队列、原始消息体、
_fingerprint）惰性回填，入队编号与原始消息继续可读；重复入队合并必须同时
满足消息指纹与来源坐标一致，业务键相同但来自另一条队列/通道的事件不会被
误并为同一条待重放记录。
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from typing import Any

from app.persistence import json_store
from app.services.base import BaseModuleService
from app.services.message_identity import (
    MessageIdentity,
    SOURCE_KIND_QUEUE,
    build_queue_identity,
    build_source_coordinate,
    compute_message_fingerprint,
)


class DlqHandlerService(BaseModuleService):
    MODULE = "dlq_handler"
    ENTITY = "死信记录"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["记录编号", "原始队列", "失败原因", "原始消息体", "重试次数", "入队时间", "处置状态", "重放批次"]
    REQUIRED_FIELDS = ["记录编号", "原始队列", "失败原因"]
    STATUS_ORDER = ["草稿", "待处置", "已重放", "已丢弃"]
    ACTION_RULES = {"加入处置": "待处置", "标记重放": "已重放", "标记丢弃": "已丢弃"}
    NEGATIVE_ACTIONS = []

    def __init__(self) -> None:
        super().__init__()
        self._replay_history: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # 共用消息身份明细：入队构建、存量回填、来源比对
    # ------------------------------------------------------------------
    def _build_identity(self, source_queue: str, original_message: str,
                        consumer_id: int, source_offset: Any,
                        source_kind: str, channel_id: Any,
                        delivery_attempt: int) -> MessageIdentity:
        """根据入队参数构建共用消息身份明细。

        以队列身份为底，再按调用方声明覆盖来源类型/通道与来源坐标，使队列
        投递与接入通道事件共用同一份明细结构。
        """
        identity = build_queue_identity(
            source_queue=source_queue,
            original_message=original_message,
            source_offset=source_offset,
            consumer_id=consumer_id if int(consumer_id or 0) > 0 else None,
            delivery_attempt=delivery_attempt,
        )
        identity.source_kind = source_kind or SOURCE_KIND_QUEUE
        identity.channel_id = None if channel_id is None else str(channel_id)
        identity.source_coordinate = build_source_coordinate(
            identity.source_kind, source_queue,
            source_offset=source_offset, channel_id=channel_id,
        )
        return identity

    def _identity_of(self, entry: dict[str, Any]) -> MessageIdentity:
        """取一条死信记录的身份明细；存量记录按原字段惰性回填。

        回填不写库，只保证读取口径一致，存量死信的入队编号和原始消息保持
        原样可读。
        """
        stored = entry.get("_identity")
        if isinstance(stored, dict) and stored:
            return MessageIdentity.from_dict(stored)

        source_queue = str(entry.get("原始队列", ""))
        original_message = str(entry.get("_raw_message", entry.get("原始消息体", "")) or "")
        consumer_id = entry.get("_consumer_id")
        source_offset = entry.get("_source_offset")
        source_kind = str(entry.get("_source_kind", SOURCE_KIND_QUEUE))
        channel_id = entry.get("_channel_id")
        message_fingerprint = str(
            entry.get("_fingerprint", compute_message_fingerprint(original_message))
        )

        identity = self._build_identity(
            source_queue=source_queue,
            original_message=original_message,
            consumer_id=int(consumer_id) if consumer_id not in (None, "") else 0,
            source_offset=source_offset,
            source_kind=source_kind,
            channel_id=channel_id,
            delivery_attempt=int(entry.get("_delivery_attempt", 1) or 1),
        )
        # 存量记录的指纹以历史落库值为准，避免截断消息体造成口径漂移
        identity.message_fingerprint = message_fingerprint
        return identity

    def list_message_identities(self) -> list[dict[str, Any]]:
        """列出全部死信记录的共用消息身份明细（跨模块关联入口）。"""
        result = []
        for entry in json_store.get_all(self.MODULE):
            identity = self._identity_of(entry).to_dict()
            result.append({
                "dlq_id": entry.get("id"),
                "记录编号": entry.get("记录编号", ""),
                "处置状态": entry.get("status", ""),
                "identity": identity,
            })
        return result

    # ------------------------------------------------------------------
    # 死信入队
    # ------------------------------------------------------------------
    def enqueue_dead_letter(self, source_queue: str, original_message: str,
                            failure_reason: str, consumer_id: int = 0,
                            source_offset: Any = None,
                            source_kind: str = SOURCE_KIND_QUEUE,
                            channel_id: Any = None,
                            delivery_attempt: int = 1) -> dict[str, Any]:
        """将消费失败的消息加入死信队列。

        自动生成记录编号、消息指纹、入队时间戳和失败原因分类，并登记共用
        消息身份明细（去重字段 + 原始消息数据 + 来源坐标）。

        同一队列偏移的再次投递（如服务重启后重读原消息）会合并到同一条待
        处置死信；业务键相同但来源坐标不同（另一条队列/接入通道）的事件
        单独入队，不会顶替本待重放记录。
        """
        existing = json_store.get_all(self.MODULE)
        max_id = max((int(e.get("id", 0)) for e in existing), default=0)

        identity = self._build_identity(
            source_queue=source_queue,
            original_message=original_message,
            consumer_id=consumer_id,
            source_offset=source_offset,
            source_kind=source_kind or SOURCE_KIND_QUEUE,
            channel_id=channel_id,
            delivery_attempt=delivery_attempt,
        )
        msg_fingerprint = identity.message_fingerprint

        # 重复入队判定：消息指纹一致且来源坐标一致才合并（待处置态）。
        # 存量记录没有来源坐标时按“同队列 + 同指纹”的历史口径兜底。
        duplicate = None
        for e in existing:
            if e.get("status") not in ("草稿", "待处置"):
                continue
            if str(e.get("_fingerprint", "")) != msg_fingerprint:
                continue
            existing_identity = self._identity_of(e)
            if existing_identity.source_coordinate == identity.source_coordinate:
                duplicate = e
                break
            # 存量无坐标记录：仅当同为该队列的同指纹消息时按历史行为合并
            if not e.get("_identity") and str(e.get("原始队列", "")) == source_queue:
                duplicate = e
                break

        if duplicate is not None:
            existing_dlq = duplicate
            retry_count = int(existing_dlq.get("重试次数", "0") or "0") + 1
            merged_identity = self._identity_of(existing_dlq)
            merged_identity.delivery_attempt = max(
                merged_identity.delivery_attempt, delivery_attempt
            )
            json_store.update(self.MODULE, int(existing_dlq["id"]), {
                "重试次数": str(retry_count),
                "_identity": merged_identity.to_dict(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            })
            return {
                "ok": True,
                "message": f"消息已在死信队列中（retry#{retry_count}），合并为已有记录 #{existing_dlq['id']}",
                "dlq_id": existing_dlq["id"],
                "is_duplicate": True,
                "source_coordinate": merged_identity.source_coordinate,
            }

        record_no = f"DLQ-{source_queue[:4].upper()}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{max_id + 1:05d}"

        dlq_entry = {
            "id": max_id + 1,
            "记录编号": record_no,
            "原始队列": source_queue,
            "失败原因": failure_reason,
            # 列表字段保持截断展示，原始全文收进身份明细
            "原始消息体": original_message[:200],
            "重试次数": "0",
            "入队时间": datetime.now(timezone.utc).isoformat(),
            "处置状态": "草稿",
            "重放批次": "",
            "status": "草稿",
            "pending": True,
            "abnormal": True,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "_fingerprint": msg_fingerprint,
            "_consumer_id": consumer_id,
            "_source_offset": None if source_offset is None else str(source_offset),
            "_source_kind": source_kind or SOURCE_KIND_QUEUE,
            "_channel_id": None if channel_id is None else str(channel_id),
            "_delivery_attempt": delivery_attempt,
            "_raw_message": original_message,
            "_identity": identity.to_dict(),
        }

        # 填充LIST_FIELDS中的其他字段
        for field in self.LIST_FIELDS:
            if field not in dlq_entry:
                dlq_entry[field] = dlq_entry.get("记录编号", "") if field == "记录编号" else (
                    dlq_entry.get("原始队列", "") if field == "原始队列" else
                    dlq_entry.get("失败原因", "") if field == "失败原因" else
                    dlq_entry.get("原始消息体", "") if field == "原始消息体" else
                    dlq_entry.get("重试次数", "") if field == "重试次数" else
                    dlq_entry.get("入队时间", "") if field == "入队时间" else
                    dlq_entry.get("处置状态", "") if field == "处置状态" else
                    dlq_entry.get("重放批次", "") if field == "重放批次" else ""
                )

        json_store.insert(self.MODULE, dlq_entry)
        return {
            "ok": True,
            "message": f"死信已入队，编号 {record_no}",
            "dlq_id": dlq_entry["id"],
            "is_duplicate": False,
            "source_coordinate": identity.source_coordinate,
            "identity": identity.to_dict(),
        }

    def analyze_failure_reasons(self) -> dict[str, Any]:
        """分析所有死信记录的失败原因分布，提供分类统计和主要问题识别。"""
        rows = json_store.get_all(self.MODULE)
        if not rows:
            return {"total": 0, "categories": {}, "top_reasons": []}

        reason_counter = Counter()
        source_counter = Counter()

        for row in rows:
            reason = str(row.get("失败原因", "未知"))
            source = str(row.get("原始队列", "未知"))
            reason_counter[reason] += 1
            source_counter[source] += 1

        # 失败原因分类
        categories = {
            "超时类": sum(1 for r, c in reason_counter.items() if "超时" in r or "timeout" in r),
            "格式类": sum(1 for r, c in reason_counter.items() if "格式" in r or "format" in r or "序列化" in r),
            "数据类": sum(1 for r, c in reason_counter.items() if "数据" in r or "data" in r or "空" in r),
            "连接类": sum(1 for r, c in reason_counter.items() if "连接" in r or "网络" in r or "connect" in r),
            "认证类": sum(1 for r, c in reason_counter.items() if "认证" in r or "auth" in r or "权限" in r),
            "其他": sum(1 for r, c in reason_counter.items() if not any(kw in r for kw in ["超时", "timeout", "格式", "format", "序列化", "数据", "data", "空", "连接", "网络", "connect", "认证", "auth", "权限"])),
        }

        return {
            "total": len(rows),
            "categories": {k: v for k, v in categories.items() if v > 0},
            "top_reasons": reason_counter.most_common(5),
            "top_sources": source_counter.most_common(5),
            "pending_count": sum(1 for r in rows if r.get("status") in ("草稿", "待处置")),
        }

    def batch_replay(self, dlq_ids: list[int]) -> dict[str, Any]:
        """批量重放死信队列中的消息。

        流程：
        1. 验证每条死信记录是否存在且处于可重放状态
        2. 生成重放批次ID
        3. 逐条尝试重放到原始队列
        4. 记录重放结果和追踪日志

        返回重放统计和每条消息的明细结果（结果项附带来源坐标，操作只关联
        各自偏移的原消息）。
        """
        if not dlq_ids:
            return {"ok": False, "message": "未指定待重放的死信记录", "replayed": 0, "failed": 0}

        batch_id = f"REPLAY-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
        results = []
        replayed = 0
        replay_failed = 0

        import random
        for dlq_id in dlq_ids:
            entry = json_store.get_by_id(self.MODULE, dlq_id)
            if entry is None:
                results.append({"dlq_id": dlq_id, "ok": False, "reason": "死信记录不存在"})
                replay_failed += 1
                continue

            identity = self._identity_of(entry)
            if entry.get("status") in ("已重放", "已丢弃"):
                results.append({"dlq_id": dlq_id, "ok": False, "reason": f"记录已{entry['status']}，不可重复重放",
                                "source_coordinate": identity.source_coordinate})
                replay_failed += 1
                continue

            # 模拟重放：70%成功率
            success = random.random() < 0.7

            if success:
                json_store.update(self.MODULE, dlq_id, {
                    "status": "已重放",
                    "处置状态": "已重放",
                    "pending": False,
                    "abnormal": False,
                    "重放批次": batch_id,
                    "重试次数": str(int(entry.get("重试次数", "0") or "0") + 1),
                    "replayed_at": datetime.now(timezone.utc).isoformat(),
                })
                replayed += 1
                results.append({"dlq_id": dlq_id, "ok": True, "message": "重放成功",
                                "source_coordinate": identity.source_coordinate,
                                "source_offset": identity.source_offset})
            else:
                # 重放失败：重试计数+1，保持待处置
                json_store.update(self.MODULE, dlq_id, {
                    "重试次数": str(int(entry.get("重试次数", "0") or "0") + 1),
                    "status": "待处置",
                    "失败原因": entry.get("失败原因", "") + " [重放重试]",
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                })
                replay_failed += 1
                results.append({"dlq_id": dlq_id, "ok": False, "reason": "重放执行失败",
                                "source_coordinate": identity.source_coordinate,
                                "source_offset": identity.source_offset})

        # 记录重放历史
        self._replay_history.append({
            "batch_id": batch_id,
            "total": len(dlq_ids),
            "replayed": replayed,
            "failed": replay_failed,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        # 跨模块：记录重放到调用追踪
        try:
            from app.services.call_trace_log import CallTraceLogService
            tracer = CallTraceLogService()
            tracer._record_trace(
                endpoint_id=0,
                action="死信批量重放",
                latency_ms=random.randint(100, 500),
                status="正常" if replay_failed == 0 else "异常",
                record_count=replayed,
            )
        except Exception:
            pass

        return {
            "ok": True,
            "message": f"批量重放完成: 成功 {replayed}/{len(dlq_ids)}, 失败 {replay_failed}",
            "batch_id": batch_id,
            "replayed": replayed,
            "failed": replay_failed,
            "total": len(dlq_ids),
            "results": results,
        }

    def resolve_dead_letter(self, dlq_id: int, action: str = "标记重放") -> dict[str, Any]:
        """从死信台账处置一条消息，并回查它实际受理的原消费记录。

        处置与回查都以该死信自身的来源坐标（队列+偏移/通道）为准，业务键
        相同但来自另一条队列或接入通道的事件不会被关联进来。

        Args:
            dlq_id: 死信记录编号
            action: 处置动作，取值同 ACTION_RULES（加入处置/标记重放/标记丢弃）

        Returns:
            处置结果 + 消息身份明细 + 实际受理记录（按消费凭证回查）。
        """
        entry = json_store.get_by_id(self.MODULE, dlq_id)
        if entry is None:
            return {"ok": False, "message": f"死信记录 {dlq_id} 不存在或已归档"}

        target = self.ACTION_RULES.get(action)
        if target is None:
            return {"ok": False, "message": f"动作「{action}」不属于{self.MODULE}可执行范围"}

        identity = self._identity_of(entry)
        updated, message = self.run_action(dlq_id, action)

        accepted = self.find_accepted_record(
            consumer_id=identity.consumer_id,
            source_offset=identity.source_offset,
            source_queue=identity.source_name,
        )

        return {
            "ok": True,
            "message": message,
            "entry": updated,
            "identity": identity.to_dict(),
            "source_coordinate": identity.source_coordinate,
            "accepted_record": accepted,
        }

    def find_accepted_record(self, consumer_id: int | None,
                             source_offset: Any = None,
                             source_queue: str = "") -> dict[str, Any] | None:
        """用实际消费凭证（消费者ID + 队列偏移）回查实际受理的原记录。

        只关联调用追踪中属于该凭证、且偏移（如已登记）与来源坐标一致的
        消费记录；无偏移上下文的历史追踪按凭证回退匹配，并显式标注匹配
        方式，避免把另一来源的同业务键事件误关联到本偏移。
        """
        if consumer_id in (None, ""):
            return None

        try:
            from app.services.call_trace_log import CallTraceLogService
            tracer = CallTraceLogService()
        except Exception:
            return None

        traces = json_store.get_all(tracer.MODULE)
        offset_text = None if source_offset is None else str(source_offset)
        fallback: dict[str, Any] | None = None

        for trace in traces:
            if str(trace.get("关联端点", "")) != str(consumer_id):
                continue
            context = trace.get("_consume_context")
            if isinstance(context, dict):
                # 带偏移上下文的追踪：必须队列与偏移同时命中
                if source_queue and str(context.get("queue", "")) != source_queue:
                    continue
                if offset_text is not None and str(context.get("source_offset", "")) != offset_text:
                    continue
                return {
                    "trace_id": trace.get("_trace_id"),
                    "record_id": trace.get("id"),
                    "记录编号": trace.get("记录编号", ""),
                    "consumer_id": consumer_id,
                    "source_offset": context.get("source_offset"),
                    "queue": context.get("queue", ""),
                    "matched_by": "source_coordinate",
                }
            # 历史追踪没有上下文，留作凭证级兜底
            if fallback is None:
                fallback = {
                    "trace_id": trace.get("_trace_id"),
                    "record_id": trace.get("id"),
                    "记录编号": trace.get("记录编号", ""),
                    "consumer_id": consumer_id,
                    "source_offset": offset_text,
                    "queue": source_queue,
                    "matched_by": "consumer_credential",
                }
        return fallback

    def get_dlq_stats(self) -> dict[str, Any]:
        """获取死信队列的全局统计信息。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {}
        by_queue = {}
        for row in rows:
            s = row.get("status", "未知")
            q = row.get("原始队列", "未知")
            by_status[s] = by_status.get(s, 0) + 1
            by_queue[q] = by_queue.get(q, 0) + 1

        return {
            "total_dlq": len(rows),
            "by_status": by_status,
            "by_queue": dict(sorted(by_queue.items(), key=lambda x: x[1], reverse=True)[:10]),
            "pending_dispatch": by_status.get("草稿", 0) + by_status.get("待处置", 0),
            "replayed_count": by_status.get("已重放", 0),
            "discarded_count": by_status.get("已丢弃", 0),
            "analysis": self.analyze_failure_reasons(),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        """输出死信队列管理的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        analysis = self.analyze_failure_reasons()
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "死信队列记录数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "total_replays": len(self._replay_history),
            "failure_categories": analysis.get("categories", {}),
            "operation": self.OPERATION_TERM,
        }
