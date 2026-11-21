"""共用消息身份明细重构测试（事件去重 × 死信队列管理）。

覆盖需求场景：
1. 同一队列偏移两次投递（含服务重启重读）合并为同一条待处置死信；
2. 业务键相同但来自另一条队列/接入通道的事件单独入队，不顶替待重放记录；
3. 从死信台账处置后，用实际消费凭证回查只关联本偏移和它的原消息；
4. 同样输入单独走去重或死信登记，既有处理结果保持不变；
5. 存量死信（无身份明细、截断消息体、旧指纹）入队编号与原始消息仍可读；
6. 共用消息身份明细同时保留去重字段与死信原始消息数据。
"""
from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.persistence import json_store
from app.services.call_trace_log import CallTraceLogService
from app.services.dedup_filter import DedupFilterService
from app.services.dlq_handler import DlqHandlerService
from app.services.message_identity import (
    SOURCE_KIND_CHANNEL,
    SOURCE_KIND_QUEUE,
    build_channel_identity,
    build_event_identity,
    build_queue_identity,
)
from app.services.mq_consumer import MqConsumerService


class TestMessageIdentityShared(unittest.TestCase):
    """共用消息身份明细：去重字段与原始消息数据同表保留、来源坐标隔离。"""

    def test_identity_keeps_both_dedup_fields_and_raw_message(self):
        raw = '{"id": "BIZ-1", "amount": 88}'
        identity = build_queue_identity(
            source_queue="order_queue",
            original_message=raw,
            source_offset=42,
            consumer_id=7,
            event_type="order_create",
            dedup_key_fields="id",
        )
        # 去重字段
        self.assertEqual(identity.business_key, "BIZ-1")
        self.assertEqual(identity.dedup_key_value, "BIZ-1")
        self.assertEqual(identity.idempotency_key, "order_create|BIZ-1")
        self.assertTrue(identity.content_fingerprint)
        # 死信原始消息数据
        self.assertEqual(identity.raw_message, raw)
        self.assertEqual(len(identity.message_fingerprint), 16)
        self.assertEqual(identity.source_offset, "42")
        self.assertEqual(identity.consumer_id, 7)
        self.assertEqual(identity.source_kind, SOURCE_KIND_QUEUE)
        self.assertEqual(identity.source_coordinate, "queue:order_queue#offset:42")
        # 序列化/反序列化不丢字段
        again = type(identity).from_dict(identity.to_dict())
        self.assertEqual(again.to_dict(), identity.to_dict())

    def test_same_business_key_different_source_has_different_coordinate(self):
        payload = {"id": "BIZ-1"}
        queue_id = build_queue_identity(
            source_queue="order_queue", original_message='{"id": "BIZ-1"}',
            source_offset=42, dedup_key_fields="id",
        )
        other_queue_id = build_queue_identity(
            source_queue="other_queue", original_message='{"id": "BIZ-1"}',
            source_offset=9, dedup_key_fields="id",
        )
        channel_id = build_channel_identity(
            payload=payload, event_type="order_create", channel_id="WH-7",
            dedup_key_fields="id",
        )
        # 业务键相同
        self.assertEqual(queue_id.business_key, other_queue_id.business_key)
        self.assertEqual(queue_id.business_key, channel_id.business_key)
        # 来源坐标互不相同，不能互相顶替
        self.assertNotEqual(queue_id.source_coordinate, other_queue_id.source_coordinate)
        self.assertNotEqual(queue_id.source_coordinate, channel_id.source_coordinate)
        self.assertTrue(channel_id.source_coordinate.startswith("channel:"))
        # 同一队列同一偏移（重启重读）坐标一致
        reread = build_queue_identity(
            source_queue="order_queue", original_message='{"id": "BIZ-1"}',
            source_offset=42, dedup_key_fields="id",
        )
        self.assertTrue(queue_id.same_source(reread))
        self.assertFalse(queue_id.same_source(other_queue_id))


class TestDedupBehaviorUnchanged(unittest.TestCase):
    """独立入口：事件去重既有判重结果保持不变。"""

    def setUp(self):
        self.dedup_svc = DedupFilterService()
        json_store.truncate("dedup_filter")
        json_store.insert("dedup_filter", {
            "规则编号": "DEDUP-TEST-001",
            "事件类型": "通用",
            "去重键列": "id",
            "比较方式": "精确匹配",
            "时间窗口": "24小时",
            "相似度阈值": "0.95",
            "冲突策略": "跳过",
            "规则版本": "v1",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        })

    def test_first_then_duplicate(self):
        first = self.dedup_svc.check_duplicate("order_create", {"id": "order_002", "amount": 200})
        self.assertFalse(first["is_duplicate"])
        second = self.dedup_svc.check_duplicate("order_create", {"id": "order_002", "amount": 200})
        self.assertTrue(second["is_duplicate"])
        self.assertIn("dedup_key", second)

    def test_still_judges_by_original_fields_only(self):
        # 去重规则仍按原字段（事件类型+id）判重，来源不同不影响判重结论
        self.dedup_svc.check_duplicate("evt", {"id": "X"})
        identity = build_event_identity(
            event_type="evt", payload={"id": "X"},
            source_kind=SOURCE_KIND_QUEUE, source_name="another_queue", source_offset=5,
        )
        result = self.dedup_svc.check_duplicate("evt", {"id": "X"}, identity=identity)
        self.assertTrue(result["is_duplicate"])

    def test_stats_and_clear(self):
        for i in range(5):
            self.dedup_svc.check_duplicate("test_event", {"id": f"evt_{i}", "value": i})
        for i in range(2):
            self.dedup_svc.check_duplicate("test_event", {"id": f"evt_{i}", "value": i})
        stats = self.dedup_svc.get_dedup_stats()
        self.assertEqual(stats["total_duplicates"], 2)
        self.assertGreater(stats["total_idempotency_keys"], 0)
        # 身份明细随幂等键登记，可按去重键回查
        registered = self.dedup_svc.get_message_identity("test_event|evt_0")
        self.assertIsNotNone(registered)
        self.assertEqual(registered["business_key"], "evt_0")
        self.dedup_svc.clear_dedup_cache()
        self.assertEqual(self.dedup_svc.get_dedup_stats()["total_idempotency_keys"], 0)
        self.assertIsNone(self.dedup_svc.get_message_identity("test_event|evt_0"))


class TestDlqEnqueueAndSourceIsolation(unittest.TestCase):
    """死信登记：偏移合并、来源隔离、原始消息保全。"""

    def setUp(self):
        self.dlq_svc = DlqHandlerService()
        json_store.truncate("dlq_handler")

    def test_same_offset_redelivery_merges_after_restart(self):
        raw = '{"id": "BIZ-1", "payload": "first delivery"}'
        first = self.dlq_svc.enqueue_dead_letter(
            "order_queue", raw, "处理超时", consumer_id=7, source_offset=42,
        )
        self.assertFalse(first["is_duplicate"])
        dlq_id = first["dlq_id"]

        # 服务重启后又读到该原消息：同队列同偏移同内容 → 合并
        second = self.dlq_svc.enqueue_dead_letter(
            "order_queue", raw, "处理超时", consumer_id=7, source_offset=42,
            delivery_attempt=2,
        )
        self.assertTrue(second["is_duplicate"])
        self.assertEqual(second["dlq_id"], dlq_id)
        entry = json_store.get_by_id("dlq_handler", dlq_id)
        self.assertEqual(int(entry["重试次数"]), 1)
        # 入队编号保持不变且可读
        self.assertTrue(entry["记录编号"].startswith("DLQ-"))

    def test_same_business_key_other_queue_not_treated_as_replay_record(self):
        raw = '{"id": "BIZ-1"}'
        first = self.dlq_svc.enqueue_dead_letter(
            "order_queue", raw, "处理超时", consumer_id=7, source_offset=42,
        )
        # 另一条队列、业务键相同的事件 → 必须单独入队
        other = self.dlq_svc.enqueue_dead_letter(
            "other_queue", raw, "处理超时", consumer_id=8, source_offset=9,
        )
        self.assertFalse(other["is_duplicate"])
        self.assertNotEqual(other["dlq_id"], first["dlq_id"])
        self.assertEqual(len(json_store.get_all("dlq_handler")), 2)

        # 同队列不同偏移的同业务键事件同样不能合并
        third = self.dlq_svc.enqueue_dead_letter(
            "order_queue", raw, "处理超时", consumer_id=7, source_offset=43,
        )
        self.assertFalse(third["is_duplicate"])
        self.assertEqual(len(json_store.get_all("dlq_handler")), 3)

    def test_channel_event_separated_from_queue_dead_letter(self):
        raw = '{"id": "BIZ-9"}'
        queue_dlq = self.dlq_svc.enqueue_dead_letter(
            "order_queue", raw, "处理超时", consumer_id=7, source_offset=1,
        )
        channel_dlq = self.dlq_svc.enqueue_dead_letter(
            "WH-7", raw, "通道处理失败", consumer_id=0,
            source_kind=SOURCE_KIND_CHANNEL, channel_id="WH-7",
        )
        self.assertNotEqual(queue_dlq["dlq_id"], channel_dlq["dlq_id"])
        channel_entry = json_store.get_by_id("dlq_handler", channel_dlq["dlq_id"])
        self.assertEqual(channel_entry["_identity"]["source_kind"], SOURCE_KIND_CHANNEL)
        self.assertTrue(channel_entry["_identity"]["source_coordinate"].startswith("channel:"))

    def test_identity_preserves_full_raw_message_and_dedup_fields(self):
        long_raw = '{"id": "BIZ-5", "data": "' + ("x" * 260) + '"}'
        result = self.dlq_svc.enqueue_dead_letter(
            "order_queue", long_raw, "数据格式异常", consumer_id=3, source_offset=77,
        )
        entry = json_store.get_by_id("dlq_handler", result["dlq_id"])
        # 列表展示字段仍按历史口径截断
        self.assertEqual(len(entry["原始消息体"]), 200)
        identity = entry["_identity"]
        # 原始消息全文、消息指纹、去重字段都在身份明细里
        self.assertEqual(identity["raw_message"], long_raw)
        self.assertEqual(identity["message_fingerprint"], entry["_fingerprint"])
        self.assertEqual(identity["business_key"], "BIZ-5")
        self.assertEqual(identity["idempotency_key"], "|BIZ-5")
        self.assertEqual(identity["source_offset"], "77")

    def test_legacy_dead_letter_backfill_keeps_number_and_message_readable(self):
        # 模拟重构前落库的存量死信：无 _identity，消息体截断，仅旧指纹。
        # 旧指纹按历史口径由原始消息全文 sha256 截断生成。
        from app.services.message_identity import compute_message_fingerprint
        legacy_raw = '{"id": "OLD-1"}'
        legacy = {
            "记录编号": "DLQ-LEG-00001",
            "原始队列": "legacy_queue",
            "失败原因": "处理超时",
            "原始消息体": legacy_raw,
            "重试次数": "2",
            "入队时间": "2026-09-01T00:00:00+00:00",
            "处置状态": "待处置",
            "重放批次": "",
            "status": "待处置",
            "pending": True,
            "abnormal": True,
            "_fingerprint": compute_message_fingerprint(legacy_raw),
            "_consumer_id": 5,
        }
        inserted = json_store.insert("dlq_handler", legacy)
        dlq_id = inserted["id"]

        entry = json_store.get_by_id("dlq_handler", dlq_id)
        identity = self.dlq_svc._identity_of(entry)
        # 存量编号与原始消息继续可读
        self.assertEqual(entry["记录编号"], "DLQ-LEG-00001")
        self.assertIn("OLD-1", identity.raw_message)
        # 旧指纹口径不被重算覆盖
        self.assertEqual(identity.message_fingerprint, compute_message_fingerprint(legacy_raw))
        self.assertEqual(identity.source_name, "legacy_queue")
        self.assertEqual(identity.consumer_id, 5)

        # 服务重启重读：同队列同消息但带偏移（存量记录无坐标），按历史口径仍合并
        again = self.dlq_svc.enqueue_dead_letter(
            "legacy_queue", legacy_raw, "处理超时", consumer_id=5, source_offset=42,
        )
        self.assertTrue(again["is_duplicate"])
        self.assertEqual(again["dlq_id"], dlq_id)


class TestDeadLetterDisposalAndCredentialLookup(unittest.TestCase):
    """从死信台账处置，并用已有凭证回查实际受理的原记录。"""

    def setUp(self):
        self.dlq_svc = DlqHandlerService()
        self.tracer = CallTraceLogService()
        json_store.truncate("dlq_handler")
        json_store.truncate("call_trace_log")

    def _make_trace(self, consumer_id: int, queue: str, offset: str) -> dict:
        trace = self.tracer._record_trace(
            endpoint_id=consumer_id, action="消息消费",
            latency_ms=120, status="正常", record_count=1,
        )
        json_store.update(self.tracer.MODULE, trace["record_id"], {
            "_consume_context": {
                "consumer_id": consumer_id,
                "queue": queue,
                "source_offset": offset,
                "message_id": f"msg-{queue}-{offset}",
            },
        })
        return trace

    def test_disposal_and_lookup_only_associate_this_offset(self):
        raw = '{"id": "BIZ-1"}'
        target = self.dlq_svc.enqueue_dead_letter(
            "order_queue", raw, "处理超时", consumer_id=7, source_offset=42,
        )
        # 另一个通道/队列的同业务键事件也进了死信台账
        other = self.dlq_svc.enqueue_dead_letter(
            "other_queue", raw, "处理超时", consumer_id=8, source_offset=9,
        )

        # 两笔实际受理凭证：各自偏移
        own_trace = self._make_trace(7, "order_queue", "42")
        self._make_trace(8, "other_queue", "9")

        # 从死信台账处置目标消息
        outcome = self.dlq_svc.resolve_dead_letter(target["dlq_id"], "标记重放")
        self.assertTrue(outcome["ok"])
        self.assertEqual(outcome["entry"]["status"], "已重放")
        # 只关联本偏移和它的原消息
        accepted = outcome["accepted_record"]
        self.assertIsNotNone(accepted)
        self.assertEqual(accepted["record_id"], own_trace["record_id"])
        self.assertEqual(accepted["source_offset"], "42")
        self.assertEqual(accepted["queue"], "order_queue")
        self.assertEqual(accepted["matched_by"], "source_coordinate")
        self.assertEqual(outcome["source_coordinate"], "queue:order_queue#offset:42")
        self.assertEqual(outcome["identity"]["raw_message"], raw)

        # 另一条记录不受影响，仍是待处置
        other_entry = json_store.get_by_id("dlq_handler", other["dlq_id"])
        self.assertEqual(other_entry["status"], "草稿")

    def test_lookup_never_returns_another_offset_with_same_business_key(self):
        raw = '{"id": "BIZ-1"}'
        target = self.dlq_svc.enqueue_dead_letter(
            "order_queue", raw, "处理超时", consumer_id=7, source_offset=42,
        )
        # 同一消费者凭证下，另一偏移的受理记录（同业务键事件）
        other_trace = self._make_trace(7, "order_queue", "43")

        identity = self.dlq_svc._identity_of(
            json_store.get_by_id("dlq_handler", target["dlq_id"])
        )
        accepted = self.dlq_svc.find_accepted_record(
            consumer_id=identity.consumer_id,
            source_offset=identity.source_offset,
            source_queue=identity.source_name,
        )
        # 偏移 42 没有受理凭证 → 不得返回偏移 43 的记录
        self.assertIsNone(accepted)
        self.assertNotEqual(other_trace["record_id"], accepted)

    def test_legacy_trace_without_context_falls_back_to_credential(self):
        target = self.dlq_svc.enqueue_dead_letter(
            "order_queue", '{"id": "BIZ-2"}', "处理超时",
            consumer_id=11, source_offset=5,
        )
        # 历史调用追踪没有 _consume_context
        legacy_trace = self.tracer._record_trace(
            endpoint_id=11, action="消息消费", latency_ms=90,
            status="正常", record_count=1,
        )
        identity = self.dlq_svc._identity_of(
            json_store.get_by_id("dlq_handler", target["dlq_id"])
        )
        accepted = self.dlq_svc.find_accepted_record(
            consumer_id=identity.consumer_id,
            source_offset=identity.source_offset,
            source_queue=identity.source_name,
        )
        self.assertIsNotNone(accepted)
        self.assertEqual(accepted["record_id"], legacy_trace["record_id"])
        self.assertEqual(accepted["matched_by"], "consumer_credential")

    def test_standalone_dlq_entry_results_unchanged(self):
        # 同样输入单独走死信登记：首次入队、重复合并的既有结论不变
        first = self.dlq_svc.enqueue_dead_letter(
            "order_queue", '{"id": 456}', "格式异常", 1,
        )
        second = self.dlq_svc.enqueue_dead_letter(
            "order_queue", '{"id": 456}', "格式异常", 1,
        )
        self.assertFalse(first["is_duplicate"])
        self.assertTrue(second["is_duplicate"])
        self.assertEqual(second["dlq_id"], first["dlq_id"])
        self.assertIn("DLQ-", first["message"])


class TestMqConsumerIdentityPropagation(unittest.TestCase):
    """消息消费跨模块：偏移身份透传到死信明细与实际消费凭证。"""

    def setUp(self):
        self.consumer_svc = MqConsumerService()
        self.dlq_svc = DlqHandlerService()
        json_store.truncate("mq_consumer")
        json_store.truncate("dlq_handler")
        json_store.truncate("call_trace_log")
        self.consumer = json_store.insert("mq_consumer", {
            "配置编号": "MQ-TEST-ID-1",
            "队列名称": "order_queue",
            "消费组名": "order_group",
            "偏移策略": "earliest",
            "批量大小": "5",
            "超时时间": "5000",
            "死信队列": "test_dlq",
            "消费状态": "消费中",
            "status": "消费中",
            "pending": False,
            "abnormal": False,
        })

    def test_route_to_dlq_carries_offset_identity(self):
        message = {
            "offset": 42,
            "message_id": "msg-42",
            "queue": "order_queue",
            "group": "order_group",
            "payload": {"id": "BIZ-1"},
        }
        result = self.consumer_svc._route_to_dlq(self.consumer["id"], message, "处理超时")
        self.assertTrue(result["ok"])
        entry = json_store.get_by_id("dlq_handler", result["dlq_id"])
        self.assertEqual(entry["_identity"]["source_offset"], "42")
        self.assertEqual(entry["_identity"]["source_name"], "order_queue")
        self.assertEqual(entry["_identity"]["consumer_id"], self.consumer["id"])

    def test_consumption_trace_carries_credential_context(self):
        message = {
            "offset": 43,
            "message_id": "msg-43",
            "queue": "order_queue",
            "group": "order_group",
            "payload": {"id": "BIZ-2"},
        }
        self.consumer_svc._record_consumption(self.consumer["id"], message, 200)
        traces = json_store.get_all("call_trace_log")
        self.assertEqual(len(traces), 1)
        context = traces[0]["_consume_context"]
        self.assertEqual(context["source_offset"], "43")
        self.assertEqual(context["queue"], "order_queue")
        self.assertEqual(context["consumer_id"], self.consumer["id"])

        # 死信侧可用该凭证精确回查
        accepted = self.dlq_svc.find_accepted_record(
            consumer_id=self.consumer["id"], source_offset="43", source_queue="order_queue",
        )
        self.assertIsNotNone(accepted)
        self.assertEqual(accepted["matched_by"], "source_coordinate")


if __name__ == "__main__":
    unittest.main(verbosity=2)
