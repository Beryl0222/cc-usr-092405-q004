"""考古现场关系管理：领域与 HTTP 端到端测试。"""

import json
import os
import tempfile
import threading
import unittest
from datetime import datetime
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import fieldwork as fw
import service
from fieldwork import FieldworkStore


class Device:
    """按设备维护逻辑时钟与哈希链，构造离线事件。"""

    def __init__(self, device_id):
        self.device_id = device_id
        self.lamport = 0
        self.events = []

    def emit(self, event_type, payload, occurred_at, *, lamport=None):
        self.lamport += 1
        lamport = lamport if lamport is not None else self.lamport
        prev = fw.event_hash(self.events[-1]) if self.events else None
        event = {
            "event_id": f"{self.device_id}-{lamport}",
            "device_id": self.device_id,
            "lamport": lamport,
            "type": event_type,
            "payload": payload,
            "occurred_at": occurred_at,
            "prev_hash": prev,
        }
        self.events.append(event)
        return event

    def raw(self, lamport, event_type, payload, occurred_at, prev_hash):
        """直接指定 prev_hash，用于制造缺环/断裂；同步设备时钟。"""
        self.lamport = lamport
        event = {
            "event_id": f"{self.device_id}-{lamport}",
            "device_id": self.device_id,
            "lamport": lamport,
            "type": event_type,
            "payload": payload,
            "occurred_at": occurred_at,
            "prev_hash": prev_hash,
        }
        self.events.append(event)
        return event


def build_expedition():
    """构造一次跨年度发掘场景，返回 (devices, events)。"""
    a = Device("tablet-A")
    b = Device("tablet-B")
    a.emit("record.register", {"record_id": "T1", "kind": "探方", "label": "T1",
                                "properties": {"坐标": "(0,0)"}}, "2025-03-01T08:00")
    a.emit("record.register", {"record_id": "L3", "kind": "地层", "label": "第3层",
                                "properties": {"土质": "黄土"}}, "2025-03-01T09:00")
    a.emit("record.register", {"record_id": "L2", "kind": "地层", "label": "第2层",
                                "properties": {"土质": "灰土"}}, "2025-03-01T09:30")
    a.emit("spatial.relate", {"source": "T1", "target": "L3", "relation": "contains"},
           "2025-03-02T08:00")
    a.emit("spatial.relate", {"source": "T1", "target": "L2", "relation": "包含"},
           "2025-03-02T08:10")
    a.emit("spatial.relate", {"source": "L2", "target": "L3", "relation": "above"},
           "2025-03-02T08:20")
    a.emit("record.register", {"record_id": "M1", "kind": "墓葬", "label": "主墓",
                                "properties": {"形制": "竖穴"}, "numbers": ["M001"]},
           "2025-03-03T08:00")
    a.emit("spatial.relate", {"source": "L3", "target": "M1", "relation": "contains"},
           "2025-03-03T09:00")
    # 后来的队伍补录器物组合，早期形制判断被校正（原值保留）。
    a.emit("record.correct", {"record_id": "M1",
                               "properties": {"形制": "甲字形", "深度": "8.2m"},
                               "change_summary": "2026年复核形制"},
           "2026-04-01T09:00")
    a.emit("record.register", {"record_id": "G1", "kind": "器物组合", "label": "铜礼器组合",
                                "properties": {"件数": 12}}, "2026-04-02T09:00")
    a.emit("spatial.relate", {"source": "M1", "target": "G1", "relation": "contains"},
           "2026-04-02T10:00")
    # 陪葬墓队伍在另一年度现场沿用了重复编号。
    b.emit("record.register", {"record_id": "M9", "kind": "陪葬墓", "label": "陪葬墓K9",
                                "properties": {}, "numbers": ["M001"]},
           "2027-05-01T08:00")
    # 样本与测年。
    a.emit("record.register", {"record_id": "S1", "kind": "测年样本", "label": "棺内炭样",
                                "properties": {"initial_amount": 10.0,
                                               "amount_unit": "g"}},
           "2026-04-03T08:00")
    a.emit("sample.consume", {"sample_id": "S1", "amount": 3.0,
                               "purpose": "AMS-C14 测年"}, "2026-04-10T08:00")
    a.emit("dating.register", {"dating_id": "D1", "sample_id": "S1",
                                "method": "AMS-C14", "result": "战国晚期",
                                "range": "-320/-280", "lab_id": "LAB-7"},
           "2026-04-20T08:00")
    # 文物出库检测后归还，再跨库移交。
    a.emit("record.register", {"record_id": "V1", "kind": "器物", "label": "铜鼎",
                                "properties": {"holder": "主库房"}},
           "2026-04-04T08:00")
    a.emit("custody.checkout", {"item_id": "V1", "actor": "库管员甲",
                                 "from_party": "主库房",
                                 "to_party": "文保中心", "purpose": "成分检测",
                                 "condition": "完好"}, "2026-05-01T08:00")
    a.emit("custody.return", {"item_id": "V1", "actor": "库管员甲",
                               "to_store": "主库房", "condition": "完好，已检测"},
           "2026-05-05T08:00")
    a.emit("custody.transfer", {"item_id": "V1", "actor": "库管员甲",
                                 "from_party": "主库房", "to_party": "分馆库房",
                                 "purpose": "展览"}, "2026-06-01T08:00")
    return a, b


def ingested_ids(store):
    return {e["event_id"] for e in store.physical_log()}


class MergeDeterminismTest(unittest.TestCase):
    def setUp(self):
        self.a, self.b = build_expedition()
        self.all_events = self.a.events + self.b.events

    def _feed(self, order):
        store = FieldworkStore()
        store.ingest_batch([self.all_events[i] for i in order])
        return store

    def test_out_of_order_merge_is_byte_stable(self):
        n = len(self.all_events)
        forward = self._feed(range(n))
        reverse = self._feed(range(n - 1, -1, -1))
        # 交替乱序（按设备穿插）。
        mixed_order = list(range(0, n, 2)) + list(range(1, n, 2))
        mixed = self._feed(mixed_order)
        canonical = fw.canonical_json(fw.build_views(forward))
        self.assertEqual(canonical, fw.canonical_json(fw.build_views(reverse)))
        self.assertEqual(canonical, fw.canonical_json(fw.build_views(mixed)))

    def test_duplicate_event_is_idempotent(self):
        store = FieldworkStore()
        event = self.all_events[0]
        first = store.ingest(event)
        second = store.ingest(dict(event))
        self.assertEqual(first["status"], "applied")
        self.assertEqual(second["status"], "duplicate")
        self.assertEqual(len(store.physical_log()), 1)

    def test_same_event_id_conflicting_payload_rejected(self):
        store = FieldworkStore()
        event = dict(self.all_events[0])
        store.ingest(event)
        tampered = json.loads(json.dumps(event, ensure_ascii=False))
        tampered["payload"]["label"] = "被篡改"
        result = store.ingest(tampered)
        self.assertEqual(result["status"], "rejected")
        self.assertEqual(len(store.physical_log()), 1)

    def test_malformed_event_rejected(self):
        store = FieldworkStore()
        result = store.ingest({"event_id": "x"})
        self.assertEqual(result["status"], "rejected")


class DuplicateNumberingTest(unittest.TestCase):
    def setUp(self):
        self.a, self.b = build_expedition()
        self.store = FieldworkStore()
        self.store.ingest_batch(self.a.events)
        self.store.ingest_batch(self.b.events)

    def test_duplicate_field_number_flagged(self):
        conflicts = fw.build_views(self.store)["conflicts"]["numbering"]
        unresolved = [c for c in conflicts if c["status"] == "unresolved"]
        self.assertEqual(unresolved, [{"number": "M001",
                                       "claimants": ["M1", "M9"],
                                       "status": "unresolved"}])

    def test_resolution_assigns_alias_and_clears_conflict(self):
        resolve = self.a.emit("number.resolve", {
            "number": "M001", "keeper_id": "M1", "rename": {"M9": "PM2027-09"}},
            "2027-05-10T08:00")
        self.assertEqual(self.store.ingest(resolve)["status"], "applied")
        conflicts = fw.build_views(self.store)["conflicts"]["numbering"]
        self.assertFalse([c for c in conflicts if c["status"] == "unresolved"])
        m9 = next(r for r in fw.build_views(self.store)["records"]
                  if r["record_id"] == "M9")
        self.assertEqual(m9["numbers"], ["PM2027-09"])

    def test_resolution_requires_real_claimant(self):
        bad = self.a.emit("number.resolve", {
            "number": "M001", "keeper_id": "M1", "rename": {"M2": "X1"}},
            "2027-05-10T09:00")
        result = self.store.ingest(bad)
        self.assertEqual(result["status"], "quarantined")


class VersioningAndSpatialTest(unittest.TestCase):
    def setUp(self):
        self.a, self.b = build_expedition()
        self.store = FieldworkStore()
        self.store.ingest_batch(self.a.events)

    def test_correction_keeps_original_values(self):
        m1 = self.store.projection.records["M1"]
        self.assertEqual(len(m1["versions"]), 2)
        self.assertEqual(m1["versions"][0]["properties"]["形制"], "竖穴")
        self.assertEqual(m1["versions"][1]["properties"]["形制"], "甲字形")
        current = next(r for r in fw.build_views(self.store)["records"]
                       if r["record_id"] == "M1")
        self.assertEqual(current["version"], 2)
        self.assertEqual(current["version_count"], 2)

    def test_cannot_rename_numbers_via_correction(self):
        bad = self.a.emit("record.correct", {"record_id": "M1",
                                              "properties": {"numbers": ["HACK"]}},
                          "2026-04-05T08:00")
        self.assertEqual(self.store.ingest(bad)["status"], "quarantined")

    def test_relation_alias_and_transitive_closure(self):
        closure = fw.spatial_closure(self.store.projection)
        inferred = {tuple(e) for e in closure["inferred"]}
        # 中文"包含"归一化为 contains；T1→L3→M1 推出 T1 包含 M1。
        self.assertIn(("T1", "contains", "M1"), inferred)
        self.assertIn(("T1", "contains", "G1"), inferred)

    def test_below_relation_normalized_to_above(self):
        event = self.a.emit("spatial.relate",
                            {"source": "L3", "target": "L2", "relation": "below"},
                            "2026-04-06T08:00")
        # L3 below L2 与既有 L2 above L3 是同一条边，幂等合并。
        self.assertEqual(self.store.ingest(event)["status"], "applied")
        edges = {tuple(e[:3]) for e in
                 fw.spatial_closure(self.store.projection)["direct"]}
        self.assertIn(("L2", "above", "L3"), edges)
        self.assertNotIn(("L3", "above", "L2"), edges)

    def test_retract_removes_edge_and_records_reason(self):
        retract = self.a.emit("spatial.retract", {
            "source": "L2", "target": "L3", "relation": "above",
            "reason": "复核为同一层位"}, "2026-04-07T08:00")
        self.store.ingest(retract)
        edges = {tuple(e[:3]) for e in
                 fw.spatial_closure(self.store.projection)["direct"]}
        self.assertNotIn(("L2", "above", "L3"), edges)
        self.assertTrue(fw.build_views(self.store)["conflicts"]["numbering"] is not None)

    def test_mutual_above_flagged_as_contradiction(self):
        bad = self.a.emit("spatial.relate",
                          {"source": "L3", "target": "L2", "relation": "above"},
                          "2026-04-08T08:00")
        self.store.ingest(bad)
        spatial = fw.build_views(self.store)["conflicts"]["spatial"]
        self.assertIn({"type": "mutual_above", "records": ["L2", "L3"]}, spatial)


class SampleAndCustodyTest(unittest.TestCase):
    def setUp(self):
        self.a, self.b = build_expedition()
        self.store = FieldworkStore()
        self.store.ingest_batch(self.a.events)

    def test_sample_balance_is_stable(self):
        sample = next(s for s in fw.build_views(self.store)["samples"]
                      if s["sample_id"] == "S1")
        self.assertEqual(sample["initial_amount"], 10.0)
        self.assertEqual(sample["consumed_amount"], 3.0)
        self.assertEqual(sample["balance"], 7.0)

    def test_over_consumption_quarantined_and_balance_unchanged(self):
        bad = self.a.emit("sample.consume",
                          {"sample_id": "S1", "amount": 9.0,
                           "purpose": "试图超额"}, "2026-04-11T08:00")
        result = self.store.ingest(bad)
        self.assertEqual(result["status"], "quarantined")
        sample = next(s for s in fw.build_views(self.store)["samples"]
                      if s["sample_id"] == "S1")
        self.assertEqual(sample["balance"], 7.0)

    def test_duplicate_consume_does_not_double_spend(self):
        original = next(e for e in self.a.events
                        if e["type"] == "sample.consume")
        self.assertEqual(self.store.ingest(dict(original))["status"], "duplicate")
        sample = next(s for s in fw.build_views(self.store)["samples"]
                      if s["sample_id"] == "S1")
        self.assertEqual(sample["consumed_amount"], 3.0)

    def test_custody_chain_recorded_and_transfer_guarded(self):
        custody = fw.build_views(self.store)["custody"]["V1"]
        self.assertEqual(custody["holder"], "分馆库房")
        kinds = [entry["type"] for entry in custody["chain"]]
        self.assertEqual(kinds, ["checkout", "return", "transfer"])

    def test_double_checkout_rejected(self):
        # V1 已移交分馆库房：由当前持有方放行的首次出库有效。
        first = self.a.emit("custody.checkout", {
            "item_id": "V1", "actor": "乙", "from_party": "分馆库房",
            "to_party": "某实验室", "purpose": "复检",
            "condition": "完好"}, "2026-07-01T08:00")
        self.assertEqual(self.store.ingest(first)["status"], "applied")
        # 在途未归还时再次出库：拒绝。
        second = self.a.emit("custody.checkout", {
            "item_id": "V1", "actor": "乙", "from_party": "某实验室",
            "to_party": "另一机构", "purpose": "再次出库",
            "condition": "完好"}, "2026-07-02T08:00")
        self.assertEqual(self.store.ingest(second)["status"], "quarantined")

    def test_transfer_from_wrong_holder_rejected(self):
        bad = self.a.emit("custody.transfer", {
            "item_id": "V1", "actor": "乙", "from_party": "主库房",
            "to_party": "外地馆"}, "2026-07-02T08:00")
        self.assertEqual(self.store.ingest(bad)["status"], "quarantined")

    def test_return_without_checkout_rejected(self):
        bad = self.a.emit("custody.return", {
            "item_id": "S1", "actor": "乙", "condition": "完好"},
            "2026-07-03T08:00")
        self.assertEqual(self.store.ingest(bad)["status"], "quarantined")


class DatingWithdrawalTest(unittest.TestCase):
    def test_withdrawn_dating_moves_lists_but_is_kept(self):
        a, _ = build_expedition()
        store = FieldworkStore()
        store.ingest_batch(a.events)
        views = fw.build_views(store)
        self.assertEqual([d["dating_id"] for d in views["datings"]], ["D1"])
        withdraw = a.emit("dating.withdraw",
                          {"dating_id": "D1", "reason": "送检样本污染"},
                          "2026-05-01T08:00")
        store.ingest(withdraw)
        views = fw.build_views(store)
        self.assertEqual(views["datings"], [])
        self.assertEqual([d["dating_id"] for d in views["datings_withdrawn"]], ["D1"])
        self.assertIn("D1", views["conflicts"]["withdrawn_datings"])
        # 样本余额不受测年结论撤回影响。
        self.assertEqual(views["samples"][0]["balance"], 7.0)

    def test_claim_cannot_use_withdrawn_dating_as_evidence(self):
        a, _ = build_expedition()
        store = FieldworkStore()
        store.ingest_batch(a.events)
        store.ingest(a.emit("dating.withdraw",
                            {"dating_id": "D1", "reason": "污染"},
                            "2026-05-01T08:00"))
        claim = a.emit("claim.submit", {
            "claim_id": "C-bad", "subject": "M1年代",
            "proposition": "战国晚期说", "confidence": 0.9,
            "evidence": ["D1"], "author": "甲"}, "2026-05-02T08:00")
        self.assertEqual(store.ingest(claim)["status"], "quarantined")


class ClaimsAndPublicationTest(unittest.TestCase):
    def setUp(self):
        self.a, self.b = build_expedition()
        self.store = FieldworkStore()
        self.store.ingest_batch(self.a.events)
        self.strong = self.a.emit("claim.submit", {
            "claim_id": "C-date-warring", "subject": "M1年代",
            "proposition": "主墓下葬于战国晚期", "confidence": 0.9,
            "evidence": ["D1", "G1"], "author": "研究员甲"},
            "2026-05-10T08:00")
        self.weak = self.a.emit("claim.submit", {
            "claim_id": "C-date-han", "subject": "M1年代",
            "proposition": "主墓或晚至西汉初", "confidence": 0.55,
            "evidence": ["G1"], "author": "研究员乙"},
            "2026-05-11T08:00")
        self.store.ingest_batch([self.strong, self.weak])

    def test_competing_claims_coexist(self):
        competing = fw.build_views(self.store)["conflicts"]["competing_claims"]
        self.assertEqual(competing, [{"subject": "M1年代",
                                      "claim_ids": ["C-date-han",
                                                    "C-date-warring"]}])

    def test_claim_requires_real_evidence(self):
        bad = self.a.emit("claim.submit", {
            "claim_id": "C-noevidence", "subject": "墓主",
            "proposition": "凭空猜测", "confidence": 0.9,
            "evidence": ["不存在的器物"], "author": "丙"},
            "2026-05-12T08:00")
        self.assertEqual(self.store.ingest(bad)["status"], "quarantined")

    def test_confidence_band(self):
        claims = {c["claim_id"]: c for c in fw.build_views(self.store)["claims"]}
        self.assertEqual(claims["C-date-warring"]["confidence_band"], "high")
        self.assertEqual(claims["C-date-han"]["confidence_band"], "medium")

    def test_author_cannot_peer_review_self(self):
        bad = self.a.emit("peer.review", {
            "claim_id": "C-date-warring", "reviewer": "研究员甲",
            "verdict": "support", "comment": "自评"}, "2026-05-12T08:00")
        self.assertEqual(self.store.ingest(bad)["status"], "quarantined")

    def test_possible_claim_cannot_be_published_as_established(self):
        review = self.a.emit("peer.review", {
            "claim_id": "C-date-warring", "reviewer": "评议人丁",
            "verdict": "support", "comment": "证据链成立"},
            "2026-05-13T08:00")
        self.store.ingest(review)
        # 0.55 的"可能"判断直接以既定事实发布：隔离。
        bad_release = self.a.emit("publication.release", {
            "release_id": "R-bad", "title": "违规简报", "date": "2026-05-20",
            "entries": [{"claim_id": "C-date-han", "as": "established"}]},
            "2026-05-18T08:00")
        result = self.store.ingest(bad_release)
        self.assertEqual(result["status"], "quarantined")
        self.assertNotIn("R-bad", self.store.projection.releases)

    def test_established_requires_support_without_challenge(self):
        # 有质疑评议时，高置信观点也不能作为既定事实发布。
        self.store.ingest(self.a.emit("peer.review", {
            "claim_id": "C-date-warring", "reviewer": "评议人丁",
            "verdict": "challenge", "comment": "测年样本存疑"},
            "2026-05-13T08:00"))
        blocked = self.a.emit("publication.release", {
            "release_id": "R-blocked", "title": "待评议简报",
            "date": "2026-05-20",
            "entries": [{"claim_id": "C-date-warring", "as": "established"}]},
            "2026-05-18T08:00")
        self.assertEqual(self.store.ingest(blocked)["status"], "quarantined")

    def test_release_snapshot_anchored_at_publication_date(self):
        # 2026-05-13 获得独立支持。
        self.store.ingest(self.a.emit("peer.review", {
            "claim_id": "C-date-warring", "reviewer": "评议人丁",
            "verdict": "support", "comment": "成立"}, "2026-05-13T08:00"))
        # 发布日 2026-05-20，强观点作为既定事实、弱观点作为假说并存。
        release = self.a.emit("publication.release", {
            "release_id": "R-2026-05", "title": "2026年5月阶段简报",
            "date": "2026-05-20",
            "entries": [
                {"claim_id": "C-date-warring", "as": "established"},
                {"claim_id": "C-date-han", "as": "hypothesis"},
            ]}, "2026-05-19T12:00")
        self.assertEqual(self.store.ingest(release)["status"], "applied")
        # 发布之后观点被修订降级，已发布快照不受影响。
        revise = self.a.emit("claim.revise", {
            "claim_id": "C-date-warring",
            "proposition": "主墓下葬于战国晚期偏晚", "confidence": 0.72,
            "evidence": ["D1", "G1"], "change_summary": "新证据下调置信度"},
            "2026-06-10T08:00")
        self.store.ingest(revise)
        snapshot = self.store.projection.releases["R-2026-05"]
        strong_entry = next(e for e in snapshot["entries"]
                            if e["claim_id"] == "C-date-warring")
        self.assertEqual(strong_entry["version"], 1)
        self.assertEqual(strong_entry["confidence"], 0.9)
        self.assertEqual(strong_entry["as"], "established")

    def test_late_arriving_release_still_snapshots_past(self):
        self.store.ingest(self.a.emit("peer.review", {
            "claim_id": "C-date-warring", "reviewer": "评议人丁",
            "verdict": "support", "comment": "成立"}, "2026-05-13T08:00"))
        # 发布事件离线，2026-08 才归队，但发布日期仍为 2026-05-20。
        self.store.ingest(self.a.emit("claim.revise", {
            "claim_id": "C-date-warring",
            "proposition": "修订后的表述", "confidence": 0.65,
            "evidence": ["D1", "G1"]}, "2026-07-01T08:00"))
        late = self.a.emit("publication.release", {
            "release_id": "R-late", "title": "迟交简报", "date": "2026-05-20",
            "entries": [{"claim_id": "C-date-warring", "as": "established"}]},
            "2026-08-01T08:00")
        # 发布日期当时为 v1(0.9) 且有支持评议，应通过。
        self.assertEqual(self.store.ingest(late)["status"], "applied")
        entry = self.store.projection.releases["R-late"]["entries"][0]
        self.assertEqual(entry["version"], 1)


class AsOfTest(unittest.TestCase):
    def test_state_as_of_reconstructs_past_conclusions(self):
        a, _ = build_expedition()
        store = FieldworkStore()
        store.ingest_batch(a.events)
        store.ingest(a.emit("claim.submit", {
            "claim_id": "C1", "subject": "M1年代",
            "proposition": "战国晚期", "confidence": 0.9,
            "evidence": ["D1", "G1"], "author": "甲"}, "2026-05-10T08:00"))
        store.ingest(a.emit("peer.review", {
            "claim_id": "C1", "reviewer": "丁", "verdict": "support",
            "comment": "成立"}, "2026-05-12T08:00"))
        store.ingest(a.emit("publication.release", {
            "release_id": "R1", "title": "五月简报", "date": "2026-05-15",
            "entries": [{"claim_id": "C1", "as": "established"}]},
            "2026-05-14T08:00"))
        # 更晚的修订与测年撤回。
        store.ingest(a.emit("dating.withdraw",
                            {"dating_id": "D1", "reason": "污染"},
                            "2026-06-01T08:00"))
        store.ingest(a.emit("claim.revise", {
            "claim_id": "C1", "proposition": "存疑，待重测",
            "confidence": 0.4, "evidence": ["G1"]},
            "2026-06-02T08:00"))

        past = fw.state_as_of(store, datetime.fromisoformat("2026-05-20T00:00"))
        c1 = next(c for c in past["claims"] if c["claim_id"] == "C1")
        self.assertEqual(c1["version"], 1)
        self.assertEqual(c1["confidence"], 0.9)
        self.assertEqual([d["dating_id"] for d in past["datings"]], ["D1"])
        self.assertEqual([r["release_id"] for r in past["releases"]], ["R1"])

        # 更早：样本尚未消耗（2026-04-10 前）。
        early = fw.state_as_of(store, datetime.fromisoformat("2026-04-09T00:00"))
        s1 = next(s for s in early["samples"] if s["sample_id"] == "S1")
        self.assertEqual(s1["balance"], 10.0)
        # 2025 年现场的主墓还是"竖穴"的原始记录（2026-04 校正之前）。
        field_2025 = fw.state_as_of(store, datetime.fromisoformat("2025-12-31T00:00"))
        m1_2025 = next(r for r in field_2025["records"] if r["record_id"] == "M1")
        self.assertEqual(m1_2025["properties"]["形制"], "竖穴")
        self.assertEqual(m1_2025["version"], 1)

    def test_release_before_claim_version_existed_blocked(self):
        a, _ = build_expedition()
        store = FieldworkStore()
        store.ingest_batch(a.events)
        store.ingest(a.emit("claim.submit", {
            "claim_id": "C1", "subject": "X", "proposition": "p",
            "confidence": 0.95, "evidence": ["M1"], "author": "甲"},
            "2026-05-10T08:00"))
        bad = a.emit("publication.release", {
            "release_id": "R-anachron", "title": "穿越发布",
            "date": "2026-01-01",
            "entries": [{"claim_id": "C1", "as": "established"}]},
            "2026-06-01T08:00")
        self.assertEqual(store.ingest(bad)["status"], "quarantined")


class ChainIntegrityTest(unittest.TestCase):
    def test_gap_waits_then_self_heals(self):
        dev = Device("C")
        e1 = dev.emit("record.register", {"record_id": "X1", "kind": "遗迹",
                                           "label": "X1", "properties": {}},
                      "2026-01-01T08:00")
        e2 = dev.emit("record.register", {"record_id": "X2", "kind": "遗迹",
                                           "label": "X2", "properties": {}},
                      "2026-01-02T08:00")
        e3 = dev.emit("spatial.relate", {"source": "X1", "target": "X2",
                                          "relation": "above"},
                      "2026-01-03T08:00")
        store = FieldworkStore()
        store.ingest(e1)
        # e2 离线，e3 先归队：缺环等待，投影不受污染。
        self.assertEqual(store.ingest(e3)["status"], "waiting")
        self.assertNotIn("X2", store.projection.records)
        # 祖先归队后自愈。
        self.assertEqual(store.ingest(e2)["status"], "applied")
        self.assertEqual(store.status_of(e3["event_id"])["status"], "applied")
        chains = store.verify_chains()
        self.assertTrue(all(info["ok"] for info in chains.values()))

    def test_broken_chain_quantined_with_successors(self):
        dev = Device("C")
        e1 = dev.emit("record.register", {"record_id": "Y1", "kind": "遗迹",
                                           "label": "Y1", "properties": {}},
                      "2026-01-01T08:00")
        e2 = dev.raw(2, "record.register", {"record_id": "Y2", "kind": "遗迹",
                                             "label": "Y2", "properties": {}},
                     "2026-01-02T08:00", prev_hash="deadbeef")
        e3 = dev.emit("record.register", {"record_id": "Y3", "kind": "遗迹",
                                           "label": "Y3", "properties": {}},
                      "2026-01-03T08:00")
        store = FieldworkStore()
        store.ingest_batch([e1, e2, e3])
        self.assertEqual(store.status_of("C-2")["status"], "quarantined")
        self.assertEqual(store.status_of("C-3")["status"], "quarantined")
        self.assertNotIn("Y2", store.projection.records)
        self.assertNotIn("Y3", store.projection.records)


class PersistenceTest(unittest.TestCase):
    def test_jsonl_reload_reproduces_state(self):
        a, b = build_expedition()
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "journal.jsonl")
            first = FieldworkStore(journal_path=path)
            # 乱序 + 重复提交落盘。
            first.ingest_batch(list(reversed(a.events)))
            first.ingest_batch(b.events)
            first.ingest(a.events[0])
            second = FieldworkStore()
            summary = second.load_jsonl(path)
            self.assertEqual(summary["loaded"], len(a.events) + len(b.events))
            self.assertEqual(
                fw.canonical_json(fw.build_views(first)),
                fw.canonical_json(fw.build_views(second)),
            )
            self.assertEqual(first.verify_chains(), second.verify_chains())


class HttpApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        service.reset_store()
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), service.Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)

    def _request(self, method, path, payload=None):
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8") if payload is not None else None
        request = Request(self.base + path, data=data, method=method,
                          headers={"Content-Type": "application/json"})
        try:
            with urlopen(request, timeout=3) as response:
                return response.status, json.load(response)
        except HTTPError as error:
            return error.code, json.load(error)

    def _get(self, path):
        with urlopen(self.base + path, timeout=3) as response:
            return json.load(response)

    def test_full_scenario_over_http(self):
        a, b = build_expedition()
        status, body = self._request("POST", "/events",
                                     {"events": list(reversed(a.events))})
        self.assertEqual(status, 200)
        status, body = self._request("POST", "/events", b.events[0])
        self.assertEqual(status, 200)

        records = self._get("/records")["records"]
        self.assertTrue({r["record_id"] for r in records} >= {"M1", "S1", "V1"})

        history = self._get("/records/history?record_id=M1")
        self.assertEqual(len(history["versions"]), 2)
        self.assertEqual(history["versions"][0]["properties"]["形制"], "竖穴")

        samples = self._get("/samples")["samples"]
        self.assertEqual(samples[0]["balance"], 7.0)

        custody = self._get("/custody")["items"]["V1"]
        self.assertEqual(custody["holder"], "分馆库房")

        conflicts = self._get("/conflicts")
        self.assertIn({"number": "M001", "claimants": ["M1", "M9"],
                       "status": "unresolved"}, conflicts["numbering"])

        relations = self._get("/relations")
        self.assertIn(["T1", "contains", "M1"], relations["inferred"])

        # 观点 -> 评议 -> 发布全链路。
        def post(event):
            code, resp = self._request("POST", "/events", event)
            return code, resp

        post(a.emit("claim.submit", {
            "claim_id": "C1", "subject": "M1年代", "proposition": "战国晚期",
            "confidence": 0.9, "evidence": ["D1", "G1"], "author": "甲"},
            "2026-05-10T08:00"))
        post(a.emit("peer.review", {
            "claim_id": "C1", "reviewer": "丁", "verdict": "support",
            "comment": "成立"}, "2026-05-12T08:00"))
        code, resp = post(a.emit("publication.release", {
            "release_id": "R1", "title": "五月简报", "date": "2026-05-15",
            "entries": [{"claim_id": "C1", "as": "established"}]},
            "2026-05-14T08:00"))
        self.assertEqual(code, 200)
        releases = self._get("/releases")["releases"]
        self.assertEqual(releases[0]["release_id"], "R1")

        asof = self._get("/asof?date=2025-12-31T00:00:00")
        self.assertFalse(any(r["record_id"] == "G1" for r in asof["records"]))

    def test_bad_json_returns_400(self):
        code, _ = self._request("POST", "/events", None)
        # 无 body 时服务端按 {} 解析，报结构错误而不是 500。
        request = Request(self.base + "/events", data=b"{not json",
                          method="POST",
                          headers={"Content-Type": "application/json"})
        with self.assertRaises(HTTPError) as error:
            urlopen(request, timeout=3)
        self.assertEqual(error.exception.code, 400)
        error.exception.close()


if __name__ == "__main__":
    unittest.main()
