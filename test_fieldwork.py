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


class LoanTestBase(unittest.TestCase):
    """借用测试公共夹具：S1 初始 10g、已直接消耗 3g（余 7g），证据 D1/G1 有效。"""

    def setUp(self):
        self.a, self.b = build_expedition()
        self.store = FieldworkStore()
        self.store.ingest_batch(self.a.events)
        self.store.ingest(self.a.emit("claim.submit", {
            "claim_id": "C-loan", "subject": "M1年代",
            "proposition": "主墓下葬于战国晚期", "confidence": 0.9,
            "evidence": ["D1", "G1"], "author": "研究员甲"},
            "2026-05-10T08:00"))

    def loan_apply(self, loan_id="L1", *, amount=4.0, lab="北大实验室",
                   applicant="研究员甲", due="2026-08-01T00:00",
                   at="2026-05-15T08:00", device=None, claim="C-loan"):
        dev = device or self.a
        return dev.emit("loan.apply", {
            "loan_id": loan_id, "sample_id": "S1", "claim_id": claim,
            "applicant": applicant, "lab": lab,
            "purpose": "残留物分析", "storage_condition": "4℃避光",
            "requested_amount": amount, "due_date": due}, at)

    def approve(self, loan_id="L1", approver="库管主任丙",
                at="2026-05-16T09:00", device=None):
        dev = device or self.a
        return dev.emit("loan.approve",
                        {"loan_id": loan_id, "approver": approver}, at)

    def sample_s1(self):
        return next(s for s in fw.build_views(self.store)["samples"]
                    if s["sample_id"] == "S1")

    def loan(self, loan_id="L1"):
        return next(l for l in fw.build_views(self.store)["loans"]
                    if l["loan_id"] == loan_id)


class LoanLifecycleTest(LoanTestBase):
    def test_apply_anchors_current_sample_and_claim_versions(self):
        self.assertEqual(self.store.ingest(self.loan_apply())["status"], "applied")
        loan = self.loan()
        # S1 注册后无校正，锚定样本 v1；主张亦为 v1。
        self.assertEqual(loan["sample_version"], 1)
        self.assertEqual(loan["claim_version"], 1)
        self.assertTrue(loan["sample_version_current"])
        self.assertTrue(loan["claim_version_current"])
        # 未审批不占用余量。
        self.assertEqual(self.sample_s1()["reserved_amount"], 0.0)
        self.assertEqual(self.sample_s1()["redistributable"], 7.0)

    def test_anchored_version_survives_later_revision(self):
        self.store.ingest(self.loan_apply())
        self.store.ingest(self.approve())
        # 主张之后出新版：借单仍锚定 v1，不被视为失效，仍可领用。
        self.store.ingest(self.a.emit("claim.revise", {
            "claim_id": "C-loan", "proposition": "战国晚期偏晚",
            "confidence": 0.85, "evidence": ["D1", "G1"]}, "2026-05-17T08:00"))
        loan = self.loan()
        self.assertEqual(loan["claim_version"], 1)
        self.assertFalse(loan["claim_version_current"])
        ev = self.a.emit("loan.pickup", {"loan_id": "L1", "amount": 1.0,
                                        "actor": "丁", "condition": "完好"},
                         "2026-05-18T08:00")
        self.assertEqual(self.store.ingest(ev)["status"], "applied")

    def test_conflicted_approvers_rejected(self):
        self.store.ingest(self.loan_apply())
        for bad in ("研究员甲", "北大实验室"):
            ev = self.approve(approver=bad, at=f"2026-05-16T{bad and '09'}:"
                                                f"00")
            result = self.store.ingest(ev)
            self.assertEqual(result["status"], "quarantined",
                             f"{bad} 存在利益冲突却审批成功")
        # 主张作者同样冲突。
        ev = self.a.emit("loan.approve", {"loan_id": "L1",
                                          "approver": "研究员甲"},
                         "2026-05-16T10:00")
        self.assertEqual(self.store.ingest(ev)["status"], "quarantined")
        # 无冲突保管人通过并锁定。
        self.assertEqual(self.store.ingest(self.approve())["status"], "applied")
        self.assertEqual(self.loan()["status"], "reserved")
        self.assertEqual(self.sample_s1()["reserved_amount"], 4.0)
        self.assertEqual(self.sample_s1()["redistributable"], 3.0)

    def test_partial_pickup_return_consume_form_lineage_and_settle(self):
        self.store.ingest(self.loan_apply(amount=4.0))
        self.store.ingest(self.approve())
        self.store.ingest(self.a.emit("loan.pickup", {
            "loan_id": "L1", "amount": 3.0, "actor": "丁",
            "condition": "封口完好"}, "2026-05-20T08:00"))
        loan = self.loan()
        self.assertEqual(loan["status"], "active")
        self.assertEqual(loan["out_amount"], 3.0)
        # 尚有 1g 已批未领，继续预锁。
        self.assertEqual(self.sample_s1()["reserved_amount"], 1.0)
        self.assertEqual(self.sample_s1()["out_on_loan"], 3.0)

        # 超额领用被拒。
        over = self.a.emit("loan.pickup", {"loan_id": "L1", "amount": 2.0,
                                           "actor": "丁", "condition": "完好"},
                           "2026-05-21T08:00")
        self.assertEqual(self.store.ingest(over)["status"], "quarantined")
        self.assertEqual(self.loan()["picked_amount"], 3.0)

        # 部分归还 2g、在外消耗 1g：在外清零但有 1g 未交付，借单仍 active。
        self.store.ingest(self.a.emit("loan.return", {
            "loan_id": "L1", "amount": 2.0, "actor": "丁",
            "condition": "2g 完好"}, "2026-06-01T08:00"))
        self.store.ingest(self.a.emit("loan.consume", {
            "loan_id": "L1", "amount": 1.0, "actor": "丁",
            "purpose": "残留提取"}, "2026-06-02T08:00"))
        loan = self.loan()
        self.assertEqual(loan["out_amount"], 0.0)
        self.assertEqual(loan["returned_amount"], 2.0)
        self.assertEqual(loan["consumed_amount"], 1.0)
        kinds = [e["type"] for e in loan["lineage"]]
        self.assertEqual(kinds, ["apply", "approve", "pickup", "return", "consume"])
        # 归还量不得超过在外量。
        bad = self.a.emit("loan.return", {
            "loan_id": "L1", "amount": 1.0, "actor": "丁",
            "condition": "重复归还"}, "2026-06-03T08:00")
        self.assertEqual(self.store.ingest(bad)["status"], "quarantined")

    def test_return_more_than_out_rejected(self):
        self.store.ingest(self.loan_apply(amount=2.0))
        self.store.ingest(self.approve())
        # 未领用即归还：拒绝。
        bad = self.a.emit("loan.return", {"loan_id": "L1", "amount": 1.0,
                                          "actor": "丁", "condition": "x"},
                          "2026-05-19T08:00")
        self.assertEqual(self.store.ingest(bad)["status"], "quarantined")

    def test_overdraft_application_quarantined_on_approval(self):
        # S1 可再分配 7g；申请 8g 在审批锁余量时隔离，申请事件保留。
        self.store.ingest(self.loan_apply(amount=8.0))
        result = self.store.ingest(self.approve())
        self.assertEqual(result["status"], "quarantined")
        self.assertIn("可再分配余量不足", result["reasons"][0])
        # 申请仍在但未锁定任何余量。
        self.assertEqual(self.loan()["status"], "requested")
        self.assertEqual(self.sample_s1()["reserved_amount"], 0.0)
        self.assertEqual(self.sample_s1()["redistributable"], 7.0)


class LoanConcurrencyTest(LoanTestBase):
    def test_concurrent_approvals_deterministically_compete_for_margin(self):
        # A、B 两台设备离线并发，各申请 6g（可再分配仅 7g）。
        # 显式 lamport 满足 happens-before：两份申请先于两份审批进入规范化全序，
        # A.apply(30) < B.apply(31) < A.approve(40) < B.approve(41)。
        a_apply = self.a.emit("loan.apply", {
            "loan_id": "LA", "sample_id": "S1", "claim_id": "C-loan",
            "applicant": "研究员甲", "lab": "北大实验室",
            "purpose": "残留分析", "storage_condition": "4℃避光",
            "requested_amount": 6.0, "due_date": "2026-08-01T00:00"},
            "2026-05-15T08:00", lamport=30)
        b_apply = self.b.emit("loan.apply", {
            "loan_id": "LB", "sample_id": "S1", "claim_id": "C-loan",
            "applicant": "研究员乙", "lab": "社科院实验室",
            "purpose": "同位素", "storage_condition": "常温干燥",
            "requested_amount": 6.0, "due_date": "2026-08-10T00:00"},
            "2026-05-15T09:00", lamport=31)
        a_approve = self.a.emit("loan.approve",
                                {"loan_id": "LA", "approver": "库管主任丙"},
                                "2026-05-16T08:00", lamport=40)
        b_approve = self.b.emit("loan.approve",
                                {"loan_id": "LB", "approver": "省所保管人戊"},
                                "2026-05-16T09:00", lamport=41)
        base_a = [e for e in self.a.events if not e["type"].startswith("loan.")]
        base_b = [e for e in self.b.events if not e["type"].startswith("loan.")]
        loan_events = [a_apply, b_apply, a_approve, b_approve]
        orders = [
            loan_events,                                   # 正序
            list(reversed(loan_events)),                  # 逆序
            [b_approve, a_approve, b_apply, a_apply],     # 穿插乱序
        ]
        canonical = None
        for order in orders:
            store = FieldworkStore()
            store.ingest_batch(base_a)
            store.ingest_batch(base_b)
            store.ingest_batch(order)
            views = fw.build_views(store)
            snapshot = fw.canonical_json(views)
            canonical = canonical or snapshot
            # 与物理到达顺序无关：任意重放字节级一致。
            self.assertEqual(snapshot, canonical)
            la = next(l for l in views["loans"] if l["loan_id"] == "LA")
            lb = next(l for l in views["loans"] if l["loan_id"] == "LB")
            # 规范化全序下 A 先锁 6g；B 的 6g 超额被隔离，零占用。
            self.assertEqual(la["status"], "reserved")
            self.assertEqual(lb["status"], "requested")
            s1 = next(s for s in views["samples"] if s["sample_id"] == "S1")
            self.assertEqual(s1["reserved_amount"], 6.0)
            self.assertEqual(s1["redistributable"], 1.0)
            self.assertTrue(s1["reconciled"])
            quarantined = {q["event_id"]
                           for q in views["conflicts"]["quarantined_events"]}
            self.assertIn(b_approve["event_id"], quarantined)

    def test_offline_duplicate_requests_do_not_double_spend(self):
        apply_ev = self.loan_apply(amount=3.0)
        self.store.ingest(apply_ev)
        self.store.ingest(self.approve())
        pickup = self.a.emit("loan.pickup", {
            "loan_id": "L1", "amount": 3.0, "actor": "丁",
            "condition": "完好"}, "2026-05-20T08:00")
        self.assertEqual(self.store.ingest(dict(pickup))["status"], "applied")
        # 离线客户端重发同一领用事件：duplicate，在外量不翻倍。
        for _ in range(3):
            self.assertEqual(self.store.ingest(json.loads(
                json.dumps(pickup, ensure_ascii=False)))["status"], "duplicate")
        self.assertEqual(self.loan()["out_amount"], 3.0)
        # 同一申请重发同样幂等（同 event_id 与载荷，不二次占用）。
        self.assertEqual(self.store.ingest(json.loads(
            json.dumps(apply_ev, ensure_ascii=False)))["status"], "duplicate")
        self.assertEqual(self.sample_s1()["out_on_loan"], 3.0)
        self.assertEqual(self.sample_s1()["reserved_amount"], 0.0)

    def test_direct_consume_cannot_take_locked_margin(self):
        self.store.ingest(self.loan_apply(amount=6.0))
        self.store.ingest(self.approve())
        # 锁定 6g 后仅剩 1g，直接消耗 2g 必须隔离。
        bad = self.a.emit("sample.consume", {
            "sample_id": "S1", "amount": 2.0,
            "purpose": "侵占锁定余量"}, "2026-05-17T08:00")
        self.assertEqual(self.store.ingest(bad)["status"], "quarantined")
        self.assertEqual(self.sample_s1()["redistributable"], 1.0)
        ok = self.a.emit("sample.consume", {
            "sample_id": "S1", "amount": 1.0,
            "purpose": "在可再分配内"}, "2026-05-17T09:00")
        self.assertEqual(self.store.ingest(ok)["status"], "applied")
        self.assertTrue(self.sample_s1()["reconciled"])


class LoanFreezeTest(LoanTestBase):
    def _approved_partial_pickup(self, amount_reserved=4.0, picked=3.0):
        self.store.ingest(self.loan_apply(amount=amount_reserved))
        self.store.ingest(self.approve())
        self.store.ingest(self.a.emit("loan.pickup", {
            "loan_id": "L1", "amount": picked, "actor": "丁",
            "condition": "完好"}, "2026-05-20T08:00"))

    def test_withdrawn_dating_freezes_undelivered_but_keeps_out_part(self):
        self._approved_partial_pickup()
        self.store.ingest(self.a.emit("dating.withdraw", {
            "dating_id": "D1", "reason": "送检样本污染"}, "2026-05-21T08:00"))
        loan = self.loan()
        self.assertEqual(loan["status"], "frozen")
        self.assertEqual(loan["base_status"], "active")
        self.assertEqual(loan["frozen_amount"], 1.0)
        self.assertEqual(loan["out_amount"], 3.0)
        self.assertTrue(loan["freeze_reasons"])
        # 冻结余量仍占用（不能偷偷再分配），可再分配维持 3g。
        self.assertEqual(self.sample_s1()["frozen_amount"], 1.0)
        self.assertEqual(self.sample_s1()["redistributable"], 3.0)
        # 未交付的 1g 禁止出库。
        bad = self.a.emit("loan.pickup", {"loan_id": "L1", "amount": 1.0,
                                          "actor": "丁", "condition": "完好"},
                          "2026-05-22T08:00")
        self.assertEqual(self.store.ingest(bad)["status"], "quarantined")
        # 冻结借单进入冲突清单。
        frozen = fw.build_views(self.store)["conflicts"]["frozen_loans"]
        self.assertEqual([f["loan_id"] for f in frozen], ["L1"])

    def test_withdrawn_claim_freezes_reserved_loan(self):
        self.store.ingest(self.loan_apply(amount=4.0))
        self.store.ingest(self.approve())
        self.store.ingest(self.a.emit("claim.withdraw", {
            "claim_id": "C-loan", "reason": "年代判断被新地层证据推翻"},
            "2026-05-19T08:00"))
        loan = self.loan()
        self.assertEqual(loan["status"], "frozen")
        self.assertEqual(loan["frozen_amount"], 4.0)
        bad = self.a.emit("loan.pickup", {"loan_id": "L1", "amount": 4.0,
                                          "actor": "丁", "condition": "完好"},
                          "2026-05-20T08:00")
        self.assertEqual(self.store.ingest(bad)["status"], "quarantined")

    def test_application_after_claim_withdrawn_is_quarantined(self):
        self.store.ingest(self.a.emit("claim.withdraw", {
            "claim_id": "C-loan", "reason": "撤回"}, "2026-05-12T08:00"))
        result = self.store.ingest(self.loan_apply(at="2026-05-15T08:00"))
        self.assertEqual(result["status"], "quarantined")
        self.assertNotIn("L1", self.store.projection.loans)

    def test_quarantined_context_cannot_back_application(self):
        # 断链设备上的主张不进入投影；引用它的借用申请必须隔离、不扣减。
        dev = Device("tablet-Q")
        broken_claim = dev.raw(1, "claim.submit", {
            "claim_id": "C-ghost", "subject": "M1年代",
            "proposition": "来自断链设备的主张", "confidence": 0.9,
            "evidence": ["G1"], "author": "某人"},
            "2026-05-14T08:00", prev_hash="deadbeef")
        self.assertEqual(self.store.ingest(broken_claim)["status"],
                         "quarantined")
        ev = self.loan_apply(claim="C-ghost", at="2026-05-15T08:00")
        self.assertEqual(self.store.ingest(ev)["status"], "quarantined")
        self.assertEqual(self.sample_s1()["reserved_amount"], 0.0)
        self.assertEqual(self.sample_s1()["redistributable"], 7.0)

    def test_recall_releases_frozen_margin_for_redistribution(self):
        self._approved_partial_pickup()
        self.store.ingest(self.a.emit("dating.withdraw", {
            "dating_id": "D1", "reason": "污染"}, "2026-05-21T08:00"))
        # 召回：冻结的 1g 未交付立即释放。
        recall = self.a.emit("loan.recall", {
            "loan_id": "L1", "by": "库管主任丙",
            "reason": "引用年代判断撤回"}, "2026-05-22T08:00")
        self.assertEqual(self.store.ingest(recall)["status"], "applied")
        loan = self.loan()
        self.assertEqual(loan["status"], "recalled")
        self.assertEqual(loan["released_amount"], 1.0)
        self.assertEqual(loan["out_amount"], 3.0)
        # 释放后 1g 回到可再分配（在外 3g 仍占用）。
        self.assertEqual(self.sample_s1()["frozen_amount"], 0.0)
        self.assertEqual(self.sample_s1()["redistributable"], 4.0)
        # 追索中的在外部分归还/消耗后结案。
        self.store.ingest(self.a.emit("loan.return", {
            "loan_id": "L1", "amount": 2.0, "actor": "丁",
            "condition": "追回 2g"}, "2026-06-10T08:00"))
        self.store.ingest(self.a.emit("loan.consume", {
            "loan_id": "L1", "amount": 1.0, "actor": "丁",
            "purpose": "追回确认已耗"}, "2026-06-11T08:00"))
        loan = self.loan()
        self.assertEqual(loan["status"], "completed")
        self.assertEqual(loan["out_amount"], 0.0)
        # 追回归还的 2g 回库、确认消耗 1g：初始10 − 直接3 − 借用消耗1 = 6。
        self.assertEqual(self.sample_s1()["redistributable"], 6.0)
        self.assertTrue(self.sample_s1()["reconciled"])


class LoanOverdueAndAsOfTest(LoanTestBase):
    def _active_loan(self, amount=2.0, due="2026-07-01T00:00"):
        self.store.ingest(self.loan_apply(amount=amount, due=due))
        self.store.ingest(self.approve())
        self.store.ingest(self.a.emit("loan.pickup", {
            "loan_id": "L1", "amount": amount, "actor": "丁",
            "condition": "完好"}, "2026-05-20T08:00"))

    def test_overdue_is_derived_purely_from_query_date(self):
        self._active_loan()
        before = fw.state_as_of(self.store,
                                datetime.fromisoformat("2026-06-30T00:00"))
        loan = next(l for l in before["loans"] if l["loan_id"] == "L1")
        s1 = next(s for s in before["samples"] if s["sample_id"] == "S1")
        self.assertFalse(loan["overdue"])
        self.assertEqual(s1["overdue_amount"], 0.0)
        self.assertEqual(s1["out_on_loan"], 2.0)
        self.assertEqual(s1["redistributable"], 5.0)

        after = fw.state_as_of(self.store,
                               datetime.fromisoformat("2026-07-05T00:00"))
        loan = next(l for l in after["loans"] if l["loan_id"] == "L1")
        s1 = next(s for s in after["samples"] if s["sample_id"] == "S1")
        self.assertTrue(loan["overdue"])
        self.assertEqual(loan["overdue_since"], "2026-07-01T00:00:00")
        self.assertEqual(s1["overdue_amount"], 2.0)
        # 逾期未召回：仍是 active（非追索），但占用不变、账面对账成立。
        self.assertEqual(loan["status"], "active")
        self.assertEqual(s1["reclaim_amount"], 0.0)
        self.assertTrue(s1["reconciled"])

    def test_recall_moves_overdue_amount_into_reclaim(self):
        self._active_loan()
        self.store.ingest(self.a.emit("loan.recall", {
            "loan_id": "L1", "by": "库管主任丙",
            "reason": "逾期未还"}, "2026-07-06T08:00"))
        after = fw.state_as_of(self.store,
                               datetime.fromisoformat("2026-07-10T00:00"))
        loan = next(l for l in after["loans"] if l["loan_id"] == "L1")
        s1 = next(s for s in after["samples"] if s["sample_id"] == "S1")
        self.assertEqual(loan["status"], "recalled")
        self.assertTrue(loan["in_recall"])
        self.assertEqual(s1["reclaim_amount"], 2.0)
        self.assertEqual(s1["overdue_amount"], 2.0)
        self.assertEqual(s1["out_on_loan"], 2.0)

    def test_historical_point_shows_reserved_then_out_then_settled(self):
        self._active_loan(amount=2.0)
        # 审批后、领用前：在借 0、预锁 2。
        point = fw.state_as_of(self.store,
                               datetime.fromisoformat("2026-05-18T00:00"))
        s1 = next(s for s in point["samples"] if s["sample_id"] == "S1")
        loan = next(l for l in point["loans"] if l["loan_id"] == "L1")
        self.assertEqual(loan["status"], "reserved")
        self.assertEqual(s1["out_on_loan"], 0.0)
        self.assertEqual(s1["reserved_amount"], 2.0)
        self.assertEqual(s1["redistributable"], 5.0)
        # 归还结案后：在借归 0、消耗/归还入账、可再分配回升。
        self.store.ingest(self.a.emit("loan.return", {
            "loan_id": "L1", "amount": 1.0, "actor": "丁",
            "condition": "完好"}, "2026-06-01T08:00"))
        self.store.ingest(self.a.emit("loan.consume", {
            "loan_id": "L1", "amount": 1.0, "actor": "丁",
            "purpose": "检测消耗"}, "2026-06-02T08:00"))
        settled = fw.state_as_of(self.store,
                                 datetime.fromisoformat("2026-06-03T00:00"))
        s1 = next(s for s in settled["samples"] if s["sample_id"] == "S1")
        loan = next(l for l in settled["loans"] if l["loan_id"] == "L1")
        self.assertEqual(loan["status"], "completed")
        self.assertEqual(s1["out_on_loan"], 0.0)
        self.assertEqual(s1["reserved_amount"], 0.0)
        # 初始10 - 直接消耗3 - 借用消耗1 - 在外0 - 预锁0 = 6。
        self.assertEqual(s1["redistributable"], 6.0)
        self.assertTrue(s1["reconciled"])


class LoanReconciliationTest(LoanTestBase):
    def test_books_match_custody_chain_across_mixed_loans(self):
        # 两张借单：LA 领 2g 在外；LB 批 3g 尚未领（预锁）。
        self.store.ingest(self.loan_apply("LA", amount=2.0,
                                          at="2026-05-15T08:00"))
        self.store.ingest(self.approve("LA", at="2026-05-16T08:00"))
        self.store.ingest(self.a.emit("loan.pickup", {
            "loan_id": "LA", "amount": 2.0, "actor": "丁",
            "condition": "完好"}, "2026-05-18T08:00"))
        self.store.ingest(self.loan_apply("LB", amount=3.0,
                                          applicant="研究员乙",
                                          lab="社科院实验室",
                                          at="2026-05-15T09:00"))
        self.store.ingest(self.approve("LB", approver="省所保管人戊",
                                       at="2026-05-16T09:00"))
        s1 = self.sample_s1()
        # 10 = 直接3 + 借用消耗0 + 在外2 + 预锁3 + 可再分配2。
        self.assertEqual(s1["out_on_loan"], 2.0)
        self.assertEqual(s1["reserved_amount"], 3.0)
        self.assertEqual(s1["redistributable"], 2.0)
        self.assertTrue(s1["reconciled"])
        # 所有样本恒等式都成立（含夹具里的其他样本）。
        self.assertTrue(all(s["reconciled"]
                            for s in fw.build_views(self.store)["samples"]))


class LoanChainGateTest(LoanTestBase):
    def test_gap_holds_reservation_until_ancestor_arrives(self):
        dev = Device("tablet-L")
        apply_ev = dev.emit("loan.apply", {
            "loan_id": "LG", "sample_id": "S1", "claim_id": "C-loan",
            "applicant": "研究员甲", "lab": "北大实验室",
            "purpose": "分析", "storage_condition": "常温",
            "requested_amount": 4.0, "due_date": "2026-08-01T00:00"},
            "2026-05-15T08:00")
        approve_ev = dev.emit("loan.approve", {"loan_id": "LG",
                                               "approver": "库管主任丙"},
                              "2026-05-16T08:00")
        # 审批先离线归队、申请缺环：waiting，不锁定任何余量。
        self.assertEqual(self.store.ingest(approve_ev)["status"], "waiting")
        self.assertEqual(self.sample_s1()["reserved_amount"], 0.0)
        self.assertNotIn("LG", self.store.projection.loans)
        # 祖先归队后自愈：申请与审批依次生效，锁定 4g。
        self.assertEqual(self.store.ingest(apply_ev)["status"], "applied")
        self.assertEqual(self.store.status_of(approve_ev["event_id"])["status"],
                         "applied")
        self.assertEqual(self.loan("LG")["status"], "reserved")
        self.assertEqual(self.sample_s1()["reserved_amount"], 4.0)
        self.assertEqual(self.sample_s1()["redistributable"], 3.0)

    def test_broken_loan_chain_quarantined_without_deduction(self):
        dev = Device("tablet-BR")
        dev.emit("loan.apply", {
            "loan_id": "LB1", "sample_id": "S1", "claim_id": "C-loan",
            "applicant": "甲", "lab": "L", "purpose": "p",
            "storage_condition": "常温", "requested_amount": 4.0,
            "due_date": "2026-08-01T00:00"}, "2026-05-15T08:00")
        bad = dev.raw(2, "loan.approve", {"loan_id": "LB1",
                                          "approver": "库管主任丙"},
                      "2026-05-16T08:00", prev_hash="deadbeef")
        successor = dev.emit("loan.pickup", {"loan_id": "LB1", "amount": 4.0,
                                             "actor": "丁",
                                             "condition": "完好"},
                             "2026-05-17T08:00")
        self.store.ingest_batch([bad, successor])
        self.assertEqual(self.store.status_of(bad["event_id"])["status"],
                         "quarantined")
        self.assertEqual(self.store.status_of(successor["event_id"])["status"],
                         "quarantined")
        # 断链审批不锁定、断链领用不扣减在外量。
        self.assertEqual(self.sample_s1()["reserved_amount"], 0.0)
        self.assertEqual(self.sample_s1()["out_on_loan"], 0.0)
        self.assertEqual(self.sample_s1()["redistributable"], 7.0)


class LoanPersistenceTest(LoanTestBase):
    def test_restart_restores_occupancy_recall_and_overdue(self):
        self.store.ingest(self.loan_apply(amount=4.0))
        self.store.ingest(self.approve())
        pickup = self.a.emit("loan.pickup", {
            "loan_id": "L1", "amount": 3.0, "actor": "丁",
            "condition": "完好"}, "2026-05-20T08:00")
        self.store.ingest(pickup)
        self.store.ingest(self.a.emit("dating.withdraw", {
            "dating_id": "D1", "reason": "污染"}, "2026-05-21T08:00"))
        self.store.ingest(self.a.emit("loan.recall", {
            "loan_id": "L1", "by": "库管主任丙",
            "reason": "依据失效"}, "2026-05-22T08:00"))
        # 离线重复领用请求：duplicate，不重复扣减。
        self.assertEqual(self.store.ingest(json.loads(
            json.dumps(pickup, ensure_ascii=False)))["status"], "duplicate")

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "loans.jsonl")
            # 用物理日志落盘一个全新 store（等价于重启加载同一份 JSONL）。
            first = FieldworkStore(journal_path=path)
            first.ingest_batch(list(reversed(self.store.physical_log())))
            second = FieldworkStore()
            second.load_jsonl(path)
            self.assertEqual(
                fw.canonical_json(fw.build_views(first)),
                fw.canonical_json(fw.build_views(second)),
            )
            views = fw.build_views(second)
            loan = next(l for l in views["loans"] if l["loan_id"] == "L1")
            s1 = next(s for s in views["samples"] if s["sample_id"] == "S1")
            # 占用/在外/追索/冻结全部还原。
            self.assertEqual(loan["status"], "recalled")
            self.assertEqual(loan["picked_amount"], 3.0)
            self.assertEqual(loan["released_amount"], 1.0)
            self.assertEqual(s1["out_on_loan"], 3.0)
            self.assertEqual(s1["reclaim_amount"], 3.0)
            self.assertEqual(s1["frozen_amount"], 0.0)
            self.assertEqual(s1["redistributable"], 4.0)
            self.assertTrue(s1["reconciled"])
            # 重启后按历史时点仍能准确还原在借/逾期。
            past = fw.state_as_of(second,
                                  datetime.fromisoformat("2026-08-05T00:00"))
            past_loan = next(l for l in past["loans"] if l["loan_id"] == "L1")
            past_s1 = next(s for s in past["samples"] if s["sample_id"] == "S1")
            self.assertTrue(past_loan["overdue"])
            self.assertEqual(past_s1["overdue_amount"], 3.0)
            # 重复领用落盘/重放后依旧不二次扣减。
            self.assertEqual(past_s1["out_on_loan"], 3.0)


class HttpLoanTest(LoanTestBase):
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

    def _post(self, event):
        data = json.dumps(event, ensure_ascii=False).encode("utf-8")
        request = Request(self.base + "/events", data=data, method="POST",
                          headers={"Content-Type": "application/json"})
        with urlopen(request, timeout=3) as response:
            return response.status, json.load(response)

    def _get(self, path):
        with urlopen(self.base + path, timeout=3) as response:
            return json.load(response)

    def test_loans_endpoint_and_asof_reconciliation(self):
        # 灌入夹具事件（含主张）。
        status, _ = self._post({"events": self.a.events})
        self.assertEqual(status, 200)
        self._post(self.loan_apply(amount=4.0, due="2026-07-01T00:00"))
        self._post(self.approve())
        self._post(self.a.emit("loan.pickup", {
            "loan_id": "L1", "amount": 4.0, "actor": "丁",
            "condition": "完好"}, "2026-05-20T08:00"))

        loans = self._get("/loans")
        loan = next(l for l in loans["loans"] if l["loan_id"] == "L1")
        self.assertEqual(loan["status"], "active")
        self.assertEqual(loan["out_amount"], 4.0)
        summary = next(s for s in loans["samples"] if s["sample_id"] == "S1")
        self.assertEqual(summary["out_on_loan"], 4.0)
        self.assertEqual(summary["redistributable"], 3.0)
        self.assertTrue(summary["reconciled"])

        # /samples 同样给出对账字段。
        s1 = next(s for s in self._get("/samples")["samples"]
                  if s["sample_id"] == "S1")
        self.assertTrue(s1["reconciled"])

        # 指定历史日期：领用前为 reserved，在借为 0。
        past = self._get("/loans?date=2026-05-18T00:00:00")
        loan = next(l for l in past["loans"] if l["loan_id"] == "L1")
        self.assertEqual(loan["status"], "reserved")
        s1p = next(s for s in past["samples"] if s["sample_id"] == "S1")
        self.assertEqual(s1p["out_on_loan"], 0.0)
        self.assertEqual(s1p["redistributable"], 3.0)

        # 逾期后查询。
        due = self._get("/loans?date=2026-07-05T00:00:00")
        loan = next(l for l in due["loans"] if l["loan_id"] == "L1")
        self.assertTrue(loan["overdue"])
        s1d = next(s for s in due["samples"] if s["sample_id"] == "S1")
        self.assertEqual(s1d["overdue_amount"], 4.0)


if __name__ == "__main__":
    unittest.main()
