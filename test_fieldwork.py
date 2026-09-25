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


class LoanFixture:
    """借用场景构造器：在发掘场景之上追加研究主张，便于借用测试复用。"""

    def __init__(self):
        self.a, self.b = build_expedition()
        self.store = FieldworkStore()
        self.store.ingest_batch(self.a.events)
        self.store.ingest(self.a.emit("claim.submit", {
            "claim_id": "C-loan", "subject": "M1年代",
            "proposition": "主墓下葬于战国晚期，需复核炭样",
            "confidence": 0.9, "evidence": ["D1", "G1"],
            "author": "研究员甲"}, "2026-05-10T08:00"))

    def sample(self, sample_id="S1", as_of=None):
        views = fw.build_views(self.store, as_of=as_of)
        return next(s for s in views["samples"] if s["sample_id"] == sample_id)

    def loan(self, loan_id, as_of=None):
        views = fw.build_views(self.store, as_of=as_of)
        return next(l for l in views["loans"] if l["loan_id"] == loan_id)

    def apply(self, loan_id="L1", amount=5.0, lab="北大实验室",
              applicant="研究员甲", due="2026-08-01T08:00", claim="C-loan",
              sample_id="S1", at="2026-05-12T08:00"):
        return self.a.emit("loan.apply", {
            "loan_id": loan_id, "sample_id": sample_id, "lab_party": lab,
            "applicant": applicant, "amount": amount,
            "purpose": "同位素分析", "due_at": due, "claim_id": claim,
            "storage_condition": "冷藏4℃避光"}, at)

    def approve(self, loan_id="L1", approver="库管员甲",
                custodian="主库房", at="2026-05-13T08:00"):
        return self.a.emit("loan.approve", {
            "loan_id": loan_id, "approver": approver,
            "custodian_party": custodian}, at)

    def pickup(self, loan_id="L1", at="2026-05-15T08:00", party="主库房"):
        return self.a.emit("loan.pickup", {
            "loan_id": loan_id, "actor": "库管员甲",
            "from_party": party, "condition": "封装完好"}, at)


class LoanLifecycleTest(unittest.TestCase):
    def setUp(self):
        self.fx = LoanFixture()

    def test_apply_anchors_current_sample_and_claim_versions(self):
        self.assertEqual(self.fx.store.ingest(self.fx.apply())["status"], "applied")
        loan = self.fx.loan("L1")
        # S1 当前为 v1；主张 C-loan 当前为 v1。
        self.assertEqual(loan["sample_version"], 1)
        self.assertEqual(loan["claim_version"], 1)
        self.assertTrue(loan["sample_version_event"].startswith("tablet-A-"))
        self.assertEqual(loan["status"], "pending")
        # 待审批不锁余量。
        self.assertEqual(self.fx.sample()["locked_amount"], 0.0)
        self.assertEqual(self.fx.sample()["available_amount"], 7.0)

    def test_apply_rejects_missing_sample_or_claim(self):
        bad = self.fx.a.emit("loan.apply", {
            "loan_id": "Lx", "sample_id": "NOPE", "lab_party": "L",
            "applicant": "甲", "amount": 1.0, "purpose": "p",
            "due_at": "2026-09-01T08:00", "claim_id": "C-loan",
            "storage_condition": "干燥"}, "2026-05-12T08:00")
        self.assertEqual(self.fx.store.ingest(bad)["status"], "quarantined")
        bad2 = self.fx.apply(loan_id="Ly", claim="NOPE")
        self.assertEqual(self.fx.store.ingest(bad2)["status"], "quarantined")

    def test_due_date_must_be_future(self):
        bad = self.fx.apply(loan_id="Lpast", due="2026-05-01T08:00")
        self.assertEqual(self.fx.store.ingest(bad)["status"], "quarantined")

    def test_conflict_of_interest_approval_blocked(self):
        self.fx.store.ingest(self.fx.apply())
        # 申请人本人 / 主张作者不得审批。
        self.assertEqual(self.fx.store.ingest(
            self.fx.approve(approver="研究员甲"))["status"], "quarantined")
        # 借入实验室本身不得充当审批保管方。
        self.assertEqual(self.fx.store.ingest(
            self.fx.approve(approver="库管员甲", custodian="北大实验室"))["status"],
            "quarantined")
        # 无利益冲突的保管人审批通过。
        self.assertEqual(self.fx.store.ingest(self.fx.approve())["status"], "applied")

    def test_approval_locks_available_balance(self):
        self.fx.store.ingest(self.fx.apply(amount=5.0))
        self.fx.store.ingest(self.fx.approve())
        s = self.fx.sample()
        self.assertEqual(s["locked_amount"], 5.0)
        self.assertEqual(s["available_amount"], 2.0)
        # 账面余额不因锁定改变（实物仍在库）。
        self.assertEqual(s["balance"], 7.0)

    def test_pickup_return_partial_consume_and_recall_form_chain(self):
        fx = self.fx
        fx.store.ingest(fx.apply(amount=5.0))
        fx.store.ingest(fx.approve())
        fx.store.ingest(fx.pickup())
        s = fx.sample(as_of=fw.parse_occurred_at("2026-06-01T00:00"))
        self.assertEqual(s["on_loan_amount"], 5.0)
        self.assertEqual(s["available_amount"], 2.0)
        # 部分归还 3g：回到库房，立即可再分配，申请仍在借（剩 2g 在外）。
        fx.store.ingest(fx.a.emit("loan.return", {
            "loan_id": "L1", "actor": "实验室员乙", "amount": 3.0,
            "condition": "封装完好", "to_store": "主库房"},
            "2026-06-10T08:00"))
        s = fx.sample()
        self.assertEqual(s["on_loan_amount"], 2.0)
        self.assertEqual(s["returned_pending_amount"], 3.0)
        self.assertEqual(s["available_amount"], 5.0)
        loan = fx.loan("L1")
        self.assertEqual(loan["status"], "on_loan")
        # 消耗确认剩余 2g：计入总账，申请结清。
        fx.store.ingest(fx.a.emit("loan.consume", {
            "loan_id": "L1", "actor": "实验室员乙", "amount": 2.0,
            "note": "上机检测消耗"}, "2026-06-11T08:00"))
        s = fx.sample()
        self.assertEqual(s["loan_consumed_amount"], 2.0)
        self.assertEqual(s["balance"], 5.0)
        self.assertEqual(s["available_amount"], 5.0)
        loan = fx.loan("L1")
        self.assertEqual(loan["status"], "consumed")
        # 连续谱系：动作顺序完整可追。
        actions = [e["action"] for e in loan["ledger"]]
        self.assertEqual(actions,
                         ["apply", "approve", "pickup", "return", "consume"])
        # 已结清申请不能再追索。
        recall = fx.a.emit("loan.recall", {"loan_id": "L1", "actor": "库管员甲",
                                           "reason": "逾期"},
                           "2026-09-01T08:00")
        self.assertEqual(fx.store.ingest(recall)["status"], "quarantined")

    def test_recall_registered_on_overdue_loan(self):
        fx = self.fx
        fx.store.ingest(fx.apply(amount=2.0))
        fx.store.ingest(fx.approve())
        fx.store.ingest(fx.pickup())
        at = fw.parse_occurred_at("2026-09-01T08:00")
        loan = fx.loan("L1", as_of=at)
        self.assertTrue(loan["overdue"])
        self.assertEqual(loan["overdue_since"], "2026-08-01T08:00")
        self.assertEqual([e["action"] for e in loan["timeline"]][-1], "overdue")
        recall = fx.a.emit("loan.recall", {"loan_id": "L1", "actor": "库管员甲",
                                          "reason": "超过归还期限"},
                          "2026-09-02T08:00")
        self.assertEqual(fx.store.ingest(recall)["status"], "applied")
        loan = fx.loan("L1", as_of=at)
        self.assertEqual(len(loan["recalls"]), 1)
        self.assertEqual(loan["recalls"][0]["reason"], "超过归还期限")

    def test_over_consume_confirmation_blocked(self):
        fx = self.fx
        fx.store.ingest(fx.apply(amount=2.0))
        fx.store.ingest(fx.approve())
        fx.store.ingest(fx.pickup())
        bad = fx.a.emit("loan.consume", {"loan_id": "L1", "actor": "乙",
                                         "amount": 3.0, "note": "超报"},
                       "2026-06-01T08:00")
        self.assertEqual(fx.store.ingest(bad)["status"], "quarantined")

    def test_decline_releases_lock(self):
        fx = self.fx
        fx.store.ingest(fx.apply(amount=5.0))
        fx.store.ingest(fx.approve())
        self.assertEqual(fx.sample()["locked_amount"], 5.0)
        decline = fx.a.emit("loan.decline", {
            "loan_id": "L1", "approver": "库管员甲",
            "reason": "余量需优先保障既定项目"}, "2026-05-14T08:00")
        self.assertEqual(fx.store.ingest(decline)["status"], "applied")
        self.assertEqual(fx.sample()["locked_amount"], 0.0)
        self.assertEqual(fx.sample()["available_amount"], 7.0)

    def test_wrong_custodian_pickup_blocked(self):
        fx = self.fx
        fx.store.ingest(fx.apply())
        fx.store.ingest(fx.approve())
        bad = fx.pickup(party="外地库房")
        self.assertEqual(fx.store.ingest(bad)["status"], "quarantined")


class LoanConcurrencyTest(unittest.TestCase):
    def setUp(self):
        self.fx = LoanFixture()

    def test_concurrent_approvals_compete_for_free_balance(self):
        # 两台离线设备（两个实验室）各自发出申请与审批；规范化顺序按
        # (lamport, device_id) 全局裁决，与到达顺序无关。
        base_a, _ = build_expedition()
        store = FieldworkStore()
        store.ingest_batch(base_a.events)
        store.ingest(base_a.emit("claim.submit", {
            "claim_id": "C-loan", "subject": "M1年代",
            "proposition": "需复核炭样", "confidence": 0.9,
            "evidence": ["D1", "G1"], "author": "研究员甲"},
            "2026-05-10T08:00"))

        labA = Device("lab-A")
        labB = Device("lab-B")
        # 跨设备引用须在规范化顺序 (lamport, device_id) 上晚于基线事件
        # （基线 tablet-A 至 lamport 19），故离线设备使用靠后逻辑时钟。
        apply_a = labA.emit("loan.apply", {
            "loan_id": "LA", "sample_id": "S1", "lab_party": "甲实验室",
            "applicant": "研究员甲", "amount": 5.0, "purpose": "p",
            "due_at": "2026-09-01T08:00", "claim_id": "C-loan",
            "storage_condition": "干燥"}, "2026-05-12T08:00", lamport=100)
        apply_b = labB.emit("loan.apply", {
            "loan_id": "LB", "sample_id": "S1", "lab_party": "乙实验室",
            "applicant": "研究员乙", "amount": 5.0, "purpose": "p",
            "due_at": "2026-09-02T08:00", "claim_id": "C-loan",
            "storage_condition": "干燥"}, "2026-05-12T09:00", lamport=100)
        approve_a = labA.emit("loan.approve", {
            "loan_id": "LA", "approver": "库管员甲",
            "custodian_party": "主库房"}, "2026-05-13T08:00", lamport=101)
        approve_b = labB.emit("loan.approve", {
            "loan_id": "LB", "approver": "库管员甲",
            "custodian_party": "主库房"}, "2026-05-13T09:00", lamport=101)

        # B 实验室的请求与审批整批先归队，在部分日志上暂时锁定成功。
        self.assertEqual(store.ingest(apply_b)["status"], "applied")
        self.assertEqual(store.ingest(approve_b)["status"], "applied")
        sample = next(s for s in fw.build_views(store)["samples"]
                      if s["sample_id"] == "S1")
        self.assertEqual(sample["locked_amount"], 5.0)
        # A 实验室（canonical 顺序更早：lab-A < lab-B）晚归队：
        # 全量重放裁决 A 赢得锁定，B 的审批翻转为隔离。
        self.assertEqual(store.ingest(apply_a)["status"], "applied")
        self.assertEqual(store.ingest(approve_a)["status"], "applied")
        self.assertEqual(store.status_of(approve_b["event_id"])["status"],
                         "quarantined")
        sample = next(s for s in fw.build_views(store)["samples"]
                      if s["sample_id"] == "S1")
        self.assertEqual(sample["locked_amount"], 5.0)
        self.assertEqual(sample["available_amount"], 2.0)
        # 负方申请回退为待审批、不占余量；事件保留在隔离清单。
        loans = {l["loan_id"]: l for l in fw.build_views(store)["loans"]}
        self.assertEqual(loans["LA"]["status"], "approved")
        self.assertEqual(loans["LB"]["status"], "pending")
        quarantined = {c["event_id"] for c in
                       fw.build_views(store)["conflicts"]["quarantined_events"]}
        self.assertIn(approve_b["event_id"], quarantined)

    def test_locked_balance_blocks_field_consumption(self):
        fx = self.fx
        fx.store.ingest(fx.apply(amount=5.0))
        fx.store.ingest(fx.approve())
        # 锁定 5g 后，现场仅可消耗 2g；试图消耗 3g 隔离。
        bad = fx.a.emit("sample.consume", {"sample_id": "S1", "amount": 3.0,
                                           "purpose": "插队检测"},
                        "2026-05-14T08:00")
        self.assertEqual(fx.store.ingest(bad)["status"], "quarantined")
        ok = fx.a.emit("sample.consume", {"sample_id": "S1", "amount": 2.0,
                                          "purpose": "余量内检测"},
                       "2026-05-14T09:00")
        self.assertEqual(fx.store.ingest(ok)["status"], "applied")

    def test_concurrent_requests_byte_stable_regardless_of_arrival(self):
        fx = self.fx
        events = [
            fx.apply(loan_id="LA", amount=5.0, lab="甲实验室",
                     applicant="甲", at="2026-05-12T08:00"),
            fx.apply(loan_id="LB", amount=2.0, lab="乙实验室",
                     applicant="乙", at="2026-05-12T09:00"),
            fx.approve(loan_id="LA", at="2026-05-13T08:00"),
            fx.approve(loan_id="LB", at="2026-05-13T09:00"),
        ]
        s1 = FieldworkStore()
        s1.ingest_batch(fx.a.events[:-4])  # 不含借用事件的基线
        s1.ingest_batch(events)
        s2 = FieldworkStore()
        s2.ingest_batch(fx.a.events[:-4])
        s2.ingest_batch(list(reversed(events)))
        self.assertEqual(fw.canonical_json(fw.build_views(s1)),
                         fw.canonical_json(fw.build_views(s2)))
        # 两笔合计 7g 恰好全部锁定。
        sample = next(x for x in fw.build_views(s1)["samples"]
                      if x["sample_id"] == "S1")
        self.assertEqual(sample["locked_amount"], 7.0)
        self.assertEqual(sample["available_amount"], 0.0)


class LoanFreezeTest(unittest.TestCase):
    def setUp(self):
        self.fx = LoanFixture()

    def test_dating_withdraw_freezes_undelivered_loan(self):
        fx = self.fx
        fx.store.ingest(fx.apply(amount=5.0))
        fx.store.ingest(fx.approve())
        withdraw = fx.a.emit("dating.withdraw",
                             {"dating_id": "D1", "reason": "送检样本污染"},
                             "2026-05-20T08:00")
        self.assertEqual(fx.store.ingest(withdraw)["status"], "applied")
        loan = fx.loan("L1")
        self.assertTrue(loan["frozen"])
        self.assertIn("D1", loan["freeze_reasons"][0])
        # 冻结的未交付申请不得领用。
        self.assertEqual(fx.store.ingest(fx.pickup())["status"], "quarantined")
        # 冻结后可驳回终结并释放锁定。
        decline = fx.a.emit("loan.decline", {
            "loan_id": "L1", "approver": "库管员甲",
            "reason": "依据测年撤回，终止外借"}, "2026-05-21T08:00")
        self.assertEqual(fx.store.ingest(decline)["status"], "applied")
        self.assertEqual(fx.sample()["locked_amount"], 0.0)

    def test_claim_withdraw_freezes_pending_application(self):
        fx = self.fx
        fx.store.ingest(fx.apply())  # 仍 pending
        fx.store.ingest(fx.a.emit("claim.withdraw", {
            "claim_id": "C-loan", "reason": "研究方向调整"},
            "2026-05-18T08:00"))
        loan = fx.loan("L1")
        self.assertTrue(loan["frozen"])
        self.assertEqual(fx.store.ingest(fx.approve())["status"], "quarantined")

    def test_delivered_loan_not_frozen_by_later_withdrawal(self):
        fx = self.fx
        fx.store.ingest(fx.apply(amount=2.0))
        fx.store.ingest(fx.approve())
        fx.store.ingest(fx.pickup())
        fx.store.ingest(fx.a.emit("dating.withdraw",
                                  {"dating_id": "D1", "reason": "污染"},
                                  "2026-05-20T08:00"))
        loan = fx.loan("L1", as_of=fw.parse_occurred_at("2026-09-01T08:00"))
        self.assertFalse(loan["frozen"])
        self.assertEqual(loan["status"], "on_loan")
        self.assertTrue(loan["overdue"])

    def test_application_after_withdrawal_is_quarantined(self):
        fx = self.fx
        fx.store.ingest(fx.a.emit("dating.withdraw",
                                  {"dating_id": "D1", "reason": "污染"},
                                  "2026-05-09T08:00"))
        # 撤回在先：新申请锚定的主张证据失效，直接隔离。
        self.assertEqual(fx.store.ingest(fx.apply())["status"], "quarantined")
        self.assertEqual(list(fx.store.projection.loans.keys()), [])


class LoanChainIsolationTest(unittest.TestCase):
    def test_broken_loan_events_never_lock_balance(self):
        dev = Device("loanlab")
        e1 = dev.emit("record.register", {
            "record_id": "S9", "kind": "测年样本", "label": "隔离炭样",
            "properties": {"initial_amount": 4.0, "amount_unit": "g"}},
            "2026-04-01T08:00")
        e2 = dev.emit("claim.submit", {
            "claim_id": "C9", "subject": "x", "proposition": "y",
            "confidence": 0.9, "evidence": ["S9"], "author": "作者"},
            "2026-04-02T08:00")
        # 第三事件起断链：申请与后续全部隔离，余量不得被锁。
        e3 = dev.raw(3, "loan.apply", {
            "loan_id": "L9", "sample_id": "S9", "lab_party": "L",
            "applicant": "作者", "amount": 4.0, "purpose": "p",
            "due_at": "2026-09-01T08:00", "claim_id": "C9",
            "storage_condition": "干燥"}, "2026-04-03T08:00", prev_hash="dead")
        store = FieldworkStore()
        store.ingest_batch([e1, e2, e3])
        self.assertEqual(store.status_of("loanlab-3")["status"], "quarantined")
        self.assertNotIn("L9", store.projection.loans)
        sample = next(s for s in fw.build_views(store)["samples"]
                      if s["sample_id"] == "S9")
        self.assertEqual(sample["locked_amount"], 0.0)
        self.assertEqual(sample["available_amount"], 4.0)

    def test_waiting_chain_does_not_lock_then_self_heals(self):
        dev = Device("loanlab2")
        e1 = dev.emit("record.register", {
            "record_id": "S8", "kind": "测年样本", "label": "缺环炭样",
            "properties": {"initial_amount": 4.0, "amount_unit": "g"}},
            "2026-04-01T08:00")
        e2 = dev.emit("claim.submit", {
            "claim_id": "C8", "subject": "x", "proposition": "y",
            "confidence": 0.9, "evidence": ["S8"], "author": "作者"},
            "2026-04-02T08:00")
        e3 = dev.emit("loan.apply", {
            "loan_id": "L8", "sample_id": "S8", "lab_party": "L",
            "applicant": "作者", "amount": 4.0, "purpose": "p",
            "due_at": "2026-09-01T08:00", "claim_id": "C8",
            "storage_condition": "干燥"}, "2026-04-03T08:00")
        store = FieldworkStore()
        store.ingest(e1)
        store.ingest(e3)  # e2 缺环：等待，不投影、不锁定。
        self.assertEqual(store.status_of("loanlab2-3")["status"], "waiting")
        self.assertNotIn("L8", store.projection.loans)
        store.ingest(e2)  # 缺环归队，自愈。
        self.assertEqual(store.status_of("loanlab2-3")["status"], "applied")

    def test_offline_duplicate_sample_registration_freezes_loan(self):
        # 两支离线队伍用同一 record_id 注册样本；后到者语义隔离，
        # 引用该编号的未交付申请冻结（断链隔离的上下文冻结）。
        a = Device("site-A")
        b = Device("site-B")
        reg_a = a.emit("record.register", {
            "record_id": "SD", "kind": "测年样本", "label": "共享编号",
            "properties": {"initial_amount": 6.0, "amount_unit": "g"}},
            "2026-04-01T08:00")
        claim = a.emit("claim.submit", {
            "claim_id": "CD", "subject": "x", "proposition": "y",
            "confidence": 0.9, "evidence": ["SD"], "author": "作者"},
            "2026-04-02T08:00")
        apply_ev = a.emit("loan.apply", {
            "loan_id": "LD", "sample_id": "SD", "lab_party": "L",
            "applicant": "作者", "amount": 3.0, "purpose": "p",
            "due_at": "2026-09-01T08:00", "claim_id": "CD",
            "storage_condition": "干燥"}, "2026-04-03T08:00")
        approve_ev = a.emit("loan.approve", {
            "loan_id": "LD", "approver": "管甲",
            "custodian_party": "主库房"}, "2026-04-04T08:00")
        dup = b.emit("record.register", {
            "record_id": "SD", "kind": "测年样本", "label": "离线重复编号",
            "properties": {"initial_amount": 99.0, "amount_unit": "g"}},
            "2026-04-05T08:00")
        store = FieldworkStore()
        for ev in (reg_a, claim, apply_ev, approve_ev):
            self.assertEqual(store.ingest(ev)["status"], "applied")
        self.assertEqual(store.ingest(dup)["status"], "quarantined")
        loan = next(l for l in fw.build_views(store)["loans"]
                    if l["loan_id"] == "LD")
        self.assertTrue(loan["frozen"])
        # 冻结原因进入谱系，隔离事件保留在冲突清单。
        self.assertTrue(any("离线重复注册" in r for r in loan["freeze_reasons"]))
        conflicts = fw.build_views(store)["conflicts"]["quarantined_events"]
        self.assertIn("site-B-1", {c["event_id"] for c in conflicts})


class LoanIdempotencyTest(unittest.TestCase):
    def test_offline_duplicate_requests_do_not_double_lock_or_spend(self):
        fx = LoanFixture()
        apply_ev = fx.apply(amount=5.0)
        approve_ev = fx.approve()
        pickup_ev = fx.pickup()
        for ev in (apply_ev, approve_ev, pickup_ev):
            self.assertEqual(fx.store.ingest(ev)["status"], "applied")
        # 离线重放同一批请求：全部 duplicate，不二次锁定/扣减。
        for ev in (apply_ev, approve_ev, pickup_ev):
            self.assertEqual(fx.store.ingest(dict(ev))["status"], "duplicate")
        s = fx.sample()
        self.assertEqual(s["on_loan_amount"], 5.0)
        self.assertEqual(s["locked_amount"], 0.0)
        # 消耗确认重复提交同样不二次扣减。
        consume = fx.a.emit("loan.consume", {"loan_id": "L1", "actor": "乙",
                                             "amount": 5.0, "note": "消耗"},
                            "2026-06-01T08:00")
        self.assertEqual(fx.store.ingest(consume)["status"], "applied")
        self.assertEqual(fx.store.ingest(dict(consume))["status"], "duplicate")
        self.assertEqual(fx.sample()["loan_consumed_amount"], 5.0)
        self.assertEqual(fx.sample()["balance"], 2.0)


class LoanAsOfTest(unittest.TestCase):
    def test_quantities_at_historical_points(self):
        fx = LoanFixture()
        fx.store.ingest(fx.apply(amount=5.0))                 # 05-12
        fx.store.ingest(fx.approve())                        # 05-13
        fx.store.ingest(fx.pickup(at="2026-05-15T08:00"))   # 05-15 领用
        fx.store.ingest(fx.a.emit("loan.return", {
            "loan_id": "L1", "actor": "乙", "amount": 3.0,
            "condition": "好"}, "2026-07-10T08:00"))
        # 消耗确认晚于归还期限：使 8 月初仍处逾期未结状态。
        fx.store.ingest(fx.a.emit("loan.consume", {
            "loan_id": "L1", "actor": "乙", "amount": 2.0,
            "note": "消耗"}, "2026-08-15T08:00"))

        def at(date):
            views = fw.state_as_of(fx.store,
                                   datetime.fromisoformat(date))
            s = next(x for x in views["samples"] if x["sample_id"] == "S1")
            loans = {l["loan_id"]: l for l in views["loans"]}
            return s, loans.get("L1")

        # 申请前：7g 全部可再分配。
        s, _ = at("2026-05-11T00:00")
        self.assertEqual((s["locked_amount"], s["on_loan_amount"],
                          s["available_amount"]), (0.0, 0.0, 7.0))
        # 批准后未领用：锁定 5g，可再分配 2g。
        s, loan = at("2026-05-14T00:00")
        self.assertEqual((s["locked_amount"], s["on_loan_amount"],
                          s["available_amount"]), (5.0, 0.0, 2.0))
        self.assertEqual(loan["status"], "approved")
        # 领用后、到期前：在借 5g，未逾期。
        s, loan = at("2026-06-01T00:00")
        self.assertEqual((s["locked_amount"], s["on_loan_amount"],
                          s["overdue_amount"], s["available_amount"]),
                         (0.0, 5.0, 0.0, 2.0))
        self.assertFalse(loan["overdue"])
        # 部分归还后（7月，尚未到期）：在外 2g，归还 3g 恢复可再分配（=5g）。
        s, loan = at("2026-07-10T20:00")
        self.assertEqual(s["on_loan_amount"], 2.0)
        self.assertEqual(s["overdue_amount"], 0.0)
        self.assertFalse(loan["overdue"])
        self.assertEqual(s["available_amount"], 5.0)
        # 到期后：剩余在外 2g 逾期（3g 已归还）。
        s, loan = at("2026-08-02T00:00")
        self.assertEqual(s["on_loan_amount"], 2.0)
        self.assertEqual(s["overdue_amount"], 2.0)
        self.assertTrue(loan["overdue"])
        # 消耗确认后结清：无在借、无逾期，外借消耗 2g 入账。
        s, loan = at("2026-09-01T00:00")
        self.assertEqual(loan["status"], "consumed")
        self.assertEqual((s["on_loan_amount"], s["overdue_amount"],
                          s["loan_consumed_amount"], s["available_amount"],
                          s["balance"]),
                         (0.0, 0.0, 2.0, 5.0, 5.0))


class LoanReconciliationTest(unittest.TestCase):
    def test_book_balance_matches_custody_lineage(self):
        """账面勾稽恒等式：初始量 = 现场消耗 + 外借消耗 + 在借 + 锁定 + 可再分配。

        每笔在借/已结申请还须满足：批准量 = 归还 + 消耗 + 未结清。
        """
        fx = LoanFixture()
        fx.store.ingest(fx.apply(loan_id="L1", amount=4.0,
                                 at="2026-05-12T08:00"))
        fx.store.ingest(fx.apply(loan_id="L2", amount=2.0, lab="乙实验室",
                                 applicant="乙", due="2026-09-01T08:00",
                                 at="2026-05-12T09:00"))
        fx.store.ingest(fx.approve(loan_id="L1"))
        fx.store.ingest(fx.approve(loan_id="L2", at="2026-05-13T09:00"))
        fx.store.ingest(fx.pickup(loan_id="L1"))
        fx.store.ingest(fx.a.emit("loan.return", {
            "loan_id": "L1", "actor": "乙", "amount": 1.0,
            "condition": "好"}, "2026-06-01T08:00"))
        fx.store.ingest(fx.a.emit("loan.consume", {
            "loan_id": "L1", "actor": "乙", "amount": 3.0,
            "note": "用完"}, "2026-06-02T08:00"))
        s = fx.sample()
        # 恒等式（初始 10g，发掘场景已现场消耗 3g）。
        self.assertEqual(
            s["initial_amount"],
            round(s["direct_consumed_amount"] + s["loan_consumed_amount"]
                  + s["on_loan_amount"] + s["locked_amount"]
                  + s["available_amount"], 9))
        # L1：4 = 归还1 + 消耗3；L2 已批准未领用，锁定 2g。
        l1 = fx.loan("L1")
        self.assertEqual(l1["amount"],
                         l1["returned_amount"] + l1["consumed_amount"]
                         + l1["outstanding_amount"])
        self.assertEqual(fx.loan("L2")["status"], "approved")
        self.assertEqual(s["locked_amount"], 2.0)
        # 谱系动作连续，无跳步。
        self.assertEqual([e["action"] for e in l1["ledger"]],
                         ["apply", "approve", "pickup", "return", "consume"])


class LoanPersistenceTest(unittest.TestCase):
    def test_restart_restores_locks_lineage_and_recall(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "loans.jsonl")
            first = FieldworkStore(journal_path=path)
            fx = LoanFixture()
            first.ingest_batch(fx.a.events)  # 含主张与发掘基线
            first.ingest(fx.apply(amount=5.0))
            first.ingest(fx.approve())
            first.ingest(fx.pickup())
            first.ingest(fx.a.emit("loan.recall", {
                "loan_id": "L1", "actor": "库管员甲",
                "reason": "逾期追索"}, "2026-09-02T08:00"))
            # 另一笔仍处于已批准未领用：锁定须跨重启还原。
            first.ingest(fx.apply(loan_id="L2", amount=1.0, lab="乙实验室",
                                  applicant="乙", due="2026-10-01T08:00",
                                  at="2026-05-12T09:00"))
            first.ingest(fx.approve(loan_id="L2", at="2026-05-13T09:00"))

            second = FieldworkStore()
            second.load_jsonl(path)
            at = fw.parse_occurred_at("2026-09-15T08:00")
            self.assertEqual(fw.canonical_json(fw.build_views(first, as_of=at)),
                             fw.canonical_json(fw.build_views(second, as_of=at)))
            v = fw.build_views(second, as_of=at)
            l1 = next(l for l in v["loans"] if l["loan_id"] == "L1")
            l2 = next(l for l in v["loans"] if l["loan_id"] == "L2")
            self.assertEqual(l1["status"], "on_loan")
            self.assertTrue(l1["overdue"])
            self.assertEqual(len(l1["recalls"]), 1)
            self.assertEqual(l2["status"], "approved")
            s = next(x for x in v["samples"] if x["sample_id"] == "S1")
            self.assertEqual((s["on_loan_amount"], s["locked_amount"],
                              s["overdue_amount"]), (5.0, 1.0, 5.0))


class LoanHttpApiTest(unittest.TestCase):
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
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8") \
            if payload is not None else None
        request = Request(self.base + path, data=data, method=method,
                          headers={"Content-Type": "application/json"})
        with urlopen(request, timeout=3) as response:
            return response.status, json.load(response)

    def _get(self, path):
        with urlopen(self.base + path, timeout=3) as response:
            return json.load(response)

    def test_loan_workflow_and_queries_over_http(self):
        fx = LoanFixture()
        status, _ = self._request("POST", "/events",
                                  {"events": fx.a.events})
        self.assertEqual(status, 200)
        for ev in (fx.apply(amount=5.0), fx.approve(), fx.pickup()):
            code, body = self._request("POST", "/events", ev)
            self.assertEqual(code, 200, body)

        # /samples 含借用账面字段。
        s1 = next(s for s in self._get("/samples")["samples"]
                  if s["sample_id"] == "S1")
        self.assertEqual(s1["on_loan_amount"], 5.0)
        self.assertEqual(s1["available_amount"], 2.0)

        # /loans 支持样本过滤；指定历史日期看逾期。
        loans = self._get("/loans?sample_id=S1&as_of=2026-09-01T00:00:00")
        self.assertEqual(loans["count"], 1)
        loan = loans["loans"][0]
        self.assertTrue(loan["overdue"])
        self.assertEqual([e["action"] for e in loan["timeline"]][-1], "overdue")
        # 到期前不逾期。
        early = self._get("/loans?sample_id=S1&as_of=2026-06-01T00:00:00")
        self.assertFalse(early["loans"][0]["overdue"])

        # /asof 同时还原借用与样本占用。
        past = self._get("/asof?date=2026-05-14T00:00:00")
        spast = next(s for s in past["samples"] if s["sample_id"] == "S1")
        self.assertEqual(spast["locked_amount"], 5.0)
        self.assertEqual(spast["available_amount"], 2.0)
        self.assertEqual(past["loans"][0]["status"], "approved")


if __name__ == "__main__":
    unittest.main()
