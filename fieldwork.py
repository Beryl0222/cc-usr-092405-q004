"""考古现场关系管理：事件溯源领域核心。

设计原则
--------
* 只追加（append-only）：所有事实均为事件，物理日志不修改、不删除。
* 每台采集设备一条哈希链：事件携带 ``prev_hash``，离线归队时按设备验链，
  缺环等待补齐，断链隔离。
* 规范化合并顺序：``(lamport, device_id, event_id)``。与到达批次无关，
  任意重放得到字节级一致的投影。
* 幂等：``event_id`` 重复提交不产生二次效果；同一 event_id 载荷冲突则隔离。
* 校正即新版本：记录属性的每次更正形成新版本，原值永久保留。
* 竞争性观点并存：观点、证据、置信度、同行评议分开建模；发布动作不可变，
  从而可以还原任一发布日期当时成立的结论。
"""

from __future__ import annotations

import hashlib
import json
import threading
from collections import defaultdict
from datetime import datetime

# ---------------------------------------------------------------------------
# 词表与常量
# ---------------------------------------------------------------------------

RECORD_KINDS = (
    "探方",
    "地层",
    "遗迹",
    "墓葬",
    "兆沟",
    "祭祀坑",
    "陪葬墓",
    "器物",
    "器物组合",
    "铭文",
    "测年样本",
    "照片",
)

# 空间/层位关系。below 是 above 的反向说法，摄入时归一化。
RELATIONS = ("contains", "above", "cuts", "depicts", "same_as")
RELATION_ALIASES = {"below": "above", "包含": "contains", "叠压": "above",
                    "打破": "cuts", "拍摄": "depicts", "同一": "same_as"}
RELATION_LABELS = {
    "contains": "包含",
    "above": "层位在上（较晚）",
    "cuts": "打破",
    "depicts": "拍摄/描绘",
    "same_as": "同一对象",
}

CONTENT_EVENTS = {
    "record.register",
    "record.correct",
    "number.resolve",
    "spatial.relate",
    "spatial.retract",
    "dating.register",
    "dating.withdraw",
    "sample.consume",
    "custody.checkout",
    "custody.return",
    "custody.transfer",
    "claim.submit",
    "claim.revise",
    "claim.withdraw",
    "peer.review",
    "publication.release",
}

REVIEW_VERDICTS = ("support", "challenge", "neutral")
PUBLISH_AS = ("established", "hypothesis")

ESTABLISHED_CONFIDENCE = 0.8
DEFAULT_HOLDER = "主库房"


class FieldworkError(ValueError):
    """事件结构性错误（无法进入投影，直接拒绝）。"""


class Quarantine(Exception):
    """事件语义上不能成立：隔离保留，不参与投影。"""

    def __init__(self, reasons):
        if isinstance(reasons, str):
            reasons = [reasons]
        self.reasons = list(reasons)
        super().__init__("; ".join(self.reasons))


# ---------------------------------------------------------------------------
# 确定性工具
# ---------------------------------------------------------------------------

def canonical_json(payload) -> str:
    """规范化 JSON：字节级稳定，供哈希与跨副本比较使用。"""
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def event_hash(event: dict) -> str:
    """事件内容哈希（不含服务器附加字段）。"""
    return hashlib.sha256(canonical_json(event).encode("utf-8")).hexdigest()


def canonical_key(event: dict) -> tuple:
    return (event["lamport"], event["device_id"], event["event_id"])


def parse_occurred_at(value: str) -> datetime:
    if not isinstance(value, str):
        raise FieldworkError("occurred_at 必须是 ISO8601 字符串")
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise FieldworkError(f"occurred_at 无法解析: {value!r}") from exc


def require(payload: dict, fields: dict) -> None:
    """fields: 字段名 -> 类型或类型元组。"""
    for name, kind in fields.items():
        if name not in payload:
            raise FieldworkError(f"缺少字段 {name}")
        if not isinstance(payload[name], kind):
            raise FieldworkError(f"字段 {name} 类型应为 {kind}")


# ---------------------------------------------------------------------------
# 投影
# ---------------------------------------------------------------------------

class Projection:
    """对全部有效事件重放后得到的确定性只读状态。"""

    def __init__(self):
        # record_id -> {"record":注册载荷, "versions": [(event, properties)...]}
        self.records = {}
        # 现场编号 -> 当前持有者 record_id（resolve 后为规范号）
        self.numbers = {}
        # number -> [claiming record_id ...]（当前声明，按 id 排序）
        self.number_claims = defaultdict(set)
        # number 解析历史
        self.number_resolutions = []
        # (source, relation, target) -> 最近一次建立该边的事件
        self.edges = {}
        self.retractions = []
        # dating_id -> 载荷；withdrawn_dates 记录撤回
        self.datings = {}
        self.datings_withdrawn = {}
        # 样本：sample_id -> {"initial","unit","consumed": [(event, amount)...]}
        self.samples = {}
        # item_id -> [保管事件...]
        self.custody = defaultdict(list)
        self.holders = {}
        # claim_id -> 观点聚合
        self.claims = {}
        # release_id -> 发布快照
        self.releases = {}

    # -- 记录 ---------------------------------------------------------------

    def apply_record_register(self, event):
        p = event["payload"]
        require(p, {"record_id": str, "kind": str, "label": str, "properties": dict})
        if p["kind"] not in RECORD_KINDS:
            raise Quarantine(f"未知记录类型 {p['kind']}")
        if p["record_id"] in self.records:
            raise Quarantine(f"record_id 重复注册: {p['record_id']}")
        numbers = p.get("numbers", [])
        if not isinstance(numbers, list) or not all(isinstance(n, str) for n in numbers):
            raise FieldworkError("numbers 必须是字符串列表")
        record = {
            "record_id": p["record_id"],
            "kind": p["kind"],
            "label": p["label"],
            "numbers": list(numbers),
            "versions": [
                {
                    "version": 1,
                    "properties": p["properties"],
                    "change_summary": p.get("change_summary", "首次记录"),
                    "event_id": event["event_id"],
                    "device_id": event["device_id"],
                    "occurred_at": event["occurred_at"],
                }
            ],
            "registered_event": event["event_id"],
        }
        self.records[p["record_id"]] = record
        for number in numbers:
            self.number_claims[number].add(p["record_id"])
        if p["kind"] == "测年样本":
            props = p["properties"]
            require(props, {"initial_amount": (int, float), "amount_unit": str})
            if props["initial_amount"] <= 0:
                raise Quarantine("样本初始量必须为正数")
            self.samples[p["record_id"]] = {
                "initial": props["initial_amount"],
                "unit": props["amount_unit"],
                "consumed": [],
            }
        self.holders[p["record_id"]] = p["properties"].get("holder", DEFAULT_HOLDER)

    def apply_record_correct(self, event):
        p = event["payload"]
        require(p, {"record_id": str, "properties": dict})
        record = self.records.get(p["record_id"])
        if record is None:
            raise Quarantine(f"校正指向不存在的记录: {p['record_id']}")
        if "numbers" in p["properties"]:
            raise Quarantine("现场编号只能通过 number.resolve 变更，禁止在校正里改写")
        record["versions"].append(
            {
                "version": len(record["versions"]) + 1,
                "properties": p["properties"],
                "change_summary": p.get("change_summary", ""),
                "event_id": event["event_id"],
                "device_id": event["device_id"],
                "occurred_at": event["occurred_at"],
            }
        )

    def apply_number_resolve(self, event):
        p = event["payload"]
        require(p, {"number": str, "keeper_id": str, "rename": dict})
        number, keeper = p["number"], p["keeper_id"]
        claimants = self.number_claims.get(number, set())
        if not claimants:
            raise Quarantine(f"编号 {number} 无任何记录声明，无可仲裁对象")
        if keeper not in claimants:
            raise Quarantine(f"规范保留方 {keeper} 并未声明编号 {number}")
        for other, new_number in p["rename"].items():
            if other == keeper:
                raise Quarantine("规范保留方不能同时出现在改号表中")
            if other not in claimants:
                raise Quarantine(f"被改号记录 {other} 并未声明编号 {number}")
            if not isinstance(new_number, str) or not new_number:
                raise FieldworkError("rename 的新编号必须是非空字符串")
        # 执行：keeper 继续持有原编号；其余记录改挂新编号。
        for other, new_number in p["rename"].items():
            self.number_claims[number].discard(other)
            record = self.records[other]
            record["numbers"] = [
                new_number if n == number else n for n in record["numbers"]
            ]
            self.number_claims[new_number].add(other)
        self.number_resolutions.append(
            {
                "number": number,
                "keeper_id": keeper,
                "rename": dict(p["rename"]),
                "event_id": event["event_id"],
                "occurred_at": event["occurred_at"],
            }
        )

    # -- 空间与层位 ---------------------------------------------------------

    def apply_spatial_relate(self, event):
        p = event["payload"]
        require(p, {"source": str, "target": str, "relation": str})
        relation = RELATION_ALIASES.get(p["relation"], p["relation"])
        if relation not in RELATIONS:
            raise Quarantine(f"未知空间关系 {p['relation']}")
        # below 归一化为 above 并交换端点。
        source, target = p["source"], p["target"]
        if p["relation"] == "below":
            source, target = target, source
        for ref in (source, target):
            if ref not in self.records:
                raise Quarantine(f"空间关系指向不存在的记录: {ref}")
        if source == target:
            raise Quarantine("记录不能与自身建立空间关系")
        edge = (source, relation, target)
        self.edges[edge] = event["event_id"]

    def apply_spatial_retract(self, event):
        p = event["payload"]
        require(p, {"source": str, "target": str, "relation": str, "reason": str})
        relation = RELATION_ALIASES.get(p["relation"], p["relation"])
        edge = (p["source"], relation, p["target"])
        if edge not in self.edges:
            raise Quarantine(f"撤回的空间关系不存在: {edge}")
        del self.edges[edge]
        self.retractions.append(
            {
                "edge": list(edge),
                "reason": p["reason"],
                "event_id": event["event_id"],
                "occurred_at": event["occurred_at"],
            }
        )

    # -- 测年 ---------------------------------------------------------------

    def apply_dating_register(self, event):
        p = event["payload"]
        require(p, {"dating_id": str, "sample_id": str, "method": str, "result": str})
        if p["dating_id"] in self.datings or p["dating_id"] in self.datings_withdrawn:
            raise Quarantine(f"测年编号重复: {p['dating_id']}")
        if p["sample_id"] not in self.records:
            raise Quarantine(f"测年关联的样本/记录不存在: {p['sample_id']}")
        self.datings[p["dating_id"]] = {
            "dating_id": p["dating_id"],
            "sample_id": p["sample_id"],
            "method": p["method"],
            "result": p["result"],
            "range": p.get("range"),
            "lab_id": p.get("lab_id"),
            "event_id": event["event_id"],
            "device_id": event["device_id"],
            "occurred_at": event["occurred_at"],
        }

    def apply_dating_withdraw(self, event):
        p = event["payload"]
        require(p, {"dating_id": str, "reason": str})
        if p["dating_id"] not in self.datings:
            raise Quarantine(f"撤回的测年不存在或已撤回: {p['dating_id']}")
        dating = self.datings.pop(p["dating_id"])
        dating["withdrawn_reason"] = p["reason"]
        dating["withdrawn_event_id"] = event["event_id"]
        dating["withdrawn_at"] = event["occurred_at"]
        self.datings_withdrawn[p["dating_id"]] = dating

    # -- 样本消耗 -----------------------------------------------------------

    def apply_sample_consume(self, event):
        p = event["payload"]
        require(p, {"sample_id": str, "amount": (int, float), "purpose": str})
        sample = self.samples.get(p["sample_id"])
        if sample is None:
            raise Quarantine(f"消耗事件指向不存在的样本: {p['sample_id']}")
        if p["amount"] <= 0:
            raise Quarantine("样本消耗量必须为正数")
        consumed = sum(amount for _, amount in sample["consumed"])
        if consumed + p["amount"] > sample["initial"] + 1e-9:
            raise Quarantine(
                f"样本 {p['sample_id']} 余额不足: 现存 "
                f"{sample['initial'] - consumed}{sample['unit']}, "
                f"申请消耗 {p['amount']}{sample['unit']}"
            )
        sample["consumed"].append((event["event_id"], p["amount"]))

    # -- 保管链 -------------------------------------------------------------

    def _custody_state(self, item_id):
        """根据保管链事件重放单项状态。"""
        state = "in_storage"  # in_storage | out
        for entry in self.custody[item_id]:
            kind = entry["type"]
            if kind == "checkout":
                state = "out"
            elif kind == "return":
                state = "in_storage"
            # transfer 不改变在库/外状态，只改变持有方
        return state

    def apply_custody(self, event):
        p = event["payload"]
        kind = event["type"].split(".", 1)[1]
        require(p, {"item_id": str, "actor": str})
        item = p["item_id"]
        if item not in self.records:
            raise Quarantine(f"保管事件指向不存在的器物/样本: {item}")
        holder = self.holders.get(item, DEFAULT_HOLDER)
        state = self._custody_state(item)

        if kind == "checkout":
            require(p, {"from_party": str, "to_party": str, "purpose": str})
            if state == "out":
                raise Quarantine(f"{item} 已出库未归还，不能重复出库")
            if holder != p["from_party"]:
                raise Quarantine(
                    f"出库放行无效：{item} 当前持有方为 {holder}，"
                    f"出库单声称 {p['from_party']}"
                )
            entry = {
                "type": "checkout",
                "actor": p["actor"],
                "from_party": p["from_party"],
                "to_party": p["to_party"],
                "purpose": p["purpose"],
                "condition": p.get("condition"),
                "cross_store": bool(p.get("cross_store", False)),
                "event_id": event["event_id"],
                "occurred_at": event["occurred_at"],
            }
            self.holders[item] = p["to_party"]
        elif kind == "return":
            require(p, {"condition": str})
            if state != "out":
                raise Quarantine(f"{item} 当前不在外，无库可归")
            # 出库/移交期间的持有方可能变化；归还目标以归还单 to_store 为准，
            # 默认回到主库房。
            target_store = p.get("to_store", DEFAULT_HOLDER)
            entry = {
                "type": "return",
                "actor": p["actor"],
                "to_store": target_store,
                "condition": p["condition"],
                "event_id": event["event_id"],
                "occurred_at": event["occurred_at"],
            }
            self.holders[item] = target_store
        elif kind == "transfer":
            require(p, {"from_party": str, "to_party": str})
            if holder != p["from_party"]:
                raise Quarantine(
                    f"跨库移交无效：{item} 当前持有方为 {holder}，"
                    f"移交单声称 {p['from_party']}"
                )
            if p["to_party"] == p["from_party"]:
                raise Quarantine("移交的接收方与移交方相同")
            entry = {
                "type": "transfer",
                "actor": p["actor"],
                "from_party": p["from_party"],
                "to_party": p["to_party"],
                "cross_store": bool(p.get("cross_store", True)),
                "purpose": p.get("purpose", ""),
                "condition": p.get("condition"),
                "event_id": event["event_id"],
                "occurred_at": event["occurred_at"],
            }
            self.holders[item] = p["to_party"]
        else:  # pragma: no cover - 分派表已约束
            raise FieldworkError(f"未知保管事件 {kind}")
        self.custody[item].append(entry)

    # -- 观点与评议 ---------------------------------------------------------

    def apply_claim_submit(self, event):
        p = event["payload"]
        require(p, {
            "claim_id": str,
            "subject": str,
            "proposition": str,
            "confidence": (int, float),
            "evidence": list,
            "author": str,
        })
        if not 0 <= p["confidence"] <= 1:
            raise FieldworkError("confidence 必须落在 [0,1]")
        if not p["evidence"] or not all(isinstance(x, str) for x in p["evidence"]):
            raise Quarantine("观点必须引用至少一条具体证据")
        for ref in p["evidence"]:
            if not self._evidence_current(ref):
                raise Quarantine(f"证据引用无法定位或已失效: {ref}")
        if p["claim_id"] in self.claims:
            raise Quarantine(f"观点编号重复: {p['claim_id']}")
        self.claims[p["claim_id"]] = self._new_claim(p, event)

    def _evidence_exists(self, ref: str) -> bool:
        return (
            ref in self.records
            or ref in self.datings
            or ref in self.datings_withdrawn
            or ref in self.releases
        )

    def _evidence_current(self, ref: str) -> bool:
        """当前可用证据：已撤回的测年不得再作为观点依据。"""
        if ref in self.datings_withdrawn:
            return False
        return self._evidence_exists(ref)

    def _evidence_available_at(self, ref: str, cutoff: datetime) -> bool:
        """证据在 cutoff 时点是否已经产生（供发布快照做时效校验）。"""
        if ref in self.records:
            return parse_occurred_at(
                self.records[ref]["versions"][0]["occurred_at"]) <= cutoff
        if ref in self.datings:
            return parse_occurred_at(self.datings[ref]["occurred_at"]) <= cutoff
        if ref in self.datings_withdrawn:
            dating = self.datings_withdrawn[ref]
            # 已撤回的测年在撤回之日起不再是可用证据。
            produced = parse_occurred_at(dating["occurred_at"]) <= cutoff
            withdrawn = parse_occurred_at(dating["withdrawn_at"]) <= cutoff
            return produced and not withdrawn
        if ref in self.releases:
            return parse_occurred_at(self.releases[ref]["date"]) <= cutoff
        return False

    @staticmethod
    def _confidence_band(value: float) -> str:
        if value >= ESTABLISHED_CONFIDENCE:
            return "high"
        if value >= 0.5:
            return "medium"
        return "low"

    def _new_claim(self, p, event):
        return {
            "claim_id": p["claim_id"],
            "subject": p["subject"],
            "author": p["author"],
            "status": "active",
            "versions": [
                {
                    "version": 1,
                    "proposition": p["proposition"],
                    "confidence": p["confidence"],
                    "confidence_band": self._confidence_band(p["confidence"]),
                    "evidence": list(p["evidence"]),
                    "change_summary": p.get("change_summary", "首次提出"),
                    "event_id": event["event_id"],
                    "occurred_at": event["occurred_at"],
                    "reviews": [],
                }
            ],
        }

    def apply_claim_revise(self, event):
        p = event["payload"]
        require(p, {
            "claim_id": str,
            "proposition": str,
            "confidence": (int, float),
            "evidence": list,
        })
        claim = self.claims.get(p["claim_id"])
        if claim is None:
            raise Quarantine(f"修订指向不存在的观点: {p['claim_id']}")
        if claim["status"] != "active":
            raise Quarantine(f"观点已 {claim['status']}，不能再修订")
        if not p["evidence"]:
            raise Quarantine("修订后的观点仍须引用证据")
        for ref in p["evidence"]:
            if not self._evidence_current(ref):
                raise Quarantine(f"证据引用无法定位或已失效: {ref}")
        claim["versions"].append(
            {
                "version": len(claim["versions"]) + 1,
                "proposition": p["proposition"],
                "confidence": p["confidence"],
                "confidence_band": self._confidence_band(p["confidence"]),
                "evidence": list(p["evidence"]),
                "change_summary": p.get("change_summary", ""),
                "event_id": event["event_id"],
                "occurred_at": event["occurred_at"],
                "reviews": [],
            }
        )

    def apply_claim_withdraw(self, event):
        p = event["payload"]
        require(p, {"claim_id": str, "reason": str})
        claim = self.claims.get(p["claim_id"])
        if claim is None:
            raise Quarantine(f"撤回指向不存在的观点: {p['claim_id']}")
        claim["status"] = "withdrawn"
        claim["withdrawn_reason"] = p["reason"]
        claim["withdrawn_event_id"] = event["event_id"]
        claim["withdrawn_at"] = event["occurred_at"]

    def apply_peer_review(self, event):
        p = event["payload"]
        require(p, {
            "claim_id": str,
            "reviewer": str,
            "verdict": str,
            "comment": str,
        })
        if p["verdict"] not in REVIEW_VERDICTS:
            raise FieldworkError(f"verdict 必须是 {REVIEW_VERDICTS} 之一")
        claim = self.claims.get(p["claim_id"])
        if claim is None:
            raise Quarantine(f"评议指向不存在的观点: {p['claim_id']}")
        if claim["status"] != "active":
            raise Quarantine(f"观点已 {claim['status']}，不再接受评议")
        if p["reviewer"] == claim["author"]:
            raise Quarantine("作者不能充当自己观点的独立评议人")
        version = p.get("version", claim["versions"][-1]["version"])
        target = next((v for v in claim["versions"] if v["version"] == version), None)
        if target is None:
            raise Quarantine(f"评议指向不存在的观点版本: {version}")
        target["reviews"].append(
            {
                "reviewer": p["reviewer"],
                "verdict": p["verdict"],
                "comment": p["comment"],
                "event_id": event["event_id"],
                "occurred_at": event["occurred_at"],
            }
        )

    # -- 发布 ---------------------------------------------------------------

    def apply_publication_release(self, event):
        p = event["payload"]
        require(p, {"release_id": str, "title": str, "date": str, "entries": list})
        cutoff = parse_occurred_at(p["date"])
        if p["release_id"] in self.releases:
            raise Quarantine(f"发布编号重复: {p['release_id']}")
        if not p["entries"]:
            raise Quarantine("发布至少要包含一条观点")
        entries = []
        problems = []
        for raw in p["entries"]:
            require(raw, {"claim_id": str, "as": str})
            if raw["as"] not in PUBLISH_AS:
                raise FieldworkError(f"as 必须是 {PUBLISH_AS} 之一")
            claim = self.claims.get(raw["claim_id"])
            if claim is None:
                problems.append(f"{raw['claim_id']}: 观点在规范化日志中尚不存在")
                continue
            # 快照锚定发布日期当时成立的版本与评议：后期修订不得回填进早先发布。
            versions_as_of = [
                v for v in claim["versions"]
                if parse_occurred_at(v["occurred_at"]) <= cutoff
            ]
            if not versions_as_of:
                problems.append(f"{raw['claim_id']}: 发布日期早于该观点的提出时间")
                continue
            if (claim["status"] == "withdrawn"
                    and parse_occurred_at(claim["withdrawn_at"]) <= cutoff):
                problems.append(f"{raw['claim_id']}: 该观点在发布日期前已撤回")
                continue
            version = versions_as_of[-1]
            reviews_as_of = [
                r for r in version["reviews"]
                if parse_occurred_at(r["occurred_at"]) <= cutoff
            ]
            stale_evidence = [
                ref for ref in version["evidence"]
                if not self._evidence_available_at(ref, cutoff)
            ]
            if stale_evidence:
                problems.append(
                    f"{raw['claim_id']}: 证据在发布日期尚不存在或已撤回: "
                    + ", ".join(stale_evidence)
                )
            if raw["as"] == "established":
                if version["confidence"] < ESTABLISHED_CONFIDENCE:
                    problems.append(
                        f"{raw['claim_id']}: 置信度 {version['confidence']} 不足，"
                        "不能作为既定事实发布"
                    )
                if not any(r["verdict"] == "support" for r in reviews_as_of):
                    problems.append(f"{raw['claim_id']}: 缺少发布日期前的独立同行支持评议")
                if any(r["verdict"] == "challenge" for r in reviews_as_of):
                    problems.append(f"{raw['claim_id']}: 存在发布日期前未消解的质疑评议")
            entries.append(
                {
                    "claim_id": raw["claim_id"],
                    "version": version["version"],
                    "subject": claim["subject"],
                    "proposition": version["proposition"],
                    "confidence": version["confidence"],
                    "confidence_band": version["confidence_band"],
                    "evidence": list(version["evidence"]),
                    "author": claim["author"],
                    "as": raw["as"],
                    "release_date": p["date"],
                }
            )
        if problems:
            raise Quarantine([f"发布未通过闸门: {p['release_id']}"] + problems)
        self.releases[p["release_id"]] = {
            "release_id": p["release_id"],
            "title": p["title"],
            "date": p["date"],
            "entries": entries,
            "event_id": event["event_id"],
        }


APPLY = {
    "record.register": Projection.apply_record_register,
    "record.correct": Projection.apply_record_correct,
    "number.resolve": Projection.apply_number_resolve,
    "spatial.relate": Projection.apply_spatial_relate,
    "spatial.retract": Projection.apply_spatial_retract,
    "dating.register": Projection.apply_dating_register,
    "dating.withdraw": Projection.apply_dating_withdraw,
    "sample.consume": Projection.apply_sample_consume,
    "custody.checkout": Projection.apply_custody,
    "custody.return": Projection.apply_custody,
    "custody.transfer": Projection.apply_custody,
    "claim.submit": Projection.apply_claim_submit,
    "claim.revise": Projection.apply_claim_revise,
    "claim.withdraw": Projection.apply_claim_withdraw,
    "peer.review": Projection.apply_peer_review,
    "publication.release": Projection.apply_publication_release,
}


# ---------------------------------------------------------------------------
# 确定性重放
# ---------------------------------------------------------------------------

def _chain_gates(log) -> tuple[set, set]:
    """按设备校验哈希链，返回 (waiting, broken) 事件 id 集合。

    * waiting：引用的祖先事件尚未归队（缺环），待补齐后自愈。
    * broken：祖先已在日志却衔接不上、同设备时钟倒退等，事件及后继隔离。
    """
    by_device = defaultdict(list)
    for event in log:
        by_device[event["device_id"]].append(event)
    known_hashes = {event_hash(event) for event in log}
    waiting, broken = set(), set()

    for device in sorted(by_device):
        ordered = sorted(by_device[device], key=lambda e: (e["lamport"], e["event_id"]))
        head = None
        stall_mode = None
        for index, event in enumerate(ordered):
            eid = event["event_id"]
            if stall_mode is not None:
                (waiting if stall_mode == "waiting" else broken).add(eid)
                continue
            if index > 0 and event["lamport"] <= ordered[index - 1]["lamport"]:
                broken.add(eid)
                stall_mode = "broken"
                continue
            actual = event.get("prev_hash")
            if actual != head:
                malformed = actual is not None and not (
                    isinstance(actual, str) and len(actual) == 64
                    and all(c in "0123456789abcdef" for c in actual))
                if actual is None or malformed or actual in known_hashes:
                    # 伪造哈希、链重置，或祖先已在日志却衔接不上：断裂。
                    broken.add(eid)
                    stall_mode = "broken"
                else:
                    # 引用的祖先事件尚未归队：整链等待，不污染投影。
                    waiting.add(eid)
                    stall_mode = "waiting"
                continue
            head = event_hash(event)
    waiting -= broken
    return waiting, broken


def replay(log, check_chains: bool) -> tuple[dict, dict, Projection, set, set]:
    """对给定日志做确定性重放。

    check_chains=False 用于时点还原：日志子集是已通过整链校验的事件，
    子集内允许链不连续（按 occurred_at 过滤会跳过事件），但前向引用
    无法闭合的事件仍会被隔离。
    """
    projection = Projection()
    status, quarantine = {}, {}
    waiting, broken = _chain_gates(log) if check_chains else (set(), set())

    ordered_all = sorted(log, key=canonical_key)
    applied = set()
    for _ in range(len(ordered_all) + 1):
        progressed = False
        for event in ordered_all:
            eid = event["event_id"]
            if eid in applied or eid in waiting or eid in broken:
                continue
            try:
                APPLY[event["type"]](projection, event)
            except Quarantine as exc:
                reasons = exc.reasons
            except FieldworkError as exc:
                reasons = [str(exc)]
            else:
                status[eid] = {"status": "applied", "reasons": []}
                applied.add(eid)
                progressed = True
                continue
            status[eid] = {"status": "quarantined", "reasons": reasons}
            quarantine[eid] = {"event": event, "reasons": reasons}
            applied.add(eid)
            progressed = True
        if not progressed:
            break

    for event in ordered_all:
        eid = event["event_id"]
        if eid in status:
            continue
        if eid in waiting:
            status[eid] = {"status": "waiting",
                           "reasons": ["设备日志缺环，等待祖先事件归队"]}
        else:
            status[eid] = {"status": "quarantined",
                           "reasons": ["重放结束时引用仍无法闭合"]}
            quarantine.setdefault(eid, {"event": event,
                                        "reasons": ["引用无法闭合（可能指向缺失事件）"]})
    return status, quarantine, projection, waiting, broken


# ---------------------------------------------------------------------------
# 存储：设备哈希链 + 规范化合并
# ---------------------------------------------------------------------------


class FieldworkStore:
    def __init__(self, journal_path: str | None = None):
        self._lock = threading.RLock()
        # 物理到达顺序的事件（持久化顺序）
        self._log = []
        self._ingested_at = {}
        # 每次摄入决策（重建时同样得到）
        self._status = {}
        self._quarantine = {}
        self._projection = Projection()
        self.journal_path = journal_path

    # -- 摄入 ---------------------------------------------------------------

    def ingest(self, event: dict) -> dict:
        """摄入单个事件，返回 {"status": ..., "reasons": [...]}。"""
        with self._lock:
            return self._ingest_locked(event)

    def ingest_batch(self, events) -> list:
        with self._lock:
            return [self._ingest_locked(e) for e in events]

    def _ingest_locked(self, event):
        try:
            self._validate_shape(event)
        except FieldworkError as exc:
            # 结构性错误不进日志（调用方应立即修正）。
            return {"status": "rejected", "reasons": [str(exc)],
                    "event_id": event.get("event_id") if isinstance(event, dict) else None}

        eid = event["event_id"]
        for existing in self._log:
            if existing["event_id"] == eid:
                if canonical_json(existing) == canonical_json(event):
                    return {"status": "duplicate", "reasons": [], "event_id": eid}
                return {"status": "rejected",
                        "reasons": [f"event_id {eid} 已有不同载荷，禁止覆盖"],
                        "event_id": eid}

        ingested_at = datetime.now().astimezone().isoformat()
        if self.journal_path:
            self._append_journal(event, ingested_at)
        self._log.append(event)
        self._ingested_at[eid] = ingested_at
        self._rebuild()
        result = self._status.get(eid, {"status": "applied", "reasons": []})
        return {"status": result["status"], "reasons": result.get("reasons", []),
                "event_id": eid, "hash": event_hash(event)}

    def _append_journal(self, event, ingested_at):
        with open(self.journal_path, "a", encoding="utf-8") as handle:
            handle.write(canonical_json(
                {"event": event, "ingested_at": ingested_at}) + "\n")

    @staticmethod
    def _validate_shape(event):
        require(event, {
            "event_id": str,
            "device_id": str,
            "lamport": int,
            "type": str,
            "payload": dict,
            "occurred_at": str,
        })
        if not event["event_id"] or not event["device_id"]:
            raise FieldworkError("event_id / device_id 不能为空")
        if event["lamport"] < 0:
            raise FieldworkError("lamport 不能为负")
        if event["type"] not in CONTENT_EVENTS:
            raise FieldworkError(f"未知事件类型 {event['type']}")
        parse_occurred_at(event["occurred_at"])
        prev = event.get("prev_hash")
        if prev is not None and not isinstance(prev, str):
            raise FieldworkError("prev_hash 必须是字符串或 null")

    # -- 重建（确定性） ------------------------------------------------------

    def _rebuild(self):
        status, quarantine, projection, self._waiting, self._broken = replay(
            self._log, check_chains=True)
        self._status = status
        self._quarantine = quarantine
        self._projection = projection

    # -- 查询 ---------------------------------------------------------------

    @property
    def projection(self) -> Projection:
        with self._lock:
            return self._projection

    def status_of(self, event_id):
        return self._status.get(event_id)

    def verify_chains(self) -> dict:
        """返回每台设备的链校验结果。"""
        with self._lock:
            result = {}
            by_device = defaultdict(list)
            for event in self._log:
                by_device[event["device_id"]].append(event)
            for device in sorted(by_device):
                ordered = sorted(by_device[device], key=lambda e: (e["lamport"], e["event_id"]))
                head = None
                ok = True
                bad_at = None
                for event in ordered:
                    actual = event.get("prev_hash")
                    if actual != head:
                        ok = False
                        bad_at = event["event_id"]
                        break
                    head = event_hash(event)
                result[device] = {
                    "events": len(ordered),
                    "ok": ok,
                    "broken_at": bad_at,
                    "tip": head,
                }
            return result

    # -- 持久化 -------------------------------------------------------------

    def load_jsonl(self, path: str) -> dict:
        loaded = 0
        with open(path, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                envelope = json.loads(line)
                event = envelope["event"]
                self._log.append(event)
                self._ingested_at[event["event_id"]] = envelope.get("ingested_at")
                loaded += 1
        if self._log:
            self._rebuild()
        return {"loaded": loaded}

    def physical_log(self):
        return list(self._log)


# ---------------------------------------------------------------------------
# 只读视图（全部确定性排序）
# ---------------------------------------------------------------------------

def _current(claim_or_record):
    return claim_or_record["versions"][-1]


def build_views(store: FieldworkStore) -> dict:
    p = store.projection
    conflicts = _collect_conflicts(p, store)

    records = []
    for rid in sorted(p.records):
        record = p.records[rid]
        current = record["versions"][-1]
        records.append({
            "record_id": rid,
            "kind": record["kind"],
            "label": record["label"],
            "numbers": sorted(record["numbers"]),
            "version": current["version"],
            "properties": current["properties"],
            "version_count": len(record["versions"]),
        })

    relations = [
        {"source": s, "relation": rel, "relation_label": RELATION_LABELS[rel],
         "target": t, "event_id": eid}
        for (s, rel, t), eid in sorted(p.edges.items())
    ]

    samples = []
    for sid in sorted(p.samples):
        sample = p.samples[sid]
        consumed = sum(amount for _, amount in sample["consumed"])
        samples.append({
            "sample_id": sid,
            "initial_amount": sample["initial"],
            "consumed_amount": round(consumed, 9),
            "balance": round(sample["initial"] - consumed, 9),
            "unit": sample["unit"],
            "consume_events": [eid for eid, _ in sorted(sample["consumed"])],
            "holder": p.holders.get(sid, DEFAULT_HOLDER),
        })

    custody = {
        item: {"holder": p.holders.get(item, DEFAULT_HOLDER), "chain": chain}
        for item, chain in sorted(p.custody.items())
    }

    claims = []
    for cid in sorted(p.claims):
        claim = p.claims[cid]
        cur = claim["versions"][-1]
        claims.append({
            "claim_id": cid,
            "subject": claim["subject"],
            "author": claim["author"],
            "status": claim["status"],
            "version": cur["version"],
            "proposition": cur["proposition"],
            "confidence": cur["confidence"],
            "confidence_band": cur["confidence_band"],
            "evidence": list(cur["evidence"]),
            "reviews": list(cur["reviews"]),
            "version_count": len(claim["versions"]),
        })

    releases = [p.releases[rid] for rid in sorted(p.releases)]

    datings = [p.datings[did] for did in sorted(p.datings)]
    withdrawn = [p.datings_withdrawn[did] for did in sorted(p.datings_withdrawn)]

    return {
        "records": records,
        "relations": relations,
        "samples": samples,
        "custody": custody,
        "claims": claims,
        "releases": releases,
        "datings": datings,
        "datings_withdrawn": withdrawn,
        "conflicts": conflicts,
    }


def _collect_conflicts(p: Projection, store: FieldworkStore) -> dict:
    numbering = []
    for number in sorted(p.number_claims):
        claimants = sorted(p.number_claims[number])
        if len(claimants) > 1:
            numbering.append({"number": number, "claimants": claimants,
                              "status": "unresolved"})
    for resolution in p.number_resolutions:
        numbering.append({"number": resolution["number"],
                          "claimants": sorted(p.number_claims[resolution["number"]]),
                          "status": "resolved",
                          "keeper_id": resolution["keeper_id"],
                          "event_id": resolution["event_id"]})

    # 层位矛盾：双向 above，以及 above 图中的环。
    edge_set = set(p.edges)
    contradictions = []
    for (s, rel, t) in sorted(edge_set):
        if rel == "above" and (t, "above", s) in edge_set:
            pair = sorted([s, t])
            contradictions.append({"type": "mutual_above", "records": pair})
    contradictions = _dedup_dicts(contradictions)
    cycles = _cycles({(s, t) for (s, rel, t) in edge_set if rel == "above"})
    for cycle in cycles:
        contradictions.append({"type": "stratigraphic_cycle", "records": cycle})

    quarantined = [
        {"event_id": eid, "event": info["event"], "reasons": info["reasons"]}
        for eid, info in sorted(store._quarantine.items())
    ]
    waiting = [
        {"event_id": eid, "reasons": meta["reasons"]}
        for eid, meta in sorted(store._status.items())
        if meta["status"] == "waiting"
    ]

    # 竞争性观点：同一 subject 下并存多种主张。
    by_subject = defaultdict(set)
    for claim in p.claims.values():
        if claim["status"] == "active":
            by_subject[claim["subject"]].add(claim["claim_id"])
    competing = [
        {"subject": subject, "claim_ids": sorted(ids)}
        for subject, ids in sorted(by_subject.items()) if len(ids) > 1
    ]

    return {
        "numbering": numbering,
        "spatial": contradictions,
        "quarantined_events": quarantined,
        "waiting_events": waiting,
        "competing_claims": competing,
        "withdrawn_datings": [d["dating_id"] for d in p.datings_withdrawn.values()],
    }


def _dedup_dicts(rows):
    seen = set()
    out = []
    for row in rows:
        key = canonical_json(row)
        if key not in seen:
            seen.add(key)
            out.append(row)
    return out


def _cycles(edges: set) -> list:
    """返回 above 图中的简单环（确定性输出）。"""
    graph = defaultdict(set)
    for s, t in edges:
        graph[s].add(t)
    cycles = []
    seen_cycles = set()

    def dfs(node, stack, visiting):
        for nxt in sorted(graph[node]):
            if nxt in visiting:
                idx = stack.index(nxt)
                cycle = stack[idx:]
                key = tuple(sorted(cycle))
                if key not in seen_cycles:
                    seen_cycles.add(key)
                    cycles.append(sorted(cycle))
            else:
                visiting.add(nxt)
                stack.append(nxt)
                dfs(nxt, stack, visiting)
                stack.pop()
                visiting.discard(nxt)

    for node in sorted(graph):
        dfs(node, [node], {node})
    return sorted(cycles, key=canonical_json)


def spatial_closure(p: Projection) -> dict:
    """确定性传递闭包：contains 与 above 分别推导。"""
    direct = sorted((s, rel, t) for (s, rel, t) in p.edges)
    closure = set()
    for wanted in ("contains", "above"):
        graph = defaultdict(set)
        nodes = set()
        for s, rel, t in direct:
            if rel == wanted:
                graph[s].add(t)
                nodes.update((s, t))
        for start in sorted(nodes):
            stack = [(start, {start})]
            while stack:
                node, seen = stack.pop()
                for nxt in graph[node]:
                    if nxt not in seen:
                        closure.add((start, wanted, nxt))
                        stack.append((nxt, seen | {nxt}))
    inferred = sorted(closure - set(direct))
    return {
        "direct": [list(e) for e in direct],
        "inferred": [list(e) for e in inferred],
    }


def state_as_of(store: FieldworkStore, cutoff: datetime) -> dict:
    """只重放 occurred_at <= cutoff 的事件，还原当时状态。

    事件仍按规范化顺序处理，因此 tie-break 与当前投影一致；子集的链校验
    已由完整日志承担，这里跳过链门，仅保留前向引用闭合检查。
    """
    sub_log = [
        event for event in store.physical_log()
        if parse_occurred_at(event["occurred_at"]) <= cutoff
    ]
    _, quarantine, projection, _, _ = replay(sub_log, check_chains=False)

    sub = FieldworkStore()
    sub._log = sub_log
    sub._quarantine = quarantine
    sub._projection = projection
    sub._status = {}
    for event in sub_log:
        eid = event["event_id"]
        if eid in quarantine:
            sub._status[eid] = {"status": "quarantined",
                                "reasons": quarantine[eid]["reasons"]}
        else:
            sub._status[eid] = {"status": "applied", "reasons": []}

    views = build_views(sub)
    views["cutoff"] = cutoff.isoformat()
    return views
