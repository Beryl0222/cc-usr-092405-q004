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
    "loan.apply",
    "loan.approve",
    "loan.decline",
    "loan.pickup",
    "loan.return",
    "loan.consume",
    "loan.recall",
}

REVIEW_VERDICTS = ("support", "challenge", "neutral")
PUBLISH_AS = ("established", "hypothesis")

# 借用申请生命周期（连续谱系）：
# pending --approve--> approved --pickup--> on_loan
#   pending/approved --decline--> declined（冻结件同样以 decline 终结）
#   approved --pickup--> on_loan --return(部分)--> on_loan
#   on_loan --全部归还--> returned --consume 确认 --> consumed
#   on_loan 超过 due_at 未结清 --> overdue（查询时派生标记，非独立状态）
#   消耗确认也可在 on_loan 上直接累计；returned_amount + consumed_amount
#   + outstanding_amount 恒等于批准借用数量。

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
        # loan_id -> 借用申请聚合（含逐事件谱系 ledger）
        self.loans = {}

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
        # 撤回的年代判断冻结尚未交付（待审批/已批准未领用）的借用申请。
        self.freeze_loans_for_dating_withdraw(p["dating_id"], event)

    # -- 样本消耗 -----------------------------------------------------------

    def apply_sample_consume(self, event):
        p = event["payload"]
        require(p, {"sample_id": str, "amount": (int, float), "purpose": str})
        sample = self.samples.get(p["sample_id"])
        if sample is None:
            raise Quarantine(f"消耗事件指向不存在的样本: {p['sample_id']}")
        if p["amount"] <= 0:
            raise Quarantine("样本消耗量必须为正数")
        consumed = sum(amount for _, amount, _ in sample["consumed"])
        if consumed + p["amount"] > sample["initial"] + 1e-9:
            raise Quarantine(
                f"样本 {p['sample_id']} 余额不足: 现存 "
                f"{sample['initial'] - consumed}{sample['unit']}, "
                f"申请消耗 {p['amount']}{sample['unit']}"
            )
        # 已被借用申请锁定或已出库的余量不得被现场消耗侵占——账面余量必须与
        # 保管链相互印证；canonical 顺序决定并发事件的胜负，负方隔离。
        free = self._free_balance(p["sample_id"])
        if p["amount"] > free + 1e-9:
            raise Quarantine(
                f"样本 {p['sample_id']} 可再分配余量不足: 未锁定在库 "
                f"{round(free, 9)}{sample['unit']}, 申请消耗 "
                f"{p['amount']}{sample['unit']}（其余在借/已锁定）"
            )
        sample["consumed"].append((event["event_id"], p["amount"], "direct"))

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

    # -- 样本借用 -----------------------------------------------------------

    def _loan(self, loan_id, *, must_exist=True):
        loan = self.loans.get(loan_id)
        if loan is None and must_exist:
            raise Quarantine(f"借用申请不存在: {loan_id}")
        return loan

    def _current_record_version(self, record_id):
        record = self.records.get(record_id)
        if record is None:
            raise Quarantine(f"引用的记录不存在: {record_id}")
        return record["versions"][-1]

    def _sample_loan_totals(self, sample_id):
        """返回该样本各借用当前占用量（按规范化重放后的当前状态）。

        locked：已批准未领用；on_loan：在外未结清；returned：已回库但
        申请尚未做消耗确认（可再分配）；consumed：经借用确认的消耗。
        """
        locked = on_loan = returned = consumed = 0.0
        for loan in self.loans.values():
            if loan["sample_id"] != sample_id:
                continue
            if loan["status"] in ("approved",):
                locked += loan["amount"]
            elif loan["status"] == "on_loan":
                on_loan += loan["outstanding"]
                returned += loan["returned_amount"]
            elif loan["status"] == "returned":
                returned += loan["returned_amount"]
            elif loan["status"] == "consumed":
                consumed += loan["consumed_amount"]
                returned += loan["returned_amount"]
        return locked, on_loan, returned, consumed

    def _free_balance(self, sample_id):
        """可再分配余量：初始量 − 直接消耗 − 在借 − 已锁定 − 已确认外借消耗。

        已部分归还但未确认消耗的量已回到库房，可再次分配，故不计占用。
        """
        sample = self.samples.get(sample_id)
        if sample is None:
            return 0.0
        direct_consumed = sum(a for _, a, kind in sample["consumed"]
                              if kind == "direct")
        locked, on_loan, _returned, loan_consumed = self._sample_loan_totals(sample_id)
        reserved = locked + on_loan + loan_consumed
        return sample["initial"] - direct_consumed - reserved

    def apply_loan_apply(self, event):
        p = event["payload"]
        require(p, {
            "loan_id": str,
            "sample_id": str,
            "lab_party": str,
            "applicant": str,
            "amount": (int, float),
            "purpose": str,
            "due_at": str,
            "claim_id": str,
            "storage_condition": str,
        })
        if p["loan_id"] in self.loans:
            raise Quarantine(f"借用编号重复: {p['loan_id']}")
        if p["amount"] <= 0:
            raise Quarantine("借用数量必须为正数")
        sample = self.samples.get(p["sample_id"])
        if sample is None:
            raise Quarantine(f"借用指向不存在的测年样本: {p['sample_id']}")
        claim = self.claims.get(p["claim_id"])
        if claim is None:
            raise Quarantine(f"借用引用的研究主张不存在: {p['claim_id']}")
        if claim["status"] != "active":
            raise Quarantine(f"研究主张已 {claim['status']}，不能据此提出借用")
        # 申请锚定的主张版本其证据必须仍为当前可用证据：撤回/隔离在先时，
        # 新申请直接隔离；撤回在申请之后则由撤回事件冻结（见 dating/claim
        # withdraw）。断链设备上的主张不在投影中，此处同样闭合不了。
        claim_version = claim["versions"][-1]
        for ref in claim_version["evidence"]:
            if not self._evidence_current(ref):
                raise Quarantine(f"研究主张的证据已撤回或不可用: {ref}")
        due = parse_occurred_at(p["due_at"])
        if due <= parse_occurred_at(event["occurred_at"]):
            raise Quarantine("归还期限 due_at 必须晚于申请时间")
        # 申请必须锚定当前样本版本与当前主张版本——离线重复请求携带的是同一
        # 事件（event_id 相同），幂等返回 duplicate；若另立新申请则重新锚定。
        sample_version = self._current_record_version(p["sample_id"])
        claim_version = claim["versions"][-1]
        loan = {
            "loan_id": p["loan_id"],
            "sample_id": p["sample_id"],
            "lab_party": p["lab_party"],
            "applicant": p["applicant"],
            "amount": p["amount"],
            "purpose": p["purpose"],
            "storage_condition": p["storage_condition"],
            "due_at": p["due_at"],
            "claim_id": p["claim_id"],
            "sample_version": sample_version["version"],
            "sample_version_event": sample_version["event_id"],
            "claim_version": claim_version["version"],
            "claim_version_event": claim_version["event_id"],
            "status": "pending",
            "outstanding": 0.0,
            "returned_amount": 0.0,
            "consumed_amount": 0.0,
            "approver": None,
            "frozen": False,
            "freeze_reasons": [],
            "frozen_events": [],
            "ledger": [],
        }
        loan["ledger"].append(self._ledger_entry(event, "apply", {
            "amount": p["amount"], "due_at": p["due_at"],
            "sample_version": sample_version["version"],
            "claim_version": claim_version["version"],
        }))
        self.loans[p["loan_id"]] = loan

    def apply_loan_approve(self, event):
        p = event["payload"]
        require(p, {"loan_id": str, "approver": str, "custodian_party": str})
        loan = self._loan(p["loan_id"])
        if loan["status"] != "pending":
            raise Quarantine(
                f"借用 {loan['loan_id']} 当前状态 {loan['status']}，不能审批")
        if loan["frozen"]:
            raise Quarantine(
                f"借用 {loan['loan_id']} 引用上下文已冻结: "
                + "; ".join(loan["freeze_reasons"]))
        # 无利益冲突：审批保管人不能是申请实验室成员，也不能是主张作者。
        claim = self.claims[loan["claim_id"]]
        if p["approver"] == loan["applicant"]:
            raise Quarantine("审批保管人不能是申请人本人")
        if p["approver"] == claim["author"]:
            raise Quarantine("审批保管人不能是所引研究主张的作者（利益冲突）")
        if p["custodian_party"] == loan["lab_party"]:
            raise Quarantine("审批保管方不能是借入实验室本身（利益冲突）")
        # 审批即锁定可用余量：并发申请按规范化顺序竞争，锁满即隔离。
        free = self._free_balance(loan["sample_id"])
        if loan["amount"] > free + 1e-9:
            sample = self.samples[loan["sample_id"]]
            raise Quarantine(
                f"样本 {loan['sample_id']} 可再分配余量不足: "
                f"{round(free, 9)}{sample['unit']}, "
                f"申请借用 {loan['amount']}{sample['unit']}"
            )
        loan["status"] = "approved"
        loan["approver"] = p["approver"]
        loan["custodian_party"] = p["custodian_party"]
        loan["approved_at"] = event["occurred_at"]
        loan["ledger"].append(self._ledger_entry(event, "approve", {
            "approver": p["approver"],
            "custodian_party": p["custodian_party"],
        }))

    def apply_loan_decline(self, event):
        p = event["payload"]
        require(p, {"loan_id": str, "approver": str, "reason": str})
        loan = self._loan(p["loan_id"])
        if loan["status"] not in ("pending", "approved"):
            raise Quarantine(
                f"借用 {loan['loan_id']} 当前状态 {loan['status']}，不能驳回")
        if loan["status"] == "approved" and p["approver"] != loan["approver"]:
            raise Quarantine("只有原审批保管人可以驳回已批准申请")
        loan["status"] = "declined"
        loan["decline_reason"] = p["reason"]
        loan["ledger"].append(self._ledger_entry(event, "decline", {
            "approver": p["approver"], "reason": p["reason"]}))

    def apply_loan_pickup(self, event):
        p = event["payload"]
        require(p, {"loan_id": str, "actor": str, "from_party": str,
                    "condition": str})
        loan = self._loan(p["loan_id"])
        if loan["status"] != "approved":
            raise Quarantine(
                f"借用 {loan['loan_id']} 未经批准或已处理，不能领用")
        if loan["frozen"]:
            raise Quarantine(
                f"借用 {loan['loan_id']} 已冻结，尚未交付的申请停止放行: "
                + "; ".join(loan["freeze_reasons"]))
        if p["from_party"] != loan.get("custodian_party"):
            raise Quarantine(
                f"领用放行无效：审批保管方为 {loan.get('custodian_party')}，"
                f"出库单声称 {p['from_party']}")
        loan["status"] = "on_loan"
        loan["outstanding"] = loan["amount"]
        loan["picked_up_at"] = event["occurred_at"]
        loan["ledger"].append(self._ledger_entry(event, "pickup", {
            "actor": p["actor"], "from_party": p["from_party"],
            "condition": p["condition"], "amount": loan["amount"]}))

    def apply_loan_return(self, event):
        p = event["payload"]
        require(p, {"loan_id": str, "actor": str, "amount": (int, float),
                    "condition": str})
        loan = self._loan(p["loan_id"])
        if loan["status"] != "on_loan":
            raise Quarantine(
                f"借用 {loan['loan_id']} 当前不在借，不能归还")
        if p["amount"] <= 0:
            raise Quarantine("归还数量必须为正数")
        if p["amount"] > loan["outstanding"] + 1e-9:
            raise Quarantine(
                f"归还数量超过未结清量: 未结清 "
                f"{loan['outstanding']}，归还 {p['amount']}")
        loan["outstanding"] = round(loan["outstanding"] - p["amount"], 9)
        loan["returned_amount"] = round(loan["returned_amount"] + p["amount"], 9)
        if loan["outstanding"] <= 1e-9:
            loan["status"] = "returned"
        loan["ledger"].append(self._ledger_entry(event, "return", {
            "actor": p["actor"], "amount": p["amount"],
            "condition": p["condition"],
            "to_store": p.get("to_store", DEFAULT_HOLDER),
            "outstanding_after": loan["outstanding"]}))

    def apply_loan_consume(self, event):
        """实验室对借用样本的消耗确认；外借消耗与现场消耗同账扣减。"""
        p = event["payload"]
        require(p, {"loan_id": str, "actor": str, "amount": (int, float),
                    "note": str})
        loan = self._loan(p["loan_id"])
        if loan["status"] not in ("on_loan", "returned"):
            raise Quarantine(
                f"借用 {loan['loan_id']} 当前状态 {loan['status']}，"
                "须领用后才能确认消耗")
        if p["amount"] <= 0:
            raise Quarantine("消耗确认数量必须为正数")
        if p["amount"] > loan["outstanding"] + 1e-9:
            raise Quarantine(
                f"消耗确认超过未结清量: 未结清 {loan['outstanding']}，"
                f"确认 {p['amount']}（已归还部分不得再报消耗）")
        loan["outstanding"] = round(loan["outstanding"] - p["amount"], 9)
        loan["consumed_amount"] = round(loan["consumed_amount"] + p["amount"], 9)
        sample = self.samples[loan["sample_id"]]
        sample["consumed"].append((event["event_id"], p["amount"], "loan"))
        if loan["outstanding"] <= 1e-9:
            loan["status"] = "consumed"
        loan["ledger"].append(self._ledger_entry(event, "consume", {
            "actor": p["actor"], "amount": p["amount"], "note": p["note"],
            "outstanding_after": loan["outstanding"]}))

    def apply_loan_recall(self, event):
        """逾期追索/主动召回：登记追索，不改变实物数量，只形成谱系节点。"""
        p = event["payload"]
        require(p, {"loan_id": str, "actor": str, "reason": str})
        loan = self._loan(p["loan_id"])
        if loan["status"] != "on_loan":
            raise Quarantine(
                f"借用 {loan['loan_id']} 当前状态 {loan['status']}，"
                "仅在借申请可以追索")
        loan.setdefault("recalls", []).append(self._ledger_entry(
            event, "recall",
            {"actor": p["actor"], "reason": p["reason"]}))
        loan["ledger"].append(loan["recalls"][-1])

    # -- 借用冻结：撤回的年代判断 / 被隔离的上下文 --------------------------

    def _freeze_loan(self, loan, reason, event):
        if not loan["frozen"]:
            loan["frozen"] = True
            loan["frozen_at"] = event["occurred_at"]
            loan["freeze_reasons"] = []
        if reason not in loan["freeze_reasons"]:
            loan["freeze_reasons"].append(reason)
        loan["frozen_events"].append(event["event_id"])
        loan["ledger"].append(self._ledger_entry(event, "frozen", {"reason": reason}))

    def freeze_loans_for_dating_withdraw(self, dating_id, event):
        """测年撤回：冻结引用该测年（经主张证据链）的未交付申请。"""
        loan_ids = self._loans_referencing_dating(dating_id)
        for loan_id in loan_ids:
            loan = self.loans[loan_id]
            if loan["status"] in ("pending", "approved"):
                self._freeze_loan(
                    loan, f"所引主张依赖的测年 {dating_id} 已撤回", event)

    def _loan_evidence(self, loan):
        claim = self.claims.get(loan["claim_id"])
        if claim is None:
            return set()
        version = next((v for v in claim["versions"]
                        if v["version"] == loan["claim_version"]), None)
        return set(version["evidence"]) if version else set()

    def _loans_referencing_dating(self, dating_id):
        return [loan_id for loan_id, loan in self.loans.items()
                if dating_id in self._loan_evidence(loan)]

    @staticmethod
    def _ledger_entry(event, action, detail):
        entry = {
            "action": action,
            "event_id": event["event_id"],
            "device_id": event["device_id"],
            "occurred_at": event["occurred_at"],
        }
        entry.update(detail)
        return entry

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
        # 研究主张撤回后，引用它的未交付借用申请同样冻结。
        for loan in self.loans.values():
            if (loan["claim_id"] == p["claim_id"]
                    and loan["status"] in ("pending", "approved")):
                self._freeze_loan(
                    loan, f"所引研究主张 {p['claim_id']} 已撤回", event)

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
    "loan.apply": Projection.apply_loan_apply,
    "loan.approve": Projection.apply_loan_approve,
    "loan.decline": Projection.apply_loan_decline,
    "loan.pickup": Projection.apply_loan_pickup,
    "loan.return": Projection.apply_loan_return,
    "loan.consume": Projection.apply_loan_consume,
    "loan.recall": Projection.apply_loan_recall,
}


# ---------------------------------------------------------------------------
# 隔离上下文冻结扫描
# ---------------------------------------------------------------------------

def freeze_loans_from_quarantine(p: Projection, quarantine: dict) -> None:
    """重放结束后，依据最终隔离清单冻结尚未交付的借用申请。

    事件驱动的撤回（dating/claim withdraw）已在投影过程中即时冻结；本扫描
    处理只能在完整重放后确定的"离线上下文冲突"：

    * 同一编号的重复注册/提交（record.register / claim.submit /
      dating.register）离线归队后被隔离——引用该编号的未交付申请冻结；
    * 申请锚定的样本版本事件、主张版本事件或证据产生事件落入隔离清单。

    冻结时间取冲突事件的 occurred_at（较早者），从而 /asof 能判定该时点
    申请是否已冻结。扫描在重放末尾对最终状态执行，与事件喂入顺序无关。
    """
    # 编号 -> 被隔离的冲突事件（取最早 occurred_at）。
    conflict_at: dict[tuple, datetime] = {}

    def note(key, when):
        old = conflict_at.get(key)
        if old is None or when < old:
            conflict_at[key] = when

    for info in quarantine.values():
        event = info["event"]
        when = parse_occurred_at(event["occurred_at"])
        payload = event["payload"]
        etype = event["type"]
        if etype == "record.register":
            note(("sample", payload.get("record_id")), when)
        elif etype == "claim.submit":
            note(("claim", payload.get("claim_id")), when)
        elif etype == "dating.register":
            note(("dating", payload.get("dating_id")), when)
        note(("event", event["event_id"]), when)

    for loan_id in sorted(p.loans):
        loan = p.loans[loan_id]
        if loan["status"] not in ("pending", "approved") or loan["frozen"]:
            continue
        reasons_at: list[tuple[str, datetime]] = []
        if ("sample", loan["sample_id"]) in conflict_at:
            reasons_at.append((
                f"样本 {loan['sample_id']} 存在离线重复注册，冲突上下文已被隔离",
                conflict_at[("sample", loan["sample_id"])]))
        if ("claim", loan["claim_id"]) in conflict_at:
            reasons_at.append((
                f"主张 {loan['claim_id']} 存在离线重复提交，冲突上下文已被隔离",
                conflict_at[("claim", loan["claim_id"])]))
        for eid in (loan["sample_version_event"], loan["claim_version_event"]):
            if ("event", eid) in conflict_at:
                reasons_at.append((f"锚定的版本事件 {eid} 已被隔离",
                                   conflict_at[("event", eid)]))
        for ref in sorted(_loan_evidence_refs(p, loan)):
            for producer in _producing_event_ids(p, ref):
                if ("event", producer) in conflict_at:
                    reasons_at.append((f"证据 {ref} 的产生事件 {producer} 已被隔离",
                                       conflict_at[("event", producer)]))
            if ("dating", ref) in conflict_at:
                reasons_at.append((f"测年 {ref} 存在离线重复登记，已隔离",
                                   conflict_at[("dating", ref)]))
        if not reasons_at:
            continue
        frozen_at = min(when for _, when in reasons_at)
        loan["frozen"] = True
        loan["frozen_at"] = frozen_at.isoformat()
        loan["freeze_reasons"] = [reason for reason, _ in reasons_at]
        loan["frozen_events"].append("quarantine-freeze")
        synthetic = {
            "event_id": "quarantine-freeze", "device_id": "system",
            "occurred_at": frozen_at.isoformat(),
        }
        loan["ledger"].append(Projection._ledger_entry(
            synthetic, "frozen",
            {"reason": "; ".join(loan["freeze_reasons"])}))


def _producing_event_ids(p: Projection, ref: str) -> set:
    if ref in p.records:
        record = p.records[ref]
        return {record["versions"][0]["event_id"], record["registered_event"]}
    if ref in p.datings:
        return {p.datings[ref]["event_id"]}
    if ref in p.datings_withdrawn:
        return {p.datings_withdrawn[ref]["event_id"]}
    return set()


def _loan_evidence_refs(p: Projection, loan: dict) -> set:
    claim = p.claims.get(loan["claim_id"])
    if claim is None:
        return set()
    version = next((v for v in claim["versions"]
                    if v["version"] == loan["claim_version"]), None)
    return set(version["evidence"]) if version else set()


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
    # 隔离清单（含断链后继）最终确定后，冻结引用被隔离上下文的未交付申请。
    freeze_loans_from_quarantine(projection, quarantine)
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


def build_views(store: FieldworkStore, as_of: datetime | None = None) -> dict:
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
        direct_consumed = sum(amount for _, amount, kind in sample["consumed"]
                              if kind == "direct")
        loan_consumed = sum(amount for _, amount, kind in sample["consumed"]
                            if kind == "loan")
        locked, on_loan, returned, loans_consumed_total = p._sample_loan_totals(sid)
        overdue = None
        if as_of is not None:
            overdue = sum(
                loan["outstanding"] for loan in p.loans.values()
                if loan["sample_id"] == sid and loan["status"] == "on_loan"
                and parse_occurred_at(loan["due_at"]) < as_of
            )
        # 可再分配 = 初始 − 现场消耗 − 在借未结 − 待交付锁定 − 外借已消耗；
        # 已部分归还待确认的部分已回到库房，计入可再分配。
        available = (sample["initial"] - direct_consumed - on_loan
                     - locked - loans_consumed_total)
        samples.append({
            "sample_id": sid,
            "initial_amount": sample["initial"],
            "consumed_amount": round(direct_consumed + loan_consumed, 9),
            "direct_consumed_amount": round(direct_consumed, 9),
            "loan_consumed_amount": round(loan_consumed, 9),
            "balance": round(sample["initial"] - direct_consumed - loan_consumed, 9),
            "locked_amount": round(locked, 9),
            "on_loan_amount": round(on_loan, 9),
            "overdue_amount": round(overdue, 9) if overdue is not None else None,
            "returned_pending_amount": round(returned, 9),
            "available_amount": round(available, 9),
            "unit": sample["unit"],
            "consume_events": [eid for eid, _, _ in sorted(sample["consumed"])],
            "holder": p.holders.get(sid, DEFAULT_HOLDER),
            "as_of": as_of.isoformat() if as_of is not None else None,
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

    loans = [_loan_view(p.loans[lid], as_of) for lid in sorted(p.loans)]

    datings = [p.datings[did] for did in sorted(p.datings)]
    withdrawn = [p.datings_withdrawn[did] for did in sorted(p.datings_withdrawn)]

    return {
        "records": records,
        "relations": relations,
        "samples": samples,
        "custody": custody,
        "claims": claims,
        "releases": releases,
        "loans": loans,
        "datings": datings,
        "datings_withdrawn": withdrawn,
        "conflicts": conflicts,
    }


def _loan_view(loan: dict, as_of: datetime | None) -> dict:
    """渲染单个借用申请：固定谱系 + 按时点派生的逾期节点。"""
    overdue = (
        as_of is not None
        and loan["status"] == "on_loan"
        and parse_occurred_at(loan["due_at"]) < as_of
    )
    timeline = list(loan["ledger"])
    if overdue:
        # 逾期不是业务事件，但在观察时点上构成谱系的当前节点。
        timeline.append({
            "action": "overdue",
            "occurred_at": loan["due_at"],
            "outstanding_after": loan["outstanding"],
        })
    recalls = [
        {k: v for k, v in entry.items() if k != "action"}
        for entry in loan.get("recalls", [])
    ]
    return {
        "loan_id": loan["loan_id"],
        "sample_id": loan["sample_id"],
        "lab_party": loan["lab_party"],
        "applicant": loan["applicant"],
        "status": loan["status"],
        "amount": loan["amount"],
        "outstanding_amount": loan["outstanding"],
        "returned_amount": loan["returned_amount"],
        "consumed_amount": loan["consumed_amount"],
        "purpose": loan["purpose"],
        "storage_condition": loan["storage_condition"],
        "due_at": loan["due_at"],
        "claim_id": loan["claim_id"],
        "sample_version": loan["sample_version"],
        "sample_version_event": loan["sample_version_event"],
        "claim_version": loan["claim_version"],
        "claim_version_event": loan["claim_version_event"],
        "approver": loan["approver"],
        "frozen": loan["frozen"],
        "frozen_at": loan.get("frozen_at"),
        "freeze_reasons": list(loan["freeze_reasons"]),
        "overdue": overdue,
        "overdue_since": loan["due_at"] if overdue else None,
        "recalls": recalls,
        "ledger": list(loan["ledger"]),
        "timeline": timeline,
        "as_of": as_of.isoformat() if as_of is not None else None,
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

    views = build_views(sub, as_of=cutoff)
    views["cutoff"] = cutoff.isoformat()
    return views
