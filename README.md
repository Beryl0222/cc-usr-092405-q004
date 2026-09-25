# 考古现场关系管理

保存跨年度勘探、发掘、研究判断与样本流转之间的可追溯关系。所有写操作都是**只追加事件**：
服务不保存可变业务状态，当前状态是事件日志的确定性投影。原值永不被覆盖，任何时点的结论都可还原。

## 为什么是事件溯源

主墓、兆沟、陪葬墓、祭祀坑和数百件器物由不同年度队伍记录；后期关于年代与墓主的推测不能覆盖早期现场信息。
为此系统把"现场事实"和"研究判断"都建模成不可变事件：

- 探方、地层、遗迹、器物组合、铭文、测年样本、照片一旦记录，只能通过新事件**校正**，旧值逐版保留。
- 空间/层位关系（包含、叠压、打破、拍摄、同一）是一等事实，并可推导传递闭包。
- 年代、制度等级、墓主的竞争性观点并存；每个观点必须引用具体证据、标注置信度、接受独立同行评议。
- 发布是不可变事件，快照锚定**发布日期当时**成立的观点版本与评议；"可能"为真不能以既定事实发布。

## 运行与测试

```bash
python3 service.py --check            # 基础配置检查
python3 service.py --store data.jsonl # 启动并加载/追加 JSONL 事件日志
npm test                              # 运行契约测试与全部领域测试（65 项）
python3 -m compileall -q .            # 编译检查全部 Python 模块
```

## 事件信封

每个采集设备维护一条 SHA-256 哈希链和一个逻辑时钟：

```json
{
  "event_id": "tablet-A-17",
  "device_id": "tablet-A",
  "lamport": 17,
  "type": "record.correct",
  "payload": { "...": "..." },
  "occurred_at": "2026-04-01T09:00:00",
  "prev_hash": "上一事件的 sha256（首事件为 null）"
}
```

事件内容哈希为规范化 JSON（`ensure_ascii=False, sort_keys=True`）的 SHA-256。
批量、乱序、跨设备提交均支持：`POST /events` 接受单个事件或 `{"events": [...]}`。

### 合并与完整性

- **规范化顺序**：`(lamport, device_id, event_id)`。与到达批次无关，任意重放结果字节级一致
  （有测试对正序、逆序、穿插三种喂入做断言）。
- **幂等**：`event_id` 与载荷均相同的重复提交返回 `duplicate`；同 id 不同载荷返回 `rejected`，
  防止后期提交覆盖原值。
- **哈希链**：
  - `waiting`——引用的祖先事件尚未归队（缺环）。事件暂不投影，祖先到齐后**自愈**。
  - `quarantined`（链断裂）——伪造哈希、链重置、同设备时钟倒退；该事件及后继隔离。
- **重复现场编号**：两支队伍同用 `M001` 时进入 `/conflicts`；`number.resolve`
  指定规范保留方并给其余记录分配新号（alias），仲裁本身也是事件。
- **语义隔离**：超额消耗样本、保管链非法转移、证据悬空的观点、不达标发布等均 `quarantined`，
  原始事件保留在日志与冲突清单中，不污染空间关系与样本余额。

## 记录与空间

| 事件 | 作用 |
| --- | --- |
| `record.register` | 注册探方/地层/遗迹/墓葬/…，可选现场编号 |
| `record.correct` | 形成新版本，旧值在 `/records/history` 逐版可查；禁止借校正改编号 |
| `number.resolve` | 重复编号仲裁与 alias 分配 |
| `spatial.relate` / `spatial.retract` | 建立/撤回关系；`below` 自动归一化为反向 `above` |
| `dating.register` / `dating.withdraw` | 测年登记与撤回；撤回后移入 withdrawn 清单，样本余额不变 |

`/relations` 同时给出直接边、`contains`/`above` 的传递推导和中文标注；
双向叠压等矛盾进入 `/conflicts.spatial`。

## 样本、保管链与跨机构借用

- `sample.consume` 逐笔扣减并**幂等**（重复事件不二次扣减）；超过初始量的消耗隔离，余额稳定。
  样本已被借用申请锁定或已出库的余量同样不得被现场消耗侵占——账面与保管链必须相互印证。
- 保管链三态动作，全部校验当前持有方：
  - `custody.checkout`：必须携带 `from_party` 且等于当前持有方；在途不得重复出库。
  - `custody.return`：仅在出库后可归还，登记现状与入库库房。
  - `custody.transfer`：跨库移交，`from_party` 不符即拒绝，杜绝"已不在本库却被移交"。

### 带期限的外借承诺

借用是一条只追加的连续谱系（`loan_id` 贯穿，每步写入 `ledger`）：

| 事件 | 作用 |
| --- | --- |
| `loan.apply` | 申请：**锚定当前样本版本**（`sample_version_event`）**与当前研究主张版本**（`claim_version_event`），载明数量、保管条件、研究目的、归还期限 `due_at` 与借入实验室；主张证据须仍为当前可用证据 |
| `loan.approve` | **无利益冲突**保管人审批：审批人不得是申请人或所引主张作者、保管方不得是借入实验室；审批即锁定可用余量 |
| `loan.decline` | 驳回待审批/已批准申请（仅原审批人可驳回已批准件），锁定立即释放 |
| `loan.pickup` | 领用出库，`from_party` 须等于审批保管方；冻结申请不得交付 |
| `loan.return` | 部分/全部归还并登记现状；归还量立即恢复可再分配，申请在外未结清量递减 |
| `loan.consume` | 实验室确认在外消耗，计入样本总账，不得超过未结清量；结清后状态 `consumed` |
| `loan.recall` | 逾期/主动追索登记，形成谱系节点，不改变实物数量 |

- **余量竞争**：并发（含离线）申请按规范化顺序 `(lamport, device_id, event_id)` 裁决。
  后归队的早序事件会使先前临时成立的审批在全量重放中翻转为 `quarantined`，
  负方申请回退待审批、不占余量；任何喂入顺序下锁定结果字节级一致。
- **逾期**：`on_loan` 且当前观察时点晚于 `due_at` 派生 `overdue` 标记（非事件、不改账）。
  `/samples?as_of=`、`/loans?as_of=` 与 `/asof?date=` 均可按指定日期查看在借/逾期/可再分配量。
- **冻结（尚未交付的申请）**：
  - 撤回的测年（`dating.withdraw`）经主张证据链冻结 `pending`/`approved` 申请；
  - 撤回的研究主张（`claim.withdraw`）同样冻结引用它的未交付申请；
  - 离线重复注册/登记（`record.register`、`claim.submit`、`dating.register`）
    被语义隔离时，引用该编号、或锚定版本事件落入隔离清单的未交付申请冻结；
  - 已领用（`on_loan` 及之后）的申请不受冻结影响，按逾期与追索流程处理。
- **断链隔离**：借用申请/审批事件自身位于断裂或缺环设备链上时不进入投影，
  绝不锁定或扣减余量；缺环补齐后自愈。
- **幂等与重启**：离线重复提交同一事件返回 `duplicate`，不二次锁定或扣减；
  全部占用、谱系与追索状态都由 JSONL 日志重放还原，服务重启无损失。

账面恒等式（可在测试中相互印证）：

```
初始量 = 现场消耗 + 外借已消耗 + 在借未结 + 待交付锁定 + 可再分配
每笔申请：批准数量 = 已归还 + 已确认消耗 + 未结清
```

## 观点、评议与发布

- `claim.submit/revise/withdraw`：观点逐版保留；`confidence ∈ [0,1]`，自动归入 high/medium/low。
- `peer.review`：独立第三方给出 `support/challenge/neutral`；作者不能评议自己的观点。
- `publication.release`：发布条目按发布日期做快照。以 `established`（既定事实）发布要求
  发布日期前：置信度 ≥ 0.8、至少一条独立 `support`、没有未消解的 `challenge`、证据均已产生且未撤回。
  不满足时整条发布隔离；竞争性或低置信判断只能以 `hypothesis`（假说）发布。
- 发布后观点再修订不影响既有快照；离线迟交的发布仍按其声明的日期锚定当时的版本。

## 时点还原

`GET /asof?date=2026-05-20T00:00:00` 只重放该时点前的事件，返回当时的记录版本、样本余额
（含在借/逾期/待交付锁定/可再分配）、借用谱系状态、测年清单、观点与已发生的发布——
研究者可以还原任一发布日期当时成立的结论。

## HTTP 一览

| 方法/路径 | 内容 |
| --- | --- |
| `GET /health` | 服务身份（保持原契约） |
| `POST /events` | 摄入单事件或批量事件，返回逐条 applied/duplicate/waiting/quarantined/rejected |
| `GET /events` | 规范化日志与每条事件状态 |
| `GET /records`、`/records/history?record_id=` | 当前记录与全部历史版本 |
| `GET /relations` | 直接边、传递推导、中文标注 |
| `GET /samples` | 初始量、现场/外借消耗、在借、逾期、待交付锁定、可再分配数量（支持 `as_of`） |
| `GET /loans` | 借用谱系（ledger/timeline、冻结原因、追索记录），支持 `sample_id`、`status`、`as_of` 过滤 |
| `GET /custody` | 每项器物/样本的持有方与保管事件链 |
| `GET /datings` | 有效测年与已撤回测年（含撤回原因） |
| `GET /claims` | 观点当前版本、置信带、评议 |
| `GET /releases` | 不可变发布快照 |
| `GET /conflicts` | 编号冲突、层位矛盾、隔离事件、缺环、竞争观点 |
| `GET /verify/chains` | 每台设备的哈希链校验结果 |
| `GET /asof?date=` | 任一时点的状态还原 |

`fixtures/domain.json` 保存统一词表（记录类型、关系、事件、置信带、发布纪律），接口联调时以此为准。
