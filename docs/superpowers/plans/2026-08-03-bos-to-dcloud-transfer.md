# BOS → 地瓜云(D-Robotics JuiceFS)搬运 — 一次性批量导入方案

> **For agentic workers:** 本计划由主会话直接执行(一次性运维脚本,非产品代码),
> 每阶段有独立 Opus 审查闸门。

**Goal:** 把已确认下载完好的数据集从 BOS 安全搬运到地瓜云 JuiceFS,全程 BOS 只读,
搬运后逐一尺寸核验。

**日期:** 2026-08-03(用户批准当晚开跑,早晨看结果)

## R — 需求

- R1 搬运 = 调用地瓜云服务端导入 API(`DCloudClient.import_from_bos`),数据流
  只发生在地瓜云↔BOS 之间;我们的机器、专线不在数据路径上。
- R2 **BOS 绝对只读**:整个流程对 BOS 只调用 list/HEAD;不存在任何 delete/put/
  copy-to-BOS 代码路径。JuiceFS 侧只新增(导入到规范目标路径),不删不改已有内容。
- R3 前缀驱动,不信任务表:待搬清单来自 2026-08-03 对账
  (`scripts/reconcile_transfers.py` v2,产物 `/root/transfer_reconcile-20260803-v2.json`),
  以 BOS 实测为准。特别地:AgiBotWorld-Alpha 在任务表没有 done 行,任何任务驱动
  的方案都会漏掉它。
- R4 串行导入(并发=1),防止把地瓜云入口打爆;单个失败即停("stop-on-failure"),
  不级联。
- R5 长任务鲁棒:单个导入可能跑数小时~天级。轮询要 token 过期自动重登、网络抖动
  重试退避、进度落盘(断点续跑:重跑时已核验项自动跳过)。
- R6 进程管理走 systemd(transient unit),不用裸 nohup/setsid(会话死进程死,
  已有前科)。
- R7 每个数据集导入完成后立即核验:JuiceFS 目标文件夹递归大小 ≥ BOS 前缀实测
  字节数(JuiceFS 有 0-4% 文件系统开销,≥ 即完整信号)。
- R8 全程留痕:状态 JSON + 日志,早晨可出示每一步证据。

## 待搬清单(按序,均为目标路径干净/不存在的 NOT_TRANSFERRED)

| # | 数据集 | BOS 源前缀 | 实测大小(TiB) | JuiceFS 目标 |
|---|---|---|---|---|
| 1 | AgiBotWorld-Alpha | `manipulation/AgiBotWorld-Alpha/` | 8.79 TiB (258 obj) | `raw-data/manipulation/AgiBotWorld-Alpha` |
| 2 | AgiBotWorld-Beta-BJ | `manipulation/AgiBotWorld-Beta-BJ/` | 42.87 TiB (1412 obj) | `raw-data/manipulation/AgiBotWorld-Beta-BJ` |
| 3 | InternData-A1 | `other/InternData-A1/` | 6.39 TiB (35979 obj) | `raw-data/other/InternData-A1` |
| 4 | DL3DV-ALL-4K | `datasets/DL3DV-ALL-4K/` | 39.92 TiB (10581 obj) | `raw-data/multimodal/DL3DV-ALL-4K` |
| 5 | PhysicalAI-Robotics-Open-H-Embodiment | `other/PhysicalAI-Robotics-Open-H-Embodiment/` | 0.76 TiB (17778 obj) | `raw-data/other/PhysicalAI-Robotics-Open-H-Embodiment` |
| 6 | MolmoAct-Dataset | `other/MolmoAct-Dataset/` | 0.67 TiB (7912 obj) | `raw-data/other/MolmoAct-Dataset` |
| 7 | RDT-1B | `manipulation/RDT-1B/` | 0.63 TiB (83 obj) | `raw-data/manipulation/RDT-1B` |
| 8 | PhysicalAI-Robotics-Locomanipulation-GRAIL | `whole-body/PhysicalAI-Robotics-Locomanipulation-GRAIL/` | 0.24 TiB (158800 obj) | `raw-data/whole-body/PhysicalAI-Robotics-Locomanipulation-GRAIL` |

顺序理由:#1 小而已验证(链路首验证)→ #2 最关键的大头 → 其余按价值/大小。
**时间预期(审查者按地瓜云历史吞吐 ~470-540 MB/s 实测推算):Alpha ~6h、
Beta-BJ ~28h、DL3DV ~26h、InternData ~4h、其余 ~3h,全程串行约 65-70 小时**——
"一夜跑完"只适用于 #1 和 #2 的一部分,盯梁按天计。

已知取舍记录:`multimodal/DL3DV-ALL-4K/`(4.54 TiB 部分重复副本)里有 **3 个对象
只存在于该副本**(`11K/ed282d12….zip`、`7K/9e11a4bd….zip` 共 ~9.8 GiB,及
`_chunked_progress.json`);本次只导 `datasets/` 主副本,这 3 个对象暂缺,
留待早晨决策(可用一次 mini-import 补齐)。
明确**排除**:Sekai(BOS 上根本没有 9.4 TB,需源仓库审计后重下)、所有 PARTIAL
项(导入 API 对已存在目标的合并语义未验证,不冒险)、所有 VERIFIED 项(已在)、
BOS_EMPTY 项。GR00T-X-Embodiment-Sim(0.10 TB)暂缓:JuiceFS 侧显示 1742%,
两侧内容对不上,需人工看过再说。

## A — 验收标准

- A1 dry-run 输出与上表逐行一致(源实测大小、目标路径、目标当前状态),且证明
  目标路径均不存在/为空。
- A2 Alpha 导入完成且核验:JuiceFS `raw-data/manipulation/AgiBotWorld-Alpha`
  大小 ≥ BOS 实测;**scope 检查通过**(目标顶层子项 ⊆ 源前缀顶层子项——防
  前缀渗漏和嵌套错位,尺寸检查对"超量"是盲的);BOS 前缀对象数/字节数与
  搬运前快照完全一致(只读证明)。
- A3 Beta-BJ 同 A2(含 scope 检查)。
- A4 清单其余项:每项要么核验通过,要么状态文件里有明确失败原因且流程按
  stop-on-failure 停住(不吞错、不跳过)。
- A5 全程无 BOS 写操作:脚本代码审查确认零写路径;搬运前后 BOS 前缀快照
  (字节+对象数)一致。
- A6 进程由 systemd transient unit 管理,主会话断开不影响;状态 JSON 可断点续跑。
- A7 终审:Opus agent 对着状态文件、日志、两侧实测大小独立复核。

## T — 任务

- T1 写 `scripts/transfer_import.py`(一次性脚本,dry-run 默认,`--execute` 实弹;
  内置上表清单;串行;每项:预检(源非空、目标干净)→ create_folder(category)
  → import_from_bos → 轮询(60s 间隔,重登/退避,单项上限 72h)→ 尺寸核验 →
  状态落盘 `/root/transfer_state-20260803.json`)。
- T2 Opus 审方案+代码(合并审:脚本小,一次审透;重点:BOS 零写、轮询鲁棒性、
  断点语义、stop-on-failure)。
- T3 dry-run,人肉比对输出与本表。
- T4 `systemd-run --unit dlm-transfer-oneshot` 实弹;主会话周期性盯状态
  (15-30 分钟一轮),异常即停即报。
- T5 早晨:Opus 终审(A1-A7 逐条),写结果报告归档 docs/reports/,汇报用户。

## 风险与对策

- 地瓜云配额/容量未知(根目录已用 ~663 TB,本批 +~100 TB):导入失败会在轮询中
  显性暴露,stop-on-failure 兜底;不预清理任何东西。
- 导入 API 对超大前缀(42.87 TB)的行为未知:先用 Alpha(9 TiB)验证链路,
  失败不碰后续。
- token 有效期未知:每次轮询失败即重登一次再试;连续 5 次失败按项目失败处理。
- D-Robotics 侧异步任务列表只有 100 页深:轮询按 task_id 过滤,找不到时不视为
  失败(列表可能翻页),连续 30 分钟找不到才报警。
