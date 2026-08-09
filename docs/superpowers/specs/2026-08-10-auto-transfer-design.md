# 自动搬运 + 手动搬运硬化 — 设计与验收标准

**日期**: 2026-08-10
**背景任务**: #83（下载完成自动触发搬运）
**前置设计**: `docs/design/auto-transfer-trigger.md`（2026-08-07）——触发点、分母、闸门、告警沿用那份，本文档只补它没有的部分并修正它的一处错误。

## 1. 要解决的问题

搬运（BOS → 地瓜云 JuiceFS）今天有三个洞：

1. **不会自动发生**。下载 `done` 之后没有任何东西触发搬运，全靠人记得跑脚本。
2. **网页上的三个按钮是死的**。`POST /transfer/trigger`（`routes/transfer.py:77`）和
   `POST /transfer/{task_id}/retry`（:109）都在往**已经死掉的 Celery broker** 投
   `apply_async(queue="transfers")`；`POST /transfer/pause` 只写内存 `cache`，web 一
   重启就失效。而网页把三个都渲染成正常控件（`index.html:645-673`）——点了没反应也
   不报错。（`docs/design/auto-transfer-trigger.md` 的"现状"表把这一行记成"可用"，
   是错的，本文档修正它。）
3. **手动脚本有两个危险行为，且搬不了模型**：
   - `ITEM_TIMEOUT_S = 72h` 到点 → `sys.exit` 停**整轮**，而且从不取消远端任务；
     重跑直接发新 import 而不查已存的 `dcloud_task_id`。2026-08-04 的 DL3DV 就是
     这样：我们的脚本 08-07 在 72h 处放弃，远端那条任务实际又跑到 **08-09 19:26
     才失败**（`failed to handle 2 objects`），中间两天我们完全不知道它还活着。
   - `DATA_BUCKET` 写死在 3 处（`:122`/`:136`/`:364`），目的端 `RAW` 是数据集布局
     字面量（`:63-64`），manifest 无桶/类型字段 → `auwomo-model-open` 里的模型
     一条都搬不了。

**不是问题的**：模型积压。2026-08-10 双端实测，`type='model'` 的 7 条任务（6 done
+ 1 revoked）在地瓜云上**全都已存在且字节吻合**。所以模型桶能力是"未来需要"，
不是积压；唯一异常是 `Wan2.2-TI2V-5B` 地瓜云那份比 BOS 源大 4.08 GB，是 **BOS
这边不全**，不是搬运漏了。

## 2. 已定的决策

| 决策 | 取值 | 依据 |
|---|---|---|
| 派发器架构 | web 进程内，`_dispatch_ready_transfers()` 作为 `_poll_transfers()` 的 sibling stage | 远端 API 一次 `list_async_tasks` 就能刷新全部在飞状态（O(1) 而非 O(N)）；本地无长跑计算，用不上 durable execution |
| 并发上限 | `DLM_TRANSFER_CONCURRENCY`，默认 **16** | 对方带宽几十 Gb 级；DL3DV 的 70–80 MB/s 是 per-object 开销不是带宽，提吞吐的杠杆是任务间并行 |
| 每轮新发上限 | 4 | 派发是阻塞 HTTP，走 `_blocking_stage`（线程池 4，`STAGE_TIMEOUT=60`）。16 是"在飞"上限，不是"每轮"上限 |
| 模型目的端 | `/727a2f92-30c/auwomo-model/{category}/{name}` | 远端 import 历史里 `auwomo-model-open.bj.bcebos.com/Qwen3-VL-30B-A3B-Thinking/ → /727a2f92-30c/auwomo-model/multimodal/Qwen3-VL-30B-A3B-Thinking/` 成功过；我们 6 个 done 模型有 5 个在 category 目录下 |
| 数据集目的端 | `/727a2f92-30c/auwomo-datasets/raw-data/{category}/{name}` | `raw-data` 下确实是 manipulation/multimodal/other/whole-body/… 等 category 目录 |
| 死按钮 | **接到新机制**，不删 | 网页已渲染；删按钮比修按钮更让人困惑 |
| `transfer_paused` | 落到 SQLite | 现在存内存 cache，web 重启即静默失效 |
| 自动开关 | `DLM_AUTO_TRANSFER`，默认 **on** | 沿用前置设计的确认 |

## 3. 状态机

```
NULL ──arm──► ready ──dispatch──► transferring ──远端成功──► verifying ──► done
  │                                    │                                └──► short
  │                                    └──远端失败──► failed
  └──闸门拒绝──► blocked（需人工，不自愈）
```

任务自己的 `status` **永不被搬运改写**——那正是 `dlm/transfer/tasks.py:76` 写
`status="transferring"` 的老 Celery bug。

`verifying` 这一步是新增的，它补的是一个真洞：现有 `_poll_transfers()`
（`scheduler.py:214`）看到远端"成功"就直接写 `transfer_status='done'`，**没做**
`transfer_import.py:398-410` 那三道验证。也就是说今天这条路径产出的 `done` 是
未经核实的 done。三道验证：

1. `jfs_bytes >= bos_bytes`（不足 → `short`）
2. scope：目的端顶层子项 ⊆ 源前缀顶层子项（有多余 → `short` + 告警）
3. BOS 未被动过：字节数与对象数与 arm 时一致（不一致 → 记录并告警，不阻塞）

## 4. 新增列（走 `snapshot.py:117` 的 additive ALTER 模式）

| 列 | 用途 |
|---|---|
| `dispatch_prefix` | 派发下载那一刻的 `bos_target()` 结果（`{bucket}/{prefix}`），由 `temporal_client.start_task_download` 写入（sharded/pool 的唯一漏斗）。用于"前缀漂移"闸门 |
| `transfer_prefix` | arm 时算定的搬运源 `{bucket}/{prefix}`，派发时直接用，**不重算** |
| `transfer_bytes` | arm 时的 `SUM(shards.total_bytes)`，验证时的分母 |
| `transfer_armed_at` | arm 时间戳，FIFO 排队 + "ready 卡住"检测 |
| `transfer_verified_bytes` | 验证实测的 JuiceFS 字节，让 `short` 可查询而不是只活在 error 字符串里 |

分母**只能**是 `SUM(shards.total_bytes)`：`size_gb` 实测 3 准 9 短，
`resume_skipped_gb` 跨轮次重复累加（RoboDojo 715.7 vs 5518.2 GB）。

## 5. 三个阶段

### 阶段一 — 手动脚本硬化（不碰 web、不碰 schema）

- 新建 `dlm/transfer/targets.py`：**BOS 源前缀 + 地瓜云目的路径的唯一映射**。
  BOS 侧复用 `bos_target()`（`dlm/core/bos.py:15-30`，桶的唯一判据）；地瓜云侧
  在这里定。手动脚本、自动派发、对账脚本共用它，杜绝"三处各写一份"。
- 例外规则：manifest 项里的显式 `src` **优先于**推导出的前缀。gen1 数据集就靠
  这条活着——`datasets/DL3DV-ALL-4K/` 的 category 是 `multimodal`，源前缀和推导
  结果**本来就不同**，不是错误。
- `transfer_import.py`：
  - 桶/类型感知：manifest 加可选 `type`（`dataset`|`model`，缺省 dataset）；
    `bos_stats`/`bos_top_children`/`import_from_bos` 全部改成按 plan 取桶。
  - 发 import 前先查在飞任务：远端 `list_async_tasks` 返回 `source`/`target`
    （已实证），同 (source, target) 且状态非终态 → **复用它的 task_id 继续轮询**，
    不发新 import。state 里已有 `dcloud_task_id` 的项同样先查状态。
  - 单项失败只跳过，不停整轮；但**连续 2 次 import 调用失败就停整轮**（那是系统性
    故障，重复 9 次没有意义）。docstring 的安全契约同步改写——不留说谎的注释。

### 阶段二 — 自动 arm（只堆队列，不搬一个字节）

- 新列 + `maybe_arm_transfer(task_id)`，在 `/api/task-progress` 的 `done` 分支、
  `complete_task()` **之后**调用（`routes/servers.py:207`）。**不挂 `complete_task()`
  本身**，也不挂 reconciler 的推断 done——后者正是 `t-20260805-460d45` 假完成的
  来源。`completed_at` 区分不了两者，所以触发必须是结构性的。
- 4 道硬闸门（任一不过 → 不 arm）：
  1. `transfer_status IS NULL`
  2. 任务不是 paused
  3. 前缀未漂移：`bos_target(row) == row.dispatch_prefix`（`dispatch_prefix` 为
     NULL 时跳过这道，老任务没有这列）
  4. shard 行 ≥1 且全部 `done`
- 比例分档（`ratio = bos_bytes / transfer_bytes`）：≥0.95 → `ready`；
  0.50–0.95 → `ready` + WARNING；<0.50 → `blocked` + CRITICAL。
  依据：实测最紧的真实比例正好 1.0000，而已知假完成差 ~100 倍（0.0002–0.013）。
- arm **只写库**，不发任何外部 HTTP（在请求处理器里发 HTTP 会把 worker 的心跳
  上报拖慢）。
- 两个死按钮改成调同一个 arm（人工触发跳过闸门 1，允许对已有 `transfer_status`
  的任务重新排队）；`transfer_paused` 持久化。
- 清死代码：`dlm/transfer/tasks.py` 与 `dlm/queue/app.py:43,48` 的 Celery 路由
  条目（全仓只有这两个 route 引用它）。

### 阶段三 — 派发 + 验证 + 告警

- `_dispatch_ready_transfers()`：60s 一轮；`transfer_paused` 为真则跳过；
  在飞数 = `transfer_status IN ('transferring','verifying')`；
  可发额度 = `min(16 - 在飞, 4)`；按 `transfer_armed_at` FIFO 取 `ready` 行；
  发 import 前**先查远端在飞任务**（和阶段一同一套逻辑）；写
  `transferring` + `transfer_task_id`。
- `_poll_transfers()` 扩展：远端成功 → `verifying` → 三道验证 → `done` / `short`。
- 4 条告警（复用 `alerts.py check_alerts`）：`transfer_prefix_drift:{id}` CRITICAL、
  `transfer_blocked:{id}` CRITICAL、`transfer_short:{id}` WARNING、
  `transfer_failed:{id}` WARNING。

## 6. 验收标准

每阶段独立验收、独立回滚。

### 阶段一
- [ ] `dlm/transfer/targets.py` 对 7 条已知任务给出正确的 (bucket, prefix, target)：
      6 个模型 → `auwomo-model-open` + `/auwomo-model/{category}/{name}`；
      molmobot-data → `auwomo-data` + `other/molmobot-data/` →
      `/auwomo-datasets/raw-data/other/molmobot-data`。
- [ ] 显式 `src` 覆盖推导前缀：DL3DV 项仍解析出源 `datasets/DL3DV-ALL-4K/` 和目的
      `/auwomo-datasets/raw-data/multimodal/DL3DV-ALL-4K`。
- [ ] 单元测试：一项 poll 超时 → 该项记 `timeout_polling`、**下一项照跑**、退出码非 0。
- [ ] 单元测试：连续 2 次 import 调用失败 → 停整轮。
- [ ] 单元测试：远端已有同 (source,target) 的非终态任务 → **不发新 import**，复用其 task_id。
- [ ] `--manifest` 里带 `type: model` 的项，`import_from_bos` 收到的 `bos_bucket`
      是 `auwomo-model-open`。
- [ ] dry-run 对现有 S1 manifest（`/root/transfer_manifest-phase1b.json`）仍能跑通，
      已 verified 的项照旧跳过。

### 阶段二
- [ ] 新列 ALTER 幂等：对已有 `/data/dlm.db` 跑两次 `init_db()` 不报错、不丢数据。
- [ ] 单元测试：4 道闸门每道单独拦住（NULL/paused/前缀漂移/shard 未全 done）。
- [ ] 单元测试：三档比例分别产出 `ready` / `ready`+WARNING / `blocked`+CRITICAL。
- [ ] 单元测试：reconciler 推断的 done **不触发** arm（只有 `/api/task-progress` 会）。
- [ ] 单元测试：arm 不改写任务自己的 `status`。
- [ ] 单元测试：arm 过程中不发起任何外部 HTTP（mock 上断言零调用）。
- [ ] `POST /transfer/trigger` 与 `/{task_id}/retry` 不再 import Celery，返回后
      库里出现 `ready` 行。
- [ ] `transfer_paused` 写入后重启进程仍为真。
- [ ] `dlm/transfer/tasks.py` 删除后全套测试仍绿（证明真没人用）。

### 阶段三
- [ ] 单元测试：在飞 16 条时不再发新的；在飞 15 条时只发 1 条。
- [ ] 单元测试：每轮新发不超过 4 条，即使 ready 有 50 条。
- [ ] 单元测试：`transfer_paused` 为真 → 一条都不发。
- [ ] 单元测试：FIFO——`transfer_armed_at` 最早的先发。
- [ ] 单元测试：远端成功但 `jfs < bos` → `short`，**不是** `done`。
- [ ] 单元测试：scope 有多余子项 → `short` + 告警。
- [ ] 单元测试：4 条告警各自能被 `check_alerts` 产出。
- [ ] `tests/test_event_loop_safety.py` 仍绿（新 stage 必须走 `_blocking_stage`）。
- [ ] 全套测试（当前 754）仍绿。

### 端到端（molmobot-data，3.40 TB，`other/molmobot-data/`）
- [ ] 部署前先只读实测 BOS 该前缀的字节数与对象数。
- [ ] 手动触发 → `ready` → 派发器发出 import → `transferring` +
      `transfer_task_id` 落库 → 远端完成 → `verifying` → `done`。
- [ ] 网页能看到整条状态变化；BOS 侧字节数与对象数**前后一致**（搬运只读 BOS）。

## 7. 明确不做（YAGNI）

- 不做搬运的并行分片（单个任务内切块并行）——对方是服务端复制，我们无从控制。
- 不把手动脚本改成 ready 队列的 consumer——阶段三上线后它只用于补搬/对账。
- 不做容量预检——用户 2026-08-07 明确：地瓜云是中心云，存储余量按无限对待。
- 不动 `bos_path` 列的历史脏数据（实测存的是地瓜云目的路径）。搬运一律用
  `transfer_prefix`，从不读 `bos_path`。

## 8. 遗留（本轮不做，单独记账）

- **DL3DV-ALL-4K**：远端任务 2026-08-09 19:26 失败于
  `juicefs <FATAL>: failed to handle 2 objects`——跑了 5 天，死在 2 个对象上。
  重跑是安全的（旧任务已终态，不会撞并发），但要先查是哪 2 个对象（很可能是
  0 字节对象，和 RoboDojo 同类问题）。
- **`PhysicalAI-Robotics-Locomanipulation`**：`done` 但 0 条 shard 行 → 闸门 4 会
  拦住它，永远不会自动 arm。这是对的（不可验证就不搬），但需要一条
  `transfer_blocked` 之外的信号让人知道它被拦了。
