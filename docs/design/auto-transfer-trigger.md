# 下载完成 → 自动搬运（BOS → 地瓜云）触发器设计

**日期**: 2026-08-07
**状态**: 设计待批准，未实现
**背景任务**: #83

## 一句话

下载完成后，只在**唯一一条真完成路径**上、通过**BOS 真实字节校验**之后，把任务标成
`transfer_status='ready'`；由 scheduler 的搬运循环调用地瓜云自己的 import API 去拉。
下载状态和搬运状态完全解耦，搬运永不改任务本身的 `status`。

## 现状（调研结论）

搬运链路其实**只缺触发器**，其余都在：

| 部件 | 位置 | 状态 |
|---|---|---|
| 目的端 import API 封装 | `dlm/transfer/dcloud.py:107` `import_from_bos()` | 可用，服务端复制，我方只读 |
| 进度轮询 `transferring → done/failed` | `dlm/web/scheduler.py:130` `_poll_transfers()`，60s | 可用，只推进别人入队的行 |
| 手动触发 / 重试 / 暂停开关 | `dlm/web/routes/transfer.py` + `app.js:533` | 可用，前端已有暂停按钮 |
| DB 字段 | `tasks.transfer_status / transfer_task_id / transfer_error` | 已存在 |
| **自动触发** | 原 `dlm/transfer/tasks.py` Celery 任务 | **死了**（Celery 下载链路 2026-06-30 废弃） |

旧 Celery 版有两个必须抛弃的设计缺陷：
1. 它**直接改任务自己的 `status`**（`status="transferring"` → `status="done"`），
   所以 `_task_for_frontend` 至今还留着 `"transferring": "done"` 的映射补丁。
2. 它不做任何真实校验，任务说 done 就搬。

## 关键设计约束（来自实测，不是推测）

### 1. 只能挂在 workflow 自己上报完成的那条路径上

`complete_task()` 是两条路径的汇合点，**不能挂在它上面**：

- ✅ `dlm/web/routes/servers.py:168` `/api/task-progress` —— coordinator workflow 跑完自己上报 `done`
- ❌ `dlm/web/reconciler.py:101` —— 从 shard 行**推断** done，这就是 `t-20260805-460d45` 假完成事故的来源

挂 `complete_task()` 会让假完成也触发搬运。所以触发点写在 `/api/task-progress` 的
`done` 分支里，reconciler 推断出来的 done 永不自动搬运。

### 2. 校验的分母用 `SUM(shards.total_bytes)`，不用 `size_gb`，也不加 `resume_skipped_gb`

实测 12 个 done 任务，候选门槛 `bos_bytes >= size_gb` 的结果是 **pass=3 / short=9**，
不可用。原因两类：

- 4 行只是浮点四舍五入（1.4 vs 1.4，差 <0.1%）→ 需要容差，不能裸 `>=`
- 5 行在 `bos_target()` 前缀下是 **0.0 GB** → 前缀漂移（见下）

而 `SUM(shards.total_bytes)` 在所有新架构行上都精确等于 `size_gb`，且方向安全：
它只是**本轮派发**的字节数，续传任务 BOS 上的字节 ≥ 这个数，所以门槛是**偏保守**的，
只会拒真完成、不会放过假完成。

`resume_skipped_gb` 虽然有值，但**跨轮次累加、严重超计**，绝不能进分母：

| 任务 | `SUM(shards.total_bytes)` | `resume_skipped_gb` |
|---|---|---|
| RoboDojo | 715.7 GB | 5518.2 GB |
| AgiBotWorld-Beta-BJ | 13126.2 GB | 28610.3 GB |

加上它会让每个续传过的任务都被误拒。

### 2b. 校验的松紧：实测所有分片时代任务的 ratio 都 ≥ 1.0

`bos_bytes / SUM(shards.total_bytes)`，全部 7 个分片时代 done 任务：

| 任务 | sh_total | BOS 实际 | ratio |
|---|---|---|---|
| PhysicalAI-…-Kitchen-Demos | 325.1 GB | 325.1 GB | **1.0000** |
| RoboDojo | 715.7 GB | 6189.9 GB | 8.65 |
| OpenVid-1M | 5863.9 GB | 11548.5 GB | 1.97 |
| InternData-A1 | 6412.1 GB | 6546.8 GB | 1.021 |
| AgiBotWorld-Beta-BJ | 13126.2 GB | 43902.0 GB | 3.34 |
| Egocentric-100K | 19526.8 GB | 23657.8 GB | 1.21 |
| AgiBotWorld-Alpha | ~0 | 9000.8 GB | 分母 0，走非空兜底 |

最紧的是 1.0000，那一行是唯一从未续传过的任务 —— 续传过的任务前缀里积累了历史轮次
的字节，所以 ratio 远大于 1。**从未续传过的首次下载任务是唯一暴露在"源端报了字节数
但实际服务 0 字节"（RoboDojo depth 文件那类）风险下的形状**，也正是它 ratio 恰好卡在
1.0000。所以门槛不能贴着 1.0 设。

同时，已知的假完成形状是**差 100 倍，不是差 5%**（Sekai 60.49/9392.89、
DL3DV-ALL-4K 10.18/40885.96、SpatialVID 51.99/7141.28 —— ratio 0.0002~0.013，
注意这几个数来自行内自报字段而非 BOS 实测，只用来说明数量级）。
所以把门槛从 0.999 放松到 0.5 几乎不损失任何检出能力，却消除了误拒风险。

### 3. 前缀漂移的根因：三代命名规则，不是操作事故

不是"第一次没下完、第二次改条件又没下完搞乱了"。是**代码层面的命名规则换过三代**：

| 代 | 规则 | 产生者 |
|---|---|---|
| gen1 | `datasets/{name}/`（无 category） | 最早期 |
| gen2 | `{category}/{repo_id 最后一段}/` | `parser.py:71` `derive_bos_path()`，对齐老 `download.sh` 的 `DIR_NAME` |
| gen3（现行） | `{category}/{name}/` | `bos.py:15` `bos_target()` |

gen2 用 **repo 目录名**，gen3 用**任务展示名**。只要运营给任务起了个比 repo basename
更好看的 `name`，两者就不一致。5 个 0 字节案例全部命中，无残余：

| 任务 | repo_id | 实际位置 | 属于 |
|---|---|---|---|
| WebVid-10M | `TempoFunk/webvid-10M` | `multimodal/webvid-10M/` | gen2 ✓ |
| NAVSIM-RecogDrive | `owl10/ReCogDrive_Pretraining` | `driving-vqa/ReCogDrive_Pretraining/` | gen2 ✓ |
| GR00T-X-Embodiment-Sim | `nvidia/PhysicalAI-Robotics-GR00T-X-Embodiment-Sim` | 同名全长 | gen2 ✓ |
| Manipulation-Kitchen | `nvidia/PhysicalAI-Robotics-Manipulation-Kitchen` | 同名全长 | gen2 ✓ |
| ManiSkill2 | `haosulab/ManiSkill2` | `datasets/ManiSkill2/` | gen1 ✓ |

`derive_bos_path()` 现在**零调用者**，是化石代码。

**推论（重要）**：新任务不可能再产生这种漂移 —— 现在只有一套规则，且 `bos_path`
全仓只有新建路由一个写入点、创建时就等于 `bos_target()`。往后唯一的漂移途径是
**人工改了已有任务的 `name`/`category`**，而这正是前缀一致门槛要拦的。

唯一确实"有两份"的是 WebVid-10M：gen1 的 `datasets/WebVid-10M/` 9363 GB
和 gen2 的 `multimodal/webvid-10M/` 2.7 GB。哪份是要的需要你判断，我不动。

### 3b. 原始探测数据

5 个 done 任务在 `bos_target()` 前缀下 0 字节，实测**数据都在，只是前缀不同**：

| 任务 | `bos_target()` 现在算出的 | 数据实际所在 |
|---|---|---|
| WebVid-10M | `multimodal/WebVid-10M/` | `multimodal/webvid-10M/` 2.70 GB（另有旧 `datasets/WebVid-10M/` 9363 GB 副本） |
| NAVSIM-RecogDrive | `driving-vqa/NAVSIM-RecogDrive/` | `driving-vqa/ReCogDrive_Pretraining/` 3.53 GB |
| ManiSkill2 | `manipulation/ManiSkill2/` | `datasets/ManiSkill2/` 6.20 GB |
| GR00T-X-Embodiment-Sim | `manipulation/GR00T-X-Embodiment-Sim/` | `manipulation/PhysicalAI-Robotics-GR00T-X-Embodiment-Sim/` 102.4 GB |
| Manipulation-Kitchen | `manipulation/Manipulation-Kitchen/` | `manipulation/PhysicalAI-Robotics-Manipulation-Kitchen/` 11.16 GB |

**没有一个是假完成**，全是下载完之后 `name` 被改成展示名，而 `bos_target()` 用当前
`name` 重算前缀导致的。

全量 73 个 done 行：`bos_target()` 与行内 `bos_path` **一致 36 / 不一致 34 / 空 3**。
不一致的分两族，两族里可信的字段正好相反：

- **改名族**（多数，旧行）：`bos_path` 才是真的，`bos_target()` 漂移了
- **死 scheme 族**（`auwomo-datasets/raw-data/...`，5 行）：`bos_target()` 才是真的，
  `bos_path` 是我刚修掉的那个 bug 写进去的垃圾

结论：**在历史行上两个字段都不可单独信任**。但对**现在完成的任务**，`bos_target()`
按构造就是对的 —— 上传器、BOS 续传过滤器、`temporal_client.py` 建 `TaskInput` 用的都是
同一行的 `category` + `name`。而且 `bos_path` 现在**全仓只有一个写入点**
（`tasks.py:217`，我刚修的新建路由），创建时就等于 `bos_target()`，之后没人改。

所以：两者不一致 ⇒ 有人事后改了 `name`/`category` ⇒ **拒绝并报警**，不猜。

## 设计

### 触发（`/api/task-progress` 的 done 分支，`complete_task()` 之后）

```
workflow 上报 done
  → complete_task(task_id, "done")        # 不变
  → maybe_arm_transfer(task_id)           # 新增，非阻塞失败
```

`maybe_arm_transfer()` 全部门槛通过才写 `transfer_status='ready'`；任一门槛失败写
`transfer_status='blocked'` + `transfer_error`（哪一关、差多少字节）。**不静默跳过** ——
假完成会以 blocked 的形式出现在仪表盘现有的 transfer 列里。

### 门槛：3 条硬底线 + 1 条分档字节比

**硬底线（任一不过 → `blocked` + 告警，绝不搬）**

1. **未搬过**：`transfer_status IS NULL`。
   `complete_task()` 没有幂等保护，重复 done 上报不能入队两次。
2. **未暂停**：现有 `transfer_paused` 开关为假。
3. **前缀未被中途改动**：`bos_target(row)` == `row.dispatch_prefix`（新列）。
   不一致 → blocked + **critical 告警**，错误信息把两个前缀都写出来。
   `dispatch_prefix` 为 NULL（本功能上线前就已派发的存量行）→ **跳过这道检查**，
   由字节门槛兜底；不能凭空拒绝。

   > **不能用 `row.bos_path` 做这个比较**（原设计如此，是错的）。实测 4 个非终态
   > 任务，`bos_path` 有 4 种状态：RealOmin `NULL`、molmobot
   > `auwomo-datasets/raw-data/molmobot-data/`（死 scheme）、
   > RoboMIND-V2-Tienkung `manipulation/RoboMIND2.0-Tienkung/`（真漂移）、
   > sim 任务同死 scheme。用它当门槛会把**两个正在下载的任务全部拦死**。
   >
   > 权威是 `bos_target()`，实测确认：molmobot 的 `bos_path` 前缀在 BOS 上
   > **0 字节**，而 `bos_target()` 前缀有 **2570.3 GB**；RealOmin
   > `manipulation/RealOmin/` 有 **10140.5 GB** 而 `bos_path` 是 NULL。
   >
   > 所以真正权威的记录点是**派发时刻构造 TaskInput 的那段代码** —— 它决定了
   > 上传器用哪个前缀。写在 `temporal_client.start_task_download`（`:482`，
   > 按 `dispatch_mode` 分流到 sharded / pool 的唯一漏斗）里，
   > 一处覆盖两种模式、两条建任务入口。
4. **分片齐全**：shard 行数 ≥ 1，且**每一行** `status='done'`；且 `bos_bytes > 0`。
   0 行直接拒 —— `PhysicalAI-Robotics-Locomanipulation` 就是这个形状
   （done、0 个 shard 行、208.4/232.6 GB），正是假完成的样子。

   > **pool 模式同样适用，无需分叉**：pool 的 batch 行和 sharded 的 shard 行
   > 共用 `shards` 表（`snapshot.py:497` 注释明说），所以"每行 done"和
   > `SUM(shards.total_bytes)` 两个量在 pool 下语义不变。
   > `release_pool_batches` 把非 done 行退回 `pending`，恰好让这道门槛在
   > 取消/暂停后自然拒绝，不需要额外判断。

`SUM(shards.total_bytes) == 0` 时（续传把文件全过滤掉的合法情况，
`AgiBotWorld-Alpha` 就是 sh_total≈0 / BOS 9000.8 GB），字节比无意义，
由 `bos_bytes > 0` 单独兜底 —— 这是对的：续传过滤器已经逐文件证明了 BOS 上都有。

**字节比分三档**（`ratio = bos_bytes / SUM(shards.total_bytes)`）：

| ratio | 结果 | 告警 |
|---|---|---|
| ≥ 0.95 | `ready`，搬 | 无 |
| 0.50 ~ 0.95 | `ready`，**照搬** | warning，写明缺多少字节 |
| < 0.50 | `blocked`，不搬 | critical |

中间档照搬的理由：搬运对 BOS 只读、目的端 import 是 skip-same、可重跑，所以搬了
97% 之后补齐是廉价的；反过来卡住一个真完成的任务要人工介入，代价更高也更容易
让人不再信自动化。但**绝不静默** —— 中间档必然告警。

两个档位阈值做成可配置，默认 0.95 / 0.50。

每个完成的任务只做一次 BOS list 扫描，只读，不写不删。

### 入队 = 只写库，不在请求里调外部 HTTP

`maybe_arm_transfer()` **不**直接调地瓜云。在进度上报的处理函数里发慢速外部 HTTP 会
拖住 web，失败还会丢。它只写库：

```
transfer_status      = 'ready'
transfer_error       = NULL
transfer_prefix      = <校验通过的那个前缀>      # 新列
transfer_bytes       = <校验时读到的 BOS 字节>    # 新列
```

两个新列走 `snapshot.py:117` 已有的 additive `ALTER TABLE` 迁移模式。

**快照前缀是关键**：派发时用快照值，不重新推导。否则事后改名又能把搬运引到错前缀去。

### 派发（scheduler，60s 循环，`_poll_transfers()` 的兄弟）

新增 `_dispatch_ready_transfers()`：取 `transfer_status='ready'` 的行 →
`DCloudClient.import_from_bos()`（源前缀用快照值，**必须以 `/` 结尾**）→
写 `transfer_status='transferring'` + `transfer_task_id`。
之后现有的 `_poll_transfers()` 原样把 `transferring → done/failed` 推进，不改动。

目的端路径沿用 `dlm/transfer/tasks.py` 里已有的映射，不变：
- dataset: `/727a2f92-30c/auwomo-datasets/raw-data/{category}/{name}`
- model: `/727a2f92-30c/auwomo-model[/{category}]/{name}`

### 绝不碰任务自己的 `status`

只写 `transfer_status` / `transfer_task_id` / `transfer_error` / 两个新列。
下载态和搬运态正交 —— 这正是"解耦，只吃下载的完成状态"的意思。

## 明确不做的事

- **历史 done 行不自动补搬**。73 行里 34 行前缀有歧义，且两族里可信字段相反。
  任何补搬都是逐个人工决定，不进自动路径。
- **不删 BOS 上任何数据**，包括 `datasets/WebVid-10M/` 那个 9363 GB 的旧副本。
- **不自己写传输脚本**，字节复制全部在地瓜云服务端发生，我方只 `list_objects`。

## 告警（复用现有机制，零新增管道）

`dlm/web/alerts.py` `check_alerts(tasks, workers)` 已经做了去重、severity 分级、
写 `/data/dlm-alerts.log`、并通过 `/api/dashboard` 暴露，scheduler 每 10s 调一次。
只需按 `transfer_status` 加规则，读的就是它已经拿到的 tasks 列表：

| 条件 | severity | key |
|---|---|---|
| `transfer_status='blocked'` 且前缀不一致 | CRITICAL | `transfer_prefix_drift:{task_id}` |
| `transfer_status='blocked'` 其他原因 | CRITICAL | `transfer_blocked:{task_id}` |
| 字节比落在 0.50~0.95 中间档 | WARNING | `transfer_short:{task_id}` |
| `transfer_status='failed'` | WARNING | `transfer_failed:{task_id}` |

## 上线方式

`DLM_AUTO_TRANSFER=on|off`，默认 **on**（已确认）。

触发器只对**部署之后完成**的任务生效，历史 done 行一个都不碰。所以最先的真实样本是
molmobot-data（约 6h，8.4 TB）然后 RealOmin（约 10h，25.7 TB）。

一句风险说明：若门槛判断有误而误搬，代价是**目的端的存储和传输带宽**，
BOS 侧只读不受影响，也不涉及删数据。需要确认地瓜云侧有 ~34 TB 的余量。

## 附带必须一起改的两处

1. **`/api/queue/add` 补写 `bos_path`**。它是第二条建任务入口（CLAUDE.md 里
   记录的 curl 就是它），`task_meta` 里**完全没有 `bos_path`**，所以走这条路
   建的任务该字段为 NULL（RoboDojo / Manipulation-Kitchen-Demos /
   AgiBotWorld-Beta-BJ 三个 done 行的空 `bos_path` 就是这么来的，RealOmin 也是）。
   补成 `bos_target()` 派生，两条入口的命名规则在字段层面也真正统一。
2. **`dispatch_prefix` 列**进 `snapshot.init_db` 的 additive 迁移清单，
   由 `temporal_client.start_task_download`（sharded / pool 的唯一漏斗）
   在分流前写入。

## 与 pool 模式（#81）的关系

本功能与 pool 一起上线，两处交汇点都已确认是**模式无关**的，不需要为 pool 写第二套：

1. **触发点**：`PoolDownloadWorkflow` 的成功路径同样走
   `report_to_dashboard(task_id, "done", ...)`（`workflows.py:1145`），
   `ShardedDownloadWorkflow` 走的是同一个 activity（`workflows.py:868`），
   两者最终落在 `/api/task-progress` 的同一个 done 分支上。挂一次，两种模式都覆盖。
2. **齐全门槛**：见上文门槛 4 的 pool 注记 —— batch 行与 shard 行共表。

顺序上先合 pool、再叠这个功能，因为 `dispatch_prefix` 的写入点
（`start_task_download`）是 pool 分支才引入的函数。

## 待批准的开放问题

1. ~~`dryrun` 先观察还是直接 `on`~~ → 直接 `on`（已确认）
2. ~~前缀不一致要不要告警~~ → 要，走 `alerts.py` 现有机制（已确认）
3. `PhysicalAI-Robotics-Locomanipulation`（done、0 shard 行）要不要并入待查的
   假完成审计清单？
4. WebVid-10M 两份副本（gen1 9363 GB / gen2 2.7 GB）哪份是要的？**不动，等你判断。**
5. 地瓜云侧是否有 ~34 TB 余量接 molmobot + RealOmin？
