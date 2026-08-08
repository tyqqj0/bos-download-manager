# Pool 模式切换 —— 实施方案

**日期**: 2026-08-08
**状态**: 待评审（用户流程第 5 步"围着方案检查"）
**合同**: `docs/design/pool-cutover-requirements.md`（R1–R9 / A1–A13）。本方案的每一项都要能指回一条 R。
**代码基线**: 本地 `feat/architecture-upgrade` @ `12bcf77`；生产 S1 @ `1c5814a`

---

## 0. 结论先说

1. **现在直接跑 `deploy-workers.sh` 会把错的代码推到 16 台机器。** pool 的 17 个提交从未推送到 origin，S1 checkout 在 `hotfix/pipeline-integrity`。这是 B1。
2. **顺带会把一份含旧 BOS Key 的备份文件复制到 16 台机器。** 这是 B2。
3. **翻转默认值对存量的两个任务毫无作用**（实测：迁移把存量行写成字面量 `'sharded'`，不是 NULL）。这是 B3。
4. **手工改库里的 `dispatch_mode` 会让首次派发立刻失败**（旧 shard 行 → `PoolBatchMismatch`，非重试类）。唯一干净路径是 `/api/queue/reshard`。这是 B4。
5. **#83 自动搬运：推迟。** 理由见第 6 节 —— 它落在 pool 关键路径上，且有一个未回答的前置问题（目的端余量）。符合用户"如果说不好搞，先太多任务的话先不搞"。
6. **一个必须让你知道的姿态变化**：切到 pool 之后，**卡住的任务不再自愈**。见第 7 节 R-1。
7. **评审新发现，最严重的一条**：`reconciler` 有**第二条通往 `done`/`failed` 的路**，
   它不认识 pool。三处：(a) `reconciler.py:112-161` 的 `if not has_workflow:` 会从 shard 行
   推断出 `done` 并直接 `complete_task` —— 它**位于 pool 跳过分支（`:183`）之上**、
   不按 `dispatch_mode` 分流、且**没有陈旧宽限**，能绕过 `verify_missing_files`、绕过上限、
   绕过必发的 WARNING —— 正是 A6 写明的"唯一失败形态"；(b) `reclaim_orphaned_shards`
   的 `SHARD_ORPHAN_GRACE = 900s` **短于** `POOL_BATCH_RETRY.maximum_interval = 30min`，
   会把正在正常退避重试的健康批次写成 `failed`，再喂给 (a) 把任务判死。
   这**不是本次改动引入的，是 pool 今天就有的 bug** —— 所以它是切换本身的前置修复，
   新增 **T7**。

---

## 1. 阻塞项（Phase 0 必须全清，之后才允许碰生产）

| # | 阻塞 | 证据 | 处置 |
|---|---|---|---|
| **B1** | pool 代码未推送；S1 在错分支 | 本地 `ahead 17`，origin tip `18089fa`，本地 HEAD `12bcf77`；S1 `git status -sb` → `## hotfix/pipeline-integrity` @ `1c5814a`，S1 本地 `feat/architecture-upgrade` 指针停在无关的 `871e2d6` | 推送本地分支 → S1 `git fetch` → checkout 到 `origin/feat/architecture-upgrade` → 用 md5 manifest 确认 S1 上的 `dlm/**/*.py` 等于本地 |
| **B2** | `.env.bak-20260807-173836`（含轮换前的旧 Key）会被 rsync 到 16 台 | `deploy-workers.sh:140-144` 的排除表只有 `.git` / `__pycache__` / `*.pyc` / `.env` / `node_modules` —— **没有 `.env.bak*`** | 双保险：(a) 把 S1 上的备份移出 `$REPO_DIR` 到 `/root/env-backups/`；(b) 给 rsync 加 `--exclude '.env.bak*'`（代码改动 T6） |
| **B3** | 翻转默认值不影响存量行 | 本地 sqlite 实测：`ALTER TABLE tasks ADD COLUMN dispatch_mode TEXT DEFAULT 'sharded'` 后两行存量行均读出 `'sharded'`，`IS NULL` 命中 0 行 | 显式回填（T6），且 `WHERE status IN ('pending','paused','failed')` —— **绝不触碰 `downloading`** |
| **B4** | 手改 `dispatch_mode` → 首次派发即 `PoolBatchMismatch` | `create_pool_batches`（`queue.py:636-751`）比对既有 shard 行与算出的批次集；两个任务的 `shards` 表各留着旧 sharded 行 | 重派一律走 `POST /api/queue/reshard {"task_id":…,"dispatch_mode":"pool"}` —— 它是唯一执行 `DELETE FROM shards WHERE task_id=?` 的路径（`queue.py:1222`），并同时置 `status='pending'` / `server=NULL` |

**B1 的一个附带事实**：16 台 worker 上没有 `.git`（rsync 排除了），所以"每台 `git rev-parse` 相同"这个验收方式**不可执行**。A2 的判定方式必须改成部署脚本自己的 md5 manifest（`find dlm -name "*.py" | LC_ALL=C sort | cat | md5sum`）。当前 17 台一致于 `3a6ba1c680ea7aa8be828c38104625c0` —— 那是**旧代码**的一致，不是目标。

---

## 2. 代码改动清单

### T1 —— 缺件持久化存储 + 查询接口（R4 / A6）

**文件**: `dlm/queue/snapshot.py`、`dlm/web/routes/servers.py`（上报入口）、`dlm/web/routes/tasks.py`（查询）

为什么不能复用 `events` 表：`servers.py:312-314` 每次收事件都执行
`DELETE FROM events WHERE timestamp < now-86400`。那是 24 小时滚动监控缓冲，
不是档案。任务跑 10 小时、失败后放两天，记录就没了。

新表（走 `snapshot.init_db` 已有的 additive 迁移模式，`CREATE TABLE IF NOT EXISTS`）：

```sql
CREATE TABLE IF NOT EXISTS missing_files (
    task_id     TEXT NOT NULL,
    file_path   TEXT NOT NULL,
    batch_index INTEGER,
    server      TEXT,
    reason      TEXT,
    size_bytes  INTEGER DEFAULT 0,
    attempts    INTEGER DEFAULT 1,
    first_seen  REAL,
    last_seen   REAL,
    PRIMARY KEY (task_id, file_path)
);
CREATE INDEX IF NOT EXISTS idx_missing_task ON missing_files(task_id);
```

主键选 `(task_id, file_path)` 而不是自增 id：同一个文件在多轮重试里会反复失败，
要的是"这个文件缺了、试了 N 次"，不是 N 行重复。upsert 时 `attempts = attempts + 1`、
刷新 `last_seen`、保留 `first_seen`。

**同时给 `tasks` 表加一列 `missing_files_count INTEGER DEFAULT 0`**（走 `snapshot.py:117-138`
已有的 additive 迁移清单）。这不是冗余：告警是**纯 DB 派生**的 —— `check_alerts(tasks, workers)`
（`alerts.py:92`）每 10s 被 `_build_dashboard` 调一次，喂给它的 `tasks` 来自
`get_all_tasks()`，而那是 `SELECT * FROM tasks`（`snapshot.py:276`，实测确认），
**只有任务列、永远没有 join 出来的计数**。所以"任务有缺件记录"这个条件在
`check_alerts` 拿到的数据里根本算不出来。加一列，`SELECT *` 免费带过去，
每个 tick 零额外查询。（备选是在 `check_alerts` 里做一次
`SELECT task_id, COUNT(*) FROM missing_files GROUP BY task_id`；
`alerts.py:84-89` 的 `_pool_task_holds_no_work` 已有"自己读一次 SQLite、
读失败就闭嘴"的先例。**绝不能**逐任务 `count_missing_files(task_id)` ——
`get_all_tasks()` 返回全部历史任务，那是每 10s 跑 O(全部任务) 次查询。）

- `snapshot.record_missing_files(task_id, rows)` —— 批量 upsert，并同步 `tasks.missing_files_count`
- `snapshot.list_missing_files(task_id)` / `count_missing_files(task_id)`
- `snapshot.clear_missing_files(task_id, paths)` —— 供 T4 的复核用，同步重算计数
- `POST /api/missing-files` —— worker 侧 activity 上报（body: `task_id`, `batch_index`, `server`, `files: [{path, reason, size_bytes}]`）
  - **必须带终态守卫**：`/api/task-progress` 拒绝为终态任务收报告（`servers.py:184`
    的 `TERMINAL_STATUSES` 判断），`/api/shards/*`、`/api/pool/batches/create` 都有等价守卫。
    没有它，一个被 revoke 掉的任务的僵尸 activity 还能往库里塞行。
  - **必须给 `files` 长度设上限**（单批次最多 500，取个宽松的硬顶如 1000 即可）。
  - 不加鉴权 —— 全部 worker→S1 端点都没有鉴权，这里单独加会破一致性；这是有意选择，不是遗漏。
- `GET /api/tasks/{id}/missing-files` —— 人和仪表盘查
- `DELETE /api/tasks/{id}/missing-files`（body 带 `paths`）—— T4 的复核 activity 跑在 worker 上，
  **碰不到 SQLite**（`activities.py` 全文零 `dlm.queue.snapshot` 导入，实测确认；
  worker 侧一切状态都走 HTTP，见 `activities.py:633-637` 的 `_coordinator()`）。
  没有这个端点，`clear_missing_files` 就只是个协调器侧函数，复核 activity 无法删它验证过的行。

**唯一允许删这张表的地方是 `snapshot.delete_task`**（`snapshot.py:410-415`，
删任务时连带删,否则会留下指向不存在 id 的孤儿行）。
`complete_task`、`staging_gc`、`reconcile`、reshard、resume、retry、doctor 的任何清理路径
**都不得触碰这张表** —— 记录是这个数据集"缺哪几个文件"的唯一档案（R4 修订版的硬要求）。
实测已确认现有代码里没有任何路径会扫到它。

**测试**: upsert 幂等（同文件两次 → 1 行、attempts=2）；`first_seen` 不被覆盖；
清理只删指定 path；跨任务隔离；**`complete_task` 之后行仍在**；
`delete_task` 之后行被清掉；`missing_files_count` 与实际行数始终一致。

### T2 —— pipeline 记录失败文件的身份，不只是计数（R4 / A6）

**文件**: `dlm/temporal/pipeline.py`

现状：9 处 `stats.failed_files += 1`，其中 **6 处不发任何事件**（我逐处核过）：

| 行 | 场景 | 现状 |
|---|---|---|
| 477 | `_AccessDenied`（403 / gated） | 只写日志 |
| 482 | 其他下载异常 | 发事件 |
| **523** | 重试耗尽 —— **最常见的毒文件路径** | 只写日志 |
| **544** | `gather` 兜底的未处理异常 | 只写日志，**且不知道是哪个文件** |
| 782 | 上传被取消 | 只写日志 |
| 787 | 上传异常 | 只写日志 |
| **804** | 待上传文件不存在 | 只写日志 |
| 825 | 上传重试耗尽 | 发事件 |
| 866 | 上传兜底 | 发事件 |

改动：
1. `PipelineStats` 增加 `failed_details: list[dict]`（`{path, reason, size_bytes}`）。
   **所有 9 处**都往里 append，不只是 6 处 —— 统一一条路径，别留两套语义。
2. **544 号点的身份缺失必须修**。当前 `results = await asyncio.gather(*tasks, ...)`
   之后只有异常对象。`tasks` 是按 `files` 顺序 `create_task` 出来的，所以改成
   `for f, r in zip(files, results)` 即可拿回身份。这是 T2 里唯一有实质风险的一处，
   要有专门的测试：构造一个抛 `BaseException` 的下载，断言 `failed_details` 里有它的路径。
3. `reason` 用短分类字符串（`access_denied` / `download_retries_exhausted` /
   `unhandled_download_error` / `upload_cancelled` / `upload_failed` /
   `staged_file_missing` / `upload_retries_exhausted`），不塞完整异常文本 ——
   异常文本里可能带 KB 级 CDN URL（项目已有的踩坑），入库会污染。

**不变量**: `len(stats.failed_details) == stats.failed_files`。加一条断言级测试。

> **实施更正（2026-08-08）**：上表有两行标错了，逐行核对源码后发现失败点是 **9 个但分类要 9 个**，
> 不是原文列的 7 个 reason：
>
> | 行 | 原文写的 | 实际是 | 新增 reason |
> |---|---|---|---|
> | 482 | "其他下载异常" | `_UpstreamEmpty` —— 源端声明了 size 却传 0 字节 | `upstream_empty` |
> | 825 | "上传重试耗尽" | 上传前 size 校验不符（866 才是重试耗尽） | `size_mismatch` |
>
> 这两个都是**源端永久损坏**的形态（ModelScope 把 RoboDojo 的 depth 当 0 字节发就是
> `upstream_empty`；RoboDojo 那 103 个 0 字节对象是 `size_mismatch` 拦下来的），
> 所以两者都必须**进** T3 的 `ARCHIVABLE_FAIL_REASONS` —— 它们恰恰是这张档案最该记的东西。
> 漏掉会让最典型的缺件类型静默，直接违反 R4。
>
> 注意与 R4 已知天花板的区别：**列表阶段**就报 0 字节的文件进不了 filelist（那是天花板）；
> 这两类是**列表说有、传输时才发现没有**，它们在 filelist 里，能记也必须记。
>
> 分类常量放 `dlm/temporal/models.py` 的 `FAIL_*`，而不是散落的字面量 ——
> `activities.py` 要按子集过滤，两边各写一遍字符串会让一个拼写错误静默地漏记或误记。
> 另外 `_count_upload_task_failures` 的两处（782/787）拿不到文件身份（参数是 Task 不是
> FileInfo），加了一个 `owners: dict[Task, FileInfo]`，计数时 pop，所以不随批次增长。

### T3 —— 批次容忍阈值（R4 / A7）

**文件**: `dlm/temporal/activities.py`（`run_pool_batch`，约 `:1272`）、`dlm/web/fleet.py`（常量）

现状：`if stats.failed_files > 0: raise` —— 和 sharded 一样全有全无。

**关键设计决定：容忍只在"最后一次机会"上生效，绝不牺牲重试次数。**

pool 的重试结构是 `POOL_BATCH_RETRY` 3 次尝试（`workflows.py:909-915`）
× 1 轮重派（`workflows.py:1100`）= 最多 6 次。如果容忍在第 1 轮第 3 次尝试就放行，
就白扔了第 2 轮"换台机器可能就好了"的价值 —— 而毒 worker 恰好是第 2 轮要治的病。

所以 activity 收一个新入参 `tolerate_missing: bool`：

```python
if stats.failed_files > 0:
    # (1) 先无条件入账 —— 与容忍判定完全解耦。
    #     R4 修订版的硬要求"无论落在哪一档，缺件一律入库、一律可查"。
    #     每次尝试都上报，靠 (task_id, file_path) 主键幂等（attempts+1）。
    #     只上报"永久性"原因；瞬时/编排类原因不进档案（见下方 reason 白名单）。
    await _report_missing_files(task_input, batch_index, server_key,
                               [d for d in stats.failed_details
                                if d["reason"] in ARCHIVABLE_FAIL_REASONS])
    # (2) 再判容忍
    final_attempt = activity.info().attempt >= POOL_BATCH_MAX_ATTEMPTS
    tolerable = (tolerate_missing
                 and final_attempt
                 and stats.failed_files <= POOL_BATCH_FAIL_MAX)
    if not tolerable:
        raise RuntimeError(...)          # 行为与今天完全一致
    # 走到这里：最后一轮的最后一次尝试、失败数在上限内 —— 批次判过
```

**为什么入账必须无条件（这是评审抓出的我自己的缺陷）**：原稿把 `_report_missing_files`
放在 `tolerable` 分支里，于是第 1 轮的失败、以及超上限的批次**一个字都不记** ——
而这两种恰恰是最需要留证据的情形（任务最终 `failed` 时，"缺哪些文件"是唯一有用的信息）。
无条件上报 + 主键幂等，代价是同一文件在最多 6 次尝试里被 upsert 6 次，
每次一个 HTTP POST，量级是几十行，可忽略。

**reason 白名单（`ARCHIVABLE_FAIL_REASONS`）**：`pipeline.py:772-792` 的
`_count_upload_task_failures` 把**被取消的上传任务也计入 `failed_files`**
（实测确认：`if t.cancelled(): self.stats.failed_files += 1; continue`，
它的 docstring 明说取消必须计数而不能致命）。所以 `failed_files > 0` 有可能
**全部由取消构成** —— 那是编排行为，不是"源端这个文件坏了"。
入档案的只有：`access_denied` / `download_retries_exhausted` /
`unhandled_download_error` / `upload_failed` / `upload_retries_exhausted`。
**排除**：`upload_cancelled`（取消）、`staged_file_missing`（本地 staging 状态，
下一轮会重下）。这两类仍然参与 `failed_files` 计数与容忍判定（宁可严），
只是不污染"这个数据集缺哪几个文件"的档案。

**一个已知的天花板，必须写明白**：源端报 0 字节的文件**根本进不到这里**。
`activities.py:56-57`（HF）要求 `item.size` 为真、`:83`（ModelScope）要求 `size > 0`，
所以 0 字节文件在列表阶段就被丢掉，永远不会出现在 filelist、更不会进 `missing_files`。
RoboDojo 的 depth 文件正是这个形状。**结论：`missing_files` 为空不等于"什么都没缺"** ——
它记的是"进了 filelist 却没传上去的文件"。这个边界要写进 `GET` 接口的文档字符串，
别让人把它当完备清单。（补齐"源端 0 字节"的审计是独立工作，不进本次范围。）

- `POOL_BATCH_MAX_ATTEMPTS` **今天不存在** —— `POOL_BATCH_RETRY`
  （`workflows.py:905-920`）里的 `maximum_attempts=3` 是个裸字面量。
  必须提成一个常量，让 `workflows.py` 的 `RetryPolicy` 与 `activities.py` 的
  `final_attempt` 判定读**同一个**值。两处各写一个 3，改一处就静默错位：
  调大 retry 却没同步，容忍会在倒数第二次就放行；调小则永不放行。
  常量放 `dlm/temporal/models.py`（workflow 与 activity 都已导入它，
  且 `activities.py` 不得 import `workflows.py`）。
- `POOL_BATCH_FAIL_MAX`，默认 **5**，`DLM_POOL_BATCH_FAIL_MAX` 可覆盖。
  **运维规则：改它必须 17 台一起改，且与 S1 保持一致**（硬约束 3 的同类要求）。
  原因不只是一致性洁癖：S1 侧的缺件扫描上限 `MISSING_VERIFY_MAX` 默认按
  `POOL_MAX_BATCHES × POOL_BATCH_FAIL_MAX` 推导（= 一个仍有资格报 `done` 的任务
  最多能带多少缺件）。若某台 worker 把它调高而 S1 没跟上，
  那台产出的档案就可能超过 S1 的扫描上限，被整档跳过复核 —— 表现为
  "缺件记录在库里但从没被 BOS 复核过"，而不是一个响的错误。
- **为什么是绝对值而不是比例**：我们要放过的是"源端永久坏文件"（ModelScope 把
  RoboDojo 的 depth 文件当 0 字节发那类），要拦住的是"系统性故障"（网断、盘满、
  鉴权挂）。系统性故障在 500 文件的批次里会挂掉远超 5 个。比例阈值反而会误伤
  小批次（3 个文件挂 1 个 = 33%，但那仍然只是 1 个坏文件）。
- **上限存在的意义**：不能让阈值退化成"永远不失败"（R4 明确要求）。
  整批全挂时 `failed_files` 远大于 5，照样 raise。

**测试**: `tolerate_missing=False` 时行为与今天逐字一致（回归保护）；
`tolerate_missing=True` 但非最后一次尝试 → 仍然 raise；最后一次尝试且 ≤5 → 放行且上报；
最后一次尝试但 >5 → raise；阈值可通过环境变量调整。

#### 实施记录（2026-08-08，已完成）

`dlm/temporal/models.py`（常量）+ `workflows.py`（`POOL_BATCH_RETRY` 读同一常量）+
`activities.py`（`_report_missing_files` + `tolerate_missing` + 先入账后判定）；
测试追加在 `tests/test_run_pool_batch.py` 尾部（+17 个，全套 529 → 546 passed）。
测试写在既有文件里而不是新建一个，是因为容忍逻辑要用它的整套 harness
（`pool_batch_env` fixture、假 `PipelineEngine`、`ActivityEnvironment` 驱动），
跨测试文件 import fixture 能跑但很脆。

**偏离 1：容忍条件从 3 个加到 5 个**（`recorded` 与 `fully_archivable`）。
计划稿的 `tolerable` 只看 `tolerate_missing and final_attempt and <= 上限`，
留了一个洞：`failed_files` 可以**整批由取消构成**（白名单一节自己论证过这点），
那种批次的 `archivable` 是空的、`_report_missing_files` 什么都不发，
于是"放行 + 无任何记录"—— 比今天的响亮失败更差，也直接违背 R4 修订版
"一律入库、一律可查"。所以：
- `recorded`：入账 POST 没落地（HTTP 失败 / 返回 `error` / 返回 `ignored`）就不放行。
  代价是协调端抽风时我们失去"宽恕"，而不是失去"记录"。
- `fully_archivable`：`len(archivable) == failed_files`，即每一个失败都是刚写进档案的
  文件级事实。附带覆盖了另一种漂移 —— 将来有新的失败路径只加计数不写
  `failed_details`（违反 `PipelineStats` 的不变式），批次会失败而不是被一份
  不完整的档案糊过去。

两条都与计划稿的"宁可严"一致，方向上只会更严，不会放过更多。

**偏离 2：`POOL_BATCH_FAIL_MAX` 的环境变量测试用独立加载而非 `importlib.reload`**。
reload 会重建 `PipelineStats` 等 dataclass，持有旧引用的模块（包括这个测试文件本身）
就不再和 activity 类型检查的是同一个类。改为用 `spec_from_file_location` 把
`models.py` 作为一次性模块加载（必须先 `sys.modules` 注册，否则 `@dataclass`
解析注解时拿到 `None`）。顺带把"负值 / 拼错的值在 import 期就 ValueError"也钉住了。

**验证方式**：五个条件逐个做了变异测试（删掉 `final_attempt` → 3 红；删掉 `recorded`
→ 3 红；删掉 `fully_archivable` → 3 红；把上限改成 `10**9` → 1 红），
确认每个条件都真有测试挡着；`activities.py` 变异后已按字节还原。
另加一个漂移守卫断言 `POOL_BATCH_RETRY.maximum_attempts == POOL_BATCH_MAX_ATTEMPTS`
—— 这正是本节担心的"两处各写一个 3"。

### T4 —— workflow 侧接线 + 诚实收尾（R4 / A6）

**文件**: `dlm/temporal/workflows.py`（`PoolDownloadWorkflow`）、`dlm/temporal/activities.py`（新 activity）

1. `_run_window_loop(..., tolerate_missing: bool)`：第 1 轮传 `False`，
   第 6 步的重派轮（`workflows.py:1100` 附近）传 `True`。
   **确定性**：这个值由 workflow 自己的循环状态推出，不读时钟不读随机数，replay 安全。
2. 新 activity `verify_missing_files(task_input: TaskInput) -> int`：对 `missing_files` 里记的每个
   path 做一次 BOS HEAD，**确认存在且大小一致的才删行**，返回剩余数。
   - 为什么要复核：缺件是稀有事件（几十个，不是几百万），逐个 HEAD 很便宜；
     而 BOS 是唯一真相 —— 某个文件可能在后一轮被别的批次捡起来传上去了。
   - **必须比 key + size，不能只比存在性。** 两个续传过滤器都是这么比的
     （`activities.py:210` 的 `existing.get(path) == f.get("size")`、
     `activities.py:1047-1054` 的 `existing_size == f.get("size")`，
     后者的 size 取自 `meta.metadata.content_length`）。只比存在性会把
     "上传中断留下的截断对象"判成完好，从而删掉一条真实的缺件记录 ——
     那是 A6 唯一失败形态的另一条通路。
   - **入参必须是 `TaskInput` 而不是 `task_id`**：算前缀要 `bos_target()`，
     它读的是 `type`/`name`/`category`（`bos.py:15-30`）。
     另外 `bos.py:29` 对 falsy `category` 会静默少一层路径 ——
     所以 `category` 为空时 `verify` 必须**直接跳过复核并保留全部行**，
     绝不能拿一个可能错的前缀去 HEAD、然后按"没找到"留行（那倒是安全的）
     或按"找到了"删行（危险）。
   - **`size_bytes <= 0` 的行、以及任何 HEAD 抛异常的行，一律保留。**
     方向必须和两个续传过滤器一致：出错就朝"还缺着"倒（fail open toward 缺件）。
   - 复核结果通过新的 `DELETE /api/tasks/{id}/missing-files` 落库
     （activity 跑在 worker 上，碰不到 SQLite —— 见 T1）。
   - **必须注册进 `dlm/temporal/__main__.py` 的 `ACTIVITIES` 列表**，
     否则 workflow 调它会挂在 `ActivityNotRegistered` 上，且只有 pool
     任务收尾那一刻才暴露 —— 加一条断言"新 activity 在注册表里"的测试。
   - 为什么不在成功上传时逐文件 DELETE：那是每个文件一次写库，百万级文件下不可接受。
3. 收尾判定（`workflows.py:1141` 附近的 `_fail` / 报 done 之前）。
   **语义按 2026-08-08 的需求修订**：少量缺件照报 `done`、照搬，诚实性由记录+告警承担。

   **实现上必须收成一个 `_finalize()` 私有方法。** `PoolDownloadWorkflow` 里
   通往 `report_to_dashboard(..., "done", ...)` 的分支不止一条（空 filelist、
   全部被续传过滤掉、正常跑完、重派后无失败批次）。逐个分支各写一遍复核+判档，
   一定会漏 —— 漏掉的那条就是"报了 done 但没告警"。所有 done 路径都走
   `_finalize()`，它内部先 `verify_missing_files` 再判档再上报。

| 重派后失败批次 | 复核后剩余缺件 N | 上报 | 告警 |
|---|---|---|---|
| 0 | 0 | `done` | 无 |
| 0 | 0 < N ≤ 上限 | **`done`** | **WARNING 必发**，含 N 与可查接口 |
| 0 | N > 上限 | `failed`，信息含 N | CRITICAL |
| M > 0 | 任意 | `failed`，信息含批次数与 N | CRITICAL |

   **上限** = `max(DLM_TASK_MISSING_ABS, 本轮总文件数 × DLM_TASK_MISSING_RATIO)`，
   默认 `max(10, 0.5%)`。
   - **分母取 `filelist_result["count"]`**（协调器列表阶段的总文件数，pool 已有此值），
     而不是"本轮派发的批次文件数之和" —— 后者被续传过滤器减过，
     一个 99% 已完成的续传任务分母会小到让 0.5% 退化成 0，比例档形同不存在。
     用 `max(1, count)` 兜底，防止空 filelist 时除零。
   - 上限必须存在：单批次容忍 5 个 × 最多 1500 个批次 = 累计 7500 个缺件，
     那不是"几个文件"。没有上限就等于把阈值做成"永远不失败"，R4 明文禁止。
   - 绝对下限 10 的作用：别让小任务被比例误伤（200 个文件的任务，0.5% = 1 个）。
   - **唯一的失败形态是"报了 `done` 但没告警、也查不到缺件"** —— 测试必须直接盯这个。

**测试**: 三种收尾组合各一条；`verify_missing_files` 在 BOS 已有**同大小**文件时清行、
大小不符时保留、HEAD 抛异常时保留；`_finalize` 的四条 done 路径各一条（含空 filelist
与全部被续传过滤掉这两条，断言它们也会发 WARNING）；新 activity 在 `ACTIVITIES` 里；
replay 测试全绿。

> **replay 夹具必须重录**：`_run_window_loop` 加入参、新增一个 activity 调用，
> 都会改变 workflow 的命令序列。`tests/fixtures/histories/pool-t-pool-1.json`
> 是按旧序列录的，改完必然 non-determinism 报错。
> 流程：改完代码 → 跑一个真 pool 任务（或本地 Temporal 上的最小任务）→
> 重新导出该 history → 确认 `tests/test_pool_workflow_replay.py` 绿。
> **sharded 的夹具一个都不许动** —— 那是回滚路径的保护网。

#### 实施记录（2026-08-08，已完成）

改动文件：
- `dlm/temporal/models.py`：`TASK_MISSING_ABS` / `TASK_MISSING_RATIO`（默认 10 / 0.005，
  env 可调，负值 import 期即报错）+ `task_missing_limit(listed_files)`。
- `dlm/queue/snapshot.py`：`tasks.missing_files_limit INTEGER DEFAULT 0` 迁移 + `set_missing_limit()`。
- `dlm/web/routes/tasks.py`：`POST /api/tasks/{id}/missing-limit`。
- `dlm/temporal/activities.py`：`verify_missing_files(task_input, limit)`。
- `dlm/temporal/__main__.py`：进 import 列表与 `ACTIVITIES`。
- `dlm/temporal/workflows.py`：`_finalize()` + 四条终态路径全部改走它；
  `_run_window_loop(..., tolerate_missing)` 第 5 个**必填**位置参数。
- 测试：`tests/test_missing_files.py` +15、`tests/test_pool_workflow_replay.py` +10，
  夹具 `pool-t-pool-1.json` 重录；全套 557 → 575 passed。

偏离方案的地方（4 处，都是实现时才看清的）：

1. **`missing_files_limit` 列 + 写入路径落在 T4，不是 T5。** 方案把这列排给了 T5 的迁移，
   但只有 `_finalize` 拿得到列表阶段的总文件数，也只有它算得出上限；等到 T5 告警去读任务行时
   那个分母早就没了。所以列、`set_missing_limit()`、`POST /missing-limit` 一起在这里落地，
   T5 只剩纯告警逻辑。
2. **activity 签名是 `(task_input, limit)`，顺手把上限落库。** 本来是 `(task_input)`。
   多一个 HTTP 往返不值得，而复核这一步天然就是"任务收尾"的唯一时刻。
   代价：`limit` 的取值判断留在 workflow（确定性安全），activity 只负责写。
3. **`_finalize` 连 `failed_batches` 那一档也一起判。** 否则失败路径不会复核、不会写上限，
   T5 的 CRITICAL 规则（`count > limit > 0`）对"重派后仍失败"的任务永远触发不了 ——
   而那恰恰是最需要说清丢了什么的一档。
4. **类别为空的跳过规则要放过 model 任务。** 方案只说"`category` 为空就跳过"，
   但 `bos.py:25` 对 `type == "model"` 直接返回 `{name}/`，根本不看 `category` ——
   照方案写会让所有模型任务的缺件行永远清不掉。守卫改成
   `type == "model" or bool(category)`。

另外一处方案没规定、需要记下的判断：**`verify_missing_files` 自己失败 ⇒ 报 `failed`，
不报 `done`。** 复核挂了就没有任何判档依据，而档案在 S1 的 SQLite 里、worker 侧没有兜底副本；
此时报 `done` 正是 T4 要消灭的那种静默。`failed` 的代价是一次续传重试
（BOS 过滤器会跳过已传文件），不是数据。

验证方式（不是"绿了就算"）：对 5 个判定条件逐个做变异测试，确认各有测试挂掉 ——
去掉 size 比较（退化成只比存在性）→ 2 条挂；去掉 model 豁免 → 1 条挂；
放行 `size_bytes <= 0` 的行 → 1 条挂；空档案时不写上限 → 3 条挂；
以及 workflow 侧 `tolerate_missing` 与上限判档各自的用例。改完 `activities.py`
用 `diff -q` 确认字节级还原。夹具重录复用测试模块自己的 `_PoolActivityStubs`
（4 批次、其中 1 个首轮失败），录制脚本是一次性的、没有入库 —— 夹具因此不可能和 harness 漂移。

### T5 —— 告警与仪表盘暴露（R4 / R8）

**文件**: `dlm/web/alerts.py`、`dlm/web/routes/doctor.py`

复用 `check_alerts` 现有的去重/分级/落盘/`/api/dashboard` 暴露管道，零新增通道：

| 条件 | severity | key |
|---|---|---|
| 任务已终态（`done`/`failed`）且 `missing_files_count > 0` | WARNING | `missing_files:{task_id}` |
| `missing_files_count` 超过该任务的收尾上限 | CRITICAL | `missing_files_many:{task_id}` |

**CRITICAL 的阈值必须和 T4 收尾判定用的是同一个上限**，不能像原稿那样写死 100。
写死会造出一个荒谬的窗口：一个 300 文件的任务缺了 50 个（远超 `max(10, 0.5%)=10`，
T4 已判 `failed`），却因为 50 < 100 只发 WARNING。反过来一个 500 万文件的任务
（上限 25000）缺 120 个，T4 认为完全正常，告警却喊 CRITICAL。
`check_alerts` 拿不到 filelist 总数，所以**上限值必须在收尾时由 workflow 一并落库**：
`tasks` 表再加一列 `missing_files_limit INTEGER DEFAULT 0`，和
`missing_files_count` 同一次 additive 迁移。`SELECT *` 免费带给 `check_alerts`，
比较就是 `count > limit and limit > 0`（`limit=0` 表示"这任务还没收尾"⇒ 不评 CRITICAL）。

> **这一列已经在 T4 落地了**（列、`set_missing_limit()`、`POST /missing-limit`、
> 收尾时的写入），原因见 T4 的实施记录偏离 1。T5 只剩上表两条告警规则本身。

同时把 `pool_orphaned` / `pool_starved`（`reconciler.py` 的 T9 patrol 已产出）
确认能到 `/api/dashboard` —— 见第 7 节 R-1，这两个告警是 pool 唯一的"卡住"信号。

#### 实施记录（2026-08-08，已完成）

改动文件：
- `dlm/web/fleet.py`：`FINALIZED_STATUSES = ("done", "failed")`（第三个状态集合）。
- `dlm/web/alerts.py`：`missing_files`（WARNING）/ `missing_files_many`（CRITICAL）两条规则
  + `MISSING_ALERT_WINDOW_S` + `_finalized_within()`。
- `dlm/web/routes/doctor.py`：`missing_files` 段（**不计入 `total_issues`**）。
- 测试：`tests/test_missing_files.py` +24（合计 70）；全套 575 → 599 passed。

偏离/补充方案的地方（3 处）：

1. **门槛用新的 `FINALIZED_STATUSES`，不是 `TERMINAL_STATUSES`。** 方案表格写的是
   "已终态（done/failed）"，但代码里现成的 `TERMINAL_STATUSES` 还含
   `paused`/`preempted`/`revoked`/`skipped` —— 前两个是本项目的**可续传**状态，
   它们的缺件行大概率下一轮就补上；后两个根本没人要那些文件。四者都没走过
   `_finalize`，也就没有上限可判。`fleet.py` 里本来已经因为
   "TERMINAL vs GC_REMOVABLE 混用过一次造成数据丢失"而并列两个集合，
   这是同一个理由的第三个集合。
2. **WARNING 有 7 天保质期，CRITICAL 没有。** 方案没提这件事，但缺件天生永久
   （上游死文件不会自己活过来），而永不 resolve 的告警会堆积 ——
   几个月后仪表盘的告警列表会以历史损失为主，5 分钟前掉线的那台 worker 埋在里面。
   那正是告警渠道失效的方式。所以**通知**有窗口、**记录**没有：
   记录在 `missing_files` 表、`/api/doctor` 的 `missing_files` 段、
   以及告警首次触发时已经写进 `/data/dlm-alerts.log` 的那行里，三处都不过期。
   `completed_at` 解析不出来时**保留**告警（歧义不能换来沉默）。
3. **`/api/doctor` 报但不计入 `total_issues`。** `healthy` 是"现在有没有活故障"
   这个判断 —— 仪表盘 doctor 弹窗、DLM skill 的健康分支、以及历次部署验收
   （如 2026-08-02 计划 A4「doctor healthy」）读的都是它。已经落定的损失是永久的，
   计进去等于让一个上游死文件把这个判断永久钉在红色，那它就什么都不再回答了。
   （**更正**：此处初稿写的是"`scripts/deploy-workers.sh` 的部署门（T10）"——
   **不成立**，该脚本 `grep -n doctor` 无命中，它的门是自己的 md5 manifest +
   pytest。`doctor.py` 里同样的错误注释与 `tests/test_missing_files.py`
   的 docstring 已一并改掉，见复审 GAP-4。）
   通知归告警引擎，这一段只负责"查得到"。

另外补了方案第 7 节 R-1 要求的确认：`check_alerts` → `summary["alerts"]` →
`/api/dashboard` 这最后一跳此前没有任何测试，现在钉住了 —— 覆盖的是全部告警类型，
不只是这两条。

验证方式：6 个变异逐一确认有测试挂掉 —— 门槛换成 `TERMINAL_STATUSES` → 4 条挂；
阈值写死 100 → 2 条挂；`limit == 0` 也参与判档 → 1 条挂；去掉保质期 → 1 条挂；
两条告警同时发 → 2 条挂；把缺件计进 `total_issues` → 1 条挂。
改完用 `diff -q` 确认 `alerts.py` / `doctor.py` 字节级还原。

### T6 —— 全局默认翻转 + 存量回填 + 部署脚本排除（R2 / A3、B2、B3）

**文件**: `dlm/web/fleet.py:32`、`scripts/deploy-workers.sh:143`、新脚本 `scripts/backfill_dispatch_mode.py`

1. `fleet.py:32` 的 fallback 字面量 `"sharded"` → `"pool"`：
   ```python
   DEFAULT_DISPATCH_MODE = os.environ.get("DLM_DEFAULT_DISPATCH_MODE", "pool")
   ```
   **为什么改代码而不是只设 systemd 环境变量**："全局默认"是代码层的陈述；
   环境变量在 git 里不可见，重装 systemd unit 就丢。回滚仍然可以只设
   `DLM_DEFAULT_DISPATCH_MODE=sharded`（不需要重新部署，见第 8 节）。
   `fleet.py:33-44` 已有的 import 期校验保留不动。
2. **`temporal_client.start_task_download:482` 与 `reconciler.py:557,637,658` 里
   硬编码的 `or "sharded"` 一律不动。** 那些是"这行没有模式 ⇒ 它是老行 ⇒ 按 sharded 跑"，
   语义不是"跟今天的默认值"。改了会让任何 NULL 行被静默拖进 pool。
3. `scripts/backfill_dispatch_mode.py`：
   ```sql
   UPDATE tasks SET dispatch_mode='pool'
    WHERE status IN ('pending','paused','failed')
      AND COALESCE(dispatch_mode,'') <> 'pool';
   ```
   - **`COALESCE` 不是装饰**。原稿写的是裸 `dispatch_mode <> 'pool'`，
     而 SQL 三值逻辑下 `NULL <> 'pool'` 求值为 **NULL 而非真** ——
     任何 `dispatch_mode` 为 NULL 的行会被**静默跳过**，正是最需要回填的那种行。
     B3 说的是"迁移把存量行写成字面量 `'sharded'`"，那对**迁移当时存在的行**成立；
     但 `storage.py:181` 这条建任务入口不传 `dispatch_mode`
     （实测确认：该文件全文零 `dispatch_mode`），所以"NULL 行不可能存在"是个
     不该赌的前提。用 `COALESCE` 之后，赌不赌都不影响正确性。
   - `--dry-run` 默认开，`--apply` 才写。
   - 执行前后各打一次 `downloading` 行的 `dispatch_mode` 快照（A3 要求"回填不得触碰 downloading"）。
   - 打印影响行数与 task id 列表。
4. `deploy-workers.sh` rsync 加 `--exclude '.env.bak*'`。
5. **`snapshot.py:201-205` 的写入侧默认（`UPDATE tasks SET dispatch_mode='sharded'
   WHERE id=? AND dispatch_mode IS NULL`）保持不动** —— 评审对这一条有分歧，已裁定：
   - 事实层面：它确实会在 caller 省略 `dispatch_mode` 时把行钉成 `'sharded'`，
     从而绕过 `DEFAULT_DISPATCH_MODE`。
   - 但**唯一省略它的 caller 是 `storage.py:181`**（实测：全仓 `upsert_task`
     的非测试调用只有 `queue.py:133`、`tasks.py:258`、`storage.py:181`，
     前两者都显式传 `dispatch_mode`）。而 `storage.py` 建的行
     `status="done"`、`max_workers=0`，是"登记 BOS 上已存在的数据"，**永不派发**。
     它的 `dispatch_mode` 是什么都无所谓。
   - 改它反而危险：把 NULL 留成 NULL，会让 `temporal_client.py:482` 与
     `reconciler.py:557,637,658` 那些 `or "sharded"` 兜底面对真 NULL 行 ——
     语义没变，但等于凭空多出一类没人测过的行。
   - **代之以一条锁死不变量的测试**：断言 `/api/queue/add` 与
     `POST /api/tasks`（`tasks.py:253`）两条**可派发**入口写出的行
     `dispatch_mode == DEFAULT_DISPATCH_MODE`，且该断言不写死 `'pool'` 字面量
     而是读常量。这样将来谁新增第三条可派发入口却忘了传这个键，测试会红。

**测试**: 回填脚本对 `downloading` 行零影响；`--dry-run` 不写库；
`tests/test_dispatch_mode_ui.py` 与 `tests/test_pool_dispatch.py` 在默认值翻转后仍全绿
（这两个套件断言的是"UI 读服务端默认值"而非字面量，预期不受影响 —— 若有断言写死
`'sharded'` 则该断言本身就是缺陷，一并修）。

#### 实施记录（2026-08-08，已完成）

`tests/test_default_dispatch_mode.py`（21 项）+ `scripts/backfill_dispatch_mode.py`，
全套件 529 passed（翻转前 508，翻转本身零回归，与本节预测一致）。三处偏离原稿：

1. **不变量测试比第 5 条的写法更强，因为按原稿写出来的测试是**没牙的**。**
   原稿说"断言两条入口写出的行 == `DEFAULT_DISPATCH_MODE`，且不写死 `'pool'`"。
   问题：`test_pool_dispatch.py:507` 那条既有测试正是这么写的 ——
   `monkeypatch.setattr(fleet, "DEFAULT_DISPATCH_MODE", "pool")` 然后断言行是 `pool`。
   它在默认值还是 `'sharded'` 的年代有意义；默认值翻成 pool 之后，
   **一条把 `"pool"` 硬编码进 dict 的路由也能让它全绿**。所以新测试对常量的
   **两个取值都参数化**（`["pool", "sharded"]`）：唯一能同时通过的实现是真的读常量。
   顺带这也是第 8 节回滚路径（`DLM_DEFAULT_DISPATCH_MODE=sharded`）的唯一测试覆盖。
2. **加了一条原稿没有的结构测试**，因为"将来谁新增第三条可派发入口却忘了传这个键，
   测试会红"这句话，行为测试**做不到** —— 没人写过的入口就没人写过它的测试。
   做法：AST 遍历 `dlm/web/**/*.py` 的每一处 `upsert_task` 调用，
   把实参名解析回它在同一路由里的 dict 字面量，检查有没有 `dispatch_mode` 键；
   解析不出字面量的（如从库里读回来的行）算"没写"，逼出显式豁免。
   豁免表 `EXEMPT_FROM_DISPATCH_MODE = {"register_bos_data", "batch_action"}`，
   每条附理由；另有一条测试断言豁免表里没有已不存在的路由名（防止过期许可
   将来悄悄覆盖一个同名的新函数）。
   **不用"在路由源码里 grep `dispatch_mode` 字符串"**：这三条路由都会为了校验/回显
   多次提到这个词，真正持久化的那一行被删掉之后 grep 版本照样绿。
   已做变异验证：删掉 `queue.py` 的 `"dispatch_mode": dispatch_mode,` 一行，该测试转红。
3. **`batch_action`（`tasks.py:487`，retry 路径）判定为豁免，而非缺陷。**
   它把**已存在的行**整个 dict 回写，因此保留该任务原本跑的模式。这是对的：
   一个已经有 sharded 分片行、worker 上还有 sharded staging 的任务，
   重试时应当按它当初被切分的方式跑，而不是中途被静默改模式。
   它本来会遗留的那批存量行，由回填脚本覆盖 `failed`/`paused` 一次性解决。

另外顺手修了 `queue.py:76` 的过期 docstring（"grayscale period: sharded"）。

### T7 —— 让 reconciler 认识 pool（R4 / A6 / A12；**pool 今天就有的 bug**）

**文件**: `dlm/web/reconciler.py`

这一项不是本次新功能的配套，而是 **pool 模式今天已经带着的缺陷**。
不修就切，两个真任务会被控制面自己判死或假判完成。

**(a) 从 shard 行推断 done/failed 的分支必须按模式分流。**
`reconciler.py:112-161` 的 `if not has_workflow:` 块：所有 shard 行 `done`
⇒ `complete_task(task_id, "done")`；全部终态且有真失败 ⇒ `complete_task(task_id, "failed")`。
三个事实叠起来才是问题：
- 它**位于** pool 跳过分支（`:183` 的 "decision C"）**之上**，所以那个 `continue` 拦不住它；
- 它**完全不看 `dispatch_mode`**；
- 它**没有陈旧宽限** —— 而下面的 pool 分支要求 `stale_seconds > DEAD_THRESHOLD` 才动手。

pool 的批次行和 sharded 的分片行**共用 `shards` 表**，所以窗口推进过程中
出现"此刻所有已建批次行都是 done"是完全正常的瞬时状态（下一窗口还没建行）。
撞上这个瞬间，reconciler 就会替 workflow 宣布 `done` ——
**绕过 `verify_missing_files`、绕过上限判档、绕过必发的 WARNING**。
这**就是 A6 写明的"报了 done 但没有告警"那唯一失败形态**，而且它的触发不需要任何缺件。

改法：在这个块的最前面加 `if (task.get("dispatch_mode") or "sharded") == "pool": continue`
（沿用仓里既有的 `or "sharded"` 兜底写法，不引入新语义）。
pool 任务的终态**只能由 `PoolDownloadWorkflow` 自己经 `/api/task-progress` 上报**。
代价是 pool 任务彻底不自愈 —— 那正是 R-1/A12 已经接受并写进验收的姿态。

**(b) `reclaim_orphaned_shards` 必须跳过 pool 行。**
`reconciler.py:426-505`：`SHARD_ORPHAN_GRACE = 900`（15 分钟）。
存活判定是 `if not shard_id or f"shard-{shard_id}" in running_ids: continue`
—— 找的是**子 workflow id**。而 pool 的批次是 **activity，不是子 workflow**，
所以这个存活测试对 pool 行**永远不成立**。接着 `if updated_at > cutoff: continue`
也拦不住：`POOL_BATCH_RETRY.maximum_interval` 是 **30 分钟**，
一个正在正常退避重试的健康批次，`updated_at` 静默超过 900s 是常态。
于是 `update_shard_progress(shard_id, status="failed", ...)` 把它写成 `failed` ——
再喂给 (a) 的"全部终态且有真失败 ⇒ `complete_task(failed)`"，任务当场判死。
全函数没有任何 `dispatch_mode` 过滤。

改法：取 shard 行对应任务的 `dispatch_mode`，pool 行直接跳过。
pool 的批次归属与释放由 `release_pool_batches` 负责，那是模式内自洽的路径。

**(c) 任何按模式保留下来的自动判死，都必须带陈旧窗口。**
(a)(b) 修掉之后 pool 侧已经没有自动判死路径了，这一条是给 sharded 的回归保护：
不要在修 (a) 时手滑把 sharded 的宽限也一起改掉。

**测试**（每条都要能在没有 Temporal 的情况下跑）：
- 一个 pool 任务、所有已建批次行 `done`、`has_workflow=False` ⇒ 任务**仍是 `downloading`**，
  `complete_task` 未被调用（这是本次最重要的一条断言）；
- 同形状但 `dispatch_mode='sharded'` ⇒ 行为与今天逐字一致（回归保护）；
- 一个 pool 批次行 `updated_at` 陈旧 20 分钟 ⇒ `reclaim_orphaned_shards` **不动它**；
- 同形状 sharded 行 ⇒ 仍被判 `failed`（回归保护）。

> **推翻既有测试（2026-08-08，实施时发现，须记录）**
>
> T7 (a) 与两条**已提交**的测试直接冲突，它们断言的正是 T7 要移除的行为：
> `tests/test_pool_observability.py::test_pool_task_with_all_done_batches_still_auto_completes`
> 和 `::test_pool_task_with_a_failed_batch_still_auto_fails`（提交 `79556d1`）。
> 名字里的 "still" 说明作者**注意到**了这条推断会作用于 pool 任务，并选择把它钉住
> 而不是报为缺陷 —— 所以这不是漏网，是一个当时的判断，推翻它必须留痕。
>
> 推翻的依据是需求文档而非我的偏好：A6 写明**"报了 done 但没有告警、也查不到缺件"
> 是该条唯一的失败形态**，而 auto-complete 这条路径产出的恰好就是这个形态 ——
> 它绕过协调器的缺件复核、绕过上限判定、绕过第二档必发的 WARNING，
> 三者 reconciler 一个都给不了（复核是跑在 worker 上的 BOS HEAD 扫描）。
> `79556d1` 的提交信息逐条列了它那次的推理组（"pool patrol 的三个触发、
> 孤儿分诊、task_stuck 豁免、仪表盘聚合"），**没有**把 auto-complete/auto-fail 列进去；
> 两条测试也没有 docstring 或理由注释，与周围重注释的推理型测试形成对比。
> 判断：那是记录当时行为的 characterization 测试，不是"pool 任务应当被批次行判完成"的决定。
> 需求 A6 是用户批准过的合同，晚于并高于它。
>
> 处理：两条测试**改断言为 T7 语义**（任务保持 `downloading`、不出现在
> `auto_completed`/`auto_failed`），并补上它们原本缺的 docstring 写清为什么
> ——包括被移除那条路径的代价：协调器在最后一刻死掉的 pool 任务会一直停在
> `downloading` 等人处理。这正是 A12 说的"pool 任务不会自愈，所以观测期是硬要求"，
> 信号是 `pool_orphaned`。


- **不删任何 sharded 代码**。`ShardedDownloadWorkflow` / `ShardWorkerWorkflow` /
  `run_pipeline_batch` / `partition_files_greedy` / `SplitDownloadWorkflow` /
  `DownloadDatasetWorkflow` 全部保留注册：历史 workflow 的 replay 依赖它们，
  `tests/fixtures/histories/` 的 replay 测试也依赖。R5 的硬约束。
- **不改 sharded 的全有全无判定**（`activities.py:407`）。它是回滚路径。
  代价是回滚回去 bug 就回来 —— 接受，因为回滚是临时且有人盯的。
- **不改 `/api/queue/reshard` 与 `shard_count`**。它们是模式翻转的唯一干净入口。
- **不改 web 表单**。调研确认表单已经读服务端默认值、`dispatch_mode` 为空时不发该键；
  唯一残留是给 pool 任务也发 `shard_count`（`app.js:272`），而 `tasks.py:255-256` 只在 `>0` 时用它、
  pool 忽略 `max_workers` —— 纯无害，不进本次关键路径。
- **不设 `HF_HUB_DISABLE_XET=1`**。用户明确"先不动"。

---

## 3. 执行顺序

### Phase 0 —— 前置（不碰生产运行时）

| 步 | 动作 | 门槛 |
|---|---|---|
| 0.1 | 本地跑全量测试 | 当前基线 **599 passed**（含 replay；2026-08-08 于 `6067e20` 实测 12.89s）—— 写这份方案时是 453，T1–T7 之后是 599 |
| 0.2 | 推送 `feat/architecture-upgrade` 到 origin | 需要你点头 —— 这是唯一的对外动作 |
| 0.3 | S1 `git fetch` → checkout `feat/architecture-upgrade` → 对齐 `origin` | S1 `git rev-parse --short HEAD` == 本地。2026-08-08 实测起点：S1 在 **`hotfix/pipeline-integrity` @ `1c5814a`** —— 是换分支，不是 fast-forward |
| 0.4 | S1 `.env.bak-*` 移到 `/root/env-backups/` | `ls $REPO_DIR/.env.bak*` 为空。实测当前有 1 个：`.env.bak-20260807-173836`（untracked，是 Key 轮换留下的；里面是旧 Key，别留在 rsync 能碰到的目录） |
| 0.5 | 记录切换前基线 | 两个前缀的 BOS 字节/对象数（用 `calendar.timegm`）；`downloading` 行的 `dispatch_mode` 快照；17 台 md5 manifest |
| 0.6 | **在 S1 上建一次性 pytest venv** | `deploy-workers.sh:88-103` 的 G7 门槛在 `python3 -m pytest --version` 失败时**硬中止部署**，而 S1 没装 pytest（2026-08-08 实测：`/usr/bin/python3` = 3.10.12，`No module named pytest`，且 `/tmp/dlm-test-venv` 不存在）。**注意 CLAUDE.md 的配方写的是 `python3.12`，S1 上没有** —— 只有 `/usr/bin/python3.10`，所以 venv 必须用 3.10 建（`pyproject.toml` 的 `requires-python = ">=3.10"`，且全仓无 3.11+ 语法：`datetime.UTC`/`Self`/`except*`/`TaskGroup`/`StrEnum`/`tomllib` 均未出现，已 grep 确认）。G7 用的是**环境里的 `python3`**，不是固定路径，所以调用方式是 `PATH=/tmp/dlm-test-venv/bin:$PATH bash scripts/deploy-workers.sh`。跑在 S1 上对生产库是安全的：`tests/conftest.py` 的 session 级 autouse `_never_touch_the_production_db` 把 `snapshot.DB_PATH` 重定向到 tmp 并**断言**不含 `/data/dlm.db`，另一个 autouse guard 把告警日志重定向到 tmp。磁盘要留意：S1 `/` 81% 已用、只剩 7.6G，且 `/data` 与 `/` **同一个 `/dev/vda2`**（不是独立盘） |
| 0.7 | **临时 mask web watchdog** | `dlm-web-watchdog.timer` 每 30s 探 `/api/dashboard`，wedged 就重启 web。Phase 3.1 手动重启 web 与它撞车会造成两次重启叠加、`init_db()` 迁移期间的窗口更难判读。`systemctl mask dlm-web-watchdog.timer`，Phase 3 完成后立刻 unmask（写进收尾清单，别忘） |

#### Phase 0 前置实测（2026-08-08，只读，已完成）

新 Key 的落地状态已量到位，A1 的"部署前"基线因此有了确定值：

| 项 | 结果 |
|---|---|
| 17 台 `$REMOTE_DIR/.env` 的 `BAIDU_AK` | **全部相同的新值**（sha256 前缀 `edccde44`）—— 轮换已覆盖 17/17 |
| 16 台 worker 进程 `/proc/<pid>/environ` 的 `BAIDU_AK` | **全部相同的旧值**（`27233f75`）—— 16/16 都还是陈的 |
| S1 `dlm-web`（pid 250543，08-07 17:41 启动） | 已是新值 `edccde44` —— 控制面不需要额外处理 |

原因不是部署漏了谁，而是 `dlm/core/config.py:load_config()` 调的
`load_dotenv(env_path)` **默认不覆盖已存在的环境变量**：worker 由启动脚本带着
`BAIDU_AK` 进环境，此后进程活多久就用多久的旧值，重复调 `load_config()` 也换不掉。
所以「重启 worker 才生效」不是经验之谈，是 dotenv 的语义。

这解释了 `t-20260808-93e25c`（fastumi100k）为什么以 `BOS resume filter failed` 失败：
旧 Key 只剩 PUT/HEAD，LIST 全挂，而续传过滤器要 LIST。**不需要代码修复**，
Phase 2 的 `deploy-workers.sh` 重启就是修复本身。

对 A1 的直接后果：部署后 16 台的进程侧前缀必须从 `27233f75` 变成 `edccde44`。
这比"探一次 API 通不通"强得多 —— 它是同一个凭证的身份比对，不依赖任何一次调用的运气。

### Phase 1 —— 暂停两个任务（保住在飞分片的成果）

当前 w2/w3/w4/w6 上有 4 个活分片（RealOmin 1.3G+2.6G、molmobot 5.5G+11G staging）。
`deploy-workers.sh` **没有任何"有任务在跑就拒绝"的门槛** —— 它会无条件
`pkill -f dlm.temporal` 打掉全部 16 台（脚本自己的注释说明这是设计如此，
靠 BOS 续传过滤器保证安全，而不是靠拒绝）。

所以先暂停，别让 sharded 的 workflow 在部署后白白重放一轮 —— 反正下一步就要 reshard 成 pool。

```
POST /api/queue/pause {"task_id":"t-20260806-319c55"}
POST /api/queue/pause {"task_id":"t-20260805-460d45"}
```

门槛：`/api/dashboard` 显示两个任务 `paused`；Temporal 上 6 个 workflow 全部关闭；
**staging 目录一个都没少**（暂停不删 staging —— 已证明，但要实测确认）。

### Phase 2 —— 部署

```
bash scripts/deploy-workers.sh          # 全 16 台
```

脚本自带的门槛（不许绕过）：G7 部署前 `pytest tests/ -q` 必须过；
G6 每台 `temporalio>=1.30,<2`；结尾 md5 manifest 17/17。

部署后立即验：
- A1：17 台 `ps -eo etimes,args` 的 etimes < 部署耗时（当前是 ~33 小时，必须归零）；
  **凭证身份比对**（Phase 0 实测给出的基线）：16 台 worker 进程 `/proc/<pid>/environ`
  的 `BAIDU_AK` sha256 前缀必须由 `27233f75` 变为 `edccde44`；
  再用**进程实际持有的凭证**分别探 LIST / HEAD / `does_bucket_exist` 三项
  （旧 Key 的特征是 PUT/HEAD 活而 LIST 死，一次调用不算）
- A2：md5 manifest 17/17 匹配，且不再等于旧值 `3a6ba1c6…`
- 迁移落地：`PRAGMA table_info(tasks)` 出现 `dispatch_mode` 与 `coordinator_phase`
  （生产库目前**没有**这两列）。**注意迁移的落地时机是 3.1 的 `systemctl restart dlm-web`，
  不是本阶段的 worker 部署** —— `init_db()` 的调用者全在 `dlm/web/*`
  加 `dispatch.py`（独立 CLI）与 `scripts/migrate-tasks.py`；
  `dlm/temporal/__main__.py` 与 `activities.py` **零 `dlm.queue.snapshot` 导入**（实测确认）。
  这是设计如此：worker 碰不到 SQLite。**而且这里必须小心** —— `DB_PATH` 默认
  `/data/dlm.db`，worker 上 `/data` 是存在的，所以任何让 worker 侧调到 `init_db()`
  的改动都会在 16 台上各建一个幽灵库。T1 的所有落库都必须走 HTTP，不许有例外。
- **pool 队列的 poller 门槛**：`start_pool_download` 有一道 `PoolPollerGateError`，
  在 `pool-hf`/`pool-ms` 队列的 activity poller 数 < 存活 worker 数时**拒绝派发**，
  而 `auto_dispatch_pending` 捕到它会把任务退回 `pending` ——
  于是 reshard 之后会每 30s 静默重试一轮，看起来像"任务卡在 pending 不动"。
  所以部署后、3.4 之前必须先确认两个 pool 队列的 poller 数已经追上存活 worker 数
  （worker 起来到注册完队列有秒级延迟，别抢跑）。
- 16 台 `dlm-sidecar@<key>.service` 仍 active

### Phase 3 —— 翻默认值 + 回填 + 重派

| 步 | 动作 |
|---|---|
| 3.1 | `systemctl restart dlm-web`（让翻转后的 `fleet.py` 生效），确认 `/api/dashboard` 的 `default_dispatch_mode == "pool"`；`PRAGMA table_info(tasks)` 确认新列都到位 |
| 3.2 | `scripts/backfill_dispatch_mode.py --dry-run` → 看行数与 id 列表 → `--apply` |
| 3.3 | 校验 `downloading` 行的 `dispatch_mode` 与 0.5 的快照逐行相同 |
| 3.4 | 重派：`POST /api/queue/reshard {"task_id":"t-20260806-319c55","dispatch_mode":"pool"}`，molmobot 同样 |
| 3.5 | 等 `auto_dispatch_pending`（30s 一轮）接手，确认起的是 `PoolDownloadWorkflow` |
| 3.6 | `systemctl unmask dlm-web-watchdog.timer && systemctl start dlm-web-watchdog.timer`（还 0.7 的债） |

**回填与两个真任务不重叠 —— 必须确认，不能假设。** 回填的 `WHERE` 是
`status IN ('pending','paused','failed')`，而 Phase 1 会把两个任务置成 `paused`，
所以 3.2 **会**把它们改成 `dispatch_mode='pool'`，而它们的旧 sharded shard 行
此时还在。这本身不炸（`auto_dispatch_pending` 只领 `pending`，`paused` 不会被派发），
但它会给 3.4 埋一个坑：

**`terminate_workflow_and_wait` 按 `dispatch_mode` 分流。**
`temporal_client.py:508-600` 里，逐 shard 行扫子 workflow handle 的那段是
`if dispatch_mode != "pool":` 才走的；而它依赖的 `_find_running_workflow_ids`
是 `except Exception: return []`（注释写明 "Best effort"）。
所以一旦行已经是 `pool` 而实际在飞的是 `ShardWorkerWorkflow`，
终止路径会走 pool 分支、跳过逐分片清理，**可能留下活着的 sharded 子 workflow**
去和新起的 pool 批次抢同一批文件（不损数据 —— 都只 PUT，但会白烧带宽且难判读）。

处置（二选一，实施时定并记录）：
- **(A)** 3.4 的 reshard 先做、3.2 的回填后做；或
- **(B)** 回填脚本加 `--exclude-task` 参数，把两个任务排除，改由 reshard 单独落模式。

倾向 **(A)** —— reshard 本身就会写 `dispatch_mode` 并 `DELETE FROM shards`，
顺序换过来两件事就都由一条路径负责，回填只管其余存量行。
但 (A) 会让 3.4 提前到"批准点"之前，而 3.4 是需要单独批准的那一步 ——
所以实际执行取 **(B)**：回填排除这两个任务，批准点保持在 3.4 之前不动。

**3.4 为什么必须是 reshard**：见 B4。它是唯一会 `DELETE FROM shards WHERE task_id=?`
的路径，不清掉旧 sharded 行，`create_pool_batches` 会抛非重试的 `PoolBatchMismatch`
把任务当场打成 `failed`。

**3.4 的前置**：`name` / `category` 一个字都不能改（决定 BOS 前缀；
`manipulation/RealOmin/`、`other/molmobot-data/`）。reshard 不碰这两个字段 —— 但要实测确认。

### Phase 4 —— 观测

按 A9 / A10 / A11 / A12 的判定方式跑。观测期内定时采样：
两个前缀的 BOS 字节与对象数（**必须单调不减**，任何一次下降 = 事故，立即停）、
在飞批次数、参与 worker 数、最新对象年龄（`calendar.timegm`）、
`/api/dashboard` 的 alerts（特别盯 `pool_orphaned` / `pool_starved`，见 R-1）。

---

## 4. 任务分解（给实施用）

| # | 任务 | 依赖 | 触及文件 |
|---|---|---|---|
| T1 | 缺件表 + 存取 + 两个接口 | — | `snapshot.py`, `routes/servers.py`, `routes/tasks.py` |
| T2 | pipeline 记录失败文件身份（9 处统一，修 544 的身份缺失） | — | `pipeline.py` |
| T3 | 批次容忍阈值 | T1, T2 | `activities.py`, `fleet.py` |
| T4 | workflow 接线 + `verify_missing_files` + 诚实收尾 | T1, T3 | `workflows.py`, `activities.py` |
| T5 | 告警与仪表盘（含 `missing_files_limit` 列） | T1, T4 | `alerts.py`, `routes/doctor.py`, `snapshot.py` |
| T6 | 默认值翻转 + 回填脚本 + rsync 排除 | — | `fleet.py`, `deploy-workers.sh`, `scripts/backfill_dispatch_mode.py` |
| **T7** | **reconciler 认识 pool（3 处）** | — | `reconciler.py` |

T1/T2/T6/T7 互不依赖，可先做。T3 依赖 T1+T2，T4 依赖 T3，T5 依赖 T4（要 limit 值）。
**T7 是切换的前置修复，不是可选项** —— 它修的是 pool 今天就有的 bug，
不修就切，控制面会替 workflow 宣布终态。

---

## 5. 验收标准的可执行性修正

评审时请一起看：以下 A 条的判定方式在实测面前需要改。

| A 条 | 原判定 | 问题 | 改成 |
|---|---|---|---|
| A2 | "17/17 台 `git rev-parse --short HEAD` 相同" | **不可执行** —— rsync 排除 `.git`，16 台没有 git 仓库 | 以部署脚本的 md5 manifest 为准；`git rev-parse` 只在 S1 上验 |
| A3(b) | "`dispatch_mode IS NULL` 的 pending 行" | 前提不成立（迁移写成字面量 `'sharded'`） | 已在需求文档中修正为"存量 pending 行 + 回填生效" |
| A12 | "无告警" | pool 任务不自愈，`pool_orphaned`/`pool_starved` 是唯一信号；且 T4 的第二档**必发 WARNING**，"无告警"与它直接矛盾 | 改成"**无 CRITICAL 告警**；WARNING 逐条判读并如实汇报"。增加"确认 `pool_orphaned`/`pool_starved` 确实可达 `/api/dashboard` 并在观测期实际盯" |
| A6 | "缺件被记录" | `missing_files` 只记"进了 filelist 却没传上去"的文件；源端报 0 字节的文件在列表阶段就被丢弃（`activities.py:56-57`、`:83`），永远进不来 | 判定语句改为"**filelist 内**的缺件被记录"；"源端 0 字节"是独立的审计工作，明确不在本次范围 |

---

## 6. #83 自动搬运：**推迟**

用户条件："如果说不好搞，先太多任务的话先不搞。"逐条对表 R9 的判定规则：

**它需要改动 pool 关键路径 —— 命中"推迟"分支。**

| 理由 | 依据 |
|---|---|
| 写入点就在 pool 的派发漏斗里 | `dispatch_prefix` 列必须由 `temporal_client.start_task_download`（`:482`，按 `dispatch_mode` 分流 sharded/pool 的唯一漏斗）在分流前写入 —— 设计文档自己写明"顺序上先合 pool、再叠这个功能" |
| 改动量与 R4 同级，不是小尾巴 | 2 个新列 + `dispatch_prefix` 列 + `/api/task-progress` 的 done 分支挂钩 + `maybe_arm_transfer` 的 4 道门槛 + 3 档字节比 + scheduler 新增 `_dispatch_ready_transfers` 循环 + 4 条告警规则 + `/api/queue/add` 补写 `bos_path` |
| 有一个未回答的前置问题 | 设计文档开放问题 5：地瓜云侧是否有 ~34 TB 余量接 molmobot + RealOmin。默认 `on` 且这两个任务就是最先的真实样本 —— 余量不够就是往满的目的端灌 34 TB |
| 现在开它没有收益 | 触发器只对**部署之后完成**的任务生效。这两个任务本轮才是第一批真实样本，而它们的收尾语义在 T4 修订后才定下来 —— 先让 pool 与缺件记账跑通一轮、拿到真实的缺件量级，再决定搬运门槛怎么设，比现在盲设更省事 |

> **原稿这一格的理由已作废，特此更正**：原文写的是"这两个任务收尾时若有缺件会报
> `failed`，`failed` 不触发搬运，所以即使上线也不会开火"。T4 的语义修订
> （少量缺件照报 `done`、照搬）让这句话**不再成立** —— 上线后它**会**开火。
> 推迟的结论不变，但支撑它的理由换成了上面那条与"目的端余量未确认"那条。

**推迟的边界（写清，下次直接接着做）**：
- 代码零改动，`docs/design/auto-transfer-trigger.md` 原样保留，不进本次任何提交
- 下次入口：pool 验收（A1–A12）全绿之后，先回答"地瓜云余量"，再按该设计文档实施
- 期间搬运仍可**手工**触发（`routes/transfer.py` + 前端按钮都在，没坏）

---

## 7. 风险

| # | 风险 | 处置 |
|---|---|---|
| **R-1** | **pool 任务卡住不会自愈。** `reconciler.reconcile()` 的孤儿重派对 pool 任务显式跳过（`reconciler.py:183` 附近 "decision C"：第二个 `PoolDownloadWorkflow` 会重跑 list→filter→chunk 并撞上批次不匹配，把任务彻底弄死，所以宁可不动）。sharded 卡 30 分钟自动重派，**pool 不会**，只产 `pool_orphaned`/`pool_starved` 告警等人 | 这是本次切换最实质的姿态变化。观测期从"最好有"变成硬要求（A12）；T5 确认告警可达 |
| R-2 | 部署无条件 pkill 全部 16 台，含 4 个活分片 | Phase 1 先暂停两个任务，把这件事变成有意选择而不是意外 |
| R-3 | `.progress.json` 批次续传标记按 shard filelist 的 md5 守卫，reshard 成 pool 会让它们失效 | 接受。BOS 过滤器让重启无损，代价只是重下"已下到 staging 但未上传"的部分（w5 那 1.9G、w2/w3/w4/w6 的 20G 量级） |
| R-4 | 生产库缺 `dispatch_mode`/`coordinator_phase` 两列 | `init_db()` 在每次 web 启动与多个 worker 入口调用，第一个新代码进程会自动补。Phase 2 用 `PRAGMA table_info(tasks)` 实测确认，不假设 |
| R-5 | 9 台 bj 主机全都没有 `MODELSCOPE_API_TOKEN`/`MS_TOKEN` | 早于本次，公开 repo 无影响（`activities.py:65` 回落匿名 `HubApi()`），私有/gated 会硬失败。**不在本次范围**，单独记一笔 |
| R-6 | S1 根分区 80% 已用、仅剩 7.6G | S1 不跑下载，但它跑 SQLite 与 Temporal。观测期一并盯 |
| R-7 | T2 对 544 号点的身份修复是唯一有实质风险的改动 | 专门测试：构造抛 `BaseException` 的下载，断言身份进了 `failed_details`；并断言 `len(failed_details) == failed_files` |
| **R-8** | **控制面自己会宣布终态**（reconciler 三处，见 T7）。不修就切 = 两个真任务可能被假判完成或直接判死 | T7，且它的第一条测试就是"pool 任务全批次 done + 无 workflow ⇒ 仍是 downloading" |
| **R-9** | 收尾判定的确定性风险：`_finalize` 里读上限用的分母若来自任何非确定量（时钟、随机、外部查询），replay 会崩 | 分母固定取协调器 filelist 阶段已经拿到的 `filelist_result["count"]`（workflow 状态内的值），阈值取 import 期常量。**不在 workflow 里读环境变量** —— 那在 replay 时可能已经变了；阈值由 activity 侧读、随返回值带回 |
| **R-10** | replay 夹具必须重录（`pool-t-pool-1.json`），重录之前 `tests/test_pool_workflow_replay.py` 必然红 | 写进 T4 的完成定义；sharded 夹具一律不动 |
| **R-11** | 部署脚本的 G7 门槛依赖 S1 上有 pytest，而 S1 没装 | Phase 0.6，先建 `/tmp/dlm-test-venv` 并确认 G7 实际调的解释器能跑 |

---

## 8. 回滚

前提不变：**BOS 只 PUT、从不 delete，所以任何回滚路径都不损数据**，代价只是时间。

| 触发 | 动作 | 是否需要重新部署 |
|---|---|---|
| BOS 字节数或对象数下降 | 最高优先级事故。立即停全部派发、暂停两个任务、查清再动 | — |
| 新任务默认值出问题 | `systemctl set-environment` / unit 里设 `DLM_DEFAULT_DISPATCH_MODE=sharded` + `systemctl restart dlm-web` | **否** |
| pool 批次大面积失败 | 两个任务 `POST /api/queue/reshard {"dispatch_mode":"sharded"}` 重派回 sharded（路径完整保留） | 否 |
| 部署后 worker 起不来 / md5 不齐 | S1 checkout 回 `1c5814a` 并重新部署；两个任务保持 paused | 是 |
| 缺件容忍误放行 | `DLM_POOL_BATCH_FAIL_MAX=0` 让容忍完全失效，行为退回今天 | 否（worker 需重启） |

---

## 9. 用户已决定的三点（2026-08-08）

1. **Phase 0.2 推送分支到 origin —— 已批准。**
2. **T4 收尾语义 —— 已改为"少量缺件照报 `done`、照搬"。**
   用户原话："缺文件好像是挺正常的事情……他说可以搬就搬呗。"
   需求文档 R4 与 A6 已同步修订；上限机制保留（`max(10, 0.5%)`），
   WARNING 告警在报 `done` 的同时必发。
3. **pool 集群验收仍需单独批准。** 不并进 Phase 3。
   Phase 3.1–3.3（翻默认值、回填、校验）可连续执行；
   **Phase 3.4 重派两个任务之前必须停下来拿一次明确批准**，
   这也对齐用户流程的第 8 步"OK 了之后再把任务起起来"。

### 由 2 带来的连带影响（评审需一并看）

- 报 `done` 之后，`missing_files` 的记录**必须继续保留**，不能因为任务 done 就清理 ——
  它是这个数据集"缺哪几个文件"的唯一档案。
- #83 上线后，带缺件的任务会**照常触发搬运**。这是用户意图，且 #83 自己还有
  一层字节比门槛（0.95 / 0.50 三档）作为第二道网，不需要为缺件另加逻辑。
- T5 的告警规则从"任务有缺件记录且已终态 ⇒ WARNING"变成硬要求：
  报 `done` 而不发告警是 A6 唯一的失败形态。

---

## 10. 代码评审第 1 轮 —— 未修复项存档（2026-08-08）

四个 GAP（GAP-1 心跳/复核、GAP-2 `--exclude-task`、GAP-3 phase 保留、GAP-4 注释失实）
已在 `17a7ff6` 修完，含 28 条新测试与 6 次变异验证。以下是评审提出但**本次不改**的项，
逐条写清理由，避免"提了但不知去哪了"。

### 待用户裁决：批次上限与任务上限不叠加（评审 IMPORTANT-1）

**现象。** 单批次容忍上限 `POOL_BATCH_FAIL_MAX=5` 是**绝对值**，与批次大小、
与任务级上限 `max(10, 总文件数 × 0.5%)` 都不联动（`activities.py:1360-1420`）。
后果：一个源端坏掉的目录若恰好聚集在同一批次里（例如 RoboDojo 那种整目录 0 字节的
depth 文件），该批次第 6 个坏文件就会让整批抛异常 → 两轮重派都失败 → 任务 `failed`，
**即使这 6 个文件远低于任务级允许的缺件数**。而 `failed` 任务永不自动重派，
也不触发搬运 —— 正是 R4 想避免的形态，只是触发条件从"1 个坏文件"放宽到了"同批 6 个"。

**为什么不擅自改。** R4 的修订表**已经明确写下了两级设计**，且原话点出了非叠加是有意的：
"单批次容忍 5 个，而一个任务可有 1500 个批次 —— 累计 7500 个缺件不是'几个'"。
即：批次级上限存在的目的就是防止比例型任务级上限被批次数放大。
所以当前行为**符合已批准的需求**，且失败方向是保守的（宁可判死也不静默少数据），
不丢任何已下好的数据。评审发现与方案文本冲突时按规矩交人裁决，不由我单方面改。

**选项。**

| 选项 | 行为 | 代价 |
|---|---|---|
| A（现状，默认） | 同批 >5 个坏文件 ⇒ 任务 `failed`，等人 reshard/重派 | 坏文件聚集的数据集需人工介入一次 |
| B | 批次上限改为 `max(5, 批次文件数 × 比例)` | 大批次容忍度上升，仍受任务级上限兜底 |
| C | 批次上限让位于任务级上限（批次只记账，判死只在任务收尾） | 最宽松；单批次的爆炸半径保护消失 |

**建议：先按 A 上线并观测。** 两个当前任务（RealOmin / molmobot）没有已知的整目录坏文件；
真撞上了，`failed` + CRITICAL 会明确告诉我们是哪个批次哪些文件，那时再拿真实数据选 B 或 C
比现在凭想象定比例更靠得住。运维口子：`DLM_POOL_BATCH_FAIL_MAX` 可在不改代码的情况下调高
（需重启 worker）。

### 记录但不改的 5 条 MINOR

1. **`pipeline.py:815-816` 的 `archivable` 占位** —— 该处对无法归因的失败填保守占位值，
   使其**不可归档** ⇒ 批次不可容忍 ⇒ 判死。方向正确（失败偏保守），
   代价是这类批次拿不到"缺哪些文件"的明细。改它要重构 pipeline 的错误归因，
   超出本次范围。
2. **`MISSING_VERIFY_MAX` 由本地 `POOL_BATCH_FAIL_MAX` 推导，而批次跑在远端 worker。**
   若某台 worker 的 `DLM_POOL_BATCH_FAIL_MAX` 与 S1 不一致，这个上限的推导前提就不成立。
   已进运维清单一行：**调 `DLM_POOL_BATCH_FAIL_MAX` 必须 17 台一起调**（与硬约束 3 一致）。
   不加代码校验，因为 worker 无法写 SQLite，做成运行时校验要新开一条 HTTP 上报路径。
3. **`recheck=False` 时，`failed` 任务的 CRITICAL 可能高报缺件数。** 该路径不查 BOS，
   直接用库里的记账数当缺件数；若其中部分文件其实已被后续批次补上，数字会偏大。
   偏大而非偏小，方向安全；且 `failed` 任务本来就要人工看明细。修它等于取消这条快路径，
   而快路径正是 GAP-1 的修复本体（避免大档案反复心跳超时）。
4. **`tasks.py` 里 `count` 的含义变了**（现在是本次返回条数，`total` 才是库里总数）。
   接口未发布给外部消费者，仪表盘同批改动一起上；不额外做兼容层。
5. **测试绕过 ASGI 直接调路由函数。** 与本仓既有测试风格一致（`test_default_dispatch_mode.py`
   等同样直调），不在本次改测试基础设施。

---

## 11. 迁移 + HTTP 契约审计（2026-08-08）

独立子代理审计了两件事：SQLite 迁移（新建库 vs 原地迁移 vs 重复运行），
以及 worker→S1 的 HTTP 契约。**迁移一侧干净**：所有 DDL 都是
`CREATE TABLE IF NOT EXISTS` + `try/except` 包裹的 `ALTER ADD COLUMN`，
新建库走 CREATE 拿到同样的列，原地库走 ALTER，重复运行幂等。

### 已修（`b7da883`）：S1 拒绝的写入不许看起来像成功了

5 个 fire-and-forget 活动（`update_shard_status`、`report_shard_progress`、
`report_resume_info`、`aggregate_task_from_shards`、`assign_shard_server`）
把响应整个丢掉。要命的地方在于**这些路由是用 HTTP 200 表示拒绝的**：
shard 行不存在（resume/reshard 已经换掉了旧 workflow 还在上报的那批行）时返回
`{"error": ...}`，父任务已终态时返回 `{"ignored": true}`。所以 `raise_for_status()`
和"请求有没有抛"都看不见 —— 一次丢掉的 `status -> done` 会让分片在仪表盘上
永远停在 `running`，而 worker 日志和 workflow 历史里什么都没有。

**只记日志，不抛异常。** 这几个调用在 workflow 里没带 `retry_policy`，
继承 Temporal 的默认无限重试；在这里抛异常会把"一次静默丢失"换成
"协调器对着一个永远不会好的拒绝无限重试"—— 那更糟。可见性是修复本身，
判断留给运维。

`report_resume_info` 是其中最要紧的一个：那一行是 A4"没有重下 BOS 已有数据"的
证据，所以它的日志行把文件数和 GB 数一起打出来。
`create_shards_in_db` 有返回值，继续抛 —— 但两个 key 都没有的响应体
（代理错误页 / S1 500）不再表现为一句 `KeyError: 'shard_ids'`。

44 条测试，7/7 变异被捕获（含"降级成 debug"、"改成抛异常"、"去掉
`raise_for_status`"三个最容易被误当成改进的方向）。

### 记录但不改的 2 条

1. **`upload_speed_mbps` 靠懒 ALTER 加列**（`dlm/web/routes/servers.py:309`，
   首次 `/api/events` POST 时执行），新建库的 `tasks` DDL 里没有它。
   于是全新库在第一次收到事件之前，列结构与原地迁移的库不同。两条路径都会自愈、
   都不崩，本次不动 —— 改 DDL 要连带处理"已有库多一次无害 ALTER"，
   收益只是整齐。
2. **`aggregate_task_from_shards` 对 pool 任务是确定的空操作**。pool 不建 shard 行，
   S1 那侧 `_aggregate_task` 见到零行就直接 `{"shards": 0}` 返回 —— 
   **不会把任务进度清零**（已读代码确认）。pool 协调器仍然调它。
   保留而不做成条件调用：这个调用在已录制的 pool 历史里，去掉它会破坏 replay
   而毫无收益（R5）。已在活动的 docstring 里写清，避免下一个人当成 bug 删掉。


