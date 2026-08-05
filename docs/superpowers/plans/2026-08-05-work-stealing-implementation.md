# Work-Stealing 调度 — 实施计划(v3.1,已过 Opus 计划评审)

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development,
> 逐任务实现 + 逐任务评审。设计依据:`2026-08-05-dynamic-work-stealing.md`(v2,
> R1-R10 + A0-A10)。v3 → v3.1:计划评审 4 BLOCKER + 14 IMPORTANT 全部闭合。

**Goal:** 共享池队列 + 协调器动态窗口替换静态分片,新旧并存灰度上线。

## Global Constraints(每任务隐含遵守)

- **G1** `ShardedDownloadWorkflow` / `ShardWorkerWorkflow` 及其所用活动**字节级
  不动**。唯一例外:活动参数列表不变前提下允许改内部实现细节;任何
  **返回值/语义**变更一律走新活动/新端点(池路径专用)
- **G2** 存量下载(Egocentric-100K 等)不被终止;集群只在 T10 碰,走两阶段
  部署;**禁止 `safe-deploy.sh`**(其 phase-1 全队取消会杀 Egocentric,违反 A0)
- **G3** 窗口循环:`workflow.start_activity`(不 await)、`workflow.wait`
  (禁 `asyncio.wait`)、完成 handle 按批号排序遍历、`h.result()` 逐个
  try/except;取消清理 `asyncio.shield`
- **G4** 批次终态 failed 只由协调器写;活动异常只 raise
- **G5** 新增/改动端点带 TERMINAL 守卫语义(对齐 /api/shard-progress)
- **G6** `temporalio>=1.30,<2`(评审实测版本;Priority 需要,>=1.9 不够);
  新列走 snapshot.init_db ALTER 迁移清单;部署门禁逐机断言
  `import temporalio; __version__`,不满足先跑 `scripts/install-deps.sh`
- **G7** 每任务:相关单测过 + py_compile;工作流任务必须带离线 replay 测试。
  测试基建(pytest 配置 + `[dev]` extra)在 T1 建立;A9 的"CI"落地为
  **部署门禁本地跑 `pytest tests/ -q` 全绿**(仓库当前无 CI)
- **G8** 池策略配置统一放 `dlm/web/fleet.py`(与 MIN_SHARD_DISK_GB 等同源):
  `POOL_MAX_CONCURRENT_TASKS`(env,默认 4)、`DEFAULT_DISPATCH_MODE`
  (env `DLM_DEFAULT_DISPATCH_MODE`,默认 'sharded')、`POOL_MAX_BATCHES=1500`

## 任务序列

顺序:**T1 → T2 →(T3、T4 并行)→ T5 → T6 → T8 →(T7、T9 并行)→ T10 → T11**。
T8 必须先于 T7 合入(T7 让池任务可启动,T8 让它可停;顺序颠倒会存在
"可启动不可停"的窗口)。T1-T9 纯代码不碰集群。

### T1 — DB、守卫与测试基建
**Files:** `dlm/queue/snapshot.py`, `dlm/web/routes/queue.py`, `dlm/web/fleet.py`,
`pyproject.toml`(pytest 配置 + dev extra), `tests/conftest.py`(新),
`tests/test_pool_db.py`(新)
**改动:**
1. `tasks` 迁移加 `dispatch_mode TEXT DEFAULT 'sharded'`、`coordinator_phase TEXT`
2. `get_running_shards()` JOIN tasks 排除 TERMINAL 父任务(存量 bug 一并修)
3. `/api/shards/assign`、`/api/shards/status` 加父任务 TERMINAL 守卫,
   命中返回 200 + `{"ignored": true}`
4. **新端点 `POST /api/pool/batches/create`**(幂等,池路径专用):
   幂等键 = `expected_count` + 逐行 `(shard_index, filelist_key, total_files,
   total_bytes)` 全等;命中后仍执行
   `UPDATE shards SET status='pending', server=NULL, speed_mbps=0
   WHERE task_id=? AND status!='done'`;单事务 executemany。
   **`/api/shards/create` 的删建语义与超时保持不变**(sharded ≤16 行无此问题;
   v3 的"改 create_shards_in_db 幂等/超时"作废——其 start_to_close=60s 在
   工作流里定死,动它违反 G1,且索引式 filelist_key 让幂等键在重派发时恒真,
   会让 Egocentric 孤儿重派发跳过删建、被陈行误判终态)
5. `_aggregate_task` 改单条 SQL(SUM/COUNT FILTER)+ 每任务 5s 去抖
6. fleet.py 落 G8 三个配置项
**Tests:** 迁移幂等;终态守卫;JOIN 行为;pool create 幂等命中/未命中/行重置;
aggregate 等值。
**验收挂钩:** A8。

### T2 — 注册表数据化 + 一致性测试
**Files:** `dlm/web/temporal_client.py`, `dlm/web/fleet.py`,
`tests/test_workflow_registry.py`(新)
**改动:**
1. `WORKFLOW_TYPES` += `"PoolDownloadWorkflow"`(`PARENT_WORKFLOW_TYPES` 是
   派生量且被 tests/test_cancel_all.py:108 锁定,**无需也不得手改**)
2. **`WORKFLOW_ID_PREFIXES: dict[type_name, prefix]`** 数据表(`pool-`、
   `sharded-`、`shard-s-` 等);`has_live_workflow` / `cancel_workflow` /
   `terminate_workflow_and_wait` 的 ID 构造全部改从它取(三处硬编码是
   历史漂移根因)
3. 一致性测试:收集 `dlm.temporal.workflows` 全部 `@workflow.defn` 类名,
   断言 ⊆ WORKFLOW_TYPES;断言 PARENT_WORKFLOW_TYPES 每类型有前缀条目
**注意:** 本任务先于 T5 提交,子集断言在 Pool 类存在前后均成立。
**验收挂钩:** A3、A9。

### T3 — 新活动:pool_alive_workers / chunk_filelist
**Files:** `dlm/temporal/activities.py`, `dlm/web/routes/queue.py`
(`/api/pool/alive-workers`), `tests/test_chunk_filelist.py`(新)
**接口:**
- `pool_alive_workers(source) -> int`:alive ∩ worker_serves,不看 busy/磁盘
- `chunk_filelist(filtered_path, task_input) -> {batch_keys, counts, bytes}`:
  钉 listing 队列;单批 ≤500 文件且 ≤32GiB(体积优先,巨文件独占);
  批清单上传 `META_BUCKET:download-manager/batchlists/{task}/batch-{i}.json`;
  并发心跳;**批数 > POOL_MAX_BATCHES(1500)时 raise 非重试错误 + alert**
  (K7:超巨任务需拆任务,运行时强制而非仅测试断言)
**Tests:** 切批规则矩阵;批清单往返;超限报错。
**验收挂钩:** A1。

### T4 — 新活动:run_pool_batch
**Files:** `dlm/temporal/activities.py`, `tests/test_run_pool_batch.py`(新)
**行为(顺序即规范):**
1. `rm -rf` 自身 staging(`{task}/shard-{i}` 命名沿用)
2. **直接 `requests.post`** 到 `/api/shards/assign`、`/api/shards/status`
   (不复用 assign_shard_server/update_shard_status 活动——G1 禁改其返回值,
   且活动不能调活动);响应 `ignored:true` → 直接正常返回(任务已停)
3. 磁盘预检:余量按**共存期本机可能并发的全部管线**折算(私有队列 2 +
   共享 1 + 池 1);不足 raise `_RetryableDiskLow`(可重试,靠 5min 退避)
4. BOS 拉批清单 → **无条件 HEAD-skip** → PipelineEngine.run(剩余)
5. 成功:清 staging → HTTP 报 done → return 统计
6. 异常:清 staging → raise(不写 failed,G4);失败消息含 server_key
7. `_speed_reporter` 循环 try/except(心跳源自保护)
**Tests:** HEAD-skip 过滤;ignored 短路;异常路径;成功链。
**验收挂钩:** A5、A7、A8。

### T5 — PoolDownloadWorkflow(核心,最高评审强度)
**Files:** `dlm/temporal/workflows.py`(只增)、`dlm/temporal/activities.py`
(`create_pool_batches_in_db`、`record_batches_and_window`、`release_batches`),
`tests/test_pool_workflow_replay.py`、`tests/fixtures/histories/`(新)
**实现:** 设计 v2 §2.3 骨架逐条照抄。要点:
- list → filter → chunk → **create_pool_batches_in_db**(新活动,
  打 T1 幂等端点,`start_to_close=5min`, retry 3)→ 窗口循环 → 末轮 failed
  补投一轮 → 终态 + aggregate
- `record_batches_and_window(task_id, results) -> window`:写批终态 + 动态窗口
  `max(1, floor(P × W_i / Σ W_active))`;每醒恰调一次
- 活动配置:`schedule_to_close=48h / start_to_close=12h / heartbeat=10min /
  retry(initial=5min, max=3) / WAIT_CANCELLATION_COMPLETED /
  Priority(priority_key=1|3)`
- CancelledError → shield `release_batches` → re-raise
- coordinator_phase 经活动逐段写
**Tests(评审修正后的正确姿势):**
- 从 S1 拉 2-3 条**真实** ShardedDownloadWorkflow / ShardWorkerWorkflow history
  (`temporal workflow show --output json`,含 Egocentric 当前 run)提交进
  `tests/fixtures/histories/`;用改后代码跑
  **`temporalio.worker.Replayer`**(Python 类名如此;WorkflowReplayer 是 Java)
  `.replay_workflows(...)` 断言无 NonDeterminismError——这是 G1 的唯一
  机器可验证证据(A9)
- Pool 自身:对 `WorkflowEnvironment.start_time_skipping()`(首次会下载
  test-server 二进制,需预置)录一次小任务 history 提交为 fixture,
  CI/本地只跑离线 Replayer;**回放至少两次,`PYTHONHASHSEED=0` 与
  `=12345` 各一次**(set 迭代序问题与 hash seed 相关,单次抓不到)
- 窗口=1 边界;全 done / 部分 failed / 补投成功 / 补投失败四种终态;
  取消路径 shielded 清理被调
**验收挂钩:** A1-A5、A9。

### T6 — Worker 进程与部署门禁
**Files:** `dlm/temporal/__main__.py`, `scripts/deploy-workers.sh`, `pyproject.toml`
**改动:**
1. **`PoolDownloadWorkflow` 加入 `__main__.py` 既有 workflows 列表**
   (两个既有 Worker 共用,一处即可——v3 漏了这条,漏掉则协调器永远无人
   轮询,任务静默挂死);第三个池 Worker:
   `task_queue=pool-{source_for_worker(key)}`,**`workflows=[]`**、
   `activities=[run_pool_batch]`、`max_concurrent_activities=1`
2. `pyproject.toml`:`temporalio>=1.30,<2`(G6)
3. deploy-workers.sh:清单改
   `cd $REPO_DIR && find dlm -name '*.py' -not -path '*/__pycache__/*' | sort
   | xargs md5sum | md5sum`;新增 `--sync-only-then-restart` 两阶段;
   门禁加逐机 temporalio 版本断言(G6)+ 本地 `pytest tests/ -q` 全绿(G7)
**Tests:** worker 三队列注册单测;脚本 shellcheck + 干跑;先 `--worker bj9`
单机演练再全量。
**验收挂钩:** A6、A9、A0。

### T8 — 停控与回滚链路(先于 T7!)
**Files:** `dlm/web/temporal_client.py`, `dlm/web/routes/queue.py`
**改动:**
1. `cancel_workflow` / `terminate_workflow_and_wait`:`dispatch_mode='pool'`
   只处理父 `pool-{task_id}`(前缀取自 T2 的 WORKFLOW_ID_PREFIXES),
   跳过逐 shard 句柄;describe 轮询每轮 15s 预算
2. pause:cancel 成功后 running 批行重置(pending/NULL/0)
3. resume:置 pending 时 delete_shards_by_task
4. reshard:接受 `dispatch_mode` 并写回;**校验放宽:`shard_count>=1` 或
  提供合法 `dispatch_mode` 即可**(纯模式回滚不带 shard_count;
  pool→sharded 时 shard_count 缺省沿用 max_workers,0 走自动分片)
**Tests:** pause 后无 running 行;resume 无陈行;模式往返;纯模式 flip。
**验收挂钩:** A3、A9。

### T7 — 派发收口、准入、模式门禁与建任务入口
**Files:** `dlm/web/temporal_client.py`, `dlm/web/reconciler.py`,
`dlm/web/routes/doctor.py`, `dlm/web/routes/queue.py`, `dlm/web/routes/tasks.py`,
`dlm/web/static/`(app.js/index.html)
**改动:**
1. **`start_task_download(task)`**(注意:`start_download` 已被 legacy 占名,
   `temporal_client.py:92`,被 dispatch.py:64 与 migrate-tasks.py 调用——
   不得同名重载;grep 断言同时覆盖 `start_sharded_download(` 与裸
   `start_download(`):按 dispatch_mode 分流;
   `start_pool_download` **显式 `task_queue="download-workers"`**
   (与 sharded 协调器一致;bj* 不轮询该队列,MS 池协调器照旧落 w*,
   list/filter/chunk 钉 listing_queue 不受影响)
2. 模式门禁:**raw gRPC `DescribeTaskQueueRequest(..., task_queue_type=
   TASK_QUEUE_TYPE_ACTIVITY)`**(池 Worker 不注册 workflow,查 WORKFLOW
   类型恒 0,Client 也没有便捷方法);轮询者 < 池预期 → 拒绝 + alert
3. 四处调用点(reconciler:126 / :273、doctor:206、queue:317)全改
   start_task_download;准入 = 每 source downloading 池任务 ≤
   POOL_MAX_CONCURRENT_TASKS;listing guard 改 coordinator_phase 判据
4. **建任务入口(A1 的前提,v3 完全遗漏)**:`/api/queue/add` 与
   `/api/tasks`(AddTaskRequest)接受可选 `dispatch_mode∈{'sharded','pool'}`,
   缺省 DEFAULT_DISPATCH_MODE;前端 add 弹窗加 mode 下拉(灰度期默认 sharded)
5. preempt 修 victim server=NULL 崩溃
**Tests:** 分流;门禁拒绝;准入上限;guard;add 两入口带 mode 落库。
**验收挂钩:** A1、A2、A3、A9。

### T9 — 可观测性改造(可与 T7 并行)
**Files:** `dlm/web/reconciler.py`, `dlm/web/routes/doctor.py`,
`dlm/web/alerts.py`, `dlm/web/health_verifier.py`, `dlm/web/scheduler.py`,
`dlm/web/static/`
**改动:**(同 v3,不变)池巡检(pending_activities,SCHEDULED>15min 或
attempt 爬升→`pool_starved` alert,替代池任务的 1800s 孤儿全量重派发)/
忙信号改 running 批行 / task_stuck 豁免排队态 / dashboard 后端按 server
聚合 + 前端弹窗聚合 / 周期 staging GC(仅终态任务目录)。
**验收挂钩:** A10。

### T10 — 集成验收与试点(真实集群,人批准后执行)
1. **两阶段部署只用 `deploy-workers.sh --sync-only-then-restart`;
   禁 safe-deploy.sh(A0)**。重启前置检查:对 Egocentric 每个
   `shard-s-*` `temporal workflow describe`,读 pendingActivities[].attempt,
   任一 run_pipeline_batch **attempt≥3 则推迟重启**等批次翻页(重启烧一次
   尝试,第 3 次上被烧会连锁把任务打成终态 failed);重启后 15 分钟内确认
   全部 shard 行重新 running、任务未落 failed。worker 全绿后
   `systemctl restart dlm-web` 触发 init_db ALTER,核对迁移日志再继续
2. MS 小数据集(100-300GB)`dispatch_mode='pool'` 建任务(走 T7 的 API)→
   A1/A5/A6/A7;**A6 对 pool-hf 与 pool-ms 都做 describe(ACTIVITY 类型),
   断言轮询者集合 = w* / bj***;A5 证据:故意 terminate → resume →
   终态后 BOS 前缀全量 list 对象数+字节逐条核对源清单
   (参照 2026-08-03 Alpha 对账做法)
3. 双任务优先级实测(P0+P1)→ A2/A3;顺带验证 priority_key(无效不阻塞)
4. kill 单机 worker → A4;停全池 worker 5 分钟 → A10 巡检告警
5. 回滚演练:半程任务 `reshard {dispatch_mode:'sharded'}` → A9
6. 全绿 → `DLM_DEFAULT_DISPATCH_MODE=pool`;观察 48h;A0 全程核对
**产物:** `docs/reports/` 验收报告(A0-A10 逐条证据)。

### T11 — 收尾(切默认 ≥1 周后,单独批准)
删 sharded 路径;CLAUDE.md 更新(含 G8 两个 env 变量进环境变量表)。
本计划不含其执行。

## 实施期风险承接

- T5 唯一高危:replay 测试 + 人工过窗口循环;T6 脚本先 bj9 单机演练
- 任何验收失败停在当前阶段回滚,不推进
- v3→v3.1 修订依据:计划评审(Opus)4 BLOCKER(协调器注册缺失、幂等键
  误伤 Egocentric、建任务入口缺失、G1 与 T1/T4 矛盾)+ 14 IMPORTANT
  (Replayer 用法、真实 history 回放、无 CI、safe-deploy 禁令、重启烧
  attempt 预检、同名函数冲突、ACTIVITY 类型 describe、temporalio 钉版与
  逐机断言、共存期磁盘折算、前缀数据化、T8/T7 顺序、reshard 校验等)全闭合
