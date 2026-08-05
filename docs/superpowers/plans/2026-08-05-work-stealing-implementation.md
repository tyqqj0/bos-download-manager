# Work-Stealing 调度 — 实施计划(v3)

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development,
> 逐任务实现 + 逐任务评审。设计依据:`2026-08-05-dynamic-work-stealing.md`(v2,
> 含 R1-R10 需求与 A0-A10 验收)。

**Goal:** 用共享池队列 + 协调器动态窗口替换静态分片,新旧并存灰度上线。

**Architecture:** 见设计 v2。本计划只列"怎么落地":任务边界、文件、接口、
测试与顺序。

## Global Constraints(每个任务隐含遵守)

- **G1** `ShardedDownloadWorkflow` / `ShardWorkerWorkflow` / 其所用活动的签名与
  行为**字节级不动**(replay 安全的充分条件)
- **G2** 存量下载(Egocentric-100K 等)不被终止;需要重启 worker 的任务只在
  批准的部署窗口做,走两阶段部署(先 `--no-restart` 全量同步+清单核对,
  全绿后统一重启)
- **G3** 窗口循环内:`workflow.start_activity`(不 await)、`workflow.wait`
  (禁 `asyncio.wait`)、完成 handle 按批号排序遍历、`h.result()` 逐个
  try/except;取消清理走 `asyncio.shield`
- **G4** 批次终态 failed 只由协调器写;活动异常只 raise
- **G5** 所有新 HTTP 端点/改动端点带 TERMINAL 守卫语义(对齐 /api/shard-progress)
- **G6** `temporalio>=1.9` 钉进 pyproject;新增列走 snapshot.init_db 的
  ALTER 迁移清单
- **G7** 每任务提交前:相关单测通过 + `python -m py_compile` 全部改动文件;
  工作流改动任务必须带 replay 测试

## 任务序列(T1-T11,依赖顺序)

### T1 — DB 与守卫地基(纯后端,无工作流)
**Files:** `dlm/queue/snapshot.py`, `dlm/web/routes/queue.py`, `dlm/web/fleet.py`,
`tests/test_pool_db.py`(新)
**改动:**
1. `tasks` 表迁移加 `dispatch_mode TEXT DEFAULT 'sharded'`、
   `coordinator_phase TEXT`(NULL=无协调器)
2. `get_running_shards()` JOIN tasks,排除父任务
   `status IN TERMINAL_STATUSES`(修存量 bug:取消后 running 行钉死 busy)
3. `/api/shards/assign`、`/api/shards/status` 加父任务 TERMINAL 守卫
   (返回 200 + `{"ignored": true}`,与 shard-progress 行为对齐)
4. `/api/shards/create`:单事务 `executemany` + 单 commit;幂等化——请求带
   `expected_count`,若现存行数=期望且 filelist_key 全匹配则直接返回成功
   (Temporal 重试不再先删后建);`create_shards_in_db` 活动客户端超时 30→120s
5. `_aggregate_task` 改单条 SQL(SUM/COUNT FILTER)+ 每任务 5s 去抖
   (进程内 dict[task_id]→last_ts)
**Tests:** 迁移幂等;终态父任务的 assign/status/progress 全被忽略;
running 行在父任务 paused 后不再计入 get_running_shards;create 幂等重放;
aggregate 数值与逐行求和一致。
**验收挂钩:** A8。

### T2 — 注册表同步 + 一致性测试
**Files:** `dlm/web/temporal_client.py`, `dlm/web/fleet.py`,
`tests/test_workflow_registry.py`(新)
**改动:**
1. `WORKFLOW_TYPES` += `"PoolDownloadWorkflow"`;`PARENT_WORKFLOW_TYPES` 同步
2. `has_live_workflow()` += `pool-{task_id}` 模式
3. 一致性测试:import `dlm.temporal.workflows`,收集全部 `@workflow.defn`
   类名,断言 ⊆ `WORKFLOW_TYPES`(防第三次漂移)
**注意:** 本任务先于 T6 提交会让一致性测试立即通过(Pool 类尚不存在时断言
仍成立);T6 合入后测试自动覆盖新类。
**验收挂钩:** A3、A9。

### T3 — 新活动:pool_alive_workers / chunk_filelist
**Files:** `dlm/temporal/activities.py`, `dlm/web/routes/queue.py`(新端点
`/api/pool/alive-workers`), `tests/test_chunk_filelist.py`(新)
**接口:**
- `pool_alive_workers(source: str) -> int`:HTTP 问 S1,返回
  alive ∩ worker_serves(source) 数量,**不看 busy/磁盘**;0 时协调器进入
  等待重查循环(不派批)
- `chunk_filelist(filtered_path, task_input) -> {"batch_keys": [...], "counts": [...], "bytes": [...]}`:
  钉 listing 队列;规则:单批 ≤500 文件 **且** ≤32GiB(体积上限优先,
  批数无 400 硬顶;>32GiB 单文件独占一批);逐批清单上传
  `META_BUCKET:download-manager/batchlists/{task}/batch-{i}.json`;
  并发心跳(复用 filter 的模式)
**Tests:** 切批规则(混合大小/巨文件/空清单);批清单往返(上传→下载→内容一致);
批数与事件预算断言(1,500 批内)。
**验收挂钩:** A1(切批正确是打满的前提)。

### T4 — 新活动:run_pool_batch
**Files:** `dlm/temporal/activities.py`, `tests/test_run_pool_batch.py`(新)
**行为(顺序即规范):**
1. `rm -rf` 自身 staging 目录(`/data/staging/{shard_task_name(task,i)}/`)
2. HTTP `assign_shard_server`(带 server_key)+ `update_shard_status(running)`
   ——被 TERMINAL 守卫拒绝(`ignored:true`)时**直接正常返回**
   (任务已被停,不做任何下载)
3. 磁盘预检:不足时 raise `_RetryableDiskLow`(可重试,靠 5min 退避等)
4. 从 BOS 拉批清单 → **无条件 HEAD-skip**(≤500 个并发 HEAD,跳过
   key+size 已匹配的文件)→ `PipelineEngine.run(剩余文件)`
5. 成功:清 staging → HTTP 报 done(经守卫端点)→ return 统计
6. 异常:清 staging(保留无意义,HEAD-skip 顶替断点)→ **raise**
   (不写 failed,G4);失败消息含 server_key
7. `_speed_reporter` 循环体 try/except(心跳源自保护)
**Tests:** mock PipelineEngine:HEAD-skip 过滤正确;ignored 短路;
异常路径不写状态且 staging 清理;成功路径状态链完整。
**验收挂钩:** A5、A7、A8。

### T5 — PoolDownloadWorkflow(核心,最高评审强度)
**Files:** `dlm/temporal/workflows.py`(只增不改旧类),
`tests/test_pool_workflow_replay.py`(新,temporalio testing env)
**实现:** 设计 v2 §2.3 的骨架**逐条照抄**,要点:
- list → filter → chunk → create_shards_in_db → 窗口循环 → 末轮 failed 补投
  一次 → 终态报告 + aggregate
- `record_batches_and_window(task_id, results) -> int`(新活动):写批次终态
  (done/failed)+ 重算窗口 `max(1, floor(P × W_i / Σ W_active))`
  (P 由 pool_alive_workers 重查,W_P0=1.5/W_P1=1.0,活跃任务集由 S1 查);
  每醒一次恰好调用一次(history ~6 事件/批)
- 活动配置:`schedule_to_close=48h / start_to_close=12h / heartbeat=10min /
  retry(initial=5min, max_attempts=3) / WAIT_CANCELLATION_COMPLETED /
  priority=Priority(priority_key=1 if P0 else 3)`
- `except asyncio.CancelledError:` → shield 调 `release_batches`(在飞批行→
  pending/NULL/0)→ re-raise
- workflow 经活动写 `coordinator_phase`(listing/filtering/chunking/dispatching)
**Tests(replay 为主):** 录一次完整小任务 history → 回放断言无
NonDeterminismError;窗口=1 边界;全批 done、部分 failed、补投成功/失败四种
终态;取消路径 shielded 清理被调用。
**验收挂钩:** A1-A5、A9。

### T6 — Worker 进程与部署门禁
**Files:** `dlm/temporal/__main__.py`, `scripts/deploy-workers.sh`,
`pyproject.toml`
**改动:**
1. 第三个 Worker:`task_queue=pool-{source_for_worker(key)}`,
   **`workflows=[]`**、`activities=[run_pool_batch]`、`max_concurrent_activities=1`
2. `pyproject.toml`:`temporalio>=1.9`
3. deploy-workers.sh:清单改为全量
   `find dlm -name '*.py' | sort | xargs md5sum | md5sum`(单值);
   新增 `--sync-only-then-restart` 两阶段模式:全 16 台 rsync+清单,
   **任一不绿则不进入重启阶段**并 exit 1
**Tests:** 本地起 worker 进程断言三队列注册(单测 mock);deploy 脚本
shellcheck + 干跑(--no-restart)。
**验收挂钩:** A6、A9、A0(两阶段部署保护存量任务)。

### T7 — 派发收口 + 准入 + 模式门禁
**Files:** `dlm/web/temporal_client.py`(新 `start_download(task)`),
`dlm/web/reconciler.py`, `dlm/web/routes/doctor.py`, `dlm/web/routes/queue.py`
**改动:**
1. `start_download(task)`:按 `dispatch_mode` 分流到
   start_sharded_download / start_pool_download(新,workflow id=`pool-{task_id}`);
   pool 模式先 `describe_task_queue(pool-{source})`,轮询者 < 池预期 →
   拒绝并告警(模式门禁)
2. **四处调用点全部改走 start_download**:reconciler.reconcile(:126)、
   auto_dispatch_pending(:273)、doctor.fix(:206)、queue.preempt(:317);
   grep 断言仓库内无其他 `start_sharded_download(` 直呼
3. 准入:pool 模式下 auto_dispatch 改为"每 source 并发 downloading 池任务
   ≤ `POOL_MAX_CONCURRENT_TASKS`(默认 4)";listing guard 判据改
   `coordinator_phase IN ('listing','filtering','chunking')`(替代
   NOT EXISTS(shards))
4. preempt 修 `victim["server"]` NULL 崩溃:pool/sharded 任务 cancel 即释放,
   不再要求 server 字段
**Tests:** 分流正确;门禁拒绝路径;准入上限;guard 在批行残留时仍有效。
**验收挂钩:** A2、A3、A9。

### T8 — 停控与回滚链路
**Files:** `dlm/web/temporal_client.py`, `dlm/web/routes/queue.py`
**改动:**
1. `cancel_workflow` / `terminate_workflow_and_wait`:`dispatch_mode='pool'`
   时只处理父 `pool-{task_id}`,跳过逐 shard 句柄展开;describe 轮询加
   每轮 15s 预算
2. `/queue/pause`:成功 cancel 后
   `UPDATE shards SET status='pending', server=NULL, speed_mbps=0
   WHERE task_id=? AND status='running'`(与协调器 shielded 清理双保险)
3. `/queue/resume`:置 pending 时 `delete_shards_by_task`(对齐 reshard,
   消灭陈行)
4. `/queue/reshard`:请求体接受 `dispatch_mode`,UPDATE 写回(回滚载体)
**Tests:** pool 任务 pause→60s 内无 running 行;resume 后无陈行;
reshard 改模式往返。
**验收挂钩:** A3、A9(回滚演练)。

### T9 — 可观测性改造
**Files:** `dlm/web/reconciler.py`, `dlm/web/routes/doctor.py`,
`dlm/web/alerts.py`, `dlm/web/health_verifier.py`, `dlm/web/scheduler.py`,
`dlm/web/static/`(app.js/index.html)
**改动:**
1. 池巡检:reconciler 对 pool+协调器存活的任务 `handle.describe()`,
   pending_activities 全 SCHEDULED>15min 或 attempt 爬升无完成 → alert
   (`pool_starved`);替代 1800s 孤儿逻辑(pool 任务不走全量重派发,
   除非协调器真死)
2. doctor/detect_idle_workers/health_verifier:忙信号 = "该 server 有
   running 批行 或 running shard 行";`download-{key}` 工作流探测仅对
   sharded 任务保留
3. task_stuck/stale 豁免:0 running 批行 + 协调器存活 + 池巡检无告警 =
   正常排队,不算 stuck
4. dashboard 后端:`shard_servers` 按 server 聚合(count + Σspeed),
   `dl["server"]` 去重;前端 shards 弹窗:>32 行时按状态聚合显示 +
   仅展开异常行
5. 周期 staging GC(scheduler 300s 层):对非活任务的 staging 目录
   fan-out 清理(复用 cleanup_staging 活动,仅 done/failed/revoked 任务)
**Tests:** 巡检触发/不触发矩阵;busy 信号单测;dashboard 聚合数值。
**验收挂钩:** A10。

### T10 — 集成测试与试点(在真实集群,人批准后执行)
**步骤:**
1. 两阶段部署(G2;Egocentric 照常,重启无损)
2. 挑一个 100-300GB 的 MS 小数据集,手动 `dispatch_mode='pool'` 建任务 →
   跑完 A1/A5/A6/A7 验收
3. 双任务优先级实测(再起一个 P1 小任务)→ A2/A3;顺带验证
   priority_key 是否生效(无效不阻塞,窗口是主机制)
4. kill 单机 worker → A4;停全池 worker 5 分钟 → A10 巡检告警
5. 回滚演练:半程任务 reshard 回 sharded → A9
6. 全部绿 → `DLM_DEFAULT_DISPATCH_MODE=pool`;观察 48h;A0 全程核对
**产物:** `docs/reports/` 验收报告(A0-A10 逐条证据)。

### T11 — 收尾(默认切换 ≥1 周后,单独批准)
删除 sharded 路径 + 兼容代码;文档与 CLAUDE.md 更新。**本计划不含其执行。**

## 顺序与依赖

T1 → T2 →(T3、T4 可并行)→ T5 → T6 →(T7、T8、T9 可并行)→ T10 → T11。
T1-T9 均为纯代码任务,可在不部署的情况下全部完成与评审;
集群只在 T10 碰,且有 G2 硬约束。

## 明确风险承接(实施期)

- T5 是唯一高危任务:评审强度最高(replay 测试必须过 + 人工过窗口循环代码)
- T6 部署脚本改动先在 `--worker bj9` 单机演练再全量
- 任何验收失败:停在当前阶段回滚,不推进
