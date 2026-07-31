# AgiBotWorld-Beta 续传 + 框架升级 — 实现计划 (v2, 审阅后修订)

> **STATUS: 已完成并验收 (2026-07-31)**。验收 A1-A10 全部通过。Beta 任务 t-20260730-c4caf4 以 6 分片（bj1/2/3/4/5/7）~1.45Gbps 续传中，BOS 过滤跳过 948 文件/27.9TB。执行中发现并追加修复了 3 个计划外缺陷：①进度上报复活 paused 任务（servers.py 状态守卫）②带后缀 legacy workflow ID 逃过取消（temporal_client 子串扫描）③速度只按上传统计致大文件期间显示 0（pipeline 按下载活动计速）。

> 需求与验收标准见同目录 `2026-07-31-beta-resume-requirements.md`（本次修订同步更新 A1/A4/A7 表述）。
> v1 审阅结论 NEEDS_REVISION，4 CRITICAL + 9 IMPORTANT/MINOR 已全部吸收，对应修订标注 [R#n]。

**Goal:** 让 AgiBotWorld-Beta 在 6 台 BJ worker 上通过新框架续传（跳过 BOS 已有的 22.6TB），同时修复 shard_count 参数、解除 bj1-4 黑名单、消灭 state.json 双数据源、统一全集群代码版本。

**Architecture:** 复用 tasks 表已有的 `max_workers` 列作为用户指定分片数；在 `ShardedDownloadWorkflow` 的 list→partition 之间插入 BOS 对比过滤 activity（全链 pin 到列表 worker 的个人队列）；所有 dispatch 路径（auto_dispatch / reconcile 孤儿重派 / preempt）统一走 sharded；分片扩容通过 terminate→改 max_workers→重新调度实现，分片进度标记带 filelist 内容哈希防陈旧。

**调研背景（已确认事实，v1 全文保留 + 审阅补充）:**
- `split_workers` 在 commit `a6ae1b6` 断掉：`queue.py:85` 读参数但不入库；`ShardedDownloadWorkflow` 按 idle worker 数自动定分片，无 signal/continue-as-new。
- BOS：`manipulation/AgiBotWorld-Beta-BJ/` 476 对象 22.59TB（仅 observations/*.tar + 2 元文件）；`manipulation/AgiBotWorld-Beta/` 631 对象 14.11TB（task_info 217 / parameters 35 / proprio_stats 7 / observations 361 + 9 个 .cache 垃圾）。重叠 82：79 相同；`.gitattributes`/`README.md` 不同（保 BJ）；`observations/709/864860-911899.tar` 差 19.5MB（真冲突，保 BJ、跳过）。Beta/ 独有 549 对象 10.26TB，平均 ~19GB/对象 → **几乎全部超 5GB CopyObject 上限，必须 multipart copy** [R#8]。逐文件清单在 S1 `/root/agibot_compare.json`。
- 心跳全走 SQLite；旧 daemon 死代码。`actions.py` **未挂载**（app.py 不 include），其 `/api/sync` 由 `queue.py:527` 的 stub 服务、`app.js:111` 在调 [R#15]。
- 版本审计：S1 落后 origin 5 提交且 activities.py 脏；w1=脏快照；w2/3/5/7=混合；w4/w6=S1 HEAD；bj1-4=7-21 冻结。部署=rsync 无 .git → **A4 改为文件哈希清单验证** [R#11]。
- `download-workers` 队列：w* worker（无 --task-queue 启动）提供；bj 只听 `download-bjN`；S1 无 temporal worker。**协调器上的 activity 不指定 task_queue 时会被任意 w* 抢走** —— filelist 只在列表 worker 本地盘上 [R#1]。
- `auto_dispatch_pending` 在 `RECONCILE_INTERVAL=300s` 的分支里，不是 10s [R#6]。
- `auto_dispatch` 先 `UPDATE tasks SET status='downloading', server=<key>` 再起协调器 → idle 查询把该 worker 算 busy → 6 台只出 5 分片 [R#2]。
- 分片断点 `.progress.json` 是**位置式**（`start_idx = len(markers) * 500`），过滤/重分片后清单变化 → 陈旧标记会静默跳过未下载的文件 [R#3]。
- `reconcile()` 孤儿重派（reconciler.py:126）和 `/queue/preempt`（queue.py:311）仍走 legacy `start_download` → 无 BOS 过滤 [R#4]。
- `cancel_workflow` 是异步 cancel，不等关闭 [R#12]。`queue.py` 模块层未 import time [R#13]。
- `upsert_task` 全列覆盖 → max_workers 用 COALESCE 防误清 [R#14]。
- Shard ID 派生 `s-{task_id}-{index}`，`cancel_workflow` 的 pattern 覆盖 `sharded-`/`shard-`。
- dup-check 拦截所有非 (failed/revoked/done) 状态 → **paused 也拦截**，旧 Beta 任务必须 revoke（skip）而非 pause；Alpha/RoboMIND 用 pause（可恢复，无同 repo 新任务需求）[R#20]。
- 真实分片查询端点是 `GET /api/tasks/{task_id}/shards` [R#17]。UI 暂无 shard_count 输入，验收用 curl [R#18]。

## Global Constraints

- 永不删除 BOS 数据；合并只 server-side copy。
- 状态只经 API / snapshot 层修改；操作走框架 API。
- 子代理：调研 Sonnet、审阅 Opus、禁 Fable；编码主 agent 完成。

---

### Task 1: shard_count 端到端（复用 `max_workers`）

**Files:** `dlm/queue/snapshot.py`, `dlm/web/routes/queue.py`, `dlm/temporal/models.py`, `dlm/web/temporal_client.py`, `dlm/temporal/workflows.py`

1. `snapshot.py` `upsert_task`：columns 加 `"max_workers"`；DO UPDATE SET 对该列用 `max_workers=COALESCE(excluded.max_workers, tasks.max_workers)` [R#14]。
2. `queue.py` add：`shard_count = int(body.get("shard_count", body.get("split_workers", 0)) or 0)` → `task_meta["max_workers"] = shard_count`；模块层补 `import time` [R#13]。
3. `models.py` TaskInput 加 `shard_count: int = 0`。
4. `temporal_client.py` `start_sharded_download`：`shard_count=int(task_dict.get("max_workers") or 0)`。
5. `workflows.py` Step 3：
```python
requested = getattr(task_input, "shard_count", 0) or 0
if requested > 0:
    num_shards = max(1, min(requested, len(idle_workers) or 1))
elif total_bytes < AUTO_SHARD_THRESHOLD or len(idle_workers) <= 1:
    num_shards = 1
else:
    num_shards = min(len(idle_workers), max(1, total_bytes // SHARD_MIN_BYTES))
```
   phase 消息注明 `requested=N got=M`。
6. Commit: `feat: shard_count end-to-end (API → DB → workflow)`

### Task 2: 统一 sharded dispatch + 移除黑名单 + 关闭调度竞态

**Files:** `dlm/web/reconciler.py`, `dlm/web/routes/queue.py`

1. 删 `reconciler.py:229-233` bj1-4 blacklist。
2. `auto_dispatch_pending` 统一 `start_sharded_download(task)`，删 legacy 分支；**claim 时 `server=NULL`**（只改 status），server 由分片各自记录 [R#2]：
```python
cursor = conn.execute(
    "UPDATE tasks SET status = 'downloading', server = NULL, updated_at = ? "
    "WHERE id = ? AND status = 'pending'", (time.time(), task["id"]))
```
3. `/api/shards/idle-workers` 加 `exclude_task` 参数：`busy_from_tasks`/`busy_from_shards` 计算时排除该 task 的行；`query_idle_workers` activity 与 workflow 传 `task_id`（双保险）[R#2]。
4. 关闭多协调器竞态 [R#7]：dispatch 前检查——同 source 存在 "status='downloading' 且无 shard 行" 的任务（协调器仍在 listing/过滤阶段）→ 本轮跳过该 source 的 dispatch：
```python
def _sources_in_listing_phase(conn) -> set:
    rows = conn.execute(
        "SELECT DISTINCT t.source FROM tasks t "
        "WHERE t.status='downloading' "
        "AND NOT EXISTS (SELECT 1 FROM shards s WHERE s.task_id = t.id)"
    ).fetchall()
    return {r[0] for r in rows}
```
5. `reconcile()` 孤儿重派与 `/queue/preempt` 的 `start_download` 全部改 `start_sharded_download` [R#4]（legacy `DownloadDatasetWorkflow` 保留代码但不再有调用方；下轮清理可删）。
6. 把 `auto_dispatch_pending()` 从 300s reconcile 分支移到 scheduler 的 30s 子周期（新增 `DISPATCH_INTERVAL = 30`），保持 reconcile 本体 300s [R#6]。
7. Commit: `feat: unified sharded dispatch — no blacklist, no self-exclusion, no coordinator race`

### Task 3: BOS 感知续传（pin 队列 + 独立过滤文件 + 哈希化断点）

**Files:** `dlm/temporal/activities.py`, `dlm/temporal/workflows.py`, `dlm/temporal/__main__.py`

1. 新 activity `filter_filelist_against_bos(filelist_path, task_input) -> dict`（代码同 v1，两处修改）：
   - 输出写 **`.filelist.filtered.json`**（不覆盖原清单），返回值加 `"filtered_path"` [R#16]。
   - bucket/prefix 规则与 pipeline `_init_bos_client` 一致。
2. `ShardedDownloadWorkflow`：
   - `list_repo_files` 返回的 `worker_queue` 记为 `listing_queue`。
   - 过滤 activity 与 **`partition_files_greedy` 都加 `task_queue=listing_queue`** [R#1]。
   - partition 的输入改为 `filter_result["filtered_path"]`。
   - `remaining_files == 0` → 直接 done（消息含 skipped 数）。
   - dispatching phase 消息带 `skipped N files (X GB) already on BOS`。
3. 断点标记哈希化 [R#3]：
   - `partition_files_greedy` 返回值每分片加 `"filelist_md5"`（写 BOS 前对 shard 清单内容算 md5）。
   - `ShardInput` 加 `filelist_md5: str = ""`。
   - 进度文件格式升级为 `{"filelist_md5": h, "batches": [...]}`；`load_progress`/保存标记的 activity 加 md5 参数：读取时 md5 不匹配或旧格式（裸 list）→ 视为无进度并清除。`ShardWorkerWorkflow` 传 `shard_input.filelist_md5`。
4. `__main__.py` 注册 `filter_filelist_against_bos`。
5. Commit: `feat: BOS-aware resume — queue-pinned filter, hash-guarded batch markers`

### Task 4: 重启式分片扩容 `/queue/reshard`

**Files:** `dlm/web/routes/queue.py`, `dlm/web/temporal_client.py`

1. `temporal_client.py` 新增 `terminate_workflow_and_wait(task_id, timeout_s=120)`：对 `sharded-{id}`/`dl-{id}`/`split-download-{id}` 及全部 `shard-{sid}` handle 调 `terminate()`，然后轮询 `describe()` 直到全部 closed 或超时（返回是否干净关闭）[R#12]。
2. `/queue/reshard`：先 `snapshot.get_task` 校验存在且 status ∈ (downloading, pending, paused)；再 terminate+wait；成功后一个事务里 `DELETE FROM shards WHERE task_id=?` + `UPDATE tasks SET status='pending', server=NULL, max_workers=?`；未干净关闭则返回错误不改状态 [R#12][R#13]。
3. Commit: `feat: /queue/reshard — terminate-and-requeue shard expansion`

### Task 5: state.json 单一数据源清理（web 域收敛）

**Files:** `dlm/web/routes/storage.py`, delete `dlm/web/routes/actions.py`, `dlm/cli.py`, delete `dlm/commands/{sync,retry,dispatch,doctor,verify,watch,transfer}.py`, delete `dlm/worker/daemon.py` + `dlm/worker/heartbeat.py`

1. `storage.py`：registered/task_status 查询改 `snapshot.get_all_tasks()` 映射；`register_bos_data` 改 `snapshot.upsert_task()`。
2. 删除 `actions.py`（未挂载死代码）；**不动** `queue.py` 的 `/api/sync` stub（app.js 在调）；scheduler.py 无需改 [R#15]。
3. `cli.py` 删死命令注册 + import；删对应 commands 文件；删 worker CLI 组与 daemon/heartbeat 文件（handlers/movers 保留——实施时复核 temporal/web 无 import 后决定去留，初步 grep 无引用）。
4. **保留** `dlm/core/state.py` + migrate 脚本（migrate 一次性历史脚本 import StateManager，删 state.py 会炸 cli import）[R#10]。A5 验证改为：`grep -rn "StateManager\|core\.state" dlm/web/ dlm/temporal/` 为零。
5. Commit: `refactor: web/temporal single state source — remove legacy state.json paths`

### Task 6: BOS 合并脚本（multipart copy + ModelScope 尺寸门控）

**Files:** `scripts/merge_beta_to_bj.py`

1. 先取 ModelScope 清单 [R#9]：脚本内用 requests 直调 ModelScope dataset files API（与 `activities._list_modelscope` 同端点分页），得 `ms_size = {path: size}`。
2. 目标集 = Beta/ 独有路径，排除 `.cache/*`、3 个冲突路径。每个候选：
   - `path in ms_size and ms_size[path] == beta_size` → copy（续传过滤会跳过它）。
   - `path not in ms_size` → copy（MS 仓库没有的额外数据，无重下风险）。
   - `path in ms_size and 尺寸不匹配` → **跳过并计数报告**（copy 了也会被重下覆盖，浪费）[R#9]。
3. Copy 实现 [R#8]：≤5GB 用 `copy_object`；>5GB 用 multipart：`initiate_multipart_upload` → 按 ≤1GB range `upload_part_copy` → `complete_multipart_upload`；失败 abort；幂等（copy 前 head 目标，存在且同尺寸跳过）。
4. `--dry-run` 默认：打印各分类计数（will-copy / size-mismatch-skip / conflict-skip / junk-skip）与总量；`--execute` 执行；绝不删源。
5. 执行后重 list 验证对象数与总量。
6. Commit: `feat: merge script — MS-size-gated multipart server-side copy, no deletes`

### Task 7: 版本收敛 + 部署脚本修复 + 全集群部署

**Files:** `scripts/deploy-workers.sh`, `scripts/start-temporal-worker.sh`

1. `deploy-workers.sh`：WORKERS 表加 bj1-4（IP 已知）与 bj5-9（实施时从 S1 heartbeat/ssh config 确认 IP）；每主机带 `TASK_QUEUE` 变量（bjN → `download-bjN`，wN → 空=默认 download-workers）[R#11]。
2. `start-temporal-worker.sh`：接受 `TASK_QUEUE` 参数，非空时传 `--task-queue` [R#11]。
3. 本地 commit + push → S1 `git stash && git pull`（stash 内容对比确认无需抢救）→ rsync 到全部 worker → 按第 8 步顺序重启。
4. 验证：每台 md5 对比 `activities.py`/`workflows.py`/`reconciler.py`/`queue.py` 与 S1 HEAD 一致；`ps` 全部 `dlm.temporal`；输出版本对照表（A4 的哈希清单形式）[R#11]。

### Task 8: 任务编排（顺序修订 [R#5]）

1. **合并先行**：Task 6 脚本 dry-run → execute（纯 BOS 操作，与 worker 无关，先做且耗时最长）。
2. 现场快照：`GET /api/dashboard`、`GET /api/queue/pending` 留档。
3. **清场 pending**：列出全部 pending 的 modelscope 任务并逐个 pause（防止 dispatch 抢占释放出的 bj worker）[R#5]。
4. 停 incumbents：
   - Alpha（bj9, t-20260729-cde1dc）→ `POST /queue/pause`（可恢复）。
   - bj2 RoboMIND2.0-Tienkung-sim、bj3 RoboMIND-V2-Tienkung → `POST /queue/pause` [R#20]。
   - bj1 legacy Beta t-20260722-98cb41 → `POST /api/tasks/{id}/skip`（revoked，释放 repo_id）[R#20]。
5. 部署 + 重启全部 worker + S1 web（Task 7）。
6. 预检 [R#19]：`GET /api/dashboard` 确认 bj1-4/5/9 disk_free > 70GB（bj1 337G 免清理；不满足者再考虑 `POST /api/servers/{key}/cleanup`——只清 revoked 任务 staging）。
7. 创建任务（repo_id 从旧任务 DB 记录读）：
```bash
curl -X POST http://154.85.43.52:8080/api/queue/add -H 'Content-Type: application/json' -d '{
  "repo_id": "<t-20260722-98cb41.repo_id>", "name": "AgiBotWorld-Beta-BJ",
  "type": "dataset", "category": "manipulation", "source": "modelscope",
  "priority": 0, "shard_count": 6}'
```
8. 观察 dispatch（30s 周期）：协调器启动 → filter 报 skipped ≈ 已有对象数（用 Task 6 的 MS 清单预先算出期望值做对照 [R#9]）→ `GET /api/tasks/{id}/shards` 确认 6 分片 6 台 bj。
9. 观察 30 分钟：分片速度 > 0、BOS 对象数只增不重。
10. 恢复清场时 pause 的 pending 任务（`/queue/resume`），让剩余 bj worker 继续消化。

### Task 9: 验收（Opus subagent）

按修订后 A1-A10 逐条验证，产出报告；未达标回对应 Task。

---

## 需求文档同步修订

- A1 验证端点改 `GET /api/tasks/{task_id}/shards` [R#17]。
- A4 改为"文件哈希清单一致"（rsync 部署无 .git）[R#11]。
- A7 改为"RoboMIND 两任务 paused（可恢复），旧 Beta 任务 revoked"[R#20]。
- A9 明确为重启式扩容（/queue/reshard）。

## Self-Review 记录 (v2)

- 4 CRITICAL 全部有对应修订：#1→Task 3.2（pin 两个 activity + filtered 独立文件）；#2→Task 2.2/2.3（server=NULL + exclude_task）；#3→Task 3.3（md5 门控断点）；#4→Task 2.5（统一 sharded）。
- #7 竞态用"listing 阶段 source 冷却"关闭；#12 用 terminate+wait；#5 顺序改为合并先行+清场 pending。
- 冲突 tar 保 BJ 版不合并；HF 独有但 MS 尺寸不符的对象不合并（省 copy，重下覆盖同结果）。
