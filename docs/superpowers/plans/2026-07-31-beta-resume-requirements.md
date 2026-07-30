# AgiBotWorld-Beta 续传 + 框架升级 — 需求文档

日期: 2026-07-31 · 来源: 用户口述需求 + 历史反复出错问题整理

## 背景

- AgiBotWorld-Beta 数据在 BOS 上已有 **36.5 TB**（`manipulation/AgiBotWorld-Beta-BJ/` 22.44TB + `manipulation/AgiBotWorld-Beta/` 14.11TB），承诺近期下完，不能重复下载。
- bj1-4 原计划专跑 Beta（旧代码），实际已全部换成新 Temporal worker，且 bj2/bj3 被分配了 RoboMIND 任务，bj4 空闲。旧的隔离方案已被破坏，决定：**全面切到新框架，在已有数据基础上续传**。
- 历史反复出现的问题：新旧代码混跑导致假数据/幽灵速度；双状态源（BOS state.json vs SQLite）导致显示时真时假；手动越过系统操作破坏内部状态。

## 需求列表

### R1 — 解除 bj1-4 调度黑名单
`auto_dispatch_pending()` 中硬编码的 bj1-4 blacklist 移除，bj1-4 重新上线为普通调度对象。

### R2 — 指定分片数（shard_count）端到端打通
- `POST /queue/add` 的分片数参数（现有 `split_workers` 被忽略——需查明原因）真正生效：API → task 记录 → workflow → 实际分片数。
- 语义：用户指定 N，则目标 N 个分片；空闲 worker 不足 N 时的行为需明确定义（等待 or 用现有数量，需在方案中定）。

### R3 — 分片数扩容（尽力而为）
运行中的任务从 6 分片扩到 8 分片。若当前 workflow 设计支持（signal / continue-as-new）则做；若改动过大，明确说明不做的原因并给出替代（如暂停后以新分片数重启且续传无损）。

### R4 — BOS 感知续传
- 下载前对照 BOS 已有对象（key + size 匹配则跳过），确保 36.5TB 已传数据不重下、不重传。
- 方案要"聪明、少 bug"：优先在**文件清单生成/分片划分阶段**一次性过滤已完成文件（一次 BOS list 全量对比），而不是每个文件下载前单独发请求（慢且易错）。
- 与现有 `.progress.json` 批次级断点机制兼容。

### R5 — 单一状态源（消灭 state.json 双数据源）
- 盘点所有仍读/写 BOS state.json 的路径（routes/actions.py, routes/storage.py, commands/retry.py, commands/sync.py 及其他）。
- 活跃 web 路径全部迁到 SQLite 或删除；旧 worker daemon 代码路径确认死代码后与调度隔离。
- 目标：dashboard 展示的所有数据只来自 SQLite。

### R6 — 全集群代码版本一致
- 部署前审计所有 worker 的 git commit，统一更新到最新代码后重启 Temporal worker。
- 部署流程输出版本对照表，不允许新旧混跑。

### R7 — Beta 任务通过框架启动（禁止手动）
- 暂停 bj9 的 AgiBotWorld-Alpha（`/queue/preempt` 或框架内暂停机制，保留续传能力）。
- 撤销 bj2 RoboMIND2.0-Tienkung-sim、bj3 RoboMIND-V2-Tienkung（框架 revoke，保留其已下数据与续传能力）。
- 通过 `POST /queue/add` 创建 Beta 任务：ModelScope 源、6 分片、priority 0（插队）、目标 BOS 前缀 = `manipulation/AgiBotWorld-Beta-BJ/`。
- 一切通过 API/Temporal，不 SSH 手动起进程。

### R8 — Beta/ 与 Beta-BJ/ 合并（视调研结果）
- 若两前缀文件布局一致且无冲突（同路径不同大小）：把 `Beta/` 中 Beta-BJ 缺失的对象合并进 `Beta-BJ/`（BOS server-side copy，不动源数据，遵守"永不删除 BOS 数据"）。
- 若布局不一致或有冲突：不合并，直接在 `Beta-BJ/` 上续传，`Beta/` 保留不动。

## 验收标准

- **A1**: `POST /queue/add` 带 `shard_count=6` 创建的任务，实际产生 6 个分片，分布在 6 台不同 BJ worker 上（`GET /api/tasks/{task_id}/shards` 验证）。
- **A2**: Beta 任务启动后，日志/DB 证明已在 BOS 存在的文件被跳过（skipped_existing 计数 > 0，且与 BOS 已有对象数一致量级）；BOS 上不出现同 key 重复上传（对象数增长 = 新文件数）。
- **A3**: bj1-4 出现在 auto_dispatch 候选中（代码里无硬编码 blacklist；bj1-4 收到分片）。
- **A4**: 全部可达 worker 关键文件（activities.py/workflows.py/reconciler.py/queue.py）md5 与 S1 HEAD 一致（rsync 部署无 .git，用哈希清单验证），且只运行 `dlm.temporal` 进程（无 dlm.worker 旧 daemon）。
- **A5**: dashboard API（dashboard/tasks/servers/storage 路由）不再读 state.json（代码审计通过）；页面速度/状态数据全部来自 SQLite。
- **A6**: AgiBotWorld-Alpha 状态为 preempted/paused，staging 与 BOS 数据完好，可后续恢复。
- **A7**: RoboMIND 两任务被框架 pause（可恢复），旧 Beta legacy 任务 revoked（释放 repo_id）；已下数据保留（BOS + staging 不删）。
- **A8**: Beta 聚合下载速度 > 0 且 6 个分片都有独立进度更新（updated_at 持续刷新）。
- **A9**: 扩容采用重启式（`POST /queue/reshard`，terminate→改分片数→重新调度，BOS 过滤保证无损）；运行中动态扩容因 workflow 无 signal 机制不做，已记录原因。
- **A10**: 全程无手动越过框架的操作（除停止旧任务走 API）。

## 硬约束（继承）

- 永不删除 BOS 上已下载的数据。
- 状态只通过 API / StateManager / SQLite 层修改，不裸写。
- 子代理模型：调研 Sonnet，审阅/验收 Opus，禁用 Fable；关键编码由主 agent 完成。
