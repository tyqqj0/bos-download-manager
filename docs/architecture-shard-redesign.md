# DLM 架构重构方案：分片下载 (Shard-Based Download)

> **日期**: 2026-07-29
> **状态**: 评审完成，待修订
> **作者**: Claude (协助 tyqqj0)
> **关联分支**: feat/architecture-upgrade

## 一、背景与动机

DLM 当前采用 "1 任务 = 1 Worker" 模型（SplitDownloadWorkflow 是手动特例）。在管理 TB 级数据集下载时，暴露了以下根本性问题：

1. **大数据集只能单机下载**，无法利用全部 worker 并行加速
2. **Split 子任务对中心调度器不可见**，导致 auto_dispatch 误判 worker 空闲、分配冲突任务
3. **无分片级的进度跟踪与断点续传**，split 子任务心跳静默失败
4. **源路由混乱**，reconciler 用旧 server 字段重新派发，可能跨源

本方案将下载的基本调度单位从 "Worker" 改为 "Shard"（分片），实现自动分片、动态调度、分片级健康检查。

---

## 二、当前架构

### 2.1 系统拓扑

```
                        S1 (154.85.43.52)
                ┌──────────────────────────────┐
                │  Temporal Server (:7233)      │
                │  FastAPI Dashboard (:8080)    │
                │  SQLite (tasks + workers)     │
                │  Reconciler (每5分钟)          │
                │  Auto-dispatch (每5分钟)       │
                └──────────┬───────────────────┘
                           │ gRPC
          ┌────────────────┼────────────────┐
    bj1-bj4 (ModelScope)           w1-w7 (HuggingFace)
    每台监听2个队列:                 每台监听2个队列:
    - "download-workers" (共享)     - "download-workers" (共享)
    - "download-{key}" (个人)       - "download-{key}" (个人)
    max_concurrent_workflow=1       max_concurrent_workflow=1
```

### 2.2 数据流

```
pending → auto_dispatch → status=downloading, server=wX → Temporal workflow → pipeline(download+upload) → status=done
```

### 2.3 数据库

**tasks 表**: id, name, repo_id, source, type, category, bos_path, status, server, priority, size_gb, downloaded_gb, progress_pct, speed_mbps, phase, error, error_class, retry_count, celery_task_id, transfer_*, timestamps, updated_at

**workers 表**: hostname, server_key, status, current_task_id, disk_free_gb, last_seen

**无 shard/分片 表。**

### 2.4 Pipeline 引擎

```
PipelineEngine.run() 启动 3 个协程:
  Producer: Semaphore(4) → hf_hub_download → stall 检测 → FileInfo 入队
  Consumer: Semaphore(8) → BOS upload → 删除本地文件
  SpeedReporter: 每15s 上报速度
  
磁盘反压: <30% free 或 <20GB → 暂停 producer
```

**关键常量**: DOWNLOAD_WORKERS=4, UPLOAD_CONCURRENCY=8, STALL_TIMEOUT=600s, HTTP_TIMEOUT=300s, MAX_FILE_RETRIES=3, BATCH_SIZE=500

---

## 三、已知问题清单

### A. 架构级缺陷

| # | 问题 | 影响 | 根因 |
|---|------|------|------|
| A1 | Split 子任务对中心不可见 | bj2/3/4 被当空闲，分配冲突任务 | SplitDownloadWorkflow 不创建 SQLite 行 |
| A2 | 1 任务 = 1 Worker，无自动并行 | 大数据集只用 1 台机器 | 无自动分片机制 |
| A3 | 心跳上报静默失败 | split 子任务进度不可见 | report_to_dashboard UPDATE 匹配 0 行 |
| A4 | 数据库无分片概念 | 无法跟踪子任务状态 | 只有 tasks 表 |

### B. 调度级缺陷

| # | 问题 | 影响 | 根因 |
|---|------|------|------|
| B1 | busy_servers 来源不完整 | 只看 SQLite，不知 split 子任务 | Split 子任务不在 SQLite |
| B2 | Reconciler 重用旧 server 字段 | 可能派到错的机器 | 孤儿任务保留原 server |
| B3 | Reconciler 不检查磁盘 | 可能派到磁盘满的 worker | 只有 auto_dispatch 检查 70GB |
| B4 | Preempt 有 TOCTOU | 抢占时可能读到过时状态 | read-then-write 无乐观锁 |
| B5 | 两个添加任务接口 | 去重逻辑不一致 | /tasks 和 /queue/add 各一套 |

### C. 下载级缺陷

| # | 问题 | 影响 | 根因 |
|---|------|------|------|
| C1 | Xet TCP 卡死 | 下载速度减半 | xet-core #789，HF_XET_HIGH_PERFORMANCE=1 加剧 |
| C2 | 无 429 检测 | 限速当普通错误 | 未解析 Retry-After header |
| C3 | hf_transfer 已废弃 | deprecation warning | 已合入 Xet |
| C4 | Mirror 失效 | hf-mirror.com fallback=None | Mirror 已关闭 |

### D. 运维级问题

| # | 问题 | 当前状态 |
|---|------|---------|
| D1 | bj2/3 跑着 404 任务 | ego4d/nuplan 无效 repo |
| D2 | w1/w4 磁盘 100% 满 | 无法下载 |
| D3 | 6 台 worker 空闲 | 利用率极低 |
| D4 | HF_HUB_DISABLE_XET=1 未部署 | HK workers 速度减半 |

---

## 四、新架构设计

### 4.1 核心模型：Task → Shard → Worker

```
旧: 1 Task ──→ 1 Worker
新: 1 Task ──→ N Shards ──→ N Workers（自动）

Task (AgiBotWorld-Beta, 50TB, 8000 files)
  ├── Shard 0: files[0:2000]    → w1  (12.5TB)
  ├── Shard 1: files[2000:4000] → w3  (12.5TB)
  ├── Shard 2: files[4000:6000] → w5  (12.5TB)
  └── Shard 3: files[6000:8000] → w7  (12.5TB)
  coordinator 等所有 shard done → task done
```

### 4.2 分片策略

#### 何时分片

| 条件 | 策略 |
|------|------|
| 总大小 < 10GB | 不分片，单 worker |
| 总大小 ≥ 10GB | 自动分片 |
| 用户指定 max_workers=1 | 强制单 worker |

#### 如何分片

1. `list_repo_files` → 获取所有文件路径 + 大小
2. 按大小降序排序
3. **贪心装箱**: 大文件优先放入当前总量最小的 shard
4. 分片数 = `min(可用兼容 worker 数, max_workers, 总大小 / 5GB)`
5. 每个 shard 最小 5GB（避免过度碎片化）

#### 为什么不是越多越好

- 分片数 = 可用 worker 数是最优解
- 超过 worker 数的 shard 只能排队，增加调度开销
- **断点续传是文件级的**（每文件下完标记 done），不取决于 shard 数量

### 4.3 优先级与排队

```
优先级模型:
  Task.priority (P0-P3) → 所有 shard 继承 task priority
  调度顺序: priority ASC → created_at ASC

抢占机制:
  P0 任务提交 → 暂停 P2+ 的 shard (status=paused)
  → 释放的 worker 接 P0 的 shard
  → P0 完成 → paused shard 自动恢复
```

### 4.4 Worker 容量

**1 Worker = 1 Shard（保持简单）**

理由：
- Pipeline 已经 4 并发下载 + 8 并发上传，磁盘 I/O 饱和
- 多 shard 竞争磁盘带宽反而更慢
- 简化调度，避免资源竞争

### 4.5 max_workers（每任务最大并发 worker 数）

```json
POST /api/tasks {
    "repo_id": "...",
    "max_workers": 3,
    "priority": "P1"
}
```

- 默认 0 = 不限，使用所有兼容空闲 worker
- 不紧急任务设 max_workers=2，留余量
- P0 紧急任务不限，全部 worker 出动

### 4.6 动态反压

```
三层反压:

Layer 1 — 文件级 (pipeline 内部，已有):
  磁盘 < 30% free 或 < 20GB → 暂停 producer
  consumer 上传释放空间后恢复

Layer 2 — Shard 级 (新增):
  Worker 磁盘 < 70GB → 不接受新 shard (MIN_DISPATCH_DISK_GB)
  Shard 速度持续 = 0 超过 10 分钟 → 标记 stalled
  Stalled shard 的剩余文件可重新分配

Layer 3 — 任务级 (新增):
  所有兼容 worker 磁盘不够 → 任务 paused
  有 worker 释放空间 → 自动恢复
```

### 4.7 健康检查

```
Shard 心跳: 每个 shard 独立上报 (速度, 已下载/已上传文件数, 磁盘)
Shard 健康:
  - 无进度 > 10min → stalled → 可重分配
  - Worker 心跳超时 180s → dead → shard 标记 orphaned → 重分配
Task 聚合:
  - task.speed = sum(shard.speed)
  - task.progress = sum(shard.done_bytes) / sum(shard.total_bytes)
  - task.status = "downloading" if any shard running, "done" if all done
Worker 忙闲:
  - 看 shards 表有没有 status=running 的 shard（不只看 tasks 表）
```

### 4.8 数据库变更

```sql
-- 新增 shards 表
CREATE TABLE shards (
    id           TEXT PRIMARY KEY,       -- "s-{task_id}-{index}"
    task_id      TEXT NOT NULL,          -- FK → tasks.id
    shard_index  INTEGER NOT NULL,
    server       TEXT,                   -- 分配到的 worker key
    status       TEXT NOT NULL DEFAULT 'pending',
                 -- pending / running / done / failed / paused / stalled
    total_files  INTEGER DEFAULT 0,
    done_files   INTEGER DEFAULT 0,
    total_bytes  INTEGER DEFAULT 0,
    done_bytes   INTEGER DEFAULT 0,
    speed_mbps   REAL DEFAULT 0,
    error        TEXT,
    started_at   TEXT,
    completed_at TEXT,
    updated_at   REAL
);
CREATE INDEX idx_shards_task ON shards(task_id);
CREATE INDEX idx_shards_server ON shards(server);
CREATE INDEX idx_shards_status ON shards(status);

-- tasks 表新增字段
ALTER TABLE tasks ADD COLUMN total_shards   INTEGER DEFAULT 1;
ALTER TABLE tasks ADD COLUMN done_shards    INTEGER DEFAULT 0;
ALTER TABLE tasks ADD COLUMN max_workers    INTEGER DEFAULT 0;  -- 0=不限
ALTER TABLE tasks ADD COLUMN shard_strategy TEXT DEFAULT 'auto';
                  -- auto: ≥10GB 自动分片
                  -- single: 强制单 worker
                  -- manual: 手动指定
```

### 4.9 Temporal Workflow 变更

```
旧:
  DownloadDatasetWorkflow    — 单 worker 下载全部文件
  SplitDownloadWorkflow      — 手动拆分，coordinator + children

新:
  ShardedDownloadWorkflow    — 替代以上两者
    1. list_repo_files
    2. 根据策略决定分片数 (可能=1)
    3. 创建 shard SQLite 行
    4. 对每个 shard: 在目标 worker 的个人队列启动 child workflow
    5. 等待所有 child 完成
    6. 聚合结果 → task done/failed

  ShardWorkerWorkflow        — 每个 shard 的执行者
    1. 读取分配的文件列表
    2. load_progress (跳过已完成文件)
    3. PipelineEngine.run() (和现在完全一样)
    4. 更新 shard 状态
    5. 完成/失败

ShardWorkerWorkflow 内部的 PipelineEngine 完全不变。
变化只在调度层，不在下载层。
```

### 4.10 Auto-dispatch 变更

```python
# 旧逻辑 (简化)
def auto_dispatch_pending():
    busy = {t.server for t in get_downloading_tasks()}
    for worker in alive_workers:
        if worker not in busy:
            task = pick_highest_priority_pending()
            assign(task, worker)

# 新逻辑
def auto_dispatch_shards():
    # 1. 找空闲 worker: 没有 running shard 的
    busy = {s.server for s in get_running_shards()}
    idle_workers = [w for w in alive_workers if w.key not in busy and w.disk_gb > 70]
    
    # 2. 找需要 worker 的 shard
    pending_shards = get_shards(status='pending', order_by='task.priority, shard.created_at')
    
    # 3. 分配
    for shard in pending_shards:
        compatible = [w for w in idle_workers if source_compatible(w, shard.task)]
        if compatible:
            worker = pick_best(compatible)  # 磁盘最大的优先
            assign_shard(shard, worker)
            idle_workers.remove(worker)
    
    # 4. 检查是否有新的 pending task 需要创建 shard
    for task in get_tasks(status='pending', no_shards=True):
        create_shards_for_task(task)
```

---

## 五、迁移策略

### Phase 1: 代码开发（2-3 天）

不动线上环境，纯本地开发：
1. 新增 shards 表 + schema 迁移
2. 实现 ShardedDownloadWorkflow + ShardWorkerWorkflow
3. 改写 auto_dispatch → shard-aware
4. 改写 reconciler → shard-aware
5. 改写 Dashboard API → 聚合 shard 进度
6. 前端显示分片进度
7. Pipeline 修复: HF_HUB_DISABLE_XET=1, 429 检测

### Phase 2: 部署 HK Workers (w1-w7)（半天）

bj1-4 **不动**，保持 AgiBotWorld-Beta 运行：
1. rsync 新代码到 S1
2. S1 重启 Dashboard（新 schema 自动迁移）
3. 分发到 w1-w7
4. 重启 w1-w7 Temporal worker
5. 新提交的 HF 任务自动走分片逻辑
6. 旧的 downloading 任务（如果有）兼容处理

### Phase 3: 部署 bj1-bj4（等 AgiBotWorld-Beta 完成后）

1. 确认所有 split child workflow 完成
2. 部署新代码到 bj1-4
3. 重启 Temporal worker
4. 删除旧 SplitDownloadWorkflow 代码

### 兼容性

- total_shards=1 的任务 = 当前单 worker 模式，完全兼容
- 旧 SplitDownloadWorkflow 在 Phase 3 前继续运行
- Reconciler 保留对 `{id}-part*` 旧格式的识别
- 新旧 worker 可以混跑（新代码兼容旧 workflow）

---

## 六、风险评估

| 风险 | 严重度 | 概率 | 应对 |
|------|--------|------|------|
| 分片装箱不均（大文件集中） | 中 | 低 | 贪心算法按大小降序分配 |
| Worker 挂掉后 shard 迁移复杂 | 高 | 中 | Phase 1 只做"标记 orphaned + 重分配未开始文件"，不做"迁移进行中文件" |
| BOS 上传路径冲突 | 低 | 低 | 对象存储不同 key 不冲突 |
| SQLite 并发写压力增加 | 中 | 中 | WAL mode + retry（已有），shard 更新频率 = 当前 task 更新频率 × N |
| list_repo_files 超时 (175K+ 文件) | 高 | 已发生 | 已有心跳修复 (activities.py)，加 10min 超时兜底 |
| 新旧代码混跑状态不一致 | 中 | 中 | Phase 2 只动 HK workers，bj 不动；schema 迁移向前兼容 |
| AgiBotWorld-Beta 被打断 | 高 | 低 | Phase 2 完全不碰 bj1-4，Phase 3 等它跑完 |
| Dashboard 前端不显示分片 | 低 | 高 | 需要前端改动，但不阻塞后端 |

---

## 七、改动文件清单

| 文件 | 改动类型 | 工作量 | 说明 |
|------|----------|--------|------|
| `dlm/queue/snapshot.py` | 修改 | 小 | 新增 shards 表 + CRUD |
| `dlm/temporal/workflows.py` | 重写 | 大 | ShardedDownloadWorkflow + ShardWorkerWorkflow |
| `dlm/temporal/activities.py` | 修改 | 中 | 新增 shard 相关 activities |
| `dlm/temporal/__main__.py` | 修改 | 小 | HF_HUB_DISABLE_XET=1, 移除 HF_XET_HIGH_PERFORMANCE |
| `dlm/web/reconciler.py` | 重写 | 大 | shard-aware 调度 + 健康检查 |
| `dlm/web/routes/queue.py` | 重写 | 大 | shard-aware auto_dispatch |
| `dlm/web/routes/tasks.py` | 修改 | 中 | shard 进度聚合 + shard 列表 API |
| `dlm/web/static/app.js` | 修改 | 中 | 分片进度可视化 |
| `dlm/web/static/index.html` | 修改 | 小 | 分片 UI 组件 |
| `dlm/temporal/pipeline.py` | 修改 | 小 | 429 检测 + Retry-After |
| `dlm/temporal/models.py` | 修改 | 小 | ShardInput/ShardResult 模型 |

**总工作量估计：3-4 天开发 + 1 天测试部署**

---

## 八、验收标准

1. 提交一个 50GB+ HF 数据集 → 自动拆分到多个 HK worker → 并行下载 → 全部完成 → task 标记 done
2. Dashboard 实时显示每个 shard 的进度、速度
3. 杀掉一个 worker → 其 shard 标记 orphaned → 10 分钟内重新分配到其他 worker
4. P0 任务提交 → P2 shard 被暂停 → P0 shard 优先执行
5. bj1-4 的 AgiBotWorld-Beta 全程不受影响
6. 所有 HK worker 使用 HF_HUB_DISABLE_XET=1（速度翻倍）

---

## 九、设计评审结果（2026-07-30）

代码审阅 agent 对照实际代码逐项检查后，发现以下问题：

### 9.1 必须修正 — 高危

#### R1: Temporal Workflow 不能写 SQLite

**问题**: 方案 4.9 节第 3 步说 ShardedDownloadWorkflow "创建 shard SQLite 行"。但 Temporal workflow 必须是确定性的，不能执行 I/O 操作（数据库写入、网络请求等）。

**修正**: 将 shard 创建逻辑封装为 activity:
```python
@activity.defn
async def create_shards(task_id: str, file_partitions: list[list[FileInfo]]) -> list[str]:
    """在 SQLite 中创建 shard 行，返回 shard ID 列表"""
    shard_ids = []
    for i, files in enumerate(file_partitions):
        shard_id = f"s-{task_id}-{i}"
        db.insert_shard(shard_id, task_id, i, files)
        shard_ids.append(shard_id)
    return shard_ids
```

#### R2: Coordinator Workflow 历史膨胀

**问题**: 50TB 数据集跑几周，coordinator workflow 持续等待子 workflow。Temporal 重放时加载全部历史，可能撞到 50MB / 50K event 限制。

**修正**: 使用 `continue-as-new` 模式。Coordinator 每完成一批 shard（或每 24h）执行 continue-as-new，携带当前状态到新的 workflow execution：
```
Round 1: 启动 7 个 shard child → 等待 → 3 个完成、4 个还在跑
  continue-as-new(state={done_shards: [0,2,5], running: [1,3,4,6]})
Round 2: 继续等待剩余 4 个 → 等待 → 全部完成
  → task done
```

#### R3: asyncio.gather 一个失败杀掉全部

**问题**: 当前 SplitDownloadWorkflow 的 `asyncio.gather(*child_handles)` 没有 `return_exceptions=True`。一个 shard 失败会导致所有其他 shard 被取消。

**修正**: 新设计必须使用 `return_exceptions=True`，并在所有子 workflow 结束后聚合结果：
```python
results = await asyncio.gather(*child_handles, return_exceptions=True)
failed = [r for r in results if isinstance(r, Exception)]
# 只标记失败的 shard，不影响其他 shard
```

### 9.2 设计补充 — 中危

#### R4: 分片文件列表必须持久化

**问题**: shards 表只存 total_files/done_files 计数，不存具体文件列表。Worker 挂掉后文件列表丢失，shard 无法迁移到其他 worker。

**修正**: 在 shard 创建时，将文件列表写入 BOS（`downloads/shards/{shard_id}/filelist.json`）。shards 表增加 `filelist_key TEXT` 字段。重分配时新 worker 从 BOS 读取文件列表。

#### R5: Preempt 机制需要 Signal 支持

**问题**: 暂停运行中的 Temporal child workflow 需要向其发送 signal。当前 PipelineEngine 没有优雅暂停机制。

**修正**: 
- ShardWorkerWorkflow 增加 `pause` signal handler
- PipelineEngine 增加 `pause_event: asyncio.Event`
- Producer 每下载一个文件前检查 pause_event
- 收到 pause signal → 设置 event → producer 停止 → consumer 清空队列 → workflow 暂停
- Phase 1 可以先不实现 preempt，用"取消 + 重新排队剩余文件"替代

#### R6: Staging 路径需包含 Shard ID

**问题**: 同一 worker 先后运行同一任务的两个 shard 时，staging 路径 `/data/staging/{task_name}/` 和 `.progress.json` 冲突。

**修正**: staging 路径改为 `/data/staging/{task_name}/{shard_id}/`，或在单 shard 时保持原路径以兼容。

### 9.3 迁移补充

#### R7: 在途 downloading 任务的兼容处理

**问题**: Phase 2 部署时，已有的 `downloading` 任务没有 shard 行。新 reconciler 需要识别这些"旧格式"任务。

**修正**: `total_shards=1` 且 shards 表无对应行 → 视为旧格式单 worker 任务，按旧逻辑处理。新 reconciler 代码中保留旧路径分支，Phase 3 后移除。

#### R8: 新旧 Workflow 类型共存

**问题**: bj1-4 注册 DownloadDatasetWorkflow / SplitDownloadWorkflow，w1-w7 注册 ShardedDownloadWorkflow / ShardWorkerWorkflow。Temporal server 和 reconciler 需同时查询两套。

**修正**: Reconciler 的 workflow 查询列表扩展为同时包含新旧 workflow type。Dashboard 的 `/api/workflows` 同理。Phase 3 后删除旧类型。

### 9.4 确认无问题的设计

- ✅ Pipeline 引擎不动，只改调度层
- ✅ 贪心装箱算法 O(n log n)，适合此场景
- ✅ 分阶段迁移、bj1-4 不动
- ✅ shards 表索引设计正确
- ✅ 三层反压模型完整
- ✅ 源路由在 shard 层面通过 `source_compatible` 执行
