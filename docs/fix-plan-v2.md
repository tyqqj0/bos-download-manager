# DLM 关键问题修复方案 v2

> 基于全面审查 (4 个 agent) 汇总的 11 个 Critical + 12 个 Important 问题，
> 筛选后保留「确实会造成数据丢失/任务卡死/功能失效」的核心修复。

---

## 修复 1: Upload 失败静默吞掉 → workflow 假装成功 (C2)

**现状:** `run_pipeline_batch` activity 返回 `{"uploaded_bytes": ...}` 时不检查 `stats.failed_files`。
即使 10% 的文件上传失败，workflow 照样认为 batch 成功，推进到下一批，永远不重试。

**修复:** 在 `activities.py` 的 `run_pipeline_batch` 结尾，检查 failed_files > 0 时抛异常触发 Temporal retry。

```python
# dlm/temporal/activities.py — run_pipeline_batch 尾部 (约 line 218)

    stats = await engine.run(files)

    # 新增：如果有文件上传失败，抛出让 Temporal 重试整个 batch
    if stats.failed_files > 0:
        raise RuntimeError(
            f"Batch incomplete: {stats.failed_files}/{stats.total_files} files failed to upload"
        )

    return {
        "downloaded_files": stats.downloaded_files,
        ...
    }
```

**影响范围:** 只影响 `run_pipeline_batch` activity 的返回路径。workflow 已有 `RetryPolicy(maximum_attempts=3)`。
**风险:** 低。如果 BOS 彻底挂掉，3 次 batch retry 后 workflow 进入 except 分支标记 failed — 和现在"假装成功但数据丢失"相比，这是正确行为。

---

## 修复 2: report_to_dashboard("done") 放在 try 内 → 失败后 task 被误标 failed (C5)

**现状:** `workflows.py` DownloadDatasetWorkflow.run() 里，`report_to_dashboard("done")` 在 try 块内。
如果 dashboard HTTP 超时，控制流落入 except → 又调一次 `report_to_dashboard("failed")` → 本来成功的任务被标 failed。

**修复:** 把 "done" 的 report 移到 try 块之后（或加单独 try/except）。

```python
# dlm/temporal/workflows.py — 约 line 178-196

        # 原来在 try 内的 block:
        except Exception as e:
            ...

        # 新增：try 结束后再 report done（即使 report 失败，workflow 仍返回 success）
        try:
            await workflow.execute_activity(
                "report_to_dashboard",
                args=[task_input.id, "done", None, 100, 0, round(total_gb, 2), server_key, None],
                start_to_close_timeout=timedelta(seconds=30),
            )
        except Exception:
            pass  # dashboard 更新是尽力而为，不影响 workflow 结果

        return TaskResult(
            status="done",
            files_uploaded=total_file_count,
            bytes_uploaded=uploaded_bytes,
        )
```

**影响范围:** 仅改变 "done" report 的位置。
**风险:** 极低。最坏情况 = dashboard 短暂没收到 "done" 状态，reconciler 5min 后会修复。

---

## 修复 3: Reconciler 查不到 SplitDownloadWorkflow → 重复创建 workflow (C6)

**现状:** reconciler.py line 57-59 只查 `WorkflowType="DownloadDatasetWorkflow"`，
SplitDownloadWorkflow 的子任务用的 workflow_id 是 `{task_id}-part{N}` 不是 `dl-{task_id}`，
reconciler 看不到它们 → 30min 后误判为"无 workflow" → 重新 dispatch → 重复下载。

**修复:** 扩展 Temporal query + 检查 split workflow ID 模式。

```python
# dlm/web/reconciler.py — reconcile() 函数内 (约 line 54-60)

    try:
        client = await get_client()
        running_ids = set()
        # 查两种 workflow type
        for wf_type in ["DownloadDatasetWorkflow", "SplitDownloadWorkflow"]:
            async for wf in client.list_workflows(
                f'WorkflowType="{wf_type}" AND ExecutionStatus="Running"'
            ):
                running_ids.add(wf.id)
        report["running_workflows"] = len(running_ids)
    except Exception as e:
        ...

    # 匹配逻辑也要扩展
    for task in downloading:
        task_id = task["id"]
        # 检查 dl-{id}、split-download-{id}、{id}-part* 三种模式
        has_workflow = (
            f"dl-{task_id}" in running_ids
            or f"split-download-{task_id}" in running_ids
            or any(wf_id.startswith(f"{task_id}-part") for wf_id in running_ids)
        )
        if not has_workflow:
            report["orphaned"].append(...)
```

**影响范围:** reconcile() 函数的查询逻辑。
**风险:** 低。只是多查了一种 workflow type，不影响现有单任务流程。any() 的性能对 running_ids < 100 无影响。

---

## 修复 4: skip/delete 不取消 Temporal workflow (C11)

**现状:** `routes/tasks.py` 的 skip_task/delete_task 只调 `celery_app.control.revoke()`，
但任务实际跑在 Temporal 上 → revoke 无效，Temporal workflow 继续跑。

**修复:** 在 skip/delete 时同时取消 Temporal workflow。

```python
# dlm/web/routes/tasks.py — skip_task 和 delete_task 函数

@router.post("/tasks/{task_id}/skip")
async def skip_task(task_id: str):
    """Skip/revoke a task."""
    from ..temporal_client import cancel_workflow

    def _do():
        from ...queue.snapshot import get_task, update_task_progress, init_db
        init_db()
        task = get_task(task_id)
        if not task:
            return {"error": f"Task not found: {task_id}"}
        update_task_progress(task_id, status="revoked", phase=None, speed_mbps=0)
        return {"id": task_id, "status": "skipped"}

    result = await run_blocking(_do)
    if "error" in result:
        raise HTTPException(400, result["error"])

    # 异步取消 Temporal workflow（尽力而为）
    try:
        await cancel_workflow(task_id)
    except Exception:
        pass

    return result
```

**影响范围:** skip_task, delete_task, batch_action 三个端点。
**风险:** 中-低。`cancel_workflow` 已存在于 temporal_client.py 且带 try/except。
如果 workflow 不存在（老任务），cancel 会静默失败。需验证 cancel 后 worker 上是否正确清理 staging。

---

## 修复 5: SplitDownloadWorkflow 不更新父任务状态 (C7)

**现状:** SplitDownloadWorkflow 结束后只返回 TaskResult，不 report_to_dashboard。
父任务在 SQLite 里永远停在 "downloading"。

**修复:** 在 SplitDownloadWorkflow.run() 结尾加 report。

```python
# dlm/temporal/workflows.py — SplitDownloadWorkflow.run() 结尾 (约 line 310-323)

        # 等待所有 child 完成后，report 父任务状态
        if failed:
            try:
                await workflow.execute_activity(
                    "report_to_dashboard",
                    args=[task_input.id, "failed", None, None, None, None, None,
                          f"{len(failed)}/{worker_count} parts failed"],
                    start_to_close_timeout=timedelta(seconds=30),
                )
            except Exception:
                pass
            return TaskResult(status="failed", ...)

        total_gb = total_bytes / (1024 ** 3)
        try:
            await workflow.execute_activity(
                "report_to_dashboard",
                args=[task_input.id, "done", None, 100, 0, round(total_gb, 2), None, None],
                start_to_close_timeout=timedelta(seconds=30),
            )
        except Exception:
            pass

        return TaskResult(status="done", ...)
```

**影响范围:** 仅 SplitDownloadWorkflow 的结束路径。
**风险:** 极低。只是加了一个 HTTP report 调用，且包在 try/except 中。

---

## 修复 6: Dispatch 阈值不一致 (C9)

**现状:**
- Reconciler dispatch 阈值: `MIN_DISPATCH_DISK_GB = 30`
- Pipeline backpressure: `max(30% × 197GB, 20GB) = 59GB`

Worker 有 40GB 空闲 → reconciler 认为"够了"，dispatch 任务 →
pipeline 立即进入 backpressure（40 < 59）→ 任务卡在 backpressure，worker 显示 0 速度。

**修复:** reconciler 用和 pipeline 一致的动态阈值。

```python
# dlm/web/reconciler.py — 顶部常量区

# 替换固定值
# MIN_DISPATCH_DISK_GB = 30
def _min_dispatch_disk_gb(worker_disk_total_gb: float = 200.0) -> float:
    """和 pipeline backpressure 阈值保持一致 + 10GB buffer."""
    return max(worker_disk_total_gb * 0.30, 20) + 10

# 使用：
# worker_disk = worker.get("disk_free_gb") or 0
# threshold = _min_dispatch_disk_gb()  # ~69GB for 200GB disk
# if worker_disk < threshold:
#     continue
```

**问题:** workers 表没存 disk_total_gb，只有 disk_free_gb。

**折中方案:** 暂时用固定 70GB 阈值（≈ 59GB pipeline 阈值 + 10GB buffer）。

```python
MIN_DISPATCH_DISK_GB = 70  # must exceed pipeline backpressure threshold (~59GB)
```

**影响范围:** reconciler 的 idle worker 选择逻辑。
**风险:** 中。如果 worker 磁盘本身不大（如 < 100GB），70GB 阈值会导致永远不 dispatch。
但我们的 worker 都是 200GB 磁盘，所以实际安全。

---

## 修复 7: Stall 检测 .incomplete 路径匹配问题 (C1)

**现状:** pipeline.py 的 `_wait_with_growth_check` 检查文件大小增长时，
查找路径是 `staging_dir / file_info.path` 和 `str(target_path) + ".incomplete"`。
但 HF hub 实际把 .incomplete 文件存在 `.cache/huggingface/download/` 下用 SHA hash 命名，
导致 glob 匹配不到 → `current_size` 始终为 0 → 30min 后误杀。

**当前代码已有修复 (line 211-220):**
```python
for candidate in [target_path, Path(str(target_path) + ".incomplete")]:
    if candidate.exists():
        current_size = max(current_size, candidate.stat().st_size)
for p in self.staging_dir.glob(".cache/**/*.incomplete"):
    if file_info.path.split("/")[-1] in str(p):
        current_size = max(current_size, p.stat().st_size)
```

**分析:** 这段代码已经覆盖了:
1. 目标路径本身（文件在下载中时可能已部分存在）
2. 目标路径 + .incomplete
3. .cache 目录下包含文件名的 .incomplete 文件

**实际风险:** HF hub 用 `local_dir` 参数时，下载直接到目标路径（不用 hash），
所以 candidate #1 应该能匹配。但如果用了默认 cache 模式，hash 名不含原始文件名 →
`.cache/**/*.incomplete` 的 `if filename in str(p)` 检查会失败。

**确认问题:** 我们代码里用了 `local_dir=str(self.staging_dir)`, 所以 HF hub 应该直接下到
`staging_dir/file_info.path`。这种情况下 stall 检测应该工作。

**结论:** 此问题可能是误报 — 需要在实际环境验证。暂不修改。
如果确认有问题，修复方案是增加对整个 staging_dir 的 mtime/size 增量监控作为 fallback。

---

## 修复 8: POST /tasks 通过 Celery 直接 dispatch（绕过 Temporal） (C10 简化版)

**现状:** `routes/tasks.py` 的 `add_task` 端点直接调 Celery `download_dataset.apply_async()`，
绕过了：
1. auto_dispatch 的磁盘检查
2. Temporal workflow 的 preflight check
3. 容量规划

**修复:** 改为只入库 pending，让 auto_dispatch 5min 内自动接手。

```python
# dlm/web/routes/tasks.py — add_task 函数 (约 line 199-204)

        upsert_task(task_meta)

        # 移除 Celery dispatch，改为等待 auto_dispatch
        # 旧代码:
        # if not req.no_dispatch:
        #     chain(download_dataset.s(task_meta), ...).apply_async(...)

        return {"task": _task_for_frontend(task_meta)}
```

**影响范围:** 新任务添加不再立即 dispatch，等待 5min auto_dispatch 周期。
**风险:** 中。用户添加任务后不再"立即开始"，需要等 auto_dispatch。
可以折中：添加后立即触发一次 auto_dispatch。

---

## 暂不修复的问题

| # | 问题 | 不修原因 |
|---|------|----------|
| C3 | ModelScope source 被忽略 | 当前没有 ModelScope 任务在跑，优先级低 |
| C4 | report_to_dashboard("done") 在 worker queue | 修复复杂度高，需重构 activity routing |
| C8 | SplitDownload 不分发到不同 worker | 需要 BOS-based filelist 架构重构，大改动 |
| I1-I12 | 各种 IMPORTANT 级问题 | 不阻断正常运行，后续迭代修复 |

---

## 修复优先级排序

1. **修复 2 (C5)** — 1 行代码移动，几乎零风险，防止成功任务被标 failed
2. **修复 1 (C2)** — 3 行代码新增，低风险，防止数据丢失
3. **修复 5 (C7)** — 10 行新增，极低风险，让 split 任务状态可见
4. **修复 3 (C6)** — 中等改动，低风险，防止重复下载
5. **修复 4 (C11)** — 中等改动，需要验证 cancel 清理逻辑
6. **修复 6 (C9)** — 1 行常量修改，但需确认不会阻止所有 dispatch
7. **修复 8 (C10)** — 行为变更，需要配合 auto_dispatch 优化
8. **修复 7 (C1)** — 可能是误报，需实际验证

---

## 部署策略

1. 修复 1-5 打包为一次 commit，部署到 S1 + 全部 worker
2. 修复 6 单独 commit，先在 S1 测试 auto_dispatch 行为
3. 修复 7-8 等验证后再决定
