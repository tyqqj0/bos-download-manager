"""Doctor API — Temporal-aware cluster health check and auto-repair.

Checks:
- Workers offline (no heartbeat in 180s)
- Orphaned tasks (downloading in SQLite but no Temporal workflow)
- Stale tasks (workflow running but no progress in 10+ min)
- Disk-full workers
- Failed tasks with high retry count
"""

import asyncio
import time
from fastapi import APIRouter
from pydantic import BaseModel

from . import run_blocking

router = APIRouter(tags=["doctor"])

WORKER_TIMEOUT = 180
STALE_THRESHOLD = 600
DEAD_THRESHOLD = 1800


@router.get("/doctor")
async def diagnose():
    """Run health diagnostics including Temporal workflow check."""
    from ...queue.snapshot import get_all_tasks, get_workers, init_db
    init_db()

    tasks = get_all_tasks()
    workers = get_workers()
    now = time.time()

    # 1. Offline workers
    offline_workers = []
    for w in workers:
        if now - (w.get("last_seen") or 0) > WORKER_TIMEOUT:
            offline_workers.append({
                "server_key": w.get("server_key", ""),
                "hostname": w.get("hostname", ""),
                "last_seen_ago_s": int(now - (w.get("last_seen") or 0)),
            })

    # 2. Stuck/orphaned downloads
    stuck = []
    for t in tasks:
        if t.get("status") == "downloading":
            age = now - (t.get("updated_at") or 0)
            if age > STALE_THRESHOLD:
                stuck.append({
                    "task_id": t["id"],
                    "name": t.get("name", ""),
                    "server": t.get("server", ""),
                    "stale_seconds": int(age),
                })

    # 3. Disk-full workers
    disk_full = []
    for w in workers:
        free = w.get("disk_free_gb")
        if free is not None and free < 10:
            disk_full.append({
                "server_key": w.get("server_key", ""),
                "disk_free_gb": free,
            })

    # 4. Failed repeat
    failed_repeat = [
        {"task_id": t["id"], "name": t.get("name", ""),
         "retry_count": t.get("retry_count", 0), "error": t.get("error", "")}
        for t in tasks
        if t.get("status") == "failed" and (t.get("retry_count") or 0) >= 5
    ]

    # 5. Check Temporal workflows vs downloading tasks
    orphaned = []
    try:
        from ..temporal_client import get_client
        client = await get_client()
        running_ids = set()
        async for wf in client.list_workflows(
            'WorkflowType="DownloadDatasetWorkflow" AND ExecutionStatus="Running"'
        ):
            running_ids.add(wf.id)

        downloading = [t for t in tasks if t.get("status") == "downloading"]
        for t in downloading:
            workflow_id = f"dl-{t['id']}"
            if workflow_id not in running_ids:
                age = now - (t.get("updated_at") or 0)
                orphaned.append({
                    "task_id": t["id"],
                    "name": t.get("name", ""),
                    "server": t.get("server", ""),
                    "stale_seconds": int(age),
                    "workflow_id": workflow_id,
                })
    except Exception as e:
        orphaned = [{"error": f"Cannot check Temporal: {e}"}]

    # 6. Get last reconciler report
    from ..cache import cache
    reconciler_report = cache.get("reconciler_report")

    total_issues = (
        len(offline_workers) + len(stuck) + len(failed_repeat)
        + len(orphaned) + len(disk_full)
    )

    return {
        "healthy": total_issues == 0,
        "total_issues": total_issues,
        "offline_workers": offline_workers,
        "stuck_tasks": stuck,
        "orphaned_tasks": orphaned,
        "disk_full": disk_full,
        "failed_repeat": failed_repeat,
        "reconciler": reconciler_report,
    }


class FixRequest(BaseModel):
    actions: list[str] = []


@router.post("/doctor")
async def fix(req: FixRequest):
    """Apply repair actions.

    Available actions:
    - redispatch_orphaned: re-dispatch tasks with no Temporal workflow
    - reset_stuck: reset stuck tasks to pending (legacy)
    - skip_zombie: revoke permanently failed tasks
    """
    from ...queue.snapshot import get_all_tasks, update_task_progress, init_db
    from ..temporal_client import start_download
    init_db()

    tasks = get_all_tasks()
    now = time.time()
    results = {}
    actions = req.actions or ["redispatch_orphaned"]

    if "redispatch_orphaned" in actions:
        redispatched = []
        try:
            from ..temporal_client import get_client
            client = await get_client()
            running_ids = set()
            async for wf in client.list_workflows(
                'WorkflowType="DownloadDatasetWorkflow" AND ExecutionStatus="Running"'
            ):
                running_ids.add(wf.id)

            downloading = [t for t in tasks if t.get("status") == "downloading"]
            for t in downloading:
                workflow_id = f"dl-{t['id']}"
                if workflow_id not in running_ids:
                    server = t.get("server", "")
                    queue = f"download-{server}" if server else "download-workers"
                    try:
                        await start_download(t, task_queue=queue)
                        redispatched.append(t.get("name", t["id"]))
                    except Exception as e:
                        if "already started" not in str(e).lower():
                            redispatched.append(f"{t.get('name', t['id'])} (FAILED: {e})")
        except Exception as e:
            redispatched.append(f"ERROR: {e}")
        results["redispatch_orphaned"] = redispatched

    if "reset_stuck" in actions:
        reset = []
        for t in tasks:
            if t.get("status") == "downloading" and now - (t.get("updated_at") or 0) > DEAD_THRESHOLD:
                update_task_progress(t["id"], status="pending", phase="reset_by_doctor")
                reset.append(t.get("name", t["id"]))
        results["reset_stuck"] = reset

    if "skip_zombie" in actions:
        skipped = []
        for t in tasks:
            if t.get("status") == "failed" and (t.get("retry_count") or 0) >= 8:
                update_task_progress(t["id"], status="revoked", phase=None)
                skipped.append(t.get("name", t["id"]))
        results["skip_zombie"] = skipped

    return results
