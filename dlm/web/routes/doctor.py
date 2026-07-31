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

    # A worker may have several heartbeat rows (e.g. wN@temporal and a dead
    # wN@sidecar) — judge liveness by the FRESHEST row per server_key, or
    # stale auxiliary rows produce phantom "offline worker" alerts.
    freshest = {}
    for w in workers:
        key = w.get("server_key", "")
        if key and (key not in freshest
                    or (w.get("last_seen") or 0) > (freshest[key].get("last_seen") or 0)):
            freshest[key] = w
    workers = list(freshest.values())

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
        client = await asyncio.wait_for(get_client(), timeout=5)
        running_ids = set()

        running_queues = set()

        async def _list_wfs():
            for wf_type in ["DownloadDatasetWorkflow", "SplitDownloadWorkflow",
                            "ShardedDownloadWorkflow", "ShardWorkerWorkflow"]:
                async for wf in client.list_workflows(
                    f'WorkflowType="{wf_type}" AND ExecutionStatus="Running"'
                ):
                    running_ids.add(wf.id)
                    if wf.task_queue:
                        running_queues.add(wf.task_queue)

        await asyncio.wait_for(_list_wfs(), timeout=10)

        downloading = [t for t in tasks if t.get("status") == "downloading"]
        for t in downloading:
            task_id = t["id"]
            workflow_id = f"dl-{task_id}"
            # Match all known workflow ID patterns
            has_workflow = (
                workflow_id in running_ids
                or f"split-download-{task_id}" in running_ids
                or f"sharded-{task_id}" in running_ids
                or any(wid.startswith(f"shard-s-{task_id}-") for wid in running_ids)
                or any(wid.startswith(f"{task_id}-part") for wid in running_ids)
                or any(wid.startswith(f"{workflow_id}-") for wid in running_ids)
            )
            if not has_workflow:
                age = now - (t.get("updated_at") or 0)
                orphaned.append({
                    "task_id": t["id"],
                    "name": t.get("name", ""),
                    "server": t.get("server", ""),
                    "stale_seconds": int(age),
                    "workflow_id": workflow_id,
                })
    except asyncio.TimeoutError:
        orphaned = [{"error": "Temporal query timed out (15s)"}]
        running_queues = None
    except Exception as e:
        orphaned = [{"error": f"Cannot check Temporal: {e}"}]
        running_queues = None

    # 6. Idle workers — online but carrying no work at all.
    #    Under the sharded architecture a worker is busy when it owns a running
    #    SHARD (the task row's `server` is NULL for sharded tasks), so shard
    #    ownership is the primary signal; workflow task-queue membership is the
    #    fallback. Skipped entirely if the Temporal query failed, since without
    #    it every worker would look idle.
    idle_workers = []
    from ...queue.snapshot import get_running_shards
    downloading_tasks = [t for t in tasks if t.get("status") == "downloading"]
    busy_servers = {t.get("server") for t in downloading_tasks if t.get("server")}
    busy_servers |= {s.get("server") for s in get_running_shards() if s.get("server")}
    alive_workers = [w for w in workers if now - (w.get("last_seen") or 0) < WORKER_TIMEOUT]
    idle_seen = set()
    for w in alive_workers:
        wkey = w.get("server_key", "")
        if not wkey or wkey in idle_seen:
            continue
        idle_seen.add(wkey)
        if wkey in busy_servers:
            continue
        if running_queues is None:
            continue  # cannot tell without Temporal — don't cry wolf
        if f"download-{wkey}" in running_queues:
            continue
        idle_workers.append({
            "server_key": wkey,
            "disk_free_gb": w.get("disk_free_gb"),
            "message": "Online, holds no shard and no workflow — free for dispatch",
        })

    # 7. Get last reconciler + idle worker reports
    from ..cache import cache
    reconciler_report = cache.get("reconciler_report")
    idle_report = cache.get("idle_worker_report")

    # An idle worker is only a PROBLEM when work is waiting for it: pending
    # tasks of a source it can serve (modelscope→bj*, hf→w*). With an empty
    # queue, idle is the correct resting state, not an incident.
    pending_sources = {
        (t.get("source") or "hf") for t in tasks if t.get("status") == "pending"
    }
    starved_idle = [
        i for i in idle_workers
        if ("modelscope" if i["server_key"].startswith("bj") else "hf") in pending_sources
    ]

    total_issues = (
        len(offline_workers) + len(stuck) + len(failed_repeat)
        + len(orphaned) + len(disk_full) + len(starved_idle)
    )

    return {
        "healthy": total_issues == 0,
        "total_issues": total_issues,
        "offline_workers": offline_workers,
        "stuck_tasks": stuck,
        "orphaned_tasks": orphaned,
        "idle_workers": starved_idle,
        "idle_workers_no_pending_work": [
            i["server_key"] for i in idle_workers if i not in starved_idle
        ],
        "disk_full": disk_full,
        "failed_repeat": failed_repeat,
        "reconciler": reconciler_report,
        "idle_worker_detail": idle_report,
    }


class FixRequest(BaseModel):
    actions: list[str] = []


@router.post("/doctor")
async def fix(req: FixRequest):
    """Apply repair actions. Unknown actions are reported, never silently dropped.

    Available actions:
    - redispatch_orphaned: re-dispatch tasks with no Temporal workflow (sharded)
    - reset_stuck: reset stuck tasks to pending
    - skip_zombie: revoke permanently failed tasks (retry_count >= 8)

    There is deliberately no "restart worker" action — restarting a worker is a
    host-level operation and goes through scripts/deploy-workers.sh.
    """
    from ...queue.snapshot import get_all_tasks, update_task_progress, init_db
    from ..temporal_client import start_sharded_download
    init_db()

    tasks = get_all_tasks()
    now = time.time()
    results = {}
    actions = req.actions or ["redispatch_orphaned"]

    unknown = [a for a in actions
               if a not in ("redispatch_orphaned", "reset_stuck", "skip_zombie")]
    if unknown:
        results["unsupported_actions"] = unknown

    if "redispatch_orphaned" in actions:
        redispatched = []
        try:
            from ..temporal_client import get_client
            client = await get_client()
            running_ids = set()
            for wf_type in ["DownloadDatasetWorkflow", "SplitDownloadWorkflow",
                            "ShardedDownloadWorkflow", "ShardWorkerWorkflow"]:
                async for wf in client.list_workflows(
                    f'WorkflowType="{wf_type}" AND ExecutionStatus="Running"'
                ):
                    running_ids.add(wf.id)

            downloading = [t for t in tasks if t.get("status") == "downloading"]
            for t in downloading:
                task_id = t["id"]
                has_wf = (
                    f"dl-{task_id}" in running_ids
                    or f"split-download-{task_id}" in running_ids
                    or f"sharded-{task_id}" in running_ids
                    or any(w.startswith(f"shard-s-{task_id}-") for w in running_ids)
                    or any(w.startswith(f"dl-{task_id}-") for w in running_ids)
                )
                if has_wf:
                    continue
                try:
                    # Sharded path only — the legacy workflow has no BOS resume
                    # filter and would re-download everything already uploaded.
                    await start_sharded_download(t)
                    redispatched.append(t.get("name", task_id))
                except Exception as e:
                    if "already started" not in str(e).lower():
                        redispatched.append(f"{t.get('name', task_id)} (FAILED: {e})")
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
