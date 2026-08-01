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

from ..fleet import DEAD_THRESHOLD, STALE_THRESHOLD, WORKER_TIMEOUT, has_live_workflow

router = APIRouter(tags=["doctor"])


def _read_state() -> tuple[list, list, list]:
    """Every SQLite read this route needs, in one executor hop.

    `init_db()` is not a read — it runs executescript + commit and takes the
    write lock — so calling it (and the queries) straight from `async def`
    put a lock wait on the event loop on every /api/doctor request.
    """
    from ...queue.snapshot import (
        get_all_tasks, get_running_shards, get_workers, init_db,
    )
    from ..fleet import dedupe_workers

    init_db()
    return get_all_tasks(), dedupe_workers(get_workers()), get_running_shards()


@router.get("/doctor")
async def diagnose():
    """Run health diagnostics including Temporal workflow check."""
    tasks, workers, running_shards = await run_blocking(_read_state)
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
        from ..temporal_client import running_workflows
        by_id = await asyncio.wait_for(running_workflows(), timeout=15)
        running_ids = set(by_id)
        running_queues = {q for q in by_id.values() if q}

        downloading = [t for t in tasks if t.get("status") == "downloading"]
        for t in downloading:
            task_id = t["id"]
            if not has_live_workflow(task_id, running_ids):
                age = now - (t.get("updated_at") or 0)
                orphaned.append({
                    "task_id": t["id"],
                    "name": t.get("name", ""),
                    "server": t.get("server", ""),
                    "stale_seconds": int(age),
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
    from ..fleet import idle_workers as compute_idle

    candidates = compute_idle(tasks, workers, running_shards, now)
    if running_queues is None:
        candidates = []  # cannot tell without Temporal — don't cry wolf
    else:
        candidates = [
            c for c in candidates
            if f"download-{c['server_key']}" not in running_queues
        ]
    for c in candidates:
        c["message"] = "Online, holds no shard and no workflow — free for dispatch"

    # 7. Get last reconciler + idle worker reports
    from ..cache import cache
    reconciler_report = cache.get("reconciler_report")
    idle_report = cache.get("idle_worker_report")

    # Idle only counts as an ISSUE when work is queued for that worker's source.
    starved_idle = [c for c in candidates if c["starved"]]

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
            c["server_key"] for c in candidates if not c["starved"]
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
    from ..temporal_client import start_sharded_download

    tasks, _, _ = await run_blocking(_read_state)
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
            from ..temporal_client import running_workflows
            # Deadline required: this scan decides whether a task gets a
            # SECOND coordinator. An untimed one leaves the operator's repair
            # request hanging with no way to tell whether it took effect.
            running_ids = set(await asyncio.wait_for(running_workflows(), timeout=15))

            downloading = [t for t in tasks if t.get("status") == "downloading"]
            for t in downloading:
                task_id = t["id"]
                if has_live_workflow(task_id, running_ids):
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

    def _rewrite(matches, **fields) -> list:
        """Apply one status change per matching task, off the event loop."""
        from ...queue.snapshot import update_task_progress

        def _do():
            touched = []
            for t in matches:
                update_task_progress(t["id"], **fields)
                touched.append(t.get("name", t["id"]))
            return touched

        return _do

    if "reset_stuck" in actions:
        stuck = [t for t in tasks
                 if t.get("status") == "downloading"
                 and now - (t.get("updated_at") or 0) > DEAD_THRESHOLD]
        results["reset_stuck"] = await run_blocking(
            _rewrite(stuck, status="pending", phase="reset_by_doctor"))

    if "skip_zombie" in actions:
        zombies = [t for t in tasks
                   if t.get("status") == "failed" and (t.get("retry_count") or 0) >= 8]
        results["skip_zombie"] = await run_blocking(
            _rewrite(zombies, status="revoked", phase=None))

    return results
