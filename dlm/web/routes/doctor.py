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


def _read_state() -> tuple[list, list, list, dict]:
    """Every SQLite read this route needs, in one executor hop.

    `init_db()` is not a read — it runs executescript + commit and takes the
    write lock — so calling it (and the queries) straight from `async def`
    put a lock wait on the event loop on every /api/doctor request.

    That includes the batch rows for downloading POOL tasks, which the stuck
    check below needs for decision E's exemption. Fetching them lazily from
    the handler would be a SQLite call on the event loop that
    tests/test_event_loop_safety.py cannot see (its AST scan only inspects a
    handler's own body, so a helper doing the read would pass and still be
    wrong). Bounded by POOL_MAX_CONCURRENT_TASKS, not by the task table.
    """
    from ...queue.snapshot import (
        get_all_tasks, get_running_shards, get_shards_by_task, get_workers, init_db,
    )
    from ..fleet import dedupe_workers

    init_db()
    tasks = get_all_tasks()
    pool_batches = {
        t["id"]: get_shards_by_task(t["id"])
        for t in tasks
        if t.get("status") == "downloading"
        and (t.get("dispatch_mode") or "sharded") == "pool"
    }
    return (tasks, dedupe_workers(get_workers()), get_running_shards(),
            pool_batches)


@router.get("/doctor")
async def diagnose():
    """Run health diagnostics including Temporal workflow check."""
    tasks, workers, running_shards, pool_batches = await run_blocking(_read_state)
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

    # 2. Stuck/orphaned downloads. A pool task waiting behind the window
    #    updates nothing by design (decision E) — counting it here made
    #    /api/doctor report unhealthy for as long as it waited, and
    #    `healthy` is what T10's deploy gate reads. Same predicate as the
    #    alert engine's task_stuck, by import, so the two cannot drift.
    from ..fleet import pool_task_holds_no_work

    stuck = []
    for t in tasks:
        if t.get("status") == "downloading":
            age = now - (t.get("updated_at") or 0)
            if age > STALE_THRESHOLD:
                if pool_task_holds_no_work(t, pool_batches.get(t["id"], [])):
                    continue
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


@router.get("/doctor/staging-gc")
async def staging_gc_preview():
    """Dry-run preview of the periodic staging GC (decision G) — exactly
    what it would remove and what it is skipping, without removing
    anything. This is the interface a human uses to sanity-check the sweep
    before it runs for real, and before T10 turns it loose on the cluster.
    """
    from ..reconciler import staging_gc

    def _do():
        return staging_gc(dry_run=True)

    return await run_blocking(_do)


class FixRequest(BaseModel):
    actions: list[str] = []


@router.post("/doctor")
async def fix(req: FixRequest):
    """Apply repair actions. Unknown actions are reported, never silently dropped.

    Available actions:
    - redispatch_orphaned: re-dispatch tasks with no Temporal workflow (sharded)
    - redispatch_pool: the same for POOL tasks — separately named on purpose,
      see the decision-C note below; never part of the default
    - reset_stuck: reset stuck tasks to pending (sharded)
    - skip_zombie: revoke permanently failed tasks (retry_count >= 8)

    There is deliberately no "restart worker" action — restarting a worker is a
    host-level operation and goes through scripts/deploy-workers.sh.

    Decision C removed the reconciler's automatic re-dispatch of pool orphans:
    a second PoolDownloadWorkflow re-runs list -> filter -> chunk, and T1's
    ruling on a chunking mismatch is no-delete + non-retryable error, so an
    unwanted re-dispatch wedges the task instead of healing it. Both manual
    paths that could reach it — `redispatch_orphaned` (the DEFAULT action, and
    what the UI's fix button posts) and `reset_stuck` (via
    status='pending' -> auto_dispatch_pending) — therefore skip pool tasks and
    report them as skipped, unless the operator asks by name with
    `redispatch_pool`. The matching `pool_orphaned` alert points at that action.
    """
    from ..temporal_client import start_task_download

    tasks, _, _, _ = await run_blocking(_read_state)
    now = time.time()
    results = {}
    actions = req.actions or ["redispatch_orphaned"]

    unknown = [a for a in actions
               if a not in ("redispatch_orphaned", "redispatch_pool",
                            "reset_stuck", "skip_zombie")]
    if unknown:
        results["unsupported_actions"] = unknown

    allow_pool = "redispatch_pool" in actions
    skipped_pool: list[str] = []

    def _skip_pool(t: dict, action: str):
        skipped_pool.append(
            f"{t.get('name') or t['id']}: pool task skipped by {action} — a new "
            f"pool coordinator re-runs list/filter/chunk and can wedge the task "
            f"on a chunking mismatch (no-delete + error). Check the "
            f"pool_orphaned alert, then POST /api/doctor with "
            f'{{"actions": ["redispatch_pool"]}} to do it deliberately.'
        )

    def _is_pool(t: dict) -> bool:
        return (t.get("dispatch_mode") or "sharded") == "pool"

    if "redispatch_orphaned" in actions or allow_pool:
        redispatched = []
        redispatched_pool = []
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
                if _is_pool(t):
                    if not allow_pool:
                        _skip_pool(t, "redispatch_orphaned")
                        continue
                elif "redispatch_orphaned" not in actions:
                    continue  # only the explicit pool action was requested
                try:
                    # Unified dispatch entry — branches on dispatch_mode. The
                    # legacy workflow has no BOS resume filter and would
                    # re-download everything already uploaded.
                    await start_task_download(t)
                    (redispatched_pool if _is_pool(t) else redispatched).append(
                        t.get("name", task_id))
                except Exception as e:
                    if "already started" not in str(e).lower():
                        (redispatched_pool if _is_pool(t) else redispatched).append(
                            f"{t.get('name', task_id)} (FAILED: {e})")
        except Exception as e:
            redispatched.append(f"ERROR: {e}")
        if "redispatch_orphaned" in actions:
            results["redispatch_orphaned"] = redispatched
        if allow_pool:
            results["redispatch_pool"] = redispatched_pool

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
        if not allow_pool:
            # status='pending' hands the task to auto_dispatch_pending, which
            # starts a fresh pool coordinator — the same wedge by another route.
            keep = []
            for t in stuck:
                if _is_pool(t):
                    _skip_pool(t, "reset_stuck")
                else:
                    keep.append(t)
            stuck = keep
        results["reset_stuck"] = await run_blocking(
            _rewrite(stuck, status="pending", phase="reset_by_doctor"))

    if "skip_zombie" in actions:
        zombies = [t for t in tasks
                   if t.get("status") == "failed" and (t.get("retry_count") or 0) >= 8]
        results["skip_zombie"] = await run_blocking(
            _rewrite(zombies, status="revoked", phase=None))

    if skipped_pool:
        results["skipped_pool_tasks"] = skipped_pool

    return results
