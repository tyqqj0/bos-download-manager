"""Workflow Reconciler — self-healing for orphaned downloads.

Runs every RECONCILE_INTERVAL seconds in the background scheduler.
Detects tasks stuck in 'downloading' that have no corresponding Temporal
workflow, and auto-re-dispatches them.

This prevents the failure mode where a code deploy or Temporal issue kills
workflows but leaves SQLite tasks in 'downloading' forever.
"""

import asyncio
import logging
import time

logger = logging.getLogger("dlm.reconciler")

RECONCILE_INTERVAL = 300  # 5 minutes
SPEED_STALE_THRESHOLD = 300  # 5 minutes without update = zero out speed
RECENT_CLOSED_LIMIT = 500  # cap on the unfiltered closed-workflow scan

# Single definitions — the doctor reports on the same thresholds the
# reconciler acts on, and they must not drift apart.
from .fleet import (  # noqa: E402
    DEAD_THRESHOLD,
    MIN_SHARD_DISK_GB as MIN_DISPATCH_DISK_GB,
    STALE_THRESHOLD,
    has_live_workflow,
)


async def reconcile() -> dict:
    """Compare SQLite downloading tasks vs Temporal running workflows.

    Returns a report of actions taken.
    """
    from ..queue.snapshot import (
        get_tasks_by_status, update_task_progress, get_shards_by_task,
        complete_task, init_db, _conn,
    )
    from .temporal_client import running_workflows, start_sharded_download

    init_db()
    report = {
        "checked_at": time.time(),
        "downloading_tasks": 0,
        "running_workflows": 0,
        "orphaned": [],
        "redispatched": [],
        "stale": [],
        "errors": [],
    }

    try:
        downloading = get_tasks_by_status("downloading")
        report["downloading_tasks"] = len(downloading)
    except Exception as e:
        report["errors"].append(f"Failed to read tasks: {e}")
        return report

    if not downloading:
        return report

    # Get running workflows from Temporal
    try:
        running_ids = set(await running_workflows())
        report["running_workflows"] = len(running_ids)
    except Exception as e:
        report["errors"].append(f"Failed to query Temporal: {e}")
        return report

    now = time.time()

    for task in downloading:
        task_id = task["id"]
        updated_at = task.get("updated_at") or 0
        stale_seconds = now - updated_at

        has_workflow = has_live_workflow(task_id, running_ids)

        if not has_workflow:
            # Before re-dispatching, check if all shards are already done.
            # This handles the case where the parent ShardedDownloadWorkflow
            # died but all child ShardWorkerWorkflows completed successfully.
            shards = get_shards_by_task(task_id)
            if shards:
                done_shards = [s for s in shards if s.get("status") == "done"]
                failed_shards = [s for s in shards if s.get("status") == "failed"]
                if len(done_shards) == len(shards):
                    complete_task(task_id, "done")
                    report.setdefault("auto_completed", []).append(task.get("name", task_id))
                    logger.info(
                        f"Reconciler: auto-completed {task.get('name', task_id)} "
                        f"— all {len(shards)} shards done, parent workflow dead"
                    )
                    continue
                if len(done_shards) + len(failed_shards) == len(shards) and failed_shards:
                    complete_task(task_id, "failed")
                    report.setdefault("auto_failed", []).append(task.get("name", task_id))
                    logger.warning(
                        f"Reconciler: marked {task.get('name', task_id)} failed "
                        f"— {len(failed_shards)}/{len(shards)} shards failed, parent dead"
                    )
                    continue

            report["orphaned"].append({
                "task_id": task_id,
                "name": task.get("name", ""),
                "server": task.get("server", ""),
                "stale_seconds": int(stale_seconds),
            })

            # Only re-dispatch if stale for > DEAD_THRESHOLD
            # (gives time for workflows that just started to appear)
            if stale_seconds > DEAD_THRESHOLD:
                try:
                    # Unified sharded path — the legacy DownloadDatasetWorkflow
                    # has no BOS resume filter and must not be reachable here.
                    # Refresh claimed_at so the listing-phase source guard
                    # covers this coordinator too.
                    conn2 = _conn()
                    conn2.execute(
                        "UPDATE tasks SET claimed_at = ? WHERE id = ?",
                        (time.time(), task_id),
                    )
                    conn2.commit()
                    await start_sharded_download(task)
                    report["redispatched"].append(task.get("name", task_id))
                    logger.info(
                        f"Reconciler: re-dispatched {task.get('name', task_id)} "
                        f"as sharded (orphaned {stale_seconds:.0f}s)"
                    )
                except Exception as e:
                    err_msg = str(e)
                    # WorkflowAlreadyStartedError is fine — means it was just dispatched
                    if "already started" in err_msg.lower():
                        logger.debug(f"Workflow already exists for {task_id}")
                    else:
                        report["errors"].append(
                            f"Re-dispatch failed for {task.get('name', '')}: {err_msg}"
                        )
                        logger.error(f"Reconciler: re-dispatch failed for {task_id}: {e}")

        # Detect stale progress (workflow exists but no updates)
        else:
            if stale_seconds > STALE_THRESHOLD:
                report["stale"].append({
                    "task_id": task_id,
                    "name": task.get("name", ""),
                    "server": task.get("server", ""),
                    "stale_seconds": int(stale_seconds),
                })

    if report["redispatched"]:
        logger.warning(
            f"Reconciler: re-dispatched {len(report['redispatched'])} orphaned tasks: "
            f"{report['redispatched']}"
        )

    return report


async def auto_dispatch_pending() -> dict:
    """Auto-dispatch pending tasks to idle workers.

    Idle = worker heartbeat alive (last_seen < 180s) but no running workflow.
    Dispatch one pending task per idle worker, priority order.

    Uses optimistic locking: UPDATE tasks SET status='downloading' WHERE status='pending' AND id=?
    to prevent TOCTOU race with concurrent dispatches.
    """
    from ..queue.snapshot import (
        get_tasks_by_status, get_workers, get_running_shards, _conn, init_db,
    )
    from .temporal_client import start_sharded_download

    init_db()
    report = {"dispatched": [], "errors": []}

    try:
        # 1. Find alive workers (deduped by freshest heartbeat row)
        from .fleet import (
            alive_workers as compute_alive, busy_servers as compute_busy,
            worker_serves,
        )

        now = time.time()
        alive = compute_alive(get_workers(), now)
        if not alive:
            return report

        # 2. Busy = holds a downloading task OR a running shard
        downloading = get_tasks_by_status("downloading")
        busy = compute_busy(downloading, get_running_shards())

        idle_workers = [w for w in alive if (w.get("server_key") or "") not in busy]
        if not idle_workers:
            return report

        # 3. Get pending tasks (priority order)
        pending = get_tasks_by_status("pending")
        if not pending:
            return report
        pending.sort(key=lambda t: (t.get("priority", 5), t.get("created_at", "")))

        # 4. Coordinator-race guard: a downloading task with no shard rows means
        #    its sharded coordinator is still listing/filtering — its workers
        #    look idle but will be claimed shortly. Don't dispatch that source
        #    until shards exist. Keyed on claimed_at (set once at claim time,
        #    never refreshed by progress reports) so a legacy non-sharded task
        #    can't pin its source forever; a dead coordinator stops blocking
        #    after 15 min (reconcile() cleans it up).
        conn = _conn()
        listing_cutoff = time.time() - 900
        rows = conn.execute(
            "SELECT DISTINCT t.source FROM tasks t "
            "WHERE t.status = 'downloading' "
            "AND t.claimed_at > ? "
            "AND NOT EXISTS (SELECT 1 FROM shards s WHERE s.task_id = t.id)",
            (listing_cutoff,),
        ).fetchall()
        sources_in_listing = {r[0] for r in rows}

        # 5. Dispatch: unified sharded path for all sources.
        #    Source routing: ModelScope → bj* workers, HuggingFace → w* workers
        for worker in idle_workers:
            if not pending:
                break

            # Disk capacity check: skip workers with insufficient free space
            worker_disk = worker.get("disk_free_gb") or 0
            server_key = worker.get("server_key", "")
            if worker_disk < MIN_DISPATCH_DISK_GB:
                logger.warning(
                    f"Auto-dispatch: skip {server_key} "
                    f"(only {worker_disk:.1f}GB free < {MIN_DISPATCH_DISK_GB}GB required)"
                )
                continue

            # Find first compatible task for this worker
            task = None
            for i, t in enumerate(pending):
                source = t.get("source", "hf")
                if source in sources_in_listing:
                    continue  # a coordinator for this source is still partitioning
                if not worker_serves(server_key, source):
                    continue  # ModelScope → bj*, everything else → w*
                task = pending.pop(i)
                break

            if task is None:
                continue

            # Optimistic lock: claim status only. server stays NULL — the
            # coordinator assigns servers per shard, and a task-level server
            # would count this worker as busy in its own idle query.
            now_ts = time.time()
            cursor = conn.execute(
                "UPDATE tasks SET status = 'downloading', server = NULL, "
                "updated_at = ?, claimed_at = ? "
                "WHERE id = ? AND status = 'pending'",
                (now_ts, now_ts, task["id"]),
            )
            conn.commit()

            if cursor.rowcount == 0:
                continue  # someone else claimed it

            # Guard the source immediately — even an "already started" race
            # below must not let a second coordinator spawn this cycle
            sources_in_listing.add(task.get("source", "hf"))

            try:
                await start_sharded_download(task)
                report["dispatched"].append({
                    "task": task.get("name", task["id"]),
                    "worker": "sharded",
                    "mode": "sharded",
                })
                logger.info(f"Auto-dispatch: {task.get('name')} → sharded coordinator")
            except Exception as e:
                err_msg = str(e).lower()
                if "already started" in err_msg:
                    # Workflow already running (raced with manual dispatch) — keep claim
                    pass
                else:
                    # Revert the status change
                    conn.execute(
                        "UPDATE tasks SET status = 'pending', server = NULL WHERE id = ?",
                        (task["id"],),
                    )
                    conn.commit()
                    report["errors"].append(f"Dispatch failed for {task.get('name')}: {e}")
                    logger.error(f"Auto-dispatch failed for {task['id']}: {e}")

    except Exception as e:
        report["errors"].append(f"Auto-dispatch error: {e}")
        logger.error(f"Auto-dispatch error: {e}")

    return report


IDLE_WORKER_THRESHOLD = 600  # 10 minutes idle = suspicious


async def detect_idle_workers() -> dict:
    """Find workers that are online but have no running workflow.

    Catches the failure mode where split child workflows fail silently
    and the worker sits idle while the dashboard shows "downloading".
    """
    from ..queue.snapshot import get_tasks_by_status, get_workers, get_running_shards, init_db
    from .temporal_client import QUERY_TIMEOUT, connected_client, running_workflows

    init_db()
    report = {"idle_workers": [], "failed_splits": [], "errors": []}

    try:
        from .fleet import alive_workers as compute_alive, busy_servers as compute_busy

        now = time.time()
        alive_workers = compute_alive(get_workers(), now)
        if not alive_workers:
            return report

        downloading = get_tasks_by_status("downloading")
        busy_servers = compute_busy(downloading, get_running_shards())

        # Query Temporal for running workflows per task queue
        client = await connected_client()
        running_by_queue = {}
        for wf_id, queue in (await running_workflows(client)).items():
            running_by_queue.setdefault(queue, []).append(wf_id)

        # Also check for recently-failed workflows on each task queue.
        # Capped: this query carries no WorkflowType filter, so it walks every
        # closed execution in the namespace over a 2-day window — unbounded,
        # it can run for minutes on every 5-minute reconcile.
        recently_failed = {}
        try:
            async for wf in client.list_workflows(
                'ExecutionStatus="Completed" AND CloseTime > "2d"',
                limit=RECENT_CLOSED_LIMIT,
                rpc_timeout=QUERY_TIMEOUT,
            ):
                recently_failed.setdefault(wf.task_queue, []).append({
                    "id": wf.id,
                    "close_time": str(wf.close_time),
                })
        except Exception:
            pass

        # Deduplicate workers by server_key
        seen_keys = set()
        for w in alive_workers:
            key = w.get("server_key", "")
            if not key or key in seen_keys:
                continue
            seen_keys.add(key)

            queue_name = f"download-{key}"
            has_running_wf = bool(running_by_queue.get(queue_name))
            # busy_servers already folds in running shard ownership, which is
            # how a sharded task claims a worker (its task row has server=NULL)
            has_downloading_task = key in busy_servers

            if not has_running_wf and not has_downloading_task:
                last_seen = w.get("last_seen") or 0
                idle_duration = now - last_seen
                entry = {
                    "server_key": key,
                    "disk_free_gb": w.get("disk_free_gb"),
                    "idle_since": last_seen,
                }

                # Check if this worker had a recently-failed workflow
                failed = recently_failed.get(queue_name, [])
                if failed:
                    entry["recent_failures"] = failed[:3]
                    report["failed_splits"].append({
                        "server_key": key,
                        "failed_workflows": failed[:3],
                    })

                report["idle_workers"].append(entry)

                if failed:
                    logger.warning(
                        f"Idle worker {key}: online but no workflow. "
                        f"Recent failures: {[f['id'] for f in failed[:3]]}"
                    )
                else:
                    logger.info(f"Idle worker {key}: online, no workflow, no recent failures")

    except Exception as e:
        report["errors"].append(f"Idle worker detection error: {e}")
        logger.error(f"Idle worker detection error: {e}")

    return report


def zero_stale_speeds():
    """Zero out speed_mbps for tasks and shards that haven't reported recently.

    Called by the dashboard builder to prevent showing stale speed values.
    """
    from ..queue.snapshot import _conn, init_db
    init_db()

    conn = _conn()
    now = time.time()
    threshold = now - SPEED_STALE_THRESHOLD

    conn.execute(
        "UPDATE tasks SET speed_mbps = 0 "
        "WHERE status = 'downloading' AND speed_mbps > 0 AND updated_at < ?",
        (threshold,),
    )
    # Also zero upload_speed_mbps if column exists
    try:
        conn.execute(
            "UPDATE tasks SET upload_speed_mbps = 0 "
            "WHERE status = 'downloading' AND upload_speed_mbps > 0 AND updated_at < ?",
            (threshold,),
        )
    except Exception:
        pass  # column may not exist yet
    # Zero stale shard speeds too — prevents ghost speed in dashboard
    try:
        conn.execute(
            "UPDATE shards SET speed_mbps = 0 "
            "WHERE status = 'running' AND speed_mbps > 0 AND updated_at < ?",
            (threshold,),
        )
    except Exception:
        pass
    conn.commit()
