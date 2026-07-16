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
STALE_THRESHOLD = 600     # 10 minutes without update = suspicious
DEAD_THRESHOLD = 1800     # 30 minutes without update = definitely dead
SPEED_STALE_THRESHOLD = 300  # 5 minutes without update = zero out speed


async def reconcile() -> dict:
    """Compare SQLite downloading tasks vs Temporal running workflows.

    Returns a report of actions taken.
    """
    from ..queue.snapshot import get_tasks_by_status, update_task_progress, init_db
    from .temporal_client import get_client, start_download

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
        client = await get_client()
        running_ids = set()
        async for wf in client.list_workflows(
            'WorkflowType="DownloadDatasetWorkflow" AND ExecutionStatus="Running"'
        ):
            running_ids.add(wf.id)
        report["running_workflows"] = len(running_ids)
    except Exception as e:
        report["errors"].append(f"Failed to query Temporal: {e}")
        return report

    now = time.time()

    for task in downloading:
        task_id = task["id"]
        workflow_id = f"dl-{task_id}"
        updated_at = task.get("updated_at") or 0
        stale_seconds = now - updated_at

        # Check if workflow exists
        if workflow_id not in running_ids:
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
                    server = task.get("server", "")
                    queue = f"download-{server}" if server else "download-workers"
                    await start_download(task, task_queue=queue)
                    report["redispatched"].append(task.get("name", task_id))
                    logger.info(
                        f"Reconciler: re-dispatched {task.get('name', task_id)} "
                        f"to {queue} (orphaned {stale_seconds:.0f}s)"
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
        elif stale_seconds > STALE_THRESHOLD:
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
    from ..queue.snapshot import get_tasks_by_status, get_workers, _conn, init_db
    from .temporal_client import get_client, start_download

    init_db()
    report = {"dispatched": [], "errors": []}

    try:
        # 1. Find alive workers
        workers = get_workers()
        now = time.time()
        alive_workers = [w for w in workers if now - (w.get("last_seen") or 0) < 180]
        if not alive_workers:
            return report

        # 2. Find busy workers via SQLite downloading tasks (simpler and more reliable
        #    than querying Temporal, which may timeout or miss recently-started workflows)
        downloading = get_tasks_by_status("downloading")
        busy_servers = {t.get("server") for t in downloading if t.get("server")}

        idle_workers = [
            w for w in alive_workers
            if w.get("server_key") not in busy_servers
        ]

        if not idle_workers:
            return report

        # 3. Get pending tasks (priority order)
        pending = get_tasks_by_status("pending")
        if not pending:
            return report
        pending.sort(key=lambda t: (t.get("priority", 5), t.get("created_at", "")))

        # 4. Dispatch: one task per idle worker (with optimistic locking)
        conn = _conn()
        for worker in idle_workers:
            if not pending:
                break
            task = pending.pop(0)
            server_key = worker.get("server_key", "")

            # Optimistic lock: only claim if still pending
            cursor = conn.execute(
                "UPDATE tasks SET status = 'downloading', server = ?, updated_at = ? "
                "WHERE id = ? AND status = 'pending'",
                (server_key, time.time(), task["id"]),
            )
            conn.commit()

            if cursor.rowcount == 0:
                continue  # someone else claimed it

            # Start workflow
            queue = f"download-{server_key}"
            try:
                await start_download(task, task_queue=queue)
                report["dispatched"].append({
                    "task": task.get("name", task["id"]),
                    "worker": server_key,
                })
                logger.info(f"Auto-dispatch: {task.get('name')} → {server_key}")
            except Exception as e:
                err_msg = str(e).lower()
                if "already started" in err_msg:
                    # Race condition with manual dispatch — harmless
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


def zero_stale_speeds():
    """Zero out speed_mbps for tasks that haven't reported in SPEED_STALE_THRESHOLD.

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
    conn.commit()
