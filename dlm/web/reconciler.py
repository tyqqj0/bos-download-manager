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
MIN_DISPATCH_DISK_GB = 70  # must exceed pipeline backpressure threshold (~60GB for 200GB disk)


async def reconcile() -> dict:
    """Compare SQLite downloading tasks vs Temporal running workflows.

    Returns a report of actions taken.
    """
    from ..queue.snapshot import (
        get_tasks_by_status, update_task_progress, get_shards_by_task,
        complete_task, init_db,
    )
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
        for wf_type in ["DownloadDatasetWorkflow", "SplitDownloadWorkflow",
                        "ShardedDownloadWorkflow", "ShardWorkerWorkflow"]:
            async for wf in client.list_workflows(
                f'WorkflowType="{wf_type}" AND ExecutionStatus="Running"'
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

        has_workflow = (
            workflow_id in running_ids
            or f"split-download-{task_id}" in running_ids
            or f"sharded-{task_id}" in running_ids
            or any(wid.startswith(f"shard-s-{task_id}-") for wid in running_ids)
            or any(wid.startswith(f"{task_id}-part") for wid in running_ids)
            or any(wid.startswith(f"{workflow_id}-") for wid in running_ids)
        )

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
    from .temporal_client import get_client, start_download, start_sharded_download

    init_db()
    report = {"dispatched": [], "errors": []}

    try:
        # 1. Find alive workers
        workers = get_workers()
        now = time.time()
        alive_workers = [w for w in workers if now - (w.get("last_seen") or 0) < 180]
        if not alive_workers:
            return report

        # 2. Find busy workers — check BOTH tasks table AND shards table
        downloading = get_tasks_by_status("downloading")
        busy_from_tasks = {t.get("server") for t in downloading if t.get("server")}
        running_shards = get_running_shards()
        busy_from_shards = {s.get("server") for s in running_shards if s.get("server")}
        busy_servers = busy_from_tasks | busy_from_shards

        # Deduplicate by server_key (heartbeat can register multiple entries)
        seen_keys = set()
        idle_workers = []
        for w in alive_workers:
            key = w.get("server_key")
            if key and key not in busy_servers and key not in seen_keys:
                seen_keys.add(key)
                idle_workers.append(w)

        if not idle_workers:
            return report

        # 3. Get pending tasks (priority order)
        pending = get_tasks_by_status("pending")
        if not pending:
            return report
        pending.sort(key=lambda t: (t.get("priority", 5), t.get("created_at", "")))

        # 4. Dispatch: one task per idle worker (with optimistic locking)
        #    Source routing: ModelScope → bj* workers, HuggingFace → w* workers
        conn = _conn()
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

            # TEMPORARY BLACKLIST (2026-07-30): bj1-4 running manual AgiBotWorld-Beta
            # split downloads — auto_dispatch must not assign new tasks to them.
            # Remove after AgiBotWorld-Beta completes (~44.75TB, est. 2026-08-01).
            if server_key in ("bj1", "bj2", "bj3", "bj4"):
                continue

            is_bj = server_key.startswith("bj")

            # Find first compatible task for this worker
            task = None
            for i, t in enumerate(pending):
                source = t.get("source", "hf")
                if is_bj and source != "modelscope":
                    continue  # BJ workers only handle ModelScope
                if not is_bj and source == "modelscope":
                    continue  # HK workers skip ModelScope (too slow)
                task = pending.pop(i)
                break

            if task is None:
                continue

            # Optimistic lock: only claim if still pending
            cursor = conn.execute(
                "UPDATE tasks SET status = 'downloading', server = ?, updated_at = ? "
                "WHERE id = ? AND status = 'pending'",
                (server_key, time.time(), task["id"]),
            )
            conn.commit()

            if cursor.rowcount == 0:
                continue  # someone else claimed it

            # Start workflow — bj workers use legacy single-worker, w* use sharded
            queue = f"download-{server_key}"
            try:
                if is_bj:
                    await start_download(task, task_queue=queue)
                else:
                    await start_sharded_download(task)
                report["dispatched"].append({
                    "task": task.get("name", task["id"]),
                    "worker": server_key,
                    "mode": "legacy" if is_bj else "sharded",
                })
                logger.info(f"Auto-dispatch: {task.get('name')} → {server_key} ({'legacy' if is_bj else 'sharded'})")
                if not is_bj:
                    break  # sharded workflow grabs all idle workers — only start one per cycle
            except Exception as e:
                err_msg = str(e).lower()
                if "already started" in err_msg:
                    # Workflow already running (raced with manual dispatch) — keep server assignment
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
    from .temporal_client import get_client

    init_db()
    report = {"idle_workers": [], "failed_splits": [], "errors": []}

    try:
        workers = get_workers()
        now = time.time()
        alive_workers = [w for w in workers if now - (w.get("last_seen") or 0) < 180]

        if not alive_workers:
            return report

        downloading = get_tasks_by_status("downloading")
        busy_from_tasks = {t.get("server") for t in downloading if t.get("server")}
        running_shards = get_running_shards()
        busy_from_shards = {s.get("server") for s in running_shards if s.get("server")}
        busy_servers = busy_from_tasks | busy_from_shards

        # Query Temporal for running workflows per task queue
        client = await get_client()
        running_by_queue = {}
        for wf_type in ["DownloadDatasetWorkflow", "SplitDownloadWorkflow",
                        "ShardedDownloadWorkflow", "ShardWorkerWorkflow"]:
            async for wf in client.list_workflows(
                f'WorkflowType="{wf_type}" AND ExecutionStatus="Running"'
            ):
                running_by_queue.setdefault(wf.task_queue, []).append(wf.id)

        # Also check for recently-failed workflows on each task queue
        recently_failed = {}
        try:
            async for wf in client.list_workflows(
                'ExecutionStatus="Completed" AND CloseTime > "2d"'
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
