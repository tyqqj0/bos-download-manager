"""Background scheduler — dashboard cache refresh and worker status polling.

Much simpler than before: Celery handles task dispatch, retry, and recovery.
This scheduler only:
1. Builds dashboard summary from SQLite snapshot (every 10s)
2. Polls Celery worker status (every 30s)
3. Polls D-Robotics transfer status for in-progress transfers (every 60s)
"""

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor

from .cache import cache

logger = logging.getLogger("dlm.web")

_executor = ThreadPoolExecutor(max_workers=4)

DASHBOARD_INTERVAL = 10
WORKER_INTERVAL = 30
TRANSFER_INTERVAL = 60


def _build_dashboard() -> dict:
    """Build dashboard from SQLite snapshot."""
    from ..queue.snapshot import get_dashboard_summary, get_all_tasks, get_workers
    summary = get_dashboard_summary()
    workers = get_workers()

    now = time.time()
    active_workers = [w for w in workers if now - (w.get("last_seen") or 0) < 180]

    summary["workers"] = workers
    summary["active_worker_count"] = len(active_workers)

    all_tasks = get_all_tasks()
    recent = sorted(
        [t for t in all_tasks if t.get("status") in ("done", "failed") and t.get("completed_at")],
        key=lambda t: t.get("completed_at", ""),
        reverse=True,
    )[:10]
    summary["recent_activity"] = recent

    queue_next = [t for t in all_tasks if t.get("status") == "pending"][:5]
    summary["queue_next"] = queue_next

    alerts = _build_alerts(all_tasks, workers)
    summary["alerts"] = alerts

    return summary


def _build_alerts(tasks: list, workers: list) -> list:
    """Generate alerts from current state."""
    alerts = []
    now = time.time()

    for w in workers:
        if now - (w.get("last_seen") or 0) > 180 and w.get("status") != "offline":
            alerts.append({
                "type": "worker_offline",
                "server": w.get("server_key", w.get("hostname", "")),
                "duration_min": int((now - (w.get("last_seen") or now)) / 60),
            })

    for t in tasks:
        if t.get("status") == "failed" and (t.get("retry_count") or 0) >= 5:
            alerts.append({
                "type": "task_failed_repeat",
                "task": t.get("name", ""),
                "count": t.get("retry_count", 0),
                "error": t.get("error_class") or t.get("error") or "",
            })

    return alerts


def _poll_celery_workers() -> list:
    """Get active Celery workers via inspect."""
    from ..queue.app import app as celery_app
    from ..queue.snapshot import update_worker

    try:
        inspect = celery_app.control.inspect(timeout=5)
        ping_result = inspect.ping() or {}
        active_result = inspect.active() or {}

        workers = []
        for worker_name, _ in ping_result.items():
            parts = worker_name.split("@")
            server_key = parts[0] if parts else worker_name

            active_tasks = active_result.get(worker_name, [])
            current_task = active_tasks[0]["id"] if active_tasks else None

            update_worker(
                hostname=worker_name,
                server_key=server_key,
                status="busy" if current_task else "idle",
                current_task_id=current_task,
            )
            workers.append({
                "hostname": worker_name,
                "server_key": server_key,
                "status": "busy" if current_task else "idle",
                "current_task_id": current_task,
            })

        return workers
    except Exception as e:
        logger.debug(f"Celery inspect failed: {e}")
        return []


def _poll_transfers():
    """Check status of in-progress D-Robotics transfers."""
    import os
    from ..queue.snapshot import get_tasks_by_status, update_task_progress, _conn

    conn = _conn()
    rows = conn.execute(
        "SELECT id, name, transfer_task_id FROM tasks WHERE transfer_status = 'transferring'"
    ).fetchall()
    transferring = [dict(r) for r in rows]

    if not transferring:
        return 0

    dcloud_user = os.environ.get("DCLOUD_USER")
    dcloud_pass = os.environ.get("DCLOUD_PASS")
    if not dcloud_user or not dcloud_pass:
        return 0

    try:
        from ..transfer.dcloud import DCloudClient
        client = DCloudClient(dcloud_user, dcloud_pass)
        client.login()

        async_tasks = client.list_async_tasks(page_size=100)
        task_status_map = {t.get("task_id"): t for t in async_tasks}

        updated = 0
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")

        for task in transferring:
            if not task.get("transfer_task_id"):
                continue
            remote = task_status_map.get(task["transfer_task_id"])
            if not remote:
                continue
            status = remote.get("status", "")
            if status in ("成功", "success", "done"):
                conn.execute(
                    "UPDATE tasks SET transfer_status = ?, transfer_error = NULL, updated_at = ? WHERE id = ?",
                    ("done", time.time(), task["id"]),
                )
                updated += 1
                logger.info(f"Transfer done: {task['name']}")
            elif status in ("失败", "failed", "error"):
                conn.execute(
                    "UPDATE tasks SET transfer_status = ?, transfer_error = ?, updated_at = ? WHERE id = ?",
                    ("failed", remote.get("error_msg", status), time.time(), task["id"]),
                )
                updated += 1
                logger.warning(f"Transfer failed: {task['name']}")

        if updated:
            conn.commit()
        return updated
    except Exception as e:
        logger.error(f"Transfer poll error: {e}")
        return 0


async def background_scheduler():
    """Main background loop — refresh dashboard, poll workers and transfers."""
    loop = asyncio.get_event_loop()
    last_worker_poll = 0
    last_transfer_poll = 0

    await asyncio.sleep(2)

    while True:
        try:
            dashboard = await loop.run_in_executor(_executor, _build_dashboard)
            cache.set_dashboard(dashboard)

            now = time.time()

            if now - last_worker_poll > WORKER_INTERVAL:
                workers = await loop.run_in_executor(_executor, _poll_celery_workers)
                cache.set_servers({w["server_key"]: w for w in workers})
                last_worker_poll = now

            if now - last_transfer_poll > TRANSFER_INTERVAL:
                count = await loop.run_in_executor(_executor, _poll_transfers)
                if count:
                    logger.info(f"Transfer poll: {count} updates")
                last_transfer_poll = now

        except Exception as e:
            logger.error(f"Scheduler error: {e}")

        await asyncio.sleep(DASHBOARD_INTERVAL)
