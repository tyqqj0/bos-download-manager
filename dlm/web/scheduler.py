"""Background scheduler — dashboard refresh, reconciliation, and transfer sync."""

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor

from .cache import cache

logger = logging.getLogger("dlm.web")

_executor = ThreadPoolExecutor(max_workers=4)

DASHBOARD_INTERVAL = 10
WORKFLOW_SYNC_INTERVAL = 30
TRANSFER_INTERVAL = 60
RECONCILE_INTERVAL = 300  # 5 minutes


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

    from .alerts import check_alerts
    alerts = check_alerts(all_tasks, workers)
    summary["alerts"] = alerts

    return summary


def _build_alerts(tasks: list, workers: list) -> list:
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

        # Stuck download detection: downloading but no update in 30 minutes
        if t.get("status") == "downloading":
            updated_at = t.get("updated_at") or 0
            if now - updated_at > 1800:
                alerts.append({
                    "type": "task_stuck",
                    "task": t.get("name", ""),
                    "task_id": t.get("id", ""),
                    "stale_min": int((now - updated_at) / 60),
                    "server": t.get("server", ""),
                })

    return alerts


def _poll_transfers():
    """Check status of in-progress D-Robotics transfers."""
    import os
    from ..queue.snapshot import _conn

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
        now_ts = time.time()

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
                    ("done", now_ts, task["id"]),
                )
                updated += 1
            elif status in ("失败", "failed", "error"):
                conn.execute(
                    "UPDATE tasks SET transfer_status = ?, transfer_error = ?, updated_at = ? WHERE id = ?",
                    ("failed", remote.get("error_msg", status), now_ts, task["id"]),
                )
                updated += 1

        if updated:
            conn.commit()
        return updated
    except Exception as e:
        logger.error(f"Transfer poll error: {e}")
        return 0


async def background_scheduler():
    """Main background loop — refresh dashboard, reconcile workflows, poll transfers."""
    loop = asyncio.get_event_loop()
    last_transfer_poll = 0
    last_reconcile = 0

    await asyncio.sleep(2)

    while True:
        try:
            # Zero stale speeds before building dashboard
            from .reconciler import zero_stale_speeds
            await loop.run_in_executor(_executor, zero_stale_speeds)

            dashboard = await loop.run_in_executor(_executor, _build_dashboard)
            cache.set_dashboard(dashboard)

            now = time.time()
            if now - last_transfer_poll > TRANSFER_INTERVAL:
                await loop.run_in_executor(_executor, _poll_transfers)
                last_transfer_poll = now

            # Reconcile: detect orphaned workflows and re-dispatch
            if now - last_reconcile > RECONCILE_INTERVAL:
                try:
                    from .reconciler import reconcile
                    report = await reconcile()
                    if report.get("redispatched") or report.get("errors"):
                        logger.info(f"Reconciler report: {report}")
                    cache.set("reconciler_report", report)
                except Exception as e:
                    logger.error(f"Reconciler error: {e}")

                # Auto-dispatch pending tasks to idle workers
                try:
                    from .reconciler import auto_dispatch_pending
                    dispatch_report = await auto_dispatch_pending()
                    if dispatch_report.get("dispatched"):
                        logger.info(f"Auto-dispatch: {dispatch_report['dispatched']}")
                    cache.set("auto_dispatch_report", dispatch_report)
                except Exception as e:
                    logger.error(f"Auto-dispatch error: {e}")

                last_reconcile = now

        except Exception as e:
            logger.error(f"Scheduler error: {e}")

        await asyncio.sleep(DASHBOARD_INTERVAL)
