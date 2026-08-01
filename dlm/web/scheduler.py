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
DISPATCH_INTERVAL = 30  # pending-task dispatch cadence (one sharded task per source per cycle)

# Every stage below must finish or give up. A `try/except` catches a failure
# but not a hang: one await that never returns stops this `while True` for
# good, and the process keeps serving HTTP the whole time while nothing is
# dispatched, reconciled or verified again. That is the same "alive but the
# control plane is dead" outcome as the 2026-07-31 fork hang, reached from a
# different direction, so the loop bounds each stage rather than trusting the
# callee. Generous: these are normally sub-second.
STAGE_TIMEOUT = 60


def _build_dashboard() -> dict:
    """Build dashboard from SQLite snapshot."""
    from ..queue.snapshot import get_dashboard_summary, get_all_tasks, get_workers, get_shards_by_task
    summary = get_dashboard_summary()
    workers = get_workers()

    # Fix sharded task aggregation: override task-level speed/progress with
    # shard aggregates so per-shard progress_fn writes don't confuse the dashboard.
    for dl in summary.get("active_downloads", []):
        shards = get_shards_by_task(dl["id"])
        # Any sharded task — including a 1-shard one — carries its servers on
        # the shards, not the task row (which stays NULL). Skipping the
        # single-shard case left those tasks rendering their server as "?".
        if shards:
            done_bytes = sum(s.get("done_bytes", 0) for s in shards)
            total_bytes = sum(s.get("total_bytes", 0) for s in shards)
            speed = sum(s.get("speed_mbps", 0) for s in shards)
            dl["speed_mbps"] = round(speed, 1)
            dl["progress_pct"] = round(done_bytes / total_bytes * 100, 1) if total_bytes > 0 else 0
            dl["downloaded_gb"] = round(done_bytes / (1024 ** 3), 2)
            dl["size_gb"] = round(total_bytes / (1024 ** 3), 2)
            dl["total_shards"] = len(shards)
            dl["done_shards"] = sum(1 for s in shards if s.get("status") == "done")
            dl["shard_servers"] = [
                {"server": s.get("server", "?"), "speed_mbps": round(s.get("speed_mbps", 0), 1),
                 "done_pct": round(s.get("done_bytes", 0) / s.get("total_bytes", 1) * 100, 1) if s.get("total_bytes") else 0}
                for s in shards
            ]
            if not dl.get("server"):
                dl["server"] = ",".join(
                    s.get("server") for s in shards if s.get("server")
                ) or None
    # Recalc aggregate speed from corrected values
    summary["aggregate_speed_mbps"] = round(
        sum(dl.get("speed_mbps", 0) for dl in summary.get("active_downloads", [])), 1)
    summary["aggregate_download_speed_mbps"] = summary["aggregate_speed_mbps"]

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
    last_dispatch = 0
    last_health_verify = 0

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

            # Auto-dispatch pending tasks to idle workers (own 30s cadence —
            # decoupled from the 5-min reconcile so new tasks start promptly)
            if now - last_dispatch > DISPATCH_INTERVAL:
                try:
                    from .reconciler import auto_dispatch_pending
                    dispatch_report = await asyncio.wait_for(
                        auto_dispatch_pending(), timeout=STAGE_TIMEOUT)
                    if dispatch_report.get("dispatched"):
                        logger.info(f"Auto-dispatch: {dispatch_report['dispatched']}")
                    cache.set("auto_dispatch_report", dispatch_report)
                except Exception as e:
                    logger.error(f"Auto-dispatch error: {e}")
                last_dispatch = now

            # Reconcile: detect orphaned workflows and re-dispatch
            if now - last_reconcile > RECONCILE_INTERVAL:
                try:
                    from .reconciler import reconcile
                    report = await asyncio.wait_for(
                        reconcile(), timeout=STAGE_TIMEOUT)
                    if report.get("redispatched") or report.get("errors"):
                        logger.info(f"Reconciler report: {report}")
                    cache.set("reconciler_report", report)
                except Exception as e:
                    logger.error(f"Reconciler error: {e}")

                # Detect idle workers (online but no workflow — failed splits)
                try:
                    from .reconciler import detect_idle_workers
                    idle_report = await asyncio.wait_for(
                        detect_idle_workers(), timeout=STAGE_TIMEOUT)
                    cache.set("idle_worker_report", idle_report)
                    if idle_report.get("idle_workers"):
                        logger.warning(
                            f"Idle workers detected: "
                            f"{[w['server_key'] for w in idle_report['idle_workers']]}"
                        )
                    if idle_report.get("failed_splits"):
                        logger.error(
                            f"Failed split workflows: {idle_report['failed_splits']}"
                        )
                except Exception as e:
                    logger.error(f"Idle worker detection error: {e}")

                last_reconcile = now

            # Layer 3: cross-layer health correlation (every 5 min).
            # Reads heartbeat data only — it must never SSH or fork, see
            # health_verifier's module docstring for what that cost us.
            if now - last_health_verify > RECONCILE_INTERVAL:
                try:
                    from .health_verifier import verify_all_workers
                    verify_report = await asyncio.wait_for(
                        verify_all_workers(), timeout=STAGE_TIMEOUT)
                    cache.set("health_verify_report", verify_report)
                    if verify_report.get("anomalies"):
                        logger.warning(
                            f"Health verify anomalies: {verify_report['anomalies']}"
                        )
                except Exception as e:
                    logger.error(f"Health verify error: {e}")
                last_health_verify = now

        except Exception as e:
            logger.error(f"Scheduler error: {e}")

        await asyncio.sleep(DASHBOARD_INTERVAL)
