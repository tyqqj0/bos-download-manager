"""Background scheduler — dashboard refresh, reconciliation, and transfer sync."""

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor

from .cache import cache

logger = logging.getLogger("dlm.web")

EXECUTOR_WORKERS = 4
_executor = ThreadPoolExecutor(max_workers=EXECUTOR_WORKERS)

# The two transfer stages get their own pool. They are the only stages that can
# legitimately run for minutes (a 3.4 TB prefix is ~50 BOS list pages per
# dispatched row, and verification lists both ends), and an abandoned stage
# keeps its thread — so on the shared pool one wedged far side would strand a
# thread that the dashboard and the staging GC need.
#
# One slot, deliberately. `_transfer_cycle` awaits poll then dispatch in
# sequence, so a second slot buys no parallelism — all it buys is the ability
# for an abandoned thread (a stage that blew the 600s deadline but is still
# stuck inside a BOS or DCloud call) to run *concurrently with the next cycle*
# and post an import the new cycle is also about to post. One slot makes the
# next cycle queue behind the stuck one instead, which is the safe failure.
TRANSFER_EXECUTOR_WORKERS = 1
_transfer_executor = ThreadPoolExecutor(max_workers=TRANSFER_EXECUTOR_WORKERS)

DASHBOARD_INTERVAL = 10
WORKFLOW_SYNC_INTERVAL = 30
TRANSFER_INTERVAL = 60
RECONCILE_INTERVAL = 300  # 5 minutes
DISPATCH_INTERVAL = 30  # pending-task dispatch cadence (one sharded task per source per cycle)
STAGING_GC_INTERVAL = 3600  # decision G — local-disk-only, terminal-tasks-only sweep

# Every stage below must finish or give up. A `try/except` catches a failure
# but not a hang: one await that never returns stops this `while True` for
# good, and the process keeps serving HTTP the whole time while nothing is
# dispatched, reconciled or verified again. That is the same "alive but the
# control plane is dead" outcome as the 2026-07-31 fork hang, reached from a
# different direction, so the loop bounds each stage rather than trusting the
# callee. Generous: these are normally sub-second.
STAGE_TIMEOUT = 60

# The transfer stages are the exception, by an order of magnitude. Measuring a
# multi-TB BOS prefix is thousands of list calls, and the dispatcher does that
# for up to four rows in one pass; 60s would abandon every cycle on a large
# dataset and never post an import at all. Still bounded — an unbounded await
# here is the wedged control plane again.
TRANSFER_STAGE_TIMEOUT = 600


async def _blocking_stage(loop, fn, name: str, timeout=None, executor=None):
    """Run a blocking stage on a thread pool with a deadline.

    `run_in_executor` carries no timeout of its own, so the three stages that
    used it bare were the hole left in the bound above. They are not
    pure-SQLite either: the transfer stages log into the DCloud API and list
    async tasks, so a peer that accepts the connection and never answers parks
    this `while True` forever — the wedged control plane again, from a third
    direction.

    One caveat stated plainly, because it bounds what this buys: cancelling
    the future does NOT stop the thread. The stage is abandoned, not killed,
    and its pool slot is held for as long as the call hangs. With
    EXECUTOR_WORKERS slots, a stage that hangs every cycle eventually starves
    the pool — but by then every cycle times out and says so in the log,
    instead of the loop going silent on the very first hang, and dispatch and
    reconcile (which await coroutines, not the pool) keep running throughout.
    That is also why the slow transfer stages are handed a separate pool: the
    starvation they can cause is confined to each other.

    Returns None if the stage timed out or raised — callers must not treat
    that as a result.
    """
    deadline = STAGE_TIMEOUT if timeout is None else timeout
    pool = _executor if executor is None else executor
    try:
        return await asyncio.wait_for(
            loop.run_in_executor(pool, fn), timeout=deadline)
    except asyncio.TimeoutError:
        logger.error(
            f"{name} exceeded {deadline}s — stage abandoned; its thread is "
            f"still held (pool has {pool._max_workers} slots)"
        )
    except Exception as e:
        logger.error(f"{name} error: {e}")
    return None


def _build_dashboard() -> dict:
    """Build dashboard from SQLite snapshot."""
    from ..queue.snapshot import get_dashboard_summary, get_all_tasks, get_workers, get_shards_by_task
    summary = get_dashboard_summary()
    workers = get_workers()

    # dispatch_mode isn't in the active_downloads projection (get_dashboard_
    # summary's query is an explicit column list, not SELECT *) — read it
    # from the same get_all_tasks() this function needs a few lines further
    # down anyway, just fetched earlier so the shard-aggregation loop below
    # can branch on it.
    all_tasks = get_all_tasks()
    dispatch_mode_by_id = {t["id"]: t.get("dispatch_mode") for t in all_tasks}

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

            if (dispatch_mode_by_id.get(dl["id"]) or "sharded") == "pool":
                # Decision F: a pool task can carry up to POOL_MAX_BATCHES
                # (1500) batch rows in this same `shards` table. Emitting one
                # shard_servers entry per row, like the sharded branch below,
                # would put a 1500-element array and a multi-kilobyte
                # dl["server"] string on a payload the browser polls every
                # 10s. Aggregate per distinct server instead. The sharded
                # branch (the `else` below) is untouched, character for
                # character — G1 requires that shape stay byte-identical.
                per_server: dict[str, dict] = {}
                for s in shards:
                    server = s.get("server")
                    if not server:
                        continue
                    row = per_server.setdefault(server, {
                        "running": 0, "done": 0, "speed_mbps": 0.0,
                        "done_bytes": 0, "total_bytes": 0,
                    })
                    status = s.get("status")
                    if status == "running":
                        row["running"] += 1
                    elif status == "done":
                        row["done"] += 1
                    row["speed_mbps"] += s.get("speed_mbps", 0) or 0
                    row["done_bytes"] += s.get("done_bytes", 0) or 0
                    row["total_bytes"] += s.get("total_bytes", 0) or 0

                dl["server_batches"] = [
                    {
                        "server": server,
                        "running": row["running"],
                        "done": row["done"],
                        "speed_mbps": round(row["speed_mbps"], 1),
                        "done_pct": round(row["done_bytes"] / row["total_bytes"] * 100, 1)
                        if row["total_bytes"] else 0,
                    }
                    for server, row in sorted(per_server.items())
                ]
                if not dl.get("server"):
                    dl["server"] = ",".join(sorted(per_server)) or None
            else:
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
    """Advance in-flight BOS→地瓜云 transfers. See `dlm/transfer/dispatch.py`.

    A thin delegation on purpose. The body that used to live here wrote
    `transfer_status='done'` the moment the far side said 成功 and verified
    nothing — the same "trust a completion report" mistake the download side
    made. The replacement verifies before it believes, and lives beside the
    dispatcher it shares a state machine with.
    """
    from ..transfer.dispatch import poll_transfers
    report = poll_transfers()
    if report.get("done") or report.get("short") or report.get("failed"):
        logger.info(f"Transfer poll: {report}")
    elif report.get("errors"):
        logger.error(f"Transfer poll errors: {report['errors']}")
    return report


def _dispatch_ready_transfers():
    """Post imports for armed transfers. See `dlm/transfer/dispatch.py`."""
    from ..transfer.dispatch import dispatch_ready_transfers
    report = dispatch_ready_transfers()
    if report.get("dispatched") or report.get("blocked"):
        logger.info(f"Transfer dispatch: {report}")
    elif report.get("errors"):
        logger.error(f"Transfer dispatch errors: {report['errors']}")
    return report


async def _transfer_cycle(loop):
    """One poll-then-dispatch pass over the transfer state machine.

    Poll first, dispatch second: polling frees in-flight slots, so a pass that
    finishes two transfers can post two more immediately instead of waiting out
    another interval. Both halves go through `_blocking_stage`, so neither can
    hang this task — and the task itself is what keeps the minutes they may take
    off the main loop.
    """
    poll_report = await _blocking_stage(
        loop, _poll_transfers, "transfer poll",
        timeout=TRANSFER_STAGE_TIMEOUT, executor=_transfer_executor)
    if poll_report is not None:
        cache.set("transfer_poll_report", poll_report)

    dispatch_report = await _blocking_stage(
        loop, _dispatch_ready_transfers, "transfer dispatch",
        timeout=TRANSFER_STAGE_TIMEOUT, executor=_transfer_executor)
    if dispatch_report is not None:
        cache.set("transfer_dispatch_report", dispatch_report)


async def background_scheduler():
    """Main background loop — refresh dashboard, reconcile workflows, poll transfers."""
    loop = asyncio.get_event_loop()
    last_transfer_poll = 0
    transfer_cycle = None
    last_reconcile = 0
    last_dispatch = 0
    last_health_verify = 0
    # Seeded to "now", unlike the stages above, so the first sweep is deferred
    # by a full STAGING_GC_INTERVAL. At 0 the GC fired on the very first pass,
    # ~2s after `systemctl restart dlm-web` — i.e. a restart could delete
    # staging before a human had any chance to read GET /api/doctor/staging-gc,
    # the dry-run preview that exists precisely to be read first.
    last_staging_gc = time.time()

    await asyncio.sleep(2)

    while True:
        try:
            # Zero stale speeds before building dashboard
            from .reconciler import zero_stale_speeds
            await _blocking_stage(loop, zero_stale_speeds, "zero_stale_speeds")

            dashboard = await _blocking_stage(loop, _build_dashboard, "build_dashboard")
            if dashboard is not None:
                cache.set_dashboard(dashboard)

            now = time.time()
            # Transfers run as their own task, not inline: a dispatch pass can
            # legitimately take minutes (see TRANSFER_STAGE_TIMEOUT), and
            # awaiting that here would park the dashboard, auto-dispatch and
            # reconciler behind it for exactly as long. One cycle at a time —
            # while a pass is still running, `last_transfer_poll` is left alone
            # so the next iteration re-checks instead of piling on a second.
            if (now - last_transfer_poll > TRANSFER_INTERVAL
                    and (transfer_cycle is None or transfer_cycle.done())):
                transfer_cycle = asyncio.create_task(_transfer_cycle(loop))
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

            # Staging GC (decision G): local-disk-only, terminal-tasks-only
            # sweep of /data/staging/{task_name} across the fleet. Hourly —
            # slower than every other stage, since it walks every server
            # over ssh, which the wait_for below still bounds at
            # STAGE_TIMEOUT like every other blocking stage here.
            if now - last_staging_gc > STAGING_GC_INTERVAL:
                # _blocking_stage, not a bare run_in_executor + wait_for: the
                # wrapper is the single place the deadline, the timeout log
                # line and the "returns None on failure" contract live, and
                # this stage is the slowest of them (ssh to every server).
                # test_event_loop_safety pins that no stage in this loop
                # re-implements it.
                from .reconciler import staging_gc
                gc_report = await _blocking_stage(loop, staging_gc, "staging_gc")
                if gc_report is not None:
                    cache.set("staging_gc_report", gc_report)
                    if gc_report.get("removed"):
                        logger.info(
                            f"Staging GC removed: "
                            f"{[r['name'] for r in gc_report['removed']]}"
                        )
                    if gc_report.get("errors"):
                        logger.error(f"Staging GC errors: {gc_report['errors']}")
                last_staging_gc = now

        except Exception as e:
            logger.error(f"Scheduler error: {e}")

        await asyncio.sleep(DASHBOARD_INTERVAL)
