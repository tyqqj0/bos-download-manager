"""Workflow Reconciler — self-healing for orphaned downloads.

Runs every RECONCILE_INTERVAL seconds in the background scheduler.
Detects tasks stuck in 'downloading' that have no corresponding Temporal
workflow, and auto-re-dispatches them.

This prevents the failure mode where a code deploy or Temporal issue kills
workflows but leaves SQLite tasks in 'downloading' forever.
"""

import asyncio
import logging
import re
import time

logger = logging.getLogger("dlm.reconciler")

RECONCILE_INTERVAL = 300  # 5 minutes
SPEED_STALE_THRESHOLD = 300  # 5 minutes without update = zero out speed
RECENT_CLOSED_LIMIT = 500  # cap on the unfiltered closed-workflow scan

# Single definitions — the doctor reports on the same thresholds the
# reconciler acts on, and they must not drift apart.
from .fleet import (  # noqa: E402
    DEAD_THRESHOLD,
    GC_REMOVABLE_STATUSES,
    MIN_SHARD_DISK_GB as MIN_DISPATCH_DISK_GB,
    POOL_MAX_CONCURRENT_TASKS,
    POOL_STARVED_ATTEMPT,
    POOL_STARVED_SAMPLE_GAP_S,
    POOL_STARVED_SCHEDULED_S,
    POOL_STARVED_ZERO_SAMPLES,
    STALE_THRESHOLD,
    has_live_workflow,
)

# Batch/shard row statuses that mean "this row is finished" (decision A/G).
# Not the task-level TERMINAL_STATUSES — a row in the shards table only ever
# takes pending/running/done/failed.
_ROW_TERMINAL = ("done", "failed")

# Trigger 1's confirmation state: pool task queue -> (consecutive zero-poller
# samples, unix ts of the latest one). Module level because "consecutive"
# spans patrol cycles, and bounded by the number of pool task queues (one per
# source). Reset by a healthy sample, aged out by POOL_STARVED_SAMPLE_GAP_S.
_POOL_ZERO_POLLER_SAMPLES: dict[str, tuple[int, float]] = {}


async def reconcile() -> dict:
    """Compare SQLite downloading tasks vs Temporal running workflows.

    Returns a report of actions taken.
    """
    from ..queue.snapshot import (
        get_tasks_by_status, update_task_progress, get_shards_by_task,
        complete_task, init_db, _conn,
    )
    # start_task_download (the funnel), not start_sharded_download +
    # coordinator_queue at this call site: the queue decision now lives inside
    # temporal_client, so both dispatch modes route by source from one place.
    from .temporal_client import running_workflows, start_task_download

    init_db()
    report = {
        "checked_at": time.time(),
        "downloading_tasks": 0,
        "running_workflows": 0,
        "orphaned": [],
        "redispatched": [],
        "stale": [],
        "reclaimed_shards": [],
        "errors": [],
    }

    try:
        downloading = get_tasks_by_status("downloading")
        report["downloading_tasks"] = len(downloading)
    except Exception as e:
        report["errors"].append(f"Failed to read tasks: {e}")
        return report

    # Get running workflows from Temporal. Queried before the no-tasks exit
    # because the shard reclaim below needs it, and the state it repairs is
    # most likely precisely when nothing is downloading — see reclaim.
    try:
        running_ids = set(await running_workflows())
        report["running_workflows"] = len(running_ids)
    except Exception as e:
        report["errors"].append(f"Failed to query Temporal: {e}")
        return report

    now = time.time()

    try:
        report["reclaimed_shards"] = reclaim_orphaned_shards(running_ids, now)
    except Exception as e:
        report["errors"].append(f"Shard reclaim failed: {e}")
        logger.error(f"Reconciler: shard reclaim failed: {e}")

    if not downloading:
        return report

    reclaimed_ids = {r["shard_id"] for r in report["reclaimed_shards"]}

    for task in downloading:
        task_id = task["id"]
        updated_at = task.get("updated_at") or 0
        stale_seconds = now - updated_at

        has_workflow = has_live_workflow(task_id, running_ids)

        if not has_workflow:
            # A pool task's terminal state is the coordinator workflow's to
            # report, never this loop's to infer. Three things make the shard
            # inference below actively wrong for pool:
            #
            #   1. Pool batch rows and sharded shard rows share the `shards`
            #      table, so "every row is done" is a legitimate *transient*
            #      state mid-run — the window loop has finished the batches it
            #      created and has not created the next window's rows yet.
            #   2. This block sits ABOVE decision C's pool skip, so that
            #      `continue` cannot protect against it.
            #   3. It has no staleness gate at all, unlike every other
            #      auto-decision here.
            #
            # Together those let one unlucky poll declare a pool task `done`,
            # bypassing the coordinator's missing-file verification, its
            # ceiling check, and the WARNING that must accompany a `done` with
            # known missing files. "Reported done with no alert and no
            # queryable record" is the single failure mode the missing-file
            # accounting exists to prevent, and this path reaches it without
            # any file even being missing.
            #
            # Skipping the inference means a pool task whose coordinator is
            # genuinely gone stays `downloading` until a human acts. That is
            # the accepted posture: pool tasks do not self-heal (see decision
            # C), and pool_orphaned/pool_starved are the signals for it.
            is_pool = (task.get("dispatch_mode") or "sharded") == "pool"

            # Before re-dispatching, check if all shards are already done.
            # This handles the case where the parent ShardedDownloadWorkflow
            # died but all child ShardWorkerWorkflows completed successfully.
            shards = [] if is_pool else get_shards_by_task(task_id)
            if shards:
                done_shards = [s for s in shards if s.get("status") == "done"]
                failed_shards = [s for s in shards if s.get("status") == "failed"]
                if len(done_shards) == len(shards):
                    # Every shard succeeded, so whatever error the row carries
                    # belongs to an earlier run (see complete_task).
                    complete_task(task_id, "done", clear_error=True)
                    report.setdefault("auto_completed", []).append(task.get("name", task_id))
                    logger.info(
                        f"Reconciler: auto-completed {task.get('name', task_id)} "
                        f"— all {len(shards)} shards done, parent workflow dead"
                    )
                    continue
                if len(done_shards) + len(failed_shards) == len(shards) and failed_shards:
                    # A shard this cycle's reclaim just failed says nothing
                    # about the download — its host was freed, and the work is
                    # resumable (the BOS filter makes a restart lossless). Let
                    # it fall through to re-dispatch instead of burying a live
                    # task as failed because its worker was restarted.
                    #
                    # Only when EVERY failed shard is one of this cycle's
                    # reclaims, though. `any()` let a single reclaimed shard
                    # shield a task whose other seven failed on real download
                    # errors: the task got re-dispatched, hit the same error,
                    # and the genuine failure never surfaced. A real failure
                    # recurs, so burying the task is the honest outcome — it
                    # stays retryable by hand, and the BOS filter means nothing
                    # already uploaded is lost.
                    reclaimed_failures = [
                        s for s in failed_shards if s.get("id") in reclaimed_ids
                    ]
                    if len(reclaimed_failures) == len(failed_shards):
                        logger.info(
                            f"Reconciler: {task.get('name', task_id)} has "
                            f"{len(reclaimed_failures)} reclaimed shard(s) and no "
                            f"genuine failure — re-dispatching rather than "
                            f"marking failed"
                        )
                    else:
                        complete_task(task_id, "failed")
                        report.setdefault("auto_failed", []).append(task.get("name", task_id))
                        logger.warning(
                            f"Reconciler: marked {task.get('name', task_id)} failed "
                            f"— {len(failed_shards)}/{len(shards)} shards failed "
                            f"({len(reclaimed_failures)} reclaimed), parent dead"
                        )
                        continue

            report["orphaned"].append({
                "task_id": task_id,
                "name": task.get("name", ""),
                "server": task.get("server", ""),
                "stale_seconds": int(stale_seconds),
            })

            # Decision C: a pool coordinator's updated_at staleness is not
            # evidence of trouble — it can legitimately sit in its window
            # loop for hours while progress is written by batch activities,
            # not the task row. Re-dispatching would also start a *second*
            # PoolDownloadWorkflow, which re-runs list -> filter -> chunk and
            # re-POSTs pool/batches/create; T1's ruling on a chunking
            # mismatch is no-delete + error (non-retryable), so a re-dispatch
            # against a changed filelist wedges the task instead of healing
            # it. So: skip the re-dispatch entirely for a pool task, record
            # it separately, and let the pool_starved alert (below) carry it
            # to a human. The sharded branch underneath is untouched — G1
            # requires it stay byte-identical, and it is what
            # Egocentric-100K's self-healing rests on.
            if is_pool:
                # DEAD_THRESHOLD, not zero: auto_dispatch commits status='downloading'
                # before start_workflow, and running_workflows() reads Temporal's
                # eventually-consistent visibility index, so a pool task dispatched
                # seconds ago legitimately has no live workflow here. Same gate the
                # sharded re-dispatch below uses — "orphan" means one thing in both modes.
                if stale_seconds > DEAD_THRESHOLD:
                    report.setdefault("pool_orphaned", []).append({
                        "task_id": task_id,
                        "name": task.get("name", ""),
                        "stale_seconds": int(stale_seconds),
                    })
                continue

            # Only re-dispatch if stale for > DEAD_THRESHOLD
            # (gives time for workflows that just started to appear)
            if stale_seconds > DEAD_THRESHOLD:
                try:
                    # Unified dispatch entry — branches on dispatch_mode.
                    # The legacy DownloadDatasetWorkflow has no BOS resume
                    # filter and must not be reachable here.
                    # Refresh claimed_at so the listing-phase source guard
                    # covers this coordinator too.
                    # snapshot.CLAIM_RESET_PHASE_SQL is deliberately NOT
                    # applied here: decision C's `continue` above means only
                    # sharded tasks reach this UPDATE, and sharded tasks have
                    # no coordinator_phase — the fragment's CASE would write
                    # the existing value straight back. It is still applied at
                    # the claim sites that can see a pool task
                    # (auto_dispatch_pending below, routes/queue.py's resume).
                    mode = task.get("dispatch_mode") or "sharded"
                    conn2 = _conn()
                    # retry_count too, not just claimed_at: a re-dispatch IS a
                    # retry, and the churn guards read that counter
                    # (alerts.py's repeated-failure alert, doctor's zombie
                    # check at retry_count >= 8). Leaving it untouched let a
                    # task be re-dispatched by this path indefinitely with
                    # every churn detector reading zero.
                    conn2.execute(
                        "UPDATE tasks SET claimed_at = ?, "
                        "retry_count = COALESCE(retry_count, 0) + 1 WHERE id = ?",
                        (time.time(), task_id),
                    )
                    conn2.commit()
                    await start_task_download(task)
                    report["redispatched"].append(task.get("name", task_id))
                    logger.info(
                        f"Reconciler: re-dispatched {task.get('name', task_id)} "
                        f"as {mode} (orphaned {stale_seconds:.0f}s)"
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

    # Pool patrol (decision A) — three triggers, one pool_starved alert.
    # Runs every RECONCILE_INTERVAL (5 min), comfortably inside A10's 15-min
    # budget. Best-effort: a patrol failure must not fail the whole
    # reconcile pass, since callers already rely on its other keys.
    try:
        report["pool_starved"] = await inspect_pool_tasks(downloading)
    except Exception as e:
        report["pool_starved"] = []
        report["errors"].append(f"Pool patrol failed: {e}")
        logger.error(f"Pool patrol failed: {e}")

    return report


async def inspect_pool_tasks(downloading: list[dict]) -> list[dict]:
    """Three-trigger pool patrol -> `pool_starved` alerts (decision A).

    `downloading` is the same list `reconcile()` already fetched — this
    never queries SQLite itself beyond per-task batch rows. Every Temporal
    RPC is best-effort: an inspection pass must never be able to stop the
    scheduler loop, so a failed RPC is logged and skipped, not raised.

    Trigger 1 (no pollers) is what A10's drill exercises. It fires on the
    SECOND consecutive zero-poller sample, i.e. ~600s after the fleet's
    pollers go to zero (two 300s cycles) — one zero can just be Temporal's
    recency-based poller view, and A10 also grades false positives. Triggers
    2/3 (SCHEDULED aged out / attempt climbing) catch the slower failure
    shapes a poller-count check alone would miss (see the plan-vs-timers
    analysis in decision A). All three emit the same alert type, keyed per
    task so the existing `_active_alerts` de-dupe in alerts.py collapses
    repeats.
    """
    from ..queue.snapshot import get_shards_by_task
    from ..temporal.workflows import pool_task_queue
    from .temporal_client import (
        POOL_DOWNLOAD_ID_PREFIX,
        _pool_poller_count,
        connected_client,
        pending_activities,
    )

    pool_tasks = [t for t in downloading if (t.get("dispatch_mode") or "sharded") == "pool"]
    if not pool_tasks:
        return []

    now = time.time()
    alerts: list[dict] = []
    alerted_task_ids: set = set()

    # Trigger 1: zero ACTIVITY pollers on a source with a downloading pool
    # task holding >=1 non-terminal batch row. One poller-count RPC per
    # affected source (not per task) — reused via T7's existing helper.
    by_source: dict[str, list[dict]] = {}
    for t in pool_tasks:
        try:
            batches = get_shards_by_task(t["id"])
        except Exception as e:
            logger.error(f"Pool patrol: cannot read batch rows for {t['id']}: {e}")
            continue
        if any(b.get("status") not in _ROW_TERMINAL for b in batches):
            by_source.setdefault(t.get("source") or "hf", []).append(t)

    if by_source:
        try:
            client = await connected_client()
        except Exception as e:
            logger.error(f"Pool patrol: cannot connect to Temporal: {e}")
            client = None

        if client is not None:
            for source, tasks_for_source in by_source.items():
                queue_name = pool_task_queue(source)
                try:
                    pollers = await _pool_poller_count(client, queue_name)
                except Exception as e:
                    # Decision B: an RPC failure is not evidence of anything,
                    # so it stays silent — and it is not a healthy sample
                    # either, so the streak below is left exactly as it is. A
                    # flapping frontend must not be able to suppress the
                    # alert by resetting confirmation on every other cycle.
                    logger.error(
                        f"Pool patrol: poller-count RPC failed for {queue_name}: {e}"
                    )
                    continue
                if pollers > 0:
                    _POOL_ZERO_POLLER_SAMPLES.pop(queue_name, None)
                    continue
                # One zero is not fleet death: Temporal's poller list is a
                # recency view (see POOL_STARVED_ZERO_SAMPLES). Confirm across
                # consecutive patrol cycles before raising CRITICAL.
                prev_zeros, prev_at = _POOL_ZERO_POLLER_SAMPLES.get(queue_name, (0, 0.0))
                if now - prev_at > POOL_STARVED_SAMPLE_GAP_S:
                    prev_zeros = 0  # too long ago to call this consecutive
                zeros = prev_zeros + 1
                _POOL_ZERO_POLLER_SAMPLES[queue_name] = (zeros, now)
                if zeros < POOL_STARVED_ZERO_SAMPLES:
                    logger.warning(
                        f"Pool patrol: {queue_name} reported 0 activity pollers "
                        f"(sample {zeros}/{POOL_STARVED_ZERO_SAMPLES}) — waiting for "
                        f"the next cycle to confirm before alerting"
                    )
                    continue
                for t in tasks_for_source:
                    alerts.append({
                        "severity": "critical",
                        "type": "pool_starved",
                        "task_id": t["id"],
                        "task_name": t.get("name", ""),
                        "source": source,
                        "trigger": "no_pollers",
                        "pollers": pollers,
                        "zero_samples": zeros,
                        "message": (
                            f"Pool task {t.get('name', t['id'])} ({source}): "
                            f"{queue_name} has 0 activity pollers "
                            f"({zeros} consecutive samples)"
                        ),
                    })
                    alerted_task_ids.add(t["id"])

    # Triggers 2/3: pending-activity inspection per pool task, skipping any
    # task trigger 1 already flagged (one alert per task; CRITICAL wins).
    for t in pool_tasks:
        if t["id"] in alerted_task_ids:
            continue
        workflow_id = f"{POOL_DOWNLOAD_ID_PREFIX}{t['id']}"
        try:
            rows = await pending_activities(workflow_id)
        except Exception as e:
            logger.error(f"Pool patrol: pending_activities failed for {t['id']}: {e}")
            continue

        for row in rows:
            if row.get("state") == "SCHEDULED" and row.get("scheduled_at") is not None:
                age = now - row["scheduled_at"]
                if age > POOL_STARVED_SCHEDULED_S:
                    alerts.append({
                        "severity": "warning",
                        "type": "pool_starved",
                        "task_id": t["id"],
                        "task_name": t.get("name", ""),
                        "source": t.get("source") or "hf",
                        "trigger": "scheduled_stuck",
                        "scheduled_age_s": int(age),
                        "message": (
                            f"Pool task {t.get('name', t['id'])}: a batch activity has "
                            f"been SCHEDULED for {int(age)}s"
                        ),
                    })
                    break
            if (row.get("attempt") or 0) >= POOL_STARVED_ATTEMPT:
                alerts.append({
                    "severity": "warning",
                    "type": "pool_starved",
                    "task_id": t["id"],
                    "task_name": t.get("name", ""),
                    "source": t.get("source") or "hf",
                    "trigger": "attempt_climbing",
                    "attempt": row["attempt"],
                    "message": (
                        f"Pool task {t.get('name', t['id'])}: a batch activity is on "
                        f"attempt {row['attempt']}"
                    ),
                })
                break

    return alerts


SHARD_ORPHAN_GRACE = 900  # 15 min quiet AND no child execution = not coming back


def reclaim_orphaned_shards(running_ids, now: float | None = None) -> list[dict]:
    """Fail shard rows whose child workflow is gone, so their host frees up.

    A `running` shard row is how a host is marked taken: `busy_servers` reads
    shard ownership as the primary signal, because a sharded task's own row
    carries `server = NULL`. Nothing wrote that row back if the child died
    without reporting — an OOM kill, a host reboot, a terminate that raced the
    report, a worker restarted mid-shard. The row stayed `running` forever, the
    host read busy forever, and `query_idle_workers` found nothing to dispatch
    to. Sixteen such rows and the fleet reports zero idle workers while every
    worker sits doing nothing.

    `reconcile()`'s task loop cannot reach this: it iterates `downloading`
    tasks, so a row belonging to a task that already went done/failed/revoked
    is never visited — and that is the common case, since terminating a task's
    workflows is exactly what leaves its children unable to report. Hence a
    pass over shard rows rather than tasks, run even when no task is
    downloading (the total-starvation state has nothing downloading by
    definition).

    Two conditions, both required:
      * Temporal has no running `shard-{id}` execution — the authority on
        whether work is alive, not a timestamp
      * the row has been quiet for SHARD_ORPHAN_GRACE — covers the window
        between `create_shards_in_db` and the child appearing in a list scan,
        and any list lag behind a just-started execution

    Both conditions are sharded-shaped, and neither holds for a pool batch,
    so pool rows are skipped entirely:
      * a pool batch runs as an ACTIVITY of the coordinator, not as a
        `shard-{id}` child workflow, so the liveness test is never satisfied
        no matter how healthy the batch is;
      * POOL_BATCH_RETRY backs off to a 30-minute maximum interval, twice
        SHARD_ORPHAN_GRACE, so a batch that is retrying exactly as designed
        goes quiet for longer than this grace as a matter of course.
    Writing such a row to `failed` does not just mislabel it: every row
    terminal with one genuine failure is what reconcile()'s auto-fail reads as
    "this task is dead". A healthy pool task would be buried by its own retry
    policy. Batch ownership and release are the pool path's own business
    (`release_pool_batches`), which is mode-internal and consistent.

    Callers must pass ids from a query that SUCCEEDED. An empty set from a
    failed query would read as "nothing is running" and fail every live shard
    in the fleet; `reconcile()` returns early on query failure for that reason.
    """
    from ..queue.snapshot import get_running_shards, init_db, update_shard_progress

    init_db()
    now = time.time() if now is None else now
    cutoff = now - SHARD_ORPHAN_GRACE
    reclaimed = []

    # include_stopped_tasks=True: this pass REWRITES rows rather than reading
    # them to decide who is busy, and the rows belonging to a terminal task are
    # the ones most likely to be stranded (terminating a task's workflows is
    # what leaves its children unable to report). See get_running_shards.
    for shard in get_running_shards(include_stopped_tasks=True):
        shard_id = shard.get("id") or ""
        if not shard_id or f"shard-{shard_id}" in running_ids:
            continue
        # Same `or "sharded"` fallback the rest of this module uses: no mode on
        # the row means it predates pool, so treat it as sharded. A row whose
        # task is missing entirely also lands here (LEFT JOIN gives None) and
        # stays reclaimable — that is the stranded case this pass exists for.
        if (shard.get("task_dispatch_mode") or "sharded") == "pool":
            continue
        updated_at = shard.get("updated_at") or 0
        if updated_at > cutoff:
            continue

        quiet = int(now - updated_at)
        update_shard_progress(
            shard_id,
            status="failed",
            speed_mbps=0,
            error=(f"orphaned: no running shard-{shard_id} workflow and no "
                   f"progress for {quiet}s — reclaimed by reconciler"),
        )
        reclaimed.append({
            "shard_id": shard_id,
            "task_id": shard.get("task_id", ""),
            "server": shard.get("server", ""),
            "quiet_seconds": quiet,
        })
        logger.warning(
            f"Reconciler: reclaimed orphaned shard {shard_id} on "
            f"{shard.get('server') or '?'} (quiet {quiet}s, no child workflow) "
            f"— its host was counted busy"
        )

    return reclaimed


async def auto_dispatch_pending() -> dict:
    """Auto-dispatch pending tasks to idle workers.

    Idle = worker heartbeat alive (last_seen < 180s) but no running workflow.
    Dispatch one pending task per idle worker, priority order.

    Uses optimistic locking: UPDATE tasks SET status='downloading' WHERE status='pending' AND id=?
    to prevent TOCTOU race with concurrent dispatches.
    """
    from ..queue.snapshot import (
        CLAIM_RESET_PHASE_SQL,
        get_tasks_by_status, get_workers, get_running_shards, _conn, init_db,
    )
    from .temporal_client import start_task_download

    init_db()
    report = {"dispatched": [], "errors": []}

    try:
        # 1. Find alive workers (deduped by freshest heartbeat row)
        from .fleet import (
            alive_workers as compute_alive, busy_servers as compute_busy,
            coordinator_queue, worker_serves,
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

        # 3b. Pool admission cap (plan change #3): count *downloading* pool
        # tasks per source. sources_in_listing (below) answers "is a
        # coordinator still partitioning"; this answers the separate
        # question "does this source already have as many pool tasks
        # running as POOL_MAX_CONCURRENT_TASKS allows" — a pool source keeps
        # admitting once its first task leaves listing, up to this cap.
        pool_downloading_counts: dict = {}
        for t in downloading:
            if (t.get("dispatch_mode") or "sharded") == "pool":
                src = t.get("source", "hf")
                pool_downloading_counts[src] = pool_downloading_counts.get(src, 0) + 1

        # 4. Coordinator-race guard: a downloading task with no shard rows means
        #    its sharded coordinator is still listing/filtering — its workers
        #    look idle but will be claimed shortly. Don't dispatch that source
        #    until shards exist. Keyed on claimed_at (written at claim time and
        #    refreshed only by reconcile()'s re-dispatch below, never by
        #    progress reports) so a legacy non-sharded task
        #    can't pin its source forever; a dead coordinator stops blocking
        #    after 15 min (reconcile() cleans it up).
        #
        #    The sharded criterion (NOT EXISTS shards) is scoped to
        #    non-pool tasks and otherwise untouched — G1 requires it stay
        #    byte-identical, and since no task's dispatch_mode was ever
        #    'pool' before this task existed, that scoping changes nothing
        #    for any row producible before now. A pool coordinator gets its
        #    own criterion instead of sharing that one: it registers batch
        #    rows into the same `shards` table once dispatch starts
        #    (create_pool_batches), so "no rows yet" would either be
        #    redundant with, or in a future world diverge from,
        #    coordinator_phase — set to 'listing' at claim time (below) and
        #    'dispatching' once create_pool_batches lands its rows
        #    (routes/queue.py). Using coordinator_phase alone for pool tasks
        #    keeps the two criteria independently meaningful and testable.
        #    NULL counts as listing: a pool task that has never reached
        #    create_pool_batches has no phase yet, and reading NULL as "not
        #    listing" left its source completely unguarded during exactly the
        #    window the guard exists for. Two claim routes put a task into
        #    `downloading` and reset a pool task's phase to 'listing' via
        #    snapshot.CLAIM_RESET_PHASE_SQL: this function, and /queue/preempt.
        #    reconcile()'s orphan re-dispatch is NOT a third one — decision C's
        #    `continue` above means a pool task never reaches that UPDATE at
        #    all, so 'dispatching' here always belongs to the coordinator
        #    currently running.
        #    Exception, pre-existing and unchanged by this task: the doctor's
        #    orphan repair (routes/doctor.py) re-dispatches without touching
        #    claimed_at at all, so its coordinator is outside this guard for
        #    both modes.
        conn = _conn()
        listing_cutoff = time.time() - 900
        rows = conn.execute(
            "SELECT DISTINCT t.source FROM tasks t "
            "WHERE t.status = 'downloading' "
            "AND t.claimed_at > ? "
            "AND ("
            "  (COALESCE(t.dispatch_mode, 'sharded') != 'pool'"
            "   AND NOT EXISTS (SELECT 1 FROM shards s WHERE s.task_id = t.id))"
            "  OR (t.dispatch_mode = 'pool'"
            "      AND (t.coordinator_phase = 'listing'"
            "           OR t.coordinator_phase IS NULL))"
            ")",
            (listing_cutoff,),
        ).fetchall()
        sources_in_listing = {r[0] for r in rows}

        # 5. Dispatch: unified entry point (start_task_download) for all
        #    sources and both dispatch modes.
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
                t_mode = t.get("dispatch_mode") or "sharded"
                if t_mode == "pool" and pool_downloading_counts.get(source, 0) >= POOL_MAX_CONCURRENT_TASKS:
                    continue  # this source already has its full share of pool tasks
                if not worker_serves(server_key, source):
                    continue  # ModelScope → bj*, everything else → w*
                task = pending.pop(i)
                break

            if task is None:
                continue

            # Optimistic lock: claim status only. server stays NULL — the
            # coordinator assigns servers per shard, and a task-level server
            # would count this worker as busy in its own idle query.
            #
            # A pool task also gets coordinator_phase='listing' in the same
            # claim — the write side of decision A. The reset is shared with
            # the other two claim sites (see snapshot.CLAIM_RESET_PHASE_SQL);
            # for a sharded row the CASE writes the existing value back, so
            # the sharded claim's effect is unchanged.
            now_ts = time.time()
            task_mode = task.get("dispatch_mode") or "sharded"
            cursor = conn.execute(
                "UPDATE tasks SET status = 'downloading', server = NULL, "
                f"updated_at = ?, claimed_at = ?, {CLAIM_RESET_PHASE_SQL} "
                "WHERE id = ? AND status = 'pending'",
                (now_ts, now_ts, task["id"]),
            )
            conn.commit()

            if cursor.rowcount == 0:
                continue  # someone else claimed it

            # Guard the source immediately — even an "already started" race
            # below must not let a second coordinator spawn this cycle
            sources_in_listing.add(task.get("source", "hf"))
            if task_mode == "pool":
                pool_downloading_counts[task.get("source", "hf")] = (
                    pool_downloading_counts.get(task.get("source", "hf"), 0) + 1
                )

            # The source routing decided above has to survive into the
            # coordinator: started on the shared HK queue, a ModelScope task
            # lists its files on an HF node (see fleet.coordinator_queue).
            queue = coordinator_queue(task.get("source", "hf"))

            try:
                await start_task_download(task)
                report["dispatched"].append({
                    "task": task.get("name", task["id"]),
                    "worker": task_mode,
                    "mode": task_mode,
                    # Informational only — the authority is the funnel, which
                    # calls the same coordinator_queue(source). Reported so an
                    # operator can see the source routing took effect without
                    # reading Temporal.
                    "queue": queue,
                })
                logger.info(f"Auto-dispatch: {task.get('name')} → {task_mode} coordinator")
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


# ── Staging GC (decision G) ─────────────────────────────────────────

# Same filter routes/servers.py's cleanup endpoint already applies before
# shlex.quote-ing a task name into a shell command — kept identical rather
# than re-derived, since it is the last line of defence between a task name
# and `rm -rf`.
_SAFE_NAME_RE = re.compile(r'^[A-Za-z0-9_.\-/]+$')


def select_staging_gc(dirs_by_server: dict, tasks: list[dict]) -> dict:
    """Decide what staging directories are safe to remove. Pure — no ssh, no
    filesystem access — so this is the whole GC's test surface (decision G).

    `dirs_by_server` is `{server_key: [dir_name, ...]}`, one entry per
    top-level name actually found under STAGING_PATH on that server (the
    impure fan-out in `staging_gc()` below is what produces this via `ls`).
    `tasks` is every task row (`get_all_tasks()`), matched by `name` — task
    `name` is not unique in this schema, so a directory is only removable
    when *every* task row sharing that name is in `GC_REMOVABLE_STATUSES`
    (rule 3).

    Returns `{"remove": [...], "keep": [...], "unknown": [...], "skipped": [...]}`.
    Each entry carries `server`/`name` plus, for `remove`, the
    removal-authorising `status`(es) (rule 5 wants that in the removal log).
    """
    by_name: dict[str, list[dict]] = {}
    for t in tasks:
        name = t.get("name")
        if name:
            by_name.setdefault(name, []).append(t)

    remove, keep, unknown, skipped = [], [], [], []
    for server, names in dirs_by_server.items():
        for name in names:
            if not _SAFE_NAME_RE.match(name):
                # A name failing the filter is skipped, not quoted-and-hoped
                # (rule 4) — whatever it is, it never reaches `rm -rf`.
                skipped.append({"server": server, "name": name, "reason": "metacharacters"})
                continue

            rows = by_name.get(name)
            if not rows:
                # Reported, never removed (rule 2) — may be a legacy dir or
                # a task whose row was deleted while data was still in flight.
                unknown.append({"server": server, "name": name})
                continue

            non_removable = [r for r in rows
                             if r.get("status") not in GC_REMOVABLE_STATUSES]
            if non_removable:
                # Any row with this name that is not in the *removable* set
                # blocks it (rule 3) — names collide, so this is the full row
                # set, not the first hit. Membership in GC_REMOVABLE_STATUSES,
                # never absence from TERMINAL_STATUSES: `paused`/`preempted`
                # are terminal-for-scheduling but resumable-for-data, and
                # deleting their staging destroys the partial files and
                # .progress.json markers a resume needs.
                keep.append({
                    "server": server, "name": name,
                    "reason": f"not removable, status(es): "
                              f"{sorted({r.get('status') for r in non_removable})}",
                })
                continue

            remove.append({
                "server": server, "name": name,
                "status": sorted({r.get("status") for r in rows}),
            })

    return {"remove": remove, "keep": keep, "unknown": unknown, "skipped": skipped}


def staging_gc(dry_run: bool = False) -> dict:
    """Periodic staging GC (decision G) — local disk only, and only for tasks
    in `fleet.GC_REMOVABLE_STATUSES` (done/failed/revoked/skipped; a paused or
    preempted task's staging is what its resume rests on).

    Lists every enabled remote server's STAGING_PATH over ssh, runs the pure
    `select_staging_gc` above to decide what is safe, and — unless
    `dry_run` — removes exactly those directories with `rm -rf`, keeping the
    same metacharacter filter + `shlex.quote` routes/servers.py's manual
    cleanup endpoint already uses (that endpoint can't reach pool/sharded
    staging at all: it matches on `task.server == key`, which is NULL for
    every modern task — this sweep discovers directories independently via
    `ls` instead of trusting that column).

    Never touches BOS — this only ever runs `ls`/`rm -rf` against a worker's
    local scratch path. Every per-host ssh failure is swallowed: S1->BJ ssh
    is known-flaky and one unreachable host must not abort the sweep (the
    scheduler additionally wraps the whole call in asyncio.wait_for, so a
    hang here cannot stop the loop either).
    """
    import shlex

    from ..core.servers import load_servers
    from ..core.ssh import ssh_exec, ssh_parallel
    from ..queue.snapshot import get_all_tasks, init_db
    from ..worker.disk import STAGING_PATH

    init_db()
    tasks = get_all_tasks()
    servers = [s for s in load_servers().values() if s.enabled and not s.local]

    errors: list[str] = []
    if not servers:
        return {"dry_run": dry_run, "candidates": [], "removed": [],
                 "kept": [], "unknown": [], "skipped": [], "errors": errors}

    listing = ssh_parallel(servers, f"ls -1 {STAGING_PATH} 2>/dev/null", timeout=15)

    dirs_by_server: dict = {}
    for server in servers:
        out, ok = listing.get(server.key, ("", False))
        if not ok:
            errors.append(f"{server.key}: could not list {STAGING_PATH}")
            continue
        dirs_by_server[server.key] = [ln.strip() for ln in out.splitlines() if ln.strip()]

    plan = select_staging_gc(dirs_by_server, tasks)

    removed = []
    if not dry_run:
        by_key = {s.key: s for s in servers}
        for item in plan["remove"]:
            server = by_key.get(item["server"])
            if server is None:
                continue
            target = shlex.quote(f"{STAGING_PATH}/{item['name']}")
            out, ok = ssh_exec(server.host, server.user, f"rm -rf {target}", timeout=30)
            if ok:
                removed.append(item)
                logger.info(
                    f"Staging GC: removed {item['name']} on {server.key} "
                    f"(task status={item['status']})"
                )
            else:
                errors.append(f"{server.key}: rm failed for {item['name']}: {out}")

    return {
        "dry_run": dry_run,
        "candidates": plan["remove"],
        "removed": removed,
        "kept": plan["keep"],
        "unknown": plan["unknown"],
        "skipped": plan["skipped"],
        "errors": errors,
    }
