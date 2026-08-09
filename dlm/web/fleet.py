"""Fleet state primitives — one definition of "alive", "busy", and "routable".

These questions are asked from four places (doctor, alerts, reconciler,
the idle-workers route) and every copy that drifted produced a false alarm:
a stale `wN@sidecar` row reported a healthy worker offline, and task-level
`server` alone reported every shard-running worker as idle. Import from here
rather than re-deriving.
"""

from __future__ import annotations

import os
import time

WORKER_TIMEOUT = 180  # seconds without a heartbeat before a worker is offline
MIN_SHARD_DISK_GB = 70  # a worker below this is not offered new shards
STALE_THRESHOLD = 600  # 10 min without a task update = suspicious
DEAD_THRESHOLD = 1800  # 30 min without a task update = definitely dead

# Pool dispatch (work-stealing) policy — kept here alongside the other fleet
# knobs (MIN_SHARD_DISK_GB etc.) rather than scattered per-module, since a
# gate on "how many pool tasks may run at once" is fleet state exactly like
# "how many workers are idle".
POOL_MAX_CONCURRENT_TASKS = int(os.environ.get("DLM_POOL_MAX_CONCURRENT_TASKS", "4"))

# The only definition of the mode vocabulary. It was previously restated as a
# local literal in four places (queue.add, queue.reshard, tasks.add and
# start_task_download's if/elif chain); a third mode would have needed four
# edits, and the four copies had already drifted on WHEN they validate.
VALID_DISPATCH_MODES = frozenset({"sharded", "pool"})

# Pool is the global default as of 2026-08-08. Stated in code rather than only
# as a systemd environment variable: "the global default is pool" is a claim
# about the system, and an env var is invisible in git and lost the next time
# the unit is reinstalled. Rollback does not need a redeploy — setting
# DLM_DEFAULT_DISPATCH_MODE=sharded and restarting dlm-web still wins.
#
# This governs NEW rows only. SQLite's ALTER TABLE ... ADD COLUMN ... DEFAULT
# materialises the literal on every pre-existing row, so the migration wrote
# 'sharded' into the whole backlog and no Python-side default can reach those:
# pending/paused/failed rows need scripts/backfill_dispatch_mode.py.
#
# The `or "sharded"` fallbacks in temporal_client and reconciler are NOT this
# default and must not be pointed at it — they mean "this row carries no mode,
# so it predates the column, so run it the old way", which stays true whatever
# the default for new work becomes.

DEFAULT_DISPATCH_MODE = os.environ.get("DLM_DEFAULT_DISPATCH_MODE", "pool")
if DEFAULT_DISPATCH_MODE not in VALID_DISPATCH_MODES:
    # Fail at import, not per-request. A typo'd DLM_DEFAULT_DISPATCH_MODE (e.g.
    # "Pool") would otherwise be persisted unvalidated by every /api/tasks add
    # — the endpoint validates only a *client-supplied* mode — and each such
    # task would then be claimed, fail start_task_download with a ValueError,
    # be reverted to pending, and retry every 30s forever with nothing but a
    # reconciler error line. A refused import surfaces in `systemctl status
    # dlm-web` immediately and is a one-character fix.
    raise ValueError(
        f"DLM_DEFAULT_DISPATCH_MODE={DEFAULT_DISPATCH_MODE!r} is not one of "
        f"{sorted(VALID_DISPATCH_MODES)}"
    )
POOL_MAX_BATCHES = 1500  # a task chunking past this many batches needs splitting, not a bigger pool

# T9 pool patrol thresholds (decision A) — the single definition the
# reconciler's patrol reads; doctor.py surfaces the patrol's output by
# passing through reconciler_report rather than re-deriving it, so there is
# no second literal anywhere to drift.
POOL_STARVED_SCHEDULED_S = 900  # a pending activity SCHEDULED longer than this (plan's 15 min)
POOL_STARVED_ATTEMPT = 3  # T10's pre-restart check treats attempt>=3 as "a human must look"

# Trigger 1 (zero activity pollers) needs confirmation before it screams.
# Temporal's poller list is a recency-based view: a frontend restart, a
# matching-service failover, or a fleet that has just reconnected can return
# an empty list while every worker is healthy — the same reason
# temporal_client.start_pool_download wraps its own poller check in
# try/except. Two consecutive samples cost one 300s patrol cycle to confirm,
# which still lands inside A10's 15-minute detection bound.
POOL_STARVED_ZERO_SAMPLES = 2
# ...and "consecutive" means consecutive patrol cycles, not any two samples
# ever taken: a zero recorded before an idle stretch says nothing about the
# fleet now. Three patrol intervals of slack, so a slow or skipped cycle does
# not discard a real confirmation.
POOL_STARVED_SAMPLE_GAP_S = 900

# ...and even a confirmed zero is not evidence on its own. A pool worker runs
# ONE batch at a time (__main__.py pins max_concurrent_activities=1 on the pool
# queue), and a Temporal worker at its concurrency limit STOPS POLLING — so a
# fleet with every worker busy on a batch reports exactly the same zero
# pollers as a fleet that never registered the queue at all. Measured
# 2026-08-09: HK 7/7 busy -> pool-hf = 0 pollers, BJ 9/9 idle -> pool-ms = 9.
# The tie-breaker is a worker-originated batch report: if some worker has
# written to a running pool batch row of this source within this window, the
# queue demonstrably has a live consumer. Two batch heartbeats
# (POOL_BATCH_HEARTBEAT = 10 min) of slack, so one missed report is not
# mistaken for a dead fleet — and a fleet that really dies goes stale here
# within 20 minutes and the alert fires as designed.
POOL_LIVE_BATCH_WINDOW_S = int(os.environ.get("DLM_POOL_LIVE_BATCH_WINDOW_S", "1200"))

# Window weights per priority band. The coordinator's per-wake window is this
# task's share of P — P being alive workers serving the source — so these decide
# how a busy pool is split between concurrent pool tasks. Priority 0-2 ("P0",
# the queue-jump band) gets 1.5x the share of everything else.
POOL_WEIGHT_P0 = 1.5
POOL_WEIGHT_DEFAULT = 1.0
POOL_P0_MAX_PRIORITY = 2  # priority <= this is the P0 band


def pool_task_weight(priority: int) -> float:
    """Window weight for one pool task's priority."""
    return POOL_WEIGHT_P0 if (priority or 0) <= POOL_P0_MAX_PRIORITY else POOL_WEIGHT_DEFAULT


# A coordinator that has not yet recorded a single batch is still on the first
# pass of its window loop, where `window` is hardcoded to 1
# (workflows.py:_run_window_loop) and is only replaced by the allocator's
# number after the first `record_batches_and_window` returns. Slots handed to
# such a task beyond the one it holds cannot be dispatched by anyone until its
# first batch finishes, however long that takes.
POOL_RAMP_SLOTS = 1


def pool_task_slot_cap(batch_rows: list[dict]) -> int:
    """How many worker slots this pool task can actually put to work.

    The allocator splits P by weight alone, which silently assumes every task
    can absorb whatever share it is given. Two ways that is false, both
    observed in production on 2026-08-09 with 3 of 7 HK workers idle:

    1. **Batches exhausted.** molmobot-data wound down to `pending=0` with one
       batch still running, yet held its full 3-slot share until its status
       flipped to `done` and it left the `active` list. RealOmin had 725
       pending batches and could not have those 2 slots.
    2. **Still ramping.** robocasa365 was allocated 4 slots (its priority
       band gives it 1.5x weight against RealOmin's 1.0) while its own
       coordinator window was still 1, because no batch of its had reported
       yet — and its first batch was the XET-slow one, so those 3 slots stayed
       dead for 17 hours and counting.

    Both reduce to the same thing: an allocation is only worth something if the
    task can dispatch into it, and a ramping task's ceiling is 1 regardless of
    how many batches wait behind it.

    `failed` rows count toward the ceiling. `run()` re-enters the window loop a
    second time over `outcome["failed"]` (workflows.py, step 6), and by then
    every row is terminal: nothing resets a failed row to `pending`, and
    `record_batches_and_window` writes the terminal row *before* it asks for the
    window, so a `running + pending` ceiling would read 0 through the entire
    retry round and floor its window to 1. That would serialize the retry pass —
    N failed batches one after another at up to POOL_BATCH_START_TO_CLOSE each,
    with the rest of the fleet idle — which is the very failure this function
    exists to prevent, moved to a later phase and made worse than before the
    cap existed.

    That makes the ceiling loose in one state. A task winding down round 1 (one
    batch running, no pending, six already failed) presents exactly the rows a
    task mid-retry-round does (one batch running, six not yet re-dispatched).

    The discriminator IS available: step 6 writes `f"retrying {n} failed
    batches"` into `tasks.phase` immediately before it re-enters the loop
    (workflows.py), round 1 writes `f"pool: {n} batches"`, nothing else touches
    `phase` for a pool task mid-run, and `get_tasks_by_status` is `SELECT *` so
    `active` already carries it at no extra query. A phase-matched cap would be
    tight in both states. It is deliberately NOT done here, because its failure
    mode is silent and severe: edit that f-string, or add any task-level
    progress ping for pool mode, and the cap silently reverts to `running +
    pending` — the ratchet below, at 42%, with nothing in the logs. Doing it
    safely means hoisting the literal into a shared constant imported by both
    workflows.py and this module, plus a test pinning the exact string the
    workflow emits (and an `endswith` guard, since `retry_task` writes a bare
    `phase="retrying"`). That is a larger change than the one this fix is, and
    `phase` is documented elsewhere in this codebase as ephemeral — true for
    sharded, incidentally false for pool, enforced nowhere. Tracked as a
    follow-up; until then the ambiguity is resolved in favour of over-counting:

    - Over-count (this choice): for any task that has accumulated a failed
      batch, the molmobot fix above is disabled for the whole tail of its run —
      every wake where `running + pending` is below its weighted share. The
      over-count is bounded by that share, not by the failed count, and 200
      done + 6 failed + 1 running measures 4 of 7 slots, which is exactly the
      pre-#89 number. A missed improvement, not a new harm: `running + pending
      + failed` counts all of a task's non-done rows, so it can never allocate
      below what the task can dispatch, and in every state it is either better
      than the previous behaviour or identical to it.
    - Under-count (`(running + pending) or failed`, which is tight for
      wind-down): the loop waits FIRST_COMPLETED, so after each record
      `running` equals the number still in flight and the truthy `or` masks the
      failed queue. Any formula returning `<= running` is a fixed point — the
      window can only grow when the cap exceeds the in-flight count — so it
      ratchets 7,6,5,4,3,2,1 and recovers only when the last batch lands.
      Simulated over 100 retried batches at P=7: mean window 3.94 vs 6.73, a
      sustained 42% loss for the whole retry pass. `running + pending +
      min(failed, 1)` is worse still: a permanent window of 1.

    A loose ceiling only ever degrades toward the weight-only behaviour that
    shipped before this function. A tight one that reads 0, or that ratchets
    down, breaks the fleet.

    A task with no batch rows at all is still listing/chunking and caps at 0 —
    the allocator's floor of 1 then gives it a nominal slot it does not use,
    which is the pre-existing over-subscription the floor already documents.

    The ramp heuristic reads terminal rows as "past the ramp", which would be
    wrong for a coordinator that restarts at window 1 in front of rows from an
    earlier generation. No such path exists: resume, retry, reshard, the
    reconciler's re-dispatch and doctor's fix all call `delete_shards_by_task`
    first, and a pool coordinator carries no workflow retry policy, so it never
    silently restarts itself either. The `failed` term cannot perturb the ramp
    branch either: any row that adds to `failed` also makes `reported` true, so
    the branch is only reachable when `failed == 0`.
    """
    running = sum(1 for r in batch_rows if r.get("status") == "running")
    pending = sum(1 for r in batch_rows if r.get("status") == "pending")
    failed = sum(1 for r in batch_rows if r.get("status") == "failed")
    dispatchable = running + pending + failed
    reported = any(r.get("status") in ("done", "failed") for r in batch_rows)
    if not reported:
        return min(dispatchable, max(POOL_RAMP_SLOTS, running))
    return dispatchable


def pool_window_allocation(active: list[dict], p: int,
                           caps: dict[str, int] | None = None) -> dict[str, int]:
    """Split P worker slots across the concurrent pool tasks. {task_id: window}

    Largest remainder (Hamilton), not a per-task floor. Flooring each task's
    share independently discards the residual, and the residual is a whole
    worker: on 2026-08-09 P=7 was split 1.5/1.0 between two live tasks, which
    floor()ed to 4 + 2 and left one HK worker idle for the entire run —
    roughly 10 hours of wall clock across a 19.7 TiB backlog, because
    per-worker throughput on both tasks was identical (~44 GiB/h), so
    throughput scales linearly with window and an unassigned slot is simply
    lost. Handing the residual out by largest fractional part makes
    `sum(alloc) == P` exactly, so the fleet is never deliberately idle.

    Ties go to whoever came first. `sorted` is stable and the caller passes
    `active` in the DB's own dispatch order (priority ASC, created_at ASC), so
    two equally-weighted tasks competing for one leftover slot resolve to the
    older task — the same first-come rule the dispatch queue already uses,
    rather than an arbitrary dict or id ordering that would look random to an
    operator watching which task got the spare worker.

    The floor of 1 is kept: a task whose fair share rounds to zero still gets
    one slot and makes slow progress instead of stalling until its neighbours
    finish. The floor is the only case where the total can exceed P, and there
    are two of them: more admitted tasks than workers (bounded by
    POOL_MAX_CONCURRENT_TASKS), and a task capped below the floor. Both are
    deliberate; over-subscription there means a batch queues on a busy worker,
    while the alternative is a task that never starts.

    `caps` (from pool_task_slot_cap, keyed by task id; a missing or None entry
    means uncapped) bounds each task by what it can actually dispatch into.
    With caps the invariant generalises from `sum(alloc) == P` to
    `sum(alloc) == min(P, sum(caps))`, plus one slot for each task whose cap
    falls below the floor: slots no task can use are left unassigned rather than
    handed to a task that would sit on them, which is the whole point — a
    reserved-but-undispatchable slot is strictly worse than an unassigned one,
    because the reservation also hides the idleness. A task capped at 0 is the
    floor's exception and is harmless: cap 0 means it has no dispatchable batch
    at all, so the window loop exits without using the nominal slot, and because
    it consumed nothing during distribution the other tasks still receive all
    of P.
    Distribution is iterative, not one pass: when a task clamps to its cap, its
    surplus is re-split by weight among the tasks that still have room, and
    that repeats until either the slots run out or every task is capped.
    """
    if not active:
        return {}

    weights = [(t.get("id"), pool_task_weight(t.get("priority") or 0)) for t in active]
    w_sum = sum(w for _, w in weights)
    if p <= 0 or w_sum <= 0:
        # No capacity to divide, or no weight to divide it by. Every task falls
        # to the floor of 1 rather than to 0, which would deadlock the
        # coordinator loop: it dispatches nothing and then waits for a
        # completion that can never arrive.
        return {tid: 1 for tid, _ in weights}

    caps = caps or {}
    # Normalized once, up front: this is a public fleet primitive, and a
    # negative cap would make `slots = p - sum(alloc)` exceed p (handing out
    # more than P on the next round) while a float would raise from the
    # remainder slice. Neither is reachable from pool_task_slot_cap, and
    # neither should depend on that staying true.
    caps = {tid: max(0, int(c)) for tid, c in caps.items() if c is not None}
    weight_of = dict(weights)
    alloc = {tid: 0 for tid, _ in weights}
    # Order preserved from `active` (the DB's priority ASC, created_at ASC) so
    # the stable sort below keeps resolving ties first-come, round after round.
    open_ids = [tid for tid, _ in weights]
    slots = p

    while slots > 0 and open_ids:
        round_weight = sum(weight_of[tid] for tid in open_ids)
        if round_weight <= 0:
            break

        exact = [(tid, slots * weight_of[tid] / round_weight) for tid in open_ids]
        share = {tid: int(x) for tid, x in exact}
        leftover = slots - sum(share.values())
        by_remainder = sorted(exact, key=lambda pair: -(pair[1] - int(pair[1])))
        for tid, _ in by_remainder[:max(0, leftover)]:
            share[tid] += 1

        capped = []
        for tid in open_ids:
            want = alloc[tid] + share[tid]
            cap = caps.get(tid)
            if cap is not None and want >= cap:
                alloc[tid] = cap
                capped.append(tid)
            else:
                alloc[tid] = want

        slots = p - sum(alloc.values())
        if not capped:
            break               # nothing clamped, so this round placed every slot
        open_ids = [tid for tid in open_ids if tid not in capped]

    return {tid: max(1, n) for tid, n in alloc.items()}


def pool_task_holds_no_work(task: dict, batch_rows: list[dict]) -> bool:
    """Decision E's staleness exemption: a POOL task that is admitted but
    waiting behind the window — zero `running` batch rows and >=1 `pending`
    one. A task shaped like this writes nothing to its task row by design,
    so every "no progress in N seconds" rule sees it as stalled while it is
    in fact healthy and queued. `pool_starved` (reconcile()'s patrol) is the
    alert for a genuinely dead one.

    Deliberately narrow. Not exempt: a task holding a `running` batch row
    (a real stall), a task with no batch rows at all (a coordinator that
    never registered any), and any sharded task (the mode gate here is what
    keeps the sharded surfaces byte-identical, G1).

    Lives here, in one place, because two surfaces apply it — the alert
    engine's `task_stuck` and /api/doctor's `stuck_tasks`. A task exempt on
    one and stuck on the other tells an operator two different things about
    the same row, and /api/doctor's `healthy` flag is what T10's deploy gate
    reads. Pure on purpose: the caller supplies the rows, so the doctor
    route can fetch them inside its existing executor hop instead of
    touching SQLite on the event loop.
    """
    if (task.get("dispatch_mode") or "sharded") != "pool":
        return False
    if not batch_rows:
        return False
    running = any(r.get("status") == "running" for r in batch_rows)
    pending = any(r.get("status") == "pending" for r in batch_rows)
    return (not running) and pending


# Terminal/operator states. A progress report must never move a task out of
# one of these — a dying workflow's late report used to resurrect tasks an
# operator had explicitly stopped (2026-07-31 incident).
#
# NOT the set whose staging may be deleted: `paused` and `preempted` are
# stopped but *resumable* (pipeline.py preserves staging for exactly that),
# so use GC_REMOVABLE_STATUSES below for any destructive decision. The two
# sets answer different questions and conflating them once already produced
# a data-loss defect.
TERMINAL_STATUSES = ("paused", "preempted", "revoked", "skipped", "failed", "done")

# The only states whose local staging directory may be removed. Deliberately
# a strict subset of TERMINAL_STATUSES — "this task is stopped" and "this
# task's data is expendable" are different questions, and `paused` /
# `preempted` are this project's resumable states: their
# partially-downloaded files and md5-guarded .progress.json batch markers are
# what a resume rests on. Mirrors CLAUDE.md's hard constraint, "staging
# cleanup only for done/skipped/failed tasks". Never gate a removal on
# absence from TERMINAL_STATUSES.
GC_REMOVABLE_STATUSES = ("done", "failed", "revoked", "skipped")

# The states in which a task's coordinator actually reached a verdict, so its
# missing-file archive describes a settled outcome rather than work in flight.
# A third set rather than a reuse of either above, because it answers a third
# question: `paused` and `preempted` are terminal for scheduling and resumable
# in fact — their archived rows are files the NEXT round will very likely
# fetch, and alerting on them would cry loss over a task that is merely
# stopped. `revoked`/`skipped` never finalize either (nobody asked for those
# files any more). Only `done` and `failed` passed through
# PoolDownloadWorkflow._finalize and have a ceiling recorded to judge by.
FINALIZED_STATUSES = ("done", "failed")


def has_live_workflow(task_id: str, running_ids) -> bool:
    """Whether any running workflow belongs to this task.

    A task's work can live under any of the historical ID schemes, and a
    check that misses one concludes the task is orphaned and re-dispatches
    it — a second coordinator downloading what is already downloading. The
    doctor's fix path had drifted from its own report path in exactly this
    way (it omitted the legacy `{task_id}-part` children).

    ID construction is read from `dlm.web.temporal_client`'s registry
    (`WORKFLOW_ID_PREFIXES`), not re-inlined here: this function, plus
    `cancel_workflow`/`terminate_workflow_and_wait`, are the three sites
    whose independently hand-copied prefixes used to drift. Iterating
    `PARENT_WORKFLOW_TYPES` (rather than three named types) means a future
    task-level workflow type is recognized as soon as it is added to that
    registry — no fourth hardcode to remember here.
    """
    from .temporal_client import (
        LEGACY_DOWNLOAD_ID_PREFIX,
        PARENT_WORKFLOW_TYPES,
        SHARD_WORKER_ID_PREFIX,
        WORKFLOW_ID_PREFIXES,
    )

    legacy = f"{LEGACY_DOWNLOAD_ID_PREFIX}{task_id}"
    return (
        any(f"{WORKFLOW_ID_PREFIXES[t]}{task_id}" in running_ids for t in PARENT_WORKFLOW_TYPES)
        or any(wid.startswith(f"{SHARD_WORKER_ID_PREFIX}s-{task_id}-") for wid in running_ids)
        or any(wid.startswith(f"{task_id}-part") for wid in running_ids)
        or any(wid.startswith(f"{legacy}-") for wid in running_ids)
    )


def dedupe_workers(workers: list[dict]) -> list[dict]:
    """One row per server_key — the freshest heartbeat wins.

    A worker reports under several hostnames (`wN@temporal`, `wN@sidecar`).
    Any check that iterates raw rows sees the dead auxiliary row and calls a
    live worker offline.
    """
    freshest: dict[str, dict] = {}
    for w in workers:
        key = w.get("server_key") or ""
        if not key:
            continue
        if key not in freshest or (w.get("last_seen") or 0) > (freshest[key].get("last_seen") or 0):
            freshest[key] = w
    return list(freshest.values())


def merge_workers(workers: list[dict], now: float | None = None) -> list[dict]:
    """One row per server_key, with fields merged across its hostnames.

    `dedupe_workers` keeps the freshest row whole, which answers "is it
    alive" and gets "what is it doing" wrong: the hostnames carry different
    columns. `wN@temporal` sends liveness only and `wN@sidecar` sends the
    metrics, and the temporal row is almost always the fresher of the two —
    so freshest-wins silently drops every sidecar metric and a busy worker
    reads as having no sidecar at all.

    A hostname that has itself gone quiet stops contributing: otherwise a
    sidecar that died hours ago would keep a worker looking healthy on the
    strength of its last reading.
    """
    now = time.time() if now is None else now
    by_key: dict[str, list[dict]] = {}
    for w in workers:
        key = w.get("server_key") or ""
        if key:
            by_key.setdefault(key, []).append(w)

    merged = []
    for key, rows in by_key.items():
        rows.sort(key=lambda x: x.get("last_seen") or 0)
        newest = rows[-1].get("last_seen") or 0
        row: dict = {"server_key": key}
        for w in rows:  # oldest first, so the freshest value of each field wins
            if newest - (w.get("last_seen") or 0) > WORKER_TIMEOUT:
                continue
            for field, value in w.items():
                if value is not None:
                    row[field] = value
        merged.append(row)
    return merged


def alive_workers(workers: list[dict], now: float | None = None) -> list[dict]:
    """Deduped workers whose freshest heartbeat is within WORKER_TIMEOUT."""
    now = time.time() if now is None else now
    return [
        w for w in dedupe_workers(workers)
        if now - (w.get("last_seen") or 0) < WORKER_TIMEOUT
    ]


def busy_servers(tasks: list[dict], running_shards: list[dict]) -> set[str]:
    """Server keys currently holding work.

    A sharded task's row carries `server = NULL` — its servers live on the
    shard rows — so shard ownership is the primary signal and the task-level
    server only covers legacy single-node tasks.
    """
    busy = {t.get("server") for t in tasks
            if t.get("status") == "downloading" and t.get("server")}
    busy |= {s.get("server") for s in running_shards if s.get("server")}
    return {b for b in busy if b}


def source_for_worker(server_key: str) -> str:
    """Which repo source a worker serves: BJ nodes do ModelScope, HK does HF."""
    return "modelscope" if server_key.startswith("bj") else "hf"


def worker_serves(server_key: str, source: str) -> bool:
    """Whether a worker may take a task of this source.

    Only ModelScope is BJ-bound; every other source (hf, wget, …) routes to
    the HK workers.
    """
    wanted = "modelscope" if source == "modelscope" else "hf"
    return source_for_worker(server_key) == wanted


SHARED_COORDINATOR_QUEUE = "download-workers"
MS_COORDINATOR_QUEUE = "download-ms-workers"


def coordinator_queue(source: str) -> str:
    """Task queue to start a sharded coordinator on, by task source.

    `download-workers` is polled by the HK workers only — a bj node's
    `--task-queue download-bjN` dedupes against its personal queue, so it never
    polled the shared one (dlm/temporal/__main__.py). Starting every
    coordinator there ran ModelScope listing on an HF node, which on
    2026-08-06 failed t-20260806-cbf39e (RoboDojo) with
    `No module named 'modelscope'` once the shared queue happened to hand the
    activity to w6 — 2 of the 7 HK nodes lack the SDK, so this was a lottery
    the source routing was already meant to settle. HK is also the wrong side
    of the network for ModelScope: the same listing call took over 10 minutes
    from w6 while bj nodes serve it in seconds.

    So ModelScope gets its own shared queue, which every bj node polls. A
    shared queue rather than one node's personal queue because step 8 of the
    coordinator runs after every child finishes — possibly days later — and
    pinning that to a single host makes one node's permanent loss a task that
    never completes.
    """
    if source == "modelscope":
        return MS_COORDINATOR_QUEUE
    return SHARED_COORDINATOR_QUEUE


def worker_coordinator_queue(server_key: str) -> str:
    """The shared coordinator queue a given worker must poll — the mirror of
    coordinator_queue, used at worker startup to register the right queues."""
    return coordinator_queue(source_for_worker(server_key))


def polled_queues(server_key: str, task_queue: str | None = None) -> list[str]:
    """Every queue a worker process registers a poller on.

    This lives here rather than inline in dlm/temporal/__main__.py so the
    routing invariant can be tested against the code production runs: the
    original defect was exactly a worker not polling the queue its
    dispatcher targets, and a test that re-derived the list from
    worker_coordinator_queue could not have caught it.

    `task_queue` is the --task-queue argument (a bj node passes its own
    personal queue, which dedupes away).
    """
    queues = [
        task_queue or SHARED_COORDINATOR_QUEUE,
        f"download-{server_key}",
        worker_coordinator_queue(server_key),
    ]
    return list(dict.fromkeys(queues))


def pending_sources(tasks: list[dict]) -> set[str]:
    """Sources that currently have queued work waiting for a worker."""
    return {(t.get("source") or "hf") for t in tasks if t.get("status") == "pending"}


def idle_workers(
    tasks: list[dict],
    workers: list[dict],
    running_shards: list[dict],
    now: float | None = None,
) -> list[dict]:
    """Alive workers holding no work.

    Idle is only actionable when work is queued for that worker's source —
    an empty queue means idle is the correct resting state, so each entry is
    tagged `starved` for callers that must not raise an alert otherwise.
    """
    busy = busy_servers(tasks, running_shards)
    waiting = pending_sources(tasks)
    out = []
    for w in alive_workers(workers, now):
        key = w.get("server_key") or ""
        if key in busy:
            continue
        source = source_for_worker(key)
        out.append({
            "server_key": key,
            "disk_free_gb": w.get("disk_free_gb"),
            "source": source,
            "starved": source in waiting,
        })
    return out
