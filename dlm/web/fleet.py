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

DEFAULT_DISPATCH_MODE = os.environ.get("DLM_DEFAULT_DISPATCH_MODE", "sharded")
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

# Window weights per priority band. The coordinator's per-wake window is
# `max(1, floor(P * W_i / sum(W_active)))` — P being alive workers serving the
# source — so these decide how a busy pool is split between concurrent pool
# tasks. Priority 0-2 ("P0", the queue-jump band) gets 1.5x the share of
# everything else; the sum keeps total concurrency bounded by P no matter how
# many tasks are admitted.
POOL_WEIGHT_P0 = 1.5
POOL_WEIGHT_DEFAULT = 1.0
POOL_P0_MAX_PRIORITY = 2  # priority <= this is the P0 band


def pool_task_weight(priority: int) -> float:
    """Window weight for one pool task's priority."""
    return POOL_WEIGHT_P0 if (priority or 0) <= POOL_P0_MAX_PRIORITY else POOL_WEIGHT_DEFAULT


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
