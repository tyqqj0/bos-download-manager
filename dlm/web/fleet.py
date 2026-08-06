"""Fleet state primitives — one definition of "alive", "busy", and "routable".

These questions are asked from four places (doctor, alerts, reconciler,
the idle-workers route) and every copy that drifted produced a false alarm:
a stale `wN@sidecar` row reported a healthy worker offline, and task-level
`server` alone reported every shard-running worker as idle. Import from here
rather than re-deriving.
"""

from __future__ import annotations

import time

WORKER_TIMEOUT = 180  # seconds without a heartbeat before a worker is offline
MIN_SHARD_DISK_GB = 70  # a worker below this is not offered new shards
STALE_THRESHOLD = 600  # 10 min without a task update = suspicious
DEAD_THRESHOLD = 1800  # 30 min without a task update = definitely dead

# Terminal/operator states. A progress report must never move a task out of
# one of these — a dying workflow's late report used to resurrect tasks an
# operator had explicitly stopped (2026-07-31 incident).
TERMINAL_STATUSES = ("paused", "preempted", "revoked", "skipped", "failed", "done")


def has_live_workflow(task_id: str, running_ids) -> bool:
    """Whether any running workflow belongs to this task.

    A task's work can live under any of the historical ID schemes, and a
    check that misses one concludes the task is orphaned and re-dispatches
    it — a second coordinator downloading what is already downloading. The
    doctor's fix path had drifted from its own report path in exactly this
    way (it omitted the legacy `{task_id}-part` children).
    """
    legacy = f"dl-{task_id}"
    return (
        legacy in running_ids
        or f"split-download-{task_id}" in running_ids
        or f"sharded-{task_id}" in running_ids
        or any(wid.startswith(f"shard-s-{task_id}-") for wid in running_ids)
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
