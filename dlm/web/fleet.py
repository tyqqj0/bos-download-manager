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
