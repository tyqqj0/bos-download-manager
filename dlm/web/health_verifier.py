"""Layer 3: cross-layer health correlation — no SSH, no subprocess, no fork.

Every number this module needs already arrives over HTTP. The sidecar pushes
disk, HTTPS connection count, recent file activity, staging size and download
process liveness to `/api/worker-heartbeat`, and they land in the `workers`
table. S1 used to re-fetch the same values by SSH-ing into each HK worker in
parallel every 5 minutes — a redundant pull path, and a dangerous one:

    `asyncio.create_subprocess_shell` forks, and it forks on the event-loop
    thread. On 2026-07-31 22:33 one of those children deadlocked on a libc
    lock it inherited from a sibling thread mid-fork and never reached
    exec(); the parent blocked forever reading the exec errpipe. The loop
    stopped calling accept(), the listen backlog filled with 1095 sockets,
    and the entire control plane was offline for 24 hours while all 16
    workers kept downloading and timing out their progress reports.

`asyncio.wait_for` could not have saved it — the timeout wraps
`communicate()`, which is only reached after the fork returns.

Nothing in this module may fork, spawn a process, or block the event loop.

The SSH path only ever covered w1-w7; the heartbeat path covers all 16, so
this is strictly more coverage on every worker whose sidecar is running.
"""

from __future__ import annotations

import asyncio
import logging
import time

from .fleet import STALE_THRESHOLD, WORKER_TIMEOUT, merge_workers

logger = logging.getLogger("dlm.health_verifier")

VERIFY_INTERVAL = 300  # how often the scheduler asks for a correlation pass

# Sidecar-only columns. A worker reporting none of them sends the basic
# heartbeat only, so there is nothing to cross-reference against.
SIDECAR_FIELDS = (
    "https_connections",
    "files_last_5min",
    "download_process_alive",
)

# Above this many new files in 5 min, Layer 2 events should be arriving too.
EVENT_DELIVERY_FLOOR = 5


async def verify_all_workers() -> dict:
    """Correlate the layers. Runs the SQLite reads off the event-loop thread."""
    return await asyncio.to_thread(collect_fleet_health)


def collect_fleet_health() -> dict:
    """Build the health report from the heartbeat data already in SQLite."""
    from ..queue.snapshot import (
        get_all_tasks, get_running_shards, get_workers, init_db,
    )

    init_db()
    now = time.time()
    # Merged, not deduped: the metrics live on the `@sidecar` hostname while
    # the fresher `@temporal` row carries only liveness.
    workers = merge_workers(get_workers(), now)
    anomalies = correlate_layers(
        workers, get_all_tasks(), get_running_shards(), now
    )
    return {
        "timestamp": now,
        "workers": workers,
        "anomalies": anomalies,
        "reachable_count": sum(1 for w in workers if _is_alive(w, now)),
        "total_count": len(workers),
    }


def _is_alive(worker: dict, now: float) -> bool:
    return now - (worker.get("last_seen") or 0) < WORKER_TIMEOUT


def _has_sidecar(worker: dict) -> bool:
    return any(worker.get(f) is not None for f in SIDECAR_FIELDS)


def work_by_server(tasks: list[dict], running_shards: list[dict]) -> dict[str, dict]:
    """The running work each server holds, freshest first.

    Shard rows are the primary source: a sharded task's own row carries
    `server = NULL`, so a task-level lookup finds nothing and every stall
    check built on it is structurally dead. The task-level scan below only
    covers legacy single-node tasks.
    """
    by_server: dict[str, dict] = {}

    def offer(server: str | None, name: str, updated_at: float | None):
        if not server:
            return
        cur = by_server.get(server)
        if cur is None or (updated_at or 0) > (cur["updated_at"] or 0):
            by_server[server] = {"name": name, "updated_at": updated_at or 0}

    tasks_by_id = {t.get("id"): t for t in tasks}
    for s in running_shards:
        parent = tasks_by_id.get(s.get("task_id")) or {}
        offer(s.get("server"), parent.get("name") or s.get("id", ""), s.get("updated_at"))
    for t in tasks:
        if t.get("status") == "downloading":
            offer(t.get("server"), t.get("name", ""), t.get("updated_at"))
    return by_server


def correlate_layers(
    workers: list[dict],
    tasks: list[dict],
    running_shards: list[dict],
    now: float | None = None,
) -> list[dict]:
    """Cross-reference Layer 1 (heartbeat) against Layer 2 (events) and the work rows."""
    from ..queue.snapshot import _conn

    now = time.time() if now is None else now
    held = work_by_server(tasks, running_shards)
    anomalies: list[dict] = []

    for w in workers:
        server_key = w.get("server_key") or ""
        if not server_key:
            continue

        # An offline worker is the doctor's `offline_workers` case. Repeating
        # it here would double-alert on a single fact.
        if not _is_alive(w, now):
            continue

        if not _has_sidecar(w):
            # Reported, but not by a sidecar — no metrics to correlate. Kept
            # in the report (alerts.py deliberately does not escalate it) so
            # a fleet-wide sidecar gap stays visible.
            anomalies.append({
                "type": "sidecar_missing",
                "server": server_key,
                "message": f"No sidecar metrics from {server_key} — "
                           f"stall detection is blind on this worker",
            })
            continue

        if w.get("download_process_alive") == 0:
            anomalies.append({
                "type": "process_dead_undetected",
                "server": server_key,
                "message": f"Layer 1 reports online but download process is dead "
                           f"on {server_key}",
            })

        work = held.get(server_key)
        if work:
            files_5min = w.get("files_last_5min") or 0
            conns = w.get("https_connections") or 0
            stale_sec = now - (work["updated_at"] or 0)

            # Progress that is still being written is not a stall, whatever
            # the counters say — this replaces the old large-file exemption,
            # which needed an SSH `find` to size the in-flight file.
            if not files_5min and stale_sec > STALE_THRESHOLD:
                anomalies.append({
                    "type": "download_stalled_confirmed" if not conns else "possible_stall",
                    "server": server_key,
                    "task": work["name"],
                    "connections": conns,
                    "stale_seconds": int(stale_sec),
                    "message": (
                        f"Download stalled on {server_key}: {work['name']} "
                        f"(no new files for {int(stale_sec / 60)}min, "
                        f"{conns} connections)"
                    ),
                })

        # Layer 1 sees file activity but Layer 2 delivered no events.
        if (w.get("files_last_5min") or 0) > EVENT_DELIVERY_FLOOR:
            try:
                recent_events = _conn().execute(
                    "SELECT COUNT(*) FROM events "
                    "WHERE server_key = ? AND timestamp > ?",
                    (server_key, now - 600),
                ).fetchone()[0]
            except Exception:
                recent_events = -1  # events table may not exist yet

            if recent_events == 0:
                anomalies.append({
                    "type": "layer2_delivery_broken",
                    "server": server_key,
                    "message": f"Worker {server_key} has file activity (Layer 1) "
                               f"but no events received (Layer 2) in 10 min",
                })

    return anomalies
