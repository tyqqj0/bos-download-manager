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

# How far back Layer 2 events are looked for, and equally the grace a worker
# gets before its silence is called broken — when there is no direct signal to
# consult. See EVENT_BACKLOG_TOLERANCE below for the fast path.
#
# Two hours, not ten minutes, and the reason is that the trigger and the
# evidence measure different things. The trigger is files_last_5min, which is
# `find -mmin -5` — files whose mtime moved, i.e. files being WRITTEN. The
# evidence is events, which are only emitted when a file FINISHES. On TB-scale
# datasets those diverge for a long time: on 2026-08-09 w5 was flagged with its
# batch at done_bytes 8.7/34.3 GB, 176 Mbps, four ~1.4 GB files in flight and
# none finished for 16 minutes, its last events matching its previous batch's
# last completion to the second. The buffer flushes every 5s, so nothing was
# late — there was nothing to send.
#
# SQLite cannot narrow this: shards.done_files is only `skipped_files` mid-run
# (activities.py:1406 says so) and the true count lands at batch end, so a
# "did a file complete recently" gate reads 0 on every running batch and would
# switch this alert off fleet-wide. A long tolerance is cruder but honest —
# over two hours even a very large file completes, so total silence is real.
EVENT_SILENCE_TOLERANCE = 7200

# The fast path, used when the worker reports a real event_buffer_pending.
# That column carries the buffer's *backlogged* count (events held past
# event_buffer.STUCK_AFTER), so a positive value is direct evidence that the
# worker emitted something and could not deliver it — no inference from file
# counters required, and no need to wait out a large file. -1/None means the
# sidecar could not read the buffer's status file (missing, stale, or a worker
# on older code), and unknown is not zero: those workers keep the two-hour
# tolerance above rather than being judged on a signal nobody sent.
EVENT_BACKLOG_TOLERANCE = 900


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


def _as_epoch(value) -> float | None:
    """shards.started_at is ISO text; shards.updated_at next to it is an epoch
    float. Accept either rather than trusting one and silently comparing a
    string to a number."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        from datetime import datetime
        return datetime.fromisoformat(str(value)).timestamp()
    except (ValueError, TypeError):
        return None


def _working_since(conn) -> dict[str, float]:
    """{server: epoch it began the OLDEST work row it holds on a live task}

    Deliberately the oldest, not the newest. Pool mode hands a worker one
    batch at a time, so "when did its current row start" restarts the clock
    every few minutes — which would give a worker whose event delivery is
    permanently broken a permanent excuse. Completed rows count: they are the
    evidence that this worker has been on this task long enough for an event
    to have arrived.

    A server missing from the result gets no grace: absent evidence that it
    only just started, silence is judged on its own.
    """
    since: dict[str, float] = {}
    rows = conn.execute(
        "SELECT s.server, s.started_at FROM shards s "
        "JOIN tasks t ON t.id = s.task_id "
        "WHERE t.status = 'downloading' AND s.server IS NOT NULL "
        "AND s.started_at IS NOT NULL"
    ).fetchall()
    for server, started_at in rows:
        epoch = _as_epoch(started_at)
        if epoch is None:
            continue
        if server not in since or epoch < since[server]:
            since[server] = epoch
    return since


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
    try:
        working_since = _working_since(_conn())
    except Exception:
        # No shards table yet, or a schema older than started_at. Empty means
        # "no grace for anyone", i.e. the pre-existing behaviour — not a
        # fleet-wide amnesty on a schema we failed to read.
        working_since = {}
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
            # A reported backlog is direct evidence: the worker emitted events
            # and its buffer could not hand them over. That shortens the wait
            # from two hours to fifteen minutes. Unknown (-1, or a worker whose
            # sidecar never read the file) keeps the long tolerance — the whole
            # point of the -1 is that it must not be read as "no backlog".
            backlog = w.get("event_buffer_pending")
            has_backlog = backlog is not None and backlog > 0
            tolerance = EVENT_BACKLOG_TOLERANCE if has_backlog \
                else EVENT_SILENCE_TOLERANCE

            try:
                recent_events = _conn().execute(
                    "SELECT COUNT(*) FROM events "
                    "WHERE server_key = ? AND timestamp > ?",
                    (server_key, now - tolerance),
                ).fetchone()[0]
            except Exception:
                recent_events = -1  # events table may not exist yet

            # A worker that started working inside the window has not been
            # silent — it has not had time to speak. Sharded mode hid this: a
            # worker took one shard and held it for hours, so it was only ever
            # fresh right after a deploy. Pool mode recruits a worker the
            # moment the window widens, and on 2026-08-09 that turned every
            # recruitment into a WARNING: w1/w2/w3/w7 were all flagged 6
            # minutes into their first batch while their events were in fact
            # arriving seconds later. An alert that fires on healthy routine is
            # how a real one gets ignored.
            began = working_since.get(server_key)
            too_new = began is not None and (now - began) < tolerance

            if recent_events == 0 and not too_new:
                detail = (f"{backlog} events backlogged" if has_backlog
                          else "buffer backlog unknown")
                anomalies.append({
                    "type": "layer2_delivery_broken",
                    "server": server_key,
                    "message": f"Worker {server_key} has file activity (Layer 1) "
                               f"but no events received (Layer 2) in "
                               f"{tolerance // 60} min ({detail})",
                })

    return anomalies
