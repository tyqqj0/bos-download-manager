"""Alert engine — detect conditions, classify severity, log to file.

Alerts are de-duplicated: only logs on state transitions (new alert or resolved).
The /api/dashboard already returns alerts; this module enhances them with severity
and adds persistent file logging.

Three-layer cross-referencing:
- Layer 1 (sidecar heartbeat) for process/disk status
- Layer 2 (event buffer) for download activity
- Layer 3 (cross-layer correlation) for contradictions between the two
"""

import logging
import socket
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger("dlm.web")

ALERT_LOG_PATH = Path("/data/dlm-alerts.log")

# Severity levels
CRITICAL = "critical"   # system-wide failure, needs immediate attention
WARNING = "warning"     # degraded but auto-healing may fix it
INFO = "info"           # resolved or informational

# De-duplication state: tracks active alerts to only log transitions
_active_alerts: dict[str, dict] = {}  # key → alert dict

# How long a finished task's missing files stay on the WARNING list.
#
# Missing files are permanent by nature (a dead upstream file stays dead), and
# an alert that can never resolve accumulates: after a few months of normal
# operation the dashboard's alert list would be mostly historical losses, with
# the one worker that went offline five minutes ago buried among them. That is
# how an alert channel stops being read.
#
# So the WARNING is a notification with a shelf life, while the RECORD is
# permanent and lives in three places that do not expire: the `missing_files`
# table, /api/doctor's `missing_files` section, and the transition line this
# alert already wrote to /data/dlm-alerts.log. The over-ceiling CRITICAL has
# no window — by construction it is rare, and it means something went wrong
# beyond a few dead files.
MISSING_ALERT_WINDOW_S = 7 * 86400


def _finalized_within(task: dict, window_s: float, now: float) -> bool:
    """Whether this task finished recently enough to still be worth notifying.

    Fails toward True — an unparseable or absent `completed_at` keeps the
    alert. Staying silent is the failure mode this whole feature exists to
    prevent, so ambiguity must not buy silence.
    """
    stamp = task.get("completed_at")
    if not stamp:
        return True
    try:
        from datetime import datetime, timezone

        when = datetime.fromisoformat(str(stamp))
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        return (now - when.timestamp()) <= window_s
    except Exception:
        return True

# Alert logger with file handler
_alert_logger: Optional[logging.Logger] = None


def _get_alert_logger() -> logging.Logger:
    global _alert_logger
    if _alert_logger is None:
        _alert_logger = logging.getLogger("dlm.alerts")
        _alert_logger.setLevel(logging.DEBUG)
        _alert_logger.propagate = False
        try:
            handler = logging.FileHandler(ALERT_LOG_PATH)
            handler.setFormatter(logging.Formatter(
                "%(asctime)s [%(levelname)s] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            ))
            _alert_logger.addHandler(handler)
        except (OSError, PermissionError):
            pass  # /data may not exist in dev
    return _alert_logger


def s1_self_check() -> bool:
    """Check if S1 itself has network connectivity.

    Returns True if at least one external target is reachable.
    Used to distinguish 'all workers dead' from 'S1 is disconnected'.
    """
    targets = [("8.8.8.8", 53), ("1.1.1.1", 53), ("114.114.114.114", 53)]
    for host, port in targets:
        try:
            sock = socket.create_connection((host, port), timeout=3)
            sock.close()
            return True
        except (OSError, socket.timeout):
            continue
    return False


def _pool_task_holds_no_work(task: dict) -> bool:
    """The DB-reading half of decision E's exemption.

    The predicate itself is `fleet.pool_task_holds_no_work` — one definition
    shared with /api/doctor's `stuck_tasks`, so the two surfaces cannot
    disagree about the same task. This wrapper only supplies the batch rows
    (check_alerts runs in a worker thread, so a plain SQLite read is fine
    here) and decides what to do when it cannot get them.

    Any read failure returns True (apply the exemption) rather than firing
    task_stuck off data we could not confirm — the same "stay silent rather
    than cry wolf" posture the idle_worker check below already uses.
    """
    try:
        from ..queue.snapshot import get_shards_by_task
        rows = get_shards_by_task(task.get("id", ""))
    except Exception:
        return True
    from .fleet import pool_task_holds_no_work
    return pool_task_holds_no_work(task, rows)


def check_alerts(tasks: list, workers: list) -> list[dict]:
    """Evaluate alert conditions. Returns list of active alerts with severity.

    De-duplicates: only emits log lines on state transitions.
    Called every 10s by the scheduler.
    """
    global _active_alerts
    now = time.time()
    new_alerts: dict[str, dict] = {}

    from .fleet import dedupe_workers, alive_workers as compute_alive

    workers = dedupe_workers(workers)
    alive_workers = compute_alive(workers, now)
    # Every worker counts for offline detection — bj nodes carry the ModelScope
    # half of the fleet and were previously exempt from these alerts.
    real_workers = list(workers)

    # CRITICAL: All workers offline — but check if S1 itself is the problem
    if real_workers and not any(w in alive_workers for w in real_workers):
        if s1_self_check():
            key = "all_workers_dead"
            new_alerts[key] = {
                "severity": CRITICAL,
                "type": "all_workers_dead",
                "message": "All workers are offline",
            }
        else:
            key = "s1_network_degraded"
            new_alerts[key] = {
                "severity": CRITICAL,
                "type": "s1_network_degraded",
                "message": "S1 network degraded — cannot reach workers (S1 issue, not workers)",
            }

    # CRITICAL: Disk full on any active worker (<10GB)
    for w in alive_workers:
        disk_free = w.get("disk_free_gb")
        if disk_free is not None and disk_free < 10:
            key = f"disk_full:{w.get('server_key', '')}"
            new_alerts[key] = {
                "severity": CRITICAL,
                "type": "disk_full",
                "server": w.get("server_key", ""),
                "disk_free_gb": disk_free,
                "message": f"Disk full on {w.get('server_key', '')}: {disk_free:.1f}GB free",
            }

    # WARNING: Individual worker offline > 5 min
    for w in real_workers:
        if w not in alive_workers:
            offline_sec = now - (w.get("last_seen") or 0)
            if offline_sec > 300:  # 5 min
                key = f"worker_offline:{w.get('server_key', '')}"
                new_alerts[key] = {
                    "severity": WARNING,
                    "type": "worker_offline",
                    "server": w.get("server_key", ""),
                    "offline_min": int(offline_sec / 60),
                    "message": f"Worker {w.get('server_key', '')} offline for {int(offline_sec/60)}min",
                }

    # WARNING: Download process dead but worker online (from sidecar)
    for w in alive_workers:
        if w.get("download_process_alive") == 0:
            key = f"process_dead:{w.get('server_key', '')}"
            new_alerts[key] = {
                "severity": WARNING,
                "type": "download_process_dead",
                "server": w.get("server_key", ""),
                "message": f"Download process dead on {w.get('server_key', '')} (sidecar still reporting)",
            }

    # WARNING: Task stuck > 1 hour. Exempt a pool task admitted but holding
    # no work (decision E) — checked first so a sharded task's `and`
    # short-circuits before ever calling the pool-only helper, keeping the
    # sharded path's alert (and its lack of extra DB reads) byte-identical.
    for t in tasks:
        if t.get("status") == "downloading":
            stale = now - (t.get("updated_at") or 0)
            if stale > 3600:
                if ((t.get("dispatch_mode") or "sharded") == "pool"
                        and _pool_task_holds_no_work(t)):
                    continue
                key = f"task_stuck:{t.get('id', '')}"
                new_alerts[key] = {
                    "severity": WARNING,
                    "type": "task_stuck",
                    "task_id": t.get("id", ""),
                    "task_name": t.get("name", ""),
                    "stale_min": int(stale / 60),
                    "message": f"Task {t.get('name', '')} stuck for {int(stale/60)}min",
                }

    # WARNING: Task failed >= 5 times
    for t in tasks:
        if t.get("status") == "failed" and (t.get("retry_count") or 0) >= 5:
            key = f"repeated_failure:{t.get('id', '')}"
            new_alerts[key] = {
                "severity": WARNING,
                "type": "repeated_failure",
                "task_id": t.get("id", ""),
                "task_name": t.get("name", ""),
                "retry_count": t.get("retry_count", 0),
                "message": f"Task {t.get('name', '')} failed {t.get('retry_count', 0)} times",
            }

    # WARNING/CRITICAL: files this task listed that never reached BOS.
    #
    # T4 already decided done-vs-failed by the task's own ceiling; this is the
    # other half of R4 ("缺件不静默"). A `done` task that quietly forgave a few
    # permanently-missing files is otherwise visible only to someone who
    # thinks to GET /api/tasks/{id}/missing-files — and nobody GETs an
    # endpoint they have no reason to suspect.
    #
    # Gated on FINALIZED_STATUSES, not the broader TERMINAL_STATUSES: a paused
    # or preempted task's archived rows are files the next round will most
    # likely fetch, and those two states never went through _finalize, so they
    # carry no ceiling to judge against either.
    from .fleet import FINALIZED_STATUSES

    for t in tasks:
        count = t.get("missing_files_count") or 0
        if count <= 0 or t.get("status") not in FINALIZED_STATUSES:
            continue
        task_id = t.get("id", "")
        name = t.get("name") or task_id
        # `limit` is what T4's finalize recorded — the SAME number its verdict
        # used, which is the point of storing it. A hardcoded threshold here
        # would contradict the verdict in both directions: a 300-file task
        # losing 50 (ceiling 10, already `failed`) would rate only a WARNING,
        # while a 5M-file task losing 120 (ceiling 25000, entirely normal)
        # would scream CRITICAL. `limit == 0` means the task never finalized
        # under T4 — an older task row, or one whose re-check never ran — so
        # its count cannot be judged, only reported.
        limit = t.get("missing_files_limit") or 0
        detail = (f"{count} file(s) never reached BOS. "
                  f"GET /api/tasks/{task_id}/missing-files for the list "
                  f"(path, reason, size, batch, worker).")
        if limit > 0 and count > limit:
            new_alerts[f"missing_files_many:{task_id}"] = {
                "severity": CRITICAL,
                "type": "missing_files_many",
                "task_id": task_id,
                "task_name": t.get("name", ""),
                "missing_files_count": count,
                "missing_files_limit": limit,
                "message": (
                    f"Task {name} is over its missing-file ceiling "
                    f"({count} > {limit}). {detail} This is not the handful of "
                    f"dead upstream files the ceiling exists to forgive — "
                    f"check whether one source, one worker, or one credential "
                    f"is behind them before re-dispatching."
                ),
            }
        else:
            # One alert per task, not both: the CRITICAL already carries the
            # count, and a duplicate would double every incident-log line.
            if not _finalized_within(t, MISSING_ALERT_WINDOW_S, now):
                continue
            new_alerts[f"missing_files:{task_id}"] = {
                "severity": WARNING,
                "type": "missing_files",
                "task_id": task_id,
                "task_name": t.get("name", ""),
                "missing_files_count": count,
                "missing_files_limit": limit,
                "message": (
                    f"Task {name} finished {t.get('status', '')} with {detail}"
                    + ("" if limit else " (no ceiling recorded — this task did"
                                        " not finalize under the missing-file"
                                        " re-check)")
                ),
            }

    # WARNING: worker holds no work while work is queued for its source.
    # Idle with an empty queue is the correct resting state, not an alert.
    try:
        from ..queue.snapshot import get_running_shards
        from .fleet import idle_workers as compute_idle

        for c in compute_idle(tasks, workers, get_running_shards(), now):
            if not c["starved"]:
                continue
            new_alerts[f"idle_worker:{c['server_key']}"] = {
                "severity": WARNING,
                "type": "idle_worker",
                "server": c["server_key"],
                "disk_free_gb": c["disk_free_gb"],
                "message": (
                    f"Worker {c['server_key']} idle while {c['source']} tasks "
                    f"wait in queue. Disk: {c['disk_free_gb'] or '?'}GB free."
                ),
            }
    except Exception:
        pass  # cannot determine — stay silent rather than cry wolf

    # WARNING: contradictions between the heartbeat and the event stream.
    # `sidecar_missing` and `possible_stall` stay out of this list on purpose:
    # both are advisory, and escalating them would alert on 11 of 16 workers.
    from .cache import cache
    verify_report = cache.get("health_verify_report")
    if verify_report and isinstance(verify_report, dict):
        for anomaly in verify_report.get("anomalies", []):
            atype = anomaly.get("type", "")
            server = anomaly.get("server", "")
            if atype in ("download_stalled_confirmed", "layer2_delivery_broken",
                         "process_dead_undetected"):
                key = f"{atype}:{server}"
                new_alerts[key] = {
                    "severity": WARNING,
                    "type": atype,
                    "server": server,
                    "message": anomaly.get("message", ""),
                }

    # CRITICAL/WARNING: pool_starved (decision A) and pool_orphaned (review
    # finding C2). The three pool_starved triggers need Temporal RPCs, which
    # only reconcile()'s async patrol can make — check_alerts runs every 10s
    # off a synchronous thread and cannot make them itself, so it reads what
    # the last reconcile pass already cached.
    # Same pattern as health_verify_report above: read a cached report,
    # re-key into new_alerts, done. The alerts are already fully shaped
    # (severity/type/task_id/trigger/message/evidence) by inspect_pool_tasks.
    reconciler_report = cache.get("reconciler_report")
    if reconciler_report and isinstance(reconciler_report, dict):
        for a in reconciler_report.get("pool_starved", []):
            new_alerts[f"pool_starved:{a.get('task_id', '')}"] = a

        # CRITICAL: pool_orphaned — a downloading pool task with no live
        # workflow. Decision C deliberately does not auto-re-dispatch it (a
        # second PoolDownloadWorkflow can wedge the task on a chunking
        # mismatch), and no other surface an operator watches can see it:
        # pool_starved's trigger 1 finds healthy pollers, its triggers 2/3
        # describe a workflow that no longer exists and get nothing back, and
        # task_stuck is exempted by decision E for exactly this row shape.
        # Distinct from pool_starved on purpose — the operator action is
        # different, so sharing the type would make the message wrong.
        for o in reconciler_report.get("pool_orphaned", []):
            task_id = o.get("task_id", "")
            name = o.get("name") or task_id
            new_alerts[f"pool_orphaned:{task_id}"] = {
                "severity": CRITICAL,
                "type": "pool_orphaned",
                "task_id": task_id,
                "task_name": o.get("name", ""),
                "stale_seconds": o.get("stale_seconds"),
                "message": (
                    f"Pool task {name} has no live workflow and is NOT "
                    f"re-dispatched automatically. Confirm the coordinator is "
                    f"gone, then POST /api/doctor with "
                    f'{{"actions": ["redispatch_pool"]}} — the default fix '
                    f"action deliberately refuses pool tasks. redispatch_pool "
                    f"clears this task's batch-row bookkeeping before starting "
                    f"the fresh coordinator; already-uploaded files are not "
                    f"re-downloaded — the BOS resume filter skips them again."
                ),
            }

    # Log state transitions
    al = _get_alert_logger()

    # New alerts (not in previous _active_alerts)
    for key, alert in new_alerts.items():
        if key not in _active_alerts:
            level = logging.CRITICAL if alert["severity"] == CRITICAL else logging.WARNING
            al.log(level, alert["message"])

    # Resolved alerts (in previous but not in new)
    for key, alert in _active_alerts.items():
        if key not in new_alerts:
            al.info(f"RESOLVED: {alert['message']}")

    _active_alerts = new_alerts
    return list(new_alerts.values())
