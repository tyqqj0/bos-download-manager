"""Alert engine — detect conditions, classify severity, log to file.

Alerts are de-duplicated: only logs on state transitions (new alert or resolved).
The /api/dashboard already returns alerts; this module enhances them with severity
and adds persistent file logging.
"""

import logging
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


def check_alerts(tasks: list, workers: list) -> list[dict]:
    """Evaluate alert conditions. Returns list of active alerts with severity.

    De-duplicates: only emits log lines on state transitions.
    Called every 10s by the scheduler.
    """
    global _active_alerts
    now = time.time()
    new_alerts: dict[str, dict] = {}

    alive_workers = [w for w in workers if now - (w.get("last_seen") or 0) < 180]
    real_workers = [w for w in workers if w.get("server_key", "").startswith("w")]

    # CRITICAL: All workers offline
    if real_workers and not any(w in alive_workers for w in real_workers):
        key = "all_workers_dead"
        new_alerts[key] = {
            "severity": CRITICAL,
            "type": "all_workers_dead",
            "message": "All workers are offline",
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

    # WARNING: Task stuck > 1 hour
    for t in tasks:
        if t.get("status") == "downloading":
            stale = now - (t.get("updated_at") or 0)
            if stale > 3600:
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
