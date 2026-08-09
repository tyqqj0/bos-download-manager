"""DLM Sidecar Monitor — independent worker health reporter.

Runs as a separate systemd service, completely independent of the download
process. Even if the Temporal worker hangs, OOMs, or crashes, this sidecar
continues to report the worker's real state to S1.

Usage:
    python -m dlm.sidecar.monitor --server-key w1

As systemd service:
    systemctl start dlm-sidecar@w1
"""

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

from ..constants import EVENT_BUFFER_STATUS_FILE, STAGING_ROOT

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("dlm.sidecar")

HEARTBEAT_INTERVAL = 30
STAGING_PATH = STAGING_ROOT
STATUS_PATH = EVENT_BUFFER_STATUS_FILE
GB = 1024 ** 3
MB = 1024 ** 2

# The event buffer rewrites its status file every FLUSH_INTERVAL (5s). Anything
# older than this means the writer is gone or wedged, so its last value is not
# evidence of anything. Generous relative to 5s because a loaded worker's flush
# cycle can slip behind on backoff (up to 60s) without being broken.
STATUS_MAX_AGE = 300


def get_disk_free_gb() -> float:
    try:
        return shutil.disk_usage(STAGING_PATH).free / GB
    except OSError:
        return -1


def get_staging_size_mb() -> float:
    try:
        total = 0
        for p in STAGING_PATH.rglob("*"):
            if p.is_file():
                total += p.stat().st_size
        return total / MB
    except OSError:
        return -1


def count_https_connections() -> int:
    try:
        result = subprocess.run(
            ["ss", "-tnp"],
            capture_output=True, text=True, timeout=5,
        )
        return sum(1 for line in result.stdout.splitlines() if ":443" in line)
    except Exception:
        return -1


def count_recent_files(minutes: int = 5) -> int:
    try:
        result = subprocess.run(
            ["find", str(STAGING_PATH), "-mmin", f"-{minutes}", "-type", "f"],
            capture_output=True, text=True, timeout=30,
        )
        return len(result.stdout.strip().splitlines()) if result.stdout.strip() else 0
    except Exception:
        return -1


def is_temporal_worker_alive() -> tuple[bool, int]:
    try:
        result = subprocess.run(
            ["pgrep", "-f", "dlm.temporal"],
            capture_output=True, text=True, timeout=5,
        )
        pids = result.stdout.strip().splitlines()
        if pids:
            return True, int(pids[0])
        return False, 0
    except Exception:
        return False, 0


def get_event_buffer_pending() -> int:
    """Backlogged events in the Temporal worker's event buffer, or -1 = unknown.

    Reports the *stuck* count (events buffered past
    `event_buffer.STUCK_AFTER`), not the raw buffer length: every flush cycle
    holds events for a few seconds by design, and Layer 3 samples every 300s,
    so a raw count would report a healthy buffer as backed up most of the time.

    -1 is load-bearing and must not collapse to 0. A worker process that has
    died leaves this file frozen at whatever it last wrote, and a frozen `0`
    would read as "the delivery channel is fine" forever — the precise
    false-negative this signal exists to remove. So a file older than
    STATUS_MAX_AGE is unknown, as is a missing or unparseable one.
    """
    try:
        status_file = STATUS_PATH
        if not status_file.exists():
            return -1
        if time.time() - status_file.stat().st_mtime > STATUS_MAX_AGE:
            return -1        # writer is gone or wedged; stale is not healthy
        raw = status_file.read_text().strip()
        if not raw:
            return -1
        try:
            payload = json.loads(raw)
        except ValueError:
            return -1
        if not isinstance(payload, dict):
            # Pre-2026-08-09 format: the file was documented as a bare count.
            # json.loads parses "7" successfully, so this is the branch a
            # legacy file actually lands in, not the ValueError above.
            return int(payload)
        value = payload.get("stuck")
        return int(value) if value is not None else -1
    except Exception:
        return -1


def collect_metrics(server_key: str) -> dict:
    process_alive, process_pid = is_temporal_worker_alive()
    return {
        "server_key": server_key,
        "hostname": f"{server_key}@sidecar",
        "timestamp": time.time(),
        "status": "online",
        # Disk
        "disk_free_gb": round(get_disk_free_gb(), 1),
        "staging_size_mb": round(get_staging_size_mb(), 0),
        # Network
        "https_connections": count_https_connections(),
        "files_last_5min": count_recent_files(5),
        # Process
        "download_process_alive": process_alive,
        "download_process_pid": process_pid,
        # Event buffer
        "event_buffer_pending": get_event_buffer_pending(),
    }


def heartbeat_loop(server_key: str, coordinator: str):
    import requests

    logger.info(f"Sidecar starting: server_key={server_key}, coordinator={coordinator}")
    consecutive_failures = 0

    while True:
        try:
            metrics = collect_metrics(server_key)
            resp = requests.post(
                f"{coordinator}/api/worker-heartbeat",
                json=metrics,
                timeout=10,
            )
            if resp.status_code == 200:
                consecutive_failures = 0
            else:
                consecutive_failures += 1
                logger.warning(f"Heartbeat POST returned {resp.status_code}")
        except Exception as e:
            consecutive_failures += 1
            if consecutive_failures <= 3 or consecutive_failures % 10 == 0:
                logger.warning(f"Heartbeat failed ({consecutive_failures}x): {e}")

        time.sleep(HEARTBEAT_INTERVAL)


def main():
    parser = argparse.ArgumentParser(description="DLM Sidecar Monitor")
    parser.add_argument("--server-key", required=True, help="Worker identifier (w1-w7)")
    parser.add_argument(
        "--coordinator",
        default=os.environ.get("DLM_COORDINATOR", "http://154.85.43.52:8080"),
    )
    args = parser.parse_args()

    heartbeat_loop(args.server_key, args.coordinator)


if __name__ == "__main__":
    main()
