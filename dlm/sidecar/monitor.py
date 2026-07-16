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
import logging
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("dlm.sidecar")

HEARTBEAT_INTERVAL = 30
STAGING_PATH = Path("/data/staging")
GB = 1024 ** 3
MB = 1024 ** 2


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
    """Check how many events are pending in the event buffer (if accessible)."""
    # Read from a shared file that event_buffer writes periodically
    try:
        status_file = STAGING_PATH / ".event_buffer_status"
        if status_file.exists():
            return int(status_file.read_text().strip())
    except Exception:
        pass
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
