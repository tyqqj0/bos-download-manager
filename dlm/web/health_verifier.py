"""Layer 3: SSH Health Verifier — S1 actively checks worker ground truth.

Runs every VERIFY_INTERVAL seconds. SSH to each worker in parallel,
checks real file activity, connections, process status. Cross-references
with Layer 1/2 data to detect lies/staleness.
"""

import asyncio
import logging
import time
from typing import Optional

logger = logging.getLogger("dlm.health_verifier")

VERIFY_INTERVAL = 300  # 5 minutes
SSH_TIMEOUT = 20       # per-worker timeout

WORKERS = {
    "w1": "156.240.120.209",
    "w2": "154.85.53.152",
    "w3": "154.85.49.95",
    "w4": "154.85.40.244",
    "w5": "154.85.54.251",
    "w6": "154.85.50.210",
    "w7": "156.240.121.60",
}

SSH_CHECK_SCRIPT = """
echo DISK_FREE_KB:$(df --output=avail /data 2>/dev/null | tail -1 | tr -d ' ')
echo CONNS:$(ss -tnp 2>/dev/null | grep -c ":443")
echo FILES5:$(find /data/staging -mmin -5 -type f 2>/dev/null | wc -l)
echo STAGING_MB:$(du -sm /data/staging/ 2>/dev/null | cut -f1)
echo PROC_PID:$(pgrep -f "dlm.temporal" 2>/dev/null | head -1)
echo SIDECAR_PID:$(pgrep -f "dlm.sidecar" 2>/dev/null | head -1)
echo BIGGEST:$(find /data/staging -type f -printf '%s %f\\n' 2>/dev/null | sort -rn | head -1)
"""


async def verify_worker(server_key: str, ip: str) -> dict:
    """SSH to a single worker, parse output into structured metrics."""
    cmd = (
        f"ssh -o ConnectTimeout=10 -o ServerAliveInterval=5 "
        f"-o ServerAliveCountMax=2 -o StrictHostKeyChecking=no "
        f"root@{ip} '{SSH_CHECK_SCRIPT}'"
    )

    result = {
        "server_key": server_key,
        "ip": ip,
        "timestamp": time.time(),
        "reachable": False,
        "disk_free_gb": None,
        "https_connections": None,
        "files_last_5min": None,
        "staging_size_mb": None,
        "process_pid": None,
        "sidecar_pid": None,
        "biggest_file": None,
    }

    try:
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=SSH_TIMEOUT)
        output = stdout.decode("utf-8", errors="replace")

        if proc.returncode != 0:
            return result

        result["reachable"] = True

        for line in output.strip().splitlines():
            if ":" not in line:
                continue
            key, _, value = line.partition(":")
            value = value.strip()
            try:
                if key == "DISK_FREE_KB" and value.isdigit():
                    result["disk_free_gb"] = round(int(value) / (1024 * 1024), 1)
                elif key == "CONNS":
                    result["https_connections"] = int(value)
                elif key == "FILES5":
                    result["files_last_5min"] = int(value)
                elif key == "STAGING_MB" and value.isdigit():
                    result["staging_size_mb"] = int(value)
                elif key == "PROC_PID" and value.isdigit():
                    result["process_pid"] = int(value)
                elif key == "SIDECAR_PID" and value.isdigit():
                    result["sidecar_pid"] = int(value)
                elif key == "BIGGEST" and value:
                    parts = value.split(maxsplit=1)
                    if len(parts) == 2 and parts[0].isdigit():
                        result["biggest_file"] = {
                            "size_bytes": int(parts[0]),
                            "name": parts[1],
                        }
            except (ValueError, IndexError):
                continue

    except asyncio.TimeoutError:
        logger.warning(f"SSH timeout for {server_key} ({ip})")
    except Exception as e:
        logger.warning(f"SSH error for {server_key}: {e}")

    return result


async def verify_all_workers() -> dict:
    """Parallel SSH verify all workers. Returns report with anomalies."""
    tasks = [verify_worker(sk, ip) for sk, ip in WORKERS.items()]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Filter out exceptions
    worker_results = []
    for r in results:
        if isinstance(r, dict):
            worker_results.append(r)
        else:
            logger.error(f"Verify error: {r}")

    # Cross-layer correlation
    anomalies = correlate_layers(worker_results)

    return {
        "timestamp": time.time(),
        "workers": worker_results,
        "anomalies": anomalies,
        "reachable_count": sum(1 for r in worker_results if r.get("reachable")),
        "total_count": len(WORKERS),
    }


def correlate_layers(layer3_results: list) -> list:
    """Cross-reference Layer 3 SSH data with Layer 1/2 in the DB."""
    from ..queue.snapshot import _conn, init_db

    init_db()
    conn = _conn()
    anomalies = []
    now = time.time()

    for r in layer3_results:
        if not r.get("reachable"):
            anomalies.append({
                "type": "ssh_unreachable",
                "server": r["server_key"],
                "message": f"Cannot SSH to {r['server_key']}",
            })
            continue

        server_key = r["server_key"]

        # Check: Layer 1 says process alive, Layer 3 finds no process
        worker_row = conn.execute(
            "SELECT * FROM workers WHERE server_key = ?", (server_key,)
        ).fetchone()

        if worker_row:
            l1_last_seen = worker_row["last_seen"] or 0
            l1_alive = (now - l1_last_seen) < 180

            # Layer 1 alive but Layer 3 no process → sidecar lying or stale
            if l1_alive and not r.get("process_pid"):
                anomalies.append({
                    "type": "process_dead_undetected",
                    "server": server_key,
                    "message": f"Layer 1 reports online but download process is dead on {server_key}",
                })

        # Check: task is 'downloading' on this server but no file activity
        task_row = conn.execute(
            "SELECT id, name, updated_at FROM tasks WHERE server = ? AND status = 'downloading'",
            (server_key,)
        ).fetchone()

        if task_row:
            task_name = task_row["name"]
            files_5min = r.get("files_last_5min", 0) or 0
            conns = r.get("https_connections", 0) or 0

            # No file activity AND no connections → definitely stalled
            if files_5min == 0 and conns == 0:
                stale_sec = now - (task_row["updated_at"] or 0)
                if stale_sec > 600:  # only flag if stale > 10 min
                    anomalies.append({
                        "type": "download_stalled_confirmed",
                        "server": server_key,
                        "task": task_name,
                        "stale_seconds": int(stale_sec),
                        "message": f"Download stalled on {server_key}: {task_name} "
                                   f"(no files, no connections for {int(stale_sec/60)}min)",
                    })

            # Has connections but no files → might be downloading large file
            elif files_5min == 0 and conns > 0:
                biggest = r.get("biggest_file")
                if biggest and biggest.get("size_bytes", 0) > 500 * 1024 * 1024:
                    # Large file detected, not a stall
                    pass
                else:
                    anomalies.append({
                        "type": "possible_stall",
                        "server": server_key,
                        "task": task_name,
                        "connections": conns,
                        "message": f"Possible stall on {server_key}: {conns} connections but 0 new files",
                    })

        # Check: sidecar not running
        if not r.get("sidecar_pid"):
            anomalies.append({
                "type": "sidecar_dead",
                "server": server_key,
                "message": f"Sidecar monitor not running on {server_key}",
            })

        # Check: Layer 3 sees file activity but Layer 2 has no recent events
        # (event delivery broken)
        if r.get("files_last_5min", 0) and r.get("files_last_5min") > 5:
            try:
                recent_events = conn.execute(
                    "SELECT COUNT(*) FROM events "
                    "WHERE server_key = ? AND timestamp > ?",
                    (server_key, now - 600),  # last 10 min
                ).fetchone()[0]
            except Exception:
                recent_events = -1  # events table may not exist yet

            if recent_events == 0:
                anomalies.append({
                    "type": "layer2_delivery_broken",
                    "server": server_key,
                    "message": f"Worker {server_key} has file activity (Layer 3) "
                               f"but no events received (Layer 2) in 10 min",
                })

    return anomalies
