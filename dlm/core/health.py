"""Health checks — worker alive, stuck detection, auto-restart."""

import re
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

from .models import Server
from .ssh import ssh_server, ssh_worker_alive, ssh_parallel, ssh_check_current

STUCK_THRESHOLD = 24 * 3600  # seconds


def restart_worker(server: Server) -> tuple[str, bool]:
    """Kill and restart the tmux worker session."""
    cmd = (
        "tmux kill-session -t worker 2>/dev/null; "
        f"tmux new-session -d -s worker 'bash {server.path}/queue-worker.sh'"
    )
    return ssh_server(server, cmd)


def check_and_restart_workers(servers: dict[str, Server]) -> list[str]:
    """Check all workers, restart dead ones. Returns list of restarted server keys."""
    enabled = [s for s in servers.values() if s.enabled]
    if not enabled:
        return []

    alive_results = ssh_parallel(enabled, "tmux has-session -t worker 2>/dev/null")

    dead = [key for key, (_, ok) in alive_results.items() if not ok]
    if not dead:
        return []

    restarted = []
    for key in dead:
        srv = servers[key]
        _, ok = restart_worker(srv)
        if ok:
            restarted.append(key)

    return restarted


def check_task_stuck(server: Server) -> dict | None:
    """Check if the current task is stuck (>24h no log activity).

    Returns dict with {task, hours, last_line} or None if not stuck / no current task.
    """
    current = ssh_check_current(server)
    if not current:
        return None

    cmd = f"grep -E 'START|DONE|FAILED' {server.path}/queue.log 2>/dev/null | tail -5"
    log_out, ok = ssh_server(server, cmd)
    if not ok or not log_out.strip():
        return None

    last_time = _parse_last_timestamp(log_out)
    if last_time is None:
        return None

    now = datetime.now(timezone.utc)
    elapsed = (now - last_time).total_seconds()

    if elapsed > STUCK_THRESHOLD:
        task_name = _extract_task_from_current(current)
        return {
            "task": task_name,
            "hours": round(elapsed / 3600, 1),
            "last_line": log_out.strip().splitlines()[-1][:80],
        }

    return None


def check_all_stuck(servers: dict[str, Server]) -> dict[str, dict]:
    """Check all servers for stuck tasks. Returns {server_key: stuck_info}."""
    enabled = [s for s in servers.values() if s.enabled]
    results = {}

    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(check_task_stuck, srv): srv.key for srv in enabled}
        for future in as_completed(futures):
            key = futures[future]
            try:
                info = future.result()
                if info:
                    results[key] = info
            except Exception:
                pass

    return results


def _parse_last_timestamp(log_text: str) -> datetime | None:
    """Parse timestamp from queue.log lines like: [Thu Jun 12 14:30:00 CST 2026] START: ..."""
    lines = log_text.strip().splitlines()
    for line in reversed(lines):
        m = re.match(r"\[(.+?)\]", line)
        if m:
            ts_str = m.group(1)
            ts_str = re.sub(r"\s+[A-Z]{3,4}\s+", " ", ts_str)
            try:
                dt = datetime.strptime(ts_str, "%a %b %d %H:%M:%S %Y")
                return dt.replace(tzinfo=timezone.utc)
            except ValueError:
                continue
    return None


def _extract_task_from_current(current_text: str) -> str:
    """Extract dataset name from current.txt content (download command line)."""
    parts = current_text.strip().split()
    for i, p in enumerate(parts):
        if p.endswith("download.sh") or p.endswith("download-modelscope.sh"):
            if i + 1 < len(parts):
                repo = parts[i + 1]
                return repo.split("/")[-1] if "/" in repo else repo
    return current_text.strip()[:40]
