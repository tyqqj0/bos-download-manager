"""Server auto-selection with live load checking."""

import socket
from typing import Optional

from .models import State, Server
from .ssh import ssh_queue_depth, ssh_check_current, ssh_worker_alive


def select_server(state: State, check_live: bool = True, exclude: list = None) -> Optional[str]:
    """
    Pick the least-loaded enabled server.

    Algorithm:
    1. Filter to enabled servers not in exclude list
    2. Score each server based on load
    3. Return the key with lowest score, or None if all overloaded
    """
    exclude = exclude or []
    candidates = {
        k: v for k, v in state.servers.items()
        if v.enabled and k not in exclude
        and getattr(v, 'role', 'worker') == 'worker'
    }
    if not candidates:
        return None

    scores = {}
    for key, server in candidates.items():
        score = _score_from_state(state, key)
        if check_live:
            live_score = _score_from_live(server)
            if live_score is not None:
                score = live_score
        scores[key] = score

    best = min(scores, key=scores.get)
    return best


def is_local_server(server: Server) -> bool:
    """Check if the current machine IS this server (no SSH needed)."""
    try:
        local_ips = _get_local_ips()
        return server.host in local_ips
    except Exception:
        return False


def _score_from_state(state: State, server_key: str) -> float:
    """Score based on state.json data (fallback when SSH unavailable)."""
    active = state.active_tasks_for_server(server_key)
    task_count = len(active)
    total_size = sum(t.size_gb for t in active if t.size_gb)
    return task_count * 100 + total_size


def _score_from_live(server: Server) -> Optional[float]:
    """Score based on live SSH check. Returns None if SSH fails."""
    try:
        if not ssh_worker_alive(server):
            return 10000  # worker down, deprioritize

        queue = ssh_queue_depth(server)
        current = ssh_check_current(server)
        running = 1 if current else 0

        return running * 100 + queue * 50
    except Exception:
        return None


def _get_local_ips() -> set:
    """Get all IP addresses of the local machine."""
    ips = {"127.0.0.1"}
    try:
        hostname = socket.gethostname()
        for info in socket.getaddrinfo(hostname, None):
            ips.add(info[4][0])
    except Exception:
        pass
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ips.add(s.getsockname()[0])
        s.close()
    except Exception:
        pass
    return ips
