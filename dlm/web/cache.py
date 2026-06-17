"""In-memory cache for dashboard data, server status, and task state."""

import time
import threading
from dataclasses import dataclass, field


@dataclass
class CacheEntry:
    data: dict = field(default_factory=dict)
    updated_at: float = 0.0


class DLMCache:
    def __init__(self):
        self._lock = threading.Lock()
        self.dashboard = CacheEntry()
        self.servers = CacheEntry()
        self.tasks = CacheEntry()
        self._prev_sizes: dict = {}
        self._prev_sizes_time: float = 0.0
        self._speeds: dict = {}
        self._prev_task_progress: dict = {}
        self._task_speeds: dict = {}

    def set_dashboard(self, data: dict):
        with self._lock:
            self.dashboard.data = data
            self.dashboard.updated_at = time.time()

    def get_dashboard(self) -> dict:
        with self._lock:
            return self.dashboard.data

    def set_servers(self, data: dict):
        with self._lock:
            self.servers.data = data
            self.servers.updated_at = time.time()

    def get_servers(self) -> dict:
        with self._lock:
            return self.servers.data

    def set_tasks(self, data: dict):
        with self._lock:
            self.tasks.data = data
            self.tasks.updated_at = time.time()

    def get_tasks(self) -> dict:
        with self._lock:
            return self.tasks.data

    def last_sync_at(self) -> float:
        with self._lock:
            return self.dashboard.updated_at

    def update_speeds(self, current_sizes: dict):
        """Compute download speeds from size deltas. current_sizes: {server_key: total_gb}"""
        with self._lock:
            now = time.time()
            if self._prev_sizes_time > 0 and self._prev_sizes:
                dt = now - self._prev_sizes_time
                if dt > 30:
                    for key, cur_gb in current_sizes.items():
                        prev_gb = self._prev_sizes.get(key, cur_gb)
                        delta_gb = cur_gb - prev_gb
                        if delta_gb > 0 and dt > 0:
                            self._speeds[key] = (delta_gb * 1024) / dt  # MB/s
                        elif delta_gb == 0:
                            self._speeds[key] = 0.0
            self._prev_sizes = dict(current_sizes)
            self._prev_sizes_time = now

    def get_speeds(self) -> dict:
        with self._lock:
            return dict(self._speeds)

    def update_task_speed(self, repo_id: str, timestamp: int, total_bytes: int):
        """Compute per-task speed from progress file data."""
        with self._lock:
            prev = self._prev_task_progress.get(repo_id)
            if prev:
                dt = timestamp - prev[0]
                if dt >= 20:
                    delta = total_bytes - prev[1]
                    self._task_speeds[repo_id] = max(0.0, (delta / dt) / (1024 * 1024))
                    self._prev_task_progress[repo_id] = (timestamp, total_bytes)
            else:
                self._prev_task_progress[repo_id] = (timestamp, total_bytes)

    def get_task_speeds(self) -> dict:
        with self._lock:
            return dict(self._task_speeds)

    def clear_task_speed(self, repo_id: str):
        with self._lock:
            self._task_speeds.pop(repo_id, None)
            self._prev_task_progress.pop(repo_id, None)


cache = DLMCache()
