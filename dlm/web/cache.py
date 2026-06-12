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


cache = DLMCache()
