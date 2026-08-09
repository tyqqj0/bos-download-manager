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
        self.tasks = CacheEntry()
        self._kv: dict = {}

    def set_dashboard(self, data: dict):
        with self._lock:
            self.dashboard.data = data
            self.dashboard.updated_at = time.time()

    def get_dashboard(self) -> dict:
        with self._lock:
            return self.dashboard.data

    # No servers entry: `set_servers` was never called after the Temporal
    # rewrite, so `get_servers` returned {} forever and GET /api/servers/{key}
    # 404'd on every live worker. Both routes read the DB directly now; the
    # per-node view is 16 rows, not the dashboard's 7+ aggregate queries.

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

    def set(self, key: str, value):
        with self._lock:
            self._kv[key] = value

    def get(self, key: str, default=None):
        with self._lock:
            return self._kv.get(key, default)


cache = DLMCache()
