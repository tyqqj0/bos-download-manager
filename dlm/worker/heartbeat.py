"""Heartbeat thread — writes worker liveness to BOS state every 60s."""

import os
import platform
import threading
import logging

from ..core.models import _now

logger = logging.getLogger(__name__)

HEARTBEAT_INTERVAL = 60


class HeartbeatThread(threading.Thread):
    def __init__(self, state_manager, server_key: str, disk_manager=None):
        super().__init__(daemon=True, name="dlm-heartbeat")
        self.state_manager = state_manager
        self.server_key = server_key
        self.disk_manager = disk_manager
        self.current_task_id = None
        self.stop_event = threading.Event()

    def run(self):
        while not self.stop_event.is_set():
            try:
                heartbeat = {
                    "alive_at": _now(),
                    "pid": os.getpid(),
                    "hostname": platform.node(),
                    "current_task": self.current_task_id,
                }
                if self.disk_manager:
                    heartbeat["disk_free_gb"] = round(self.disk_manager.available_gb(), 1)
                self.state_manager.update_heartbeat(self.server_key, heartbeat)
            except Exception as e:
                logger.warning(f"Heartbeat write failed: {e}")
            self.stop_event.wait(timeout=HEARTBEAT_INTERVAL)

    def stop(self):
        self.stop_event.set()
