"""DLM Worker Daemon — main loop that polls BOS state for tasks."""

import os
import signal
import logging
import time
import threading

from ..core.models import _now
from ..core.state import StateManager
from ..constants import PRIORITIES
from .errors import TaskError, classify_error
from .heartbeat import HeartbeatThread
from .disk import DiskManager
from .task_runner import TaskRunner

logger = logging.getLogger(__name__)

POLL_INTERVAL = 10  # seconds between task polls
RETRY_COOLDOWN = 60  # seconds to wait after a failure before polling again


class WorkerDaemon:
    def __init__(self, server_key: str):
        self.server_key = server_key
        self.state_manager = StateManager.create()
        self.disk = DiskManager()
        self.heartbeat = HeartbeatThread(self.state_manager, server_key, self.disk)
        self.current_runner: TaskRunner = None
        self.shutdown_event = threading.Event()

    def run(self):
        """Main daemon loop."""
        logger.info(f"DLM Worker starting on {self.server_key} (pid={os.getpid()})")
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

        self.heartbeat.start()

        # Startup cleanup: remove stale staging dirs
        try:
            removed = self.disk.cleanup_stale()
            if removed:
                logger.info(f"Cleaned {len(removed)} stale staging dirs: {removed}")
        except Exception as e:
            logger.warning(f"Startup cleanup failed: {e}")

        # Recover zombie tasks left in "downloading" by previous daemon instance
        self._recover_zombie_tasks()

        while not self.shutdown_event.is_set():
            try:
                task = self._poll_for_task()
                if task:
                    self._execute_task(task)
                else:
                    self._maintenance()
                    self.shutdown_event.wait(timeout=POLL_INTERVAL)
            except Exception as e:
                logger.error(f"Main loop error: {e}", exc_info=True)
                self.shutdown_event.wait(timeout=RETRY_COOLDOWN)

        self.heartbeat.stop()
        logger.info("Worker daemon stopped")

    def _poll_for_task(self):
        """Check BOS state for tasks assigned to this server."""
        pressure = self.disk.pressure_level()
        if pressure != "ok":
            logger.debug(f"Skipping poll, disk pressure={pressure}")
            return None

        state = self.state_manager.load(use_cache=False)
        candidates = [
            t for t in state.tasks
            if t.server == self.server_key
            and t.status in ("dispatched",)
        ]
        if not candidates:
            return self._try_steal_task()

        # Sort by priority (P0 first) then creation time
        priority_order = {p: i for i, p in enumerate(PRIORITIES)}
        candidates.sort(key=lambda t: (
            priority_order.get(t.priority, 99),
            t.created_at or "",
        ))
        return candidates[0]

    def _execute_task(self, task):
        """Claim and execute a single task."""
        logger.info(f"Claiming task {task.id}: {task.repo_id}")

        # Claim the task
        try:
            self.state_manager.update_task(task.id, {
                "status": "downloading",
                "phase": "starting",
                "worker_pid": os.getpid(),
                "worker_heartbeat": _now(),
                "error": None,
                "error_class": None,
            })
        except Exception as e:
            logger.error(f"Failed to claim task {task.id}: {e}")
            return

        self.heartbeat.current_task_id = task.id
        runner = TaskRunner(task, self.state_manager, self.server_key)
        self.current_runner = runner

        try:
            runner.run()
            # Success — set downloaded_gb to size_gb so totals are accurate
            self.state_manager.update_task(task.id, {
                "status": "done",
                "phase": None,
                "completed_at": _now(),
                "progress_pct": 100.0,
                "speed_mbps": 0,
                "eta_seconds": None,
                "downloaded_gb": round(task.size_gb, 2) if task.size_gb else None,
            })
            logger.info(f"Task {task.id} completed: {task.repo_id}")

        except TaskError as e:
            logger.warning(f"Task {task.id} failed ({e.classification}): {e}")
            updates = {
                "error": str(e),
                "error_class": e.classification,
                "phase": None,
                "speed_mbps": 0,
                "eta_seconds": None,
            }
            if e.should_retry(task.retry_count):
                updates["status"] = "dispatched"
                updates["retry_count"] = task.retry_count + 1
                delay = min(e.retry_delay(task.retry_count), 300)
                updates["retry_after"] = _now()  # Mark when failure happened
                logger.info(f"Will retry task {task.id} (attempt {task.retry_count + 1}, after {delay}s)")
            else:
                if e.classification == "auth":
                    updates["status"] = "needs-auth"
                else:
                    updates["status"] = "failed"
            self.state_manager.update_task(task.id, updates)

        except Exception as e:
            logger.error(f"Task {task.id} unexpected error: {e}", exc_info=True)
            error_class = classify_error(e)
            self.state_manager.update_task(task.id, {
                "status": "failed",
                "error": str(e),
                "error_class": error_class.value,
                "phase": None,
            })

        finally:
            self.current_runner = None
            self.heartbeat.current_task_id = None

    def _recover_zombie_tasks(self):
        """Reset tasks stuck in 'downloading' from a previous daemon instance."""
        try:
            state = self.state_manager.load(use_cache=False)
            for t in state.tasks:
                if (t.server == self.server_key
                        and t.status == "downloading"
                        and t.worker_pid != os.getpid()):
                    logger.warning(f"Recovering zombie task {t.id} ({t.name})")
                    self.state_manager.update_task(t.id, {
                        "status": "dispatched",
                        "phase": None,
                        "speed_mbps": 0,
                        "eta_seconds": None,
                        "worker_pid": None,
                    })
        except Exception as e:
            logger.warning(f"Zombie recovery failed: {e}")

    def _try_steal_task(self):
        """When idle, steal a dispatched task from an overloaded worker."""
        state = self.state_manager.load(use_cache=False)

        # Build: server → list of dispatched tasks
        load_map = {}
        for t in state.tasks:
            if t.status == "dispatched" and t.server and t.server != self.server_key:
                load_map.setdefault(t.server, []).append(t)

        # Only steal from servers with queue > 1 (leave them at least 1 task)
        for server_key, tasks in sorted(load_map.items(), key=lambda x: -len(x[1])):
            if len(tasks) <= 1:
                break

            # Steal the lowest-priority / latest-created task
            priority_order = {"P0": 0, "P1": 1, "P2": 2, "P3": 3}
            steal_target = sorted(tasks, key=lambda t: (
                priority_order.get(t.priority, 9),
                t.created_at or "",
            ))[-1]

            logger.info(
                f"Work-stealing: taking {steal_target.id} ({steal_target.name}) "
                f"from {server_key} (queue={len(tasks)})"
            )
            self.state_manager.update_task(steal_target.id, {
                "server": self.server_key,
            })
            return steal_target

        return None

    def _handle_signal(self, signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        self.shutdown_event.set()
        if self.current_runner:
            self.current_runner.cancel()

    def _maintenance(self):
        """Self-healing when idle: cleanup disk, retry disk-failed tasks."""
        pressure = self.disk.pressure_level()

        if pressure in ("warning", "critical"):
            logger.info(f"Disk pressure={pressure}, running emergency cleanup")
            freed = self.disk.emergency_cleanup()
            logger.info(f"Freed {freed:.1f}GB")

        if self.disk.pressure_level() == "ok":
            try:
                state = self.state_manager.load(use_cache=False)
                for t in state.tasks:
                    if (t.server == self.server_key
                            and t.status == "failed"
                            and t.error_class == "disk"
                            and t.retry_count < 3):
                        logger.info(f"Auto-retrying disk-failed task: {t.name}")
                        self.state_manager.update_task(t.id, {
                            "status": "dispatched",
                            "error": None,
                            "error_class": None,
                            "retry_count": t.retry_count + 1,
                        })
                        break
            except Exception as e:
                logger.debug(f"Maintenance check failed: {e}")


def main():
    """Entry point for the worker daemon."""
    import argparse

    parser = argparse.ArgumentParser(description="DLM Worker Daemon")
    parser.add_argument("--server-key", required=True, help="This server's key (e.g. w1, w2)")
    parser.add_argument("--log-level", default="INFO", help="Log level")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    daemon = WorkerDaemon(server_key=args.server_key)
    daemon.run()


if __name__ == "__main__":
    main()
