# DEPRECATED: This was the Celery-based download task.
# The current system uses Temporal workflows (see dlm/temporal/).
# Kept for reference only — do not run in production.

"""Celery task: download a dataset and upload to BOS.

Wraps the existing TaskRunner with Celery lifecycle management:
- Progress reporting via update_state()
- Automatic retry with exponential backoff
- Graceful interrupt via SoftTimeLimitExceeded
- Staging directory preserved on interrupt for resume
"""

import logging
import os
import signal
import socket
import threading
import time
from pathlib import Path

from celery import states
from celery.exceptions import SoftTimeLimitExceeded

from ..queue.app import app
from ..queue import snapshot
from .errors import TaskError, ErrorClass

logger = logging.getLogger(__name__)


class _ProgressReporter:
    """Adapts TaskRunner's progress callbacks to Celery update_state."""

    def __init__(self, celery_task, task_id: str):
        self.celery_task = celery_task
        self.task_id = task_id
        self._last_snapshot_time = 0
        self._snapshot_interval = 15

    def on_download_progress(self, downloaded_bytes: int, total_bytes: int, speed_bps: float):
        pct = (downloaded_bytes / total_bytes * 100) if total_bytes > 0 else 0
        speed_mbps = speed_bps / (1024 * 1024) if speed_bps > 0 else 0
        downloaded_gb = downloaded_bytes / (1024 ** 3)

        self.celery_task.update_state(state="PROGRESS", meta={
            "progress_pct": round(min(pct, 100), 1),
            "speed_mbps": round(speed_mbps, 1),
            "downloaded_gb": round(downloaded_gb, 2),
            "phase": "downloading",
        })

        now = time.time()
        if now - self._last_snapshot_time > self._snapshot_interval:
            self._last_snapshot_time = now
            try:
                snapshot.update_task_progress(
                    self.task_id,
                    progress_pct=round(min(pct, 100), 1),
                    speed_mbps=round(speed_mbps, 1),
                    downloaded_gb=round(downloaded_gb, 2),
                    phase="downloading",
                    status="downloading",
                )
            except Exception as e:
                logger.debug(f"Snapshot update failed: {e}")

    def on_move_progress(self, uploaded_bytes: int, total_bytes: int):
        pct = (uploaded_bytes / total_bytes * 100) if total_bytes > 0 else 0

        self.celery_task.update_state(state="PROGRESS", meta={
            "progress_pct": round(min(pct, 100), 1),
            "phase": "uploading",
        })

        now = time.time()
        if now - self._last_snapshot_time > self._snapshot_interval:
            self._last_snapshot_time = now
            try:
                snapshot.update_task_progress(
                    self.task_id,
                    progress_pct=round(min(pct, 100), 1),
                    phase="uploading",
                    status="downloading",
                )
            except Exception as e:
                logger.debug(f"Snapshot update failed: {e}")


@app.task(
    bind=True,
    name="dlm.worker.download.download_dataset",
    max_retries=8,
    retry_backoff=True,
    retry_backoff_max=3600,
    retry_jitter=True,
)
def download_dataset(self, task_meta: dict):
    """Download a dataset and upload to BOS.

    Args:
        task_meta: Task metadata dict with keys:
            id, name, repo_id, source, type, category, bos_path, size_gb, priority, ...
    """
    from .task_runner import TaskRunner
    from .disk import DiskManager
    from ..core.models import Task

    task_id = task_meta["id"]
    task_name = task_meta["name"]
    server_key = os.environ.get("DLM_SERVER_KEY", socket.gethostname())

    logger.info(f"Starting download: {task_name} (id={task_id}, attempt={self.request.retries + 1})")

    # Dedup guard: check if another worker is already running this task
    try:
        inspect = app.control.inspect(timeout=3)
        active = inspect.active() or {}
        for worker_name, tasks in active.items():
            if server_key in worker_name:
                continue
            for t in tasks:
                args = t.get("args", [])
                other_id = args[0].get("id") if args and isinstance(args[0], dict) else t.get("id")
                if other_id == task_id:
                    logger.warning(f"Dedup: {task_name} already running on {worker_name}, skipping")
                    return {"status": "dedup_skipped", "task_id": task_id}
    except Exception as e:
        logger.debug(f"Dedup check failed (continuing): {e}")

    snapshot.init_db()
    snapshot.update_task_progress(
        task_id, status="downloading", phase="starting",
        server=server_key, progress_pct=0, speed_mbps=0,
    )
    snapshot.update_worker(
        hostname=socket.gethostname(),
        server_key=server_key,
        status="busy",
        current_task_id=task_id,
    )

    reporter = _ProgressReporter(self, task_id)
    cancel_event = threading.Event()

    def _handle_soft_timeout(signum, frame):
        logger.warning(f"Soft timeout received for {task_name}, cancelling...")
        cancel_event.set()

    old_handler = signal.signal(signal.SIGUSR1, _handle_soft_timeout)

    # Signal watcher: polls Redis every 5s for pause/preempt signals
    stop_watcher = threading.Event()

    def _signal_watcher():
        from ..queue.signals import check_signal
        while not stop_watcher.is_set():
            try:
                sig = check_signal(task_id)
                if sig:
                    logger.info(f"Signal '{sig}' received for task {task_name}")
                    cancel_event.set()
                    return
            except Exception:
                pass
            stop_watcher.wait(5)

    watcher = threading.Thread(target=_signal_watcher, daemon=True)
    watcher.start()

    try:
        task_obj = Task(**{k: v for k, v in task_meta.items() if k in Task.__dataclass_fields__})
        runner = TaskRunner(task_obj, server_key=server_key, cancel_event=cancel_event,
                           progress_callback=reporter.on_download_progress,
                           move_progress_callback=reporter.on_move_progress)
        runner.run()

        snapshot.update_task_progress(
            task_id, status="done", phase=None,
            progress_pct=100, speed_mbps=0,
        )
        snapshot.complete_task(task_id, status="done")
        snapshot.update_worker(
            hostname=socket.gethostname(),
            server_key=server_key,
            status="idle",
            current_task_id=None,
        )

        logger.info(f"Download completed: {task_name}")
        return {"status": "done", "task_id": task_id}

    except SoftTimeLimitExceeded:
        cancel_event.set()
        logger.warning(f"Task {task_name} hit soft time limit, will retry")
        snapshot.update_task_progress(task_id, status="pending", phase="interrupted")
        raise self.retry(countdown=600)

    except TaskError as e:
        # Check if this was triggered by a pause/preempt signal
        from ..queue.signals import check_signal, signal_clear
        sig = check_signal(task_id)
        if sig:
            signal_clear(task_id)
            status = "preempted" if sig == "preempt" else "paused"
            snapshot.update_task_progress(
                task_id, status=status, phase=None, speed_mbps=0,
            )
            logger.info(f"Task {task_name} {status} (reason={sig})")
            return {"status": status, "task_id": task_id}

        logger.error(f"Task {task_name} failed: {e} (class={e.classification})")
        snapshot.update_task_progress(
            task_id, status="failed", phase=None,
            error=str(e), error_class=e.classification,
            speed_mbps=0,
        )

        if e.classification in (ErrorClass.AUTH.value, ErrorClass.NOT_FOUND.value):
            snapshot.complete_task(task_id, status="failed")
            return {"status": "failed", "task_id": task_id, "error": str(e), "retryable": False}

        retry_delay = min(60 * (2 ** self.request.retries), 3600)
        snapshot.update_task_progress(task_id, status="pending", phase="waiting_retry")
        raise self.retry(exc=e, countdown=retry_delay)

    except Exception as e:
        logger.exception(f"Unexpected error in task {task_name}")
        snapshot.update_task_progress(
            task_id, status="failed", error=str(e), error_class="unknown",
            phase=None, speed_mbps=0,
        )
        retry_delay = min(120 * (2 ** self.request.retries), 3600)
        raise self.retry(exc=e, countdown=retry_delay)

    finally:
        stop_watcher.set()
        signal.signal(signal.SIGUSR1, old_handler)
        snapshot.update_worker(
            hostname=socket.gethostname(),
            server_key=server_key,
            status="idle",
            current_task_id=None,
        )
