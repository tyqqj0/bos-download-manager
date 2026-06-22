"""ModelScope download handler."""

import os
import time
import subprocess
import logging
from pathlib import Path
from threading import Event
from typing import Optional, Callable

from ..errors import TaskError, ErrorClass
from .base import DownloadHandler

logger = logging.getLogger(__name__)


class ModelScopeHandler(DownloadHandler):
    def validate(self, task) -> None:
        try:
            import modelscope  # noqa: F401
        except ImportError:
            raise TaskError("modelscope package not installed", ErrorClass.UNKNOWN)

    def estimate_size(self, task) -> Optional[float]:
        if task.size_gb and task.size_gb > 0:
            return task.size_gb
        return None

    def download(
        self,
        task,
        staging_dir: Path,
        progress_callback: Callable[[int, int, float], None],
        cancel_event: Event,
    ) -> None:
        cmd = [
            "modelscope", "download",
            "--model" if task.type == "model" else "--dataset",
            task.repo_id,
            "--local_dir", str(staging_dir),
        ]

        env = os.environ.copy()
        logger.info(f"Starting modelscope download: {' '.join(cmd)}")

        proc = subprocess.Popen(
            cmd, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True,
        )

        total_bytes = int((task.size_gb or 0) * 1024 ** 3) or 1
        last_size = 0
        last_time = time.time()

        while proc.poll() is None:
            if cancel_event.is_set():
                proc.terminate()
                proc.wait(timeout=10)
                raise TaskError("Download cancelled", ErrorClass.TRANSIENT)

            time.sleep(10)

            try:
                current_size = sum(
                    f.stat().st_size for f in staging_dir.rglob("*") if f.is_file()
                )
            except Exception:
                current_size = last_size

            now = time.time()
            elapsed = now - last_time
            speed = (current_size - last_size) / elapsed if elapsed > 0 else 0
            progress_callback(current_size, total_bytes, speed)
            last_size = current_size
            last_time = now

        exit_code = proc.returncode
        if exit_code != 0:
            output = proc.stdout.read() if proc.stdout else ""
            raise TaskError(
                f"modelscope download failed (exit {exit_code}): {output[:300]}",
                ErrorClass.UNKNOWN,
            )
