"""Rsync-to-FUSE mover — legacy fallback when BOS SDK is unavailable."""

import subprocess
import time
import logging
from pathlib import Path
from threading import Event
from typing import Callable

from ..errors import TaskError, ErrorClass
from .base import Mover

logger = logging.getLogger(__name__)

DATA_MOUNT = Path("/mnt/auwomo-data")
MODEL_MOUNT = Path("/mnt/auwomo-model")


class RsyncFuseMover(Mover):
    def move(
        self,
        source_dir: Path,
        task,
        progress_callback: Callable[[int, int], None],
        cancel_event: Event,
    ) -> None:
        target = self._resolve_target(task)
        target.mkdir(parents=True, exist_ok=True)

        # Calculate total size
        total_bytes = sum(
            f.stat().st_size for f in source_dir.rglob("*") if f.is_file()
        )
        if total_bytes == 0:
            logger.warning(f"No files to move in {source_dir}")
            return

        logger.info(f"rsync {source_dir} → {target} ({total_bytes / 1024**3:.1f}GB)")

        cmd = [
            "rsync", "-a", "--remove-source-files",
            "--exclude=*.incomplete",
            "--exclude=.huggingface/",
            "--exclude=.cache/",
            str(source_dir) + "/",
            str(target) + "/",
        ]

        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        while proc.poll() is None:
            if cancel_event.is_set():
                proc.terminate()
                proc.wait(timeout=10)
                raise TaskError("Rsync cancelled", ErrorClass.TRANSIENT)
            time.sleep(10)
            # Estimate progress by checking how much has been removed from source
            try:
                remaining = sum(
                    f.stat().st_size for f in source_dir.rglob("*") if f.is_file()
                )
                uploaded = total_bytes - remaining
                progress_callback(uploaded, total_bytes)
            except Exception:
                pass

        if proc.returncode != 0:
            stderr = proc.stderr.read().decode() if proc.stderr else ""
            if "no space" in stderr.lower():
                raise TaskError("FUSE mount full", ErrorClass.DISK)
            raise TaskError(f"rsync failed (exit {proc.returncode}): {stderr[:200]}", ErrorClass.TRANSIENT)

        progress_callback(total_bytes, total_bytes)

    def _resolve_target(self, task) -> Path:
        if task.type == "model":
            return MODEL_MOUNT / task.name
        return DATA_MOUNT / task.category / task.name
