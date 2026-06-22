"""HuggingFace download handler using huggingface_hub."""

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


class HuggingFaceHandler(DownloadHandler):
    def validate(self, task) -> None:
        token = os.environ.get("HF_TOKEN", "")
        if not token:
            logger.warning("HF_TOKEN not set — gated repos will fail")

    def estimate_size(self, task) -> Optional[float]:
        """Try to get repo size from HuggingFace API."""
        if task.size_gb and task.size_gb > 0:
            return task.size_gb
        try:
            from huggingface_hub import HfApi
            api = HfApi()
            if task.type == "model":
                info = api.model_info(task.repo_id)
            else:
                info = api.dataset_info(task.repo_id)
            if hasattr(info, "siblings") and info.siblings:
                total = sum(s.size or 0 for s in info.siblings)
                return total / (1024 ** 3)
        except Exception as e:
            logger.debug(f"Could not estimate size for {task.repo_id}: {e}")
        return None

    def download(
        self,
        task,
        staging_dir: Path,
        progress_callback: Callable[[int, int, float], None],
        cancel_event: Event,
    ) -> None:
        """Download using hf CLI (subprocess) with progress monitoring."""
        cmd = [
            "hf", "download", task.repo_id,
            "--local-dir", str(staging_dir),
            "--repo-type", task.type,
        ]
        if task.include:
            cmd.extend(["--include", task.include])

        env = os.environ.copy()
        env["HF_HUB_CACHE"] = "/tmp/hf_cache"
        if os.environ.get("HF_TOKEN"):
            env["HF_TOKEN"] = os.environ["HF_TOKEN"]

        logger.info(f"Starting download: {' '.join(cmd)}")

        proc = subprocess.Popen(
            cmd, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True,
        )

        # Monitor download progress by checking staging dir size
        total_bytes = int((task.size_gb or 0) * 1024 ** 3) or 1
        last_size = 0
        last_time = time.time()
        last_progress_time = time.time()
        stall_timeout = 1800  # 30min with no new bytes = stalled

        while proc.poll() is None:
            if cancel_event.is_set():
                proc.terminate()
                proc.wait(timeout=10)
                raise TaskError("Download cancelled", ErrorClass.TRANSIENT)

            time.sleep(30)

            # Measure current staging size
            try:
                current_size = sum(
                    f.stat().st_size for f in staging_dir.rglob("*") if f.is_file()
                )
            except Exception:
                current_size = last_size

            now = time.time()
            elapsed = now - last_time
            speed = (current_size - last_size) / elapsed if elapsed > 0 else 0

            # Stall detection
            if current_size > last_size:
                last_progress_time = now
            elif now - last_progress_time > stall_timeout:
                logger.warning(
                    f"Download stalled for {stall_timeout//60}min, killing process"
                )
                proc.terminate()
                proc.wait(timeout=10)
                raise TaskError(
                    f"Download stalled: no progress for {stall_timeout//60}min",
                    ErrorClass.TRANSIENT,
                )

            progress_callback(current_size, total_bytes, speed)

            last_size = current_size
            last_time = now

        exit_code = proc.returncode
        if exit_code != 0:
            output = proc.stdout.read() if proc.stdout else ""
            self._handle_error(exit_code, output, task)

    def _handle_error(self, exit_code: int, output: str, task):
        """Classify the hf download error and raise appropriate TaskError."""
        output_lower = output.lower() if output else ""

        if "gated" in output_lower or "access" in output_lower:
            raise TaskError(
                f"Repository {task.repo_id} is gated/restricted",
                ErrorClass.AUTH,
            )
        if "not found" in output_lower or "404" in output_lower:
            raise TaskError(
                f"Repository {task.repo_id} not found",
                ErrorClass.NOT_FOUND,
            )
        if "no space" in output_lower:
            raise TaskError("Disk full during download", ErrorClass.DISK)
        if "connection" in output_lower or "timeout" in output_lower:
            raise TaskError(
                f"Network error downloading {task.repo_id}: {output[:200]}",
                ErrorClass.TRANSIENT,
            )
        raise TaskError(
            f"hf download failed (exit {exit_code}): {output[:300]}",
            ErrorClass.UNKNOWN,
        )
