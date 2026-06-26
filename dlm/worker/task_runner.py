"""Task runner — orchestrates download + move for a single task."""

import shutil
import subprocess
import logging
import threading
import time
from pathlib import Path

from ..core.models import _now
from ..core.state import StateManager
from ..constants import DATA_BUCKET
from .errors import TaskError, ErrorClass, classify_error
from .disk import DiskManager, STAGING_PATH

logger = logging.getLogger(__name__)

MODEL_BUCKET = "auwomo-model-open"

# Throttle state updates to avoid BOS write storms
_MIN_UPDATE_INTERVAL = 30

# Chunked mode: use when dataset size > this fraction of available disk
_CHUNK_THRESHOLD_RATIO = 0.5
# Each batch should use at most this fraction of free disk
_BATCH_DISK_RATIO = 0.6


class TaskRunner:
    def __init__(self, task, state_manager: StateManager, server_key: str):
        self.task = task
        self.state_manager = state_manager
        self.server_key = server_key
        self.cancel_event = threading.Event()
        self.disk = DiskManager()
        self._last_update_time = 0

    def run(self):
        """Execute the full task pipeline."""
        handler = self._get_handler()
        mover = self._get_mover()

        # Phase 0: Validate
        self._update_phase("validating")
        handler.validate(self.task)

        # Auto-correct size_gb if estimate is way off
        real_size = handler.estimate_size(self.task)
        if real_size and (real_size > self.task.size_gb * 1.5 or self.task.size_gb == 0):
            logger.info(
                f"Correcting size_gb: {self.task.size_gb:.1f} -> {real_size:.1f}"
            )
            try:
                self.state_manager.update_task(self.task.id, {
                    "size_gb": round(real_size, 2)
                })
            except Exception:
                pass
            self.task.size_gb = real_size

        # Phase 1: Pre-flight — determine if we need chunked mode
        est_size = real_size or self.task.size_gb
        avail_gb = self.disk.available_gb()

        need_chunked = False
        if est_size and est_size > avail_gb * _CHUNK_THRESHOLD_RATIO:
            need_chunked = True
            logger.info(
                f"Chunked mode: {est_size:.1f}GB dataset, {avail_gb:.1f}GB available"
            )
        elif est_size == 0 and self.task.source == "hf":
            need_chunked = True
            logger.info(
                f"Chunked mode: size unknown, defaulting to chunked for safety"
            )
        elif est_size and est_size > 0:
            ok, reason = self.disk.preflight_check(est_size)
            if not ok:
                raise TaskError(reason, ErrorClass.DISK)

        if need_chunked and self.task.source == "hf":
            self._run_chunked(handler, mover, est_size)
        else:
            self._run_simple(handler, mover)

    def _run_simple(self, handler, mover):
        """Standard mode: download all → upload all."""
        # Download
        self._update_phase("downloading")
        staging_dir = STAGING_PATH / self.task.name
        staging_dir.mkdir(parents=True, exist_ok=True)

        try:
            handler.download(
                self.task,
                staging_dir,
                progress_callback=self._on_download_progress,
                cancel_event=self.cancel_event,
            )
        except TaskError:
            raise
        except Exception as e:
            error_class = classify_error(e)
            raise TaskError(str(e), error_class) from e

        # Move to BOS
        self._update_phase("moving")
        try:
            mover.move(
                staging_dir,
                self.task,
                progress_callback=self._on_move_progress,
                cancel_event=self.cancel_event,
            )
        except TaskError:
            raise
        except Exception as e:
            error_class = classify_error(e)
            raise TaskError(str(e), error_class) from e

        # Cleanup
        shutil.rmtree(staging_dir, ignore_errors=True)
        logger.info(f"Task {self.task.id} completed successfully")

    def _run_chunked(self, handler, mover, est_size_gb: float):
        """Chunked mode: download in batches, upload each batch, free space, repeat."""
        logger.info(f"Starting chunked download for {self.task.repo_id}")

        # List all files in the repo
        files = self._list_repo_files()
        if not files:
            raise TaskError(
                f"Could not list files for {self.task.repo_id}",
                ErrorClass.UNKNOWN,
            )

        total_bytes = sum(f["size"] for f in files)
        uploaded_bytes = 0

        # Update size_gb now that we know the real size
        real_gb = total_bytes / (1024 ** 3)
        if real_gb > self.task.size_gb:
            try:
                self.state_manager.update_task(self.task.id, {"size_gb": round(real_gb, 2)})
            except Exception:
                pass
            self.task.size_gb = real_gb

        logger.info(f"Chunked: {len(files)} files, {real_gb:.1f}GB total")

        # Sort files largest first for better bin-packing
        files_sorted = sorted(files, key=lambda f: f["size"], reverse=True)
        remaining_files = list(files_sorted)
        batch_num = 0

        while remaining_files:
            if self.cancel_event.is_set():
                raise TaskError("Cancelled", ErrorClass.TRANSIENT)

            batch_num += 1

            # Recalculate available space each iteration
            avail_gb = self.disk.available_gb()
            max_batch_bytes = int(avail_gb * _BATCH_DISK_RATIO * 1024 ** 3)

            # Build this batch from remaining files
            batch = []
            batch_size = 0
            leftover = []
            for f in remaining_files:
                if batch_size + f["size"] <= max_batch_bytes or not batch:
                    batch.append(f)
                    batch_size += f["size"]
                else:
                    leftover.append(f)
            remaining_files = leftover

            batch_files_list = [f["path"] for f in batch]

            logger.info(
                f"Batch {batch_num}: "
                f"{len(batch)} files, {batch_size / 1024**3:.1f}GB "
                f"(avail: {avail_gb:.1f}GB)"
            )

            # Update progress
            pct = (uploaded_bytes / total_bytes * 100) if total_bytes > 0 else 0
            self._throttled_update({
                "progress_pct": round(pct, 1),
                "phase": f"downloading batch {batch_num}",
                "worker_heartbeat": _now(),
            })

            # Download this batch
            staging_dir = STAGING_PATH / self.task.name
            staging_dir.mkdir(parents=True, exist_ok=True)

            self._download_batch(batch_files_list, staging_dir)

            # Upload this batch to BOS
            self._throttled_update({
                "phase": f"uploading batch {batch_num}",
                "worker_heartbeat": _now(),
            })

            try:
                mover.move(
                    staging_dir,
                    self.task,
                    progress_callback=self._on_move_progress,
                    cancel_event=self.cancel_event,
                )
            except TaskError:
                raise
            except Exception as e:
                raise TaskError(str(e), classify_error(e)) from e

            uploaded_bytes += batch_size

            # Update downloaded_gb so totals reflect progress between batches
            pct = (uploaded_bytes / total_bytes * 100) if total_bytes > 0 else 0
            try:
                self.state_manager.update_task(self.task.id, {
                    "downloaded_gb": round(uploaded_bytes / 1024**3, 2),
                    "progress_pct": round(pct, 1),
                    "worker_heartbeat": _now(),
                })
            except Exception:
                pass

            # Clean staging for next batch
            shutil.rmtree(staging_dir, ignore_errors=True)
            logger.info(f"Batch {batch_num} done, {uploaded_bytes / 1024**3:.1f}GB uploaded total")

        logger.info(f"Task {self.task.id} chunked download completed: {batch_num} batches")

    def _list_repo_files(self) -> list:
        """List all files in the HF repo with sizes."""
        try:
            from huggingface_hub import HfApi
            import os
            api = HfApi(token=os.environ.get("HF_TOKEN"))
            repo_type = "dataset" if self.task.type == "dataset" else "model"

            files = []
            for item in api.list_repo_tree(
                self.task.repo_id, repo_type=repo_type, recursive=True
            ):
                if hasattr(item, "size") and item.size and hasattr(item, "rfilename"):
                    files.append({
                        "path": item.rfilename,
                        "size": item.size,
                    })
            return files
        except Exception as e:
            logger.error(f"Failed to list repo files: {e}")
            return []

    def _make_batches(self, files: list) -> list:
        """Split files into batches that each fit in available disk."""
        avail = self.disk.available_gb()
        max_batch_gb = avail * _BATCH_DISK_RATIO
        max_batch_bytes = int(max_batch_gb * 1024 ** 3)

        # Sort files largest first for better bin-packing
        files_sorted = sorted(files, key=lambda f: f["size"], reverse=True)

        batches = []
        current_batch = []
        current_size = 0

        for f in files_sorted:
            if current_size + f["size"] > max_batch_bytes and current_batch:
                batches.append(current_batch)
                current_batch = []
                current_size = 0
            current_batch.append(f)
            current_size += f["size"]

        if current_batch:
            batches.append(current_batch)

        return batches

    def _download_batch(self, file_paths: list, staging_dir: Path):
        """Download specific files from the repo."""
        import os

        # Split into sub-batches if file list is too long (avoid Errno 7: arg list too long)
        MAX_ARGS = 500
        if len(file_paths) > MAX_ARGS:
            for i in range(0, len(file_paths), MAX_ARGS):
                chunk = file_paths[i:i + MAX_ARGS]
                self._download_batch(chunk, staging_dir)
            return

        cmd = [
            "hf", "download", self.task.repo_id,
            "--local-dir", str(staging_dir),
            "--repo-type", self.task.type,
        ]
        cmd.extend(file_paths)

        env = os.environ.copy()
        env["HF_HUB_CACHE"] = "/tmp/hf_cache"
        if os.environ.get("HF_TOKEN"):
            env["HF_TOKEN"] = os.environ["HF_TOKEN"]

        logger.debug(f"Downloading {len(file_paths)} files")

        proc = subprocess.Popen(
            cmd, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True,
        )

        stall_timeout = 1800  # 30min with no new bytes = stalled
        last_progress_time = time.time()
        last_staging_size = self._dir_size(staging_dir)

        while proc.poll() is None:
            if self.cancel_event.is_set():
                proc.terminate()
                proc.wait(timeout=10)
                raise TaskError("Download cancelled", ErrorClass.TRANSIENT)

            time.sleep(30)

            current_size = self._dir_size(staging_dir)
            if current_size > last_staging_size:
                delta = current_size - last_staging_size
                last_staging_size = current_size
                last_progress_time = time.time()
                self._throttled_update({
                    "downloaded_gb": round(current_size / (1024 ** 3), 2),
                    "speed_mbps": round(delta / (30 * 1024 * 1024), 1),
                    "worker_heartbeat": _now(),
                })
            elif time.time() - last_progress_time > stall_timeout:
                logger.warning(
                    f"Batch download stalled for {stall_timeout//60}min, killing"
                )
                proc.terminate()
                proc.wait(timeout=10)
                raise TaskError(
                    f"Download stalled: no progress for {stall_timeout//60}min",
                    ErrorClass.TRANSIENT,
                )

        if proc.returncode != 0:
            output = proc.stdout.read() if proc.stdout else ""
            output_lower = output.lower()
            if "gated" in output_lower or "access" in output_lower:
                raise TaskError(f"Gated repo: {self.task.repo_id}", ErrorClass.AUTH)
            if "not found" in output_lower or "404" in output_lower:
                raise TaskError(f"Repo not found: {self.task.repo_id}", ErrorClass.NOT_FOUND)
            if "no space" in output_lower:
                raise TaskError("Disk full during batch download", ErrorClass.DISK)
            raise TaskError(
                f"Batch download failed (exit {proc.returncode}): {output[:300]}",
                ErrorClass.TRANSIENT,
            )

    def cancel(self):
        self.cancel_event.set()

    def _update_phase(self, phase: str):
        try:
            self.state_manager.update_task(self.task.id, {
                "phase": phase,
                "worker_heartbeat": _now(),
            })
        except Exception as e:
            logger.warning(f"Failed to update phase to {phase}: {e}")

    def _throttled_update(self, updates: dict):
        """Update state, throttled to avoid write storms."""
        now = time.time()
        if now - self._last_update_time < _MIN_UPDATE_INTERVAL:
            return
        self._last_update_time = now
        try:
            self.state_manager.update_task(self.task.id, updates)
        except Exception as e:
            logger.debug(f"Throttled update failed: {e}")

    def _on_download_progress(self, downloaded_bytes: int, total_bytes: int, speed_bps: float):
        """Called by handler periodically."""
        pressure = self.disk.pressure_level()
        if pressure == "critical":
            self.cancel_event.set()
            raise TaskError("Disk critically full during download", ErrorClass.DISK)

        now = time.time()
        if now - self._last_update_time < _MIN_UPDATE_INTERVAL:
            return
        self._last_update_time = now

        pct = (downloaded_bytes / total_bytes * 100) if total_bytes > 0 else 0
        pct = min(pct, 100.0)
        speed_mbps = speed_bps / (1024 * 1024) if speed_bps > 0 else 0
        eta = int((total_bytes - downloaded_bytes) / speed_bps) if speed_bps > 0 and total_bytes > downloaded_bytes else None

        updates = {
            "progress_pct": round(pct, 1),
            "speed_mbps": round(speed_mbps, 1),
            "downloaded_gb": round(downloaded_bytes / (1024 ** 3), 2),
            "eta_seconds": eta,
            "worker_heartbeat": _now(),
        }
        if total_bytes > 0:
            actual_gb = total_bytes / (1024 ** 3)
            if actual_gb > self.task.size_gb:
                updates["size_gb"] = round(actual_gb, 2)

        try:
            self.state_manager.update_task(self.task.id, updates)
        except Exception as e:
            logger.debug(f"Progress update failed: {e}")

    def _on_move_progress(self, uploaded_bytes: int, total_bytes: int):
        """Called by mover periodically."""
        now = time.time()
        if now - self._last_update_time < _MIN_UPDATE_INTERVAL:
            return
        self._last_update_time = now

        pct = (uploaded_bytes / total_bytes * 100) if total_bytes > 0 else 0
        pct = min(pct, 100.0)
        try:
            self.state_manager.update_task(self.task.id, {
                "progress_pct": round(pct, 1),
                "phase": "moving",
                "worker_heartbeat": _now(),
            })
        except Exception as e:
            logger.debug(f"Move progress update failed: {e}")

    def _dir_size(self, path: Path) -> int:
        """Fast size check using du (Linux) for stall detection."""
        try:
            result = subprocess.run(
                ["du", "-sb", str(path)],
                capture_output=True, text=True, timeout=10,
            )
            return int(result.stdout.split()[0]) if result.returncode == 0 else 0
        except Exception:
            return 0

    def _get_handler(self):
        from .handlers import get_handler
        return get_handler(self.task.source)

    def _get_mover(self):
        from .movers import get_mover
        return get_mover()
