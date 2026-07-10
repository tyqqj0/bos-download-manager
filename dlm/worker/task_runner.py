"""Task runner — orchestrates download + move for a single task."""

import json
import shutil
import subprocess
import logging
import threading
import time
from pathlib import Path

from ..constants import DATA_BUCKET
from .errors import TaskError, ErrorClass, classify_error
from .disk import DiskManager, STAGING_PATH

logger = logging.getLogger(__name__)

MODEL_BUCKET = "auwomo-model-open"

_CHUNK_THRESHOLD_RATIO = 0.5
_BATCH_DISK_RATIO = 0.6


class TaskRunner:
    """Orchestrates download + BOS upload for a single task.

    Args:
        task: Task object (or dataclass) with id, name, repo_id, source, type, bos_path, size_gb, category.
        server_key: This worker's server identifier.
        cancel_event: Threading event to signal cancellation (e.g. from SIGUSR1).
        progress_callback: Called with (downloaded_bytes, total_bytes, speed_bps) during download.
        move_progress_callback: Called with (uploaded_bytes, total_bytes) during BOS upload.
    """

    def __init__(self, task, server_key: str, cancel_event: threading.Event = None,
                 progress_callback=None, move_progress_callback=None):
        self.task = task
        self.server_key = server_key
        self.cancel_event = cancel_event or threading.Event()
        self.disk = DiskManager()
        self._progress_cb = progress_callback
        self._move_progress_cb = move_progress_callback

    def run(self):
        """Execute the full task pipeline."""
        handler = self._get_handler()
        mover = self._get_mover()

        handler.validate(self.task)

        real_size = handler.estimate_size(self.task)
        if real_size and (real_size > self.task.size_gb * 1.5 or self.task.size_gb == 0):
            logger.info(f"Correcting size_gb: {self.task.size_gb:.1f} -> {real_size:.1f}")
            self.task.size_gb = real_size

        est_size = real_size or self.task.size_gb
        avail_gb = self.disk.available_gb()

        need_chunked = False
        if est_size and est_size > avail_gb * _CHUNK_THRESHOLD_RATIO:
            need_chunked = True
            logger.info(f"Chunked mode: {est_size:.1f}GB dataset, {avail_gb:.1f}GB available")
        elif est_size == 0 and self.task.source == "hf":
            need_chunked = True
            logger.info("Chunked mode: size unknown, defaulting to chunked for safety")
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
        staging_dir = STAGING_PATH / self.task.name
        staging_dir.mkdir(parents=True, exist_ok=True)

        try:
            handler.download(
                self.task,
                staging_dir,
                progress_callback=self._progress_cb,
                cancel_event=self.cancel_event,
            )
        except TaskError:
            raise
        except Exception as e:
            error_class = classify_error(e)
            raise TaskError(str(e), error_class) from e

        try:
            mover.move(
                staging_dir,
                self.task,
                progress_callback=self._move_progress_cb,
                cancel_event=self.cancel_event,
            )
        except TaskError:
            raise
        except Exception as e:
            error_class = classify_error(e)
            raise TaskError(str(e), error_class) from e

        shutil.rmtree(staging_dir, ignore_errors=True)
        logger.info(f"Task {self.task.id} completed successfully")

    def _run_chunked(self, handler, mover, est_size_gb: float):
        """Chunked mode: download in batches, upload each batch, free space, repeat."""
        logger.info(f"Starting chunked download for {self.task.repo_id}")

        files = self._list_repo_files()
        if not files:
            raise TaskError(f"Could not list files for {self.task.repo_id}", ErrorClass.UNKNOWN)

        total_bytes = sum(f["size"] for f in files)

        real_gb = total_bytes / (1024 ** 3)
        if real_gb > self.task.size_gb:
            self.task.size_gb = real_gb

        logger.info(f"Chunked: {len(files)} files, {real_gb:.1f}GB total")

        # Resume support: load progress from BOS to skip already-uploaded files
        completed_files = self._load_progress()
        files_sorted = sorted(files, key=lambda f: f["size"], reverse=True)
        if completed_files:
            before = len(files_sorted)
            files_sorted = [f for f in files_sorted if f["path"] not in completed_files]
            skipped = before - len(files_sorted)
            skipped_bytes = sum(
                f["size"] for f in files if f["path"] in completed_files
            )
            logger.info(
                f"Resume: skipping {skipped} already-uploaded files "
                f"({skipped_bytes / 1024**3:.1f}GB)"
            )
            uploaded_bytes = skipped_bytes
        else:
            uploaded_bytes = 0

        remaining_files = list(files_sorted)
        batch_num = 0

        while remaining_files:
            if self.cancel_event.is_set():
                raise TaskError("Cancelled", ErrorClass.TRANSIENT)

            batch_num += 1
            avail_gb = self.disk.available_gb()

            if self.disk.pressure_level() != "ok":
                logger.info(f"Disk pressure before batch {batch_num}, running cleanup")
                self.disk.emergency_cleanup()
                avail_gb = self.disk.available_gb()
                if self.disk.pressure_level() == "critical":
                    raise TaskError(
                        f"Disk critically full even after cleanup ({avail_gb:.1f}GB free)",
                        ErrorClass.DISK,
                    )

            max_batch_bytes = int(avail_gb * _BATCH_DISK_RATIO * 1024 ** 3)

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
                f"Batch {batch_num}: {len(batch)} files, {batch_size / 1024**3:.1f}GB "
                f"(avail: {avail_gb:.1f}GB)"
            )

            staging_dir = STAGING_PATH / self.task.name
            staging_dir.mkdir(parents=True, exist_ok=True)

            self._download_batch(batch_files_list, staging_dir)

            try:
                mover.move(
                    staging_dir,
                    self.task,
                    progress_callback=self._move_progress_cb,
                    cancel_event=self.cancel_event,
                )
            except TaskError:
                raise
            except Exception as e:
                raise TaskError(str(e), classify_error(e)) from e

            # Save progress after successful batch upload
            completed_files.update(batch_files_list)
            self._save_progress(completed_files)

            uploaded_bytes += batch_size

            if self._progress_cb and total_bytes > 0:
                self._progress_cb(uploaded_bytes, total_bytes, 0)

            shutil.rmtree(staging_dir, ignore_errors=True)
            logger.info(f"Batch {batch_num} done, {uploaded_bytes / 1024**3:.1f}GB uploaded total")

        # All done — remove progress file
        self._clear_progress()
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
                    files.append({"path": item.rfilename, "size": item.size})
            return files
        except Exception as e:
            logger.error(f"Failed to list repo files: {e}")
            return []

    def _download_batch(self, file_paths: list, staging_dir: Path):
        """Download specific files from the repo."""
        import os

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
            "--max-workers", "32",
        ]
        cmd.extend(file_paths)

        env = os.environ.copy()
        env["HF_HUB_CACHE"] = "/tmp/hf_cache"
        env["HF_HUB_ENABLE_HF_TRANSFER"] = "1"
        if os.environ.get("HF_TOKEN"):
            env["HF_TOKEN"] = os.environ["HF_TOKEN"]

        logger.debug(f"Downloading {len(file_paths)} files")

        proc = subprocess.Popen(
            cmd, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True,
        )

        stall_timeout = max(1800, int((self.task.size_gb or 0) * 30 * 60 / max(self.task.size_gb, 1)))
        stall_timeout = min(stall_timeout, 5400)  # cap at 90 min
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
                if self._progress_cb:
                    speed_bps = delta / 30
                    self._progress_cb(current_size, int(self.task.size_gb * 1024**3) or current_size * 2, speed_bps)
            elif time.time() - last_progress_time > stall_timeout:
                logger.warning(f"Batch download stalled for {stall_timeout//60}min, killing")
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

    def _progress_key(self) -> tuple:
        """Return (bucket, key) for this task's progress file on BOS."""
        if self.task.type == "model":
            bucket = MODEL_BUCKET
            prefix = f"{self.task.name}/"
        else:
            bucket = DATA_BUCKET
            prefix = f"{self.task.category}/{self.task.name}/"
        return bucket, prefix + "_chunked_progress.json"

    def _load_progress(self) -> set:
        """Load set of already-uploaded file paths from BOS progress file."""
        try:
            from ..core.bos import create_bos_client
            from ..core.config import load_config
            config = load_config()
            client = create_bos_client(config["BAIDU_AK"], config["BAIDU_SK"], config["BOS_ENDPOINT"])
            bucket, key = self._progress_key()
            resp = client.get_object_as_string(bucket, key)
            data = json.loads(resp)
            return set(data) if isinstance(data, list) else set()
        except Exception:
            return set()

    def _save_progress(self, completed: set):
        """Save completed file paths to BOS progress file."""
        try:
            from ..core.bos import create_bos_client
            from ..core.config import load_config
            config = load_config()
            client = create_bos_client(config["BAIDU_AK"], config["BAIDU_SK"], config["BOS_ENDPOINT"])
            bucket, key = self._progress_key()
            data = json.dumps(sorted(completed)).encode()
            client.put_object(bucket, key, data, len(data))
        except Exception as e:
            logger.warning(f"Failed to save progress to BOS: {e}")

    def _clear_progress(self):
        """Remove progress file from BOS after task completes."""
        try:
            from ..core.bos import create_bos_client
            from ..core.config import load_config
            config = load_config()
            client = create_bos_client(config["BAIDU_AK"], config["BAIDU_SK"], config["BOS_ENDPOINT"])
            bucket, key = self._progress_key()
            client.delete_object(bucket, key)
        except Exception:
            pass

    def _dir_size(self, path: Path) -> int:
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
