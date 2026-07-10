"""Pipeline engine — concurrent download + upload with disk backpressure.

Architecture:
  Download threads → [disk buffer] → Upload threads
                         ↕
                    Disk Monitor
                    (>80% → pause downloads)
                    (<50% → resume downloads)
"""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import time
from pathlib import Path
from typing import Callable, Optional

from .models import TaskInput, FileInfo, PipelineStats

logger = logging.getLogger(__name__)

STAGING_PATH = Path("/data/staging")
DISK_PAUSE_THRESHOLD = 0.80   # pause downloads at 80% disk usage
DISK_RESUME_THRESHOLD = 0.50  # resume downloads at 50% disk usage
DOWNLOAD_CONCURRENCY = 4      # parallel download tasks
UPLOAD_CONCURRENCY = 8        # parallel upload tasks


def _disk_usage_pct() -> float:
    stat = shutil.disk_usage(STAGING_PATH)
    return stat.used / stat.total


def _disk_free_gb() -> float:
    stat = shutil.disk_usage(STAGING_PATH)
    return stat.free / (1024 ** 3)


class PipelineEngine:
    """Runs download+upload pipeline with backpressure."""

    def __init__(
        self,
        task_input: TaskInput,
        staging_dir: Path,
        heartbeat_fn: Callable[[str], None],
    ):
        self.task = task_input
        self.staging_dir = staging_dir
        self.heartbeat_fn = heartbeat_fn
        self.stats = PipelineStats()
        self._cancel = False
        self._download_paused = asyncio.Event()
        self._download_paused.set()  # not paused initially

    async def run(self, files: list[FileInfo]) -> PipelineStats:
        """Execute the pipeline. Returns final stats."""
        self.stats.total_files = len(files)
        self.stats.total_bytes = sum(f.size for f in files)
        self.stats.phase = "downloading"

        upload_queue: asyncio.Queue = asyncio.Queue(maxsize=100)

        # Start disk monitor
        monitor_task = asyncio.create_task(self._disk_monitor())

        # Start uploaders
        upload_tasks = [
            asyncio.create_task(self._uploader(upload_queue, i))
            for i in range(UPLOAD_CONCURRENCY)
        ]

        # Start downloaders
        file_queue: asyncio.Queue = asyncio.Queue()
        for f in files:
            await file_queue.put(f)

        download_tasks = [
            asyncio.create_task(self._downloader(file_queue, upload_queue, i))
            for i in range(DOWNLOAD_CONCURRENCY)
        ]

        # Wait for all downloads to finish
        await asyncio.gather(*download_tasks)

        # Signal uploaders to stop
        for _ in upload_tasks:
            await upload_queue.put(None)

        # Wait for all uploads to finish
        self.stats.phase = "uploading_remaining"
        await asyncio.gather(*upload_tasks)

        # Stop monitor
        self._cancel = True
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass

        self.stats.phase = "done"
        return self.stats

    async def _downloader(self, file_queue: asyncio.Queue,
                          upload_queue: asyncio.Queue, worker_id: int):
        """Download files from queue, respecting backpressure."""
        from .downloader import download_file

        while True:
            try:
                file_info = file_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            # Backpressure: wait if disk is too full
            while not self._download_paused.is_set():
                self.stats.paused = True
                self.heartbeat_fn(
                    f"⏸ disk {_disk_free_gb():.0f}GB free, waiting for uploads... "
                    f"↓{self.stats.downloaded_files}/{self.stats.total_files} "
                    f"↑{self.stats.uploaded_files}/{self.stats.total_files}"
                )
                await asyncio.sleep(3)
            self.stats.paused = False

            if self._cancel:
                break

            # Download the file
            local_path = self.staging_dir / file_info.path
            local_path.parent.mkdir(parents=True, exist_ok=True)

            try:
                await download_file(
                    self.task, file_info, local_path,
                )
                self.stats.downloaded_files += 1

                # Pass to upload queue
                await upload_queue.put((file_info, local_path))

                self.heartbeat_fn(
                    f"↓{self.stats.downloaded_files}/{self.stats.total_files} "
                    f"↑{self.stats.uploaded_files}/{self.stats.total_files} "
                    f"({self.stats.uploaded_bytes / 1024**3:.1f}GB done)"
                )
            except Exception as e:
                logger.error(f"Download failed {file_info.path}: {e}")
                # Put back in queue for retry
                await file_queue.put(file_info)
                await asyncio.sleep(5)

    async def _uploader(self, upload_queue: asyncio.Queue, worker_id: int):
        """Upload files to BOS, delete local after success."""
        from ..core.config import load_config
        from ..core.bos import create_bos_client, upload_file
        from ..constants import DATA_BUCKET, MODEL_BUCKET

        config = load_config()
        client = create_bos_client(
            config["BAIDU_AK"], config["BAIDU_SK"], config["BOS_ENDPOINT"]
        )

        if self.task.type == "model":
            bucket = MODEL_BUCKET
            prefix = f"{self.task.name}/"
        else:
            bucket = DATA_BUCKET
            prefix = f"{self.task.category}/{self.task.name}/" if self.task.category else f"{self.task.name}/"

        while True:
            item = await upload_queue.get()
            if item is None:
                break

            file_info, local_path = item
            key = prefix + file_info.path

            try:
                # Run blocking upload in thread pool
                await asyncio.to_thread(
                    upload_file, client, bucket, key, str(local_path)
                )

                # Delete local file immediately after upload
                try:
                    local_path.unlink()
                except OSError:
                    pass

                self.stats.uploaded_files += 1
                self.stats.uploaded_bytes += file_info.size

            except Exception as e:
                logger.error(f"Upload failed {file_info.path}: {e}")
                # Re-queue for retry
                await upload_queue.put((file_info, local_path))
                await asyncio.sleep(5)

    async def _disk_monitor(self):
        """Monitor disk usage, pause/resume downloads."""
        while not self._cancel:
            usage = _disk_usage_pct()

            if usage > DISK_PAUSE_THRESHOLD and self._download_paused.is_set():
                self._download_paused.clear()
                logger.warning(
                    f"Disk at {usage:.0%}, pausing downloads "
                    f"(free: {_disk_free_gb():.0f}GB)"
                )
            elif usage < DISK_RESUME_THRESHOLD and not self._download_paused.is_set():
                self._download_paused.set()
                logger.info(
                    f"Disk at {usage:.0%}, resuming downloads "
                    f"(free: {_disk_free_gb():.0f}GB)"
                )

            await asyncio.sleep(5)
