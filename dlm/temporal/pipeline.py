"""Pipeline engine — per-file producer-consumer with Python API downloads.

Architecture:
  Producer (ThreadPoolExecutor, 8 workers)  Queue(maxsize=64)  Consumer (asyncio, 8 concurrent)
  ─────────────────────────────────────────  ───────────────    ───────────────────────────────
  hf_hub_download() per file                [FileInfo, ...]    BOS upload + unlink per file
    mirror fallback per file                                   asyncio.Semaphore(8)
    stall detection: 300s timeout
    max 3 retries per file
    backpressure: pause if disk < 50GB free
"""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Callable, Optional

from .models import TaskInput, FileInfo, PipelineStats

logger = logging.getLogger(__name__)

DOWNLOAD_WORKERS = 8          # parallel file downloads (ThreadPoolExecutor)
UPLOAD_CONCURRENCY = 8        # parallel BOS uploads (asyncio.Semaphore)
DISK_FREE_MIN_GB = 50         # backpressure threshold
STALL_TIMEOUT = 300           # 5 min no progress → timeout
MAX_FILE_RETRIES = 3          # per-file retry limit
MIRROR_PRIMARY = "https://hf-mirror.com"
MIRROR_FALLBACK = "https://huggingface.co"
SPEED_REPORT_INTERVAL = 15    # seconds between progress reports
QUEUE_MAX_SIZE = 64           # max files buffered between download and upload
STAGING_PATH = Path("/data/staging")


def _disk_free_gb() -> float:
    stat = shutil.disk_usage(STAGING_PATH)
    return stat.free / (1024 ** 3)


class PipelineEngine:
    """Parallel download+upload pipeline with disk backpressure."""

    def __init__(
        self,
        task_input: TaskInput,
        staging_dir: Path,
        heartbeat_fn: Callable[[str], None],
        progress_fn: Optional[Callable[[int, int, float], None]] = None,
    ):
        self.task = task_input
        self.staging_dir = staging_dir
        self.heartbeat_fn = heartbeat_fn
        self.progress_fn = progress_fn
        self.stats = PipelineStats()
        self._executor: Optional[ThreadPoolExecutor] = None
        self._cancel_events: list = []
        self._bos_client = None
        self._bucket = ""
        self._prefix = ""

    def _download_one_file(self, file_info: FileInfo, cancel_event: "threading.Event | None" = None) -> Optional[Path]:
        """Download a single file with mirror fallback. Runs in thread pool.

        Uses HF_HUB_DOWNLOAD_TIMEOUT env var to enforce HTTP-level timeouts,
        ensuring threads exit on stall rather than hanging indefinitely.
        """
        import threading
        from huggingface_hub import hf_hub_download

        os.environ.setdefault("HF_HUB_DOWNLOAD_TIMEOUT", str(STALL_TIMEOUT))

        for endpoint in [MIRROR_PRIMARY, MIRROR_FALLBACK]:
            if cancel_event and cancel_event.is_set():
                return None
            try:
                local_path = hf_hub_download(
                    repo_id=self.task.repo_id,
                    filename=file_info.path,
                    repo_type=self.task.type,
                    local_dir=str(self.staging_dir),
                    endpoint=endpoint,
                    token=os.environ.get("HF_TOKEN"),
                    force_download=False,  # resume partial downloads
                )
                return Path(local_path)
            except Exception as e:
                logger.warning(f"Download failed from {endpoint} for {file_info.path}: {e}")
                continue
        return None  # all mirrors failed

    async def _producer(self, files: list[FileInfo], queue: asyncio.Queue):
        """Download files using thread pool, put completed FileInfo onto queue."""
        import threading

        loop = asyncio.get_running_loop()
        sem = asyncio.Semaphore(DOWNLOAD_WORKERS)

        async def download_with_limit(file_info: FileInfo):
            async with sem:
                # Backpressure: wait for disk space
                while _disk_free_gb() < DISK_FREE_MIN_GB:
                    self.stats.paused = True
                    self.heartbeat_fn(f"backpressure: {_disk_free_gb():.0f}GB free")
                    await asyncio.sleep(10)
                self.stats.paused = False

                # Download with stall detection and retries
                for attempt in range(MAX_FILE_RETRIES):
                    cancel_event = threading.Event()
                    self._cancel_events.append(cancel_event)
                    try:
                        local_path = await asyncio.wait_for(
                            loop.run_in_executor(
                                self._executor, self._download_one_file, file_info, cancel_event
                            ),
                            timeout=STALL_TIMEOUT,
                        )
                        if local_path:
                            self.stats.downloaded_files += 1
                            await queue.put(file_info)
                            return
                        # None means all mirrors failed for this attempt
                        logger.warning(
                            f"All mirrors failed for {file_info.path} (attempt {attempt + 1})"
                        )
                    except asyncio.TimeoutError:
                        cancel_event.set()
                        logger.warning(
                            f"Timeout downloading {file_info.path} (attempt {attempt + 1})"
                        )
                        continue

                # All retries exhausted
                self.stats.failed_files += 1
                logger.error(
                    f"Failed to download {file_info.path} after {MAX_FILE_RETRIES} attempts"
                )

        # Launch all downloads concurrently (semaphore limits to DOWNLOAD_WORKERS at a time)
        tasks = [asyncio.create_task(download_with_limit(f)) for f in files]
        await asyncio.gather(*tasks, return_exceptions=True)
        await queue.put(None)  # signal consumer to stop

    async def _consumer(self, queue: asyncio.Queue):
        """Upload files from queue to BOS, delete local file after success."""
        from ..core.bos import upload_file

        sem = asyncio.Semaphore(UPLOAD_CONCURRENCY)
        upload_tasks = []

        while True:
            file_info = await queue.get()
            if file_info is None:
                break

            async def upload_one(fi: FileInfo):
                local_path = self.staging_dir / fi.path
                if not local_path.exists():
                    logger.warning(f"File not found for upload: {local_path}")
                    self.stats.failed_files += 1
                    return
                key = self._prefix + fi.path
                async with sem:
                    try:
                        await asyncio.to_thread(
                            upload_file,
                            self._bos_client,
                            self._bucket,
                            key,
                            str(local_path),
                        )
                        local_path.unlink(missing_ok=True)
                        self.stats.uploaded_files += 1
                        self.stats.uploaded_bytes += fi.size
                    except Exception as e:
                        logger.error(f"Upload failed {fi.path}: {e}")
                        self.stats.failed_files += 1

            upload_tasks.append(asyncio.create_task(upload_one(file_info)))

        # Wait for all in-flight uploads to complete
        if upload_tasks:
            await asyncio.gather(*upload_tasks, return_exceptions=True)

        # Signal speed reporter to exit (checked in _speed_reporter loop condition)
        self.stats.phase = "done"

        # Clean up empty directories left after file deletions
        for d in sorted(self.staging_dir.rglob("*"), reverse=True):
            if d.is_dir() and d != self.staging_dir:
                try:
                    d.rmdir()
                except OSError:
                    pass

    async def _speed_reporter(self):
        """Periodically report speed and progress until pipeline is done."""
        last_bytes = 0
        last_time = time.time()
        while self.stats.phase != "done":
            await asyncio.sleep(SPEED_REPORT_INTERVAL)
            now = time.time()
            elapsed = now - last_time
            if elapsed > 0:
                delta_bytes = self.stats.uploaded_bytes - last_bytes
                speed_bps = delta_bytes / elapsed
                self.stats.speed_mbps = speed_bps * 8 / 1_000_000
                last_bytes = self.stats.uploaded_bytes
                last_time = now

                self.heartbeat_fn(f"downloading {self.stats.speed_mbps:.0f}Mbps")
                if self.progress_fn:
                    self.progress_fn(
                        self.stats.uploaded_bytes, self.stats.total_bytes, speed_bps
                    )

    def _init_bos_client(self):
        """Initialize BOS client and determine target bucket/prefix."""
        from ..core.config import load_config
        from ..core.bos import create_bos_client
        from ..constants import DATA_BUCKET, MODEL_BUCKET

        config = load_config()
        self._bos_client = create_bos_client(
            config["BAIDU_AK"], config["BAIDU_SK"], config["BOS_ENDPOINT"]
        )

        if self.task.type == "model":
            self._bucket = MODEL_BUCKET
            self._prefix = f"{self.task.name}/"
        else:
            self._bucket = DATA_BUCKET
            if self.task.category:
                self._prefix = f"{self.task.category}/{self.task.name}/"
            else:
                self._prefix = f"{self.task.name}/"

    async def run(self, files: list[FileInfo]) -> PipelineStats:
        """Execute parallel pipeline: producer downloads per file, consumer uploads per file."""
        self.stats.total_files = len(files)
        self.stats.total_bytes = sum(f.size for f in files)
        if not files:
            self.stats.phase = "done"
            return self.stats

        self._init_bos_client()
        self._executor = ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS)
        queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)

        self.stats.phase = "downloading"
        try:
            await asyncio.gather(
                self._producer(files, queue),
                self._consumer(queue),
                self._speed_reporter(),
            )
        except asyncio.CancelledError:
            for evt in self._cancel_events:
                evt.set()
            self._executor.shutdown(wait=False, cancel_futures=True)
            logger.info("Pipeline cancelled, staging preserved for resume")
            raise
        finally:
            self._executor.shutdown(wait=False)

        self.stats.phase = "done"
        return self.stats
