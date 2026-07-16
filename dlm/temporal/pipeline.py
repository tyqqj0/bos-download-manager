"""Pipeline engine — parallel producer-consumer with disk backpressure.

Architecture:
  Producer (hf download chunks) → Queue(maxsize=8) → Consumer (BOS upload + delete)
                                        ↕
                                   Disk Monitor
                                   (free < 50GB → pause producer)

Download and upload run concurrently. As soon as one chunk finishes downloading,
it's queued for upload. If uploads can't keep up and disk fills, downloads pause.
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
CHUNK_MAX_BYTES = 20 * 1024**3  # 20GB per chunk
CHUNK_MAX_FILES = 500           # CLI arg length limit
DISK_FREE_MIN_GB = 50           # pause downloads below this
UPLOAD_CONCURRENCY = 8          # concurrent BOS uploads per chunk
HF_MAX_WORKERS = 16             # hf download --max-workers
SPEED_REPORT_INTERVAL = 15      # seconds between speed reports
QUEUE_MAX_SIZE = 8              # max chunks buffered between download and upload


def _disk_free_gb() -> float:
    stat = shutil.disk_usage(STAGING_PATH)
    return stat.free / (1024 ** 3)


def _dir_size(path: Path) -> int:
    """Total bytes on disk under path (includes dotfiles like .cache)."""
    total = 0
    try:
        for f in path.rglob("*"):
            if f.is_file():
                total += f.stat().st_size
    except (OSError, PermissionError):
        pass
    return total


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
        self._proc: Optional[asyncio.subprocess.Process] = None
        self._bos_client = None
        self._bucket = ""
        self._prefix = ""
        self._cumulative_downloaded = 0
        self._download_start_time = 0.0

    async def run(self, files: list[FileInfo]) -> PipelineStats:
        """Execute parallel pipeline: producer downloads chunks while consumer uploads them."""
        self.stats.total_files = len(files)
        self.stats.total_bytes = sum(f.size for f in files)

        chunks = self._make_chunks(files)
        if not chunks:
            self.stats.phase = "done"
            return self.stats

        self._init_bos_client()
        queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
        producer_error: list = [None]

        async def producer():
            try:
                for i, chunk in enumerate(chunks):
                    # Backpressure: wait until enough disk free
                    while _disk_free_gb() < DISK_FREE_MIN_GB:
                        self.stats.paused = True
                        self.heartbeat_fn(
                            f"backpressure: {_disk_free_gb():.0f}GB free, "
                            f"waiting for uploads to free space"
                        )
                        await asyncio.sleep(10)
                    self.stats.paused = False

                    # Download this chunk
                    self.stats.phase = "downloading"
                    file_paths = [f.path for f in chunk]
                    await self._hf_download(file_paths)
                    self.stats.downloaded_files += len(chunk)
                    self._clean_cache()

                    self.heartbeat_fn(
                        f"downloaded chunk {i+1}/{len(chunks)} "
                        f"({self.stats.downloaded_files}/{self.stats.total_files} files)"
                    )

                    # Queue for upload
                    await queue.put(chunk)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                producer_error[0] = e
                logger.error(f"Producer error: {e}")
            finally:
                await queue.put(None)

        async def consumer():
            while True:
                chunk = await queue.get()
                if chunk is None:
                    queue.task_done()
                    break
                self.stats.phase = "uploading"
                await self._upload_chunk(chunk)
                self._clean_cache()
                queue.task_done()

        try:
            self.stats.phase = "downloading"
            await asyncio.gather(producer(), consumer())
        except asyncio.CancelledError:
            if self._proc and self._proc.returncode is None:
                self._proc.terminate()
                try:
                    await asyncio.wait_for(self._proc.wait(), timeout=10)
                except asyncio.TimeoutError:
                    self._proc.kill()
            logger.info("Pipeline cancelled, staging preserved for resume")
            raise

        if producer_error[0]:
            raise producer_error[0]

        self.stats.phase = "done"
        return self.stats

    def _make_chunks(self, files: list[FileInfo]) -> list[list[FileInfo]]:
        """Partition files into chunks of max 20GB or 500 files."""
        chunks = []
        current: list[FileInfo] = []
        current_bytes = 0

        for f in files:
            # If adding this file would exceed limits AND we have files already, flush
            if current and (
                current_bytes + f.size > CHUNK_MAX_BYTES
                or len(current) >= CHUNK_MAX_FILES
            ):
                chunks.append(current)
                current = []
                current_bytes = 0
            current.append(f)
            current_bytes += f.size

        if current:
            chunks.append(current)

        logger.info(
            f"Partitioned {len(files)} files into {len(chunks)} chunks "
            f"(total {self.stats.total_bytes / 1024**3:.1f}GB)"
        )
        return chunks

    async def _hf_download(self, file_paths: list[str]):
        """Run a single hf download invocation for a batch of files."""
        rtype = "dataset" if self.task.type == "dataset" else "model"
        cmd = [
            "hf", "download", self.task.repo_id,
            "--local-dir", str(self.staging_dir),
            "--repo-type", rtype,
            "--max-workers", str(HF_MAX_WORKERS),
        ]
        cmd.extend(file_paths)

        env = os.environ.copy()
        env["HF_ENDPOINT"] = os.environ.get("HF_ENDPOINT", "https://hf-mirror.com")
        env["HF_HUB_CACHE"] = "/tmp/hf_cache"
        if os.environ.get("HF_TOKEN"):
            env["HF_TOKEN"] = os.environ["HF_TOKEN"]

        logger.info(f"hf download: {len(file_paths)} files → {self.staging_dir}")

        self._proc = await asyncio.create_subprocess_exec(
            *cmd,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )

        chunk_start_size = _dir_size(self.staging_dir)
        chunk_start_time = time.time()

        while self._proc.returncode is None:
            try:
                await asyncio.wait_for(self._proc.wait(), timeout=SPEED_REPORT_INTERVAL)
                break
            except asyncio.TimeoutError:
                pass

            current_size = _dir_size(self.staging_dir)
            now = time.time()
            elapsed = now - chunk_start_time
            if elapsed > 0:
                chunk_downloaded = max(0, current_size - chunk_start_size)
                speed_bps = chunk_downloaded / elapsed
                speed_mbps = speed_bps * 8 / 1_000_000
                self.stats.speed_mbps = speed_mbps

                self.heartbeat_fn(
                    f"downloading {speed_mbps:.0f}Mbps "
                    f"{current_size / 1e9:.1f}GB on disk"
                )

                if self.progress_fn:
                    self.progress_fn(
                        self.stats.uploaded_bytes + self._cumulative_downloaded + chunk_downloaded,
                        self.stats.total_bytes,
                        speed_bps,
                    )

        # After download completes, record cumulative bytes for this chunk
        final_size = _dir_size(self.staging_dir)
        chunk_bytes = max(0, final_size - chunk_start_size)
        self._cumulative_downloaded += chunk_bytes

        # Compute final speed for this chunk
        total_elapsed = time.time() - chunk_start_time
        if total_elapsed > 0:
            self.stats.speed_mbps = (chunk_bytes / total_elapsed) * 8 / 1_000_000

        rc = self._proc.returncode
        if rc != 0:
            stdout = await self._proc.stdout.read()
            output = stdout.decode(errors="replace")[-500:] if stdout else ""
            self._proc = None
            raise RuntimeError(f"hf download failed (rc={rc}): {output}")

        self._proc = None

    async def _upload_chunk(self, chunk: list[FileInfo]):
        """Upload a chunk of files to BOS with concurrent workers, delete after success."""
        from ..core.bos import upload_file

        self.heartbeat_fn(
            f"uploading chunk ({len(chunk)} files, "
            f"{sum(f.size for f in chunk) / 1024**3:.1f}GB)"
        )

        sem = asyncio.Semaphore(UPLOAD_CONCURRENCY)
        errors = []
        upload_start = time.time()
        upload_start_bytes = self.stats.uploaded_bytes

        async def _upload_one(file_info: FileInfo):
            local_path = self.staging_dir / file_info.path
            if not local_path.exists():
                logger.warning(f"File not found for upload: {local_path}")
                self.stats.failed_files += 1
                return

            key = self._prefix + file_info.path
            async with sem:
                try:
                    await asyncio.to_thread(
                        upload_file, self._bos_client, self._bucket, key, str(local_path)
                    )
                    local_path.unlink(missing_ok=True)
                    self.stats.uploaded_files += 1
                    self.stats.uploaded_bytes += file_info.size
                except Exception as e:
                    logger.error(f"Upload failed {file_info.path}: {e}")
                    self.stats.failed_files += 1
                    errors.append(str(e))

            if self.stats.uploaded_files % 10 == 0 or self.stats.uploaded_files == self.stats.total_files:
                elapsed = time.time() - upload_start
                upload_speed_bps = (self.stats.uploaded_bytes - upload_start_bytes) / max(elapsed, 1)
                self.stats.speed_mbps = upload_speed_bps * 8 / 1_000_000
                self.heartbeat_fn(
                    f"uploaded {self.stats.uploaded_files}/{self.stats.total_files} "
                    f"({self.stats.uploaded_bytes / 1024**3:.1f}GB, "
                    f"{self.stats.speed_mbps:.0f}Mbps)"
                )
                if self.progress_fn:
                    self.progress_fn(
                        self.stats.uploaded_bytes,
                        self.stats.total_bytes,
                        upload_speed_bps,
                    )

        tasks = [asyncio.create_task(_upload_one(f)) for f in chunk]
        await asyncio.gather(*tasks, return_exceptions=True)

        # Clean empty directories left after file deletion
        for d in sorted(self.staging_dir.rglob("*"), reverse=True):
            if d.is_dir() and d != self.staging_dir:
                try:
                    d.rmdir()
                except OSError:
                    pass

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

    def _clean_cache(self):
        """Remove hf download's .cache directory to prevent unbounded growth."""
        cache_dir = self.staging_dir / ".cache"
        if cache_dir.exists():
            shutil.rmtree(cache_dir, ignore_errors=True)
