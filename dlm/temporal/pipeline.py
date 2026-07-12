"""Pipeline engine — batch download + concurrent upload with disk backpressure.

Architecture:
  hf download (batch) → [staging dir] → concurrent upload to BOS → delete local
                              ↕
                         Disk Monitor
                         (>80% → pause before next batch)
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
DISK_PAUSE_THRESHOLD = 0.80
DISK_RESUME_THRESHOLD = 0.50
UPLOAD_CONCURRENCY = 8
HF_BATCH_SIZE = 500  # max files per hf download invocation (CLI arg limit)
SPEED_REPORT_INTERVAL = 15  # seconds


def _disk_usage_pct() -> float:
    stat = shutil.disk_usage(STAGING_PATH)
    return stat.used / stat.total


def _disk_free_gb() -> float:
    stat = shutil.disk_usage(STAGING_PATH)
    return stat.free / (1024 ** 3)


def _dir_size(path: Path) -> int:
    total = 0
    try:
        for f in path.rglob("*"):
            if f.is_file() and not f.name.startswith("."):
                total += f.stat().st_size
    except (OSError, PermissionError):
        pass
    return total


class PipelineEngine:
    """Runs batch download + upload pipeline."""

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
        self._cancel = False
        self._proc: Optional[asyncio.subprocess.Process] = None

    async def run(self, files: list[FileInfo]) -> PipelineStats:
        """Execute pipeline: batch download → concurrent upload → delete."""
        self.stats.total_files = len(files)
        self.stats.total_bytes = sum(f.size for f in files)

        try:
            # Step 1: Batch download
            self.stats.phase = "downloading"
            file_paths = [f.path for f in files]
            await self._download_batch(file_paths)

            # Step 2: Concurrent upload + delete
            self.stats.phase = "uploading"
            await self._upload_all(files)

            self.stats.phase = "done"
        except asyncio.CancelledError:
            if self._proc and self._proc.returncode is None:
                self._proc.terminate()
                try:
                    await asyncio.wait_for(self._proc.wait(), timeout=10)
                except asyncio.TimeoutError:
                    self._proc.kill()
            logger.info("Pipeline cancelled, staging preserved for resume")
            raise

        return self.stats

    async def _download_batch(self, file_paths: list[str]):
        """Download all files in batches of HF_BATCH_SIZE using hf download."""
        for i in range(0, len(file_paths), HF_BATCH_SIZE):
            # Disk backpressure: wait if too full
            while _disk_usage_pct() > DISK_PAUSE_THRESHOLD:
                self.stats.paused = True
                self.heartbeat_fn(
                    f"disk {_disk_free_gb():.0f}GB free, waiting... "
                    f"downloaded {self.stats.downloaded_files}/{self.stats.total_files}"
                )
                await asyncio.sleep(10)
            self.stats.paused = False

            chunk = file_paths[i:i + HF_BATCH_SIZE]
            await self._hf_download(chunk)
            self.stats.downloaded_files += len(chunk)

            self.heartbeat_fn(
                f"downloaded {self.stats.downloaded_files}/{self.stats.total_files} files"
            )

    async def _hf_download(self, file_paths: list[str]):
        """Single hf download invocation for a batch of files."""
        rtype = "dataset" if self.task.type == "dataset" else "model"
        cmd = [
            "hf", "download", self.task.repo_id,
            "--local-dir", str(self.staging_dir),
            "--repo-type", rtype,
            "--max-workers", "32",
        ]
        cmd.extend(file_paths)

        env = os.environ.copy()
        env["HF_XET_HIGH_PERFORMANCE"] = "1"
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

        # Monitor staging dir growth for speed reporting
        last_size = _dir_size(self.staging_dir)
        last_time = time.time()

        while self._proc.returncode is None:
            try:
                await asyncio.wait_for(self._proc.wait(), timeout=SPEED_REPORT_INTERVAL)
                break  # process finished
            except asyncio.TimeoutError:
                pass  # still running, report progress

            current_size = _dir_size(self.staging_dir)
            now = time.time()
            elapsed = now - last_time
            if elapsed > 0:
                speed_bps = (current_size - last_size) / elapsed
                speed_mbps = speed_bps * 8 / 1_000_000

                self.heartbeat_fn(
                    f"downloading {speed_mbps:.0f}Mbps "
                    f"{current_size / 1e9:.1f}GB on disk"
                )

                if self.progress_fn:
                    self.progress_fn(current_size, self.stats.total_bytes, speed_bps)

            last_size = current_size
            last_time = now

        if self._proc.returncode != 0:
            stdout = await self._proc.stdout.read()
            output = stdout.decode(errors="replace")[-500:] if stdout else ""
            raise RuntimeError(
                f"hf download failed (rc={self._proc.returncode}): {output}"
            )

        self._proc = None

    async def _upload_all(self, files: list[FileInfo]):
        """Upload all downloaded files to BOS with UPLOAD_CONCURRENCY workers."""
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

        sem = asyncio.Semaphore(UPLOAD_CONCURRENCY)

        async def _upload_one(file_info: FileInfo):
            local_path = self.staging_dir / file_info.path
            if not local_path.exists():
                logger.warning(f"File not found for upload: {local_path}")
                self.stats.failed_files += 1
                return

            key = prefix + file_info.path
            async with sem:
                try:
                    await asyncio.to_thread(
                        upload_file, client, bucket, key, str(local_path)
                    )
                    try:
                        local_path.unlink()
                    except OSError:
                        pass
                    self.stats.uploaded_files += 1
                    self.stats.uploaded_bytes += file_info.size
                except Exception as e:
                    logger.error(f"Upload failed {file_info.path}: {e}")
                    self.stats.failed_files += 1

            # Periodic heartbeat
            if self.stats.uploaded_files % 50 == 0:
                self.heartbeat_fn(
                    f"uploaded {self.stats.uploaded_files}/{self.stats.total_files} "
                    f"({self.stats.uploaded_bytes / 1024**3:.1f}GB)"
                )

        tasks = [asyncio.create_task(_upload_one(f)) for f in files]
        await asyncio.gather(*tasks)

        # Clean up empty directories
        for d in sorted(self.staging_dir.rglob("*"), reverse=True):
            if d.is_dir():
                try:
                    d.rmdir()
                except OSError:
                    pass
