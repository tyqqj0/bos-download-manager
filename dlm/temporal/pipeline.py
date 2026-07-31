"""Pipeline engine — per-file producer-consumer with Python API downloads.

Architecture:
  Producer (ThreadPoolExecutor, 8 workers)  Queue(maxsize=16)  Consumer (asyncio, 12 max pending)
  ─────────────────────────────────────────  ───────────────    ─────────────────────────────────
  hf_hub_download() per file                [FileInfo, ...]    BOS upload + unlink per file
    mirror fallback per file                                   asyncio.Semaphore(8)
    stall detection: file-growth based                         3 retries, delete on final failure
    max 3 retries per file
    backpressure: pause if disk < 30% free (dynamic)
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


class _StallDetected(Exception):
    """Raised when file download stalls (no size growth for STALL_TIMEOUT)."""
    pass


class _AccessDenied(Exception):
    """Raised for 403/gated repo errors — no point retrying."""
    pass


# Download concurrency adapts to file size. Big files saturate the link on
# their own and more streams only add disk pressure; small files are pure
# round-trip latency (HEAD + GET + CDN GET each), so throughput scales with
# concurrency until the link fills.
DOWNLOAD_WORKERS = 4          # large-file default; see _download_concurrency()
DOWNLOAD_WORKERS_MAX = 32     # small-file ceiling
SMALL_FILE_BYTES = 8 * 1024 ** 2    # <8MB mean → latency-bound, scale up
LARGE_FILE_BYTES = 256 * 1024 ** 2  # >256MB mean → bandwidth-bound, stay low
UPLOAD_CONCURRENCY = 8        # parallel BOS uploads (asyncio.Semaphore)
DISK_FREE_MIN_PCT = 0.30      # keep 30% disk free (dynamic threshold)
DISK_FREE_ABSOLUTE_MIN_GB = 20  # absolute minimum free space
STALL_CHECK_INTERVAL = 15    # check file growth every 15s (was 30)
STALL_TIMEOUT = 600           # 10 min no file growth → stall (was 1800s/30min)
HTTP_TIMEOUT = 300            # HF_HUB_DOWNLOAD_TIMEOUT: per-read HTTP timeout
MAX_FILE_RETRIES = 3          # per-file retry limit
MIRROR_PRIMARY = "https://huggingface.co"
MIRROR_FALLBACK = None        # hf-mirror.com broken (308 redirect), disabled
SPEED_REPORT_INTERVAL = 15    # seconds between progress reports
QUEUE_MAX_SIZE = 16           # max files buffered between download and upload
MAX_PENDING_UPLOADS = UPLOAD_CONCURRENCY + 4  # max in-flight upload tasks before backpressure
STAGING_PATH = Path("/data/staging")


def _download_concurrency(files: list) -> int:
    """Pick download parallelism from the batch's mean file size.

    A 372k-file / 300KB-each dataset spent ~20h at 13 Mbps on 4 streams
    because every file costs three round-trips to the CDN; the link was
    nowhere near saturated.
    """
    if not files:
        return DOWNLOAD_WORKERS
    mean = sum(f.size for f in files) / len(files)
    if mean >= LARGE_FILE_BYTES:
        return DOWNLOAD_WORKERS
    if mean <= SMALL_FILE_BYTES:
        return DOWNLOAD_WORKERS_MAX
    # Log-ish interpolation between the two anchors
    span = LARGE_FILE_BYTES - SMALL_FILE_BYTES
    ratio = 1 - (mean - SMALL_FILE_BYTES) / span
    return max(
        DOWNLOAD_WORKERS,
        int(DOWNLOAD_WORKERS + (DOWNLOAD_WORKERS_MAX - DOWNLOAD_WORKERS) * ratio),
    )


def _disk_free_gb() -> float:
    stat = shutil.disk_usage(STAGING_PATH)
    return stat.free / (1024 ** 3)


def _disk_free_threshold_gb() -> float:
    """Dynamic backpressure threshold: max(30% of total disk, 20GB)."""
    stat = shutil.disk_usage(STAGING_PATH)
    total_gb = stat.total / (1024 ** 3)
    return max(total_gb * DISK_FREE_MIN_PCT, DISK_FREE_ABSOLUTE_MIN_GB)


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
        self._concurrency = DOWNLOAD_WORKERS  # set from batch contents in run()

    def _download_one_file(self, file_info: FileInfo, cancel_event: "threading.Event | None" = None) -> Optional[Path]:
        """Download a single file. Runs in thread pool.

        Dispatches to HuggingFace or ModelScope based on task.source.
        """
        if self.task.source == "modelscope":
            return self._download_one_file_modelscope(file_info, cancel_event)
        return self._download_one_file_hf(file_info, cancel_event)

    def _download_one_file_hf(self, file_info: FileInfo, cancel_event) -> Optional[Path]:
        """HuggingFace download with mirror fallback."""
        import threading
        from huggingface_hub import hf_hub_download

        os.environ.setdefault("HF_HUB_DOWNLOAD_TIMEOUT", str(HTTP_TIMEOUT))

        endpoints = [MIRROR_PRIMARY]
        if MIRROR_FALLBACK:
            endpoints.append(MIRROR_FALLBACK)

        for endpoint in endpoints:
            if cancel_event and cancel_event.is_set():
                return None
            t0 = time.time()
            try:
                local_path = hf_hub_download(
                    repo_id=self.task.repo_id,
                    filename=file_info.path,
                    repo_type=self.task.type,
                    local_dir=str(self.staging_dir),
                    endpoint=endpoint,
                    token=os.environ.get("HF_TOKEN"),
                    force_download=False,
                )
                self._emit_event("file_downloaded", {
                    "file": file_info.path,
                    "size_bytes": file_info.size,
                    "duration_s": round(time.time() - t0, 1),
                    "endpoint": endpoint,
                })
                return Path(local_path)
            except Exception as e:
                err_str = str(e)
                if "403" in err_str or "gated" in err_str.lower() or "restricted" in err_str.lower():
                    raise _AccessDenied(f"Access denied for {file_info.path}: {err_str[:200]}")
                if "429" in err_str or "rate limit" in err_str.lower() or "too many requests" in err_str.lower():
                    wait = 60
                    logger.warning(f"Rate limited on {endpoint}, sleeping {wait}s")
                    time.sleep(wait)
                self._emit_event("file_failed", {
                    "file": file_info.path,
                    "error": err_str[:200],
                    "endpoint": endpoint,
                })
                logger.warning(f"Download failed from {endpoint} for {file_info.path}: {e}")
                continue
        return None

    def _download_one_file_modelscope(self, file_info: FileInfo, cancel_event) -> Optional[Path]:
        """ModelScope per-file download."""
        from modelscope import dataset_file_download

        if cancel_event and cancel_event.is_set():
            return None

        t0 = time.time()
        token = os.environ.get("MODELSCOPE_API_TOKEN") or os.environ.get("MS_TOKEN")
        try:
            local_path = dataset_file_download(
                dataset_id=self.task.repo_id,
                file_path=file_info.path,
                local_dir=str(self.staging_dir),
                token=token,
            )
            self._emit_event("file_downloaded", {
                "file": file_info.path,
                "size_bytes": file_info.size,
                "duration_s": round(time.time() - t0, 1),
                "endpoint": "modelscope",
            })
            return Path(local_path)
        except Exception as e:
            err_str = str(e)
            if "403" in err_str or "forbidden" in err_str.lower():
                raise _AccessDenied(f"Access denied for {file_info.path}: {err_str[:200]}")
            self._emit_event("file_failed", {
                "file": file_info.path,
                "error": err_str[:200],
                "endpoint": "modelscope",
            })
            logger.warning(f"ModelScope download failed for {file_info.path}: {e}")
            return None

    async def _producer(self, files: list[FileInfo], queue: asyncio.Queue):
        """Download files using thread pool, put completed FileInfo onto queue."""
        import threading

        loop = asyncio.get_running_loop()
        sem = asyncio.Semaphore(self._concurrency)

        async def download_with_limit(file_info: FileInfo):
            async with sem:
                # Backpressure: wait for disk space
                threshold = _disk_free_threshold_gb()
                while _disk_free_gb() < threshold:
                    self.stats.paused = True
                    self.heartbeat_fn(f"backpressure: {_disk_free_gb():.0f}GB free < {threshold:.0f}GB threshold")
                    await asyncio.sleep(10)
                self.stats.paused = False

                # Download with file-growth-based stall detection
                for attempt in range(MAX_FILE_RETRIES):
                    cancel_event = threading.Event()
                    self._cancel_events.append(cancel_event)
                    try:
                        download_future = loop.run_in_executor(
                            self._executor, self._download_one_file, file_info, cancel_event
                        )
                        # Monitor file growth instead of hard timeout
                        local_path = await self._wait_with_growth_check(
                            download_future, file_info, cancel_event
                        )
                        if local_path:
                            self.stats.downloaded_files += 1
                            await queue.put(file_info)
                            return
                        logger.warning(
                            f"All mirrors failed for {file_info.path} (attempt {attempt + 1})"
                        )
                    except _AccessDenied as e:
                        cancel_event.set()
                        self.stats.failed_files += 1
                        logger.error(str(e))
                        return
                    except _StallDetected:
                        cancel_event.set()
                        # Clean up .incomplete residue for THIS specific file only
                        filename = file_info.path.split("/")[-1]
                        for p in self.staging_dir.rglob(f"{filename}.incomplete"):
                            p.unlink(missing_ok=True)
                        for p in self.staging_dir.rglob(f".cache/**/{filename}.incomplete"):
                            p.unlink(missing_ok=True)
                        logger.warning(
                            f"Stall detected for {file_info.path} "
                            f"(no growth for {STALL_TIMEOUT}s, attempt {attempt + 1})"
                        )
                        continue

                # All retries exhausted
                self.stats.failed_files += 1
                logger.error(
                    f"Failed to download {file_info.path} after {MAX_FILE_RETRIES} attempts"
                )

        # Launch all downloads concurrently (semaphore limits to DOWNLOAD_WORKERS at a time)
        tasks = [asyncio.create_task(download_with_limit(f)) for f in files]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        # Count any unhandled exceptions as failures (prevents silent file drops)
        for r in results:
            if isinstance(r, Exception):
                self.stats.failed_files += 1
                logger.error(f"Unhandled download error: {r}")
        await queue.put(None)  # signal consumer to stop

    async def _wait_with_growth_check(
        self,
        download_future: asyncio.Future,
        file_info: FileInfo,
        cancel_event,
    ) -> Optional[Path]:
        """Wait for download to complete, checking file size growth periodically.

        Unlike asyncio.wait_for(timeout=300) which kills legitimate large downloads,
        this only declares a stall if the file on disk stops growing for STALL_TIMEOUT
        seconds. A 50GB file downloading at 1MB/s will take 14 hours but won't be killed
        as long as bytes keep arriving.
        """
        last_size = 0
        last_growth_time = time.time()
        target_path = self.staging_dir / file_info.path

        while not download_future.done():
            await asyncio.sleep(STALL_CHECK_INTERVAL)
            if download_future.done():
                break

            # Emergency disk check: abort if critically low
            free_gb = _disk_free_gb()
            if free_gb < DISK_FREE_ABSOLUTE_MIN_GB:
                cancel_event.set()
                logger.warning(
                    f"Disk critically low ({free_gb:.0f}GB free), "
                    f"aborting download: {file_info.path}"
                )
                raise _StallDetected(f"Disk full abort: {file_info.path}")

            # Check file size growth (handles .incomplete and ModelScope temp files)
            current_size = 0
            try:
                for candidate in [
                    target_path,
                    Path(str(target_path) + ".incomplete"),
                    self.staging_dir / "._____temp" / file_info.path,
                ]:
                    if candidate.exists():
                        current_size = max(current_size, candidate.stat().st_size)
                for p in self.staging_dir.glob(".cache/**/*.incomplete"):
                    if file_info.path.split("/")[-1] in str(p):
                        current_size = max(current_size, p.stat().st_size)
            except OSError:
                pass

            if current_size > last_size:
                last_size = current_size
                last_growth_time = time.time()
            elif time.time() - last_growth_time > STALL_TIMEOUT:
                raise _StallDetected(
                    f"{file_info.path}: no growth for {STALL_TIMEOUT}s "
                    f"(stuck at {last_size / 1024 / 1024:.1f} MB)"
                )

        return download_future.result()

    async def _consumer(self, queue: asyncio.Queue):
        """Upload files from queue to BOS, delete local file after success.

        Backpressure: limits in-flight upload tasks to MAX_PENDING_UPLOADS.
        When the cap is hit, waits for at least one upload to finish before
        pulling the next file from the queue — this is what makes queue.put()
        in the producer actually block, limiting disk accumulation.
        """
        sem = asyncio.Semaphore(UPLOAD_CONCURRENCY)
        pending: set = set()

        while True:
            # Backpressure: wait for a slot before pulling from queue
            while len(pending) >= MAX_PENDING_UPLOADS:
                done, _ = await asyncio.wait(
                    pending, return_when=asyncio.FIRST_COMPLETED
                )
                pending -= done
                for t in done:
                    exc = t.exception()
                    if exc:
                        logger.error(f"Upload task error: {exc}")

            file_info = await queue.get()
            if file_info is None:
                break

            task = asyncio.create_task(self._upload_one(file_info, sem))
            pending.add(task)

        # Drain remaining uploads
        if pending:
            done, _ = await asyncio.wait(pending)
            for t in done:
                exc = t.exception()
                if exc:
                    logger.error(f"Upload task error: {exc}")

        self.stats.phase = "done"

        # Clean up empty directories left after file deletions
        for d in sorted(self.staging_dir.rglob("*"), reverse=True):
            if d.is_dir() and d != self.staging_dir:
                try:
                    d.rmdir()
                except OSError:
                    pass

    async def _upload_one(self, fi: FileInfo, sem: asyncio.Semaphore):
        """Upload a single file to BOS with retry.

        Deletes local file on success. On final failure, keeps file on disk
        (Temporal batch-level retry will re-attempt the whole batch) but counts
        it as failed for stats.
        """
        from ..core.bos import upload_file

        MAX_UPLOAD_RETRIES = 5
        local_path = self.staging_dir / fi.path

        if not local_path.exists():
            logger.warning(f"File not found for upload: {local_path}")
            self.stats.failed_files += 1
            return

        key = self._prefix + fi.path

        async with sem:
            for attempt in range(MAX_UPLOAD_RETRIES):
                t0 = time.time()
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
                    self._emit_event("file_uploaded", {
                        "file": fi.path,
                        "size_bytes": fi.size,
                        "duration_s": round(time.time() - t0, 1),
                    })
                    return
                except Exception as e:
                    if attempt < MAX_UPLOAD_RETRIES - 1:
                        wait = min(2 ** attempt, 30)
                        logger.warning(
                            f"Upload retry {attempt + 1}/{MAX_UPLOAD_RETRIES} "
                            f"for {fi.path}: {e} (wait {wait}s)"
                        )
                        await asyncio.sleep(wait)
                        continue
                    # All retries exhausted — keep file for batch-level retry
                    logger.error(
                        f"Upload failed x{MAX_UPLOAD_RETRIES} for {fi.path}: {e}"
                    )
                    self.stats.failed_files += 1
                    self._emit_event("file_failed", {
                        "file": fi.path,
                        "error": str(e)[:200],
                        "phase": "upload",
                    })

    def _staging_bytes(self) -> int:
        """Current on-disk size of the staging dir (in-flight download progress)."""
        total = 0
        try:
            for root, _dirs, files in os.walk(self.staging_dir):
                for f in files:
                    try:
                        total += os.path.getsize(os.path.join(root, f))
                    except OSError:
                        pass
        except Exception:
            pass
        return total

    async def _speed_reporter(self):
        """Periodically report speed and progress until pipeline is done.

        Speed is measured from DOWNLOAD activity (staging growth + uploads),
        not uploads alone — with multi-GB files nothing uploads for a long
        time and an upload-only metric shows a false 0 while the network
        is saturated.
        """
        # Seed the baseline from what is ALREADY on disk. A resumed batch starts
        # with a full staging dir; measuring from 0 would report that entire
        # backlog as one interval's traffic (observed: 12,977 Mbps spikes).
        last_bytes = self.stats.uploaded_bytes + await asyncio.to_thread(self._staging_bytes)
        last_time = time.time()
        while self.stats.phase != "done":
            await asyncio.sleep(SPEED_REPORT_INTERVAL)
            now = time.time()
            elapsed = now - last_time
            if elapsed > 0:
                # A file leaves staging exactly when it lands in uploaded_bytes,
                # so the sum is monotonic download progress. Walked off-loop —
                # a shard can hold 80k staged files.
                progress_bytes = self.stats.uploaded_bytes + await asyncio.to_thread(
                    self._staging_bytes
                )
                delta_bytes = max(0, progress_bytes - last_bytes)
                speed_bps = delta_bytes / elapsed
                self.stats.speed_mbps = speed_bps * 8 / 1_000_000
                last_bytes = progress_bytes
                last_time = now

                self.heartbeat_fn(f"downloading {self.stats.speed_mbps:.0f}Mbps")
                if self.progress_fn:
                    self.progress_fn(
                        self.stats.uploaded_bytes, self.stats.total_bytes, speed_bps
                    )

    def _init_bos_client(self):
        """Initialize BOS client and determine target bucket/prefix."""
        from ..core.config import load_config
        from ..core.bos import bos_target, create_bos_client

        config = load_config()
        self._bos_client = create_bos_client(
            config["BAIDU_AK"], config["BAIDU_SK"], config["BOS_ENDPOINT"]
        )
        self._bucket, self._prefix = bos_target(self.task)

    def _emit_event(self, event_type: str, data: dict):
        """Emit a monitoring event to the event buffer (if available)."""
        try:
            from .event_buffer import get_event_buffer
            buf = get_event_buffer()
            if buf:
                data["task_id"] = self.task.id
                data["task_name"] = self.task.name
                buf.emit(event_type, data)
        except Exception:
            pass  # monitoring should never break downloads

    async def run(self, files: list[FileInfo]) -> PipelineStats:
        """Execute parallel pipeline: producer downloads per file, consumer uploads per file."""
        self.stats.total_files = len(files)
        self.stats.total_bytes = sum(f.size for f in files)
        if not files:
            self.stats.phase = "done"
            return self.stats

        self._init_bos_client()
        self._concurrency = _download_concurrency(files)
        mean_mb = (self.stats.total_bytes / len(files)) / 1024 ** 2
        logger.info(
            "Pipeline: %d files, mean %.1f MB → %d download streams",
            len(files), mean_mb, self._concurrency,
        )
        self.heartbeat_fn(
            f"{len(files)} files, mean {mean_mb:.1f}MB, {self._concurrency} streams"
        )
        self._executor = ThreadPoolExecutor(max_workers=self._concurrency)
        queue: asyncio.Queue = asyncio.Queue(maxsize=max(QUEUE_MAX_SIZE, self._concurrency * 2))

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
