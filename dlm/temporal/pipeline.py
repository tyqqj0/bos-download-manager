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
import errno
import logging
import os
import shutil
import threading
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


class _TempPathHolder:
    """Cross-thread channel: the download call (running in a worker thread)
    reports the real temp path it is writing to; the async stall monitor
    (running on the event loop thread) reads it.

    One instance per download attempt. A plain attribute swap is enough —
    CPython's GIL makes the write atomically visible to the reading thread;
    this is progress monitoring, not a correctness-critical lock.
    """

    def __init__(self):
        self.path: Optional[Path] = None


# huggingface_hub writes to a path it names with `uuid.uuid4()` *inside*
# `_download_to_tmp_and_move` (added upstream to defend against broken
# flock() on Lustre/GPFS/some NFS mounts — huggingface_hub#4228). That name
# cannot be predicted from data we hold ahead of time — not from
# `file_info.path`, not even from the file's HTTP etag — it is only
# knowable from inside the call. `http_get` (classic) and `xet_get` (Xet
# storage) both receive that real path as an argument, so patching those —
# not the private function that invents the uuid — is the stable
# interception point: both are called with the fully-resolved temp
# path/file already in hand, on both the http_get and xet_get branches.
_HF_TEMP_PATH_LOCAL = threading.local()
_hf_patch_lock = threading.Lock()
_hf_patched = False


def _ensure_hf_temp_path_patch():
    """Monkeypatch huggingface_hub's low-level writers once per process so a
    download's real temp path is observable from outside the call that
    creates it. Idempotent and thread-safe to call from every attempt.
    """
    global _hf_patched
    if _hf_patched:
        return
    with _hf_patch_lock:
        if _hf_patched:
            return
        from huggingface_hub import file_download as _fd

        _orig_http_get = _fd.http_get
        _orig_xet_get = _fd.xet_get

        def _patched_http_get(url, temp_file, **kwargs):
            holder = getattr(_HF_TEMP_PATH_LOCAL, "holder", None)
            if holder is not None:
                name = getattr(temp_file, "name", None)
                if name:
                    holder.path = Path(name)
            return _orig_http_get(url, temp_file, **kwargs)

        def _patched_xet_get(*, incomplete_path, **kwargs):
            holder = getattr(_HF_TEMP_PATH_LOCAL, "holder", None)
            if holder is not None:
                holder.path = incomplete_path
            return _orig_xet_get(incomplete_path=incomplete_path, **kwargs)

        _fd.http_get = _patched_http_get
        _fd.xet_get = _patched_xet_get
        _hf_patched = True


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

    def _download_one_file(
        self,
        file_info: FileInfo,
        cancel_event: "threading.Event | None" = None,
        temp_path_holder: "_TempPathHolder | None" = None,
    ) -> Optional[Path]:
        """Download a single file. Runs in thread pool.

        Dispatches to HuggingFace or ModelScope based on task.source.
        """
        if self.task.source == "modelscope":
            return self._download_one_file_modelscope(file_info, cancel_event, temp_path_holder)
        return self._download_one_file_hf(file_info, cancel_event, temp_path_holder)

    def _download_one_file_hf(
        self, file_info: FileInfo, cancel_event, temp_path_holder: "_TempPathHolder | None" = None
    ) -> Optional[Path]:
        """HuggingFace download with mirror fallback."""
        from huggingface_hub import hf_hub_download

        os.environ.setdefault("HF_HUB_DOWNLOAD_TIMEOUT", str(HTTP_TIMEOUT))
        _ensure_hf_temp_path_patch()

        endpoints = [MIRROR_PRIMARY]
        if MIRROR_FALLBACK:
            endpoints.append(MIRROR_FALLBACK)

        for endpoint in endpoints:
            if cancel_event and cancel_event.is_set():
                return None
            if temp_path_holder is not None:
                temp_path_holder.path = None
            t0 = time.time()
            prev_holder = getattr(_HF_TEMP_PATH_LOCAL, "holder", None)
            _HF_TEMP_PATH_LOCAL.holder = temp_path_holder
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
            finally:
                _HF_TEMP_PATH_LOCAL.holder = prev_holder
        return None

    def _download_one_file_modelscope(
        self, file_info: FileInfo, cancel_event, temp_path_holder: "_TempPathHolder | None" = None
    ) -> Optional[Path]:
        """ModelScope per-file download.

        `temp_path_holder` is accepted for signature symmetry with the HF
        path but unused here: the SDK's own `.part`/`._____temp` staging
        names are already exact and derivable (see `_wait_with_growth_check`),
        unlike huggingface_hub's uuid-named temp files.
        """
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
            p = Path(local_path)
            self._emit_event("file_downloaded", {
                "file": file_info.path,
                "size_bytes": file_info.size,
                "duration_s": round(time.time() - t0, 1),
                "endpoint": "modelscope",
            })
            return p
        except Exception as e:
            err_str = str(e)
            if isinstance(e, OSError) and e.errno == errno.ENAMETOOLONG:
                # The SDK flattens the full repo path into a lock-file name,
                # which overflows the 255-byte filename limit on deep paths.
                # Fetch the raw file over HTTP instead. (Checked before the
                # 403 guard: the OSError message embeds the full path, which
                # may itself contain "403".)
                try:
                    local_path = self._download_via_http_modelscope(file_info, token, cancel_event)
                    self._emit_event("file_downloaded", {
                        "file": file_info.path,
                        "size_bytes": file_info.size,
                        "duration_s": round(time.time() - t0, 1),
                        "endpoint": "modelscope-http",
                    })
                    return local_path
                except Exception as e2:
                    err_str = f"{err_str[:150]}; http fallback failed: {e2}"
            elif "403" in err_str or "forbidden" in err_str.lower():
                raise _AccessDenied(f"Access denied for {file_info.path}: {err_str[:200]}")
            self._emit_event("file_failed", {
                "file": file_info.path,
                "error": err_str[:200],
                "endpoint": "modelscope",
            })
            logger.warning(f"ModelScope download failed for {file_info.path}: {err_str}")
            return None

    def _download_via_http_modelscope(
        self, file_info: FileInfo, token: "str | None", cancel_event=None
    ) -> Path:
        """Raw-file fetch bypassing the SDK's path-derived lock files."""
        import requests
        from urllib.parse import quote

        kind = "datasets" if self.task.type == "dataset" else "models"
        url = (
            f"https://www.modelscope.cn/api/v1/{kind}/{self.task.repo_id}"
            f"/repo?Revision=master&FilePath={quote(file_info.path, safe='')}"
        )
        dest = self.staging_dir / file_info.path
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_name(dest.name + ".part")

        header_sets = [{"Authorization": f"Bearer {token}"}] if token else [{}]
        if token:
            header_sets.append({})  # stale token can 401 where anonymous succeeds

        last_exc: Exception = RuntimeError("no attempt made")
        for headers in header_sets:
            try:
                with requests.get(
                    url, stream=True, timeout=(10, HTTP_TIMEOUT), headers=headers
                ) as r:
                    r.raise_for_status()
                    with open(tmp, "wb") as f:
                        for chunk in r.iter_content(chunk_size=1 << 20):
                            if cancel_event and cancel_event.is_set():
                                raise RuntimeError(f"cancelled: {file_info.path}")
                            if chunk:
                                f.write(chunk)
                size = tmp.stat().st_size
                if file_info.size and size != file_info.size:
                    raise RuntimeError(
                        f"size mismatch for {file_info.path}: got {size}, expected {file_info.size}"
                    )
                tmp.replace(dest)
                return dest
            except Exception as exc:
                tmp.unlink(missing_ok=True)
                last_exc = exc
                status = getattr(getattr(exc, "response", None), "status_code", None)
                if status in (401, 403) and headers:
                    continue  # retry anonymously
                raise
        raise last_exc

    async def _producer(self, files: list[FileInfo], queue: asyncio.Queue):
        """Download files using thread pool, put completed FileInfo onto queue."""
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
                    temp_path_holder = _TempPathHolder()
                    try:
                        download_future = loop.run_in_executor(
                            self._executor, self._download_one_file, file_info, cancel_event, temp_path_holder
                        )
                        # Monitor file growth instead of hard timeout
                        local_path = await self._wait_with_growth_check(
                            download_future, file_info, cancel_event, temp_path_holder
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
                        # Clean up THIS file's residue only, using the exact
                        # same paths growth was checked against (defect 3 +
                        # 4 share one source of truth for "where is this
                        # file's partial data" — never a basename/rglob
                        # search across the whole staging tree, which used
                        # to delete a *different* file's in-progress bytes
                        # whenever basenames collided across directories).
                        for p in self._residue_candidates(file_info, temp_path_holder):
                            try:
                                p.unlink(missing_ok=True)
                            except OSError:
                                pass
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
        # Count any unhandled exceptions as failures (prevents silent file drops).
        # asyncio.CancelledError is a BaseException, not an Exception, since
        # Python 3.8 — return_exceptions=True still puts the instance into
        # `results`, so `isinstance(r, Exception)` alone silently let every
        # cancelled download (pause/preempt/reshard, or _AccessDenied/
        # _StallDetected setting cancel_event) through uncounted. Note
        # download_with_limit never increments failed_files itself when a
        # CancelledError escapes here — both of its except clauses catch
        # Exception subclasses only, so a CancelledError bypasses them and
        # the "all retries exhausted" tail below them is never reached
        # either — so counting it here cannot double-count.
        for r in results:
            if isinstance(r, (Exception, asyncio.CancelledError)):
                self.stats.failed_files += 1
                logger.error(f"Unhandled download error: {r}")
        await queue.put(None)  # signal consumer to stop

    def _residue_candidates(
        self, file_info: FileInfo, temp_path_holder: "_TempPathHolder | None"
    ) -> list[Path]:
        """Every path this file's partial data could currently live at.

        Shared by the stall monitor (reads sizes) and the stall cleanup
        (unlinks) so there is exactly one answer to "where is this file's
        partial data" — see `_wait_with_growth_check` for why a basename
        search across the whole staging tree is not on this list.
        """
        target_path = self.staging_dir / file_info.path
        candidates = [
            target_path,
            Path(str(target_path) + ".incomplete"),
            Path(str(target_path) + ".part"),
            self.staging_dir / "._____temp" / file_info.path,
        ]
        if temp_path_holder is not None and temp_path_holder.path is not None:
            candidates.append(temp_path_holder.path)
        return candidates

    async def _wait_with_growth_check(
        self,
        download_future: asyncio.Future,
        file_info: FileInfo,
        cancel_event,
        temp_path_holder: "_TempPathHolder | None" = None,
    ) -> Optional[Path]:
        """Wait for download to complete, checking file size growth periodically.

        Unlike asyncio.wait_for(timeout=300) which kills legitimate large downloads,
        this only declares a stall if the file on disk stops growing for STALL_TIMEOUT
        seconds. A 50GB file downloading at 1MB/s will take 14 hours but won't be killed
        as long as bytes keep arriving.

        Growth is measured only from this file's own known-or-reported
        candidate paths (`_residue_candidates`) — never from a substring/
        basename match against the whole staging tree. That used to both
        (a) miss real growth entirely for HuggingFace xet/local-dir
        downloads, whose temp file is named with a uuid huggingface_hub
        generates *inside* the download call — unknowable in advance, only
        knowable by having that call report it via `temp_path_holder` (see
        `_ensure_hf_temp_path_patch`) — and (b) credit a *different* file's
        growth to this one whenever basenames collided across directories,
        which is common in sharded datasets.
        """
        last_size = 0
        last_growth_time = time.time()

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

            # Check file size growth across every path this file's partial
            # data could live at right now.
            current_size = None
            try:
                for candidate in self._residue_candidates(file_info, temp_path_holder):
                    if candidate.exists():
                        current_size = max(current_size or 0, candidate.stat().st_size)
            except OSError:
                pass

            if current_size is None:
                # No candidate exists yet for this file — inconclusive, not
                # evidence of a stall. This is expected early in an attempt
                # (before any temp file exists) and can persist for the
                # whole attempt if huggingface_hub never reports a temp
                # path (e.g. served from its own cache with no incomplete
                # file at all). Reset the clock rather than accumulate
                # silent "no growth" time against a file we cannot observe:
                # a false stall here bricked every large HF download for
                # 35 hours straight (molmobot-data, 2026-08). The activity's
                # own heartbeat/start_to_close timeout is the backstop for
                # an actual hang.
                last_growth_time = time.time()
                continue

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
                self._count_upload_task_failures(done)

            file_info = await queue.get()
            if file_info is None:
                break

            task = asyncio.create_task(self._upload_one(file_info, sem))
            pending.add(task)

        # Drain remaining uploads
        if pending:
            done, _ = await asyncio.wait(pending)
            self._count_upload_task_failures(done)

        self.stats.phase = "done"

        # Clean up empty directories left after file deletions
        for d in sorted(self.staging_dir.rglob("*"), reverse=True):
            if d.is_dir() and d != self.staging_dir:
                try:
                    d.rmdir()
                except OSError:
                    pass

    def _count_upload_task_failures(self, done: set) -> None:
        """Classify finished `_upload_one` tasks without re-raising.

        Shared by both `_consumer` call sites (the backpressure wait inside
        the pull loop, and the final drain after the queue is exhausted) —
        one place so the two cannot drift, as they did when this was four
        duplicated lines at each site. The sites differ in which tasks end
        up in `done` (`FIRST_COMPLETED` vs. waiting for everything) and in
        what happens to `pending` afterward; that bookkeeping stays at each
        call site — only the "given a finished task, count it correctly"
        step is shared, and that step is identical at both sites.

        `Task.exception()` *raises* `CancelledError` for a cancelled task
        instead of returning it, so a single cancelled upload used to
        propagate straight out of `_consumer` — killing the whole consumer
        loop while the producer kept filling a queue nobody was left to
        drain. Check `cancelled()` first so cancellation is counted, not
        fatal. `_upload_one` already counts and logs its own
        retry-exhausted failures internally (it catches `Exception` and
        never re-raises), so anything visible here is either cancellation
        or a bug outside its try block — neither already counted — safe to
        count unconditionally without double counting.
        """
        for t in done:
            if t.cancelled():
                self.stats.failed_files += 1
                logger.error("Upload task cancelled")
                continue
            exc = t.exception()
            if exc:
                self.stats.failed_files += 1
                logger.error(f"Upload task error: {exc}")

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
