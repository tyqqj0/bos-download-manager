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
from .models import (
    FAIL_ACCESS_DENIED,
    FAIL_DOWNLOAD_RETRIES_EXHAUSTED,
    FAIL_SIZE_MISMATCH,
    FAIL_STAGED_FILE_MISSING,
    FAIL_UNHANDLED_DOWNLOAD_ERROR,
    FAIL_UPLOAD_CANCELLED,
    FAIL_UPLOAD_FAILED,
    FAIL_UPLOAD_RETRIES_EXHAUSTED,
    FAIL_UPSTREAM_EMPTY,
)

logger = logging.getLogger(__name__)


class _StallDetected(Exception):
    """Raised when file download stalls (no size growth for STALL_TIMEOUT)."""
    pass


class _AccessDenied(Exception):
    """Raised for 403/gated repo errors — no point retrying."""
    pass


class _UpstreamEmpty(Exception):
    """Raised when the source serves an empty body for a file it lists as
    non-empty — no point retrying.

    ModelScope answers `HTTP 200` with `Content-Length: 0` for files whose
    listed size is hundreds of MB (verified 2026-08-07 against 14 paths under
    `data/RoboDojo_depth/`, both through the SDK and the raw-file endpoint):
    their metadata still carries the size but the blob is gone. The size guard
    correctly refuses to hand 0 bytes to the uploader, but routing that through
    the ordinary retry path re-asked the same dead endpoint every ~15s forever
    at no cost to anyone but us. Treated like `_AccessDenied`: counted as a
    failed file so the shard reports honestly, and not retried.

    Only the ModelScope paths raise it, because they have one endpoint. An
    empty response on the HF path may be one bad mirror, which a retry can
    route around.
    """
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

    Best-effort by design. `http_get`/`xet_get` are library internals, and the
    fleet does not run a uniform huggingface_hub (1.15.0 and 1.16.1 both in
    production as of 2026-08-06), so a future upgrade may rename or remove
    them. Observing progress is a monitoring nicety; downloading is the job.
    A failure here therefore degrades to "temp path not observable", which
    `_wait_with_growth_check` already treats as inconclusive rather than as a
    stall — it must never propagate and fail the download itself.
    """
    global _hf_patched
    if _hf_patched:
        return
    with _hf_patch_lock:
        if _hf_patched:
            return
        # Set before attempting: on failure this must not be retried for every
        # attempt of every file in the batch (log spam, no new outcome).
        _hf_patched = True
        try:
            from huggingface_hub import file_download as _fd

            # Resolve both originals before installing either, so a missing
            # second symbol cannot leave the library half-patched.
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
        except Exception as exc:
            logger.warning(
                "Could not instrument huggingface_hub for temp-path observation "
                f"({type(exc).__name__}: {exc}); stall detection will treat HF "
                "downloads as unobservable rather than stalled. Downloads are "
                "unaffected."
            )


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
UNOBSERVABLE_TIMEOUT = 3 * STALL_TIMEOUT  # ceiling for "no candidate path exists
                              # yet" — see _wait_with_growth_check for why this
                              # must exist and why it is longer than STALL_TIMEOUT
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
            # The ModelScope SDK hands back a path with no size verification
            # of its own — unlike the HF path (trusts hf_hub_download's
            # internal integrity checks) and the HTTP fallback just below
            # (checks explicitly). A truncated/empty SDK download used to
            # sail straight through to the uploader and land on BOS as a
            # short or 0-byte object while the shard reported success (the
            # RoboDojo incident). Route a mismatch through the same
            # exception handling as any other failed attempt below, so it
            # retries instead of raising past the retry loop.
            size = p.stat().st_size
            if file_info.size and size == 0:
                p.unlink(missing_ok=True)
                raise _UpstreamEmpty(
                    f"upstream served 0 bytes for {file_info.path} "
                    f"(listed size {file_info.size}) — skipping, not retrying"
                )
            if file_info.size and size != file_info.size:
                p.unlink(missing_ok=True)
                raise RuntimeError(
                    f"size mismatch for {file_info.path}: got {size}, expected {file_info.size}"
                )
            self._emit_event("file_downloaded", {
                "file": file_info.path,
                "size_bytes": file_info.size,
                "duration_s": round(time.time() - t0, 1),
                "endpoint": "modelscope",
            })
            return p
        except Exception as e:
            err_str = str(e)
            if isinstance(e, _UpstreamEmpty):
                raise  # permanent — must not be swallowed into a retry
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
                except _UpstreamEmpty:
                    raise  # permanent — the fallback endpoint is empty too
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
                if file_info.size and size == 0:
                    tmp.unlink(missing_ok=True)
                    raise _UpstreamEmpty(
                        f"upstream served 0 bytes for {file_info.path} "
                        f"(listed size {file_info.size}) — skipping, not retrying"
                    )
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
                        self._fail_file(file_info.path, FAIL_ACCESS_DENIED,
                                        file_info.size)
                        logger.error(str(e))
                        return
                    except _UpstreamEmpty as e:
                        cancel_event.set()
                        self._fail_file(file_info.path, FAIL_UPSTREAM_EMPTY,
                                        file_info.size)
                        self._emit_event("file_failed", {
                            "file": file_info.path,
                            "error": "upstream_empty",
                            "endpoint": "modelscope",
                        })
                        logger.error(str(e))
                        return
                    except _StallDetected:
                        cancel_event.set()
                        # Clean up THIS file's *partial* residue only, using
                        # _unlink_candidates — never _residue_candidates,
                        # which additionally includes target_path (the
                        # file's finished destination). cancel_event.set()
                        # cannot interrupt the executor thread
                        # (run_in_executor has no cancellation and nothing
                        # inside hf_hub_download polls the event), so an
                        # orphaned thread from a prior attempt can finish and
                        # move a *correct, complete* file to target_path
                        # while this attempt's monitor is still running.
                        # Unlinking target_path here would delete that file
                        # (I1). _unlink_candidates shares the same partial-
                        # path list _residue_candidates measures growth from
                        # (defect 3 + 4: one source of truth for "where is
                        # this file's partial data" — never a basename/rglob
                        # search across the whole staging tree, which used
                        # to delete a *different* file's in-progress bytes
                        # whenever basenames collided across directories) —
                        # it just excludes the one entry that is not partial.
                        for p in self._unlink_candidates(file_info, temp_path_holder):
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
                self._fail_file(file_info.path,
                                FAIL_DOWNLOAD_RETRIES_EXHAUSTED, file_info.size)
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
        #
        # zip(files, results) recovers the identity this site used to lose:
        # `tasks` is built by one create_task per entry of `files` in order, and
        # asyncio.gather preserves that order in `results`, so results[i] is
        # files[i]'s outcome. Without the zip the only thing available was an
        # exception object, and a file that died here was counted but
        # unnameable — which made it invisible to the missing-file archive.
        for f, r in zip(files, results):
            if isinstance(r, (Exception, asyncio.CancelledError)):
                self._fail_file(f.path, FAIL_UNHANDLED_DOWNLOAD_ERROR, f.size)
                logger.error(f"Unhandled download error: {r}")
        await queue.put(None)  # signal consumer to stop

    def _unlink_candidates(
        self, file_info: FileInfo, temp_path_holder: "_TempPathHolder | None"
    ) -> list[Path]:
        """Every path this file's *partial* (not-yet-final) data could
        currently live at. Safe for stall cleanup to unlink.

        Deliberately excludes `target_path` — the file's finished
        destination — which `_residue_candidates` (the growth-measurement
        superset) includes. `cancel_event.set()` cannot interrupt the
        executor thread (`run_in_executor` has no cancellation, and nothing
        inside `hf_hub_download`/the ModelScope SDK polls that event), so an
        orphaned thread from a previous attempt can finish and move a
        complete, correct file to `target_path` while a later attempt's
        monitor is still running against it. If `target_path` were on this
        list, the next stall on that later attempt would unlink the correct
        file that a different thread already finished (I1) — no BOS data
        lost (the retry is lossless), but it manufactures the exact false
        stall this branch exists to remove.

        Shared with `_residue_candidates` so there is exactly one answer to
        "where is this file's partial data" for both purposes — see
        `_wait_with_growth_check` for why a basename search across the whole
        staging tree is not on either list.
        """
        target_path = self.staging_dir / file_info.path
        candidates = [
            Path(str(target_path) + ".incomplete"),
            Path(str(target_path) + ".part"),
            self.staging_dir / "._____temp" / file_info.path,
        ]
        if temp_path_holder is not None and temp_path_holder.path is not None:
            candidates.append(temp_path_holder.path)
        return candidates

    def _residue_candidates(
        self, file_info: FileInfo, temp_path_holder: "_TempPathHolder | None"
    ) -> list[Path]:
        """Every path this file's data could live at right now, complete or
        partial — used ONLY for growth measurement, never for cleanup.

        This is `_unlink_candidates` plus `target_path`, the file's finished
        destination: a completed download (this attempt's or an orphaned
        earlier one still racing against `cancel_event`) is real, valid
        growth and must count as such. Cleanup must use
        `_unlink_candidates` instead — see its docstring for why
        `target_path` must never be unlinked by stall cleanup (I1).
        """
        target_path = self.staging_dir / file_info.path
        return [target_path] + self._unlink_candidates(file_info, temp_path_holder)

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

        Two independent clocks, not one:

        - Observed and not growing: `_StallDetected` at `STALL_TIMEOUT`,
          exactly as always.
        - Nothing observable at all (no candidate path exists on disk and
          `temp_path_holder` has never reported one): `_StallDetected` at
          the separate, longer `UNOBSERVABLE_TIMEOUT`. The moment any
          candidate path appears, the unobservable clock resets and the
          file moves onto the growth clock; if it later becomes
          unobservable again, it goes back onto a *fresh* unobservable
          clock, not the growth clock it left.

        `UNOBSERVABLE_TIMEOUT` is generous — three times `STALL_TIMEOUT` —
        because a legitimate pre-download phase (etag resolution, repo
        metadata calls) produces no temp file yet, and because killing a
        healthy download is what caused the 35-hour molmobot outage. But it
        must have a ceiling, because nothing else will ever time this out:
        `_speed_reporter` heartbeats unconditionally every 15s regardless of
        progress, so the activity's 10-minute `heartbeat_timeout` can never
        fire on its own, and `start_to_close_timeout` is 7 days — without
        this clock, a genuine hang before any candidate path exists (e.g.
        blocked in HTTP metadata/etag resolution) would wedge a shard at 0
        Mbps for up to 7 days with no automatic recovery.
        """
        last_size = 0
        last_growth_time = time.time()
        # None means "currently observable" (on the growth clock above).
        # A timestamp means "unobservable since this moment" (on the
        # UNOBSERVABLE_TIMEOUT clock below). Starts unobservable: nothing
        # has been seen yet at the top of a fresh attempt.
        unobservable_since: Optional[float] = time.time()

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
            # data could live at right now. Each candidate is checked on
            # its own: one unreadable/vanished candidate (e.g. renamed out
            # from under us mid-stat) must not mask growth on another
            # candidate that is healthy (M2) — a single `except` around the
            # whole loop used to let an early failure skip the rest.
            current_size = None
            for candidate in self._residue_candidates(file_info, temp_path_holder):
                try:
                    if candidate.exists():
                        current_size = max(current_size or 0, candidate.stat().st_size)
                except OSError:
                    continue

            if current_size is None:
                # No candidate exists yet for this file, and none has been
                # reported — inconclusive, not evidence of a stall. Expected
                # early in an attempt (before any temp file exists) and can
                # persist for a while if huggingface_hub never reports a
                # temp path (e.g. served from its own cache with no
                # incomplete file at all). Bounded by UNOBSERVABLE_TIMEOUT,
                # not unbounded — see the docstring above for why.
                if unobservable_since is None:
                    unobservable_since = time.time()
                elif time.time() - unobservable_since > UNOBSERVABLE_TIMEOUT:
                    raise _StallDetected(
                        f"{file_info.path}: no temp path ever locatable for "
                        f"{UNOBSERVABLE_TIMEOUT}s"
                    )
                continue

            # Observable now: leave the unobservable clock (a later gap
            # starts a fresh one, not this one) and evaluate growth.
            unobservable_since = None
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
        # Which file each in-flight upload task is for. _count_upload_task_
        # failures needs the identity, and a bare Task does not carry it.
        # Entries are popped as tasks are counted, so this stays bounded by
        # MAX_PENDING_UPLOADS rather than growing to the size of the batch.
        owners: dict = {}

        while True:
            # Backpressure: wait for a slot before pulling from queue
            while len(pending) >= MAX_PENDING_UPLOADS:
                done, _ = await asyncio.wait(
                    pending, return_when=asyncio.FIRST_COMPLETED
                )
                pending -= done
                self._count_upload_task_failures(done, owners)

            file_info = await queue.get()
            if file_info is None:
                break

            task = asyncio.create_task(self._upload_one(file_info, sem))
            pending.add(task)
            owners[task] = file_info

        # Drain remaining uploads
        if pending:
            done, _ = await asyncio.wait(pending)
            self._count_upload_task_failures(done, owners)

        self.stats.phase = "done"

        # Clean up empty directories left after file deletions
        for d in sorted(self.staging_dir.rglob("*"), reverse=True):
            if d.is_dir() and d != self.staging_dir:
                try:
                    d.rmdir()
                except OSError:
                    pass

    def _count_upload_task_failures(self, done: set, owners: dict) -> None:
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

        `owners` maps each task back to its FileInfo so the failure can be
        named, and entries are popped here so the caller's dict stays bounded.
        A task missing from it is a bug, not a reason to lose the count: fall
        back to a placeholder path so failed_details keeps matching
        failed_files.
        """
        for t in done:
            fi = owners.pop(t, None)
            path = fi.path if fi is not None else "<unknown-upload>"
            size = fi.size if fi is not None else 0
            if t.cancelled():
                # Cancellation is orchestration (pause / preempt / reshard), not
                # "the source lost this file" — counted here so the batch is not
                # reported clean, but activities.py keeps it out of the archive.
                self._fail_file(path, FAIL_UPLOAD_CANCELLED, size)
                logger.error(f"Upload task cancelled: {path}")
                continue
            exc = t.exception()
            if exc:
                self._fail_file(path, FAIL_UPLOAD_FAILED, size)
                logger.error(f"Upload task error for {path}: {exc}")

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
            self._fail_file(fi.path, FAIL_STAGED_FILE_MISSING, fi.size)
            return

        key = self._prefix + fi.path

        async with sem:
            for attempt in range(MAX_UPLOAD_RETRIES):
                t0 = time.time()
                try:
                    # Second of two independent guards against a short/empty
                    # file reaching BOS: defect 5 already verifies size for
                    # every download path, so this should never trip — but
                    # it makes the failure mode impossible to reach BOS even
                    # if a future download path forgets that check. Do not
                    # delete this as "redundant" — it is the last gate.
                    actual_size = local_path.stat().st_size
                    if fi.size and actual_size != fi.size:
                        logger.error(
                            f"Pre-upload size check failed for {fi.path}: "
                            f"got {actual_size}, expected {fi.size} — not uploading"
                        )
                        self._fail_file(fi.path, FAIL_SIZE_MISMATCH, fi.size)
                        self._emit_event("file_failed", {
                            "file": fi.path,
                            "error": f"size mismatch: got {actual_size}, expected {fi.size}",
                            "phase": "pre_upload_verify",
                        })
                        return
                    await asyncio.to_thread(
                        upload_file,
                        self._bos_client,
                        self._bucket,
                        key,
                        str(local_path),
                    )
                    local_path.unlink(missing_ok=True)
                    self.stats.uploaded_files += 1
                    # Credit what was actually uploaded (measured just above,
                    # right before the call), not fi.size — the size the
                    # *source* claims. Crediting the claimed size is how the
                    # RoboDojo incident's dashboard showed 900MB uploaded for
                    # each of 103 objects that landed on BOS as 0 bytes.
                    self.stats.uploaded_bytes += actual_size
                    self._emit_event("file_uploaded", {
                        "file": fi.path,
                        "size_bytes": actual_size,
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
                    self._fail_file(fi.path, FAIL_UPLOAD_RETRIES_EXHAUSTED, fi.size)
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

    def _fail_file(self, path: str, reason: str, size_bytes: int = 0) -> None:
        """Count a failed file AND record which file it was.

        Every `failed_files += 1` in this class goes through here, so the
        invariant len(failed_details) == failed_files holds by construction
        rather than by nine sites remembering to do both. Before this, the
        count said a batch lost N files and the identity of those N existed
        only in a log line on one worker — nothing downstream could archive it.

        `reason` is a short classifier from models.FAIL_*, never exception
        text: exceptions on this fleet routinely carry KB-scale xet CDN URLs,
        and those must not reach the database.
        """
        self.stats.failed_files += 1
        self.stats.failed_details.append({
            "path": path,
            "reason": reason,
            "size_bytes": int(size_bytes or 0),
        })

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
