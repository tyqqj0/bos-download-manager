"""Data-integrity defects in the download pipeline (hotfix/pipeline-integrity).

The incident: `manipulation/RoboDojo/` reported 9/9 shards `done`,
`done_files == total_files`, and healthy uploaded-bytes metrics. A
source-vs-BOS diff proved 103 objects sitting on BOS as 0 bytes (91.4GB of
real data lost). Defect 5 (no size check on the ModelScope SDK download
path, in a later commit on this branch) caused it; defects 1 and 2 here are
why a cancelled/failed transfer could still slip through uncounted. Defects
3 and 4 are a second, independent outage: a false stall detector bricked
every large HuggingFace xet-backed download for 35 hours straight
(molmobot-data, 0/4765 files done) because its real temp file is named with
an opaque hash the old code never matched.

These tests exercise the real filesystem via `tmp_path` and real asyncio
primitives rather than mocking `Path` internals — these are filesystem and
concurrency bugs, and a mock that models either wrongly would happily pass
a broken fix.

Run: python3 -m pytest tests/test_pipeline_integrity.py -q
"""

from __future__ import annotations

import asyncio
import sys
import threading
import time
import types
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from dlm.temporal import pipeline
from dlm.temporal.models import FileInfo, TaskInput


def _engine(tmp_path, monkeypatch, source="hf") -> pipeline.PipelineEngine:
    # _disk_free_gb/_disk_free_threshold_gb read the module-level STAGING_PATH
    # constant (/data/staging in production), not engine.staging_dir — point
    # it at the test's own tmp_path so backpressure/emergency-disk checks
    # don't fail with FileNotFoundError on a dev machine with no /data.
    monkeypatch.setattr(pipeline, "STAGING_PATH", tmp_path)
    task = TaskInput(id="t1", name="task", repo_id="org/name", source=source)
    return pipeline.PipelineEngine(task, tmp_path, heartbeat_fn=lambda *_: None)


# ---------------------------------------------------------------------------
# Defect 5 — ModelScope SDK downloads were never size-verified.
# ---------------------------------------------------------------------------

def test_robodojo_regression_modelscope_short_file_never_uploaded(tmp_path, monkeypatch):
    """RoboDojo regression: a truncated/empty ModelScope SDK download must be
    treated as a failed attempt (retried, then counted), never handed to the
    uploader as if it succeeded. This was the root cause of the incident —
    the SDK path was the only one of three download paths with no
    post-download size check, so a 0-byte file sailed through as "done"."""
    monkeypatch.setattr(pipeline, "MAX_FILE_RETRIES", 2)
    monkeypatch.setattr(pipeline, "STALL_CHECK_INTERVAL", 0.01)

    engine = _engine(tmp_path, monkeypatch, source="modelscope")
    engine._executor = ThreadPoolExecutor(max_workers=2)
    engine._concurrency = 2

    file_info = FileInfo(path="data/episode_0000001.hdf5", size=900_000_000)

    calls = []

    def fake_dataset_file_download(dataset_id, file_path, local_dir, token=None):
        dest = Path(local_dir) / file_path
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"")  # 0 bytes, like the incident — SDK call "succeeds"
        calls.append(file_path)
        return str(dest)

    fake_module = types.ModuleType("modelscope")
    fake_module.dataset_file_download = fake_dataset_file_download
    monkeypatch.setitem(sys.modules, "modelscope", fake_module)

    queue = asyncio.Queue()
    asyncio.run(engine._producer([file_info], queue))

    assert len(calls) == 2, "must retry MAX_FILE_RETRIES times, not raise past the retry loop"
    assert engine.stats.failed_files == 1, "counted as failed exactly once, not silently dropped"
    assert engine.stats.downloaded_files == 0

    queued = []
    while not queue.empty():
        queued.append(queue.get_nowait())
    assert queued == [None], "a failed download must never reach the uploader"


# ---------------------------------------------------------------------------
# Defect 3 — stall cleanup used to delete a different file's residue.
# ---------------------------------------------------------------------------

def test_stall_cleanup_does_not_touch_sibling_with_same_basename(tmp_path, monkeypatch):
    """A same-basename file under a different directory must survive a
    stall on its sibling. Dataset repos routinely reuse basenames across
    directories; the old `rglob(f"{filename}.incomplete")` matched that
    basename anywhere under the staging dir, so a stall on one file used to
    delete a *different, actively downloading* file's partial data."""
    monkeypatch.setattr(pipeline, "MAX_FILE_RETRIES", 1)

    engine = _engine(tmp_path, monkeypatch)
    engine._executor = ThreadPoolExecutor(max_workers=2)
    engine._concurrency = 2

    file_info = FileInfo(path="dirA/episode_0000001.hdf5", size=1000)

    target_residue = tmp_path / "dirA" / "episode_0000001.hdf5.incomplete"
    target_residue.parent.mkdir(parents=True)
    target_residue.write_bytes(b"x" * 10)

    sibling = tmp_path / "dirB" / "episode_0000001.hdf5.incomplete"
    sibling.parent.mkdir(parents=True)
    sibling.write_bytes(b"y" * 10)

    monkeypatch.setattr(engine, "_download_one_file", lambda *a, **k: None)

    async def always_stall(*args, **kwargs):
        raise pipeline._StallDetected("forced stall for test")

    monkeypatch.setattr(engine, "_wait_with_growth_check", always_stall)

    queue = asyncio.Queue()
    asyncio.run(engine._producer([file_info], queue))

    assert engine.stats.failed_files == 1
    assert not target_residue.exists(), "this file's own residue should be cleaned up"
    assert sibling.exists(), "a different file's same-basename residue must survive"


# ---------------------------------------------------------------------------
# I1 — the unlink list must exclude the file's own finished destination.
# ---------------------------------------------------------------------------

def test_stall_cleanup_does_not_unlink_completed_target_path(tmp_path, monkeypatch):
    """`cancel_event.set()` cannot interrupt the executor thread —
    `run_in_executor` has no cancellation and nothing inside `hf_hub_download`
    polls that event — so an orphaned thread from attempt N can finish and
    move a complete, correct file to `target_path` while attempt N+1's
    monitor is still running and then stalls. That monitor's cleanup must
    not delete the finished file: `target_path` belongs in the growth list
    (`_residue_candidates`) but not the unlink list (`_unlink_candidates`)."""
    monkeypatch.setattr(pipeline, "MAX_FILE_RETRIES", 1)

    engine = _engine(tmp_path, monkeypatch)
    engine._executor = ThreadPoolExecutor(max_workers=2)
    engine._concurrency = 2

    file_info = FileInfo(path="dirA/episode_0000001.hdf5", size=1000)

    # An orphaned earlier attempt already moved the completed file here.
    target_path = tmp_path / "dirA" / "episode_0000001.hdf5"
    target_path.parent.mkdir(parents=True)
    target_path.write_bytes(b"x" * 1000)

    monkeypatch.setattr(engine, "_download_one_file", lambda *a, **k: None)

    async def always_stall(*args, **kwargs):
        raise pipeline._StallDetected("forced stall for test")

    monkeypatch.setattr(engine, "_wait_with_growth_check", always_stall)

    queue = asyncio.Queue()
    asyncio.run(engine._producer([file_info], queue))

    assert engine.stats.failed_files == 1
    assert target_path.exists(), (
        "the file's finished destination must survive stall cleanup for "
        "that same file"
    )
    assert target_path.read_bytes() == b"x" * 1000, "the completed data must be untouched"


# ---------------------------------------------------------------------------
# Defect 4 (mechanism) — the actual monkeypatch that reports huggingface_hub's
# real, uuid-suffixed temp path back to the monitor.
# ---------------------------------------------------------------------------

def test_hf_temp_path_patch_reports_the_real_path(monkeypatch, tmp_path):
    """`_ensure_hf_temp_path_patch` must capture the real path/file object
    that `http_get`/`xet_get` are called with — the actual mechanism the
    molmobot fix depends on — not a guessed or predicted one. Exercised
    against a fake `huggingface_hub.file_download` module (never the real
    one) so this test cannot leave the real library monkeypatched for other
    tests in the process."""
    import huggingface_hub
    import huggingface_hub.file_download  # noqa: F401 — ensure the submodule attribute exists to patch

    fake_fd = types.ModuleType("huggingface_hub.file_download")

    def fake_http_get(url, temp_file, **kwargs):
        temp_file.write(b"hello")

    def fake_xet_get(*, incomplete_path, **kwargs):
        incomplete_path.write_bytes(b"hello")

    fake_fd.http_get = fake_http_get
    fake_fd.xet_get = fake_xet_get

    monkeypatch.setattr(huggingface_hub, "file_download", fake_fd, raising=False)
    monkeypatch.setattr(pipeline, "_hf_patched", False)

    pipeline._ensure_hf_temp_path_patch()

    holder_a = pipeline._TempPathHolder()
    pipeline._HF_TEMP_PATH_LOCAL.holder = holder_a
    real_tmp_http = tmp_path / "http-real-a1b2c3d4.incomplete"
    with open(real_tmp_http, "wb") as f:
        fake_fd.http_get("http://x", f)
    pipeline._HF_TEMP_PATH_LOCAL.holder = None

    assert holder_a.path == real_tmp_http

    holder_b = pipeline._TempPathHolder()
    pipeline._HF_TEMP_PATH_LOCAL.holder = holder_b
    real_tmp_xet = tmp_path / "xet-real-e5f6g7h8.incomplete"
    fake_fd.xet_get(incomplete_path=real_tmp_xet)
    pipeline._HF_TEMP_PATH_LOCAL.holder = None

    assert holder_b.path == real_tmp_xet


def test_hf_temp_path_patch_degrades_instead_of_failing_the_download(monkeypatch):
    """The patch targets huggingface_hub internals (`http_get`/`xet_get`) and
    the fleet is not on a uniform version (1.15.0 and 1.16.1 both in
    production). If an upgrade renames or removes one of them, instrumenting
    must degrade to "temp path not observable" — which the stall monitor
    treats as inconclusive — and must NOT raise out of the download path.
    Raising here would fail every HF download on that host at once."""
    import huggingface_hub

    fake_fd = types.ModuleType("huggingface_hub.file_download")
    fake_fd.http_get = lambda url, temp_file, **kwargs: None
    # xet_get deliberately absent: simulates a version that renamed/removed it.

    monkeypatch.setattr(huggingface_hub, "file_download", fake_fd, raising=False)
    monkeypatch.setattr(pipeline, "_hf_patched", False)

    pipeline._ensure_hf_temp_path_patch()  # must not raise

    # Half-patching is worse than not patching: the surviving symbol must be
    # left exactly as it was, not wrapped by a partially-installed patch.
    assert fake_fd.http_get.__name__ == "<lambda>"
    # And it must not retry (and re-log) on every subsequent file.
    assert pipeline._hf_patched is True
    pipeline._ensure_hf_temp_path_patch()  # still must not raise


def test_residue_candidates_includes_the_reported_temp_path(tmp_path, monkeypatch):
    """Narrow, whitebox companion to the molmobot regression test below: the
    reported `temp_path_holder.path` must actually be one of the paths
    growth is measured from. Without this, "not stalled" in the end-to-end
    test could pass vacuously through the "cannot observe" branch (defect 4
    correction, requirement 3) even if the holder were never consulted at
    all — this test pins the specific mechanism, not just the outcome."""
    engine = _engine(tmp_path, monkeypatch)
    file_info = FileInfo(path="a/b.tar", size=10)
    holder = pipeline._TempPathHolder()
    holder.path = tmp_path / ".cache/huggingface/download/a/HASH.etag.uuid.incomplete"

    candidates = engine._residue_candidates(file_info, holder)

    assert holder.path in candidates


def test_molmobot_regression_xet_temp_path_seen_as_growth_not_stalled(tmp_path, monkeypatch):
    """molmobot-data regression: a HuggingFace xet-backed download's real
    temp file is named with an opaque hash (huggingface_hub generates it
    with `uuid.uuid4()` inside the download call), not the file's basename
    — "00016.tar" appears nowhere in
    `.cache/huggingface/download/<mirrored dirs>/<hash>.incomplete`. The old
    substring-on-basename check therefore saw zero growth for the file's
    entire lifetime and declared it stalled 600s after its GET succeeded.
    The fix reads growth from `temp_path_holder`, which is how the patched
    `http_get`/`xet_get` report the real path back to the monitor."""
    monkeypatch.setattr(pipeline, "STALL_CHECK_INTERVAL", 0.02)
    monkeypatch.setattr(pipeline, "STALL_TIMEOUT", 0.05)

    engine = _engine(tmp_path, monkeypatch)
    file_info = FileInfo(path="RBY1OpenDataGenConfig/train_shards/00016.tar", size=10_000_000)

    # Real observed shape: parent dirs mirror the repo path, basename is an
    # opaque hash unrelated to "00016.tar".
    hf_dir = tmp_path / ".cache" / "huggingface" / "download" / "RBY1OpenDataGenConfig" / "train_shards"
    hf_dir.mkdir(parents=True)
    hf_temp = hf_dir / "4hvnJHwtwbPSorGiDZiW8-xBOJw=.b7bd9770351e3b.a1b2c3d4.incomplete"
    hf_temp.write_bytes(b"x" * 100)

    holder = pipeline._TempPathHolder()
    holder.path = hf_temp
    cancel_event = threading.Event()

    async def run_it():
        loop = asyncio.get_running_loop()
        future = loop.create_future()

        async def grower():
            for _ in range(6):
                await asyncio.sleep(0.02)
                with open(hf_temp, "ab") as f:
                    f.write(b"x" * 100)
            if not future.done():
                future.set_result(Path("done"))

        grow_task = asyncio.create_task(grower())
        result = await engine._wait_with_growth_check(future, file_info, cancel_event, holder)
        await grow_task
        return result

    result = asyncio.run(run_it())
    assert result == Path("done")


def test_no_temp_path_locatable_is_inconclusive_not_stalled(tmp_path, monkeypatch):
    """A file for which no candidate temp path exists anywhere (no exact
    target/.incomplete/.part/._____temp path, and no reported
    `temp_path_holder`) must not be declared stalled on that basis alone —
    "cannot observe" is not evidence of "stalled". It stays inconclusive up
    to the separate `UNOBSERVABLE_TIMEOUT` (see I2 tests below for the
    ceiling itself) — not forever, and not backstopped by the activity's own
    timeout, which cannot serve that role (heartbeats fire unconditionally
    every 15s regardless of progress; start_to_close_timeout is 7 days)."""
    monkeypatch.setattr(pipeline, "STALL_CHECK_INTERVAL", 0.02)
    monkeypatch.setattr(pipeline, "STALL_TIMEOUT", 0.05)

    engine = _engine(tmp_path, monkeypatch)
    file_info = FileInfo(path="a/nope.bin", size=123)
    holder = pipeline._TempPathHolder()  # .path stays None: nothing ever observed
    cancel_event = threading.Event()

    async def run_it():
        loop = asyncio.get_running_loop()
        future = loop.create_future()

        async def finish_later():
            await asyncio.sleep(0.15)  # several STALL_TIMEOUT windows pass, unobserved
            future.set_result(Path("done"))

        finisher = asyncio.create_task(finish_later())
        result = await engine._wait_with_growth_check(future, file_info, cancel_event, holder)
        await finisher
        return result

    result = asyncio.run(run_it())
    assert result == Path("done")


def test_stall_still_detected_despite_unrelated_incomplete_growing(tmp_path, monkeypatch):
    """The original over-matching bug: growth belonging to a *different*
    file must not be credited to this one. A genuinely stalled file (its
    own residue exists but stops growing) must still be declared stalled
    even while an unrelated `.incomplete` — whose basename contains this
    file's basename as a substring, e.g. `sub_episode_1.hdf5` vs
    `episode_1.hdf5` — keeps growing nearby for far longer than a correct
    stall should take to fire. (A finite/short-lived "other" grower would
    make this test pass vacuously once it stops, even against the old
    substring-crediting bug — it must outlast the bounded wait below.)"""
    monkeypatch.setattr(pipeline, "STALL_CHECK_INTERVAL", 0.02)
    monkeypatch.setattr(pipeline, "STALL_TIMEOUT", 0.05)

    engine = _engine(tmp_path, monkeypatch)
    file_info = FileInfo(path="dir_a/episode_1.hdf5", size=1000)

    target_incomplete = tmp_path / "dir_a" / "episode_1.hdf5.incomplete"
    target_incomplete.parent.mkdir(parents=True)
    target_incomplete.write_bytes(b"x" * 10)  # this file's own progress: stuck

    other_dir = tmp_path / ".cache" / "huggingface" / "download" / "dir_b"
    other_dir.mkdir(parents=True)
    other = other_dir / "sub_episode_1.hdf5.incomplete"
    other.write_bytes(b"y" * 10)

    holder = pipeline._TempPathHolder()
    cancel_event = threading.Event()

    async def run_it():
        loop = asyncio.get_running_loop()
        future = loop.create_future()  # never resolves — only the stall should end this

        async def grow_other_forever():
            # Outlives the bounded wait below by a wide margin (5s vs 1s) —
            # a correct fix must declare the stall well before this runs out.
            for _ in range(250):
                await asyncio.sleep(0.02)
                with open(other, "ab") as f:
                    f.write(b"y" * 10)

        grower = asyncio.create_task(grow_other_forever())
        try:
            with pytest.raises(pipeline._StallDetected):
                await asyncio.wait_for(
                    engine._wait_with_growth_check(future, file_info, cancel_event, holder),
                    timeout=1.0,
                )
        finally:
            grower.cancel()
            future.cancel()

    asyncio.run(run_it())


# ---------------------------------------------------------------------------
# I2 — "cannot observe" needs its own ceiling, separate from STALL_TIMEOUT.
# ---------------------------------------------------------------------------

def test_never_observable_stalls_after_unobservable_timeout_not_before(tmp_path, monkeypatch):
    """A file whose temp path never appears at all (e.g. blocked in HTTP
    etag resolution before huggingface_hub creates any temp file) must
    eventually be declared stalled — the pre-I2 code reset an unbounded
    clock on every unobservable check, so this could hang for the
    activity's full 7-day start_to_close_timeout with no recovery
    (_speed_reporter heartbeats unconditionally every 15s regardless of
    progress, so the 10-minute heartbeat_timeout can never fire either).
    UNOBSERVABLE_TIMEOUT is the only backstop, and it must not fire early
    either — STALL_TIMEOUT is set high here so only the unobservable clock
    can be the one that trips."""
    monkeypatch.setattr(pipeline, "STALL_CHECK_INTERVAL", 0.02)
    monkeypatch.setattr(pipeline, "STALL_TIMEOUT", 600)
    monkeypatch.setattr(pipeline, "UNOBSERVABLE_TIMEOUT", 0.08)

    engine = _engine(tmp_path, monkeypatch)
    file_info = FileInfo(path="a/nope.bin", size=123)
    holder = pipeline._TempPathHolder()  # .path stays None: nothing ever observed
    cancel_event = threading.Event()

    async def run_it():
        loop = asyncio.get_running_loop()
        future = loop.create_future()  # never resolves on its own
        start = time.monotonic()
        try:
            with pytest.raises(pipeline._StallDetected) as exc_info:
                await asyncio.wait_for(
                    engine._wait_with_growth_check(future, file_info, cancel_event, holder),
                    timeout=2.0,
                )
        finally:
            future.cancel()
        elapsed = time.monotonic() - start
        return exc_info.value, elapsed

    exc, elapsed = asyncio.run(run_it())
    assert elapsed >= pipeline.UNOBSERVABLE_TIMEOUT, (
        "must not fire before UNOBSERVABLE_TIMEOUT has actually elapsed"
    )
    assert "no temp path" in str(exc).lower(), (
        "the unobservable case must say no temp path was ever locatable, "
        "not 'no growth for Ns' — the two must be distinguishable in logs"
    )


def test_late_appearing_temp_path_moves_off_unobservable_clock(tmp_path, monkeypatch):
    """Once a candidate path appears, the file must move onto the growth
    clock and off the unobservable one — and if it later goes unobservable
    again, that must start a *fresh* unobservable clock, not resume the one
    from before it was ever seen. A buggy implementation that resets
    `unobservable_since` to a new timestamp only the first time (or never
    clears it back to None while observable) would carry the original,
    stale start time into the second unobservable phase and see it as
    already past `UNOBSERVABLE_TIMEOUT` the instant the path disappears
    again — even though barely any time has passed since it actually went
    unobservable. This test's three phases (unobservable, then observed and
    growing, then unobservable again) only pass under a correct reset."""
    monkeypatch.setattr(pipeline, "STALL_CHECK_INTERVAL", 0.02)
    monkeypatch.setattr(pipeline, "STALL_TIMEOUT", 600)
    monkeypatch.setattr(pipeline, "UNOBSERVABLE_TIMEOUT", 0.3)

    engine = _engine(tmp_path, monkeypatch)
    file_info = FileInfo(path="a/late.bin", size=1000)
    holder = pipeline._TempPathHolder()
    cancel_event = threading.Event()

    temp_path = tmp_path / "a" / "late.bin.incomplete"

    async def run_it():
        loop = asyncio.get_running_loop()
        future = loop.create_future()

        async def appear_grow_then_vanish():
            # Phase 1: unobservable, well under UNOBSERVABLE_TIMEOUT (0.3s).
            await asyncio.sleep(0.05)
            # Phase 2: observed and growing — must reset the unobservable
            # clock to None.
            temp_path.parent.mkdir(parents=True, exist_ok=True)
            temp_path.write_bytes(b"x" * 50)
            for _ in range(5):
                await asyncio.sleep(0.02)
                with open(temp_path, "ab") as f:
                    f.write(b"x" * 50)
            # Phase 3: unobservable again. A fresh clock tolerates this
            # (0.2s < UNOBSERVABLE_TIMEOUT); a stale clock counted from
            # phase 1's start (~0s) would already read ~0.25s elapsed here
            # and keep accumulating past 0.3s well before this phase ends.
            temp_path.unlink()
            await asyncio.sleep(0.2)
            if not future.done():
                future.set_result(Path("done"))

        grow_task = asyncio.create_task(appear_grow_then_vanish())
        result = await asyncio.wait_for(
            engine._wait_with_growth_check(future, file_info, cancel_event, holder),
            timeout=5.0,
        )
        await grow_task
        return result

    result = asyncio.run(run_it())
    assert result == Path("done")


# ---------------------------------------------------------------------------
# M2 — one candidate's OSError must not mask growth on another candidate.
# ---------------------------------------------------------------------------

def test_growth_on_healthy_candidate_survives_error_on_another(tmp_path, monkeypatch):
    """The old code wrapped a single `except OSError` around the whole
    candidate scan, so an error reading one candidate (e.g. a TOCTOU race —
    `exists()` true at check time, gone or unreadable by the time `stat()`
    runs) skipped every candidate after it in the list, including a
    healthy, growing one. With I2's ceiling in place, that masked growth
    would push a healthy download onto the unobservable clock. `.incomplete`
    is checked before the reported temp-path holder in
    `_residue_candidates`, so breaking `.incomplete` while the real growth
    lives at the holder path reproduces the exact ordering that let this
    bug hide real progress. `UNOBSERVABLE_TIMEOUT` is set short here so a
    masking regression would show up as a false stall within the test's
    bounded runtime."""
    monkeypatch.setattr(pipeline, "STALL_CHECK_INTERVAL", 0.02)
    monkeypatch.setattr(pipeline, "STALL_TIMEOUT", 600)
    monkeypatch.setattr(pipeline, "UNOBSERVABLE_TIMEOUT", 0.05)

    engine = _engine(tmp_path, monkeypatch)
    file_info = FileInfo(path="dir_a/broken.hdf5", size=1000)

    broken_incomplete = tmp_path / "dir_a" / "broken.hdf5.incomplete"
    broken_incomplete.parent.mkdir(parents=True)
    broken_incomplete.write_bytes(b"x")

    real_stat = Path.stat

    def flaky_stat(self, *a, **k):
        if self == broken_incomplete:
            raise OSError("simulated stat failure (TOCTOU)")
        return real_stat(self, *a, **k)

    monkeypatch.setattr(Path, "stat", flaky_stat)

    holder = pipeline._TempPathHolder()
    holder.path = tmp_path / ".cache" / "huggingface" / "download" / "HASH.incomplete"
    holder.path.parent.mkdir(parents=True)
    holder.path.write_bytes(b"x" * 100)  # the real, healthy growth

    cancel_event = threading.Event()

    async def run_it():
        loop = asyncio.get_running_loop()
        future = loop.create_future()

        async def grower():
            # Outlasts UNOBSERVABLE_TIMEOUT (0.05s) by a wide margin — a
            # masking regression must show up as a stall well before this
            # completes.
            for _ in range(8):
                await asyncio.sleep(0.02)
                with open(holder.path, "ab") as f:
                    f.write(b"x" * 100)
            if not future.done():
                future.set_result(Path("done"))

        grow_task = asyncio.create_task(grower())
        result = await engine._wait_with_growth_check(future, file_info, cancel_event, holder)
        await grow_task
        return result

    result = asyncio.run(run_it())
    assert result == Path("done"), (
        "growth on the healthy holder-path candidate must be observed "
        "despite the broken .incomplete candidate earlier in the list"
    )


# ---------------------------------------------------------------------------
# Defect 1 — a cancelled download task was not counted as a failure.
# ---------------------------------------------------------------------------

def test_cancelled_download_counts_as_failed_exactly_once(tmp_path, monkeypatch):
    """asyncio.CancelledError is a BaseException, not an Exception, since
    Python 3.8 — `isinstance(r, Exception)` alone let every cancelled
    download slip past `_producer`'s failure count uncounted."""
    engine = _engine(tmp_path, monkeypatch)
    engine._executor = ThreadPoolExecutor(max_workers=2)
    engine._concurrency = 2

    file_info = FileInfo(path="a/b.bin", size=100)

    monkeypatch.setattr(engine, "_download_one_file", lambda *a, **k: None)

    async def cancelled(*args, **kwargs):
        raise asyncio.CancelledError()

    monkeypatch.setattr(engine, "_wait_with_growth_check", cancelled)

    queue = asyncio.Queue()
    asyncio.run(engine._producer([file_info], queue))

    assert engine.stats.failed_files == 1


# ---------------------------------------------------------------------------
# Defect 2 — a cancelled upload task used to kill `_consumer`.
# ---------------------------------------------------------------------------

def test_cancelled_upload_does_not_kill_consumer(tmp_path, monkeypatch):
    """`Task.exception()` raises CancelledError for a cancelled task instead
    of returning it, so a single cancelled upload used to propagate out of
    `_consumer`, abandoning every remaining queued upload while the
    producer kept filling a queue nobody was left to drain."""
    engine = _engine(tmp_path, monkeypatch)

    uploaded = []

    async def fake_upload_one(fi, sem):
        if fi.path == "cancel_me.bin":
            raise asyncio.CancelledError()
        uploaded.append(fi.path)

    engine._upload_one = fake_upload_one

    queue = asyncio.Queue()
    file_a = FileInfo(path="cancel_me.bin", size=10)
    file_b = FileInfo(path="ok.bin", size=10)

    async def run_it():
        await queue.put(file_a)
        await queue.put(file_b)
        await queue.put(None)
        await engine._consumer(queue)

    asyncio.run(run_it())

    assert uploaded == ["ok.bin"], "the second, non-cancelled upload must still run"
    assert engine.stats.failed_files == 1
    assert engine.stats.phase == "done", "_consumer must reach normal completion, not die mid-way"


# ---------------------------------------------------------------------------
# Defect 6 — no pre-upload size check, and uploaded_bytes credited the
# claimed size instead of what was actually uploaded.
# ---------------------------------------------------------------------------

def test_pre_upload_size_mismatch_is_failed_and_not_uploaded(tmp_path, monkeypatch):
    """An upload whose local file size disagrees with `fi.size` must be
    counted failed and never sent to BOS — the second, independent guard
    against exactly the 0-byte-object failure mode defect 5 already
    prevents at the download step."""
    import dlm.core.bos as bos_mod

    engine = _engine(tmp_path, monkeypatch)
    engine._bos_client = object()
    engine._bucket = "bucket"
    engine._prefix = "prefix/"

    local = tmp_path / "f.bin"
    local.write_bytes(b"")  # 0 bytes on disk — the RoboDojo failure mode

    fi = FileInfo(path="f.bin", size=900_000_000)  # source claims ~900MB

    called = []
    monkeypatch.setattr(bos_mod, "upload_file", lambda *a, **k: called.append(a))

    asyncio.run(engine._upload_one(fi, asyncio.Semaphore(1)))

    assert called == [], "must never call upload_file for a mismatched file"
    assert local.exists(), "must not delete the local file on a failed pre-check"
    assert engine.stats.failed_files == 1
    assert engine.stats.uploaded_bytes == 0


def test_uploaded_bytes_credits_actual_size_not_claimed_size(tmp_path, monkeypatch):
    """`uploaded_bytes` must reflect what was actually uploaded. Crediting
    `fi.size` (the source's claim) instead is why the RoboDojo dashboard
    showed ~900MB uploaded for each of 103 objects that landed on BOS as
    0-byte objects. Use an unknown source size (0) to force a case where
    the claimed and actual sizes could diverge — if the code credited
    `fi.size` here it would record 0, not the real 777 bytes on disk."""
    import dlm.core.bos as bos_mod

    engine = _engine(tmp_path, monkeypatch)
    engine._bos_client = object()
    engine._bucket = "bucket"
    engine._prefix = "prefix/"

    local = tmp_path / "h.bin"
    local.write_bytes(b"z" * 777)

    fi = FileInfo(path="h.bin", size=0)  # source listing reported no size

    monkeypatch.setattr(bos_mod, "upload_file", lambda *a, **k: None)

    asyncio.run(engine._upload_one(fi, asyncio.Semaphore(1)))

    assert engine.stats.uploaded_bytes == 777
    assert engine.stats.uploaded_files == 1
    assert not local.exists()

