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
    "cannot observe" is not evidence of "stalled". The activity's own
    heartbeat/start_to_close timeout is the backstop for a genuine hang."""
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

