"""T4 — run_pool_batch: pool execution activity.

Covers the pure HEAD-skip filter, the ignored-response short-circuit, the
assign+status(running)-before-download ordering, the coexistence disk floor,
raise-without-failed-post on engine failure, and the done-post payload shape
on success.

The real PipelineEngine is never exercised here (no network) — orchestration
tests swap in a fake with the exact constructor+run surface `run_pool_batch`
actually calls, and a dedicated signature test keeps that fake honest against
the real engine so drift breaks loudly.

Run: python3 -m pytest tests/test_run_pool_batch.py -q   (needs temporalio)
"""

from __future__ import annotations

import asyncio
import inspect
import json
import time
from pathlib import Path

import pytest

from dlm.core.naming import shard_row_id
from dlm.temporal import activities
from dlm.temporal.models import (
    FAIL_ACCESS_DENIED,
    FAIL_DOWNLOAD_RETRIES_EXHAUSTED,
    FAIL_SIZE_MISMATCH,
    FAIL_STAGED_FILE_MISSING,
    FAIL_UPLOAD_CANCELLED,
    FAIL_UPSTREAM_EMPTY,
    POOL_BATCH_FAIL_MAX,
    POOL_BATCH_MAX_ATTEMPTS,
    PipelineStats,
    TaskInput,
)
from dlm.temporal.pipeline import PipelineEngine


def _task_input(task_id="t-1", name="pool-task", category="manipulation"):
    return TaskInput(id=task_id, name=name, repo_id="org/repo", source="hf",
                      type="dataset", category=category)


# ── _head_skip_filter: pure logic over a fake BOS client ────────────────


class _FakeMeta:
    def __init__(self, size):
        self.content_length = size


class _FakeHeadResponse:
    def __init__(self, size):
        self.metadata = _FakeMeta(size)


class _FakeBosClient:
    """existing: {key: size} — get_object_meta_data raises for missing keys,
    mirroring the real SDK's behavior on a 404."""

    def __init__(self, existing: dict):
        self.existing = existing

    def get_object_meta_data(self, bucket, key):
        if key not in self.existing:
            raise RuntimeError(f"404: {key}")
        return _FakeHeadResponse(self.existing[key])


@pytest.fixture
def fake_bos_config(monkeypatch):
    monkeypatch.setattr(
        "dlm.core.config.load_config",
        lambda: {"BAIDU_AK": "ak", "BAIDU_SK": "sk", "BOS_ENDPOINT": "https://x"},
    )


def _install_fake_bos_client(monkeypatch, existing: dict):
    monkeypatch.setattr(
        "dlm.core.bos.create_bos_client",
        lambda ak, sk, endpoint: _FakeBosClient(existing),
    )


def test_head_skip_drops_files_present_with_matching_size(fake_bos_config, monkeypatch):
    task = _task_input()
    # bos_target(task) -> ("auwomo-data", "manipulation/pool-task/")
    _install_fake_bos_client(monkeypatch, {
        "manipulation/pool-task/a.bin": 100,   # present, matching size -> skip
        "manipulation/pool-task/b.bin": 999,   # present, WRONG size -> keep
    })
    files = [
        {"path": "a.bin", "size": 100},
        {"path": "b.bin", "size": 200},
        {"path": "c.bin", "size": 50},   # not on BOS at all -> keep
    ]

    remaining, skipped_files, skipped_bytes = activities._head_skip_filter(files, task)

    assert skipped_files == 1
    assert skipped_bytes == 100
    assert sorted(f["path"] for f in remaining) == ["b.bin", "c.bin"]


def test_head_skip_empty_filelist_is_a_noop(fake_bos_config, monkeypatch):
    _install_fake_bos_client(monkeypatch, {})
    remaining, skipped_files, skipped_bytes = activities._head_skip_filter([], _task_input())
    assert remaining == []
    assert skipped_files == 0
    assert skipped_bytes == 0


# ── fake PipelineEngine — surface must match the real one ───────────────


class _FakePipelineEngine:
    """Same constructor+run surface as PipelineEngine; see the signature
    test below that keeps this honest against the real class."""

    instances = []
    call_log = None  # set by pool_batch_env to the shared POST log, so
                     # ordering assertions can place engine start among the
                     # HTTP calls rather than only relative to each other

    def __init__(self, task_input, staging_dir, heartbeat_fn, progress_fn=None):
        self.task_input = task_input
        self.staging_dir = staging_dir
        self.heartbeat_fn = heartbeat_fn
        self.progress_fn = progress_fn
        self.run_called_with = None
        _FakePipelineEngine.instances.append(self)
        if _FakePipelineEngine.call_log is not None:
            _FakePipelineEngine.call_log.append(("<engine-start>", None))

    async def run(self, files):
        self.run_called_with = files
        stats = PipelineStats()
        stats.total_files = len(files)
        stats.downloaded_files = len(files)
        stats.uploaded_files = len(files)
        stats.uploaded_bytes = sum(f.size for f in files)
        stats.total_bytes = stats.uploaded_bytes
        stats.failed_files = 0
        return stats


class _FailingPipelineEngine(_FakePipelineEngine):
    async def run(self, files):
        stats = await super().run(files)
        stats.failed_files = len(files) or 1
        return stats


def _names_and_defaults(sig: inspect.Signature):
    """(name, default) pairs, ignoring type annotations — those are allowed
    to differ (or be absent on the fake) without this guard's intent (catch
    a changed parameter name/order/default) being defeated."""
    return [(p.name, p.default) for p in sig.parameters.values()]


def test_fake_engine_surface_matches_real_pipeline_engine():
    """Drift guard: if PipelineEngine's constructor or run() signature ever
    changes shape (params added/removed/renamed/reordered, or a default
    changes), this fails loudly instead of the orchestration tests below
    silently testing a stale contract."""
    assert _names_and_defaults(inspect.signature(_FakePipelineEngine.__init__)) == \
        _names_and_defaults(inspect.signature(PipelineEngine.__init__))
    assert _names_and_defaults(inspect.signature(_FakePipelineEngine.run)) == \
        _names_and_defaults(inspect.signature(PipelineEngine.run))


# ── orchestration: requests.post / check_disk_space / download / engine mocked ──


class _FakeResponse:
    def __init__(self, payload, status_ok=True):
        self._payload = payload
        self._status_ok = status_ok

    def raise_for_status(self):
        if not self._status_ok:
            raise RuntimeError("http error")

    def json(self):
        return self._payload


def _run_activity(coro_fn, *args, **kwargs):
    from temporalio.testing import ActivityEnvironment

    env = ActivityEnvironment()

    async def main():
        return await env.run(coro_fn, *args, **kwargs)

    return asyncio.run(main())


@pytest.fixture
def pool_batch_env(tmp_path, monkeypatch):
    """Common wiring for run_pool_batch orchestration tests: isolated
    staging root, a recording fake `requests.post`, a fake batch manifest
    download, a fake HEAD-skip (0 skipped by default), and DLM_SERVER_KEY."""
    monkeypatch.setattr(activities, "STAGING_PATH", tmp_path)
    monkeypatch.setenv("DLM_SERVER_KEY", "w9")

    calls = []

    def fake_post(url, json=None, timeout=None):
        calls.append((url, json))
        return _FakeResponse({"ok": True})

    monkeypatch.setattr("requests.post", fake_post)

    async def fake_download_shard_filelist(filelist_key, staging_dir):
        from pathlib import Path
        p = Path(staging_dir) / "manifest.json"
        p.write_text(json.dumps([{"path": "a.bin", "size": 10},
                                  {"path": "b.bin", "size": 20}]))
        return str(p)

    monkeypatch.setattr(activities, "download_shard_filelist", fake_download_shard_filelist)
    monkeypatch.setattr(activities, "_head_skip_filter",
                         lambda files, task_input: (files, 0, 0))

    # Real disk_usage on the test machine would make these tests
    # environment-dependent; pin a comfortably-above-floor volume.
    monkeypatch.setattr(activities, "_pool_disk_floor_gb", lambda: (52, 150.0, 200.0))
    monkeypatch.setattr("dlm.temporal.pipeline.PipelineEngine", _FakePipelineEngine)
    _FakePipelineEngine.instances.clear()
    _FakePipelineEngine.call_log = calls

    yield calls

    _FakePipelineEngine.call_log = None


def _calls_by_path(calls, suffix):
    return [(u, b) for u, b in calls if u.endswith(suffix)]


def _liveness_beats(calls):
    """Preflight liveness writes, told apart from real progress reports by the
    absence of `done_bytes` — which is exactly what makes them safe to send."""
    return [b for u, b in _calls_by_path(calls, "/api/shard-progress")
            if "done_bytes" not in (b or {})]


def test_ignored_assign_short_circuits_before_download(pool_batch_env, monkeypatch, tmp_path):
    calls = pool_batch_env

    def fake_post(url, json=None, timeout=None):
        calls.append((url, json))
        if url.endswith("/api/shards/assign"):
            return _FakeResponse({"ok": True, "ignored": True})
        raise AssertionError(f"unexpected POST after ignored assign: {url}")

    monkeypatch.setattr("requests.post", fake_post)

    downloaded = []

    async def spy_download(filelist_key, staging_dir):
        downloaded.append(filelist_key)
        raise AssertionError("must not download after an ignored assign")

    monkeypatch.setattr(activities, "download_shard_filelist", spy_download)

    result = _run_activity(activities.run_pool_batch, _task_input(), 0, "download-manager/batchlists/pool-task/batch-0.json")

    assert result == {"ignored": True}
    assert not downloaded
    assert not _FakePipelineEngine.instances
    assert len(_calls_by_path(calls, "/api/shards/assign")) == 1
    assert len(_calls_by_path(calls, "/api/shards/status")) == 0


def test_ignored_status_running_short_circuits_before_download(pool_batch_env, monkeypatch):
    calls = pool_batch_env

    def fake_post(url, json=None, timeout=None):
        calls.append((url, json))
        if url.endswith("/api/shards/assign"):
            return _FakeResponse({"ok": True})
        if url.endswith("/api/shards/status") and json.get("status") == "running":
            return _FakeResponse({"ok": True, "ignored": True})
        raise AssertionError(f"unexpected POST: {url} {json}")

    monkeypatch.setattr("requests.post", fake_post)

    result = _run_activity(activities.run_pool_batch, _task_input(), 0, "k")

    assert result == {"ignored": True}
    assert not _FakePipelineEngine.instances


def test_assign_and_running_status_posted_before_download(pool_batch_env):
    calls = pool_batch_env

    result = _run_activity(activities.run_pool_batch, _task_input(task_id="t-1"), 3, "download-manager/batchlists/pool-task/batch-3.json")

    assert result["ignored"] is False
    urls = [u for u, _ in calls]
    assign_idx = next(i for i, u in enumerate(urls) if u.endswith("/api/shards/assign"))
    running_idx = next(i for i, (u, b) in enumerate(calls)
                        if u.endswith("/api/shards/status") and b.get("status") == "running")
    # engine start is in the same log, so "before download" is asserted
    # against the download actually starting, not just against the done post
    engine_idx = urls.index("<engine-start>")
    done_idx = next(i for i, (u, b) in enumerate(calls)
                     if u.endswith("/api/shards/status") and b.get("status") == "done")
    assert assign_idx < running_idx < engine_idx < done_idx

    # assign body carries this worker's server key; batch row id convention
    # matches T1's shard_row_id(task_id, batch_index)
    assign_body = calls[assign_idx][1]
    expected_batch_id = shard_row_id("t-1", 3)
    assert assign_body == {"shard_id": expected_batch_id, "server_key": "w9"}


# ── preflight liveness: the phase that used to write nothing to SQLite ──────
# A Temporal heartbeat is invisible to SQLite. Between `status=running` and
# `engine.run()` (batch-list fetch + up to BATCH_MAX_FILES BOS HEADs) nothing
# used to touch the row, while POOL_BATCH_HEARTBEAT gives that phase 10 minutes
# and dlm.web.fleet.POOL_LIVE_BATCH_WINDOW_S calls a row stale after 240s — so
# a slow BOS made a live batch read as a queue with no live consumer.


def test_the_head_sweep_keeps_vouching_for_a_live_batch(pool_batch_env, monkeypatch):
    """The HEAD sweep must write to SQLite while it runs, not just after."""
    calls = pool_batch_env
    monkeypatch.setattr(activities, "POOL_PREFLIGHT_BEAT_S", 0.01)

    def slow_head_skip(files, task_input):
        # Stands in for a slow BOS. Runs in a worker thread (the activity hands
        # it to asyncio.to_thread), so blocking here does not block the beater.
        deadline = time.time() + 5
        while time.time() < deadline and not _liveness_beats(calls):
            time.sleep(0.01)
        return files, 0, 0

    monkeypatch.setattr(activities, "_head_skip_filter", slow_head_skip)

    result = _run_activity(activities.run_pool_batch,
                           _task_input(task_id="t-1"), 3, "k")

    assert result["ignored"] is False
    beats = _liveness_beats(calls)
    assert beats, ("the BOS HEAD sweep wrote nothing to SQLite: the row goes "
                   "stale past POOL_LIVE_BATCH_WINDOW_S and a live batch reads "
                   "as a dead pool")
    # `shard_id` alone. /api/shard-progress falls back to the row's current
    # done_files/done_bytes for absent fields, so a retried batch's recorded
    # progress cannot be walked back to zero by a liveness write.
    assert beats[0] == {"shard_id": shard_row_id("t-1", 3)}

    urls = [u for u, _ in calls]
    running_idx = next(i for i, (u, b) in enumerate(calls)
                       if u.endswith("/api/shards/status") and b.get("status") == "running")
    beat_idx = next(i for i, (u, b) in enumerate(calls)
                    if u.endswith("/api/shard-progress") and "done_bytes" not in b)
    assert running_idx < beat_idx < urls.index("<engine-start>")


def test_no_liveness_beat_before_the_batch_row_is_running(pool_batch_env, monkeypatch):
    """Vouching early would vouch for the wrong thing.

    Before the assign lands, this worker may still be told to drop the batch;
    and fleet.py's liveness probe reads running batch rows only, so bumping a
    still-`pending` row is evidence of nothing.
    """
    calls = pool_batch_env
    monkeypatch.setattr(activities, "POOL_PREFLIGHT_BEAT_S", 0.005)

    def fake_post(url, json=None, timeout=None):
        calls.append((url, json))
        if url.endswith("/api/shards/assign"):
            time.sleep(0.1)          # ~20 beat intervals to fire in
        return _FakeResponse({"ok": True})

    monkeypatch.setattr("requests.post", fake_post)

    _run_activity(activities.run_pool_batch, _task_input(), 0, "k")

    running_idx = next(i for i, (u, b) in enumerate(calls)
                       if u.endswith("/api/shards/status") and b.get("status") == "running")
    assert not [u for u, _ in calls[:running_idx]
                if u.endswith("/api/shard-progress")]


def test_a_failed_liveness_beat_does_not_fail_the_batch(pool_batch_env, monkeypatch):
    """The write is evidence, not a checkpoint.

    A wedged S1 is a known failure mode; losing a liveness report costs us one
    stale-row sample, while raising here would kill a batch that is otherwise
    downloading fine — and burn one of its 3 attempts.
    """
    calls = pool_batch_env
    monkeypatch.setattr(activities, "POOL_PREFLIGHT_BEAT_S", 0.01)
    attempted = []

    def fake_post(url, json=None, timeout=None):
        calls.append((url, json))
        if url.endswith("/api/shard-progress") and "done_bytes" not in (json or {}):
            attempted.append(json)
            raise RuntimeError("S1 wedged")
        return _FakeResponse({"ok": True})

    monkeypatch.setattr("requests.post", fake_post)

    def slow_head_skip(files, task_input):
        deadline = time.time() + 5
        while time.time() < deadline and not attempted:
            time.sleep(0.01)
        return files, 0, 0

    monkeypatch.setattr(activities, "_head_skip_filter", slow_head_skip)

    result = _run_activity(activities.run_pool_batch, _task_input(), 0, "k")

    assert attempted, "the beat never fired, so this proves nothing"
    assert result["ignored"] is False
    assert _FakePipelineEngine.instances, "the batch must still have run"


def test_disk_floor_raises_retryable_without_any_status_post(pool_batch_env, monkeypatch):
    calls = pool_batch_env

    # free (10GB) below the computed floor (52GB) on a 200GB volume
    monkeypatch.setattr(activities, "_pool_disk_floor_gb", lambda: (52, 10.0, 200.0))

    with pytest.raises(activities._RetryableDiskLow) as excinfo:
        _run_activity(activities.run_pool_batch, _task_input(), 0, "k")

    # the message must let an operator act without reading the code: what was
    # free, what was required, on how big a volume, and what to override
    msg = str(excinfo.value)
    assert "10.0GB free" in msg and "52GB floor" in msg
    assert "200GB staging volume" in msg and "min_free_gb" in msg

    # assign + running were posted (spec: those happen before the disk
    # check), but nothing about done/failed ever went out.
    assert len(_calls_by_path(calls, "/api/shards/assign")) == 1
    status_calls = _calls_by_path(calls, "/api/shards/status")
    assert all(b.get("status") == "running" for _, b in status_calls)
    assert not _FakePipelineEngine.instances


def test_explicit_min_free_gb_overrides_computed_floor(pool_batch_env, monkeypatch):
    """The coordinator owns the fleet-wide floor; an explicit parameter wins
    over the relative default in both directions."""
    # computed floor would reject (free 30 < 52), the override accepts
    monkeypatch.setattr(activities, "_pool_disk_floor_gb", lambda: (52, 30.0, 200.0))
    result = _run_activity(activities.run_pool_batch, _task_input(), 0, "k", 25)
    assert result["ignored"] is False

    # computed floor would accept (free 100 > 52), the override rejects
    monkeypatch.setattr(activities, "_pool_disk_floor_gb", lambda: (52, 100.0, 200.0))
    with pytest.raises(activities._RetryableDiskLow, match="120GB floor"):
        _run_activity(activities.run_pool_batch, _task_input(), 0, "k", 120)


def test_pool_disk_floor_tracks_the_engines_own_backpressure_line():
    """Drift guard for the floor's formula shape. The two constants are
    imported from the engine, so they can't drift; what can drift is the
    engine's *formula* — if `_disk_free_threshold_gb` stops being
    `max(total * PCT, ABSOLUTE_MIN)`, the pool floor is no longer derived
    from the line it claims to track."""
    import shutil as shutil_mod
    from dlm.temporal import pipeline

    src = inspect.getsource(pipeline._disk_free_threshold_gb)
    assert "max(total_gb * DISK_FREE_MIN_PCT, DISK_FREE_ABSOLUTE_MIN_GB)" in src


def test_pool_disk_floor_is_relative_to_volume_size(monkeypatch, tmp_path):
    """A fixed floor is unsatisfiable in principle on a small volume; the
    default tracks the engine's own backpressure line (pipeline.py:91-95)
    plus one batch's headroom."""
    import collections
    import shutil as shutil_mod

    monkeypatch.setattr(activities, "STAGING_PATH", tmp_path)
    Usage = collections.namedtuple("Usage", "total used free")
    gib = 1024 ** 3

    # 2TB volume: 30% of total dominates the 20GB absolute minimum, and the
    # half-volume cap is far above the sum
    monkeypatch.setattr(shutil_mod, "disk_usage",
                        lambda p: Usage(2000 * gib, 0, 900 * gib))
    floor, free, total = activities._pool_disk_floor_gb()
    assert floor == int(2000 * 0.30 + 32)
    assert (round(free), round(total)) == (900, 2000)

    # 200GB worker (the real fleet): floor sits just above the engine's own
    # 60GB backoff line, so a worker at ~95GB free may still take a batch
    monkeypatch.setattr(shutil_mod, "disk_usage",
                        lambda p: Usage(200 * gib, 0, 95 * gib))
    floor, _, _ = activities._pool_disk_floor_gb()
    assert floor == int(200 * 0.30 + 32) == 92
    assert 95 > floor

    # 50GB volume: the sum (20+32) would exceed half the volume, so the cap
    # keeps the floor satisfiable — a fixed 100GB never could be
    monkeypatch.setattr(shutil_mod, "disk_usage",
                        lambda p: Usage(50 * gib, 0, 45 * gib))
    floor, _, _ = activities._pool_disk_floor_gb()
    assert floor == 25
    assert floor < 50


def test_engine_failure_raises_and_never_posts_failed_status(pool_batch_env, monkeypatch):
    calls = pool_batch_env
    monkeypatch.setattr("dlm.temporal.pipeline.PipelineEngine", _FailingPipelineEngine)

    with pytest.raises(RuntimeError, match="w9"):
        _run_activity(activities.run_pool_batch, _task_input(), 7, "k")

    statuses_posted = [b.get("status") for u, b in calls if u.endswith("/api/shards/status")]
    assert "failed" not in statuses_posted
    assert "done" not in statuses_posted


def test_cancellation_posts_nothing_and_preserves_staging(pool_batch_env, monkeypatch, tmp_path):
    """Temporal cancels the activity when the task is paused mid-batch. The
    engine handles stopping its own transfers; this activity must add no
    status write of any kind and must leave staging for the resume."""
    class _CancellingPipelineEngine(_FakePipelineEngine):
        async def run(self, files):
            (Path(self.staging_dir) / "partial.bin").write_bytes(b"x" * 8)
            raise asyncio.CancelledError()

    calls = pool_batch_env
    monkeypatch.setattr("dlm.temporal.pipeline.PipelineEngine", _CancellingPipelineEngine)

    with pytest.raises(asyncio.CancelledError):
        _run_activity(activities.run_pool_batch, _task_input(), 5, "k")

    statuses_posted = [b.get("status") for u, b in calls if u.endswith("/api/shards/status")]
    assert statuses_posted == ["running"]  # nothing after running
    staging = tmp_path / "pool-task" / "pool-batch-5"
    assert (staging / "partial.bin").exists()


def test_success_reports_final_progress_then_done(pool_batch_env, monkeypatch):
    calls = pool_batch_env
    monkeypatch.setattr(activities, "_head_skip_filter",
                         lambda files, task_input: (
                             [f for f in files if f["path"] != "a.bin"],
                             1, 10,
                         ))

    result = _run_activity(activities.run_pool_batch, _task_input(), 2, "k")

    assert result["ignored"] is False
    assert result["skipped_files"] == 1
    assert result["skipped_bytes"] == 10
    assert result["uploaded_files"] == 1     # only b.bin (20 bytes) went through the engine
    assert result["uploaded_bytes"] == 20
    assert result["total_files"] == 2
    assert result["total_bytes"] == 30

    progress_calls = _calls_by_path(calls, "/api/shard-progress")
    assert len(progress_calls) == 1
    _, progress_body = progress_calls[0]
    assert progress_body["done_files"] == 2
    assert progress_body["done_bytes"] == 30
    assert progress_body["speed_mbps"] == 0

    done_calls = [(u, b) for u, b in calls if u.endswith("/api/shards/status") and b.get("status") == "done"]
    assert len(done_calls) == 1

    # progress must be posted before the terminal status write
    progress_idx = calls.index(progress_calls[0])
    done_idx = calls.index(done_calls[0])
    assert progress_idx < done_idx


def test_success_cleans_up_batch_staging_dir(pool_batch_env, tmp_path):
    _run_activity(activities.run_pool_batch, _task_input(name="cleanup-task"), 0, "k")
    assert not (tmp_path / "cleanup-task" / "pool-batch-0").exists()


def test_failure_also_cleans_up_batch_staging_dir(pool_batch_env, monkeypatch, tmp_path):
    monkeypatch.setattr("dlm.temporal.pipeline.PipelineEngine", _FailingPipelineEngine)
    with pytest.raises(RuntimeError):
        _run_activity(activities.run_pool_batch, _task_input(name="fail-task"), 0, "k")
    assert not (tmp_path / "fail-task" / "pool-batch-0").exists()


# ── T3: missing-file tolerance ──────────────────────────────────────────
#
# Until T3 a single unfetchable file killed its batch, and three killed the
# shard — the whole reason RoboDojo/RoboMIND-class tasks end `failed` with 99%
# of their bytes on BOS. The rule now is "forgive a few permanently-missing
# files, but only once every retry is spent, and only for files we have
# written down". Each test below pins one of the conditions that make up
# "only", because every one of them is a way for the tolerance to either
# never fire (leaving the bug in place) or fire too eagerly (dropping files
# with nobody the wiser).


def _engine_failing_with(*reasons, failed_count=None):
    """A PipelineEngine whose run() reports one failed file per reason.

    `failed_count` overrides the count independently of the details, which is
    how a real cancellation-heavy batch looks: `failed_files` counts things
    `failed_details` classifies as non-archivable, so count and archivable
    length are genuinely allowed to diverge.
    """
    class _E(_FakePipelineEngine):
        async def run(self, files):
            stats = await super().run(files)
            stats.failed_details = [
                {"path": f"gone-{i}.bin", "reason": reason, "size_bytes": 100 + i}
                for i, reason in enumerate(reasons)
            ]
            stats.failed_files = (
                len(reasons) if failed_count is None else failed_count
            )
            stats.uploaded_files = max(0, len(files) - stats.failed_files)
            return stats

    return _E


def _run_activity_on_attempt(attempt, coro_fn, *args, **kwargs):
    """Same as `_run_activity` but with `activity.info().attempt` pinned.

    The attempt number is the only way the activity can know whether it still
    has retries left, and every "not yet / now" assertion below turns on it.
    """
    import dataclasses

    from temporalio.testing import ActivityEnvironment

    env = ActivityEnvironment()
    env.info = dataclasses.replace(env.info, attempt=attempt)

    async def main():
        return await env.run(coro_fn, *args, **kwargs)

    return asyncio.run(main())


def test_retry_ceiling_matches_the_workflow_policy():
    """Drift guard for the pair that has to agree: the RetryPolicy decides how
    many attempts a batch gets, and the activity decides "am I on my last one"
    from POOL_BATCH_MAX_ATTEMPTS. If they diverge the tolerance either fires an
    attempt early (throwing away the retry that might land on a healthy
    worker) or never fires at all — and neither shows up as an error anywhere."""
    from dlm.temporal.models import POOL_BATCH_MAX_ATTEMPTS
    from dlm.temporal.workflows import POOL_BATCH_RETRY

    assert POOL_BATCH_RETRY.maximum_attempts == POOL_BATCH_MAX_ATTEMPTS


def test_tolerate_missing_defaults_to_false(pool_batch_env, monkeypatch):
    """The default must be the old all-or-nothing behaviour: only the
    coordinator's final re-dispatch round opts in (T4), so a batch invoked the
    way every other caller invokes it — three positional args — still fails on
    one missing file, on any attempt."""
    monkeypatch.setattr("dlm.temporal.pipeline.PipelineEngine",
                        _engine_failing_with(FAIL_UPSTREAM_EMPTY))

    with pytest.raises(RuntimeError, match="tolerate_missing=False"):
        _run_activity_on_attempt(POOL_BATCH_MAX_ATTEMPTS,
                                 activities.run_pool_batch, _task_input(), 1, "k")


@pytest.mark.parametrize("attempt", [1, POOL_BATCH_MAX_ATTEMPTS - 1])
def test_a_non_final_attempt_is_never_tolerated(pool_batch_env, monkeypatch, attempt):
    """With retries left, fail — the retry is the cheapest fix available. Most
    "missing" files are a flaky mirror or a poisoned worker, and this round is
    what cures them; forgiving early converts a recoverable file into a
    permanent hole in the dataset."""
    monkeypatch.setattr("dlm.temporal.pipeline.PipelineEngine",
                        _engine_failing_with(FAIL_UPSTREAM_EMPTY))

    with pytest.raises(RuntimeError,
                       match=f"attempt={attempt}/{POOL_BATCH_MAX_ATTEMPTS}"):
        _run_activity_on_attempt(attempt, activities.run_pool_batch,
                                 _task_input(), 1, "k", None, True)


def test_final_attempt_within_the_ceiling_is_forgiven(pool_batch_env, monkeypatch):
    """The whole point of T3: last attempt, two source-missing files, archive
    landed — the batch is judged complete and reports `done`, so the task
    finishes instead of dying at 99%."""
    calls = pool_batch_env
    monkeypatch.setattr("dlm.temporal.pipeline.PipelineEngine",
                        _engine_failing_with(FAIL_UPSTREAM_EMPTY, FAIL_ACCESS_DENIED))

    result = _run_activity_on_attempt(POOL_BATCH_MAX_ATTEMPTS,
                                      activities.run_pool_batch,
                                      _task_input(), 4, "k", None, True)

    assert result["ignored"] is False
    done = [b for u, b in calls
            if u.endswith("/api/shards/status") and b.get("status") == "done"]
    assert len(done) == 1


def test_the_forgiven_batch_archives_exactly_what_it_gave_up_on(pool_batch_env, monkeypatch):
    """`missing_files` is the only record that survives the run, so its body is
    load-bearing: identity, reason and size per file, plus which task and which
    batch/worker produced them (that is how an operator tells "the source lost
    it" from "this one worker cannot reach it")."""
    calls = pool_batch_env
    monkeypatch.setattr("dlm.temporal.pipeline.PipelineEngine",
                        _engine_failing_with(FAIL_UPSTREAM_EMPTY, FAIL_SIZE_MISMATCH))

    _run_activity_on_attempt(POOL_BATCH_MAX_ATTEMPTS, activities.run_pool_batch,
                             _task_input(task_id="t-arch"), 6, "k", None, True)

    archive = _calls_by_path(calls, "/api/missing-files")
    assert len(archive) == 1
    _, body = archive[0]
    assert body["task_id"] == "t-arch"
    assert body["batch_index"] == 6
    assert body["server"] == "w9"
    assert body["files"] == [
        {"path": "gone-0.bin", "reason": FAIL_UPSTREAM_EMPTY, "size_bytes": 100},
        {"path": "gone-1.bin", "reason": FAIL_SIZE_MISMATCH, "size_bytes": 101},
    ]


def test_the_archive_is_written_even_when_the_batch_is_not_forgiven(pool_batch_env, monkeypatch):
    """Archiving is deliberately not gated on the tolerance decision. The two
    cases that most need evidence are exactly the ones that keep failing — a
    first-attempt loss, and a batch over the ceiling that takes the task down
    with it. "Which files are missing" is the only useful thing to know about a
    task that ends `failed`."""
    calls = pool_batch_env
    monkeypatch.setattr("dlm.temporal.pipeline.PipelineEngine",
                        _engine_failing_with(FAIL_DOWNLOAD_RETRIES_EXHAUSTED))

    with pytest.raises(RuntimeError):
        _run_activity_on_attempt(1, activities.run_pool_batch,
                                 _task_input(), 2, "k", None, True)

    archive = _calls_by_path(calls, "/api/missing-files")
    assert len(archive) == 1
    assert [f["path"] for f in archive[0][1]["files"]] == ["gone-0.bin"]


def test_over_the_ceiling_still_raises(pool_batch_env, monkeypatch):
    """The tolerance must not degenerate into "batches never fail". A systemic
    fault — credentials rotated, mirror down, disk gone — takes out far more
    than POOL_BATCH_FAIL_MAX files, and that must stay loud."""
    calls = pool_batch_env
    over = POOL_BATCH_FAIL_MAX + 1
    monkeypatch.setattr("dlm.temporal.pipeline.PipelineEngine",
                        _engine_failing_with(*[FAIL_UPSTREAM_EMPTY] * over))

    with pytest.raises(RuntimeError, match=f"ceiling={POOL_BATCH_FAIL_MAX}"):
        _run_activity_on_attempt(POOL_BATCH_MAX_ATTEMPTS, activities.run_pool_batch,
                                 _task_input(), 3, "k", None, True)

    # …and it still recorded all of them before deciding.
    assert len(_calls_by_path(calls, "/api/missing-files")[0][1]["files"]) == over


def test_the_ceiling_is_read_from_the_module_not_hardcoded(pool_batch_env, monkeypatch):
    """A batch that is over the shipped ceiling is forgiven once the ceiling is
    raised — i.e. the comparison really consults POOL_BATCH_FAIL_MAX, which is
    what makes the env var below reach the decision."""
    over = POOL_BATCH_FAIL_MAX + 1
    monkeypatch.setattr(activities, "POOL_BATCH_FAIL_MAX", over)
    monkeypatch.setattr("dlm.temporal.pipeline.PipelineEngine",
                        _engine_failing_with(*[FAIL_UPSTREAM_EMPTY] * over))

    result = _run_activity_on_attempt(POOL_BATCH_MAX_ATTEMPTS,
                                      activities.run_pool_batch,
                                      _task_input(), 3, "k", None, True)
    assert result["ignored"] is False


def _load_models_module(monkeypatch, name, **env):
    """Import dlm/temporal/models.py as a fresh, throwaway module under `env`.

    Deliberately not `importlib.reload(models)`: that would rebind the
    dataclasses every other module already holds references to, so a
    surviving reference (PipelineStats in this very file) would stop being
    the class the activity type-checks against.
    """
    import importlib.util
    import sys

    for k, v in env.items():
        monkeypatch.setenv(k, v)
    path = Path(activities.__file__).parent / "models.py"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    # @dataclass resolves annotations through sys.modules[cls.__module__];
    # an unregistered module makes that lookup return None. monkeypatch
    # unregisters it again afterwards, so the real `dlm.temporal.models`
    # every other test holds stays the only registered one.
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    return module


def test_the_ceiling_is_env_adjustable(monkeypatch):
    """Operators can widen or disable the tolerance without a code deploy —
    0 restores the pre-T3 all-or-nothing behaviour, which is the documented
    rollback if forgiveness turns out to hide something."""
    assert _load_models_module(monkeypatch, "_models_wide",
                               DLM_POOL_BATCH_FAIL_MAX="42").POOL_BATCH_FAIL_MAX == 42
    assert _load_models_module(monkeypatch, "_models_off",
                               DLM_POOL_BATCH_FAIL_MAX="0").POOL_BATCH_FAIL_MAX == 0


def test_a_nonsense_ceiling_fails_at_import(monkeypatch):
    """Read at import so a typo surfaces in the worker's startup log, not
    hours into a batch — and a negative value is a config error, not a
    silently-never-tolerate setting."""
    with pytest.raises(ValueError):
        _load_models_module(monkeypatch, "_models_bad",
                            DLM_POOL_BATCH_FAIL_MAX="-1")
    with pytest.raises(ValueError):
        _load_models_module(monkeypatch, "_models_typo",
                            DLM_POOL_BATCH_FAIL_MAX="five")


@pytest.mark.parametrize("reply", [
    pytest.param(_FakeResponse(None, status_ok=False), id="http-error"),
    pytest.param(_FakeResponse({"error": "no such task"}), id="body-error"),
    pytest.param(_FakeResponse({"ignored": True}), id="ignored"),
])
def test_an_unrecorded_loss_is_never_forgiven(pool_batch_env, monkeypatch, reply):
    """If the archive did not land, forgiving the batch would drop files with
    no record anywhere — strictly worse than today, where the batch at least
    fails loudly. So a coordinator hiccup costs us the forgiveness, not the
    record. `ignored` counts as not-landed: an operator stopped the task, so
    S1 threw the report away."""
    calls = pool_batch_env

    def fake_post(url, json=None, timeout=None):
        calls.append((url, json))
        return reply if url.endswith("/api/missing-files") else _FakeResponse({"ok": True})

    monkeypatch.setattr("requests.post", fake_post)
    monkeypatch.setattr("dlm.temporal.pipeline.PipelineEngine",
                        _engine_failing_with(FAIL_UPSTREAM_EMPTY))

    with pytest.raises(RuntimeError, match="archived=False"):
        _run_activity_on_attempt(POOL_BATCH_MAX_ATTEMPTS, activities.run_pool_batch,
                                 _task_input(), 1, "k", None, True)


def test_failures_that_are_not_about_the_file_are_never_forgiven(pool_batch_env, monkeypatch):
    """A cancelled upload counts toward `failed_files` (pipeline.py must not
    report the batch clean) but says an operator paused the task, not that the
    source lost anything — so it is not archivable, and a batch made of those
    cannot be forgiven. Nothing is posted either: there is nothing missing to
    record."""
    calls = pool_batch_env
    monkeypatch.setattr("dlm.temporal.pipeline.PipelineEngine",
                        _engine_failing_with(FAIL_UPLOAD_CANCELLED, FAIL_STAGED_FILE_MISSING))

    with pytest.raises(RuntimeError, match="archivable=0/2"):
        _run_activity_on_attempt(POOL_BATCH_MAX_ATTEMPTS, activities.run_pool_batch,
                                 _task_input(), 1, "k", None, True)

    assert _calls_by_path(calls, "/api/missing-files") == []


def test_a_partly_archivable_batch_is_not_forgiven_but_is_partly_archived(pool_batch_env, monkeypatch):
    """Mixed batch: forgiveness needs EVERY failure accounted for, so this one
    raises — but the one genuinely-missing file is still recorded, so the next
    attempt (or the operator) knows about it."""
    calls = pool_batch_env
    monkeypatch.setattr("dlm.temporal.pipeline.PipelineEngine",
                        _engine_failing_with(FAIL_UPSTREAM_EMPTY, FAIL_UPLOAD_CANCELLED))

    with pytest.raises(RuntimeError, match="archivable=1/2"):
        _run_activity_on_attempt(POOL_BATCH_MAX_ATTEMPTS, activities.run_pool_batch,
                                 _task_input(), 1, "k", None, True)

    archive = _calls_by_path(calls, "/api/missing-files")
    assert [f["reason"] for f in archive[0][1]["files"]] == [FAIL_UPSTREAM_EMPTY]


def test_an_uncounted_loss_is_never_forgiven(pool_batch_env, monkeypatch):
    """`failed_details` is required to account for every failed file
    (PipelineStats' documented invariant). If it ever does not — a new failure
    path that bumps the counter without recording the file — the batch must
    fail, not be forgiven on the strength of an incomplete archive."""
    monkeypatch.setattr("dlm.temporal.pipeline.PipelineEngine",
                        _engine_failing_with(FAIL_UPSTREAM_EMPTY, failed_count=3))

    with pytest.raises(RuntimeError, match="archivable=1/3"):
        _run_activity_on_attempt(POOL_BATCH_MAX_ATTEMPTS, activities.run_pool_batch,
                                 _task_input(), 1, "k", None, True)
