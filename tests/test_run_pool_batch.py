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

import pytest

from dlm.core.naming import shard_row_id
from dlm.temporal import activities
from dlm.temporal.models import PipelineStats, TaskInput
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

    def __init__(self, task_input, staging_dir, heartbeat_fn, progress_fn=None):
        self.task_input = task_input
        self.staging_dir = staging_dir
        self.heartbeat_fn = heartbeat_fn
        self.progress_fn = progress_fn
        self.run_called_with = None
        _FakePipelineEngine.instances.append(self)

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

    async def fake_check_disk_space(min_free_gb=25):
        return True

    monkeypatch.setattr(activities, "check_disk_space", fake_check_disk_space)
    monkeypatch.setattr("dlm.temporal.pipeline.PipelineEngine", _FakePipelineEngine)
    _FakePipelineEngine.instances.clear()

    return calls


def _calls_by_path(calls, suffix):
    return [(u, b) for u, b in calls if u.endswith(suffix)]


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
    done_idx = next(i for i, (u, b) in enumerate(calls)
                     if u.endswith("/api/shards/status") and b.get("status") == "done")
    assert assign_idx < running_idx < done_idx

    # assign body carries this worker's server key; batch row id convention
    # matches T1's shard_row_id(task_id, batch_index)
    assign_body = calls[assign_idx][1]
    expected_batch_id = shard_row_id("t-1", 3)
    assert assign_body == {"shard_id": expected_batch_id, "server_key": "w9"}


def test_disk_floor_raises_retryable_without_any_status_post(pool_batch_env, monkeypatch):
    calls = pool_batch_env

    async def fake_check_disk_space(min_free_gb=25):
        assert min_free_gb == activities.POOL_DISK_FLOOR_GB
        return False

    monkeypatch.setattr(activities, "check_disk_space", fake_check_disk_space)

    with pytest.raises(activities._RetryableDiskLow):
        _run_activity(activities.run_pool_batch, _task_input(), 0, "k")

    # assign + running were posted (spec: those happen before the disk
    # check), but nothing about done/failed ever went out.
    assert len(_calls_by_path(calls, "/api/shards/assign")) == 1
    status_calls = _calls_by_path(calls, "/api/shards/status")
    assert all(b.get("status") == "running" for _, b in status_calls)
    assert not _FakePipelineEngine.instances


def test_engine_failure_raises_and_never_posts_failed_status(pool_batch_env, monkeypatch):
    calls = pool_batch_env
    monkeypatch.setattr("dlm.temporal.pipeline.PipelineEngine", _FailingPipelineEngine)

    with pytest.raises(RuntimeError, match="w9"):
        _run_activity(activities.run_pool_batch, _task_input(), 7, "k")

    statuses_posted = [b.get("status") for u, b in calls if u.endswith("/api/shards/status")]
    assert "failed" not in statuses_posted
    assert "done" not in statuses_posted


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
