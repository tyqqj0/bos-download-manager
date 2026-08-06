"""Guards against dispatching work nothing will ever pick up, and against
retry orphaning live shard workflows.

Both defects share a failure shape: the system believes work is in flight
(task `downloading`, or a host free) while the truth on the cluster is the
opposite, and no reconciler can tell the difference afterwards.

Run: pytest tests/test_dispatch_guards.py -q
"""

from __future__ import annotations

import asyncio
import importlib.util
import sys
import time
from pathlib import Path

import pytest

from dlm.queue import snapshot
from dlm.web import temporal_client


# --- no coordinator onto a queue nobody polls -------------------------------

class _FakeClient:
    """Records whether start_workflow was reached."""

    namespace = "default"

    def __init__(self):
        self.started = []

    async def start_workflow(self, *a, **kw):
        self.started.append(kw.get("task_queue"))
        return object()


def _patch_client(monkeypatch, pollers):
    client = _FakeClient()

    async def fake_connected():
        return client

    async def fake_count(_client, _queue):
        return pollers

    monkeypatch.setattr(temporal_client, "connected_client", fake_connected)
    monkeypatch.setattr(temporal_client, "queue_poller_count", fake_count)
    return client


TASK = {"id": "t-guard-1", "name": "X", "repo_id": "org/x",
        "source": "modelscope", "category": "manipulation", "max_workers": 4}


def test_refuses_to_start_a_coordinator_on_a_queue_with_no_pollers(monkeypatch):
    """Temporal accepts such a start and the execution stays RUNNING forever:
    has_live_workflow reads true, so reconcile only records it stale and
    redispatch_orphaned skips it. The task must stay pending instead."""
    client = _patch_client(monkeypatch, 0)
    with pytest.raises(RuntimeError, match="no worker polls"):
        asyncio.run(temporal_client.start_sharded_download(
            TASK, task_queue="download-ms-workers"))
    assert client.started == []


def test_starts_normally_when_the_queue_has_pollers(monkeypatch):
    client = _patch_client(monkeypatch, 9)
    asyncio.run(temporal_client.start_sharded_download(
        TASK, task_queue="download-ms-workers"))
    assert client.started == ["download-ms-workers"]


def test_unknown_poller_count_does_not_block_dispatch(monkeypatch):
    """describe_task_queue failing must not wedge the whole dispatcher —
    unknown is not zero."""
    client = _patch_client(monkeypatch, None)
    asyncio.run(temporal_client.start_sharded_download(
        TASK, task_queue="download-workers"))
    assert client.started == ["download-workers"]


# --- retry must close live shard workflows before deleting their rows -------

def _task(task_id, status, source="modelscope"):
    snapshot.upsert_task({
        "id": task_id, "name": f"n-{task_id}", "repo_id": "org/r",
        "source": source, "type": "dataset", "category": "manipulation",
        "status": status, "server": None, "priority": 0, "size_gb": 100.0,
        "downloaded_gb": 0.0, "progress_pct": 0, "speed_mbps": 0,
        "retry_count": 0, "max_workers": 2,
    })


def _shard(shard_id, task_id, status, index=0, server="bj1"):
    snapshot.upsert_shard({
        "id": shard_id, "task_id": task_id, "shard_index": index,
        "status": status, "server": server, "total_files": 10,
        "done_files": 0, "updated_at": time.time(),
    })


def _make_task_with_running_shard(task_id="t-retry-1"):
    _task(task_id, "paused")
    _shard(f"s-{task_id}-0", task_id, "running", 0, "bj1")
    _shard(f"s-{task_id}-1", task_id, "done", 1, "bj2")
    return task_id


def test_retry_terminates_live_shards_before_clearing_rows(dlm_db, monkeypatch):
    """Order matters twice over: terminate_workflow_and_wait finds the child
    workflows *through* the shard rows, and until they are closed those rows are
    the only thing keeping the host in busy_servers."""
    from dlm.web.routes import queue as queue_routes

    task_id = _make_task_with_running_shard()
    seen = {}

    async def fake_terminate(tid, timeout_s=120):
        seen["rows_at_terminate"] = len(snapshot.get_shards_by_task(tid))
        return True

    monkeypatch.setattr(
        "dlm.web.temporal_client.terminate_workflow_and_wait", fake_terminate)
    result = asyncio.run(queue_routes.retry_task({"task_id": task_id}))

    assert result.get("ok") is True
    assert seen["rows_at_terminate"] == 2, "rows were deleted before terminate"
    assert snapshot.get_shards_by_task(task_id) == []
    assert snapshot.get_task(task_id)["status"] == "pending"


def test_retry_leaves_state_untouched_when_workflows_will_not_close(
        dlm_db, monkeypatch):
    """A half-applied retry is worse than a refused one: the rows would be gone
    while the pipelines keep writing to /data/staging."""
    from dlm.web.routes import queue as queue_routes

    task_id = _make_task_with_running_shard("t-retry-2")

    async def fake_terminate(tid, timeout_s=120):
        return False

    monkeypatch.setattr(
        "dlm.web.temporal_client.terminate_workflow_and_wait", fake_terminate)
    result = asyncio.run(queue_routes.retry_task({"task_id": task_id}))

    assert "error" in result and "did not close" in result["error"]
    assert len(snapshot.get_shards_by_task(task_id)) == 2
    assert snapshot.get_task(task_id)["status"] == "paused"


def test_retry_of_a_failed_task_with_no_live_shards_skips_termination(
        dlm_db, monkeypatch):
    """The common case must not pay a 120s terminate timeout."""
    from dlm.web.routes import queue as queue_routes

    task_id = "t-retry-3"
    _task(task_id, "failed", source="hf")
    _shard(f"s-{task_id}-0", task_id, "failed", 0, "w4")

    called = []

    async def fake_terminate(tid, timeout_s=120):
        called.append(tid)
        return True

    monkeypatch.setattr(
        "dlm.web.temporal_client.terminate_workflow_and_wait", fake_terminate)
    result = asyncio.run(queue_routes.retry_task({"task_id": task_id}))

    assert result.get("ok") is True
    assert called == []
    assert snapshot.get_shards_by_task(task_id) == []


# --- --only must filter the manifest actually in use ------------------------

def _transfer_import_module():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location(
        "transfer_import_under_test", root / "scripts" / "transfer_import.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


BUILTIN = [{"name": "A", "src": "manipulation/A/", "category": "manipulation"}]
CUSTOM = [{"name": "A", "src": "other/A-repair/", "category": "other"},
          {"name": "B", "src": "manipulation/B/", "category": "manipulation"}]


def test_only_filters_the_custom_manifest_not_the_builtin():
    """The dangerous case: a name in both files with different prefixes. The
    old code imported the built-in prefix — a different dataset."""
    mod = _transfer_import_module()
    picked = mod.select_manifest(BUILTIN, CUSTOM, "A")
    assert picked == [CUSTOM[0]]


def test_only_finds_a_name_present_only_in_the_custom_manifest():
    mod = _transfer_import_module()
    assert mod.select_manifest(BUILTIN, CUSTOM, "B") == [CUSTOM[1]]


def test_only_still_rejects_a_name_in_neither():
    mod = _transfer_import_module()
    with pytest.raises(ValueError, match="matches nothing in the --manifest"):
        mod.select_manifest(BUILTIN, CUSTOM, "ZZZ")
    with pytest.raises(ValueError, match="matches nothing in the built-in"):
        mod.select_manifest(BUILTIN, None, "ZZZ")


def test_no_only_returns_the_whole_manifest_in_play():
    mod = _transfer_import_module()
    assert mod.select_manifest(BUILTIN, CUSTOM, "") == CUSTOM
    assert mod.select_manifest(BUILTIN, None, "") == BUILTIN


# --- the shard pool must exclude queues nothing polls -----------------------

def _patch_pollers(monkeypatch, per_queue, connect_error=False):
    async def fake_connected():
        if connect_error:
            raise RuntimeError("temporal unreachable")
        return _FakeClient()

    async def fake_count(_client, queue):
        return per_queue.get(queue, 0)

    monkeypatch.setattr(temporal_client, "connected_client", fake_connected)
    monkeypatch.setattr(temporal_client, "queue_poller_count", fake_count)


def test_idle_pool_drops_a_worker_whose_queue_has_no_pollers(monkeypatch):
    """The `{key}@sidecar` heartbeat keeps a node looking alive after its
    Temporal process dies. A child shard started on that node's queue sits
    RUNNING forever and no reconciler can reclaim it."""
    from dlm.web.routes import queue as queue_routes

    _patch_pollers(monkeypatch, {"download-bj1": 2, "download-bj3": 2})
    kept = asyncio.run(queue_routes._drop_unpolled(["bj1", "bj2", "bj3"]))
    assert kept == ["bj1", "bj3"]


def test_idle_pool_keeps_a_worker_whose_poller_count_is_unknown(monkeypatch):
    """None means Temporal could not be asked. Treating that as zero would
    empty the whole fleet on one RPC hiccup."""
    from dlm.web.routes import queue as queue_routes

    async def fake_connected():
        return _FakeClient()

    async def fake_count(_client, _queue):
        return None

    monkeypatch.setattr(temporal_client, "connected_client", fake_connected)
    monkeypatch.setattr(temporal_client, "queue_poller_count", fake_count)
    assert asyncio.run(queue_routes._drop_unpolled(["bj1", "bj2"])) == ["bj1", "bj2"]


def test_idle_pool_survives_temporal_being_unreachable(monkeypatch):
    from dlm.web.routes import queue as queue_routes

    _patch_pollers(monkeypatch, {}, connect_error=True)
    assert asyncio.run(queue_routes._drop_unpolled(["w1", "w2"])) == ["w1", "w2"]


def test_idle_pool_does_not_call_temporal_for_an_empty_pool(monkeypatch):
    from dlm.web.routes import queue as queue_routes

    calls = []

    async def fake_connected():
        calls.append(1)
        return _FakeClient()

    monkeypatch.setattr(temporal_client, "connected_client", fake_connected)
    assert asyncio.run(queue_routes._drop_unpolled([])) == []
    assert calls == []
