"""Queue routes that mutate rows belonging to running workflows.

Both routes here have the same hazard, and it is the reason /api/tasks was
hardened first: the shard rows are the ONLY handle on live work.
`terminate_workflow_and_wait` finds the children *through* those rows, and
`busy_servers` frees the host the moment they are gone — so a row deleted
while its pipeline is alive gives you a second pipeline stacked on the same
staging directory, with the first one invisible to the dashboard.

`reshard` had the right order already; nothing pinned it. `DELETE
/api/queue/{id}` did not: it fired a best-effort `cancel_workflow` and
deleted regardless of the result.

Run: pytest tests/test_queue_mutations.py -q
"""

from __future__ import annotations

import asyncio
import time

import pytest
from fastapi import HTTPException

from dlm.queue import snapshot
from dlm.web.routes import queue as queue_routes


def _task(task_id, status, name="X", max_workers=4):
    snapshot.upsert_task({
        "id": task_id, "name": name, "repo_id": "org/r",
        "source": "modelscope", "type": "dataset", "category": "manipulation",
        "status": status, "server": None, "priority": 0, "size_gb": 100.0,
        "downloaded_gb": 0.0, "progress_pct": 0, "speed_mbps": 0,
        "retry_count": 0, "max_workers": max_workers,
    })


def _shard(shard_id, task_id, status="running", index=0, server="bj1"):
    snapshot.upsert_shard({
        "id": shard_id, "task_id": task_id, "shard_index": index,
        "status": status, "server": server, "total_files": 10,
        "done_files": 0, "updated_at": time.time(),
    })


def _patch_terminate(monkeypatch, closed: bool, seen=None, raises=False):
    async def fake(task_id, timeout_s=120):
        if seen is not None:
            seen["task_id"] = task_id
            seen["shard_rows"] = len(snapshot.get_shards_by_task(task_id))
            seen["status"] = (snapshot.get_task(task_id) or {}).get("status")
        if raises:
            raise RuntimeError("temporal unreachable")
        return closed

    monkeypatch.setattr(
        "dlm.web.temporal_client.terminate_workflow_and_wait", fake)


# --- reshard ----------------------------------------------------------------

def test_reshard_terminates_before_it_deletes_the_shard_rows(dlm_db, monkeypatch):
    seen = {}
    _patch_terminate(monkeypatch, True, seen)
    _task("t-rs1", "downloading", max_workers=4)
    _shard("s-t-rs1-0", "t-rs1", "running", 0)
    _shard("s-t-rs1-1", "t-rs1", "running", 1)

    result = asyncio.run(queue_routes.reshard_task(
        {"task_id": "t-rs1", "shard_count": 8}))

    assert seen["shard_rows"] == 2, (
        "rows were deleted before terminate — the children it had to find "
        "through them were still running")
    assert result["ok"] is True
    assert result["shard_count"] == 8
    task = snapshot.get_task("t-rs1")
    assert task["status"] == "pending"
    assert task["server"] is None
    assert task["max_workers"] == 8
    assert snapshot.get_shards_by_task("t-rs1") == []


def test_reshard_changes_nothing_when_the_workflows_will_not_close(dlm_db, monkeypatch):
    """A requeued row plus a live coordinator is the stacked-pipeline case:
    auto_dispatch would start a second one on the same staging directory."""
    _patch_terminate(monkeypatch, False)
    _task("t-rs2", "downloading", max_workers=4)
    _shard("s-t-rs2-0", "t-rs2", "running")

    result = asyncio.run(queue_routes.reshard_task(
        {"task_id": "t-rs2", "shard_count": 8}))

    assert "error" in result
    task = snapshot.get_task("t-rs2")
    assert task["status"] == "downloading"
    assert task["max_workers"] == 4, "shard count changed despite the refusal"
    assert len(snapshot.get_shards_by_task("t-rs2")) == 1


def test_reshard_refuses_a_task_that_is_already_terminal(dlm_db, monkeypatch):
    """`done` here would mean re-downloading a complete dataset."""
    seen = {}
    _patch_terminate(monkeypatch, True, seen)
    _task("t-rs3", "done")

    result = asyncio.run(queue_routes.reshard_task(
        {"task_id": "t-rs3", "shard_count": 8}))

    assert "error" in result
    assert seen == {}, "terminated the workflows of a task it then refused"
    assert snapshot.get_task("t-rs3")["status"] == "done"


def test_reshard_rejects_a_nonsense_shard_count(dlm_db, monkeypatch):
    seen = {}
    _patch_terminate(monkeypatch, True, seen)
    _task("t-rs4", "downloading")

    assert "error" in asyncio.run(queue_routes.reshard_task(
        {"task_id": "t-rs4", "shard_count": 0}))
    assert seen == {}, "terminated live work before validating the request"


def test_reshard_does_not_requeue_a_task_claimed_during_the_terminate_window(
        dlm_db, monkeypatch):
    """auto_dispatch runs every 30s and terminate can take minutes. Writing
    the new shard count unconditionally would discard it against a row that
    has since moved on."""
    async def fake(task_id, timeout_s=120):
        # A dispatch cycle claims the pending row while we wait for the close,
        # so the status this call validated against no longer holds.
        snapshot.update_task_progress(task_id, status="downloading")
        return True

    monkeypatch.setattr(
        "dlm.web.temporal_client.terminate_workflow_and_wait", fake)
    _task("t-rs5", "pending", max_workers=4)

    result = asyncio.run(queue_routes.reshard_task(
        {"task_id": "t-rs5", "shard_count": 8}))

    assert "error" in result
    assert snapshot.get_task("t-rs5")["max_workers"] == 4


# --- DELETE /api/queue/{id} -------------------------------------------------

def test_queue_delete_terminates_before_removing_the_rows(dlm_db, monkeypatch):
    seen = {}
    _patch_terminate(monkeypatch, True, seen)
    _task("t-qd1", "downloading")
    _shard("s-t-qd1-0", "t-qd1", "running")

    result = asyncio.run(queue_routes.delete_from_queue("t-qd1"))

    assert seen["shard_rows"] == 1, "rows were deleted before terminate"
    assert result["deleted"] is True
    assert result["workflows_closed"] is True
    assert snapshot.get_task("t-qd1") is None


def test_queue_delete_keeps_everything_when_workflows_will_not_close(
        dlm_db, monkeypatch):
    _patch_terminate(monkeypatch, False)
    _task("t-qd2", "downloading")
    _shard("s-t-qd2-0", "t-qd2", "running")

    with pytest.raises(HTTPException) as e:
        asyncio.run(queue_routes.delete_from_queue("t-qd2"))

    assert e.value.status_code == 502
    assert snapshot.get_task("t-qd2") is not None
    assert len(snapshot.get_shards_by_task("t-qd2")) == 1


def test_queue_delete_force_deletes_even_with_temporal_down(dlm_db, monkeypatch):
    """The escape hatch, and the honest report that goes with it."""
    _patch_terminate(monkeypatch, False, raises=True)
    _task("t-qd3", "downloading")

    result = asyncio.run(queue_routes.delete_from_queue("t-qd3", force=True))

    assert result["deleted"] is True
    assert result["workflows_closed"] is False
    assert snapshot.get_task("t-qd3") is None
