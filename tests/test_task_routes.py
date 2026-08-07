"""The dashboard-reachable task routes must close workflows before touching rows.

static/app.js calls /api/tasks/{id}/retry, /skip and DELETE — not the hardened
/api/queue/retry. Each of these used to mutate (or delete) state while the
coordinator and its children kept downloading into /data/staging: the rows are
the only handle on that work, so removing them frees the host in busy_servers,
auto_dispatch stacks a second pipeline on it, and terminate can no longer find
the children it would have reached through those rows.

Run: pytest tests/test_task_routes.py -q
"""

from __future__ import annotations

import asyncio
import time

import pytest
from fastapi import HTTPException

from dlm.queue import snapshot
from dlm.web.routes import tasks as task_routes


def _task(task_id, status, name="X"):
    snapshot.upsert_task({
        "id": task_id, "name": name, "repo_id": "org/r",
        "source": "modelscope", "type": "dataset", "category": "manipulation",
        "status": status, "server": None, "priority": 0, "size_gb": 100.0,
        "downloaded_gb": 0.0, "progress_pct": 0, "speed_mbps": 0,
        "retry_count": 0, "max_workers": 2,
    })


def _shard(shard_id, task_id, status="running", index=0, server="bj1"):
    snapshot.upsert_shard({
        "id": shard_id, "task_id": task_id, "shard_index": index,
        "status": status, "server": server, "total_files": 10,
        "done_files": 0, "updated_at": time.time(),
    })


def _patch_terminate(monkeypatch, closed: bool, calls=None, raises=False):
    async def fake(task_id, timeout_s=120):
        if calls is not None:
            calls.append(task_id)
        if raises:
            raise RuntimeError("temporal unreachable")
        return closed

    monkeypatch.setattr(
        "dlm.web.temporal_client.terminate_workflow_and_wait", fake)


# --- retry must go through the hardened implementation ----------------------

def test_dashboard_retry_terminates_and_clears_shard_rows(dlm_db, monkeypatch):
    calls = []
    _patch_terminate(monkeypatch, True, calls)
    _task("t-r1", "failed")
    _shard("s-t-r1-0", "t-r1", "running")

    result = asyncio.run(task_routes.retry_task("t-r1"))

    assert calls == ["t-r1"], "must not requeue a task whose children are live"
    assert result["status"] == "pending"
    assert snapshot.get_shards_by_task("t-r1") == []
    assert snapshot.get_task("t-r1")["status"] == "pending"


def test_dashboard_retry_inherits_the_done_gate(dlm_db, monkeypatch):
    """A genuinely complete task must stay refused from this path too."""
    _patch_terminate(monkeypatch, True)
    _task("t-r2", "done")
    _shard("s-t-r2-0", "t-r2", "done")

    with pytest.raises(HTTPException) as e:
        asyncio.run(task_routes.retry_task("t-r2"))
    assert "refusing to re-download" in str(e.value.detail)
    assert snapshot.get_task("t-r2")["status"] == "done"


# --- skip ------------------------------------------------------------------

def test_skip_marks_revoked_once_workflows_are_closed(dlm_db, monkeypatch):
    calls = []
    _patch_terminate(monkeypatch, True, calls)
    _task("t-s1", "downloading")

    result = asyncio.run(task_routes.skip_task("t-s1"))

    assert calls == ["t-s1"]
    assert result["workflows_closed"] is True
    assert snapshot.get_task("t-s1")["status"] == "revoked"


def test_skip_refuses_to_revoke_a_task_it_could_not_stop(dlm_db, monkeypatch):
    """Revoking a live task is the worst outcome available: /api/task-progress
    discards reports for terminal tasks, so the dashboard shows it stopped
    while bytes keep landing on BOS."""
    _patch_terminate(monkeypatch, False)
    _task("t-s2", "downloading")

    with pytest.raises(HTTPException) as e:
        asyncio.run(task_routes.skip_task("t-s2"))
    assert e.value.status_code == 502
    assert snapshot.get_task("t-s2")["status"] == "downloading"


def test_skip_force_revokes_anyway(dlm_db, monkeypatch):
    """The escape hatch for Temporal itself being down."""
    _patch_terminate(monkeypatch, False, raises=True)
    _task("t-s3", "downloading")

    result = asyncio.run(task_routes.skip_task("t-s3", force=True))

    assert result["workflows_closed"] is False
    assert snapshot.get_task("t-s3")["status"] == "revoked"


# --- delete ----------------------------------------------------------------

def test_delete_terminates_before_removing_the_rows(dlm_db, monkeypatch):
    seen = {}

    async def fake(task_id, timeout_s=120):
        seen["rows_at_terminate"] = len(snapshot.get_shards_by_task(task_id))
        return True

    monkeypatch.setattr(
        "dlm.web.temporal_client.terminate_workflow_and_wait", fake)
    _task("t-d1", "downloading")
    _shard("s-t-d1-0", "t-d1", "running")

    result = asyncio.run(task_routes.delete_task("t-d1"))

    assert seen["rows_at_terminate"] == 1, "rows were deleted before terminate"
    assert result["deleted"] is True
    assert snapshot.get_task("t-d1") is None


def test_delete_keeps_everything_when_workflows_will_not_close(dlm_db, monkeypatch):
    _patch_terminate(monkeypatch, False)
    _task("t-d2", "downloading")
    _shard("s-t-d2-0", "t-d2", "running")

    with pytest.raises(HTTPException) as e:
        asyncio.run(task_routes.delete_task("t-d2"))
    assert e.value.status_code == 502
    assert snapshot.get_task("t-d2") is not None
    assert len(snapshot.get_shards_by_task("t-d2")) == 1
