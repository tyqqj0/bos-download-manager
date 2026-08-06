"""/api/queue/retry — correcting a task whose `done` its own shard rows deny.

Background: the sharded coordinator used to report a task `done` whenever its
children returned, without reading ShardResult.status. t-20260805-460d45
(molmobot-data) came out of that as `done` at 0 of 9611 GB with its single
shard row reading `failed`. The workflow bug is fixed, but the row it left
behind could not be corrected through any endpoint: retry refused anything not
in (failed, revoked, paused), and hand-editing SQLite is not a supported
operation on the single state source.

The gate added here is evidence, not an override — a `done` task is retryable
only when one of its OWN shard rows is not `done`. These tests pin both
directions of that gate, because a gate that let any `done` task through would
put a 22 TB re-download one curl away.

Run: pytest tests/test_retry_false_done.py -q
"""

from __future__ import annotations

import asyncio
import time

import pytest

from dlm.queue import snapshot
from dlm.web.routes import queue as queue_routes


@pytest.fixture()
def db(dlm_db):
    """A real SQLite file — the route runs actual SQL, so a fake would test
    nothing about the DELETE and the status gate. The route reads through its
    module-level ThreadPoolExecutor, so this only isolates because snapshot's
    cached connection is keyed by path (see conftest)."""
    return dlm_db


@pytest.fixture(autouse=True)
def _no_temporal(monkeypatch):
    """retry now closes the task's workflows unconditionally before touching
    the rows (see test_dispatch_guards), so every accepted path here would try
    to reach a real Temporal server. These tests are about the status gate, not
    the terminate — that it happens at all is pinned in test_dispatch_guards
    (`test_retry_terminates_even_when_no_shard_row_looks_live`)."""
    async def fake_terminate(task_id, timeout_s=120):
        return True

    monkeypatch.setattr(
        "dlm.web.temporal_client.terminate_workflow_and_wait", fake_terminate)


def _task(task_id, status, **over):
    row = {
        "id": task_id, "name": over.pop("name", "some-dataset"),
        "repo_id": "org/some-dataset", "source": "hf", "type": "dataset",
        "category": "other", "bos_path": None, "status": status,
        "server": "w4", "priority": 2, "size_gb": 9611.01,
        "downloaded_gb": 0.0, "progress_pct": 100, "speed_mbps": 0,
        "phase": None, "error": None, "error_class": None, "retry_count": 0,
        "celery_task_id": None, "transfer_status": None,
        "transfer_task_id": None, "transfer_error": None,
        "created_at": "2026-08-05T03:58:09+00:00", "started_at": None,
        "completed_at": "2026-08-06T17:20:44+00:00", "max_workers": None,
    }
    row.update(over)
    snapshot.upsert_task(row)
    return row


def _shard(shard_id, task_id, status, index=0):
    snapshot.upsert_shard({
        "id": shard_id, "task_id": task_id, "shard_index": index,
        "status": status, "server": "w4", "total_files": 4765,
        "done_files": 0, "updated_at": time.time(),
    })


def _retry(task_id):
    return asyncio.run(queue_routes.retry_task({"task_id": task_id}))


def test_done_task_with_a_failed_shard_row_is_retryable(db):
    """The molmobot-data case: the task says done, its shard says failed."""
    _task("t-fake-done", "done", name="molmobot-data")
    _shard("s-t-fake-done-0", "t-fake-done", "failed")

    result = _retry("t-fake-done")

    assert result.get("ok") is True, result
    assert result["status"] == "pending"
    task = snapshot.get_task("t-fake-done")
    assert task["status"] == "pending"
    assert task["retry_count"] == 1
    # The stale claim and completion timestamp described the previous run.
    assert task["server"] is None
    assert task["completed_at"] is None


def test_done_task_with_all_shards_done_is_refused(db):
    """The load-bearing half of the gate: a genuinely complete task must not
    become re-downloadable. AgiBotWorld-Alpha is 258 verified files at
    manipulation/AgiBotWorld-Alpha/ — a retry there is 22 TB of egress."""
    _task("t-really-done", "done", downloaded_gb=9611.01)
    _shard("s-t-really-done-0", "t-really-done", "done", index=0)
    _shard("s-t-really-done-1", "t-really-done", "done", index=1)

    result = _retry("t-really-done")

    assert "error" in result, result
    assert "refusing to re-download" in result["error"]
    assert snapshot.get_task("t-really-done")["status"] == "done"
    # Nothing was cleared on the refused path.
    assert len(snapshot.get_shards_by_task("t-really-done")) == 2


def test_done_task_with_no_shard_rows_is_refused(db):
    """A pre-sharding task (13 of them are `done` with zero shard rows) offers
    no contradicting evidence, so it does not qualify. Those need an
    object-level BOS audit and a deliberate new task, not a retry."""
    _task("t-migrate-legacy", "done", name="Sekai", size_gb=9392.89,
          downloaded_gb=60.49)

    result = _retry("t-migrate-legacy")

    assert "error" in result, result
    assert "all 0 shard rows agree" in result["error"]
    assert snapshot.get_task("t-migrate-legacy")["status"] == "done"


def test_retry_clears_stale_shard_rows(db):
    """The re-dispatched coordinator creates its own rows. Leaving the old ones
    inflates total_shards so the task can never read 100%, and keeps a `failed`
    row the aggregate would count against a healthy run."""
    _task("t-stale", "failed")
    _shard("s-t-stale-0", "t-stale", "failed", index=0)
    _shard("s-t-stale-1", "t-stale", "done", index=1)
    assert len(snapshot.get_shards_by_task("t-stale")) == 2

    result = _retry("t-stale")

    assert result.get("ok") is True, result
    assert result["cleared_shard_rows"] == 2
    assert snapshot.get_shards_by_task("t-stale") == []


@pytest.mark.parametrize("status", ["failed", "revoked", "paused"])
def test_previously_allowed_statuses_still_work(status, db):
    """Widening the gate must not narrow it: the three statuses retry always
    accepted still go through, shard rows or not."""
    _task(f"t-{status}", status)

    result = _retry(f"t-{status}")

    assert result.get("ok") is True, result
    assert snapshot.get_task(f"t-{status}")["status"] == "pending"


def test_downloading_task_is_still_refused(db):
    """Retrying a live download would race auto_dispatch against a running
    coordinator."""
    _task("t-live", "downloading")

    result = _retry("t-live")

    assert "error" in result, result
    assert "status=downloading" in result["error"]
    assert snapshot.get_task("t-live")["status"] == "downloading"


def test_missing_task_reports_not_found(db):
    result = _retry("t-nope")
    assert result == {"error": "Task t-nope not found"}
