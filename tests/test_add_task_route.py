"""POST /api/tasks — the only add path the dashboard and `dlm add` can reach.

Three of the parameters this route advertised did nothing:

  * `shard_count` did not exist, so the modal could not size a task at all.
    Sharding was reachable only from /api/queue/add (curl) or a reshard after
    the fact — the coordinator reads `max_workers` off the row and this route
    never wrote it.
  * `no_dispatch` was declared, sent by both the UI and the CLI, and never
    read. The row went in `pending`, which auto_dispatch_pending() claims
    within 30s: the checkbox promised the opposite of what it did.
  * `source` was a `dlm add` flag the body never carried, so a bare `org/name`
    that lives on ModelScope was filed as hf — and hf tasks only dispatch to
    the HK fleet, which cannot reach ModelScope.

And `bos_path` was built by hand as `auwomo-datasets/raw-data/{category}/
{name}/`: a bucket that does not exist, wrapped around a prefix scheme the
uploader abandoned. 12 live rows carry that value.

Run: pytest tests/test_add_task_route.py -q
"""

from __future__ import annotations

import asyncio

import pytest
from fastapi import HTTPException

from dlm.queue import snapshot
from dlm.web.routes import tasks as task_routes


def _add(**over):
    body = {"url_or_repo": "org/some-dataset", "category": "manipulation"}
    body.update(over)
    req = task_routes.AddTaskRequest(**body)
    return asyncio.run(task_routes.add_task(req))


# --- shard count -----------------------------------------------------------

def test_shard_count_lands_on_the_column_the_coordinator_reads(dlm_db):
    """`max_workers` is the row field start_sharded_download turns into
    TaskInput.shard_count. Anything else the route stores is decorative."""
    result = _add(shard_count=6)

    row = snapshot.get_task(result["task"]["id"])
    assert row["max_workers"] == 6
    assert result["task"]["shard_count"] == 6


def test_shard_count_reaches_the_workflow_input(dlm_db, monkeypatch):
    """End of the chain, pinned against the real translation rather than a
    restatement of it: the row this route writes has to make the dispatcher
    build a TaskInput asking for 6 shards."""
    from dlm.web import temporal_client

    result = _add(shard_count=6)
    row = snapshot.get_task(result["task"]["id"])

    started = {}

    class FakeHandle:
        id = "sharded-x"
        result_run_id = "r"

    class FakeClient:
        async def start_workflow(self, wf, task_input, **kw):
            started["input"] = task_input
            return FakeHandle()

    async def fake_client():
        return FakeClient()

    async def fake_pollers(client, queue):
        return 1

    monkeypatch.setattr(temporal_client, "connected_client", fake_client)
    monkeypatch.setattr(temporal_client, "queue_poller_count", fake_pollers)

    asyncio.run(temporal_client.start_sharded_download(row))

    assert started["input"].shard_count == 6


def test_no_shard_count_leaves_the_coordinator_to_size_it(dlm_db):
    """0 means auto. Writing 0 rather than leaving the key out matters only for
    readability here — upsert_task COALESCEs max_workers — but the row must not
    come out with a shard count nobody asked for."""
    result = _add()

    assert (snapshot.get_task(result["task"]["id"])["max_workers"] or 0) == 0


# --- no_dispatch -----------------------------------------------------------

def test_no_dispatch_writes_a_row_auto_dispatch_will_not_claim(dlm_db):
    """`paused` is what "queued but not started" means to the rest of the
    system: auto_dispatch_pending() selects `pending` only, and /queue/resume
    takes paused back to pending when the operator is ready."""
    result = _add(no_dispatch=True)

    assert snapshot.get_task(result["task"]["id"])["status"] == "paused"
    assert result["task"]["status"] == "paused"


def test_a_no_dispatch_task_survives_a_dispatch_cycle(dlm_db, monkeypatch):
    """The claim above, checked against the dispatcher itself instead of a
    comment about it."""
    from dlm.web import reconciler

    result = _add(no_dispatch=True)
    task_id = result["task"]["id"]

    dispatched = []

    async def fake_start(task, task_queue=None):
        dispatched.append(task["id"])

    monkeypatch.setattr("dlm.web.temporal_client.start_sharded_download",
                        fake_start)
    asyncio.run(reconciler.auto_dispatch_pending())

    assert dispatched == []
    assert snapshot.get_task(task_id)["status"] == "paused"


def test_the_default_is_still_dispatchable(dlm_db):
    """Honoring the flag must not quietly pause every task."""
    result = _add()
    assert snapshot.get_task(result["task"]["id"])["status"] == "pending"


# --- bos_path --------------------------------------------------------------
#
# bos_path has to be the key prefix inside the bucket, identical to what the
# uploader and the BOS resume filter compute. A prefix that differs by one
# segment silently re-downloads the whole dataset.

@pytest.mark.parametrize("category,dtype,expected", [
    ("manipulation", "dataset", "manipulation/some-dataset/"),
    # "other" is a real path segment, not a synonym for "no category" —
    # bos_target reads any non-empty category, and other/ exists on BOS.
    ("other", "dataset", "other/some-dataset/"),
    ("manipulation", "model", "some-dataset/"),
])
def test_bos_path_is_the_prefix_the_uploader_writes_to(dlm_db, category, dtype,
                                                       expected):
    result = _add(category=category, type=dtype)

    assert snapshot.get_task(result["task"]["id"])["bos_path"] == expected


def test_bos_path_agrees_with_bos_target_on_the_dispatched_input(dlm_db):
    """The two derivations have to converge for the same row: what this route
    stored, and what bos_target returns for the TaskInput the dispatcher builds
    from that row. Pinning them against each other catches a drift in either."""
    from dlm.core.bos import bos_target
    from dlm.temporal.models import TaskInput

    result = _add(category="other", name="Custom-Name")
    row = snapshot.get_task(result["task"]["id"])

    task_input = TaskInput(
        id=row["id"], name=row["name"], repo_id=row["repo_id"],
        source=row["source"], type=row["type"], category=row["category"],
        priority=row["priority"], size_gb=row["size_gb"] or 0,
        shard_count=int(row["max_workers"] or 0),
    )
    _, prefix = bos_target(task_input)

    assert row["bos_path"] == prefix
    assert prefix == "other/Custom-Name/", \
        "a custom name must reach the prefix — deriving it from repo_id instead " \
        "would upload to some-dataset/ while the row claims Custom-Name/"


# --- source override -------------------------------------------------------

def test_source_override_is_honored(dlm_db):
    """`org/name` has no host in it, so parse_repo can only guess hf. Routing
    reads this field: an hf row never dispatches to the BJ fleet."""
    result = _add(source="modelscope")

    assert snapshot.get_task(result["task"]["id"])["source"] == "modelscope"


def test_a_bogus_source_is_refused_rather_than_stored(dlm_db):
    """A row with a source no worker serves is undispatchable and reads as a
    stuck task rather than a rejected request."""
    with pytest.raises(HTTPException) as e:
        _add(source="s3")

    assert "Unknown source" in str(e.value.detail)
    assert snapshot.get_all_tasks() == []


def test_without_an_override_the_parsed_source_stands(dlm_db):
    result = _add(url_or_repo="https://www.modelscope.cn/datasets/org/x")

    assert snapshot.get_task(result["task"]["id"])["source"] == "modelscope"
