"""Coordinator queue routing — a task's coordinator must run on a node that
serves its source.

The bug, observed 2026-08-06: every sharded coordinator was started on the
hardcoded shared queue `download-workers`. Only the HK nodes poll that queue —
a bj node's `--task-queue download-bjN` dedupes against its personal queue, so
it polled no shared queue at all. So a ModelScope task's `list_repo_files` ran
on whichever HK node grabbed it. t-20260806-cbf39e (RoboDojo) drew w6, which
has no `modelscope` SDK, and the task failed with ModuleNotFoundError. w1-w5
happen to have the SDK, which is why this read as intermittent rather than
broken. The same listing measured from w6 (after installing the SDK) took over
18 minutes and had not finished — HK is the wrong side of the network for
ModelScope, which is what the source routing existed to express.

The invariant these tests pin: for every (worker, source) the dispatcher would
pair, the queue the coordinator starts on is one that worker actually polls.

Run: pytest tests/test_coordinator_routing.py -q
"""

from __future__ import annotations

import asyncio
import time

import pytest

from dlm.web.fleet import (
    MS_COORDINATOR_QUEUE,
    SHARED_COORDINATOR_QUEUE,
    coordinator_queue,
    source_for_worker,
    worker_coordinator_queue,
    worker_serves,
)

ALL_WORKERS = [f"w{i}" for i in range(1, 8)] + [f"bj{i}" for i in range(1, 10)]


def polled_queues(server_key: str) -> list[str]:
    """The queues a worker registers, mirroring dlm/temporal/__main__.py:
    its --task-queue (personal for bj*, the shared HK queue for w*), its
    personal queue, and the shared coordinator queue for its source."""
    task_queue = (f"download-{server_key}" if server_key.startswith("bj")
                  else SHARED_COORDINATOR_QUEUE)
    personal = f"download-{server_key}"
    return list(dict.fromkeys([task_queue, personal,
                               worker_coordinator_queue(server_key)]))


def test_modelscope_coordinator_does_not_go_to_the_hk_queue():
    assert coordinator_queue("modelscope") == MS_COORDINATOR_QUEUE
    assert coordinator_queue("modelscope") != SHARED_COORDINATOR_QUEUE


@pytest.mark.parametrize("source", ["hf", "wget", "other", ""])
def test_every_non_modelscope_source_keeps_the_hk_queue(source):
    """Only ModelScope is BJ-bound; widening the split would strand wget and
    `other` tasks on a queue nobody polls."""
    assert coordinator_queue(source) == SHARED_COORDINATOR_QUEUE


@pytest.mark.parametrize("server_key", ALL_WORKERS)
def test_every_worker_polls_the_queue_its_own_sources_dispatch_to(server_key):
    """The load-bearing invariant. Before the fix this failed for bj1-bj9:
    coordinator_queue('modelscope') was 'download-workers', which no bj node
    polls."""
    for source in ("hf", "modelscope", "wget"):
        if not worker_serves(server_key, source):
            continue
        assert coordinator_queue(source) in polled_queues(server_key), (
            f"{server_key} serves {source} but does not poll "
            f"{coordinator_queue(source)}"
        )


@pytest.mark.parametrize("server_key", ALL_WORKERS)
def test_no_worker_polls_a_coordinator_queue_for_a_source_it_cannot_serve(server_key):
    """The other direction: an HK node must not be handed ModelScope
    coordinators, or the SDK lottery comes back."""
    for source in ("hf", "modelscope"):
        if worker_serves(server_key, source):
            continue
        assert coordinator_queue(source) not in polled_queues(server_key)


@pytest.mark.parametrize("server_key", ALL_WORKERS)
def test_worker_side_and_dispatch_side_agree(server_key):
    """Two functions compute the shared queue — the worker registers one and
    the dispatcher targets one. If they drift, coordinators queue forever."""
    assert worker_coordinator_queue(server_key) == coordinator_queue(
        source_for_worker(server_key))


# --- the call site, not just the helper -------------------------------------

def test_auto_dispatch_starts_a_modelscope_coordinator_on_the_ms_queue(
        dlm_db, monkeypatch):
    """The helper being right is not enough: the original defect was that the
    dispatcher never passed a queue at all, so start_sharded_download's
    hardcoded default won. This drives the real auto_dispatch_pending against a
    temp DB and records the queue it asks for."""
    from dlm.web import reconciler, temporal_client

    snapshot = dlm_db

    now = time.time()
    snapshot.upsert_task({
        "id": "t-ms-1", "name": "RoboDojo", "repo_id": "RoboDojo-Benchmark/RoboDojo",
        "source": "modelscope", "type": "dataset", "category": "manipulation",
        "status": "pending", "priority": 0, "size_gb": 715.67,
        "created_at": "2026-08-06T17:13:38+00:00",
    })
    # One idle bj node with room. Only bj* may serve modelscope, so this is the
    # node the dispatcher must route to.
    snapshot.update_worker("bj3@temporal", "bj3", status="online",
                           disk_free_gb=460.0)

    started = []

    async def fake_start(task_dict, task_queue=None):
        started.append((task_dict["id"], task_queue))

    monkeypatch.setattr(temporal_client, "start_sharded_download", fake_start)

    report = asyncio.run(reconciler.auto_dispatch_pending())

    assert started == [("t-ms-1", MS_COORDINATOR_QUEUE)], (started, report)
    assert snapshot.get_task("t-ms-1")["status"] == "downloading"


def test_auto_dispatch_starts_an_hf_coordinator_on_the_shared_queue(
        dlm_db, monkeypatch):
    """Guard on the fix: HF dispatch must not have moved."""
    from dlm.web import reconciler, temporal_client

    snapshot = dlm_db

    snapshot.upsert_task({
        "id": "t-hf-1", "name": "molmobot-data", "repo_id": "org/molmobot-data",
        "source": "hf", "type": "dataset", "category": "other",
        "status": "pending", "priority": 2, "size_gb": 9611.01,
        "created_at": "2026-08-05T03:58:09+00:00",
    })
    snapshot.update_worker("w3@temporal", "w3", status="online",
                           disk_free_gb=170.0)

    started = []

    async def fake_start(task_dict, task_queue=None):
        started.append((task_dict["id"], task_queue))

    monkeypatch.setattr(temporal_client, "start_sharded_download", fake_start)

    asyncio.run(reconciler.auto_dispatch_pending())

    assert started == [("t-hf-1", SHARED_COORDINATOR_QUEUE)], started


def test_a_modelscope_task_is_not_dispatched_to_an_hk_worker(
        dlm_db, monkeypatch):
    """Routing precondition this fix depends on: with only HK nodes alive, a
    ModelScope task waits rather than landing on a node without the SDK."""
    from dlm.web import reconciler, temporal_client

    snapshot = dlm_db

    snapshot.upsert_task({
        "id": "t-ms-2", "name": "RoboDojo", "repo_id": "RoboDojo-Benchmark/RoboDojo",
        "source": "modelscope", "type": "dataset", "category": "manipulation",
        "status": "pending", "priority": 0, "size_gb": 715.67,
        "created_at": "2026-08-06T17:13:38+00:00",
    })
    for k in ("w1", "w2", "w3"):
        snapshot.update_worker(f"{k}@temporal", k, status="online",
                               disk_free_gb=170.0)

    started = []

    async def fake_start(task_dict, task_queue=None):
        started.append((task_dict["id"], task_queue))

    monkeypatch.setattr(temporal_client, "start_sharded_download", fake_start)

    asyncio.run(reconciler.auto_dispatch_pending())

    assert started == []
    assert snapshot.get_task("t-ms-2")["status"] == "pending"
