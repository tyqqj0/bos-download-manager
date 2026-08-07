"""T7 — dispatch consolidation, admission, mode gate, and create-task entry
points (see plan v3.1 §T7).

Before this task, nothing in dlm/web/ ever read dispatch_mode when starting
a workflow: `grep -rn dispatch_mode dlm/web/reconciler.py dlm/web/scheduler.py`
returned nothing. PoolDownloadWorkflow (T1-T6) existed but no caller could
reach it. This file exercises the five things that make it reachable:

1. start_task_download routes on dispatch_mode (missing/NULL -> sharded).
2. start_pool_download's mode gate: refuses (raises + logs CRITICAL) when
   the pool activity queue has fewer live pollers than alive workers for
   that source; proceeds when it has enough. The gRPC describe call is
   stubbed — nothing here needs a live Temporal server.
3. auto_dispatch_pending's per-source pool admission cap
   (fleet.POOL_MAX_CONCURRENT_TASKS), independent of admission on other
   sources.
4. The listing guard's coordinator_phase criterion for pool tasks, plus the
   G1 regression guard: the pre-existing "no shard rows" criterion for
   sharded tasks must still block exactly as before.
5. Both /queue/add and /api/tasks (AddTaskRequest) accept, persist, and
   default dispatch_mode, and reject an invalid mode string.

Run: python3 -m pytest tests/test_pool_dispatch.py -q
"""

from __future__ import annotations

import asyncio
import logging
import time
from types import SimpleNamespace

import pytest


def _call(coro):
    return asyncio.run(coro) if asyncio.iscoroutine(coro) else coro


def _task(db, task_id, status="downloading", *, mode="pool", priority=5, source="hf"):
    """mode=None omits dispatch_mode entirely — what every existing caller
    does, and the case where the schema default ('sharded') has to apply."""
    row = {"id": task_id, "name": task_id, "repo_id": "org/x",
           "status": status, "priority": priority, "source": source}
    if mode is not None:
        row["dispatch_mode"] = mode
    db.upsert_task(row)


def _worker(db, key, *, disk_free_gb=500):
    db.update_worker(hostname=key, server_key=key, disk_free_gb=disk_free_gb)


def _set_claimed_now(db, task_id):
    conn = db._conn()
    conn.execute("UPDATE tasks SET claimed_at=? WHERE id=?", (time.time(), task_id))
    conn.commit()


def _set_phase(db, task_id, phase):
    conn = db._conn()
    conn.execute("UPDATE tasks SET coordinator_phase=? WHERE id=?", (phase, task_id))
    conn.commit()


# ── 1. 分流 — start_task_download routes by dispatch_mode ──────────────


def test_start_task_download_routes_sharded_by_default(monkeypatch):
    """Missing dispatch_mode key (every existing caller's task dict shape)
    and an explicit NULL both mean sharded — never a silent pool fallback."""
    import dlm.web.temporal_client as tc

    calls = []

    # task_queue is keyword-only in practice: the funnel decides the
    # coordinator queue from the task's source (fleet.coordinator_queue) so no
    # call site can get it wrong, so the fakes have to accept and record it.
    async def fake_sharded(task, task_queue=None):
        calls.append(("sharded", task["id"], task_queue))
        return "sharded-handle"

    async def fake_pool(task, task_queue=None):
        calls.append(("pool", task["id"], task_queue))
        return "pool-handle"

    monkeypatch.setattr(tc, "start_sharded_download", fake_sharded)
    monkeypatch.setattr(tc, "start_pool_download", fake_pool)

    asyncio.run(tc.start_task_download({"id": "t-a"}))                       # no key at all
    asyncio.run(tc.start_task_download({"id": "t-b", "dispatch_mode": "sharded"}))
    asyncio.run(tc.start_task_download({"id": "t-c", "dispatch_mode": None}))  # explicit NULL

    # hf (the default when no source key is present) -> the shared HK queue
    assert calls == [("sharded", "t-a", "download-workers"),
                     ("sharded", "t-b", "download-workers"),
                     ("sharded", "t-c", "download-workers")]


def test_start_task_download_routes_pool_to_pool_starter(monkeypatch):
    import dlm.web.temporal_client as tc

    calls = []

    async def fake_sharded(task, task_queue=None):
        calls.append(("sharded", task["id"], task_queue))

    async def fake_pool(task, task_queue=None):
        calls.append(("pool", task["id"], task_queue))
        return "pool-handle"

    monkeypatch.setattr(tc, "start_sharded_download", fake_sharded)
    monkeypatch.setattr(tc, "start_pool_download", fake_pool)

    result = asyncio.run(tc.start_task_download(
        {"id": "t-d", "dispatch_mode": "pool", "source": "modelscope"}))

    # A pool task routes by source exactly like a sharded one: a ModelScope
    # coordinator on the HK-only queue dies with `No module named 'modelscope'`
    # whichever mode started it (t-20260806-cbf39e).
    assert calls == [("pool", "t-d", "download-ms-workers")]
    assert result == "pool-handle"


def test_start_task_download_rejects_unknown_mode_instead_of_falling_back(monkeypatch):
    import dlm.web.temporal_client as tc

    async def fail(*a, **k):
        raise AssertionError("should not be called for an unknown mode")

    monkeypatch.setattr(tc, "start_sharded_download", fail)
    monkeypatch.setattr(tc, "start_pool_download", fail)

    with pytest.raises(ValueError):
        asyncio.run(tc.start_task_download({"id": "t-e", "dispatch_mode": "bogus"}))


# ── 2. 门禁拒绝 — the raw-gRPC poller gate ──────────────────────────────


class _FakeWorkflowService:
    def __init__(self, poller_count):
        self.poller_count = poller_count
        self.requests = []

    async def describe_task_queue(self, req, timeout=None):
        self.requests.append(req)
        return SimpleNamespace(pollers=[object()] * self.poller_count)


class _FakeServiceClient:
    def __init__(self, poller_count):
        self.workflow_service = _FakeWorkflowService(poller_count)


class _FakeClient:
    """Stands in for temporalio.client.Client — no live server involved."""

    def __init__(self, poller_count):
        self.namespace = "default"
        self.service_client = _FakeServiceClient(poller_count)
        self.started = []

    async def start_workflow(self, run_fn, *args, id=None, task_queue=None, **kwargs):
        self.started.append({"id": id, "task_queue": task_queue, "args": args})
        return "fake-pool-handle"


def test_pool_gate_rejects_and_alerts_when_pollers_below_alive_workers(db, monkeypatch, caplog):
    import dlm.web.temporal_client as tc

    _worker(db, "w1")
    _worker(db, "w2")  # 2 alive hf workers -> expected >= 2 pollers

    fake_client = _FakeClient(poller_count=1)  # only 1 poller

    async def fake_connected(timeout=None):
        return fake_client
    monkeypatch.setattr(tc, "connected_client", fake_connected)

    task = {"id": "t-gate-reject", "source": "hf", "name": "x", "repo_id": "org/x"}

    with caplog.at_level(logging.CRITICAL, logger="dlm.web"):
        with pytest.raises(tc.PoolPollerGateError):
            asyncio.run(tc.start_pool_download(task))

    assert any("pool gate rejected" in r.message for r in caplog.records), caplog.records
    assert fake_client.started == []  # refused before ever starting the workflow


def test_pool_gate_proceeds_when_pollers_meet_alive_workers(db, monkeypatch):
    import dlm.web.temporal_client as tc

    _worker(db, "w1")
    _worker(db, "w2")  # 2 alive hf workers

    fake_client = _FakeClient(poller_count=2)  # exactly meets expectation

    async def fake_connected(timeout=None):
        return fake_client
    monkeypatch.setattr(tc, "connected_client", fake_connected)

    task = {"id": "t-gate-ok", "source": "hf", "name": "x", "repo_id": "org/x", "priority": 5}

    handle = asyncio.run(tc.start_pool_download(task))

    assert handle == "fake-pool-handle"
    assert len(fake_client.started) == 1
    started = fake_client.started[0]
    assert started["task_queue"] == "download-workers"  # coordinator, not the batch queue
    assert started["id"] == "pool-t-gate-ok"


def test_pool_gate_uses_activity_task_queue_type():
    """The gate must query the ACTIVITY type, not WORKFLOW — pool workers
    register activities only, so a WORKFLOW-type describe reads zero
    pollers on a perfectly healthy fleet (plan resolved ambiguity C)."""
    from temporalio.api.enums.v1 import TaskQueueType

    import dlm.web.temporal_client as tc

    fake_client = _FakeClient(poller_count=3)
    asyncio.run(tc._pool_poller_count(fake_client, "pool-hf"))
    req = fake_client.service_client.workflow_service.requests[0]
    assert req.task_queue_type == TaskQueueType.TASK_QUEUE_TYPE_ACTIVITY
    assert req.task_queue.name == "pool-hf"


# ── 3. 准入上限 — POOL_MAX_CONCURRENT_TASKS per source ──────────────────


def test_pool_admission_cap_blocks_further_pool_tasks_on_a_full_source(db, monkeypatch):
    from dlm.web import reconciler
    from dlm.web.fleet import POOL_MAX_CONCURRENT_TASKS
    import dlm.web.temporal_client as tc

    calls = []

    async def fake_start(task):
        calls.append(task["id"])

    monkeypatch.setattr(tc, "start_task_download", fake_start)

    # Fill the hf pool cap with already-downloading pool tasks.
    for i in range(POOL_MAX_CONCURRENT_TASKS):
        _task(db, f"t-run-hf-{i}", status="downloading", mode="pool", source="hf")

    # One more pending pool task on the SAME (full) source — must not be admitted.
    _task(db, "t-pending-hf", status="pending", mode="pool", source="hf")
    # A pending pool task on the OTHER source — admission there is unaffected.
    _task(db, "t-pending-ms", status="pending", mode="pool", source="modelscope")

    _worker(db, "w1")   # idle hf worker
    _worker(db, "bj1")  # idle modelscope worker

    asyncio.run(reconciler.auto_dispatch_pending())

    assert db.get_task("t-pending-hf")["status"] == "pending"
    assert "t-pending-hf" not in calls

    assert db.get_task("t-pending-ms")["status"] == "downloading"
    assert "t-pending-ms" in calls


def test_pool_admission_cap_does_not_affect_sharded_dispatch_on_same_source(db, monkeypatch):
    """The G1 corollary: the pool cap must not block a sharded task on the
    same, otherwise-full-of-pool-tasks source."""
    from dlm.web import reconciler
    from dlm.web.fleet import POOL_MAX_CONCURRENT_TASKS
    import dlm.web.temporal_client as tc

    calls = []

    async def fake_start(task):
        calls.append(task["id"])

    monkeypatch.setattr(tc, "start_task_download", fake_start)

    for i in range(POOL_MAX_CONCURRENT_TASKS):
        _task(db, f"t-run-hf2-{i}", status="downloading", mode="pool", source="hf")

    _task(db, "t-pending-sharded", status="pending", mode="sharded", source="hf")
    _worker(db, "w1")

    asyncio.run(reconciler.auto_dispatch_pending())

    assert db.get_task("t-pending-sharded")["status"] == "downloading"
    assert "t-pending-sharded" in calls


# ── 4. guard — coordinator_phase for pool, G1 regression for sharded ───


def test_create_pool_batches_sets_coordinator_phase_dispatching(db):
    """Decision A.2's writer: create_pool_batches is the only place besides
    the claim UPDATE that touches coordinator_phase — it must flip
    'listing' -> 'dispatching' once batch rows land, or the guard never
    lets a second pool task onto an already-dispatching source."""
    from dlm.web.routes.queue import create_pool_batches

    _task(db, "t-cpb", status="downloading", mode="pool", source="hf")
    _set_phase(db, "t-cpb", "listing")

    out = _call(create_pool_batches({
        "task_id": "t-cpb",
        "shard_infos": [
            {"shard_index": 0, "filelist_key": "k0", "total_files": 10, "total_bytes": 1000},
            {"shard_index": 1, "filelist_key": "k1", "total_files": 10, "total_bytes": 1000},
        ],
    }))
    assert out.get("ok") is True
    assert db.get_task("t-cpb")["coordinator_phase"] == "dispatching"


def test_pool_task_in_listing_phase_blocks_its_source(db, monkeypatch):
    from dlm.web import reconciler
    import dlm.web.temporal_client as tc

    calls = []

    async def fake_start(task):
        calls.append(task["id"])

    monkeypatch.setattr(tc, "start_task_download", fake_start)

    _task(db, "t-coord", status="downloading", mode="pool", source="hf")
    _set_claimed_now(db, "t-coord")
    _set_phase(db, "t-coord", "listing")

    _task(db, "t-wait", status="pending", mode="pool", source="hf")
    _worker(db, "w1")

    asyncio.run(reconciler.auto_dispatch_pending())

    assert db.get_task("t-wait")["status"] == "pending"
    assert "t-wait" not in calls


def test_pool_listing_phase_blocks_even_with_shard_rows_present(db, monkeypatch):
    """Isolates the coordinator_phase criterion from shard-row existence: in
    steady-state production the two co-vary (create_pool_batches sets both
    together), which would let a broken coordinator_phase check hide behind
    the pre-existing NOT-EXISTS-shards criterion. Give the task shard rows
    anyway and confirm 'listing' still blocks — a mode-scoped legacy branch
    (as implemented) would NOT block this on its own; only the coordinator_
    phase clause does."""
    from dlm.web import reconciler
    import dlm.web.temporal_client as tc

    calls = []

    async def fake_start(task):
        calls.append(task["id"])

    monkeypatch.setattr(tc, "start_task_download", fake_start)

    _task(db, "t-coord-iso", status="downloading", mode="pool", source="hf")
    _set_claimed_now(db, "t-coord-iso")
    _set_phase(db, "t-coord-iso", "listing")
    db.upsert_shard({"id": "s-coord-iso-0", "task_id": "t-coord-iso", "shard_index": 0,
                      "status": "pending"})

    _task(db, "t-wait-iso", status="pending", mode="pool", source="hf")
    _worker(db, "w1")

    asyncio.run(reconciler.auto_dispatch_pending())

    assert db.get_task("t-wait-iso")["status"] == "pending"
    assert "t-wait-iso" not in calls


def test_pool_dispatching_phase_does_not_block_even_without_shard_rows(db, monkeypatch):
    """The mirror isolation: no shard rows (so the legacy criterion alone
    would block) but coordinator_phase already 'dispatching' — must NOT
    block. Confirms the guard for a pool task depends on coordinator_phase,
    not on shard-row existence."""
    from dlm.web import reconciler
    import dlm.web.temporal_client as tc

    calls = []

    async def fake_start(task):
        calls.append(task["id"])

    monkeypatch.setattr(tc, "start_task_download", fake_start)

    _task(db, "t-coord-iso2", status="downloading", mode="pool", source="hf")
    _set_claimed_now(db, "t-coord-iso2")
    _set_phase(db, "t-coord-iso2", "dispatching")
    # Deliberately no shard rows for t-coord-iso2.

    _task(db, "t-wait-iso2", status="pending", mode="pool", source="hf")
    _worker(db, "w1")

    asyncio.run(reconciler.auto_dispatch_pending())

    assert db.get_task("t-wait-iso2")["status"] == "downloading"
    assert "t-wait-iso2" in calls


def test_pool_task_in_dispatching_phase_does_not_block(db, monkeypatch):
    from dlm.web import reconciler
    import dlm.web.temporal_client as tc

    calls = []

    async def fake_start(task):
        calls.append(task["id"])

    monkeypatch.setattr(tc, "start_task_download", fake_start)

    _task(db, "t-coord2", status="downloading", mode="pool", source="hf")
    _set_claimed_now(db, "t-coord2")
    _set_phase(db, "t-coord2", "dispatching")
    # Reflects reality: create_pool_batches registers batch rows in the same
    # `shards` table exactly when it flips the phase to 'dispatching'.
    db.upsert_shard({"id": "s-coord2-0", "task_id": "t-coord2", "shard_index": 0,
                      "status": "pending"})

    _task(db, "t-wait2", status="pending", mode="pool", source="hf")
    _worker(db, "w1")

    asyncio.run(reconciler.auto_dispatch_pending())

    assert db.get_task("t-wait2")["status"] == "downloading"
    assert "t-wait2" in calls


def test_pool_task_with_null_coordinator_phase_still_blocks_its_source(db, monkeypatch):
    """The hole the T7 review reproduced. Only create_pool_batches writes
    'dispatching', so a pool task that has not reached it yet carries no
    phase at all, and reading NULL as "not listing" left its source entirely
    unguarded during exactly the window the guard exists for: a second
    coordinator races the listing one for workers that are idle only because
    they are about to be claimed. Every claim route now also resets a pool
    phase to 'listing' (snapshot.CLAIM_RESET_PHASE_SQL, section 7 below), so
    NULL is the never-dispatched case — this test keeps it guarded even if a
    future claim route forgets the reset.

    The row below is shaped as a claim leaves it: status downloading,
    priority 0, server NULL, fresh claimed_at, no batch rows yet.
    """
    from dlm.web import reconciler
    import dlm.web.temporal_client as tc

    calls = []

    async def fake_start(task):
        calls.append(task["id"])

    monkeypatch.setattr(tc, "start_task_download", fake_start)

    _task(db, "t-preempted", status="downloading", mode="pool", source="hf")
    conn = db._conn()
    with conn:
        conn.execute(
            "UPDATE tasks SET priority = 0, server = NULL, claimed_at = ?, "
            "coordinator_phase = NULL WHERE id = ?",
            (time.time(), "t-preempted"),
        )
    assert db.get_task("t-preempted")["coordinator_phase"] is None

    _task(db, "t-wait-null", status="pending", mode="pool", source="hf")
    _worker(db, "w1")

    asyncio.run(reconciler.auto_dispatch_pending())

    assert db.get_task("t-wait-null")["status"] == "pending"
    assert calls == []


def test_sharded_task_with_no_shard_rows_still_blocks_source_g1_regression(db, monkeypatch):
    """This must fail if the pre-existing sharded criterion (NOT EXISTS
    shards) is ever narrowed, scoped to dispatch_mode, or otherwise altered
    — G1 requires the sharded path stay byte-identical."""
    from dlm.web import reconciler
    import dlm.web.temporal_client as tc

    calls = []

    async def fake_start(task):
        calls.append(task["id"])

    monkeypatch.setattr(tc, "start_task_download", fake_start)

    _task(db, "t-coord-sharded", status="downloading", mode="sharded", source="hf")
    _set_claimed_now(db, "t-coord-sharded")
    # No shard rows — the coordinator is still listing/filtering.

    _task(db, "t-wait-sharded", status="pending", mode="sharded", source="hf")
    _worker(db, "w1")

    asyncio.run(reconciler.auto_dispatch_pending())

    assert db.get_task("t-wait-sharded")["status"] == "pending"
    assert "t-wait-sharded" not in calls


# ── 5. add 两入口带 mode 落库 ────────────────────────────────────────────


def test_queue_add_persists_dispatch_mode(db):
    from dlm.web.routes.queue import add_to_queue

    out = _call(add_to_queue({"repo_id": "org/repo-pool", "dispatch_mode": "pool"}))
    assert out.get("ok") is True
    row = db.get_task(out["task_id"])
    assert row["dispatch_mode"] == "pool"


def test_queue_add_defaults_dispatch_mode_when_omitted(db, monkeypatch):
    """Reads fleet.DEFAULT_DISPATCH_MODE rather than hardcoding 'sharded'
    (G8) — proven by flipping the constant and checking the row follows."""
    import dlm.web.fleet as fleet
    from dlm.web.routes.queue import add_to_queue

    monkeypatch.setattr(fleet, "DEFAULT_DISPATCH_MODE", "pool")
    out = _call(add_to_queue({"repo_id": "org/repo-default"}))
    assert out.get("ok") is True
    row = db.get_task(out["task_id"])
    assert row["dispatch_mode"] == "pool"


def test_queue_add_rejects_invalid_dispatch_mode(db):
    from dlm.web.routes.queue import add_to_queue

    out = _call(add_to_queue({"repo_id": "org/repo-bad", "dispatch_mode": "bogus"}))
    assert "error" in out
    # Nothing was persisted for the rejected request.
    all_tasks = db.get_all_tasks()
    assert not any(t.get("repo_id") == "org/repo-bad" for t in all_tasks)


def test_add_task_endpoint_persists_dispatch_mode(db):
    from dlm.web.routes.tasks import AddTaskRequest, add_task

    req = AddTaskRequest(url_or_repo="org/repo-tasks-pool", dispatch_mode="pool")
    out = _call(add_task(req))
    task_id = out["task"]["id"]
    row = db.get_task(task_id)
    assert row["dispatch_mode"] == "pool"


def test_add_task_endpoint_defaults_dispatch_mode_when_omitted(db, monkeypatch):
    import dlm.web.fleet as fleet
    from dlm.web.routes.tasks import AddTaskRequest, add_task

    monkeypatch.setattr(fleet, "DEFAULT_DISPATCH_MODE", "pool")
    req = AddTaskRequest(url_or_repo="org/repo-tasks-default")
    out = _call(add_task(req))
    task_id = out["task"]["id"]
    row = db.get_task(task_id)
    assert row["dispatch_mode"] == "pool"


def test_add_task_endpoint_rejects_invalid_dispatch_mode(db):
    from fastapi import HTTPException

    from dlm.web.routes.tasks import AddTaskRequest, add_task

    req = AddTaskRequest(url_or_repo="org/repo-tasks-bad", dispatch_mode="bogus")
    with pytest.raises(HTTPException):
        _call(add_task(req))
    assert db.get_task is not None  # sanity: db fixture still usable
    all_tasks = db.get_all_tasks()
    assert not any(t.get("repo_id") == "org/repo-tasks-bad" for t in all_tasks)


# ── 6. the mode vocabulary is single-sourced and the default is validated ──
#
# /api/tasks validates only a *client-supplied* dispatch_mode; the value
# resolved from DEFAULT_DISPATCH_MODE went to upsert_task unchecked. So
# DLM_DEFAULT_DISPATCH_MODE="Pool" (a capitalisation typo in S1's .env) would
# have stored dispatch_mode='Pool' on every add: auto_dispatch claims it via
# the sharded branch, start_task_download raises ValueError, the claim is
# reverted to pending, and the task retries every 30s forever with nothing but
# a reconciler error line. Validating at import turns that into one refused
# `systemctl start dlm-web`.


def test_default_dispatch_mode_is_validated_at_import():
    import importlib

    import dlm.web.fleet as fleet

    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("DLM_DEFAULT_DISPATCH_MODE", "Pool")
        with pytest.raises(ValueError, match="DLM_DEFAULT_DISPATCH_MODE"):
            importlib.reload(fleet)
    # Restore the module to its real (env-free) state for the rest of the suite.
    importlib.reload(fleet)
    assert fleet.DEFAULT_DISPATCH_MODE in fleet.VALID_DISPATCH_MODES


def test_dispatch_mode_vocabulary_has_one_definition():
    """Four local `VALID_DISPATCH_MODES = {...}` literals (queue.add,
    queue.reshard, tasks.add) had already drifted on *when* they validate;
    a third mode would have needed four edits (G8: pool policy lives in
    fleet.py)."""
    import pathlib
    import re

    from dlm.web.fleet import VALID_DISPATCH_MODES

    assert VALID_DISPATCH_MODES == frozenset({"sharded", "pool"})

    root = pathlib.Path(__file__).resolve().parent.parent / "dlm"
    literal = re.compile(r"VALID_DISPATCH_MODES\s*=")
    definers = [
        p.relative_to(root).as_posix()
        for p in root.rglob("*.py")
        if literal.search(p.read_text())
    ]
    assert definers == ["web/fleet.py"], definers


# ── 7. every claim route resets a pool task's coordinator_phase ─────────
#
# The T7 re-review found the sibling of the NULL hole above: nothing ever
# CLEARS coordinator_phase, and resume/reshard put a task back to `pending`
# and delete its batch rows while leaving it at 'dispatching'. A later claim
# that refreshed only status/claimed_at therefore presented a coordinator
# that is still listing as "past listing", and the guard let a second
# coordinator onto the same source — with batch rows gone, "no rows" could
# not catch it either. Reproduced with a real preempt claim before the fix.
#
# The fix is one shared assignment (snapshot.CLAIM_RESET_PHASE_SQL) in every
# claim site a pool task can reach — /queue/preempt and auto_dispatch_pending;
# these tests pin the write side of each. Sharded rows write their own value
# back, which is the G1 case each test also asserts. reconcile()'s orphan
# re-dispatch was a third site until decision C removed pool from that path
# entirely, so the fragment is gone from there (T9 review finding I6).


def test_preempt_claim_resets_stale_pool_phase_to_listing(db, monkeypatch):
    from dlm.web.routes import queue as queue_routes
    import dlm.web.temporal_client as tc

    async def fake_start(task):
        return None

    async def fake_cancel(task_id, dispatch_mode=None):
        return None

    monkeypatch.setattr(tc, "start_task_download", fake_start)
    monkeypatch.setattr(tc, "cancel_workflow", fake_cancel)

    # Shaped by resume-after-reshard: the phase survived, the batch rows did not.
    _task(db, "t-urgent", status="pending", mode="pool", source="hf")
    _set_phase(db, "t-urgent", "dispatching")
    _task(db, "t-victim", status="downloading", mode="sharded", source="hf")

    result = asyncio.run(queue_routes.preempt_for_task({
        "urgent_task_id": "t-urgent",
        "victim_task_id": "t-victim",
        "target_server": "w1",
    }))

    assert result.get("ok") is True, result
    row = db.get_task("t-urgent")
    assert row["status"] == "downloading"
    assert row["coordinator_phase"] == "listing"


def test_preempt_claim_leaves_a_sharded_phase_untouched(db, monkeypatch):
    """G1: the CASE writes a sharded row's own value back."""
    from dlm.web.routes import queue as queue_routes
    import dlm.web.temporal_client as tc

    async def fake_start(task):
        return None

    async def fake_cancel(task_id, dispatch_mode=None):
        return None

    monkeypatch.setattr(tc, "start_task_download", fake_start)
    monkeypatch.setattr(tc, "cancel_workflow", fake_cancel)

    _task(db, "t-urgent-s", status="pending", mode="sharded", source="hf")
    _task(db, "t-victim-s", status="downloading", mode="sharded", source="hf")

    result = asyncio.run(queue_routes.preempt_for_task({
        "urgent_task_id": "t-urgent-s",
        "victim_task_id": "t-victim-s",
        "target_server": "w1",
    }))

    assert result.get("ok") is True, result
    assert db.get_task("t-urgent-s")["coordinator_phase"] is None


def test_auto_dispatch_claim_resets_stale_pool_phase_to_listing(db, monkeypatch):
    """auto_dispatch claims `pending` tasks, and resume leaves exactly this
    row: pending, phase 'dispatching', batch rows deleted."""
    from dlm.web import reconciler
    import dlm.web.temporal_client as tc

    async def fake_start(task):
        return None

    monkeypatch.setattr(tc, "start_task_download", fake_start)

    _task(db, "t-resumed", status="pending", mode="pool", source="hf")
    _set_phase(db, "t-resumed", "dispatching")
    _worker(db, "w1")

    asyncio.run(reconciler.auto_dispatch_pending())

    row = db.get_task("t-resumed")
    assert row["status"] == "downloading"
    assert row["coordinator_phase"] == "listing"


