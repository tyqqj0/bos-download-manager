"""T8 — stop/rollback control for pool tasks (must land before T7 makes pool
tasks startable — see plan v3.1 §T8).

Four things, each mapped straight to a plan requirement:

1. pause on a pool task resets its running batch rows (reusing
   release_pool_batches' SQL) but a *sharded* task's pause must not go
   through that reset — its SQL clears every non-done/non-failed row
   regardless of dispatch_mode, and a sharded task's shard rows are still
   owned by the sharded workflow's own bookkeeping.
2. resume deletes shard/batch rows so a pool task's next chunking pass
   doesn't collide with stale rows in create_pool_batches_in_db's
   idempotency check.
3. reshard round-trips dispatch_mode (sharded -> pool -> sharded) and
   max_workers falls back correctly (NULL treated as 0, never passed bare
   into int()).
4. reshard's relaxed validation: shard_count>=1 OR a valid dispatch_mode is
   enough; neither is an error; an invalid dispatch_mode string is an error,
   not a silent fallback to 'sharded'.

Nothing here talks to a live Temporal server — cancel_workflow and
terminate_workflow_and_wait are monkeypatched on the dlm.web.temporal_client
module, which is where pause_task/reshard_task import them from (inside the
function body, so the patched attribute is what they see at call time).

Run: python3 -m pytest tests/test_pool_stop_control.py -q
"""

from __future__ import annotations

import asyncio

import pytest


def _call(coro):
    return asyncio.run(coro) if asyncio.iscoroutine(coro) else coro


def _task(db, task_id, status="downloading", *, mode="pool", priority=5, source="hf"):
    row = {"id": task_id, "name": task_id, "repo_id": "org/x",
           "status": status, "priority": priority, "source": source}
    if mode is not None:
        row["dispatch_mode"] = mode
    db.upsert_task(row)


def _stub_cancel(monkeypatch, calls):
    import dlm.web.temporal_client as tc

    async def fake_cancel(task_id, dispatch_mode=None):
        calls.append((task_id, dispatch_mode))

    monkeypatch.setattr(tc, "cancel_workflow", fake_cancel)


def _stub_terminate(monkeypatch, calls, *, ok=True):
    import dlm.web.temporal_client as tc

    async def fake_terminate(task_id, timeout_s=120, dispatch_mode=None):
        calls.append((task_id, dispatch_mode))
        return ok

    monkeypatch.setattr(tc, "terminate_workflow_and_wait", fake_terminate)


# ── 1. pause 后无 running 行 ────────────────────────────────────────────


def test_pause_pool_task_resets_running_rows_but_keeps_terminal_ones(db, monkeypatch):
    from dlm.web.routes.queue import pause_task

    calls = []
    _stub_cancel(monkeypatch, calls)

    _task(db, "t-pool-pause", status="downloading", mode="pool")
    db.upsert_shard({"id": "s-pp-0", "task_id": "t-pool-pause", "shard_index": 0,
                      "status": "running", "server": "w1", "speed_mbps": 12})
    db.upsert_shard({"id": "s-pp-1", "task_id": "t-pool-pause", "shard_index": 1,
                      "status": "done", "server": "w2"})
    db.upsert_shard({"id": "s-pp-2", "task_id": "t-pool-pause", "shard_index": 2,
                      "status": "failed", "server": "w3"})

    out = _call(pause_task({"task_id": "t-pool-pause"}))
    assert out["ok"] is True
    assert out["status"] == "paused"
    assert calls == [("t-pool-pause", "pool")]   # cancel_workflow told the task's mode

    rows = {r["id"]: r for r in db.get_shards_by_task("t-pool-pause")}
    assert rows["s-pp-0"]["status"] == "pending"    # no row is left running
    assert rows["s-pp-0"]["server"] is None
    assert rows["s-pp-1"]["status"] == "done"        # untouched
    assert rows["s-pp-2"]["status"] == "failed"      # untouched — attribution preserved
    assert db.get_task("t-pool-pause")["status"] == "paused"


def test_pause_sharded_task_leaves_its_running_shard_rows_alone(db, monkeypatch):
    """The G1 regression guard: this must fail if the dispatch_mode gate on
    the pause reset is ever removed (release_pool_batches' SQL would then
    also sweep sharded shard rows, which the sharded workflow still owns)."""
    from dlm.web.routes.queue import pause_task

    calls = []
    _stub_cancel(monkeypatch, calls)

    _task(db, "t-sharded-pause", status="downloading", mode="sharded")
    db.upsert_shard({"id": "s-sp-0", "task_id": "t-sharded-pause", "shard_index": 0,
                      "status": "running", "server": "w1", "speed_mbps": 99})

    out = _call(pause_task({"task_id": "t-sharded-pause"}))
    assert out["ok"] is True
    assert calls == [("t-sharded-pause", "sharded")]

    row = db.get_shards_by_task("t-sharded-pause")[0]
    assert row["status"] == "running"
    assert row["server"] == "w1"
    assert row["speed_mbps"] == 99


# ── 2. resume 无陈行 ─────────────────────────────────────────────────────


def test_resume_pool_task_deletes_stale_shard_rows(db):
    from dlm.web.routes.queue import resume_task

    _task(db, "t-pool-resume", status="paused", mode="pool")
    db.upsert_shard({"id": "s-pr-0", "task_id": "t-pool-resume", "shard_index": 0,
                      "status": "done", "server": "w1"})
    db.upsert_shard({"id": "s-pr-1", "task_id": "t-pool-resume", "shard_index": 1,
                      "status": "failed", "server": "w2"})
    assert len(db.get_shards_by_task("t-pool-resume")) == 2

    out = _call(resume_task({"task_id": "t-pool-resume"}))
    assert out["ok"] is True
    assert out["status"] == "pending"
    assert db.get_shards_by_task("t-pool-resume") == []
    assert db.get_task("t-pool-resume")["status"] == "pending"


# ── 3. 模式往返 ──────────────────────────────────────────────────────────


def test_reshard_mode_round_trip_sharded_pool_sharded(db, monkeypatch):
    from dlm.web.routes.queue import reshard_task

    calls = []
    _stub_terminate(monkeypatch, calls)

    _task(db, "t-roundtrip", status="downloading", mode="sharded")
    conn = db._conn()
    conn.execute("UPDATE tasks SET max_workers=6 WHERE id=?", ("t-roundtrip",))
    conn.commit()

    # sharded -> pool: a pure mode flip, no shard_count needed for pool.
    out1 = _call(reshard_task({"task_id": "t-roundtrip", "dispatch_mode": "pool"}))
    assert out1.get("ok") is True
    row1 = db.get_task("t-roundtrip")
    assert row1["dispatch_mode"] == "pool"
    assert row1["status"] == "pending"
    assert calls[-1] == ("t-roundtrip", "sharded")   # terminate saw the PRE-flip mode

    # pool -> sharded: no shard_count supplied either — falls back to the
    # max_workers already on the row (decision #5).
    out2 = _call(reshard_task({"task_id": "t-roundtrip", "dispatch_mode": "sharded"}))
    assert out2.get("ok") is True
    row2 = db.get_task("t-roundtrip")
    assert row2["dispatch_mode"] == "sharded"
    assert row2["max_workers"] == 6                  # preserved across the round trip
    assert calls[-1] == ("t-roundtrip", "pool")       # this terminate saw pool as pre-flip

    # An explicit shard_count still overrides max_workers as before.
    out3 = _call(reshard_task({"task_id": "t-roundtrip", "shard_count": 3}))
    assert out3.get("ok") is True
    row3 = db.get_task("t-roundtrip")
    assert row3["max_workers"] == 3
    assert row3["dispatch_mode"] == "sharded"          # unspecified -> unchanged


def test_reshard_pool_to_sharded_treats_null_max_workers_as_zero(db, monkeypatch):
    """decision #5: max_workers can be NULL even though the schema default is
    0. Flipping pool -> sharded with no shard_count must not crash trying to
    int(None), and must land on 0 (= auto shard count), not None."""
    from dlm.web.routes.queue import reshard_task

    _stub_terminate(monkeypatch, [])

    _task(db, "t-null-mw", status="downloading", mode="pool")
    assert db.get_task("t-null-mw")["max_workers"] is None

    out = _call(reshard_task({"task_id": "t-null-mw", "dispatch_mode": "sharded"}))
    assert out.get("ok") is True
    row = db.get_task("t-null-mw")
    assert row["dispatch_mode"] == "sharded"
    assert row["max_workers"] == 0


# ── 4. 纯模式 flip ───────────────────────────────────────────────────────


def test_reshard_pure_mode_flip_and_its_rejections(db, monkeypatch):
    from dlm.web.routes.queue import reshard_task

    _stub_terminate(monkeypatch, [])

    _task(db, "t-flip", status="downloading", mode="sharded")

    # dispatch_mode alone, no shard_count: accepted.
    out = _call(reshard_task({"task_id": "t-flip", "dispatch_mode": "pool"}))
    assert out.get("ok") is True
    assert db.get_task("t-flip")["dispatch_mode"] == "pool"

    # Neither shard_count nor dispatch_mode: rejected.
    _task(db, "t-flip2", status="downloading", mode="sharded")
    out2 = _call(reshard_task({"task_id": "t-flip2"}))
    assert "error" in out2
    assert db.get_task("t-flip2")["status"] == "downloading"   # unchanged

    # An invalid dispatch_mode string: rejected outright, not silently
    # defaulted to 'sharded'.
    out3 = _call(reshard_task({"task_id": "t-flip2", "dispatch_mode": "bogus"}))
    assert "error" in out3
    assert db.get_task("t-flip2")["dispatch_mode"] == "sharded"   # unchanged
    assert db.get_task("t-flip2")["status"] == "downloading"      # unchanged


def test_reshard_still_requires_task_id(db):
    from dlm.web.routes.queue import reshard_task

    out = _call(reshard_task({"dispatch_mode": "pool"}))
    assert "error" in out


def test_reshard_rejects_a_negative_shard_count_instead_of_ignoring_it(db, monkeypatch):
    """A negative shard_count is a typo, never "not supplied". Read as the
    latter it takes the shard_count<1 branch, so the call terminates the
    workflows and answers ok with the OLD count — the operator is told the
    reshard happened and it did not."""
    from dlm.web.routes.queue import reshard_task

    calls: list = []
    _stub_terminate(monkeypatch, calls)
    _task(db, "t-neg", status="downloading", mode="sharded")
    conn = db._conn()
    with conn:
        conn.execute("UPDATE tasks SET max_workers = 12 WHERE id = ?", ("t-neg",))

    out = _call(reshard_task({"task_id": "t-neg", "shard_count": -1,
                              "dispatch_mode": "sharded"}))

    assert "error" in out
    # rejected before anything irreversible: no terminate, count untouched
    assert calls == []
    assert db.get_task("t-neg")["max_workers"] == 12


# ── supplementary: cancel_workflow / terminate_workflow_and_wait's own
#    dispatch_mode branch (plan change #1) ──────────────────────────────
#
# The four tests above stub these two functions out entirely at the
# pause/reshard call sites, so they never exercise the branch inside
# cancel_workflow/terminate_workflow_and_wait itself — the part that skips
# building a ShardWorkerWorkflow handle per shard row for a pool task. These
# two tests cover that directly, against a fake Temporal client, so the
# dispatch_mode=='pool' skip has its own falsifiable coverage.


class _FakeHandle:
    def __init__(self, wf_id, log):
        self.wf_id = wf_id
        self._log = log

    async def cancel(self, rpc_timeout=None):
        self._log.append(("cancel", self.wf_id))

    async def terminate(self, reason=None, rpc_timeout=None):
        self._log.append(("terminate", self.wf_id))

    async def describe(self, rpc_timeout=None):
        from types import SimpleNamespace
        from temporalio.client import WorkflowExecutionStatus
        return SimpleNamespace(status=WorkflowExecutionStatus.COMPLETED)


class _FakeClient:
    def __init__(self, log):
        self._log = log

    def get_workflow_handle(self, wf_id):
        return _FakeHandle(wf_id, self._log)


def _patch_temporal_plumbing(monkeypatch, log, shard_rows):
    import dlm.web.temporal_client as tc

    async def fake_connected_client(timeout=None):
        return _FakeClient(log)

    async def fake_find_running(client, task_id):
        return []

    monkeypatch.setattr(tc, "connected_client", fake_connected_client)
    monkeypatch.setattr(tc, "_find_running_workflow_ids", fake_find_running)
    monkeypatch.setattr(
        "dlm.queue.snapshot.get_shards_by_task",
        lambda task_id: (log.append(("get_shards_by_task", task_id)), shard_rows)[1],
    )
    monkeypatch.setattr("dlm.queue.snapshot.init_db", lambda: None)


def test_cancel_workflow_skips_the_shard_sweep_in_pool_mode(monkeypatch):
    from dlm.web import temporal_client as tc

    log = []
    _patch_temporal_plumbing(monkeypatch, log, [{"id": "s-should-not-be-touched"}])

    asyncio.run(tc.cancel_workflow("t-pc", dispatch_mode="pool"))
    assert ("get_shards_by_task", "t-pc") not in log
    assert not any(op == "cancel" and "shard-" in wf_id for op, wf_id in log)
    assert ("cancel", "pool-t-pc") in log   # the parent handle is still cancelled

    log.clear()
    asyncio.run(tc.cancel_workflow("t-sc", dispatch_mode="sharded"))
    assert ("get_shards_by_task", "t-sc") in log
    assert ("cancel", "shard-s-should-not-be-touched") in log


def test_terminate_workflow_and_wait_skips_the_shard_sweep_in_pool_mode(monkeypatch):
    from dlm.web import temporal_client as tc

    log = []
    _patch_temporal_plumbing(monkeypatch, log, [{"id": "s-should-not-be-touched"}])

    closed = asyncio.run(tc.terminate_workflow_and_wait("t-pt", dispatch_mode="pool"))
    assert closed is True
    assert ("get_shards_by_task", "t-pt") not in log
    assert not any(op == "terminate" and "shard-" in wf_id for op, wf_id in log)

    log.clear()
    closed2 = asyncio.run(tc.terminate_workflow_and_wait("t-st", dispatch_mode="sharded"))
    assert closed2 is True
    assert ("get_shards_by_task", "t-st") in log
    assert ("terminate", "shard-s-should-not-be-touched") in log
