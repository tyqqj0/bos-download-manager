"""T5 — pool window math and batch release endpoints.

The window formula is the fairness mechanism the whole pool design rests on
(it is what fixes v1's three bugs: windows that never constrain, a frozen
window that can't absorb a recovered worker, and unbounded concurrency once
several tasks are admitted). It shipped with only a stubbed constant behind
it in the workflow tests; these exercise the real thing against a real DB.

Run: pytest tests/test_pool_window.py -q
"""

from __future__ import annotations

import asyncio
import time

import pytest


def _call(coro):
    return asyncio.run(coro) if asyncio.iscoroutine(coro) else coro


def _task(db, task_id, status="downloading", *, mode="pool", priority=5, source="hf"):
    """mode=None omits dispatch_mode entirely, i.e. what every existing caller
    does — the case where the schema default has to be applied."""
    row = {"id": task_id, "name": task_id, "repo_id": "org/x",
           "status": status, "priority": priority, "source": source}
    if mode is not None:
        row["dispatch_mode"] = mode
    db.upsert_task(row)


def _worker(db, key, *, fresh=True):
    db.update_worker(hostname=key, server_key=key, disk_free_gb=500)
    if not fresh:
        # alive_workers reads last_seen; update_worker always stamps "now", so
        # an offline worker has to be aged directly.
        conn = db._conn()
        with conn:
            conn.execute("UPDATE workers SET last_seen=? WHERE server_key=?",
                         (time.time() - 100000, key))


# ── weights ─────────────────────────────────────────────────────────


def test_pool_task_weight_bands():
    from dlm.web.fleet import POOL_WEIGHT_DEFAULT, POOL_WEIGHT_P0, pool_task_weight

    assert pool_task_weight(0) == POOL_WEIGHT_P0
    assert pool_task_weight(2) == POOL_WEIGHT_P0       # inclusive upper bound
    assert pool_task_weight(3) == POOL_WEIGHT_DEFAULT
    assert pool_task_weight(9) == POOL_WEIGHT_DEFAULT
    assert pool_task_weight(None) == POOL_WEIGHT_P0    # missing priority = 0
    assert POOL_WEIGHT_P0 > POOL_WEIGHT_DEFAULT


# ── GET /api/pool/window ────────────────────────────────────────────


def test_window_is_full_capacity_for_a_lone_task(db):
    from dlm.web.routes.queue import pool_window_api

    _task(db, "t-solo")
    for k in ("w1", "w2", "w3", "w4", "w5", "w6", "w7"):
        _worker(db, k)

    out = _call(pool_window_api("t-solo"))
    assert out["p"] == 7
    assert out["window"] == 7          # the only task gets the whole fleet
    assert out["active_pool_tasks"] == 1


def test_window_splits_by_weight_between_two_tasks(db):
    """P0 (priority<=2) takes 1.5x the share of a normal task: with P=7 and
    weights 1.5 + 1.0, the P0 task gets floor(7*1.5/2.5)=4 and the other
    floor(7*1.0/2.5)=2. Total <= P, which is what bounds pool concurrency."""
    from dlm.web.routes.queue import pool_window_api

    _task(db, "t-p0", priority=0)
    _task(db, "t-normal", priority=5)
    for k in ("w1", "w2", "w3", "w4", "w5", "w6", "w7"):
        _worker(db, k)

    p0 = _call(pool_window_api("t-p0"))
    normal = _call(pool_window_api("t-normal"))

    assert (p0["window"], normal["window"]) == (4, 2)
    assert p0["window"] + normal["window"] <= p0["p"]
    assert p0["window"] > normal["window"]


def test_window_floor_is_one_when_share_rounds_to_zero(db):
    """A task outvoted by many others still gets one slot — it makes slow
    progress instead of stalling until its neighbours finish."""
    from dlm.web.routes.queue import pool_window_api

    for i in range(6):
        _task(db, f"t-{i}", priority=0)
    _task(db, "t-tiny", priority=9)
    _worker(db, "w1")           # P=1, so every fair share is < 1

    out = _call(pool_window_api("t-tiny"))
    assert out["window"] == 1


def test_window_is_one_when_no_workers_are_alive(db):
    """P=0 must not produce window=0 — that would deadlock the loop, which
    dispatches nothing and then waits for a completion that cannot arrive."""
    from dlm.web.routes.queue import pool_window_api

    _task(db, "t-solo")
    _worker(db, "w1", fresh=False)      # stale heartbeat = not alive

    out = _call(pool_window_api("t-solo"))
    assert out["p"] == 0
    assert out["window"] == 1


def test_window_ignores_tasks_on_a_different_source(db):
    """A ModelScope task competes for bj* workers, an HF task for w* — neither
    consumes the other's P, so neither may dilute the other's share."""
    from dlm.web.routes.queue import pool_window_api

    _task(db, "t-hf", source="hf")
    _task(db, "t-ms", source="modelscope")
    for k in ("w1", "w2", "w3", "w4"):
        _worker(db, k)
    for k in ("bj1", "bj2"):
        _worker(db, k)

    hf = _call(pool_window_api("t-hf"))
    assert hf["p"] == 4                 # only the w* workers serve hf
    assert hf["active_pool_tasks"] == 1  # the MS task is not competition
    assert hf["window"] == 4


def test_window_ignores_sharded_tasks(db):
    """Sharded tasks own whole machines through a different mechanism; they
    are not pool competitors."""
    from dlm.web.routes.queue import pool_window_api

    _task(db, "t-pool", mode="pool")
    _task(db, "t-sharded", mode="sharded")
    for k in ("w1", "w2"):
        _worker(db, k)

    out = _call(pool_window_api("t-pool"))
    assert out["active_pool_tasks"] == 1
    assert out["window"] == 2


def test_window_counts_the_asking_task_even_before_its_status_lands(db):
    """The first wake can race the task's own `downloading` write. Without the
    self-inclusion fixup the denominator would omit this task and hand it more
    than its share."""
    from dlm.web.routes.queue import pool_window_api

    _task(db, "t-new", status="pending")       # not yet downloading
    _task(db, "t-running", status="downloading")
    for k in ("w1", "w2", "w3", "w4"):
        _worker(db, k)

    out = _call(pool_window_api("t-new"))
    assert out["weight_sum"] == 2.0    # itself + the running task
    assert out["window"] == 2          # floor(4 * 1.0 / 2.0)


def test_window_for_unknown_task_is_an_error(db):
    from dlm.web.routes.queue import pool_window_api

    out = _call(pool_window_api("t-nope"))
    assert "error" in out


# ── POST /api/pool/batches/release ──────────────────────────────────


def test_release_resets_claiming_rows_and_leaves_done_and_failed_alone(db):
    from dlm.web.routes.queue import release_pool_batches

    _task(db, "t-rel", status="paused")
    db.upsert_shard({"id": "s-t-rel-0", "task_id": "t-rel", "shard_index": 0,
                     "status": "running", "server": "w3", "speed_mbps": 42})
    db.upsert_shard({"id": "s-t-rel-1", "task_id": "t-rel", "shard_index": 1,
                     "status": "done", "server": "w4", "speed_mbps": 0})
    db.upsert_shard({"id": "s-t-rel-2", "task_id": "t-rel", "shard_index": 2,
                     "status": "failed", "server": "w5", "speed_mbps": 0})

    out = _call(release_pool_batches({"task_id": "t-rel"}))
    assert out["ok"] is True
    assert out["released"] == 1        # only the row claiming a worker

    rows = {r["id"]: r for r in db.get_shards_by_task("t-rel")}
    assert rows["s-t-rel-0"]["status"] == "pending"
    assert rows["s-t-rel-0"]["server"] is None
    assert rows["s-t-rel-0"]["speed_mbps"] == 0
    # a done row's bytes are on BOS; its server attribution stays intact
    assert rows["s-t-rel-1"]["status"] == "done"
    assert rows["s-t-rel-1"]["server"] == "w4"
    # a failed row keeps its status: it is the only per-batch attribution an
    # operator has after "N/M batches failed". Re-dispatch resets it via the
    # batch-create endpoint, not via release.
    assert rows["s-t-rel-2"]["status"] == "failed"


def test_release_is_idempotent_and_needs_a_task_id(db):
    from dlm.web.routes.queue import release_pool_batches

    _task(db, "t-rel2", status="paused")
    db.upsert_shard({"id": "s-t-rel2-0", "task_id": "t-rel2", "shard_index": 0,
                     "status": "running", "server": "w3"})

    assert _call(release_pool_batches({"task_id": "t-rel2"}))["released"] == 1
    # Calling again is harmless: an already-pending row still matches the
    # not-done filter, so rowcount stays 1 while the state does not move.
    assert _call(release_pool_batches({"task_id": "t-rel2"}))["ok"] is True
    row = db.get_shards_by_task("t-rel2")[0]
    assert (row["status"], row["server"]) == ("pending", None)
    assert "error" in _call(release_pool_batches({}))


def test_release_does_not_touch_another_task(db):
    from dlm.web.routes.queue import release_pool_batches

    _task(db, "t-a", status="paused")
    _task(db, "t-b")
    db.upsert_shard({"id": "s-t-a-0", "task_id": "t-a", "shard_index": 0,
                     "status": "running", "server": "w1"})
    db.upsert_shard({"id": "s-t-b-0", "task_id": "t-b", "shard_index": 0,
                     "status": "running", "server": "w2"})

    _call(release_pool_batches({"task_id": "t-a"}))

    other = db.get_shards_by_task("t-b")[0]
    assert other["status"] == "running" and other["server"] == "w2"


def test_upsert_task_defaults_dispatch_mode_rather_than_nulling_it(db):
    """Naming dispatch_mode in upsert_task's column list means an omitted key
    INSERTs an explicit NULL, which overrides the schema DEFAULT. Every task
    is created through this path, so the default has to be applied here."""
    _task(db, "t-plain", status="pending", mode=None)
    assert db.get_task("t-plain")["dispatch_mode"] == "sharded"


def test_upsert_task_preserves_pool_mode_across_a_partial_write(db):
    """A progress write that omits dispatch_mode must not revert a running
    pool task to sharded."""
    _task(db, "t-pool-x", status="pending", mode="pool")
    _task(db, "t-pool-x", status="downloading", mode=None)
    assert db.get_task("t-pool-x")["dispatch_mode"] == "pool"
