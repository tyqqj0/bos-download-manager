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


def _dispatchable(db, task_id, *, pending=99, reported=True):
    """Register batch rows so the task reads as having work it can dispatch.

    A downloading pool task always has its batch rows in production — they are
    registered at chunk time, before the window loop's first wake — so a test
    of the weight math needs them to travel the same path the fleet does.
    Without them pool_task_slot_cap caps the task at 0 (correctly: a task with
    no rows is still listing) and every weighted share collapses to the floor
    of 1, which would make these tests assert the floor rather than the split.
    """
    if reported:
        db.upsert_shard({"id": f"s-{task_id}-r", "task_id": task_id,
                         "shard_index": 0, "status": "done", "server": "w1"})
    for i in range(1, pending + 1):
        db.upsert_shard({"id": f"s-{task_id}-{i}", "task_id": task_id,
                         "shard_index": i, "status": "pending"})


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
    _dispatchable(db, "t-solo")
    for k in ("w1", "w2", "w3", "w4", "w5", "w6", "w7"):
        _worker(db, k)

    out = _call(pool_window_api("t-solo"))
    assert out["p"] == 7
    assert out["window"] == 7          # the only task gets the whole fleet
    assert out["active_pool_tasks"] == 1


def test_window_splits_by_weight_between_two_tasks(db):
    """P0 (priority<=2) takes 1.5x the share of a normal task, and the split
    leaves nothing on the floor: with P=7 and weights 1.5 + 1.0 the exact
    shares are 4.2 and 2.8, so the P0 task takes 4 and the residual slot goes
    to the larger fractional part (0.8) — 4 + 3 = 7. Flooring each share
    independently would have given 4 + 2 and idled a worker."""
    from dlm.web.routes.queue import pool_window_api

    _task(db, "t-p0", priority=0)
    _task(db, "t-normal", priority=5)
    _dispatchable(db, "t-p0")
    _dispatchable(db, "t-normal")
    for k in ("w1", "w2", "w3", "w4", "w5", "w6", "w7"):
        _worker(db, k)

    p0 = _call(pool_window_api("t-p0"))
    normal = _call(pool_window_api("t-normal"))

    assert (p0["window"], normal["window"]) == (4, 3)
    assert p0["window"] + normal["window"] == p0["p"]
    assert p0["window"] > normal["window"]


def test_window_floor_is_one_when_share_rounds_to_zero(db):
    """A task outvoted by many others still gets one slot — it makes slow
    progress instead of stalling until its neighbours finish."""
    from dlm.web.routes.queue import pool_window_api

    for i in range(6):
        _task(db, f"t-{i}", priority=0)
        _dispatchable(db, f"t-{i}")
    _task(db, "t-tiny", priority=9)
    # Batch rows on every task, so each cap is non-binding and the window really
    # is decided by the rounding — without them every cap is 0 and the test
    # passes off the cap-0 floor instead, which is a different code path.
    _dispatchable(db, "t-tiny")
    _worker(db, "w1")           # P=1, so every fair share is < 1

    out = _call(pool_window_api("t-tiny"))
    assert out["window"] == 1
    assert out["caps"]["t-tiny"] == 99       # the cap is not what floored it


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
    _dispatchable(db, "t-hf")
    _dispatchable(db, "t-ms")
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
    _dispatchable(db, "t-pool")
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
    _dispatchable(db, "t-new")
    _dispatchable(db, "t-running")
    for k in ("w1", "w2", "w3", "w4"):
        _worker(db, k)

    out = _call(pool_window_api("t-new"))
    assert out["weight_sum"] == 2.0    # itself + the running task
    assert out["window"] == 2          # floor(4 * 1.0 / 2.0)


def test_window_for_unknown_task_is_an_error(db):
    from dlm.web.routes.queue import pool_window_api

    out = _call(pool_window_api("t-nope"))
    assert "error" in out


# ── the allocation itself: no slot left unassigned ──────────────────
#
# The formula used to floor each task's share independently, which throws the
# residual away. In production on 2026-08-09 that residual was a whole HK
# worker idling for the length of a 19.7 TiB backlog: per-worker throughput was
# identical on both live tasks (~44 GiB/h), so an unassigned slot is not
# absorbed anywhere — it is simply lost throughput.


def test_allocation_hands_out_every_slot(db):
    """sum(window) == P for any weight mix, as long as tasks <= P."""
    from dlm.web.fleet import pool_window_allocation

    for p in range(1, 12):
        for priorities in ([0, 5], [0, 0, 5], [5, 5, 5], [0, 5, 5, 9], [0]):
            if len(priorities) > p:
                continue        # the floor of 1 legitimately over-subscribes
            active = [{"id": f"t{i}", "priority": pr}
                      for i, pr in enumerate(priorities)]
            alloc = pool_window_allocation(active, p)
            assert sum(alloc.values()) == p, (p, priorities, alloc)
            assert all(n >= 1 for n in alloc.values()), (p, priorities, alloc)


def test_equal_weights_give_the_leftover_slot_to_whoever_came_first(db):
    """Two identical tasks, an odd number of workers: the older task takes the
    spare one. Ties resolve by position because the caller passes `active` in
    the DB's dispatch order (priority ASC, created_at ASC) and the sort is
    stable — the same first-come rule the queue already runs on, instead of an
    ordering that would look arbitrary to whoever is watching the dashboard."""
    from dlm.web.fleet import pool_window_allocation

    older = {"id": "t-older", "priority": 5}
    newer = {"id": "t-newer", "priority": 5}

    alloc = pool_window_allocation([older, newer], 7)

    assert (alloc["t-older"], alloc["t-newer"]) == (4, 3)
    # ...and it is genuinely the order that decides, not the id.
    flipped = pool_window_allocation([newer, older], 7)
    assert (flipped["t-newer"], flipped["t-older"]) == (4, 3)


def test_the_asking_task_is_last_in_the_tie_break_before_its_status_lands(db):
    """A task racing its own `downloading` write is the newest competitor by
    definition, so it must not win the spare slot from a task already running."""
    from dlm.web.routes.queue import pool_window_api

    _task(db, "t-running", status="downloading", priority=5)
    _task(db, "t-new", status="pending", priority=5)
    _dispatchable(db, "t-running")
    _dispatchable(db, "t-new")
    for k in ("w1", "w2", "w3", "w4", "w5"):
        _worker(db, k)

    new = _call(pool_window_api("t-new"))

    assert new["p"] == 5
    assert new["window"] == 2                     # 3 goes to the running task
    assert new["allocation"] == {"t-running": 3, "t-new": 2}


def test_allocation_never_returns_zero_when_there_is_no_capacity(db):
    """P=0 (or no weight) must floor to 1, not 0: a window of 0 dispatches
    nothing and then waits for a completion that cannot arrive."""
    from dlm.web.fleet import pool_window_allocation

    assert pool_window_allocation([{"id": "t1", "priority": 0}], 0) == {"t1": 1}
    assert pool_window_allocation([], 7) == {}


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


# ═══════════════════════════════════════════════════════════════════════
# #89 — an allocation is only worth something if the task can dispatch into it
# ═══════════════════════════════════════════════════════════════════════
#
# Weight-only allocation assumes every task can absorb its share. Production,
# 2026-08-09, 3 of 7 HK workers idle for 17 hours with two live tasks:
#
#   molmobot-data   pending=0, running=1   held 3 slots until it went `done`
#   robocasa365     allocated 4 (1.5x weight), used 1 — its coordinator's own
#                   window is hardcoded to 1 until the first batch reports, and
#                   that batch was the XET-slow one
#   RealOmin        699 pending batches, capped at 3
#
# Neither is #88 regressing: under the old floor() the same moment produced
# 2+1+1 = 4 of 7 and idled the same three machines.


def _batches(*statuses):
    """Batch rows shaped the way pool_task_slot_cap reads them."""
    return [{"status": s} for s in statuses]


def test_slot_cap_is_running_plus_pending_once_a_batch_has_reported(db):
    from dlm.web.fleet import pool_task_slot_cap

    # 2 running + 3 pending, and a done row proving the ramp is over
    assert pool_task_slot_cap(_batches("done", "running", "running",
                                       "pending", "pending", "pending")) == 5
    # done rows themselves are not capacity
    assert pool_task_slot_cap(_batches("done", "done", "done")) == 0
    # a failed row also counts as "has reported" — the coordinator writes
    # failures through the same record_batches_and_window call — AND as
    # capacity, because run()'s step 6 re-dispatches exactly those rows
    assert pool_task_slot_cap(_batches("failed", "pending", "pending")) == 3


def test_slot_cap_of_a_task_that_has_never_reported_is_one(db):
    """The ramp: workflows.py:_run_window_loop starts at window=1 and only
    reads the allocator's number after the first record_batches_and_window
    returns. Until then the task cannot dispatch a second batch no matter what
    it is allocated, so anything beyond the one slot it holds is dead."""
    from dlm.web.fleet import pool_task_slot_cap

    assert pool_task_slot_cap(_batches("running", *["pending"] * 14)) == 1
    assert pool_task_slot_cap(_batches("pending", "pending")) == 1
    # ...and a task still listing/chunking has no rows at all: it caps at 0 and
    # picks up the allocator's floor of 1, which it does not use.
    assert pool_task_slot_cap([]) == 0


def test_a_task_with_no_pending_batches_stops_reserving_slots(db):
    """Criterion 1 — the molmobot half. A winding-down task must not hold the
    share its weight would earn it; the surplus goes to whoever has work."""
    from dlm.web.fleet import pool_window_allocation

    winding_down = {"id": "t-winddown", "priority": 5}
    hungry = {"id": "t-hungry", "priority": 5}
    caps = {"t-winddown": 1, "t-hungry": 699}

    alloc = pool_window_allocation([winding_down, hungry], 7, caps)

    assert alloc == {"t-winddown": 1, "t-hungry": 6}
    assert sum(alloc.values()) == 7          # still no idle slot


def test_a_ramping_task_does_not_reserve_slots_it_cannot_dispatch(db):
    """Criterion 2 — the robocasa365 half, weights and all: the ramping task
    carries the HEAVIER weight (priority 2 = 1.5x against priority 5's 1.0),
    so weight-only allocation gives it the larger share precisely when it can
    use the least. 4/3 becomes 1/6."""
    from dlm.web.fleet import pool_window_allocation

    ramping = {"id": "t-ramp", "priority": 2}
    running = {"id": "t-run", "priority": 5}

    uncapped = pool_window_allocation([ramping, running], 7)
    assert uncapped == {"t-ramp": 4, "t-run": 3}          # today's behaviour

    alloc = pool_window_allocation([ramping, running], 7,
                                   {"t-ramp": 1, "t-run": 699})
    assert alloc == {"t-ramp": 1, "t-run": 6}


def test_a_task_reclaims_its_weighted_share_once_its_first_batch_lands(db):
    """Criterion 3 — the cap must not be sticky. The moment the ramping task
    has a terminal row, record_batches_and_window's next call (it writes the
    terminal rows BEFORE querying the window, activities.py:1611-1629) sees the
    full weight again."""
    from dlm.web.fleet import pool_task_slot_cap, pool_window_allocation

    ramping = {"id": "t-ramp", "priority": 2}
    running = {"id": "t-run", "priority": 5}

    before = _batches("running", *["pending"] * 14)
    after = _batches("done", *["pending"] * 14)
    assert pool_task_slot_cap(before) == 1
    assert pool_task_slot_cap(after) == 14

    alloc = pool_window_allocation(
        [ramping, running], 7,
        {"t-ramp": pool_task_slot_cap(after), "t-run": 699})
    assert alloc == {"t-ramp": 4, "t-run": 3}


def test_caps_do_not_regress_the_88_invariant(db):
    """Criterion 4 — when every task has room, capping changes nothing: the
    #88 guarantee (sum == P, every slot assigned) has to survive verbatim."""
    from dlm.web.fleet import pool_window_allocation

    for p in range(1, 12):
        for priorities in ([0, 5], [0, 0, 5], [5, 5, 5], [0, 5, 5, 9], [0]):
            if len(priorities) > p:
                continue
            active = [{"id": f"t{i}", "priority": pr}
                      for i, pr in enumerate(priorities)]
            roomy = {t["id"]: 1000 for t in active}
            assert (pool_window_allocation(active, p, roomy)
                    == pool_window_allocation(active, p)), (p, priorities)


def test_slots_no_task_can_use_are_left_unassigned(db):
    """Criterion 5 — the generalised invariant, sum == min(P, sum(caps)).
    Handing a slot to a task that will sit on it is strictly worse than leaving
    it unassigned, because the reservation also hides the idleness from
    whoever is looking at the dashboard."""
    from dlm.web.fleet import pool_window_allocation

    active = [{"id": "t-a", "priority": 5}, {"id": "t-b", "priority": 5}]
    alloc = pool_window_allocation(active, 7, {"t-a": 2, "t-b": 3})

    assert alloc == {"t-a": 2, "t-b": 3}
    assert sum(alloc.values()) == 5           # NOT 7 — two slots have no taker
    # and the caps are honoured, not quietly exceeded to satisfy sum == P
    assert alloc["t-a"] <= 2 and alloc["t-b"] <= 3


def test_a_zero_cap_still_floors_to_one(db):
    """Criterion 6 — a window of 0 dispatches nothing and then waits for a
    completion that cannot arrive. The floor predates caps and must outlive
    them, including for a task still listing (no batch rows, cap 0)."""
    from dlm.web.fleet import pool_window_allocation

    alloc = pool_window_allocation(
        [{"id": "t-listing", "priority": 5}, {"id": "t-work", "priority": 5}],
        7, {"t-listing": 0, "t-work": 699})

    assert alloc["t-listing"] == 1
    assert alloc["t-work"] == 7          # the listing task consumed no slots


def test_surplus_is_redistributed_iteratively_not_in_one_pass(db):
    """Criterion 7 — one clamp-and-stop pass leaks slots. Three tasks, P=9,
    equal weights: A caps at 1, so B and C must end up splitting 8, not the 3
    each a single pass would give them (which would leave 2 slots unplaced)."""
    from dlm.web.fleet import pool_window_allocation

    active = [{"id": "t-a", "priority": 5}, {"id": "t-b", "priority": 5},
              {"id": "t-c", "priority": 5}]
    alloc = pool_window_allocation(active, 9, {"t-a": 1, "t-b": 99, "t-c": 99})

    assert alloc == {"t-a": 1, "t-b": 4, "t-c": 4}
    assert sum(alloc.values()) == 9

    # ...and it keeps going through more than one clamp: A caps at 1, B at 2,
    # so C has to absorb the whole remainder rather than one round of it.
    chained = pool_window_allocation(active, 9,
                                     {"t-a": 1, "t-b": 2, "t-c": 99})
    assert chained == {"t-a": 1, "t-b": 2, "t-c": 6}
    assert sum(chained.values()) == 9


def test_capped_allocation_still_breaks_ties_first_come(db):
    """The #88 tie-break rule has to survive the extra rounds: equal weights,
    odd slot count, older task takes the spare — and it is the position in
    `active`, not the id, that decides."""
    from dlm.web.fleet import pool_window_allocation

    older = {"id": "t-older", "priority": 5}
    newer = {"id": "t-newer", "priority": 5}
    roomy = {"t-older": 99, "t-newer": 99}

    assert pool_window_allocation([older, newer], 7, roomy) == {
        "t-older": 4, "t-newer": 3}
    assert pool_window_allocation([newer, older], 7, roomy) == {
        "t-newer": 4, "t-older": 3}


def test_pool_window_endpoint_applies_the_cap_end_to_end(db):
    """The production path: two downloading pool tasks on the same source, one
    ramping with 14 batches queued behind it, 7 alive HK workers. This is the
    2026-08-09 fleet, and the endpoint has to report 1/6 with the caps it
    derived — not the 4/3 it served all day."""
    from dlm.web.routes.queue import pool_window_api

    _task(db, "t-ramp", status="downloading", priority=2)
    _task(db, "t-run", status="downloading", priority=5)
    for k in ("w1", "w2", "w3", "w4", "w5", "w6", "w7"):
        _worker(db, k)

    # ramping: one batch out, nothing reported yet
    db.upsert_shard({"id": "s-ramp-0", "task_id": "t-ramp", "shard_index": 0,
                     "status": "running", "server": "w1"})
    for i in range(1, 15):
        db.upsert_shard({"id": f"s-ramp-{i}", "task_id": "t-ramp",
                         "shard_index": i, "status": "pending"})
    # running: has reported, plenty pending
    db.upsert_shard({"id": "s-run-0", "task_id": "t-run", "shard_index": 0,
                     "status": "done", "server": "w4"})
    for i in range(1, 40):
        db.upsert_shard({"id": f"s-run-{i}", "task_id": "t-run",
                         "shard_index": i, "status": "pending"})

    out = _call(pool_window_api("t-ramp"))

    assert out["p"] == 7
    assert out["caps"] == {"t-ramp": 1, "t-run": 39}
    assert out["allocation"] == {"t-ramp": 1, "t-run": 6}
    assert out["window"] == 1
    # the other task's own query agrees — the allocation is fleet-wide, not
    # per-caller
    assert _call(pool_window_api("t-run"))["window"] == 6


def test_the_retry_round_is_not_serialized(db):
    """run() step 6 re-enters the window loop over the failed batches, and by
    then every row is terminal — record_batches_and_window writes the terminal
    row before it asks for the window. A running+pending ceiling would read 0
    here and floor the retry round to one batch at a time, up to
    POOL_BATCH_START_TO_CLOSE each, with the rest of the fleet idle. Counting
    failed rows keeps the retry pass as wide as the first pass was."""
    from dlm.web.fleet import pool_task_slot_cap
    from dlm.web.routes.queue import pool_window_api

    assert pool_task_slot_cap(_batches("done", "done", "failed", "failed")) == 2

    _task(db, "t-retry")
    db.upsert_shard({"id": "s-d", "task_id": "t-retry", "shard_index": 0,
                     "status": "done", "server": "w1"})
    for i in range(1, 7):
        db.upsert_shard({"id": f"s-f{i}", "task_id": "t-retry",
                         "shard_index": i, "status": "failed", "server": "w1"})
    for k in ("w1", "w2", "w3", "w4", "w5", "w6", "w7"):
        _worker(db, k)

    out = _call(pool_window_api("t-retry"))
    assert out["caps"]["t-retry"] == 6
    assert out["window"] == 6        # not 1: six failed batches retry in parallel


def test_slot_cap_when_every_batch_is_running():
    """Pins the `max(POOL_RAMP_SLOTS, running)` branch. Not a regression test —
    it passes against the pre-#89 body too. The branch is purely defensive:
    window > 1 requires a prior report and a report writes a terminal row, so
    several running batches with no terminal row should not occur. It is kept so
    that if it ever does, the cap is `running` and not POOL_RAMP_SLOTS."""
    from dlm.web.fleet import pool_task_slot_cap

    assert pool_task_slot_cap(_batches("running", "running", "running")) == 3
    # ...and once a batch reports, pending behind it becomes reachable
    assert pool_task_slot_cap(_batches("running", "running", "pending")) == 2


def test_a_negative_or_float_cap_cannot_over_issue_slots():
    """pool_window_allocation is a public primitive; a caller passing junk must
    not make `slots` exceed P or raise from the remainder slice."""
    from dlm.web.fleet import pool_window_allocation

    active = [{"id": "t-a", "priority": 5}, {"id": "t-b", "priority": 5}]
    assert sum(pool_window_allocation(
        active, 7, {"t-a": -3, "t-b": 1000}).values()) <= 7 + 1
    assert pool_window_allocation(active, 7, {"t-a": 1.5, "t-b": 1000}) \
        == {"t-a": 1, "t-b": 6}


def test_the_retry_round_window_does_not_ratchet_down():
    """The tempting tightening of pool_task_slot_cap is
    `(running + pending) or failed`, which reads the honest ceiling during
    round-1 wind-down. It must not be applied: _run_window_loop waits
    FIRST_COMPLETED (workflows.py), so it re-queries the window with the rest of
    the batch still in flight. Under that formula `running` is window-1 and the
    truthy `or` hides the failed queue, so the window ratchets 7,6,5,4,3,2,1 and
    only recovers once the last batch lands — a sustained ~42% loss across the
    retry pass (simulated: mean window 3.94 vs 6.73 over 100 retried batches).

    This walks the successive wakes of a retry round and asserts the cap stays
    wide the whole way, which is the property that formula would break.
    """
    from dlm.web.fleet import pool_task_slot_cap

    total, in_flight = 100, 6
    caps = []
    wakes = 20
    for completed in range(1, wakes):
        # what the shards table shows at each wake: some done, the window still
        # occupied, and everything not yet re-dispatched still sitting `failed`
        rows = (_batches(*(["done"] * completed))
                + _batches(*(["running"] * in_flight))
                + _batches(*(["failed"] * (total - completed - in_flight))))
        caps.append(pool_task_slot_cap(rows))

    # The durable assertion: the cap must never pin to the in-flight count, the
    # ratchet's signature — the window can only grow when cap > len(in_flight).
    assert all(c > in_flight for c in caps), f"cap pinned to in-flight: {caps}"
    assert caps == sorted(caps, reverse=True)   # decreases only as work is done
    # Derived from the loop bound, not a hand-picked constant, so widening
    # `wakes` cannot break a correct implementation.
    assert min(caps) == total - (wakes - 1)
