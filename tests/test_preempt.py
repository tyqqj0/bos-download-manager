"""Task P deliverable 1 — /api/queue/preempt works in both dispatch modes.

Before this task `preempt_for_task` was dead on arrival: `tasks.server` is
always NULL for BOTH dispatch modes (the coordinator assigns servers per
shard/batch, never at the task level — `do_claim` writes server=NULL on
purpose), so `server = target_server or (victim["server"] if victim else
"")` produced "" whenever target_server was omitted and the route returned
"Cannot determine target server" before ever mutating anything. The
auto-pick-a-victim path had never worked; target_server, when supplied, did
nothing but satisfy that dead gate — it never influenced which task got
preempted.

Machines a task occupies live in the shards table (shared by sharded shard
rows and pool batch rows — dispatch_mode is the only discriminator), not on
tasks.server. `snapshot.get_task_servers()` is the new read helper; this
file exercises the route logic built on it.

Chosen as a new file rather than adding to tests/test_pool_dispatch.py:
that file is already 700+ lines and organised around dispatch-mode routing/
admission/listing-guard concerns (T7). Preempt is a distinct route with its
own ~10 scenarios; a dedicated file keeps both readable. (test_pool_dispatch.py
already has two narrow preempt tests pinning the CLAIM_RESET_PHASE_SQL claim
behaviour — those are left in place and still pass unmodified against the
rewritten route.)

Run: python3 -m pytest tests/test_preempt.py -q
"""

from __future__ import annotations

import asyncio

import pytest


def _call(coro):
    return asyncio.run(coro) if asyncio.iscoroutine(coro) else coro


def _task(db, task_id, status="downloading", *, mode="sharded", priority=5,
          source="hf", created_at="2026-01-01T00:00:00+00:00", coordinator_phase=None):
    row = {"id": task_id, "name": task_id, "repo_id": "org/x",
           "status": status, "priority": priority, "source": source,
           "created_at": created_at}
    if mode is not None:
        row["dispatch_mode"] = mode
    db.upsert_task(row)
    if coordinator_phase is not None:
        conn = db._conn()
        conn.execute("UPDATE tasks SET coordinator_phase=? WHERE id=?",
                     (coordinator_phase, task_id))
        conn.commit()


def _shard(db, shard_id, task_id, idx, *, status="running", server=None):
    db.upsert_shard({"id": shard_id, "task_id": task_id, "shard_index": idx,
                      "status": status, "server": server})


def _stub_cancel(monkeypatch, calls, *, raises=None):
    import dlm.web.temporal_client as tc

    async def fake_cancel(task_id, dispatch_mode=None):
        calls.append((task_id, dispatch_mode))
        if raises is not None:
            raise raises

    monkeypatch.setattr(tc, "cancel_workflow", fake_cancel)


def _stub_start(monkeypatch, calls, *, raises=None):
    import dlm.web.temporal_client as tc

    async def fake_start(task):
        calls.append(task["id"])
        if raises is not None:
            raise raises

    monkeypatch.setattr(tc, "start_task_download", fake_start)


# ── get_task_servers itself ─────────────────────────────────────────────


def test_get_task_servers_reads_shards_table_not_tasks_server(db):
    """The regression this whole task is about: tasks.server is NULL by
    design, so the helper must answer from the shards table."""
    _task(db, "t1", status="downloading", mode="sharded")
    _shard(db, "s1", "t1", 0, status="running", server="w3")
    _shard(db, "s2", "t1", 1, status="running", server="w1")
    _shard(db, "s3", "t1", 2, status="pending", server=None)  # not running -> excluded

    assert db.get_task("t1")["server"] is None
    servers = db.get_task_servers("t1")
    assert servers == ["w1", "w3"]  # ORDER BY server — not incidental, the
    # route interpolates this list into its response message


# ── auto-pick, no target_server: the dead-gate regression test ─────────


def test_auto_pick_no_target_server_succeeds_for_sharded_task(db, monkeypatch):
    """Before the fix this always failed with "Cannot determine target
    server" — the entire auto-pick path was unreachable."""
    from dlm.web.routes.queue import preempt_for_task

    cancel_calls, start_calls = [], []
    _stub_cancel(monkeypatch, cancel_calls)
    _stub_start(monkeypatch, start_calls)

    _task(db, "t-urgent", status="pending", mode="sharded", priority=0)
    _task(db, "t-victim", status="downloading", mode="sharded", priority=9)
    _shard(db, "s-v0", "t-victim", 0, status="running", server="w2")

    out = _call(preempt_for_task({"urgent_task_id": "t-urgent"}))

    assert out.get("ok") is True, out
    assert out["victim_task_id"] == "t-victim"
    assert out["freed_servers"] == ["w2"]
    assert db.get_task("t-victim")["status"] == "preempted"
    assert db.get_task("t-urgent")["status"] == "downloading"
    assert cancel_calls == [("t-victim", "sharded")]
    assert start_calls == ["t-urgent"]


# ── target_server actually filters victim choice ────────────────────────


def test_auto_pick_target_server_prefers_lower_priority_on_that_server_over_globally_lowest(db, monkeypatch):
    """Pins rule 2: two candidates exist. bj7 holds the globally
    lowest-priority (most preemptable) task, but target_server=bj3 must
    still choose the (higher-priority-than-bj7, but present-on-bj3)
    candidate. A single-candidate test would pass against the old code too
    (which ignored target_server) — this one would not."""
    from dlm.web.routes.queue import preempt_for_task

    cancel_calls, start_calls = [], []
    _stub_cancel(monkeypatch, cancel_calls)
    _stub_start(monkeypatch, start_calls)

    _task(db, "t-urgent", status="pending", mode="sharded", priority=0)
    _task(db, "t-on-bj3", status="downloading", mode="sharded", priority=5)
    _shard(db, "s-bj3", "t-on-bj3", 0, status="running", server="bj3")
    _task(db, "t-on-bj7", status="downloading", mode="sharded", priority=9)  # globally lowest
    _shard(db, "s-bj7", "t-on-bj7", 0, status="running", server="bj7")

    out = _call(preempt_for_task({"urgent_task_id": "t-urgent", "target_server": "bj3"}))

    assert out.get("ok") is True, out
    assert out["victim_task_id"] == "t-on-bj3"
    assert db.get_task("t-on-bj3")["status"] == "preempted"
    assert db.get_task("t-on-bj7")["status"] == "downloading"  # untouched


# ── auto-pick skips a listing-phase (no machine) candidate ──────────────


def test_auto_pick_skips_candidate_with_no_running_shards(db, monkeypatch):
    from dlm.web.routes.queue import preempt_for_task

    cancel_calls, start_calls = [], []
    _stub_cancel(monkeypatch, cancel_calls)
    _stub_start(monkeypatch, start_calls)

    _task(db, "t-urgent", status="pending", mode="sharded", priority=0)
    # Globally lowest priority, but still listing — holds no machine.
    _task(db, "t-listing", status="downloading", mode="sharded", priority=9)
    _shard(db, "s-listing", "t-listing", 0, status="pending", server=None)
    _task(db, "t-holder", status="downloading", mode="sharded", priority=5)
    _shard(db, "s-holder", "t-holder", 0, status="running", server="w4")

    out = _call(preempt_for_task({"urgent_task_id": "t-urgent"}))

    assert out.get("ok") is True, out
    assert out["victim_task_id"] == "t-holder"
    assert db.get_task("t-listing")["status"] == "downloading"  # never touched


# ── none on target_server: specific error, no mutation ──────────────────


def test_auto_pick_none_on_target_server_returns_specific_error_and_mutates_nothing(db, monkeypatch):
    from dlm.web.routes.queue import preempt_for_task

    cancel_calls, start_calls = [], []
    _stub_cancel(monkeypatch, cancel_calls)
    _stub_start(monkeypatch, start_calls)

    _task(db, "t-urgent", status="pending", mode="sharded", priority=0)
    _task(db, "t-candidate", status="downloading", mode="sharded", priority=5)
    _shard(db, "s-c", "t-candidate", 0, status="running", server="w9")

    out = _call(preempt_for_task({"urgent_task_id": "t-urgent", "target_server": "bj99"}))

    assert "error" in out
    assert "bj99" in out["error"]
    assert db.get_task("t-candidate")["status"] == "downloading"
    assert db.get_task("t-urgent")["status"] == "pending"
    assert cancel_calls == []
    assert start_calls == []


def test_auto_pick_none_holding_a_machine_is_a_distinct_error_from_none_on_target_server(db, monkeypatch):
    """Rule 2 asks for THREE distinct messages: no downloading tasks at all,
    none on target_server, none holding a machine. This pins the third
    against the second: "no candidate holds any machine" (checked before
    target_server is even consulted, so it fires the same with or without
    one) must read differently from "candidates hold machines, just not
    this one" — an operator misreading one for the other would believe the
    fleet is idle when it is actually all still listing, or vice versa."""
    from dlm.web.routes.queue import preempt_for_task

    # Scenario 1: nothing holds any machine at all — target_server present
    # or not must not change which branch fires.
    _task(db, "t-urgent", status="pending", mode="sharded", priority=0)
    _task(db, "t-listing-only", status="downloading", mode="sharded", priority=5)
    _shard(db, "s-lo", "t-listing-only", 0, status="pending", server=None)

    out_none_holding = _call(preempt_for_task({"urgent_task_id": "t-urgent", "target_server": "bj1"}))
    assert "error" in out_none_holding
    assert "listing phase" in out_none_holding["error"]

    # Scenario 2: a candidate DOES hold a machine, just not target_server —
    # this must be a different message from scenario 1's.
    _shard(db, "s-lo2", "t-listing-only", 1, status="running", server="w2")
    out_none_on_target = _call(preempt_for_task({"urgent_task_id": "t-urgent", "target_server": "bj1"}))
    assert "error" in out_none_on_target
    assert "bj1" in out_none_on_target["error"]
    assert out_none_on_target["error"] != out_none_holding["error"]


# ── pool victim: three machines freed, batches released ─────────────────


def test_pool_victim_releases_batches_and_frees_all_three_machines(db, monkeypatch):
    from dlm.web.routes.queue import preempt_for_task

    cancel_calls, start_calls = [], []
    _stub_cancel(monkeypatch, cancel_calls)
    _stub_start(monkeypatch, start_calls)

    _task(db, "t-urgent", status="pending", mode="pool", priority=0)
    _task(db, "t-pool-victim", status="downloading", mode="pool", priority=5)
    _shard(db, "s-p0", "t-pool-victim", 0, status="running", server="bj1")
    _shard(db, "s-p1", "t-pool-victim", 1, status="running", server="bj2")
    _shard(db, "s-p2", "t-pool-victim", 2, status="running", server="bj3")
    _shard(db, "s-p3", "t-pool-victim", 3, status="done", server="bj4")     # untouched

    out = _call(preempt_for_task({"urgent_task_id": "t-urgent", "victim_task_id": "t-pool-victim"}))

    assert out.get("ok") is True, out
    assert out["freed_servers"] == ["bj1", "bj2", "bj3"]
    assert cancel_calls == [("t-pool-victim", "pool")]

    rows = {r["id"]: r for r in db.get_shards_by_task("t-pool-victim")}
    assert rows["s-p0"]["status"] == "pending" and rows["s-p0"]["server"] is None
    assert rows["s-p1"]["status"] == "pending" and rows["s-p1"]["server"] is None
    assert rows["s-p2"]["status"] == "pending" and rows["s-p2"]["server"] is None
    assert rows["s-p3"]["status"] == "done"  # release_pool_batches leaves done rows alone
    assert db.get_task("t-pool-victim")["status"] == "preempted"


# ── pool victim named explicitly, zero running batches: admission slot ──


def test_explicit_pool_victim_with_zero_running_batches_frees_admission_slot(db, monkeypatch):
    from dlm.web.routes.queue import preempt_for_task

    cancel_calls, start_calls = [], []
    _stub_cancel(monkeypatch, cancel_calls)
    _stub_start(monkeypatch, start_calls)

    _task(db, "t-urgent", status="pending", mode="pool", priority=0)
    _task(db, "t-pool-idle-victim", status="downloading", mode="pool", priority=5)
    # No shard/batch rows at all — nothing running.

    out = _call(preempt_for_task({
        "urgent_task_id": "t-urgent", "victim_task_id": "t-pool-idle-victim",
    }))

    assert out.get("ok") is True, out
    assert out["freed_servers"] == []
    assert "机器" not in out["message"] or "准入" in out["message"]
    assert "准入" in out["message"] or "admission" in out["message"].lower()
    assert cancel_calls == [("t-pool-idle-victim", "pool")]


# ── cancel_workflow raises: abort, victim restored, urgent never claimed ─


def test_cancel_failure_aborts_restores_victim_and_never_claims_urgent(db, monkeypatch):
    from dlm.web.routes.queue import preempt_for_task

    cancel_calls, start_calls = [], []
    _stub_cancel(monkeypatch, cancel_calls, raises=RuntimeError("temporal unreachable"))
    _stub_start(monkeypatch, start_calls)

    _task(db, "t-urgent", status="pending", mode="sharded", priority=3)
    _task(db, "t-victim", status="downloading", mode="sharded", priority=5)
    _shard(db, "s-v", "t-victim", 0, status="running", server="w5")

    out = _call(preempt_for_task({
        "urgent_task_id": "t-urgent", "victim_task_id": "t-victim",
    }))

    assert "error" in out
    assert "temporal unreachable" in out["error"] or "cancel" in out["error"].lower()
    assert db.get_task("t-victim")["status"] == "downloading"  # restored
    urgent_row = db.get_task("t-urgent")
    assert urgent_row["status"] == "pending"      # never claimed
    assert urgent_row["priority"] == 3             # never touched
    assert start_calls == []                       # never even reached start


# ── start_task_download raises: full rollback of the claim ───────────────


def test_start_failure_restores_status_priority_claimed_at_and_phase(db, monkeypatch):
    """Uses a P5 urgent task with coordinator_phase='dispatching' — a bug
    that restored to P0 (do_claim's write) or left coordinator_phase at
    'listing' (the claim's own reset) would be invisible with a P0/no-phase
    starting point, per the brief's own instruction."""
    from dlm.web.routes.queue import preempt_for_task

    cancel_calls, start_calls = [], []
    _stub_cancel(monkeypatch, cancel_calls)
    _stub_start(monkeypatch, start_calls, raises=RuntimeError("workflow start failed"))

    _task(db, "t-urgent", status="paused", mode="pool", priority=5,
          coordinator_phase="dispatching")
    conn = db._conn()
    conn.execute("UPDATE tasks SET claimed_at=? WHERE id=?", (1234.5, "t-urgent"))
    conn.commit()
    _task(db, "t-victim", status="downloading", mode="sharded", priority=6)
    _shard(db, "s-v2", "t-victim", 0, status="running", server="w6")

    out = _call(preempt_for_task({
        "urgent_task_id": "t-urgent", "victim_task_id": "t-victim",
    }))

    assert "error" in out
    assert out.get("victim_task_id") == "t-victim"
    assert "resume" in out.get("recovery", "").lower()
    assert "t-victim" in out.get("recovery", "")

    row = db.get_task("t-urgent")
    assert row["status"] == "paused"
    assert row["priority"] == 5
    assert row["claimed_at"] == 1234.5
    assert row["coordinator_phase"] == "dispatching"

    # The victim's cancel genuinely succeeded — it must stay preempted, not
    # be reverted to downloading with a dead workflow (rule 6).
    assert db.get_task("t-victim")["status"] == "preempted"


# ── falsifiability spot-checks (mutation testing per the brief) ─────────
#
# These pin specific lines against a plausible near-miss mutation, so a
# future edit that silently reintroduces one of the historical bugs fails
# loudly instead of merely "looking" green.


def test_target_server_does_not_leak_into_explicit_victim_path(db, monkeypatch):
    """If target_server were (wrongly) applied even when victim_task_id is
    given, this would error instead of succeeding — pins rule 2's "when no
    victim_task_id is given" scope."""
    from dlm.web.routes.queue import preempt_for_task

    cancel_calls, start_calls = [], []
    _stub_cancel(monkeypatch, cancel_calls)
    _stub_start(monkeypatch, start_calls)

    _task(db, "t-urgent", status="pending", mode="sharded", priority=0)
    _task(db, "t-victim", status="downloading", mode="sharded", priority=5)
    # Victim holds no machine at all, and target_server names an unrelated
    # server — an accidental filter would reject this outright.
    out = _call(preempt_for_task({
        "urgent_task_id": "t-urgent", "victim_task_id": "t-victim",
        "target_server": "totally-unrelated-server",
    }))
    assert out.get("ok") is True, out
