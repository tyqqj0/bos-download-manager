"""T9 — observability rework (see plan v3.1 §T9).

Tasks 1-8 built, dispatched, ran and stopped a pool task. Nothing in
dlm/web/ could yet answer "is this running pool task healthy" at anything
above one-batch-row granularity — this file exercises the six things that
make it observable (plan decisions A-G):

1. The pool patrol's three triggers -> one `pool_starved` alert type.
2. Orphan branch triage: a pool task is never re-dispatched by the 1800s
   staleness rule (replaced by the patrol); a sharded task still is (G1).
3. task_stuck's narrow exemption for a pool task admitted but holding no work.
4. Dashboard per-server batch aggregation for pool, unchanged shard_servers
   for sharded (G1 regression guard).
5. The busy signal (fleet.busy_servers / health_verifier.work_by_server) —
   pinned as already-correct for pool batch rows (decision D).
6. The staging GC's pure selection function.

Plus one item routed here from T7's review: the pool poller gate's describe
RPC needs a distinguishable signal for "RPC failed" vs "genuinely
under-polled".

No test opens an ssh connection or a Temporal connection — every RPC/ssh
call is stubbed, following tests/test_pool_dispatch.py and
tests/test_pool_stop_control.py's patterns.

Run: python3 -m pytest tests/test_pool_observability.py -q
"""

from __future__ import annotations

import asyncio
import logging
import time
from types import SimpleNamespace

import pytest


def _call(coro):
    return asyncio.run(coro) if asyncio.iscoroutine(coro) else coro


def _task(db, task_id, status="downloading", *, mode="pool", priority=5, source="hf",
          updated_at=None):
    row = {"id": task_id, "name": task_id, "repo_id": "org/x",
           "status": status, "priority": priority, "source": source}
    if mode is not None:
        row["dispatch_mode"] = mode
    db.upsert_task(row)
    if updated_at is not None:
        conn = db._conn()
        with conn:
            conn.execute("UPDATE tasks SET updated_at=? WHERE id=?", (updated_at, task_id))


def _worker(db, key, *, disk_free_gb=500):
    db.update_worker(hostname=key, server_key=key, disk_free_gb=disk_free_gb)


# ═══════════════════════════════════════════════════════════════════════
# 1. Pool patrol — three triggers, one pool_starved alert (decision A)
# ═══════════════════════════════════════════════════════════════════════


class _FakeWorkflowService:
    def __init__(self, poller_count=None, raises=None):
        self.poller_count = poller_count
        self.raises = raises
        self.requests = []

    async def describe_task_queue(self, req, timeout=None):
        self.requests.append(req)
        if self.raises:
            raise self.raises
        return SimpleNamespace(pollers=[object()] * self.poller_count)


class _FakeServiceClient:
    def __init__(self, poller_count=None, raises=None):
        self.workflow_service = _FakeWorkflowService(poller_count, raises)


class _FakeClient:
    def __init__(self, poller_count=None, raises=None):
        self.namespace = "default"
        self.service_client = _FakeServiceClient(poller_count, raises)


def _stub_connected_client(monkeypatch, client):
    import dlm.web.temporal_client as tc

    async def fake_connected(timeout=None):
        return client

    monkeypatch.setattr(tc, "connected_client", fake_connected)


def _stub_pending_activities(monkeypatch, rows_by_workflow_id):
    import dlm.web.temporal_client as tc

    async def fake_pending(workflow_id):
        return rows_by_workflow_id.get(workflow_id, [])

    monkeypatch.setattr(tc, "pending_activities", fake_pending)


def test_no_pollers_triggers_critical_pool_starved(db, monkeypatch):
    from dlm.web import reconciler

    _task(db, "t-nopoll", status="downloading", mode="pool", source="hf")
    db.upsert_shard({"id": "s-nopoll-0", "task_id": "t-nopoll", "shard_index": 0,
                      "status": "running", "server": "w1"})

    _stub_connected_client(monkeypatch, _FakeClient(poller_count=0))
    _stub_pending_activities(monkeypatch, {})

    alerts = asyncio.run(reconciler.inspect_pool_tasks([db.get_task("t-nopoll")]))

    assert len(alerts) == 1
    a = alerts[0]
    assert a["severity"] == "critical"
    assert a["type"] == "pool_starved"
    assert a["trigger"] == "no_pollers"
    assert a["task_id"] == "t-nopoll"
    assert a["pollers"] == 0


def test_scheduled_stuck_triggers_warning_pool_starved(db, monkeypatch):
    from dlm.web import reconciler
    from dlm.web.fleet import POOL_STARVED_SCHEDULED_S

    _task(db, "t-sched", status="downloading", mode="pool", source="hf")
    db.upsert_shard({"id": "s-sched-0", "task_id": "t-sched", "shard_index": 0,
                      "status": "running", "server": "w1"})

    _stub_connected_client(monkeypatch, _FakeClient(poller_count=3))  # healthy pollers
    old_scheduled = time.time() - POOL_STARVED_SCHEDULED_S - 1
    _stub_pending_activities(monkeypatch, {
        "pool-t-sched": [{"activity_type": "run_pool_batch", "state": "SCHEDULED",
                           "attempt": 1, "scheduled_at": old_scheduled, "last_started_at": None}],
    })

    alerts = asyncio.run(reconciler.inspect_pool_tasks([db.get_task("t-sched")]))

    assert len(alerts) == 1
    a = alerts[0]
    assert a["severity"] == "warning"
    assert a["trigger"] == "scheduled_stuck"
    assert a["scheduled_age_s"] >= POOL_STARVED_SCHEDULED_S


def test_attempt_climbing_triggers_warning_pool_starved(db, monkeypatch):
    from dlm.web import reconciler
    from dlm.web.fleet import POOL_STARVED_ATTEMPT

    _task(db, "t-attempt", status="downloading", mode="pool", source="hf")
    db.upsert_shard({"id": "s-attempt-0", "task_id": "t-attempt", "shard_index": 0,
                      "status": "running", "server": "w1"})

    _stub_connected_client(monkeypatch, _FakeClient(poller_count=3))
    _stub_pending_activities(monkeypatch, {
        "pool-t-attempt": [{"activity_type": "run_pool_batch", "state": "STARTED",
                             "attempt": POOL_STARVED_ATTEMPT, "scheduled_at": None,
                             "last_started_at": time.time()}],
    })

    alerts = asyncio.run(reconciler.inspect_pool_tasks([db.get_task("t-attempt")]))

    assert len(alerts) == 1
    a = alerts[0]
    assert a["severity"] == "warning"
    assert a["trigger"] == "attempt_climbing"
    assert a["attempt"] == POOL_STARVED_ATTEMPT


def test_healthy_pool_task_produces_no_alert(db, monkeypatch):
    from dlm.web import reconciler

    _task(db, "t-healthy", status="downloading", mode="pool", source="hf")
    db.upsert_shard({"id": "s-healthy-0", "task_id": "t-healthy", "shard_index": 0,
                      "status": "running", "server": "w1"})

    _stub_connected_client(monkeypatch, _FakeClient(poller_count=3))
    _stub_pending_activities(monkeypatch, {
        "pool-t-healthy": [{"activity_type": "run_pool_batch", "state": "STARTED",
                             "attempt": 1, "scheduled_at": None, "last_started_at": time.time()}],
    })

    alerts = asyncio.run(reconciler.inspect_pool_tasks([db.get_task("t-healthy")]))
    assert alerts == []


def test_pending_activities_normalises_the_raw_proto(monkeypatch):
    """Decision B, against the real proto types (not a hand-rolled stub) —
    catches a wrong attribute name that stubbing pending_activities
    everywhere else in this file would hide."""
    from temporalio.api.enums.v1 import PendingActivityState
    from temporalio.api.workflow.v1 import PendingActivityInfo
    from google.protobuf.timestamp_pb2 import Timestamp

    import dlm.web.temporal_client as tc

    scheduled_ts = Timestamp()
    scheduled_ts.FromSeconds(1700000000)
    pai = PendingActivityInfo(
        activity_type={"name": "run_pool_batch"},
        state=PendingActivityState.PENDING_ACTIVITY_STATE_SCHEDULED,
        attempt=2,
    )
    pai.scheduled_time.CopyFrom(scheduled_ts)
    # last_started_time deliberately left unset.

    class FakeHandle:
        async def describe(self, rpc_timeout=None):
            return SimpleNamespace(
                raw_description=SimpleNamespace(pending_activities=[pai])
            )

    class FakeClient:
        def get_workflow_handle(self, wf_id):
            return FakeHandle()

    async def fake_connected(timeout=None):
        return FakeClient()

    monkeypatch.setattr(tc, "connected_client", fake_connected)

    rows = asyncio.run(tc.pending_activities("pool-t1"))

    assert rows == [{
        "activity_type": "run_pool_batch",
        "state": "SCHEDULED",
        "attempt": 2,
        "scheduled_at": 1700000000.0,
        "last_started_at": None,
    }]


def test_pending_activities_returns_empty_list_on_rpc_failure(monkeypatch):
    """An inspection pass must never be able to stop the scheduler loop —
    WorkflowNotFound/RPCError returns [], not a raise."""
    import dlm.web.temporal_client as tc

    async def fake_connected(timeout=None):
        raise RuntimeError("workflow not found")

    monkeypatch.setattr(tc, "connected_client", fake_connected)

    rows = asyncio.run(tc.pending_activities("pool-does-not-exist"))
    assert rows == []


def test_non_pool_tasks_are_ignored_by_the_patrol(db, monkeypatch):
    """A sharded task must never reach the poller/pending-activity RPCs at
    all — this is the G1 corollary for the patrol."""
    from dlm.web import reconciler
    import dlm.web.temporal_client as tc

    calls = []

    async def fail_connected(timeout=None):
        calls.append("connected_client")
        raise AssertionError("sharded task should never trigger a poller query")

    monkeypatch.setattr(tc, "connected_client", fail_connected)

    _task(db, "t-sharded-ignored", status="downloading", mode="sharded", source="hf")
    alerts = asyncio.run(reconciler.inspect_pool_tasks([db.get_task("t-sharded-ignored")]))

    assert alerts == []
    assert calls == []


def test_task_with_only_terminal_batches_skips_the_poller_query(db, monkeypatch):
    """Trigger 1 only fires for a source with >=1 non-terminal batch row —
    a pool task whose batches are all done/failed shouldn't hold its
    source's poller RPC hostage (it's about to auto-complete/auto-fail)."""
    from dlm.web import reconciler
    import dlm.web.temporal_client as tc

    calls = []

    async def fail_connected(timeout=None):
        calls.append("connected_client")
        raise AssertionError("should not be reached — no non-terminal batch rows")

    monkeypatch.setattr(tc, "connected_client", fail_connected)
    monkeypatch.setattr(tc, "pending_activities", lambda wf_id: asyncio.sleep(0, result=[]))

    _task(db, "t-all-done", status="downloading", mode="pool", source="hf")
    db.upsert_shard({"id": "s-all-done-0", "task_id": "t-all-done", "shard_index": 0,
                      "status": "done", "server": "w1"})

    alerts = asyncio.run(reconciler.inspect_pool_tasks([db.get_task("t-all-done")]))

    assert alerts == []
    assert calls == []  # trigger 1's poller RPC never ran


def test_reconcile_wires_pool_starved_into_its_report(db, monkeypatch):
    """reconcile() calls the patrol itself and carries the result on
    report["pool_starved"] — proves the wiring, not just the standalone
    function."""
    from dlm.web import reconciler
    import dlm.web.temporal_client as tc

    async def fake_running(client=None):
        return {}

    async def fake_start(task):
        return None

    monkeypatch.setattr(tc, "running_workflows", fake_running)
    monkeypatch.setattr(tc, "start_task_download", fake_start)
    _stub_connected_client(monkeypatch, _FakeClient(poller_count=0))
    _stub_pending_activities(monkeypatch, {})

    _task(db, "t-wired", status="downloading", mode="pool", source="hf")
    db.upsert_shard({"id": "s-wired-0", "task_id": "t-wired", "shard_index": 0,
                      "status": "running", "server": "w1"})

    report = asyncio.run(reconciler.reconcile())

    assert any(a["task_id"] == "t-wired" and a["trigger"] == "no_pollers"
               for a in report["pool_starved"])


def test_alerts_check_alerts_surfaces_cached_pool_starved(monkeypatch):
    """check_alerts can't make the Temporal RPCs itself (synchronous, no
    event loop) — it must read what reconcile()'s last pass cached, the
    same pattern health_verify_report already uses."""
    from dlm.web import alerts as alerts_mod
    from dlm.web.cache import cache

    cache.set("reconciler_report", {
        "pool_starved": [{
            "severity": "critical", "type": "pool_starved", "task_id": "t-cached",
            "task_name": "t-cached", "source": "hf", "trigger": "no_pollers",
            "pollers": 0, "message": "Pool task t-cached (hf): pool-hf has 0 activity pollers",
        }],
    })
    try:
        out = alerts_mod.check_alerts(tasks=[], workers=[])
    finally:
        cache.set("reconciler_report", None)

    assert any(a["type"] == "pool_starved" and a["task_id"] == "t-cached" for a in out)


# ═══════════════════════════════════════════════════════════════════════
# 2. Orphan triage — pool skips the 1800s redispatch (decision C, G1 guard)
# ═══════════════════════════════════════════════════════════════════════


def test_stale_orphaned_pool_task_is_not_redispatched(db, monkeypatch):
    from dlm.web import reconciler
    from dlm.web.fleet import DEAD_THRESHOLD
    import dlm.web.temporal_client as tc

    calls = []

    async def fake_running(client=None):
        return {}

    async def fake_start(task):
        calls.append(task["id"])

    monkeypatch.setattr(tc, "running_workflows", fake_running)
    monkeypatch.setattr(tc, "start_task_download", fake_start)
    _stub_connected_client(monkeypatch, _FakeClient(poller_count=3))
    _stub_pending_activities(monkeypatch, {})

    _task(db, "t-pool-orphan", status="downloading", mode="pool", source="hf",
          updated_at=time.time() - DEAD_THRESHOLD - 100)
    db.upsert_shard({"id": "s-po-0", "task_id": "t-pool-orphan", "shard_index": 0,
                      "status": "running", "server": "w1"})

    report = asyncio.run(reconciler.reconcile())

    assert "t-pool-orphan" not in report["redispatched"]
    assert calls == []
    assert any(o["task_id"] == "t-pool-orphan" for o in report.get("pool_orphaned", []))
    assert any(o["task_id"] == "t-pool-orphan" for o in report["orphaned"])


def test_stale_orphaned_sharded_task_is_still_redispatched_g1_regression(db, monkeypatch):
    """The same shape, as sharded: must still redispatch exactly as before
    T9 — this is the G1 guard, and it must fail if the pool skip is ever
    widened to cover sharded too."""
    from dlm.web import reconciler
    from dlm.web.fleet import DEAD_THRESHOLD
    import dlm.web.temporal_client as tc

    calls = []

    async def fake_running(client=None):
        return {}

    async def fake_start(task):
        calls.append(task["id"])

    monkeypatch.setattr(tc, "running_workflows", fake_running)
    monkeypatch.setattr(tc, "start_task_download", fake_start)

    _task(db, "t-sharded-orphan", status="downloading", mode="sharded", source="hf",
          updated_at=time.time() - DEAD_THRESHOLD - 100)

    report = asyncio.run(reconciler.reconcile())

    assert "t-sharded-orphan" in report["redispatched"]
    assert "t-sharded-orphan" in calls
    assert "pool_orphaned" not in report or not any(
        o["task_id"] == "t-sharded-orphan" for o in report["pool_orphaned"]
    )


def test_pool_task_with_all_done_batches_still_auto_completes(db, monkeypatch):
    from dlm.web import reconciler
    import dlm.web.temporal_client as tc

    async def fake_running(client=None):
        return {}

    monkeypatch.setattr(tc, "running_workflows", fake_running)
    _stub_connected_client(monkeypatch, _FakeClient(poller_count=3))
    _stub_pending_activities(monkeypatch, {})

    _task(db, "t-pool-done", status="downloading", mode="pool", source="hf")
    db.upsert_shard({"id": "s-pd-0", "task_id": "t-pool-done", "shard_index": 0,
                      "status": "done", "server": "w1"})
    db.upsert_shard({"id": "s-pd-1", "task_id": "t-pool-done", "shard_index": 1,
                      "status": "done", "server": "w2"})

    report = asyncio.run(reconciler.reconcile())

    assert "t-pool-done" in report.get("auto_completed", [])
    assert db.get_task("t-pool-done")["status"] == "done"


def test_pool_task_with_a_failed_batch_still_auto_fails(db, monkeypatch):
    from dlm.web import reconciler
    import dlm.web.temporal_client as tc

    async def fake_running(client=None):
        return {}

    monkeypatch.setattr(tc, "running_workflows", fake_running)
    _stub_connected_client(monkeypatch, _FakeClient(poller_count=3))
    _stub_pending_activities(monkeypatch, {})

    _task(db, "t-pool-failed", status="downloading", mode="pool", source="hf")
    db.upsert_shard({"id": "s-pf-0", "task_id": "t-pool-failed", "shard_index": 0,
                      "status": "done", "server": "w1"})
    db.upsert_shard({"id": "s-pf-1", "task_id": "t-pool-failed", "shard_index": 1,
                      "status": "failed", "server": "w2"})

    report = asyncio.run(reconciler.reconcile())

    assert "t-pool-failed" in report.get("auto_failed", [])
    assert db.get_task("t-pool-failed")["status"] == "failed"


# ═══════════════════════════════════════════════════════════════════════
# 3. task_stuck exemption (decision E)
# ═══════════════════════════════════════════════════════════════════════


def _stale_downloading(db, task_id, mode):
    _task(db, task_id, status="downloading", mode=mode,
          updated_at=time.time() - 3700)


def test_pool_task_waiting_behind_the_window_is_exempt(db):
    from dlm.web.alerts import check_alerts

    _stale_downloading(db, "t-waiting", "pool")
    db.upsert_shard({"id": "s-w-0", "task_id": "t-waiting", "shard_index": 0,
                      "status": "pending"})

    alerts = check_alerts(tasks=[db.get_task("t-waiting")], workers=[])
    assert not any(a["type"] == "task_stuck" for a in alerts)


def test_pool_task_with_a_running_batch_still_fires_task_stuck(db):
    from dlm.web.alerts import check_alerts

    _stale_downloading(db, "t-running-batch", "pool")
    db.upsert_shard({"id": "s-rb-0", "task_id": "t-running-batch", "shard_index": 0,
                      "status": "running", "server": "w1"})

    alerts = check_alerts(tasks=[db.get_task("t-running-batch")], workers=[])
    assert any(a["type"] == "task_stuck" for a in alerts)


def test_pool_task_with_no_batch_rows_at_all_still_fires_task_stuck(db):
    from dlm.web.alerts import check_alerts

    _stale_downloading(db, "t-no-rows", "pool")
    # Deliberately no shard rows — a coordinator that never registered any.

    alerts = check_alerts(tasks=[db.get_task("t-no-rows")], workers=[])
    assert any(a["type"] == "task_stuck" for a in alerts)


def test_sharded_task_stuck_is_unaffected_by_the_exemption_g1_regression(db):
    """A stale sharded task alerts regardless of its shard rows — fails if
    the exemption is ever widened past dispatch_mode=='pool'.

    Shaped exactly like decision E's pool exemption (zero running, one
    pending shard row) on purpose: if the dispatch_mode=='pool' gate were
    ever dropped from the exemption check, THIS is the shape that would
    wrongly go quiet for a sharded task. A running shard row (a real stall,
    which pool doesn't exempt either) wouldn't tell the two conditions
    apart."""
    from dlm.web.alerts import check_alerts

    _stale_downloading(db, "t-sharded-stuck", "sharded")
    db.upsert_shard({"id": "s-ss-0", "task_id": "t-sharded-stuck", "shard_index": 0,
                      "status": "pending"})

    alerts = check_alerts(tasks=[db.get_task("t-sharded-stuck")], workers=[])
    assert any(a["type"] == "task_stuck" for a in alerts)


# ═══════════════════════════════════════════════════════════════════════
# 4. Dashboard aggregation (decision F, backend only — no build step for JS)
# ═══════════════════════════════════════════════════════════════════════


def test_pool_task_gets_server_batches_and_deduped_server(db):
    from dlm.web.scheduler import _build_dashboard

    _task(db, "t-dash-pool", status="downloading", mode="pool")
    db.upsert_shard({"id": "s-d-0", "task_id": "t-dash-pool", "shard_index": 0,
                      "status": "running", "server": "w1", "speed_mbps": 10,
                      "done_bytes": 100, "total_bytes": 1000})
    db.upsert_shard({"id": "s-d-1", "task_id": "t-dash-pool", "shard_index": 1,
                      "status": "done", "server": "w1",
                      "done_bytes": 500, "total_bytes": 500})
    db.upsert_shard({"id": "s-d-2", "task_id": "t-dash-pool", "shard_index": 2,
                      "status": "running", "server": "w2", "speed_mbps": 5,
                      "done_bytes": 50, "total_bytes": 200})

    summary = _build_dashboard()
    dl = next(d for d in summary["active_downloads"] if d["id"] == "t-dash-pool")

    assert "shard_servers" not in dl
    servers = {sb["server"]: sb for sb in dl["server_batches"]}
    assert set(servers) == {"w1", "w2"}
    assert servers["w1"]["running"] == 1
    assert servers["w1"]["done"] == 1
    assert servers["w2"]["running"] == 1
    assert servers["w2"]["done"] == 0
    assert dl["server"] == "w1,w2"  # deduped, sorted


def test_sharded_task_shard_servers_unchanged_g1_regression(db):
    """One entry per shard row, exactly as before T9 — must fail if the
    sharded branch is ever touched."""
    from dlm.web.scheduler import _build_dashboard

    _task(db, "t-dash-sharded", status="downloading", mode="sharded")
    db.upsert_shard({"id": "s-ds-0", "task_id": "t-dash-sharded", "shard_index": 0,
                      "status": "running", "server": "w1", "speed_mbps": 10,
                      "done_bytes": 100, "total_bytes": 1000})
    db.upsert_shard({"id": "s-ds-1", "task_id": "t-dash-sharded", "shard_index": 1,
                      "status": "running", "server": "w1", "speed_mbps": 3,
                      "done_bytes": 30, "total_bytes": 200})

    summary = _build_dashboard()
    dl = next(d for d in summary["active_downloads"] if d["id"] == "t-dash-sharded")

    assert "server_batches" not in dl
    assert len(dl["shard_servers"]) == 2  # one entry PER ROW, not per server
    assert dl["server"] == "w1,w1"  # non-deduplicated, exactly like today


# ═══════════════════════════════════════════════════════════════════════
# 5. Busy signal (decision D — verify, don't manufacture a change)
# ═══════════════════════════════════════════════════════════════════════


def test_running_pool_batch_marks_its_server_busy(db):
    from dlm.queue.snapshot import get_running_shards
    from dlm.web.fleet import busy_servers

    _task(db, "t-busy-pool", status="downloading", mode="pool")
    db.upsert_shard({"id": "s-bp-0", "task_id": "t-busy-pool", "shard_index": 0,
                      "status": "running", "server": "w1"})

    busy = busy_servers([db.get_task("t-busy-pool")], get_running_shards())
    assert "w1" in busy


def test_running_pool_batch_appears_in_work_by_server(db):
    from dlm.web.health_verifier import work_by_server

    _task(db, "t-work-pool", status="downloading", mode="pool")
    db.upsert_shard({"id": "s-wp-0", "task_id": "t-work-pool", "shard_index": 0,
                      "status": "running", "server": "w1", "updated_at": time.time()})

    held = work_by_server([db.get_task("t-work-pool")], db.get_running_shards())
    assert held["w1"]["name"] == "t-work-pool"


def test_pool_task_row_server_null_contributes_nothing_via_task_level_scan(db):
    """The task-level scan in work_by_server is for legacy single-node tasks
    only — a pool task's own row carries server=NULL (its servers live on
    the batch rows), so on its own it must not appear via that path."""
    from dlm.web.health_verifier import work_by_server

    _task(db, "t-null-server-pool", status="downloading", mode="pool")
    # No shard rows at all — only the task-level scan could pick this up,
    # and it must not: the task row's own "server" column is NULL.
    held = work_by_server([db.get_task("t-null-server-pool")], [])
    assert held == {}


# ═══════════════════════════════════════════════════════════════════════
# 4b. list_shards additive dispatch_mode field (T7-authorised scope
#     deviation #2) — the popup's only way to know which table to render.
# ═══════════════════════════════════════════════════════════════════════


def test_list_shards_reports_dispatch_mode_pool(db):
    from dlm.web.routes.queue import list_shards

    _task(db, "t-mode-pool", status="downloading", mode="pool")
    out = _call(list_shards("t-mode-pool"))
    assert out["dispatch_mode"] == "pool"


def test_list_shards_reports_dispatch_mode_sharded(db):
    from dlm.web.routes.queue import list_shards

    _task(db, "t-mode-sharded", status="downloading", mode="sharded")
    out = _call(list_shards("t-mode-sharded"))
    assert out["dispatch_mode"] == "sharded"


# ═══════════════════════════════════════════════════════════════════════
# 6. Staging GC selection logic (decision G) — pure function, no ssh
# ═══════════════════════════════════════════════════════════════════════


def test_gc_removes_a_done_tasks_dir():
    from dlm.web.reconciler import select_staging_gc

    tasks = [{"name": "repo-a", "status": "done"}]
    out = select_staging_gc({"w1": ["repo-a"]}, tasks)

    assert out["remove"] == [{"server": "w1", "name": "repo-a", "status": ["done"]}]
    assert out["keep"] == []
    assert out["unknown"] == []
    assert out["skipped"] == []


def test_gc_leaves_a_downloading_tasks_dir():
    from dlm.web.reconciler import select_staging_gc

    tasks = [{"name": "repo-b", "status": "downloading"}]
    out = select_staging_gc({"w1": ["repo-b"]}, tasks)

    assert out["remove"] == []
    assert len(out["keep"]) == 1
    assert out["keep"][0]["name"] == "repo-b"


def test_gc_reports_an_unknown_dir_without_removing_it():
    from dlm.web.reconciler import select_staging_gc

    out = select_staging_gc({"w1": ["mystery-dir"]}, tasks=[])

    assert out["remove"] == []
    assert out["unknown"] == [{"server": "w1", "name": "mystery-dir"}]


def test_gc_keeps_a_name_shared_by_a_terminal_and_a_non_terminal_task():
    """Names are not unique — task `name` + `category` determine the BOS
    prefix, so two rows can share a name. Any non-terminal row blocks it."""
    from dlm.web.reconciler import select_staging_gc

    tasks = [
        {"name": "shared-name", "status": "done"},
        {"name": "shared-name", "status": "downloading"},
    ]
    out = select_staging_gc({"w1": ["shared-name"]}, tasks)

    assert out["remove"] == []
    assert len(out["keep"]) == 1
    assert out["keep"][0]["name"] == "shared-name"


def test_gc_skips_a_name_with_shell_metacharacters():
    from dlm.web.reconciler import select_staging_gc

    tasks = [{"name": "repo; rm -rf /", "status": "done"}]
    out = select_staging_gc({"w1": ["repo; rm -rf /"]}, tasks)

    assert out["remove"] == []
    assert out["skipped"] == [
        {"server": "w1", "name": "repo; rm -rf /", "reason": "metacharacters"},
    ]


def test_gc_considers_every_server_independently():
    from dlm.web.reconciler import select_staging_gc

    tasks = [{"name": "repo-c", "status": "failed"}]
    out = select_staging_gc({"w1": ["repo-c"], "w2": ["repo-c"]}, tasks)

    assert sorted(r["server"] for r in out["remove"]) == ["w1", "w2"]


def test_doctor_staging_gc_preview_never_removes(db, monkeypatch):
    """The dry-run endpoint calls staging_gc(dry_run=True) — verified here
    by monkeypatching reconciler.staging_gc and asserting the flag it's
    called with, so the route can't accidentally drop the flag."""
    from dlm.web.routes import doctor

    calls = []

    def fake_staging_gc(dry_run=False):
        calls.append(dry_run)
        return {"dry_run": dry_run, "candidates": [], "removed": [],
                "kept": [], "unknown": [], "skipped": [], "errors": []}

    monkeypatch.setattr("dlm.web.reconciler.staging_gc", fake_staging_gc)

    out = asyncio.run(doctor.staging_gc_preview())

    assert calls == [True]
    assert out["removed"] == []


# ═══════════════════════════════════════════════════════════════════════
# 7. Pool poller gate — distinguishable signal for RPC failure vs
#    genuine under-polling (item routed here from T7's review)
# ═══════════════════════════════════════════════════════════════════════


def test_pool_gate_rpc_failure_is_distinguishable_from_under_polling(db, monkeypatch, caplog):
    import dlm.web.temporal_client as tc

    _worker(db, "w1")

    fake_client = _FakeClient(raises=RuntimeError("deadline exceeded"))

    async def fake_connected(timeout=None):
        return fake_client
    monkeypatch.setattr(tc, "connected_client", fake_connected)

    task = {"id": "t-gate-rpcfail", "source": "hf", "name": "x", "repo_id": "org/x"}

    with caplog.at_level(logging.CRITICAL, logger="dlm.web"):
        with pytest.raises(tc.PoolPollerGateError):
            asyncio.run(tc.start_pool_download(task))

    messages = [r.message for r in caplog.records]
    assert any("describe_task_queue RPC failed" in m for m in messages), messages
    # Distinguishable from the "rejected task ... has N poller(s)" wording
    # the under-polling branch uses (test_pool_dispatch.py's existing
    # test_pool_gate_rejects_and_alerts_when_pollers_below_alive_workers).
    assert not any("pool gate rejected" in m for m in messages)


def test_pool_gate_still_fails_closed_on_rpc_error_no_sharded_fallback(db, monkeypatch):
    """The refusal must not weaken: an RPC failure still raises (never
    proceeds to start_workflow), and there is no fallback to sharded."""
    import dlm.web.temporal_client as tc

    _worker(db, "w1")
    fake_client = _FakeClient(raises=RuntimeError("network blip"))

    async def fake_connected(timeout=None):
        return fake_client
    monkeypatch.setattr(tc, "connected_client", fake_connected)

    task = {"id": "t-gate-rpcfail2", "source": "hf", "name": "x", "repo_id": "org/x"}

    with pytest.raises(tc.PoolPollerGateError):
        asyncio.run(tc.start_pool_download(task))

    assert fake_client.service_client.workflow_service.requests  # the RPC was attempted

