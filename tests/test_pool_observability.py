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


@pytest.fixture(autouse=True)
def _never_touch_the_production_alert_log(monkeypatch, tmp_path):
    """Two independent guards, because one monkeypatch is "unlikely", not
    "impossible" (see the section-0 note below for what this cost):

    1. the module's cached logger is replaced with a NullHandler-only one, so
       _get_alert_logger() short-circuits and never builds a FileHandler;
    2. ALERT_LOG_PATH points at this test's tmp_path, so anything that DOES
       rebuild the logger — including a test that deliberately resets the
       cache — lands in the test's own directory.

    _active_alerts is reset too: it is module-global de-dupe state, and a leak
    across tests turns one test's alerts into another's RESOLVED lines.
    """
    import logging as _logging
    from dlm.web import alerts as alerts_mod

    monkeypatch.setattr(alerts_mod, "ALERT_LOG_PATH", tmp_path / "dlm-alerts.log")

    null = _logging.getLogger("dlm.alerts.pytest-null")
    null.handlers = [_logging.NullHandler()]
    null.propagate = False
    monkeypatch.setattr(alerts_mod, "_alert_logger", null)
    monkeypatch.setattr(alerts_mod, "_active_alerts", {})


@pytest.fixture(autouse=True)
def _isolate_patrol_state():
    """Trigger 1's zero-poller confirmation streak is module state that spans
    patrol cycles by design (review finding I8). Reset it around every test so
    no test can be made to pass — or fail — by another test's samples."""
    from dlm.web import reconciler

    reconciler._POOL_ZERO_POLLER_SAMPLES.clear()
    yield
    reconciler._POOL_ZERO_POLLER_SAMPLES.clear()


# ═══════════════════════════════════════════════════════════════════════
# 0. Test-harness safety: this file must never write the real alert log
# ═══════════════════════════════════════════════════════════════════════
#
# check_alerts unconditionally calls _get_alert_logger(), which opens
# logging.FileHandler(alerts.ALERT_LOG_PATH) in append mode and swallows only
# OSError/PermissionError. This file is the suite's only caller. On a dev box
# /data is absent so nothing happens — but on S1 /data exists and
# scripts/deploy-workers.sh runs `pytest tests/ -q` as its deploy gate, so
# every deploy appended fabricated CRITICALs ("pool-hf has 0 activity
# pollers") and RESOLVED churn to the live incident log a human greps.


def test_no_test_in_this_file_can_touch_the_production_alert_log(db, monkeypatch, tmp_path):
    """Pins the autouse guard below, and does it in a way that fails on a dev
    box too: without the guard, /data is unwritable here and the real
    FileHandler's OSError is swallowed, so merely inspecting the logger's
    handlers would find nothing wrong and pass. Spy on the FileHandler
    construction instead — that happens regardless of whether /data exists."""
    import logging as _logging
    from dlm.web.alerts import check_alerts
    from dlm.web import alerts as alerts_mod

    opened: list[str] = []

    class _SpyHandler(_logging.NullHandler):
        def __init__(self, filename, *args, **kwargs):
            super().__init__()
            opened.append(str(filename))

    monkeypatch.setattr(_logging, "FileHandler", _SpyHandler)
    monkeypatch.setattr(alerts_mod, "_alert_logger", None)  # force a rebuild

    _stale_downloading(db, "t-alertlog", "sharded")
    out = check_alerts(tasks=[db.get_task("t-alertlog")], workers=[])

    assert any(a["type"] == "task_stuck" for a in out)  # the logging path really ran
    assert opened, "expected the alert logger to open a file at all"
    assert all(not p.startswith("/data") for p in opened), opened
    assert str(tmp_path) in opened[0], opened


def test_the_alert_logger_opens_no_file_at_all_in_this_suite(db, monkeypatch):
    """Guard 1, pinned separately: with the cached NullHandler logger in
    place, check_alerts opens no file at all — not even in tmp_path. The test
    above deliberately clears that cache to exercise guard 2, so without this
    one the first guard could be deleted with the suite still green."""
    import logging as _logging
    from dlm.web.alerts import check_alerts

    opened: list[str] = []

    class _SpyHandler(_logging.NullHandler):
        def __init__(self, filename, *args, **kwargs):
            super().__init__()
            opened.append(str(filename))

    monkeypatch.setattr(_logging, "FileHandler", _SpyHandler)

    _stale_downloading(db, "t-alertlog2", "sharded")
    out = check_alerts(tasks=[db.get_task("t-alertlog2")], workers=[])

    assert any(a["type"] == "task_stuck" for a in out)
    assert opened == []


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


def test_no_pollers_triggers_critical_pool_starved_only_after_confirmation(db, monkeypatch):
    """Temporal's poller list is a recency view — a frontend restart, a
    matching-service failover or a fleet that just reconnected can report
    zero while every worker is healthy. One zero sample must stay silent;
    the second consecutive one is the CRITICAL (review finding I8)."""
    from dlm.web import reconciler
    from dlm.web.fleet import POOL_STARVED_ZERO_SAMPLES

    assert POOL_STARVED_ZERO_SAMPLES == 2  # one confirmation cycle (300s)

    _task(db, "t-nopoll", status="downloading", mode="pool", source="hf")
    db.upsert_shard({"id": "s-nopoll-0", "task_id": "t-nopoll", "shard_index": 0,
                      "status": "running", "server": "w1"})

    _stub_connected_client(monkeypatch, _FakeClient(poller_count=0))
    _stub_pending_activities(monkeypatch, {})

    first = asyncio.run(reconciler.inspect_pool_tasks([db.get_task("t-nopoll")]))
    assert first == []  # one sample is not evidence of fleet death

    alerts = asyncio.run(reconciler.inspect_pool_tasks([db.get_task("t-nopoll")]))

    assert len(alerts) == 1
    a = alerts[0]
    assert a["severity"] == "critical"
    assert a["type"] == "pool_starved"
    assert a["trigger"] == "no_pollers"
    assert a["task_id"] == "t-nopoll"
    assert a["pollers"] == 0
    assert a["zero_samples"] == 2


def test_a_healthy_poller_sample_clears_the_zero_streak(db, monkeypatch):
    """The exact false positive I8 describes: one blip, pollers back, later
    another blip. Neither may alert."""
    from dlm.web import reconciler

    _task(db, "t-blip", status="downloading", mode="pool", source="hf")
    db.upsert_shard({"id": "s-blip-0", "task_id": "t-blip", "shard_index": 0,
                      "status": "running", "server": "w1"})
    _stub_pending_activities(monkeypatch, {})

    def _pass(pollers):
        _stub_connected_client(monkeypatch, _FakeClient(poller_count=pollers))
        return asyncio.run(reconciler.inspect_pool_tasks([db.get_task("t-blip")]))

    assert _pass(0) == []
    assert _pass(4) == []
    assert _pass(0) == []  # first zero of a NEW streak, not the second of the old


def test_a_failed_poller_rpc_neither_alerts_nor_clears_the_streak(db, monkeypatch):
    """Decision B keeps an RPC failure silent. It is also not a healthy
    sample, so it must not discard a pending confirmation — otherwise a
    flapping frontend could suppress the alert indefinitely."""
    from dlm.web import reconciler

    _task(db, "t-rpcfail", status="downloading", mode="pool", source="hf")
    db.upsert_shard({"id": "s-rf-0", "task_id": "t-rpcfail", "shard_index": 0,
                      "status": "running", "server": "w1"})
    _stub_pending_activities(monkeypatch, {})
    _stub_connected_client(monkeypatch, _FakeClient(poller_count=0))

    assert asyncio.run(reconciler.inspect_pool_tasks([db.get_task("t-rpcfail")])) == []

    import dlm.web.temporal_client as tc
    real = tc._pool_poller_count

    async def boom(client, queue_name):
        raise RuntimeError("frontend unavailable")

    monkeypatch.setattr(tc, "_pool_poller_count", boom)
    assert asyncio.run(reconciler.inspect_pool_tasks([db.get_task("t-rpcfail")])) == []

    monkeypatch.setattr(tc, "_pool_poller_count", real)
    alerts = asyncio.run(reconciler.inspect_pool_tasks([db.get_task("t-rpcfail")]))
    assert [a["trigger"] for a in alerts] == ["no_pollers"]


def test_a_stale_zero_sample_does_not_count_as_consecutive(db, monkeypatch):
    """"Consecutive" means consecutive patrol cycles. A zero recorded hours
    ago (pool idle in between, then work resumed on a fleet still
    reconnecting) must not let a single fresh zero alert immediately."""
    from dlm.web import reconciler
    from dlm.web.fleet import POOL_STARVED_SAMPLE_GAP_S

    _task(db, "t-stale-sample", status="downloading", mode="pool", source="hf")
    db.upsert_shard({"id": "s-ss2-0", "task_id": "t-stale-sample", "shard_index": 0,
                      "status": "running", "server": "w1"})
    _stub_pending_activities(monkeypatch, {})
    _stub_connected_client(monkeypatch, _FakeClient(poller_count=0))

    assert asyncio.run(reconciler.inspect_pool_tasks([db.get_task("t-stale-sample")])) == []

    # Age the recorded sample past the gap window.
    for queue_name, (count, at) in list(reconciler._POOL_ZERO_POLLER_SAMPLES.items()):
        reconciler._POOL_ZERO_POLLER_SAMPLES[queue_name] = (
            count, at - POOL_STARVED_SAMPLE_GAP_S - 1)

    assert asyncio.run(reconciler.inspect_pool_tasks([db.get_task("t-stale-sample")])) == []


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

    # Two passes: trigger 1 needs a confirming sample (review finding I8), so
    # a single reconcile() legitimately reports nothing here.
    assert asyncio.run(reconciler.reconcile())["pool_starved"] == []
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
    # claimed_at is refreshed so the listing-phase source guard covers the new
    # coordinator. The only assertion worth keeping from the test I6 deleted
    # (tests/test_pool_dispatch.py's retargeted phase test) — after the SQL
    # fragment came out of that UPDATE this is all it still writes.
    assert db.get_task("t-sharded-orphan")["claimed_at"] > time.time() - 60
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
# 2b. The recovery story for a pool orphan (review findings C2 + I7):
#     an alert tells the human, an explicitly-named action lets them act,
#     and the default fix button can no longer do it by accident.
# ═══════════════════════════════════════════════════════════════════════


def test_alerts_raise_critical_pool_orphaned_naming_the_explicit_action():
    """Decision C removed the automatic re-dispatch for pool orphans and
    assumed pool_starved would carry them to a human. It structurally
    cannot: pollers are alive (trigger 1 sees pollers > 0), and describe on
    a workflow that no longer exists returns no pending activities at all
    (triggers 2/3 never run), while task_stuck is exempted by decision E.
    Without its own alert the state is detected and reported nowhere."""
    from dlm.web.alerts import CRITICAL, check_alerts
    from dlm.web.cache import cache

    cache.set("reconciler_report", {
        "pool_orphaned": [
            {"task_id": "t-orphan-pool", "name": "Egocentric-100K",
             "stale_seconds": 4200},
        ],
    })
    try:
        out = check_alerts(tasks=[], workers=[])
    finally:
        cache.set("reconciler_report", None)

    hits = [a for a in out if a["type"] == "pool_orphaned"]
    assert len(hits) == 1, out
    assert hits[0]["severity"] == CRITICAL
    assert hits[0]["task_id"] == "t-orphan-pool"
    # The operator action after I7 is the explicitly-named pool re-dispatch,
    # NOT the default fix button (which now refuses pool tasks).
    assert "redispatch_pool" in hits[0]["message"]


def _doctor_stub(monkeypatch, started):
    import dlm.web.temporal_client as tc

    async def fake_running(client=None):
        return {}

    async def fake_start(task):
        started.append(task["id"])

    monkeypatch.setattr(tc, "running_workflows", fake_running)
    monkeypatch.setattr(tc, "start_task_download", fake_start)


def test_doctor_default_fix_refuses_to_redispatch_a_pool_orphan(db, monkeypatch):
    """`redispatch_orphaned` is the DEFAULT action and what the UI's fix
    button posts. Re-dispatching a pool task there re-runs list -> filter ->
    chunk and hits T1's non-retryable no-delete-and-error on a chunking
    mismatch — the exact wedge decision C forbids."""
    from dlm.web.routes import doctor

    started: list = []
    _doctor_stub(monkeypatch, started)

    _task(db, "t-doc-pool", status="downloading", mode="pool")
    _task(db, "t-doc-sharded", status="downloading", mode="sharded")

    out = asyncio.run(doctor.fix(doctor.FixRequest()))

    assert started == ["t-doc-sharded"]  # sharded orphan still healed (G1)
    assert "t-doc-sharded" in out["redispatch_orphaned"]
    skipped = out["skipped_pool_tasks"]
    assert len(skipped) == 1 and "t-doc-pool" in skipped[0]
    assert "redispatch_pool" in skipped[0]  # names the way to do it on purpose


def test_doctor_redispatches_a_pool_orphan_only_on_the_explicit_action(db, monkeypatch):
    from dlm.web.routes import doctor

    started: list = []
    _doctor_stub(monkeypatch, started)

    _task(db, "t-doc-pool2", status="downloading", mode="pool")

    out = asyncio.run(doctor.fix(doctor.FixRequest(actions=["redispatch_pool"])))

    assert started == ["t-doc-pool2"]
    assert "t-doc-pool2" in out["redispatch_pool"]
    assert not out.get("unsupported_actions")


def test_doctor_reset_stuck_skips_pool_tasks(db, monkeypatch):
    """`reset_stuck` reaches the same wedge indirectly: status='pending'
    hands the task straight to auto_dispatch_pending, which starts a fresh
    pool coordinator."""
    from dlm.web.fleet import DEAD_THRESHOLD
    from dlm.web.routes import doctor

    started: list = []
    _doctor_stub(monkeypatch, started)

    stale = time.time() - DEAD_THRESHOLD - 100
    _task(db, "t-stuck-pool", status="downloading", mode="pool", updated_at=stale)
    _task(db, "t-stuck-sharded", status="downloading", mode="sharded", updated_at=stale)

    out = asyncio.run(doctor.fix(doctor.FixRequest(actions=["reset_stuck"])))

    assert out["reset_stuck"] == ["t-stuck-sharded"]
    assert db.get_task("t-stuck-pool")["status"] == "downloading"
    assert db.get_task("t-stuck-sharded")["status"] == "pending"
    assert any("t-stuck-pool" in s and "redispatch_pool" in s
               for s in out["skipped_pool_tasks"])


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


# ── 3b. The same exemption on /api/doctor's surface (review finding I3) ──
#
# A10 grades doctor/alerts at ZERO false positives, and `stuck` feeds
# total_issues -> healthy. A pool task admitted but waiting behind the window
# writes nothing to its task row by design, so it crossed STALE_THRESHOLD
# (600s) and drove /api/doctor unhealthy for as long as it waited.


def _stub_doctor_temporal(monkeypatch, running: dict):
    import dlm.web.temporal_client as tc

    async def fake_running(client=None):
        return running

    monkeypatch.setattr(tc, "running_workflows", fake_running)


def test_doctor_exempts_a_window_queued_pool_task_from_stuck(db, monkeypatch):
    from dlm.web.fleet import STALE_THRESHOLD
    from dlm.web.routes import doctor

    _stub_doctor_temporal(monkeypatch, {"pool-t-doc-wait": "pool-hf"})

    _task(db, "t-doc-wait", status="downloading", mode="pool",
          updated_at=time.time() - STALE_THRESHOLD - 60)
    db.upsert_shard({"id": "s-dw-0", "task_id": "t-doc-wait", "shard_index": 0,
                      "status": "pending"})

    out = asyncio.run(doctor.diagnose())

    assert out["stuck_tasks"] == []
    assert out["total_issues"] == 0
    assert out["healthy"] is True


def test_the_exemption_predicate_gates_on_dispatch_mode():
    """Unit-level, because neither surface can pin this: alerts short-circuits
    on mode before calling, and the doctor only collects batch rows for pool
    tasks — so a sharded task reaches the predicate with no rows either way.
    The gate inside the predicate is what keeps it safe to call from a future
    third caller that doesn't pre-filter."""
    from dlm.web.fleet import pool_task_holds_no_work

    queued = [{"status": "pending"}]
    assert pool_task_holds_no_work({"dispatch_mode": "pool"}, queued) is True
    assert pool_task_holds_no_work({"dispatch_mode": "sharded"}, queued) is False
    assert pool_task_holds_no_work({}, queued) is False  # default is sharded


def test_doctor_still_reports_a_pool_task_with_a_running_batch_as_stuck(db, monkeypatch):
    """Running AND pending rows together — the shape a live pool task
    actually has, and the one that tells "waiting behind the window" apart
    from "downloading but not progressing". Only the absence of running work
    may exempt a task."""
    from dlm.web.fleet import STALE_THRESHOLD
    from dlm.web.routes import doctor

    _stub_doctor_temporal(monkeypatch, {"pool-t-doc-stall": "pool-hf"})

    _task(db, "t-doc-stall", status="downloading", mode="pool",
          updated_at=time.time() - STALE_THRESHOLD - 60)
    db.upsert_shard({"id": "s-dst-0", "task_id": "t-doc-stall", "shard_index": 0,
                      "status": "running", "server": "w1"})
    db.upsert_shard({"id": "s-dst-1", "task_id": "t-doc-stall", "shard_index": 1,
                      "status": "pending"})

    out = asyncio.run(doctor.diagnose())

    assert [s["task_id"] for s in out["stuck_tasks"]] == ["t-doc-stall"]
    assert out["healthy"] is False


def test_doctor_still_reports_a_stale_sharded_task_as_stuck_g1_regression(db, monkeypatch):
    """Same row shape as the pool exemption (zero running, one pending) —
    fails if the doctor's exemption is ever widened past pool."""
    from dlm.web.fleet import STALE_THRESHOLD
    from dlm.web.routes import doctor

    _stub_doctor_temporal(monkeypatch, {"sharded-t-doc-sh": "download-workers"})

    _task(db, "t-doc-sh", status="downloading", mode="sharded",
          updated_at=time.time() - STALE_THRESHOLD - 60)
    db.upsert_shard({"id": "s-dsh-0", "task_id": "t-doc-sh", "shard_index": 0,
                      "status": "pending"})

    out = asyncio.run(doctor.diagnose())

    assert [s["task_id"] for s in out["stuck_tasks"]] == ["t-doc-sh"]


def test_alerts_and_doctor_read_one_exemption_predicate(db, monkeypatch):
    """The exemption is one function both surfaces call. If either inlines
    its own copy, /api/doctor and the alert log can disagree about the same
    task — so patching the shared predicate must silence BOTH.

    The task here holds a *running* batch row, i.e. it is normally NOT
    exempt on either surface; only the shared predicate says otherwise."""
    from dlm.web import fleet
    from dlm.web.alerts import check_alerts
    from dlm.web.fleet import STALE_THRESHOLD
    from dlm.web.routes import doctor

    _stub_doctor_temporal(monkeypatch, {"pool-t-shared-pred": "pool-hf"})
    monkeypatch.setattr(fleet, "pool_task_holds_no_work", lambda task, rows: True)

    _task(db, "t-shared-pred", status="downloading", mode="pool",
          updated_at=time.time() - max(STALE_THRESHOLD, 3600) - 60)
    db.upsert_shard({"id": "s-sp-0", "task_id": "t-shared-pred", "shard_index": 0,
                      "status": "running", "server": "w1"})

    alerts = check_alerts(tasks=[db.get_task("t-shared-pred")], workers=[])
    out = asyncio.run(doctor.diagnose())

    assert not any(a["type"] == "task_stuck" for a in alerts)
    assert out["stuck_tasks"] == []


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


@pytest.mark.parametrize("status", ["paused", "preempted"])
def test_gc_never_removes_a_resumable_tasks_dir(status):
    """`paused`/`preempted` are stopped but *resumable* (pipeline.py logs
    "staging preserved for resume" when it cancels), and CLAUDE.md's hard
    constraint is "staging cleanup only for done/skipped/failed tasks".
    Gating on absence from TERMINAL_STATUSES conflated the two sets and made
    a paused Egocentric-100K's partial files plus its md5-guarded
    .progress.json markers a GC candidate on seven workers."""
    from dlm.web.reconciler import select_staging_gc

    out = select_staging_gc({"w1": ["repo-resumable"]},
                            [{"name": "repo-resumable", "status": status}])

    assert out["remove"] == []
    assert len(out["keep"]) == 1
    assert out["keep"][0]["name"] == "repo-resumable"
    assert status in out["keep"][0]["reason"]


def test_gc_keeps_a_name_shared_by_a_done_and_a_paused_task():
    """Rule 3 over the *removable* set, not the terminal one: a resumable
    row sharing the name blocks removal just like a downloading one."""
    from dlm.web.reconciler import select_staging_gc

    out = select_staging_gc({"w1": ["shared-resumable"]}, [
        {"name": "shared-resumable", "status": "done"},
        {"name": "shared-resumable", "status": "paused"},
    ])

    assert out["remove"] == []
    assert len(out["keep"]) == 1


def test_gc_removable_statuses_is_not_the_terminal_status_set():
    """"this task is stopped" and "this task's staging may be deleted" are
    different questions; conflating them is the C1 data-loss defect."""
    from dlm.web.fleet import GC_REMOVABLE_STATUSES, TERMINAL_STATUSES

    assert set(GC_REMOVABLE_STATUSES) == {"done", "failed", "revoked", "skipped"}
    assert set(GC_REMOVABLE_STATUSES) < set(TERMINAL_STATUSES)
    assert "paused" not in GC_REMOVABLE_STATUSES
    assert "preempted" not in GC_REMOVABLE_STATUSES


class _StopSchedulerLoop(BaseException):
    """Deliberately not an Exception — background_scheduler's own
    `except Exception` would swallow it and keep looping."""


class _NoSleepAsyncio:
    """`asyncio`, with sleep() collapsed to a yield.

    Only the scheduler's interval bookkeeping is under test; its real
    cadence (2s startup + 10s per pass) would make the test a 12s one.
    """

    def __getattr__(self, name):
        return getattr(asyncio, name)


def test_staging_gc_does_not_sweep_on_a_fresh_web_start(db, monkeypatch):
    """`last_staging_gc = 0` made the sweep fire on the FIRST loop pass,
    ~2s after `systemctl restart dlm-web` — before any human could read
    /api/doctor/staging-gc's dry-run preview, which exists for exactly that
    purpose. The first sweep must be deferred by one full interval."""
    from dlm.web import health_verifier, reconciler, scheduler

    gc_calls: list = []
    passes: list = []

    async def fake_async(*a, **kw):
        return {}

    monkeypatch.setattr(scheduler, "_build_dashboard", lambda: {})
    monkeypatch.setattr(scheduler, "_poll_transfers", lambda: 0)
    monkeypatch.setattr(reconciler, "zero_stale_speeds", lambda: None)
    monkeypatch.setattr(reconciler, "auto_dispatch_pending", fake_async)
    monkeypatch.setattr(reconciler, "reconcile", fake_async)
    monkeypatch.setattr(reconciler, "detect_idle_workers", fake_async)
    monkeypatch.setattr(health_verifier, "verify_all_workers", fake_async)
    monkeypatch.setattr(
        reconciler, "staging_gc",
        lambda dry_run=False: gc_calls.append(dry_run) or {"removed": [], "errors": []},
    )

    fake_asyncio = _NoSleepAsyncio()

    async def fake_sleep(delay, *a, **kw):
        if delay == scheduler.DASHBOARD_INTERVAL:  # bottom of the while True
            passes.append(1)
            if len(passes) >= 2:
                raise _StopSchedulerLoop()
        await asyncio.sleep(0)

    fake_asyncio.sleep = fake_sleep
    monkeypatch.setattr(scheduler, "asyncio", fake_asyncio)

    with pytest.raises(_StopSchedulerLoop):
        asyncio.run(scheduler.background_scheduler())

    assert len(passes) == 2  # the loop really did run twice
    assert gc_calls == []  # ... and swept on neither pass


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

