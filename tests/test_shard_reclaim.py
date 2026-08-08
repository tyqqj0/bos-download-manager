"""A `running` shard row is how a host is marked taken. Nothing wrote it back
when the child workflow died without reporting.

`busy_servers` reads shard ownership as the primary signal — a sharded task's
own row carries `server = NULL` — so a row stuck at `running` makes its host
read busy forever. `query_idle_workers` then finds nothing, `auto_dispatch`
dispatches nothing, and the dashboard shows sixteen busy workers all doing
nothing. The row survives an OOM kill, a host reboot, a worker restarted
mid-shard, and a terminate that raced the shard's own status report.

`reconcile()`'s task loop cannot see this: it iterates `downloading` tasks, so
rows belonging to a task that already went done/failed/revoked are never
visited — and that is the common case, since terminating a task's workflows is
exactly what leaves its children unable to report.

Run: pytest tests/test_shard_reclaim.py -q
"""

from __future__ import annotations

import asyncio
import time

import pytest

from dlm.web import reconciler
from dlm.web.fleet import busy_servers
from dlm.web.reconciler import SHARD_ORPHAN_GRACE, reclaim_orphaned_shards

OLD = time.time() - SHARD_ORPHAN_GRACE - 60
FRESH = time.time()
# Past fleet.DEAD_THRESHOLD (1800s), so reconcile() re-dispatches rather
# than only recording the task as orphaned.
DEAD = time.time() - 3600


def _shard(snapshot, shard_id, server, updated_at, status="running",
           task_id="t-1", index=0):
    # A shard row never exists without its task row, and since get_running_shards
    # JOINs tasks (so a stopped task's rows stop marking hosts busy), a parentless
    # row is invisible to busy_servers — which is not the state under test here.
    # Created only if absent: several tests below write their own parent row with
    # a specific status first.
    if snapshot.get_task(task_id) is None:
        snapshot.upsert_task({
            "id": task_id, "name": task_id, "repo_id": f"org/{task_id}",
            "status": "downloading", "source": "modelscope", "priority": 0,
        })
    snapshot.upsert_shard({
        "id": shard_id, "task_id": task_id, "shard_index": index,
        "server": server, "status": status, "updated_at": updated_at,
        "total_files": 100, "done_files": 10,
    })


def _age_task(snapshot, task_id, when):
    """upsert_task stamps updated_at = now, so staleness has to be written
    afterwards — reconcile()'s re-dispatch gate reads it."""
    conn = snapshot._conn()
    conn.execute("UPDATE tasks SET updated_at = ?, claimed_at = ? WHERE id = ?",
                 (when, when, task_id))
    conn.commit()


def test_a_quiet_shard_with_no_child_workflow_is_failed(dlm_db):
    _shard(dlm_db, "s-t-1-0", "bj3", OLD)

    reclaimed = reclaim_orphaned_shards(running_ids=set())

    assert [r["shard_id"] for r in reclaimed] == ["s-t-1-0"]
    assert reclaimed[0]["server"] == "bj3"
    row = dlm_db.get_shard("s-t-1-0")
    assert row["status"] == "failed"
    assert "orphaned" in row["error"]
    assert row["speed_mbps"] == 0


def test_reclaiming_frees_the_host(dlm_db):
    """The whole point: the host has to come back as idle afterwards."""
    _shard(dlm_db, "s-t-1-0", "bj3", OLD)
    assert busy_servers([], dlm_db.get_running_shards()) == {"bj3"}

    reclaim_orphaned_shards(running_ids=set())

    assert busy_servers([], dlm_db.get_running_shards()) == set()


def test_a_shard_whose_workflow_is_running_is_left_alone(dlm_db):
    """Temporal is the authority on whether work is alive. A long single-file
    download can be quiet for far longer than the grace period."""
    _shard(dlm_db, "s-t-1-0", "bj3", OLD)

    reclaimed = reclaim_orphaned_shards(running_ids={"shard-s-t-1-0"})

    assert reclaimed == []
    assert dlm_db.get_shard("s-t-1-0")["status"] == "running"


def test_a_recently_reporting_shard_is_left_alone(dlm_db):
    """Covers the window between create_shards_in_db and the child appearing
    in a list scan — the row exists before Temporal will admit the execution."""
    _shard(dlm_db, "s-t-1-0", "bj3", FRESH)

    assert reclaim_orphaned_shards(running_ids=set()) == []
    assert dlm_db.get_shard("s-t-1-0")["status"] == "running"


def test_only_running_rows_are_touched(dlm_db):
    """A done shard's counters are the task's record of what it moved."""
    _shard(dlm_db, "s-t-1-0", "bj3", OLD, status="done")
    _shard(dlm_db, "s-t-1-1", "bj4", OLD, status="pending", index=1)

    assert reclaim_orphaned_shards(running_ids=set()) == []
    assert dlm_db.get_shard("s-t-1-0")["status"] == "done"
    assert dlm_db.get_shard("s-t-1-1")["status"] == "pending"


def test_one_live_shard_does_not_shield_its_dead_siblings(dlm_db):
    _shard(dlm_db, "s-t-1-0", "bj3", OLD, index=0)
    _shard(dlm_db, "s-t-1-1", "bj4", OLD, index=1)
    _shard(dlm_db, "s-t-1-2", "bj5", OLD, index=2)

    reclaimed = reclaim_orphaned_shards(running_ids={"shard-s-t-1-1"})

    assert sorted(r["shard_id"] for r in reclaimed) == ["s-t-1-0", "s-t-1-2"]
    assert busy_servers([], dlm_db.get_running_shards()) == {"bj4"}


# --- reconcile() wiring -----------------------------------------------------
#
# The reclaim has to run on a cycle where nothing is downloading. That is not a
# corner case: total starvation — every host counted busy by a stale row, so
# nothing can be dispatched — has no downloading tasks by definition, and the
# old code returned before it ever queried Temporal.

def _reconcile(monkeypatch, running=(), fail_query=False):
    async def fake_running_workflows(client=None):
        if fail_query:
            raise RuntimeError("temporal unreachable")
        return {wid: "download-bj3" for wid in running}

    monkeypatch.setattr("dlm.web.temporal_client.running_workflows",
                        fake_running_workflows)
    return asyncio.run(reconciler.reconcile())


def test_reconcile_reclaims_when_no_task_is_downloading(dlm_db, monkeypatch):
    _shard(dlm_db, "s-t-gone-0", "bj3", OLD, task_id="t-gone")
    dlm_db.upsert_task({"id": "t-gone", "name": "gone", "repo_id": "org/x",
                        "status": "revoked", "source": "modelscope",
                        "priority": 0})

    report = _reconcile(monkeypatch)

    assert [r["shard_id"] for r in report["reclaimed_shards"]] == ["s-t-gone-0"]
    assert busy_servers([], dlm_db.get_running_shards()) == set()


def test_reconcile_reclaims_nothing_when_temporal_cannot_be_reached(dlm_db, monkeypatch):
    """An empty id set from a failed query reads as "nothing is running" and
    would fail every live shard in the fleet at once."""
    _shard(dlm_db, "s-t-1-0", "bj3", OLD)

    report = _reconcile(monkeypatch, fail_query=True)

    assert report["reclaimed_shards"] == []
    assert report["errors"]
    assert dlm_db.get_shard("s-t-1-0")["status"] == "running"


def test_a_reclaimed_shard_does_not_bury_its_task_as_failed(dlm_db, monkeypatch):
    """The task loop marks a task failed once every shard is terminal. A shard
    this cycle's reclaim just failed says nothing about the download — the work
    is resumable and the BOS filter makes a restart lossless — so the task must
    go down the re-dispatch path instead."""
    dlm_db.upsert_task({"id": "t-orph", "name": "orph", "repo_id": "org/x",
                        "status": "downloading", "source": "modelscope",
                        "priority": 0})
    _age_task(dlm_db, "t-orph", DEAD)
    _shard(dlm_db, "s-t-orph-0", "bj3", OLD, task_id="t-orph", index=0)
    _shard(dlm_db, "s-t-orph-1", "bj4", OLD, task_id="t-orph", index=1,
           status="done")

    dispatched = []

    async def fake_start(task, task_queue=None):
        dispatched.append((task["id"], task_queue))

    monkeypatch.setattr("dlm.web.temporal_client.start_sharded_download",
                        fake_start)
    report = _reconcile(monkeypatch)

    assert [r["shard_id"] for r in report["reclaimed_shards"]] == ["s-t-orph-0"]
    assert dlm_db.get_task("t-orph")["status"] == "downloading", \
        "buried a resumable task as failed because its worker restarted"
    assert report.get("auto_failed") is None
    assert dispatched == [("t-orph", "download-ms-workers")]


def test_a_genuinely_failed_shard_still_fails_its_task(dlm_db, monkeypatch):
    """The guard above must not disable the auto-fail it sits in front of."""
    dlm_db.upsert_task({"id": "t-bad", "name": "bad", "repo_id": "org/x",
                        "status": "downloading", "source": "modelscope",
                        "priority": 0})
    _age_task(dlm_db, "t-bad", DEAD)
    _shard(dlm_db, "s-t-bad-0", "bj3", OLD, task_id="t-bad", status="failed")

    report = _reconcile(monkeypatch)

    assert report["reclaimed_shards"] == []
    assert dlm_db.get_task("t-bad")["status"] == "failed"
    assert report["auto_failed"] == ["bad"]


def test_one_reclaimed_shard_does_not_shield_a_genuine_failure(dlm_db, monkeypatch):
    """The mixed case, and the reason the guard tests every failed shard rather
    than `any`. One orphaned shard alongside a real download failure used to
    send the whole task down the re-dispatch path: it hit the same error, failed
    the same way, was reclaimed again, and re-dispatched again — the genuine
    failure never surfaced. A real failure recurs, so burying the task is the
    honest outcome; it stays retryable by hand and the BOS filter means nothing
    already uploaded is lost."""
    dlm_db.upsert_task({"id": "t-mix", "name": "mix", "repo_id": "org/x",
                        "status": "downloading", "source": "modelscope",
                        "priority": 0})
    _age_task(dlm_db, "t-mix", DEAD)
    # index 0 is quiet with no workflow -> this cycle reclaims it.
    _shard(dlm_db, "s-t-mix-0", "bj3", OLD, task_id="t-mix", index=0)
    # index 1 already failed on its own -> a real error, not an orphan.
    _shard(dlm_db, "s-t-mix-1", "bj4", OLD, task_id="t-mix", index=1,
           status="failed")

    dispatched = []

    async def fake_start(task, task_queue=None):
        dispatched.append(task["id"])

    monkeypatch.setattr("dlm.web.temporal_client.start_sharded_download",
                        fake_start)
    report = _reconcile(monkeypatch)

    assert [r["shard_id"] for r in report["reclaimed_shards"]] == ["s-t-mix-0"]
    assert dlm_db.get_task("t-mix")["status"] == "failed"
    assert report["auto_failed"] == ["mix"]
    assert dispatched == [], "re-dispatched a task with a real download failure"


def test_re_dispatch_is_visible_to_the_churn_guards(dlm_db, monkeypatch):
    """`retry_count` is what every churn detector reads — alerts.py's
    repeated-failure alert and doctor's zombie check at retry_count >= 8. A
    re-dispatch IS a retry, and leaving the counter untouched let this path
    restart a task indefinitely with all of them reading zero."""
    dlm_db.upsert_task({"id": "t-churn", "name": "churn", "repo_id": "org/x",
                        "status": "downloading", "source": "modelscope",
                        "priority": 0})
    _age_task(dlm_db, "t-churn", DEAD)
    _shard(dlm_db, "s-t-churn-0", "bj3", OLD, task_id="t-churn")

    async def fake_start(task, task_queue=None):
        pass

    monkeypatch.setattr("dlm.web.temporal_client.start_sharded_download",
                        fake_start)

    _reconcile(monkeypatch)
    assert dlm_db.get_task("t-churn")["retry_count"] == 1


# --- pool rows are not the control plane's to judge --------------------------
#
# Pool batch rows and sharded shard rows share the `shards` table, and until
# these tests the reconciler read them identically. Both paths below are
# pre-existing pool bugs, not regressions from the missing-file work: a pool
# task could be declared done or failed by the reconciler, bypassing the
# coordinator's own finalization entirely.

def _pool_task(snapshot, task_id, name=None, source="hf"):
    snapshot.upsert_task({
        "id": task_id, "name": name or task_id, "repo_id": f"org/{task_id}",
        "status": "downloading", "source": source, "priority": 0,
        "dispatch_mode": "pool",
    })


def test_a_backing_off_pool_batch_is_not_reclaimed(dlm_db):
    """POOL_BATCH_RETRY backs off to a 30-minute maximum interval, twice
    SHARD_ORPHAN_GRACE, and a pool batch runs as an activity rather than a
    `shard-{id}` child workflow — so both reclaim conditions are satisfied by a
    batch that is retrying exactly as designed."""
    _pool_task(dlm_db, "t-pool-backoff")
    _shard(dlm_db, "b-t-pool-backoff-0", "w1", OLD, task_id="t-pool-backoff")

    reclaimed = reclaim_orphaned_shards(running_ids=set())

    assert reclaimed == []
    assert dlm_db.get_shard("b-t-pool-backoff-0")["status"] == "running"


def test_a_sharded_shard_is_still_reclaimed_when_pool_rows_are_present(dlm_db):
    """The skip must key off the row's own parent, not disable the pass."""
    _pool_task(dlm_db, "t-pool-mixed")
    _shard(dlm_db, "b-t-pool-mixed-0", "w1", OLD, task_id="t-pool-mixed")
    _shard(dlm_db, "s-t-sharded-0", "bj3", OLD, task_id="t-sharded")

    reclaimed = reclaim_orphaned_shards(running_ids=set())

    assert [r["shard_id"] for r in reclaimed] == ["s-t-sharded-0"]


def test_an_orphan_row_whose_task_is_gone_is_still_reclaimed(dlm_db):
    """The LEFT JOIN gives task_dispatch_mode=None for a parentless row. Those
    rows are the stranded case this pass exists for, so `or "sharded"` must let
    them through rather than reading None as pool."""
    _shard(dlm_db, "s-t-orphaned-0", "bj3", OLD, task_id="t-vanished")
    conn = dlm_db._conn()
    conn.execute("DELETE FROM tasks WHERE id = ?", ("t-vanished",))
    conn.commit()

    reclaimed = reclaim_orphaned_shards(running_ids=set())

    assert [r["shard_id"] for r in reclaimed] == ["s-t-orphaned-0"]


def test_reconcile_does_not_auto_complete_a_pool_task_from_batch_rows(
        dlm_db, monkeypatch):
    """The single most important assertion in this file.

    "Every batch row is done" is a legitimate transient state mid-run: the
    window loop finished the batches it created and has not created the next
    window's rows yet. Reading it as task completion lets the reconciler report
    `done` without the coordinator's missing-file verification, without the
    ceiling check, and without the WARNING that must accompany a `done` with
    known missing files — the exact "reported done, no alert, no queryable
    record" failure the accounting exists to prevent, reached without any file
    being missing at all.
    """
    _pool_task(dlm_db, "t-pool-window", name="windowed")
    _age_task(dlm_db, "t-pool-window", DEAD)
    _shard(dlm_db, "b-t-pool-window-0", "w1", OLD, task_id="t-pool-window",
           index=0, status="done")
    _shard(dlm_db, "b-t-pool-window-1", "w2", OLD, task_id="t-pool-window",
           index=1, status="done")

    report = _reconcile(monkeypatch)

    assert dlm_db.get_task("t-pool-window")["status"] == "downloading", \
        "reconciler declared a pool task done from batch rows"
    assert report.get("auto_completed") is None
    assert dlm_db.get_task("t-pool-window")["completed_at"] in (None, "")


def test_reconcile_does_not_auto_fail_a_pool_task_from_batch_rows(
        dlm_db, monkeypatch):
    """A batch that exhausted its attempts still has the coordinator's
    re-dispatch round ahead of it (workflows.py' second pass over failed
    batches). Burying the task here removes that round and, because a failed
    task is never auto-re-dispatched, ends the download for good."""
    _pool_task(dlm_db, "t-pool-failed", name="poolfailed")
    _age_task(dlm_db, "t-pool-failed", DEAD)
    _shard(dlm_db, "b-t-pool-failed-0", "w1", OLD, task_id="t-pool-failed",
           index=0, status="failed")
    _shard(dlm_db, "b-t-pool-failed-1", "w2", OLD, task_id="t-pool-failed",
           index=1, status="done")

    report = _reconcile(monkeypatch)

    assert dlm_db.get_task("t-pool-failed")["status"] == "downloading"
    assert report.get("auto_failed") is None


def test_reconcile_records_a_stale_pool_task_as_pool_orphaned(dlm_db, monkeypatch):
    """Skipping the inference must not make a genuinely dead pool coordinator
    invisible: pool_orphaned is the only signal a human gets, since pool tasks
    do not self-heal."""
    _pool_task(dlm_db, "t-pool-dead", name="pooldead")
    _age_task(dlm_db, "t-pool-dead", DEAD)
    _shard(dlm_db, "b-t-pool-dead-0", "w1", OLD, task_id="t-pool-dead",
           status="done")

    report = _reconcile(monkeypatch)

    assert [p["task_id"] for p in report.get("pool_orphaned", [])] == \
        ["t-pool-dead"]


def test_reconcile_still_auto_completes_a_sharded_task(dlm_db, monkeypatch):
    """Regression guard: the sharded inference is what Egocentric-100K's
    self-healing rests on and must behave exactly as before."""
    dlm_db.upsert_task({"id": "t-sh-done", "name": "shdone", "repo_id": "org/x",
                        "status": "downloading", "source": "modelscope",
                        "priority": 0, "dispatch_mode": "sharded"})
    _age_task(dlm_db, "t-sh-done", DEAD)
    _shard(dlm_db, "s-t-sh-done-0", "bj3", OLD, task_id="t-sh-done",
           status="done")

    report = _reconcile(monkeypatch)

    assert dlm_db.get_task("t-sh-done")["status"] == "done"
    assert report["auto_completed"] == ["shdone"]
