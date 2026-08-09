"""Phase two: arming a finished download for transfer.

Run: python3 -m pytest tests/test_transfer_arm.py -q

Arming is the gate between "a download reported done" and "copy these bytes to
另一个云". Everything here pins one of two properties:

1. **A `done` that cannot be believed never becomes a transfer.** The four hard
   gates. `t-20260805-460d45` was a `done` task that had downloaded nothing —
   gate 4 is what stands between that shape and an import.
2. **Arming cannot hurt the thing that triggers it.** It runs inside the
   coordinator's own progress report, so it writes only transfer columns, issues
   zero network I/O, and swallows its own failures.
"""

from __future__ import annotations

import asyncio
import socket
import threading

import pytest

from dlm.constants import DATA_BUCKET
from dlm.transfer import arm as arm_mod
from dlm.transfer.arm import band_ratio, maybe_arm_transfer


def _task(db, task_id="t-1", **over):
    row = {
        "id": task_id,
        "name": "molmobot-data",
        "repo_id": "org/molmobot",
        "source": "hf",
        "type": "dataset",
        "category": "other",
        "status": "done",
        "priority": 0,
        "dispatch_mode": "pool",
    }
    row.update(over)
    db.upsert_task(row)
    if "dispatch_prefix" in over:
        db.set_dispatch_prefix(task_id, over["dispatch_prefix"])
    return row


def _shards(db, task_id="t-1", n=2, status="done", total_bytes=500):
    for i in range(n):
        db.upsert_shard({
            "id": f"{task_id}-s{i}",
            "task_id": task_id,
            "shard_index": i,
            "status": status,
            "total_bytes": total_bytes,
        })


@pytest.fixture
def armable(db):
    """A task in the exact shape that SHOULD arm: done, never armed, prefix
    matching what it dispatched to, every shard row done."""
    _task(db, dispatch_prefix=f"{DATA_BUCKET}/other/molmobot-data/")
    _shards(db)
    return db


# ── the happy path ────────────────────────────────────────────────────────


def test_arming_writes_ready_with_the_prefix_and_bytes_frozen(armable):
    result = maybe_arm_transfer("t-1")
    assert result["armed"] is True
    row = armable.get_task("t-1")
    assert row["transfer_status"] == "ready"
    assert row["transfer_error"] is None
    # The snapshot the dispatcher will use verbatim — never re-derived, or a
    # later rename would redirect the transfer to a prefix nobody uploaded to.
    assert row["transfer_prefix"] == f"{DATA_BUCKET}/other/molmobot-data/"
    assert row["transfer_bytes"] == 1000        # SUM(shards.total_bytes)
    assert row["transfer_armed_at"] > 0


def test_arming_never_writes_the_tasks_own_status(armable):
    """The old Celery transfer set status="transferring" on the task itself, so
    a transfer failure read as a download failure. Download state and transfer
    state are orthogonal."""
    before = armable.get_task("t-1")["status"]
    maybe_arm_transfer("t-1")
    assert armable.get_task("t-1")["status"] == before == "done"


def test_arming_issues_no_network_io(armable, monkeypatch):
    """Arming runs inside /api/task-progress — the request a coordinator makes to
    report its own completion. A BOS prefix scan of a 3.4 TB dataset is thousands
    of list pages; doing that here would park the workflow that sent the report.
    So: any socket at all is a bug, not just a slow one."""
    def _no(*a, **k):
        raise AssertionError("arming opened a socket")

    monkeypatch.setattr(socket.socket, "connect", _no)
    monkeypatch.setattr(socket.socket, "connect_ex", _no)
    assert maybe_arm_transfer("t-1")["armed"] is True


def test_a_task_that_is_not_done_does_not_arm(db):
    _task(db, status="downloading")
    _shards(db)
    result = maybe_arm_transfer("t-1")
    assert result["armed"] is False
    assert db.get_task("t-1")["transfer_status"] is None


def test_an_unknown_task_is_a_skip_not_a_crash(db):
    assert maybe_arm_transfer("t-nope") == {
        "armed": False, "status": None, "reason": "unknown task"}


# ── gate 1: never armed ───────────────────────────────────────────────────


def test_a_repeated_done_report_does_not_arm_twice(armable):
    assert maybe_arm_transfer("t-1")["armed"] is True
    armed_at = armable.get_task("t-1")["transfer_armed_at"]

    again = maybe_arm_transfer("t-1")
    assert again["armed"] is False
    assert "already transfer_status='ready'" in again["reason"]
    assert armable.get_task("t-1")["transfer_armed_at"] == armed_at


def test_manual_arming_re_queues_a_failed_row(armable):
    arm_mod._write("t-1", "failed", "remote said 失败")
    result = maybe_arm_transfer("t-1", manual=True)
    assert result["armed"] is True
    assert armable.get_task("t-1")["transfer_status"] == "ready"
    assert armable.get_task("t-1")["transfer_error"] is None


@pytest.mark.parametrize("state", ["transferring", "verifying"])
def test_not_even_a_manual_trigger_re_queues_an_in_flight_transfer(armable, state):
    """Two imports writing one destination directory is the failure mode the
    in-flight check exists for (DL3DV, 2026-08-04): our side gave up at its 72h
    poll cap while the remote task kept writing for two more days."""
    arm_mod._write("t-1", state, None)
    result = maybe_arm_transfer("t-1", manual=True)
    assert result["armed"] is False
    assert "in flight" in result["reason"]
    assert armable.get_task("t-1")["transfer_status"] == state


# ── gate 2: not paused ────────────────────────────────────────────────────


def test_paused_transfers_do_not_arm(armable):
    arm_mod.set_transfers_paused(True)
    result = maybe_arm_transfer("t-1")
    assert result["armed"] is False
    assert result["reason"] == "transfers are paused"
    assert armable.get_task("t-1")["transfer_status"] is None


def test_pause_survives_a_process_restart(db, tmp_path, monkeypatch):
    """It used to live in the web process's in-memory `cache`, so the pause
    button worked until the next restart and then silently un-paused."""
    arm_mod.set_transfers_paused(True)
    # A restart is a new thread-local connection against the same file.
    db._local = threading.local()
    assert arm_mod.transfers_paused() is True
    arm_mod.set_transfers_paused(False)
    db._local = threading.local()
    assert arm_mod.transfers_paused() is False


def test_the_auto_switch_only_gates_the_automatic_path(armable, monkeypatch):
    monkeypatch.setenv("DLM_AUTO_TRANSFER", "off")
    assert maybe_arm_transfer("t-1")["armed"] is False
    assert maybe_arm_transfer("t-1", manual=True)["armed"] is True


# ── gate 3: the prefix has not drifted ────────────────────────────────────


def test_a_renamed_task_is_blocked_with_both_prefixes_named(db):
    """Five `done` tasks read 0 bytes under their current bos_target() prefix
    purely because someone gave them a nicer display name after they finished.
    That is the only remaining way a prefix can drift, and it must stop a
    transfer rather than silently copy an empty prefix."""
    _task(db, dispatch_prefix=f"{DATA_BUCKET}/other/molmobot-data-v1/")
    _shards(db)
    result = maybe_arm_transfer("t-1")
    assert (result["armed"], result["status"]) == (False, "blocked")
    row = db.get_task("t-1")
    assert row["transfer_status"] == "blocked"
    assert "molmobot-data-v1" in row["transfer_error"]
    assert "other/molmobot-data/" in row["transfer_error"]


def test_a_null_dispatch_prefix_skips_the_drift_gate(db):
    """Every row created before this column existed has NULL here. Rejecting on
    a fact we never recorded would block the entire existing backlog."""
    _task(db)
    _shards(db)
    assert db.get_task("t-1")["dispatch_prefix"] is None
    assert maybe_arm_transfer("t-1")["armed"] is True


def test_a_model_matches_on_its_own_bucket(db):
    from dlm.constants import MODEL_BUCKET

    _task(db, name="Qwen3-VL-30B", category="multimodal", type="model",
          dispatch_prefix=f"{MODEL_BUCKET}/Qwen3-VL-30B/")
    _shards(db)
    assert maybe_arm_transfer("t-1")["armed"] is True
    assert db.get_task("t-1")["transfer_prefix"] == f"{MODEL_BUCKET}/Qwen3-VL-30B/"


def test_the_same_key_shape_under_a_different_bucket_is_drift(db):
    """A category-less dataset and a model share the key shape `{name}/`. If the
    comparison dropped the bucket, flipping a task's type would pass this gate
    and then read a prefix in the wrong bucket."""
    from dlm.constants import MODEL_BUCKET

    _task(db, name="X", category="", type="dataset",
          dispatch_prefix=f"{MODEL_BUCKET}/X/")
    _shards(db)
    assert maybe_arm_transfer("t-1")["armed"] is False
    assert db.get_task("t-1")["transfer_status"] == "blocked"


# ── gate 4: the shard rows account for the task ───────────────────────────


def test_zero_shard_rows_is_blocked(db):
    """`PhysicalAI-Robotics-Locomanipulation`'s shape: done, 0 shard rows,
    208.4/232.6 GB. Nothing proves what was downloaded, so nothing may move."""
    _task(db)
    result = maybe_arm_transfer("t-1")
    assert result["status"] == "blocked"
    assert "0 shard rows" in db.get_task("t-1")["transfer_error"]


@pytest.mark.parametrize("status", ["pending", "running", "failed"])
def test_any_unfinished_shard_row_blocks(db, status):
    _task(db)
    _shards(db, n=3)
    db.upsert_shard({"id": "t-1-s3", "task_id": "t-1", "shard_index": 3,
                     "status": status, "total_bytes": 500})
    result = maybe_arm_transfer("t-1")
    assert result["status"] == "blocked"
    assert "1/4 shard rows are not done" in db.get_task("t-1")["transfer_error"]


def test_pool_batches_need_no_separate_gate(db):
    """Pool batch rows and sharded shard rows share the `shards` table, so gate
    4 covers both modes unchanged — and release_pool_batches returning rows to
    `pending` is what makes a cancelled pool task fail it on its own."""
    _task(db, dispatch_mode="pool")
    _shards(db, n=1500, status="done", total_bytes=1)
    assert maybe_arm_transfer("t-1")["armed"] is True
    assert db.get_task("t-1")["transfer_bytes"] == 1500


# ── the byte-ratio bands (applied by the dispatcher, decided here) ────────


def test_a_full_prefix_is_ready_without_an_alert():
    assert band_ratio(1000, 1000) == ("ready", None, "ratio 1.0000")
    assert band_ratio(960, 1000)[:2] == ("ready", None)


def test_a_resumed_task_whose_prefix_holds_earlier_rounds_is_ready():
    """`transfer_bytes` counts only THIS round's dispatch, so a resumed task's
    ratio is legitimately far above 1 (RoboDojo measured 8.65)."""
    assert band_ratio(8_650, 1_000)[:2] == ("ready", None)


def test_the_middle_band_transfers_but_always_warns():
    status, severity, note = band_ratio(700, 1000)
    assert (status, severity) == ("ready", "warning")
    assert "300 short" in note


def test_a_hundredfold_shortfall_is_blocked_and_critical():
    """The known false-done shape: Sekai 60.49/9392.89, DL3DV 10.18/40885.96,
    SpatialVID 51.99/7141.28 — ratios 0.0002–0.013."""
    status, severity, _ = band_ratio(13, 1000)
    assert (status, severity) == ("blocked", "critical")


def test_an_empty_bos_prefix_is_blocked_whatever_the_denominator():
    assert band_ratio(0, 1000)[:2] == ("blocked", "critical")
    assert band_ratio(0, 0)[:2] == ("blocked", "critical")


def test_a_fully_resumed_task_is_judged_only_on_being_non_empty():
    """AgiBotWorld-Alpha dispatched ~0 bytes and has 9000.8 GB on BOS: the
    resume filter already proved file-by-file that BOS holds them."""
    status, severity, note = band_ratio(9_000, 0)
    assert (status, severity) == ("ready", None)
    assert "fully resumed" in note


def test_the_band_edges_are_the_documented_ones():
    assert arm_mod.RATIO_READY == 0.95 and arm_mod.RATIO_MIN == 0.50
    assert band_ratio(950, 1000)[1] is None          # exactly 0.95 → clean
    assert band_ratio(949, 1000)[1] == "warning"
    assert band_ratio(500, 1000)[1] == "warning"     # exactly 0.50 → still goes
    assert band_ratio(499, 1000)[1] == "critical"


# ── the trigger is structural, not status-based ───────────────────────────


def test_complete_task_alone_does_not_arm(armable):
    """The reconciler INFERS `done` from shard rows and calls complete_task —
    that inference is how t-20260805-460d45 became a done task with nothing
    downloaded. Arming must not hang off complete_task, or the inferred done
    would transfer too."""
    armable.complete_task("t-1", "done")
    assert armable.get_task("t-1")["transfer_status"] is None


def test_the_reconciler_does_not_reach_the_arm_module():
    """Pinned structurally: the guard above is only as good as the reconciler
    never growing an arm call."""
    from pathlib import Path

    src = Path("dlm/web/reconciler.py").read_text()
    assert "transfer.arm" not in src and "maybe_arm_transfer" not in src


@pytest.fixture
def reporting(db):
    """A task in the shape a coordinator reports from: still `downloading`, with
    its batch rows already done. A report against an already-`done` row is
    ignored by the route's terminal-status guard, so it can't exercise arming."""
    _task(db, status="downloading",
          dispatch_prefix=f"{DATA_BUCKET}/other/molmobot-data/")
    _shards(db)
    return db


def test_the_progress_route_arms_a_workflow_reported_done(reporting):
    from dlm.web.routes import servers as servers_routes

    result = asyncio.run(servers_routes.task_progress({
        "task_id": "t-1", "status": "done", "progress_pct": 100}))
    assert result == {"ok": True, "completed": "done"}
    assert reporting.get_task("t-1")["transfer_status"] == "ready"


def test_a_failed_report_does_not_arm(reporting):
    from dlm.web.routes import servers as servers_routes

    asyncio.run(servers_routes.task_progress({
        "task_id": "t-1", "status": "failed", "error": "boom"}))
    assert reporting.get_task("t-1")["transfer_status"] is None


def test_an_arming_bug_cannot_fail_a_workers_done_report(reporting, monkeypatch):
    monkeypatch.setattr(arm_mod, "maybe_arm_transfer",
                        lambda *a, **k: 1 / 0)
    from dlm.web.routes import servers as servers_routes

    result = asyncio.run(servers_routes.task_progress({
        "task_id": "t-1", "status": "done"}))
    assert result == {"ok": True, "completed": "done"}
    assert reporting.get_task("t-1")["status"] == "done"
    assert reporting.get_task("t-1")["transfer_status"] is None


# ── the three routes that used to be dead ─────────────────────────────────


def test_the_transfer_routes_no_longer_touch_celery():
    from pathlib import Path

    src = Path("dlm/web/routes/transfer.py").read_text()
    # The docstring names Celery to explain what these routes used to do, so
    # match on what actually reaches it: an import, or a task send.
    assert "queue.app" not in src and "import celery" not in src.lower()
    assert "apply_async" not in src and "delay(" not in src
    assert "transfer_to_juicefs" not in src


def test_the_celery_transfer_task_is_gone():
    from pathlib import Path

    assert not Path("dlm/transfer/tasks.py").exists()
    assert "transfer_to_juicefs" not in Path("dlm/queue/app.py").read_text()
    with pytest.raises(ImportError):
        __import__("dlm.transfer.tasks")


def test_trigger_arms_the_backlog(armable):
    from dlm.web.routes import transfer as transfer_routes

    result = asyncio.run(transfer_routes.trigger_transfer({}))
    assert result["count"] == 1
    assert armable.get_task("t-1")["transfer_status"] == "ready"

    # Second sweep: already armed, so the backlog sweep leaves it alone.
    assert asyncio.run(transfer_routes.trigger_transfer({}))["count"] == 0


def test_trigger_reports_why_it_skipped_rather_than_silently_doing_nothing(db):
    from dlm.web.routes import transfer as transfer_routes

    _task(db)          # done, but zero shard rows
    result = asyncio.run(transfer_routes.trigger_transfer({}))
    assert result["count"] == 0
    assert "0 shard rows" in result["skipped"][0]


def test_retry_re_arms_one_blocked_row(armable):
    from dlm.web.routes import transfer as transfer_routes

    arm_mod._write("t-1", "failed", "remote 失败")
    result = asyncio.run(transfer_routes.retry_transfer("t-1"))
    assert result["ok"] is True
    assert armable.get_task("t-1")["transfer_status"] == "ready"


def test_retry_on_an_ungateable_task_returns_the_reason(db):
    from dlm.web.routes import transfer as transfer_routes

    _task(db)
    result = asyncio.run(transfer_routes.retry_transfer("t-1"))
    assert "0 shard rows" in result["error"]
    assert result["transfer_status"] == "blocked"


def test_retry_on_an_unknown_task(db):
    from dlm.web.routes import transfer as transfer_routes

    assert asyncio.run(transfer_routes.retry_transfer("t-nope")) == {
        "error": "Task not found"}


def test_the_pause_route_persists_and_reads_back(db):
    from dlm.web.routes import transfer as transfer_routes

    assert asyncio.run(transfer_routes.pause_transfer({"paused": True})) == {
        "paused": True}
    db._local = threading.local()
    assert asyncio.run(transfer_routes.get_transfer_status())["paused"] is True
    assert asyncio.run(transfer_routes.pause_transfer({"paused": False})) == {
        "paused": False}


def test_the_status_route_counts_the_new_states_apart(armable):
    from dlm.web.routes import transfer as transfer_routes

    _task(armable, "t-2", name="B")
    _shards(armable, "t-2")
    arm_mod._write("t-1", "blocked", "gate 4")
    arm_mod._write("t-2", "short", "jfs < bos")

    summary = asyncio.run(transfer_routes.get_transfer_status())["summary"]
    assert summary["blocked"] == 1 and summary["short"] == 1
    assert summary["failed"] == 0 and summary["transferred"] == 0


# ── the dispatch-time prefix record ───────────────────────────────────────


def test_dispatch_records_the_prefix_the_uploader_will_use(db, monkeypatch):
    """One write site for both dispatch modes and all four dispatch callers:
    whatever name/category the row holds at dispatch is what the uploader uses,
    so that is the only prefix a later transfer may read from."""
    from dlm.web import temporal_client

    async def _started(task, task_queue):
        return "started"

    _task(db, status="pending")
    monkeypatch.setattr(temporal_client, "start_pool_download", _started)
    assert asyncio.run(temporal_client.start_task_download(
        db.get_task("t-1"))) == "started"
    assert db.get_task("t-1")["dispatch_prefix"] == \
        f"{DATA_BUCKET}/other/molmobot-data/"


def test_a_dispatch_still_happens_if_the_prefix_cannot_be_recorded(db, monkeypatch):
    from dlm.web import temporal_client

    async def _started(task, task_queue):
        return "started"

    def _boom(*a, **k):
        raise RuntimeError("db is locked")

    _task(db, status="pending")
    monkeypatch.setattr("dlm.queue.snapshot.set_dispatch_prefix", _boom)
    monkeypatch.setattr(temporal_client, "start_pool_download", _started)
    assert asyncio.run(temporal_client.start_task_download(
        db.get_task("t-1"))) == "started"
    assert db.get_task("t-1")["dispatch_prefix"] is None


def test_queue_add_fills_bos_path(db, monkeypatch):
    """This route left the column NULL on every task it ever created."""
    from dlm.web.routes import queue as queue_routes

    result = asyncio.run(queue_routes.add_to_queue({
        "repo_id": "org/molmobot", "name": "molmobot-data",
        "category": "other", "source": "hf"}))
    assert result["ok"] is True
    assert db.get_task(result["task_id"])["bos_path"] == "other/molmobot-data/"


# ── the migration itself ──────────────────────────────────────────────────


def test_init_db_is_idempotent_and_keeps_the_new_columns(db):
    _task(db)
    db.set_dispatch_prefix("t-1", "b/p/")
    db.init_db()
    db.init_db()
    row = db.get_task("t-1")
    assert row["dispatch_prefix"] == "b/p/"
    for col in ("transfer_prefix", "transfer_bytes", "transfer_armed_at",
                "transfer_verified_bytes"):
        assert col in row.keys()
    assert row["transfer_bytes"] == 0 and row["transfer_verified_bytes"] == 0
