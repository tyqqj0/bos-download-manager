"""T1 — pool dispatch DB groundwork: migration, TERMINAL guards, get_running_shards
JOIN, the idempotent /api/pool/batches/create endpoint, and the aggregate
SQL+debounce rewrite.

Run: python3 -m pytest tests/test_pool_db.py -q
"""

from __future__ import annotations

import asyncio
import time

import pytest


# ── migration ───────────────────────────────────────────────────────────


def test_migration_adds_pool_columns_with_expected_defaults(db):
    conn = db._conn()
    conn.execute("INSERT INTO tasks (id, name, status) VALUES ('t1', 'X', 'pending')")
    conn.commit()
    row = dict(conn.execute("SELECT * FROM tasks WHERE id='t1'").fetchone())
    assert row["dispatch_mode"] == "sharded"
    assert row["coordinator_phase"] is None


def test_migration_is_idempotent(db):
    # The fixture already ran init_db() once; running it again must not raise
    # and must not clobber existing data.
    conn = db._conn()
    conn.execute("INSERT INTO tasks (id, name, status) VALUES ('t1', 'X', 'pending')")
    conn.commit()
    db.init_db()
    db.init_db()
    row = dict(conn.execute("SELECT * FROM tasks WHERE id='t1'").fetchone())
    assert row["id"] == "t1"
    assert row["dispatch_mode"] == "sharded"


# ── get_running_shards() JOIN ──────────────────────────────────────────


def _task(db, task_id, status):
    db.upsert_task({"id": task_id, "name": task_id, "status": status,
                     "priority": 5, "created_at": "now"})


def _running_shard(db, shard_id, task_id, server="w1"):
    db.upsert_shard({
        "id": shard_id, "task_id": task_id, "shard_index": 0,
        "status": "running", "server": server,
        "total_files": 1, "total_bytes": 100, "done_bytes": 0,
    })


def test_running_shard_of_a_live_task_is_returned(db):
    _task(db, "t-live", "downloading")
    _running_shard(db, "s-t-live-0", "t-live")

    ids = [s["id"] for s in db.get_running_shards()]
    assert "s-t-live-0" in ids


@pytest.mark.parametrize("terminal_status", [
    "paused", "preempted", "revoked", "skipped", "failed", "done",
])
def test_running_shard_of_a_terminal_task_is_excluded(db, terminal_status):
    """The stale-busy-forever bug: a stopped task's running row used to pin
    its server busy for every caller until something deleted the row."""
    _task(db, "t-stopped", terminal_status)
    _running_shard(db, "s-t-stopped-0", "t-stopped")

    ids = [s["id"] for s in db.get_running_shards()]
    assert "s-t-stopped-0" not in ids


def test_running_shards_mix_of_live_and_terminal_parents(db):
    _task(db, "t-live", "downloading")
    _running_shard(db, "s-t-live-0", "t-live")
    _task(db, "t-dead", "failed")
    _running_shard(db, "s-t-dead-0", "t-dead")

    ids = {s["id"] for s in db.get_running_shards()}
    assert ids == {"s-t-live-0"}


# ── /api/shards/assign and /api/shards/status TERMINAL guards ─────────


def _call(coro):
    return asyncio.run(coro)


def test_assign_on_terminal_task_is_ignored_not_written(db):
    from dlm.web.routes.queue import assign_shard_server_api

    _task(db, "t-done", "done")
    db.upsert_shard({"id": "s-t-done-0", "task_id": "t-done", "shard_index": 0,
                      "status": "pending", "total_files": 1, "total_bytes": 1})

    result = _call(assign_shard_server_api({"shard_id": "s-t-done-0", "server_key": "w9"}))

    assert result == {"ok": True, "ignored": True}
    assert db.get_shard("s-t-done-0")["server"] is None


def test_assign_on_live_task_still_writes(db):
    from dlm.web.routes.queue import assign_shard_server_api

    _task(db, "t-live", "downloading")
    db.upsert_shard({"id": "s-t-live-0", "task_id": "t-live", "shard_index": 0,
                      "status": "pending", "total_files": 1, "total_bytes": 1})

    result = _call(assign_shard_server_api({"shard_id": "s-t-live-0", "server_key": "w9"}))

    assert result == {"ok": True}
    assert db.get_shard("s-t-live-0")["server"] == "w9"


def test_status_update_on_terminal_task_is_ignored(db):
    from dlm.web.routes.queue import update_shard_status_api

    _task(db, "t-paused", "paused")
    db.upsert_shard({"id": "s-t-paused-0", "task_id": "t-paused", "shard_index": 0,
                      "status": "running", "total_files": 1, "total_bytes": 1})

    result = _call(update_shard_status_api({"shard_id": "s-t-paused-0", "status": "failed"}))

    assert result == {"ok": True, "ignored": True}
    assert db.get_shard("s-t-paused-0")["status"] == "running"  # unchanged


def test_status_update_on_live_task_still_writes(db):
    from dlm.web.routes.queue import update_shard_status_api

    _task(db, "t-live", "downloading")
    db.upsert_shard({"id": "s-t-live-0", "task_id": "t-live", "shard_index": 0,
                      "status": "running", "total_files": 1, "total_bytes": 1})

    result = _call(update_shard_status_api({"shard_id": "s-t-live-0", "status": "done"}))

    assert result == {"ok": True}
    assert db.get_shard("s-t-live-0")["status"] == "done"


# ── POST /api/pool/batches/create ──────────────────────────────────────


def _batch_infos(n=3):
    return [
        {"shard_index": i, "filelist_key": f"batch-{i}.json",
         "total_files": 10 + i, "total_bytes": 1000 + i}
        for i in range(n)
    ]


def test_pool_batches_create_inserts_all_rows_in_one_go(db):
    from dlm.web.routes.queue import create_pool_batches

    _task(db, "t-pool", "downloading")
    infos = _batch_infos(3)

    result = _call(create_pool_batches({"task_id": "t-pool", "shard_infos": infos}))

    assert result["ok"] is True
    assert result["idempotent"] is False
    assert len(result["shard_ids"]) == 3
    rows = db.get_shards_by_task("t-pool")
    assert len(rows) == 3
    assert {r["status"] for r in rows} == {"pending"}


def test_pool_batches_create_is_idempotent_on_exact_match(db):
    from dlm.web.routes.queue import create_pool_batches

    _task(db, "t-pool", "downloading")
    infos = _batch_infos(3)
    body = {"task_id": "t-pool", "shard_infos": infos}

    first = _call(create_pool_batches(body))
    second = _call(create_pool_batches(body))

    assert second["ok"] is True
    assert second["idempotent"] is True
    assert second["shard_ids"] == first["shard_ids"]
    # still exactly 3 rows — no double insert
    assert len(db.get_shards_by_task("t-pool")) == 3


def test_pool_batches_create_idempotent_hit_resets_non_done_rows(db):
    from dlm.web.routes.queue import create_pool_batches

    _task(db, "t-pool", "downloading")
    infos = _batch_infos(3)
    body = {"task_id": "t-pool", "shard_infos": infos}
    first = _call(create_pool_batches(body))
    shard_ids = sorted(first["shard_ids"])

    # Simulate a prior attempt that got partway through a window: one row
    # running on a server, one row already fully uploaded (done).
    db.update_shard_progress(shard_ids[0], status="running", server="bj3", speed_mbps=42)
    db.complete_shard(shard_ids[1], "done")
    db.update_shard_progress(shard_ids[1], server="bj4")

    second = _call(create_pool_batches(body))
    assert second["idempotent"] is True

    running_reset = db.get_shard(shard_ids[0])
    assert running_reset["status"] == "pending"
    assert running_reset["server"] is None
    assert running_reset["speed_mbps"] == 0

    done_row = db.get_shard(shard_ids[1])
    assert done_row["status"] == "done"  # left alone
    assert done_row["server"] == "bj4"   # untouched, not reset


def test_pool_batches_create_rejects_mismatched_retry(db):
    from dlm.web.routes.queue import create_pool_batches

    _task(db, "t-pool", "downloading")
    first = _call(create_pool_batches({"task_id": "t-pool", "shard_infos": _batch_infos(3)}))
    assert first["idempotent"] is False

    # A retry claiming a different chunking (different total_bytes) is not
    # the same idempotency key — must not silently overwrite.
    different = _batch_infos(3)
    different[0]["total_bytes"] += 1
    result = _call(create_pool_batches({"task_id": "t-pool", "shard_infos": different}))

    assert "error" in result
    assert len(db.get_shards_by_task("t-pool")) == 3  # unchanged


def test_pool_batches_create_rejects_expected_count_mismatch(db):
    from dlm.web.routes.queue import create_pool_batches

    _task(db, "t-pool", "downloading")
    result = _call(create_pool_batches({
        "task_id": "t-pool", "shard_infos": _batch_infos(3), "expected_count": 5,
    }))

    assert "error" in result
    assert db.get_shards_by_task("t-pool") == []


def test_pool_batches_create_on_terminal_task_is_ignored_not_written(db):
    """A late coordinator retry after an operator revokes the task must not
    resurrect it — mirrors the /shards/status and /shards/assign guards."""
    from dlm.web.routes.queue import create_pool_batches

    _task(db, "t-revoked", "revoked")
    db.upsert_shard({"id": "s-t-revoked-0", "task_id": "t-revoked", "shard_index": 0,
                      "status": "failed", "server": "bj3",
                      "total_files": 1, "total_bytes": 1})

    result = _call(create_pool_batches({
        "task_id": "t-revoked", "shard_infos": _batch_infos(3),
    }))

    assert result == {"ok": True, "ignored": True}
    row = db.get_shard("s-t-revoked-0")
    assert row["status"] == "failed"  # untouched, not reset to pending
    assert row["server"] == "bj3"
    assert len(db.get_shards_by_task("t-revoked")) == 1  # no new rows created


def test_pool_batches_create_on_nonexistent_task_is_ignored(db):
    from dlm.web.routes.queue import create_pool_batches

    result = _call(create_pool_batches({
        "task_id": "t-nope", "shard_infos": _batch_infos(3),
    }))

    assert result == {"ok": True, "ignored": True, "reason": "task not found"}
    assert db.get_shards_by_task("t-nope") == []


def test_pool_batches_create_partial_insert_failure_leaves_no_rows(db, monkeypatch):
    """A crash mid-executemany must not leave a stray open transaction on the
    pool thread's connection for the next request to inherit and commit —
    the exact bug the `with conn:` transaction wrap guards against."""
    import sqlite3

    from dlm.web.routes.queue import create_pool_batches

    _task(db, "t-pool", "downloading")
    infos = _batch_infos(3)

    # sqlite3.Connection is an immutable C type, so the flaky executemany goes
    # on a delegating proxy handed out by a patched snapshot._conn instead.
    class FlakyConn:
        def __init__(self, real):
            self._real = real

        def __getattr__(self, name):
            return getattr(self._real, name)

        def __enter__(self):
            self._real.__enter__()
            return self

        def __exit__(self, *exc):
            return self._real.__exit__(*exc)

        def executemany(self, sql, seq):
            rows = list(seq)
            self._real.execute(sql, rows[0])  # one row lands, uncommitted...
            raise sqlite3.OperationalError("simulated mid-batch failure")

    real_conn_fn = db._conn
    monkeypatch.setattr(db, "_conn", lambda: FlakyConn(real_conn_fn()))
    try:
        with pytest.raises(sqlite3.OperationalError):
            _call(create_pool_batches({"task_id": "t-pool", "shard_infos": infos}))
    finally:
        monkeypatch.undo()

    assert db.get_shards_by_task("t-pool") == []  # rollback ate the partial insert

    # A subsequent correct call on the same (now-restored) connection must
    # still succeed cleanly — no stray open transaction left behind.
    result = _call(create_pool_batches({"task_id": "t-pool", "shard_infos": infos}))
    assert result["ok"] is True
    assert result["idempotent"] is False
    assert len(db.get_shards_by_task("t-pool")) == 3


# ── _aggregate_task: SQL aggregate + 5s debounce ───────────────────────


def test_aggregate_task_matches_manual_sum(db):
    from dlm.web.routes.queue import _aggregate_task

    _task(db, "t-agg-1", "downloading")
    db.upsert_shard({"id": "s-t-agg-1-0", "task_id": "t-agg-1", "shard_index": 0,
                      "status": "running", "total_bytes": 1000, "done_bytes": 400,
                      "speed_mbps": 10})
    db.upsert_shard({"id": "s-t-agg-1-1", "task_id": "t-agg-1", "shard_index": 1,
                      "status": "done", "total_bytes": 2000, "done_bytes": 2000,
                      "speed_mbps": 0})

    _aggregate_task("t-agg-1", force=True)

    task = db.get_task("t-agg-1")
    assert task["downloaded_gb"] == round(2400 / (1024 ** 3), 2)
    assert task["progress_pct"] == round(2400 / 3000 * 100, 1)
    assert task["speed_mbps"] == 10
    assert task["done_shards"] == 1
    assert task["total_shards"] == 2


def test_aggregate_task_debounces_within_5s(db):
    from dlm.web.routes.queue import _aggregate_task

    _task(db, "t-agg-2", "downloading")
    db.upsert_shard({"id": "s-t-agg-2-0", "task_id": "t-agg-2", "shard_index": 0,
                      "status": "running", "total_bytes": 1000, "done_bytes": 100})

    first = _aggregate_task("t-agg-2")
    assert first.get("skipped") != "debounced"  # first call for this task_id always runs

    # Shard makes more progress, but a second call within the debounce
    # window must be a no-op — same task-level totals as before.
    db.update_shard_progress("s-t-agg-2-0", done_bytes=900)
    second = _aggregate_task("t-agg-2")

    assert second == {"ok": True, "skipped": "debounced"}
    task = db.get_task("t-agg-2")
    assert task["downloaded_gb"] == round(100 / (1024 ** 3), 2)  # not 900 — debounced


def test_aggregate_task_force_bypasses_debounce(db):
    from dlm.web.routes.queue import _aggregate_task

    _task(db, "t-agg-3", "downloading")
    db.upsert_shard({"id": "s-t-agg-3-0", "task_id": "t-agg-3", "shard_index": 0,
                      "status": "running", "total_bytes": 1000, "done_bytes": 100})

    _aggregate_task("t-agg-3")
    db.update_shard_progress("s-t-agg-3-0", done_bytes=900)
    _aggregate_task("t-agg-3", force=True)

    task = db.get_task("t-agg-3")
    assert task["downloaded_gb"] == round(900 / (1024 ** 3), 2)


def test_status_endpoint_reaching_terminal_forces_aggregate_past_debounce(db):
    """The regression this guards: /shards/status marking a shard done/failed
    is that shard's last possible write — if the debounce ate it, the task's
    aggregate would stay stale forever for that shard."""
    from dlm.web.routes.queue import _aggregate_task, update_shard_status_api

    _task(db, "t-agg-4", "downloading")
    db.upsert_shard({"id": "s-t-agg-4-0", "task_id": "t-agg-4", "shard_index": 0,
                      "status": "running", "total_bytes": 1000, "done_bytes": 500})

    # Burn the debounce window immediately before the terminal write.
    _aggregate_task("t-agg-4")

    db.update_shard_progress("s-t-agg-4-0", done_bytes=1000)
    _call(update_shard_status_api({"shard_id": "s-t-agg-4-0", "status": "done"}))

    task = db.get_task("t-agg-4")
    assert task["downloaded_gb"] == round(1000 / (1024 ** 3), 2)
    assert task["done_shards"] == 1
