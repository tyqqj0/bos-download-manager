"""T1 — the missing-file archive: schema, upsert semantics, and its endpoints.

R4's "缺件不静默" turns "we didn't lose data" from a verbal assurance into a
checkable one, and this table is where the evidence lives. So the properties
that matter here are archival ones: a repeat sighting must not multiply rows,
first_seen must survive, and — the one that decides whether the archive is
worth anything — the rows must outlive the task reaching a terminal state,
because `done`/`failed` is exactly when a human comes asking what was lost.

Run: python3 -m pytest tests/test_missing_files.py -q
"""

from __future__ import annotations

import asyncio

import pytest
from fastapi import HTTPException

from dlm.web.routes import servers as server_routes
from dlm.web.routes import tasks as task_routes


def _task(db, task_id="t-mf", status="downloading", name="X"):
    db.upsert_task({
        "id": task_id, "name": name, "repo_id": "org/r", "source": "hf",
        "type": "dataset", "category": "manipulation", "status": status,
        "priority": 0, "size_gb": 1.0,
    })


def _f(path, reason="download_retries_exhausted", size_bytes=100):
    return {"path": path, "reason": reason, "size_bytes": size_bytes}


# ── schema ──────────────────────────────────────────────────────────────


def test_migration_creates_the_table_and_the_task_counter(db):
    conn = db._conn()
    conn.execute("INSERT INTO tasks (id, name, status) VALUES ('t1','X','pending')")
    conn.commit()
    assert dict(conn.execute("SELECT * FROM tasks WHERE id='t1'").fetchone())[
        "missing_files_count"] == 0
    # Table is queryable, not merely declared.
    assert conn.execute("SELECT COUNT(*) FROM missing_files").fetchone()[0] == 0


def test_migration_is_idempotent_and_keeps_existing_rows(db):
    _task(db)
    db.record_missing_files("t-mf", [_f("a/1.bin")])
    db.init_db()
    db.init_db()
    assert [r["file_path"] for r in db.list_missing_files("t-mf")] == ["a/1.bin"]


# ── upsert semantics ────────────────────────────────────────────────────


def test_the_same_file_seen_twice_is_one_row_with_attempts_2(db):
    """The point of keying on (task_id, file_path).

    T3 reports on EVERY attempt unconditionally — up to 3 activity attempts ×
    2 dispatch rounds — so a poison file arrives six times. The useful fact is
    "this file is missing, tried 6 times", not six rows to deduplicate later.
    """
    _task(db)
    db.record_missing_files("t-mf", [_f("a/1.bin")])
    db.record_missing_files("t-mf", [_f("a/1.bin")])

    rows = db.list_missing_files("t-mf")
    assert len(rows) == 1
    assert rows[0]["attempts"] == 2


def test_first_seen_survives_later_sightings_while_last_seen_moves(db):
    _task(db)
    db.record_missing_files("t-mf", [_f("a/1.bin")])
    first = db.list_missing_files("t-mf")[0]

    conn = db._conn()
    conn.execute("UPDATE missing_files SET first_seen = 1000, last_seen = 1000")
    conn.commit()
    db.record_missing_files("t-mf", [_f("a/1.bin")])

    row = db.list_missing_files("t-mf")[0]
    assert row["first_seen"] == 1000, "first_seen must record the FIRST sighting"
    assert row["last_seen"] > 1000
    assert first["file_path"] == row["file_path"]


def test_a_later_sighting_refreshes_reason_batch_and_server(db):
    """Latest attempt wins on the mutable columns.

    A file that failed on w1 in batch 3 and again on w5 in batch 3's re-dispatch
    is most usefully described by where it last failed — that is the host and
    reason someone would go look at.
    """
    _task(db)
    db.record_missing_files("t-mf", [
        dict(_f("a/1.bin", reason="access_denied"), batch_index=3, server="w1"),
    ])
    db.record_missing_files("t-mf", [
        dict(_f("a/1.bin", reason="upload_failed"), batch_index=3, server="w5"),
    ])

    row = db.list_missing_files("t-mf")[0]
    assert (row["reason"], row["server"], row["batch_index"]) == \
        ("upload_failed", "w5", 3)
    assert row["attempts"] == 2


def test_rows_without_a_path_are_skipped_rather_than_stored_empty(db):
    _task(db)
    total = db.record_missing_files("t-mf", [_f("a/1.bin"), {"reason": "x"}, {}])
    assert total == 1


def test_the_count_column_tracks_the_real_row_count(db):
    _task(db)
    assert db.record_missing_files("t-mf", [_f("a/1.bin"), _f("a/2.bin")]) == 2
    assert db.get_task("t-mf")["missing_files_count"] == 2

    # A re-sighting bumps attempts, not the count.
    db.record_missing_files("t-mf", [_f("a/1.bin")])
    assert db.get_task("t-mf")["missing_files_count"] == 2

    assert db.clear_missing_files("t-mf", ["a/1.bin"]) == 1
    assert db.get_task("t-mf")["missing_files_count"] == 1
    assert db.count_missing_files("t-mf") == 1


def test_clearing_removes_only_the_named_paths(db):
    """The re-check verifies file by file, so the clear must be path-scoped.

    A wholesale clear would erase the rows it could NOT verify along with the
    ones it could — silently emptying the archive.
    """
    _task(db)
    db.record_missing_files("t-mf", [_f("a/1.bin"), _f("a/2.bin"), _f("a/3.bin")])
    db.clear_missing_files("t-mf", ["a/2.bin"])
    assert [r["file_path"] for r in db.list_missing_files("t-mf")] == \
        ["a/1.bin", "a/3.bin"]


def test_clearing_a_path_that_was_never_recorded_is_a_no_op(db):
    _task(db)
    db.record_missing_files("t-mf", [_f("a/1.bin")])
    assert db.clear_missing_files("t-mf", ["nope.bin"]) == 1


def test_tasks_do_not_see_each_others_rows(db):
    _task(db, "t-a")
    _task(db, "t-b")
    db.record_missing_files("t-a", [_f("a/1.bin")])
    db.record_missing_files("t-b", [_f("a/1.bin")])

    assert db.count_missing_files("t-a") == 1
    assert db.count_missing_files("t-b") == 1
    db.clear_missing_files("t-a", ["a/1.bin"])
    assert db.count_missing_files("t-a") == 0
    assert db.count_missing_files("t-b") == 1


# ── lifetime ────────────────────────────────────────────────────────────


@pytest.mark.parametrize("status", ["done", "failed"])
def test_rows_survive_the_task_completing(db, status):
    """The whole reason this is not the `events` table.

    A terminal task is when someone asks "so what did we lose?" — if
    complete_task swept the archive, the answer would be gone at the exact
    moment it becomes interesting. R4's revised semantics make this load-
    bearing: a task with missing files reports `done` and gets transferred,
    so the record is the ONLY remaining trace.
    """
    _task(db)
    db.record_missing_files("t-mf", [_f("a/1.bin")])
    db.complete_task("t-mf", status)

    assert db.get_task("t-mf")["status"] == status
    assert db.count_missing_files("t-mf") == 1


def test_deleting_the_task_takes_its_rows_with_it(db):
    """delete_task is the one permitted deleter — orphans keyed on a dead id
    are unactionable, and the id can be reused."""
    _task(db)
    db.record_missing_files("t-mf", [_f("a/1.bin")])
    db.delete_task("t-mf")
    assert db.count_missing_files("t-mf") == 0


def test_resharding_a_task_does_not_touch_its_rows(db):
    """A reshard deletes batch rows and returns the task to pending. It must
    not take the archive with it: the files were missing before the reshard and
    the next round is what proves them present or not."""
    _task(db)
    db.record_missing_files("t-mf", [_f("a/1.bin")])
    db.delete_shards_by_task("t-mf")
    db.update_task_progress("t-mf", status="pending")
    assert db.count_missing_files("t-mf") == 1


# ── POST /api/missing-files ─────────────────────────────────────────────


def test_the_report_endpoint_records_and_stamps_batch_and_server(db):
    _task(db)
    result = asyncio.run(server_routes.report_missing_files({
        "task_id": "t-mf", "batch_index": 7, "server": "w3",
        "files": [_f("a/1.bin"), _f("a/2.bin")],
    }))

    assert result == {"ok": True, "recorded": 2, "task_missing_total": 2}
    rows = db.list_missing_files("t-mf")
    assert {r["server"] for r in rows} == {"w3"}
    assert {r["batch_index"] for r in rows} == {7}


def test_the_report_endpoint_requires_a_task_id(db):
    assert "error" in asyncio.run(
        server_routes.report_missing_files({"files": [_f("a/1.bin")]}))


@pytest.mark.parametrize("status", ["revoked", "paused"])
def test_a_terminal_task_rejects_reports(db, status):
    """Same durability guard /task-progress carries.

    Without it a zombie activity from a task an operator revoked keeps writing
    rows against it, and the archive of a stopped task grows on its own.
    """
    _task(db, status=status)
    result = asyncio.run(server_routes.report_missing_files({
        "task_id": "t-mf", "files": [_f("a/1.bin")]}))

    assert result["ok"] is True and "ignored" in result
    assert db.count_missing_files("t-mf") == 0


def test_the_guard_uses_the_shared_terminal_status_set(db):
    """Pinned so a status added to TERMINAL_STATUSES covers this endpoint too,
    rather than needing someone to remember a second list."""
    from dlm.web.fleet import TERMINAL_STATUSES

    for status in TERMINAL_STATUSES:
        _task(db, status=status)
        asyncio.run(server_routes.report_missing_files({
            "task_id": "t-mf", "files": [_f("a/1.bin")]}))
        assert db.count_missing_files("t-mf") == 0, f"{status} accepted a report"


def test_an_oversized_report_is_refused_not_truncated(db):
    """A batch caps at 500 files, so a report near the ceiling is a bug.

    Refusing is the honest failure: a truncated archive reads as complete.
    """
    _task(db)
    files = [_f(f"a/{i}.bin") for i in range(server_routes.MISSING_FILES_REPORT_MAX + 1)]
    with pytest.raises(HTTPException) as exc:
        asyncio.run(server_routes.report_missing_files({
            "task_id": "t-mf", "files": files}))

    assert exc.value.status_code == 413
    assert db.count_missing_files("t-mf") == 0


def test_a_report_at_exactly_the_limit_is_accepted(db):
    _task(db)
    files = [_f(f"a/{i}.bin") for i in range(server_routes.MISSING_FILES_REPORT_MAX)]
    result = asyncio.run(server_routes.report_missing_files({
        "task_id": "t-mf", "files": files}))
    assert result["recorded"] == server_routes.MISSING_FILES_REPORT_MAX


def test_a_non_list_files_field_is_rejected(db):
    _task(db)
    assert "error" in asyncio.run(server_routes.report_missing_files({
        "task_id": "t-mf", "files": "a/1.bin"}))


def test_a_report_for_an_unknown_task_is_still_recorded(db):
    """Fails open on purpose: the guard is about tasks that are terminal, not
    about ones the reporter cannot prove exist. Losing the evidence because a
    row went missing is the wrong trade."""
    result = asyncio.run(server_routes.report_missing_files({
        "task_id": "t-ghost", "files": [_f("a/1.bin")]}))
    assert result["recorded"] == 1


# ── GET / DELETE /api/tasks/{id}/missing-files ──────────────────────────


def test_the_query_endpoint_lists_the_rows(db):
    _task(db)
    db.record_missing_files("t-mf", [_f("a/2.bin"), _f("a/1.bin")])
    result = asyncio.run(task_routes.list_task_missing_files("t-mf"))

    assert result["count"] == 2
    assert [f["file_path"] for f in result["files"]] == ["a/1.bin", "a/2.bin"]


def test_the_query_endpoint_404s_for_an_unknown_task(db):
    with pytest.raises(HTTPException) as exc:
        asyncio.run(task_routes.list_task_missing_files("t-ghost"))
    assert exc.value.status_code == 404


def test_the_query_endpoint_documents_the_zero_byte_blind_spot(db):
    """Source-reported 0-byte files never enter a filelist (the HF and
    ModelScope list activities both require a positive size), so they can never
    appear in this table — ModelScope's RoboDojo depth files are that shape.
    Anyone reading this endpoint's output as a complete inventory would be
    wrong, so the docstring has to say so.
    """
    doc = task_routes.list_task_missing_files.__doc__ or ""
    assert "0 byte" in doc or "0-byte" in doc


def test_the_clear_endpoint_removes_the_named_paths(db):
    _task(db)
    db.record_missing_files("t-mf", [_f("a/1.bin"), _f("a/2.bin")])
    req = task_routes.ClearMissingFilesRequest(paths=["a/1.bin"])
    result = asyncio.run(task_routes.clear_task_missing_files("t-mf", req))

    assert result == {"ok": True, "cleared": 1, "remaining": 1}
    assert [r["file_path"] for r in db.list_missing_files("t-mf")] == ["a/2.bin"]


def test_the_clear_endpoint_404s_for_an_unknown_task(db):
    req = task_routes.ClearMissingFilesRequest(paths=["a/1.bin"])
    with pytest.raises(HTTPException) as exc:
        asyncio.run(task_routes.clear_task_missing_files("t-ghost", req))
    assert exc.value.status_code == 404
