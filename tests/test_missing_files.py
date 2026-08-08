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


# ── T4: POST /api/tasks/{id}/missing-limit ──────────────────────────────
#
# The ceiling has to be stored, not just used: alerting runs off the task row
# long after the coordinator's listing count is gone (see T5).


def test_the_limit_endpoint_stores_the_ceiling_and_returns_the_count(db):
    _task(db)
    db.record_missing_files("t-mf", [_f("a/1.bin"), _f("a/2.bin")])
    req = task_routes.MissingLimitRequest(limit=17)
    result = asyncio.run(task_routes.set_task_missing_limit("t-mf", req))

    assert result == {"ok": True, "limit": 17, "missing_files_count": 2}
    row = db._conn().execute(
        "SELECT missing_files_limit FROM tasks WHERE id='t-mf'").fetchone()
    assert row["missing_files_limit"] == 17


def test_the_limit_endpoint_404s_for_an_unknown_task(db):
    req = task_routes.MissingLimitRequest(limit=5)
    with pytest.raises(HTTPException) as exc:
        asyncio.run(task_routes.set_task_missing_limit("t-ghost", req))
    assert exc.value.status_code == 404


def test_a_negative_limit_is_clamped_rather_than_stored(db):
    """A stored negative would make T5's `count > limit` rule fire on every
    task, and `limit > 0` (its "was finalized" test) is the wrong shape for it."""
    _task(db)
    db.set_missing_limit("t-mf", -3)
    row = db._conn().execute(
        "SELECT missing_files_limit FROM tasks WHERE id='t-mf'").fetchone()
    assert row["missing_files_limit"] == 0


def test_an_unfinalized_task_has_a_zero_ceiling(db):
    """T5 reads `limit == 0` as "never finalized" and stays quiet. A default of
    anything else would have unfinished tasks alerting."""
    _task(db)
    row = db._conn().execute(
        "SELECT missing_files_limit FROM tasks WHERE id='t-mf'").fetchone()
    assert row["missing_files_limit"] == 0


# ── T4: task_missing_limit — the two-term ceiling ───────────────────────


def test_the_ceiling_is_the_larger_of_the_floor_and_the_ratio():
    from dlm.temporal.models import (TASK_MISSING_ABS, TASK_MISSING_RATIO,
                                     task_missing_limit)

    # Small task: the ratio term is under the floor, so the floor governs —
    # 0.5% of 200 files is 1, which would fail a task over a single bad file.
    assert task_missing_limit(200) == TASK_MISSING_ABS
    # Large task: the ratio governs, because 10 out of a million is not a
    # meaningful bound on what a fleet-wide problem can lose.
    assert task_missing_limit(1_000_000) == int(1_000_000 * TASK_MISSING_RATIO)


def test_the_ceiling_never_goes_negative_on_a_nonsense_count():
    from dlm.temporal.models import TASK_MISSING_ABS, task_missing_limit
    assert task_missing_limit(0) == TASK_MISSING_ABS
    assert task_missing_limit(-5) == TASK_MISSING_ABS


# ── T4: verify_missing_files — BOS is the only truth ───────────────────
#
# The dangerous direction is one-way: clearing a row that is genuinely missing
# destroys the only record that it was ever lost. Keeping a row that is
# actually present costs a false WARNING, or at worst one resume-filtered
# retry. So every uncertainty below must resolve toward "still missing".


class _Meta:
    def __init__(self, size):
        self.content_length = size


class _Head:
    def __init__(self, size):
        self.metadata = _Meta(size)


class _Bos:
    def __init__(self, existing):
        self.existing = existing
        self.looked_up = []

    def get_object_meta_data(self, bucket, key):
        self.looked_up.append(key)
        if key not in self.existing:
            raise RuntimeError(f"404: {key}")
        size = self.existing[key]
        if isinstance(size, Exception):
            raise size
        return _Head(size)


class _Resp:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def _run_verify(rows, existing, monkeypatch, task=None, limit=10):
    """Drive the activity against a fake coordinator + fake BOS.

    Returns (result, bos, calls) where calls is [(verb, url, body)] in order.
    """
    from temporalio.testing import ActivityEnvironment

    from dlm.temporal import activities
    from dlm.temporal.models import TaskInput

    task = task or TaskInput(id="t-mf", name="X", repo_id="org/r", source="hf",
                             type="dataset", category="manipulation")
    bos = _Bos(existing)
    calls = []
    kept = {"count": len(rows)}

    def fake_get(url, timeout=None):
        calls.append(("GET", url, None))
        return _Resp({"count": len(rows), "files": rows})

    def fake_delete(url, json=None, timeout=None):
        calls.append(("DELETE", url, json))
        kept["count"] = len(rows) - len(json["paths"])
        return _Resp({"ok": True, "cleared": len(json["paths"]),
                      "remaining": kept["count"]})

    def fake_post(url, json=None, timeout=None):
        calls.append(("POST", url, json))
        return _Resp({"ok": True, "limit": json["limit"],
                      "missing_files_count": kept["count"]})

    monkeypatch.setattr("requests.get", fake_get)
    monkeypatch.setattr("requests.delete", fake_delete)
    monkeypatch.setattr("requests.post", fake_post)
    monkeypatch.setattr(
        "dlm.core.config.load_config",
        lambda: {"BAIDU_AK": "ak", "BAIDU_SK": "sk", "BOS_ENDPOINT": "https://x"})
    monkeypatch.setattr("dlm.core.bos.create_bos_client",
                        lambda ak, sk, endpoint: bos)
    monkeypatch.setenv("DLM_COORDINATOR", "http://s1:8080")

    env = ActivityEnvironment()
    result = asyncio.run(env.run(activities.verify_missing_files, task, limit))
    return result, bos, calls


def _cleared(calls):
    return [b["paths"] for v, _, b in calls if v == "DELETE"]


def test_a_file_on_bos_at_the_recorded_size_is_cleared(monkeypatch):
    rows = [{"file_path": "a/1.bin", "size_bytes": 100}]
    remaining, bos, calls = _run_verify(
        rows, {"manipulation/X/a/1.bin": 100}, monkeypatch)

    assert _cleared(calls) == [["a/1.bin"]]
    assert remaining == 0


def test_a_file_on_bos_at_the_wrong_size_is_kept(monkeypatch):
    """An interrupted upload leaves a short object. Existence alone would read
    that as delivery and erase the record — both resume filters compare
    key + size for exactly this reason."""
    rows = [{"file_path": "a/1.bin", "size_bytes": 100}]
    remaining, bos, calls = _run_verify(
        rows, {"manipulation/X/a/1.bin": 99}, monkeypatch)

    assert _cleared(calls) == []
    assert remaining == 1


def test_a_head_that_raises_keeps_its_row(monkeypatch):
    rows = [{"file_path": "a/1.bin", "size_bytes": 100}]
    remaining, bos, calls = _run_verify(
        rows, {"manipulation/X/a/1.bin": RuntimeError("BOS 503")}, monkeypatch)

    assert _cleared(calls) == []
    assert remaining == 1


def test_a_row_with_no_recorded_size_is_kept_unchecked(monkeypatch):
    """Without a size there is nothing to compare, so the row cannot be
    cleared safely — and there is no point spending the HEAD."""
    rows = [{"file_path": "a/1.bin", "size_bytes": 0}]
    remaining, bos, calls = _run_verify(
        rows, {"manipulation/X/a/1.bin": 100}, monkeypatch)

    assert bos.looked_up == []
    assert _cleared(calls) == []
    assert remaining == 1


def test_only_the_verified_paths_are_cleared(monkeypatch):
    rows = [
        {"file_path": "ok.bin", "size_bytes": 100},
        {"file_path": "short.bin", "size_bytes": 100},
        {"file_path": "gone.bin", "size_bytes": 100},
    ]
    remaining, bos, calls = _run_verify(rows, {
        "manipulation/X/ok.bin": 100,
        "manipulation/X/short.bin": 5,
    }, monkeypatch)

    assert _cleared(calls) == [["ok.bin"]]
    assert remaining == 2


def test_a_dataset_with_no_category_is_skipped_wholesale(monkeypatch):
    """`bos_target` silently drops a path segment for a falsy category
    (bos.py:29), so every HEAD would run against a prefix that is probably not
    this task's — and a coincidental hit there would clear a real record."""
    from dlm.temporal.models import TaskInput

    task = TaskInput(id="t-mf", name="X", repo_id="org/r", source="hf",
                     type="dataset", category="")
    rows = [{"file_path": "a/1.bin", "size_bytes": 100}]
    remaining, bos, calls = _run_verify(rows, {"X/a/1.bin": 100}, monkeypatch,
                                        task=task)

    assert bos.looked_up == []
    assert _cleared(calls) == []
    assert remaining == 1


def test_a_model_task_is_checked_even_with_no_category(monkeypatch):
    """Model prefixes are `{name}/` and never involve category, so the guard
    above must not sweep them up — a model task's rows would then never clear."""
    from dlm.temporal.models import TaskInput

    task = TaskInput(id="t-mf", name="Qwen", repo_id="org/r", source="hf",
                     type="model", category="")
    rows = [{"file_path": "a/1.bin", "size_bytes": 100}]
    remaining, bos, calls = _run_verify(rows, {"Qwen/a/1.bin": 100}, monkeypatch,
                                        task=task)

    assert bos.looked_up == ["Qwen/a/1.bin"]
    assert _cleared(calls) == [["a/1.bin"]]
    assert remaining == 0


@pytest.mark.parametrize("rows", [[], [{"file_path": "a/1.bin", "size_bytes": 100}]])
def test_the_ceiling_is_recorded_on_every_path(rows, monkeypatch):
    """Including the skip paths and the nothing-to-do path: T5 cannot tell
    "finalized with no losses" from "never finalized" without this write."""
    remaining, bos, calls = _run_verify(rows, {}, monkeypatch, limit=42)

    posts = [(u, b) for v, u, b in calls if v == "POST"]
    assert posts == [("http://s1:8080/api/tasks/t-mf/missing-limit", {"limit": 42})]


def test_no_rows_means_no_bos_calls_at_all(monkeypatch):
    remaining, bos, calls = _run_verify([], {}, monkeypatch)
    assert bos.looked_up == []
    assert remaining == 0
    assert [v for v, _, _ in calls] == ["GET", "POST"]


def test_the_returned_count_comes_from_the_database_not_arithmetic(monkeypatch):
    """The coordinator's verdict and the dashboard row must not be able to
    disagree, so the number returned is whatever SQLite says after the clear —
    not `len(rows) - len(present)` computed on the worker."""
    rows = [{"file_path": "a/1.bin", "size_bytes": 100}]

    from temporalio.testing import ActivityEnvironment

    from dlm.temporal import activities
    from dlm.temporal.models import TaskInput

    monkeypatch.setattr("requests.get", lambda url, timeout=None: _Resp(
        {"count": 1, "files": rows}))
    monkeypatch.setattr("requests.delete",
                        lambda url, json=None, timeout=None: _Resp(
                            {"ok": True, "cleared": 1, "remaining": 0}))
    # The DB has a row the worker never saw (another batch reported one while
    # this pass was running); the activity must report 7, not 0.
    monkeypatch.setattr("requests.post", lambda url, json=None, timeout=None: _Resp(
        {"ok": True, "limit": json["limit"], "missing_files_count": 7}))
    monkeypatch.setattr(
        "dlm.core.config.load_config",
        lambda: {"BAIDU_AK": "ak", "BAIDU_SK": "sk", "BOS_ENDPOINT": "https://x"})
    monkeypatch.setattr("dlm.core.bos.create_bos_client",
                        lambda ak, sk, endpoint: _Bos({"manipulation/X/a/1.bin": 100}))
    monkeypatch.setenv("DLM_COORDINATOR", "http://s1:8080")

    task = TaskInput(id="t-mf", name="X", repo_id="org/r", source="hf",
                     type="dataset", category="manipulation")
    env = ActivityEnvironment()
    assert asyncio.run(env.run(activities.verify_missing_files, task, 10)) == 7


def test_a_coordinator_error_body_is_raised_not_swallowed(monkeypatch):
    """A silent failure here means the ceiling never lands and the coordinator
    reports `done` off a count nobody can audit."""
    from temporalio.testing import ActivityEnvironment

    from dlm.temporal import activities
    from dlm.temporal.models import TaskInput

    monkeypatch.setattr("requests.get", lambda url, timeout=None: _Resp(
        {"count": 0, "files": []}))
    monkeypatch.setattr("requests.post", lambda url, json=None, timeout=None: _Resp(
        {"error": "no such task"}))
    monkeypatch.setenv("DLM_COORDINATOR", "http://s1:8080")

    task = TaskInput(id="t-mf", name="X", repo_id="org/r", source="hf",
                     type="dataset", category="manipulation")
    env = ActivityEnvironment()
    with pytest.raises(RuntimeError, match="no such task"):
        asyncio.run(env.run(activities.verify_missing_files, task, 10))


# ── T5: alerts and /api/doctor ──────────────────────────────────────────
#
# The record from T1/T4 is only half of "缺件不静默": an archive nobody is told
# to read is still silence. These pin the telling — and pin that it uses the
# SAME ceiling the verdict used, because a threshold that disagrees with the
# verdict is worse than none (it calls normal tasks critical and critical tasks
# normal, in that order).


def _finished(db, task_id="t-mf", status="done", count=0, limit=0,
             completed_at="2026-08-08T00:00:00+00:00"):
    _task(db, task_id, status=status)
    conn = db._conn()
    conn.execute(
        "UPDATE tasks SET missing_files_count = ?, missing_files_limit = ?, "
        "completed_at = ? WHERE id = ?",
        (count, limit, completed_at, task_id))
    conn.commit()
    return db.get_task(task_id)


def _alerts(tasks, now=None):
    from dlm.web.alerts import check_alerts
    return check_alerts(tasks=tasks, workers=[])


def _of_type(alerts, atype):
    return [a for a in alerts if a["type"] == atype]


def test_a_done_task_with_missing_files_warns(db):
    t = _finished(db, count=3, limit=10)
    out = _of_type(_alerts([t]), "missing_files")

    assert len(out) == 1
    assert out[0]["severity"] == "warning"
    assert out[0]["missing_files_count"] == 3
    assert "3 file(s)" in out[0]["message"]
    assert "/api/tasks/t-mf/missing-files" in out[0]["message"], \
        "an alert that does not say where to look is a dead end"


def test_over_the_ceiling_is_critical(db):
    t = _finished(db, status="failed", count=50, limit=10)
    out = _of_type(_alerts([t]), "missing_files_many")

    assert len(out) == 1
    assert out[0]["severity"] == "critical"
    assert "50 > 10" in out[0]["message"]


def test_a_task_never_gets_both_alerts(db):
    """Two alerts for one condition doubles every incident-log line and makes
    the dashboard count wrong."""
    t = _finished(db, status="failed", count=50, limit=10)
    out = _alerts([t])
    assert _of_type(out, "missing_files") == []


def test_the_ceiling_is_the_tasks_own_not_a_constant(db):
    """The failure this guards is subtle and two-directional. A hardcoded
    threshold (the original design said 100) would rate a 300-file task that
    lost 50 — already `failed` by T4 — as merely a WARNING, and a 5M-file task
    that lost 120 — entirely normal, reported `done` — as CRITICAL."""
    small = _finished(db, "t-small", status="failed", count=50, limit=10)
    huge = _finished(db, "t-huge", count=120, limit=25000)
    out = _alerts([small, huge])

    assert [a["task_id"] for a in _of_type(out, "missing_files_many")] == ["t-small"]
    assert [a["task_id"] for a in _of_type(out, "missing_files")] == ["t-huge"]


def test_a_count_equal_to_the_ceiling_is_only_a_warning(db):
    """`done` at exactly the ceiling is what T4 reports, so CRITICAL here would
    contradict the verdict on the boundary itself."""
    t = _finished(db, count=10, limit=10)
    out = _alerts([t])
    assert len(_of_type(out, "missing_files")) == 1
    assert _of_type(out, "missing_files_many") == []


def test_a_task_with_no_recorded_ceiling_warns_and_says_so(db):
    """`limit == 0` means the task never went through T4's finalize (an older
    row, or a re-check that never ran). Its count cannot be judged — but it can
    still be reported, and the alert must not silently imply it was judged."""
    t = _finished(db, count=999, limit=0)
    out = _of_type(_alerts([t]), "missing_files")

    assert len(out) == 1
    assert out[0]["severity"] == "warning"
    assert "no ceiling recorded" in out[0]["message"]
    assert _of_type(_alerts([t]), "missing_files_many") == []


def test_a_task_with_no_missing_files_is_silent(db):
    t = _finished(db, count=0, limit=10)
    out = _alerts([t])
    assert _of_type(out, "missing_files") == []
    assert _of_type(out, "missing_files_many") == []


@pytest.mark.parametrize("status", ["downloading", "pending"])
def test_an_unfinished_task_does_not_alert(status, db):
    """Rows accumulate while a task runs — every batch that gives up on a file
    writes one. Alerting mid-flight would fire on files the next round fetches."""
    t = _finished(db, status=status, count=5, limit=10)
    assert _of_type(_alerts([t]), "missing_files") == []


@pytest.mark.parametrize("status", ["paused", "preempted", "revoked", "skipped"])
def test_a_stopped_but_unfinalized_task_does_not_alert(status, db):
    """These are in TERMINAL_STATUSES but never reached _finalize: paused and
    preempted are this project's resumable states, and nobody wants the files
    of a revoked task. Gating on the broader set would alert on all four."""
    t = _finished(db, status=status, count=5, limit=10)
    out = _alerts([t])
    assert _of_type(out, "missing_files") == []
    assert _of_type(out, "missing_files_many") == []


def test_the_gate_uses_the_shared_finalized_status_set(db):
    """Pins the import, not a copy: a local tuple here would drift from
    fleet.py the first time someone adds a status."""
    import dlm.web.alerts as alerts_mod
    from dlm.web.fleet import FINALIZED_STATUSES

    assert FINALIZED_STATUSES == ("done", "failed")
    src = (alerts_mod.__file__ or "")
    with open(src) as fh:
        body = fh.read()
    assert "FINALIZED_STATUSES" in body
    assert 'status") not in ("done", "failed")' not in body


def test_an_old_loss_stops_warning_but_stays_on_the_record(db):
    """An alert that can never resolve accumulates until the list is unreadable.
    The WARNING has a shelf life; the record does not — it stays in the table,
    in /api/doctor, and in the transition line already written to the alert
    log."""
    import time as _time

    from dlm.web.alerts import MISSING_ALERT_WINDOW_S

    old = _time.time() - MISSING_ALERT_WINDOW_S - 3600
    from datetime import datetime, timezone
    stamp = datetime.fromtimestamp(old, timezone.utc).isoformat(timespec="seconds")
    t = _finished(db, count=3, limit=10, completed_at=stamp)

    assert _of_type(_alerts([t]), "missing_files") == []
    assert db.get_task("t-mf")["missing_files_count"] == 3


def test_an_old_loss_over_the_ceiling_still_alerts(db):
    """No window on the CRITICAL: it is rare by construction, and it means
    something bigger than a few dead upstream files went wrong."""
    t = _finished(db, status="failed", count=50, limit=10,
                  completed_at="2020-01-01T00:00:00+00:00")
    assert len(_of_type(_alerts([t]), "missing_files_many")) == 1


def test_an_unparseable_completion_time_keeps_the_warning(db):
    """Ambiguity must not buy silence — that is the failure mode."""
    t = _finished(db, count=3, limit=10, completed_at="not a timestamp")
    assert len(_of_type(_alerts([t]), "missing_files")) == 1


# ── T5: /api/doctor exposure ────────────────────────────────────────────


def _stub_doctor_temporal(monkeypatch, running=None):
    import dlm.web.temporal_client as tc

    async def fake_running(client=None):
        return running or {}

    monkeypatch.setattr(tc, "running_workflows", fake_running)


def test_doctor_reports_the_missing_files_of_finalized_tasks(db, monkeypatch):
    from dlm.web.routes import doctor

    _stub_doctor_temporal(monkeypatch)
    _finished(db, "t-few", count=3, limit=10)
    _finished(db, "t-many", status="failed", count=50, limit=10)
    _finished(db, "t-clean", count=0, limit=10)

    out = asyncio.run(doctor.diagnose())
    by_id = {m["task_id"]: m for m in out["missing_files"]}

    assert set(by_id) == {"t-few", "t-many"}
    assert by_id["t-few"]["over_ceiling"] is False
    assert by_id["t-many"]["over_ceiling"] is True
    assert by_id["t-many"]["missing_files_count"] == 50


def test_missing_files_never_make_the_doctor_unhealthy(db, monkeypatch):
    """`healthy` is scripts/deploy-workers.sh's deploy gate. A settled loss is
    permanent, so counting it as an issue would pin the gate red forever — one
    dead upstream file would block every future deploy. Notification is the
    alert engine's job; this section is the record."""
    from dlm.web.routes import doctor

    _stub_doctor_temporal(monkeypatch)
    _finished(db, "t-many", status="failed", count=5000, limit=10)

    out = asyncio.run(doctor.diagnose())

    assert out["missing_files"], "the loss must still be reported"
    assert out["total_issues"] == 0
    assert out["healthy"] is True


@pytest.mark.parametrize("status", ["downloading", "paused", "revoked"])
def test_doctor_ignores_tasks_that_never_finalized(status, db, monkeypatch):
    from dlm.web.routes import doctor

    _stub_doctor_temporal(monkeypatch)
    _finished(db, "t-mid", status=status, count=5, limit=0)

    out = asyncio.run(doctor.diagnose())
    assert out["missing_files"] == []


def test_doctor_reports_the_loss_with_no_time_limit(db, monkeypatch):
    """The alert WARNING expires after a week so the alert list stays readable;
    this section is where the record has to remain findable afterwards."""
    from dlm.web.routes import doctor

    _stub_doctor_temporal(monkeypatch)
    _finished(db, "t-old", count=3, limit=10,
              completed_at="2020-01-01T00:00:00+00:00")

    out = asyncio.run(doctor.diagnose())
    assert [m["task_id"] for m in out["missing_files"]] == ["t-old"]


def test_the_dashboard_carries_the_alert(db, monkeypatch):
    """The last hop, untested until now: check_alerts' output has to reach
    `summary["alerts"]`, which is what /api/dashboard serves and what the web UI
    renders. An alert the engine computes and nobody forwards is the same
    silence as no alert at all — and this covers every alert type, not just
    this feature's two."""
    from dlm.web import scheduler

    _finished(db, "t-dash", count=3, limit=10)

    summary = scheduler._build_dashboard()

    assert any(a["type"] == "missing_files" and a["task_id"] == "t-dash"
               for a in summary["alerts"])
