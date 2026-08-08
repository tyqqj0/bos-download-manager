"""Re-dispatching a task must clear the failure string from its last run.

Found in production: both pool tasks were retried through /api/queue/retry
after the chunk_filelist fix, came back up, and downloaded fine — while the
dashboard still showed `error = "Chunking failed: Activity task failed"` on a
row whose status was `downloading`. Both routes already *asked* for the clear
(`update_task_progress(..., error=None)`) and neither got it: every field in
that function is skipped when None, so `error=None` means "don't touch error",
not "empty it". The request was a silent no-op.

That is worse than a cosmetic bug. Monitoring this fleet means reading task
rows, and a healthy task wearing a dead run's error makes every reading of
that row wrong in the alarming direction — which is exactly the signal that
should be trustworthy right after a re-dispatch.

Run: pytest tests/test_stale_error_cleared.py -q
"""

from __future__ import annotations

import asyncio
import time

import pytest

from dlm.queue import snapshot
from dlm.web.routes import queue as queue_routes


@pytest.fixture(autouse=True)
def _no_temporal(monkeypatch):
    """retry/resume close the task's workflows before touching rows; that
    behaviour is pinned in test_dispatch_guards, not here."""
    async def fake_terminate(task_id, timeout_s=120):
        return True

    monkeypatch.setattr(
        "dlm.web.temporal_client.terminate_workflow_and_wait", fake_terminate)


def _failed_task(task_id, status="failed"):
    snapshot.upsert_task({
        "id": task_id, "name": "molmobot-data", "repo_id": "org/molmobot",
        "source": "modelscope", "type": "dataset", "category": "manipulation",
        "status": status, "server": "bj1", "priority": 0, "size_gb": 100.0,
        "downloaded_gb": 0.0, "progress_pct": 0, "speed_mbps": 0,
        "retry_count": 0, "max_workers": None,
        "error": "Chunking failed: Activity task failed",
        "error_class": "activity_error",
    })


def test_retry_clears_the_previous_runs_error(dlm_db):
    _failed_task("t-retry-err")

    result = asyncio.run(queue_routes.retry_task({"task_id": "t-retry-err"}))
    assert result.get("ok") is True, result

    row = snapshot.get_task("t-retry-err")
    assert row["status"] == "pending"
    assert not row["error"], f"stale error survived retry: {row['error']!r}"
    assert not row["error_class"], row["error_class"]


def test_resume_clears_the_previous_runs_error(dlm_db):
    _failed_task("t-resume-err", status="paused")

    result = asyncio.run(queue_routes.resume_task({"task_id": "t-resume-err"}))
    assert result.get("ok") is True, result

    row = snapshot.get_task("t-resume-err")
    assert row["status"] == "pending"
    assert not row["error"], f"stale error survived resume: {row['error']!r}"
    assert not row["error_class"], row["error_class"]


def test_error_none_is_still_a_no_op(dlm_db):
    """Pin the trap itself, so nobody 'fixes' it into a silent data loser.

    `error=None` MUST keep meaning "leave error alone" — every worker progress
    report calls update_task_progress without passing error, and if None
    started clearing, the first heartbeat after a real failure would erase the
    only record of why the task died.
    """
    _failed_task("t-noop")

    snapshot.update_task_progress("t-noop", progress_pct=50, error=None)

    row = snapshot.get_task("t-noop")
    assert row["error"] == "Chunking failed: Activity task failed"
    assert row["error_class"] == "activity_error"


def test_clear_error_together_with_an_error_is_rejected(dlm_db):
    """Set-and-clear in one call has no correct answer; refuse it loudly."""
    _failed_task("t-both")

    with pytest.raises(ValueError):
        snapshot.update_task_progress("t-both", clear_error=True, error="boom")
    with pytest.raises(ValueError):
        snapshot.update_task_progress(
            "t-both", clear_error=True, error_class="x")

    row = snapshot.get_task("t-both")
    assert row["error"] == "Chunking failed: Activity task failed"


def test_clear_error_leaves_the_other_columns_alone(dlm_db):
    """The clear rides along with a status/phase update — it must not become a
    blanket reset of the row it is attached to."""
    _failed_task("t-narrow")
    snapshot.update_task_progress("t-narrow", downloaded_gb=42.0)

    snapshot.update_task_progress(
        "t-narrow", status="pending", phase="retrying", clear_error=True)

    row = snapshot.get_task("t-narrow")
    assert not row["error"]
    assert row["downloaded_gb"] == 42.0
    assert row["phase"] == "retrying"


# ── The completion path ──────────────────────────────────────────────────
#
# Clearing at re-dispatch fixes tasks retried from now on. It does nothing for
# a row already carrying a stale error into a run that is going to succeed —
# and both live pool tasks are in exactly that position, having been retried
# before the fix. So the terminal report clears it too.

def _report(task_id, body):
    from dlm.web.routes import servers as servers_routes
    return asyncio.run(servers_routes.task_progress({"task_id": task_id, **body}))


def test_done_report_clears_an_error_from_an_earlier_run(dlm_db):
    _failed_task("t-done-clean", status="downloading")

    _report("t-done-clean", {"status": "done", "progress_pct": 100})

    row = snapshot.get_task("t-done-clean")
    assert row["status"] == "done"
    assert not row["error"], f"`done` row kept a dead run's error: {row['error']!r}"
    assert not row["error_class"]


def test_done_report_with_its_own_error_keeps_it(dlm_db):
    """T4 reports `done` for a task that forgave a few missing files, and may
    say so in error. The clear must not swallow the current run's own note."""
    _failed_task("t-done-warn", status="downloading")

    _report("t-done-warn", {"status": "done", "error": "3 file(s) missing"})

    row = snapshot.get_task("t-done-warn")
    assert row["status"] == "done"
    assert row["error"] == "3 file(s) missing"


def test_failed_report_without_an_error_keeps_the_old_reason(dlm_db):
    """A `failed` row with an empty error tells an operator nothing. A stale
    reason is at least directionally true, so failed does NOT clear."""
    _failed_task("t-failed-quiet", status="downloading")

    _report("t-failed-quiet", {"status": "failed"})

    row = snapshot.get_task("t-failed-quiet")
    assert row["status"] == "failed"
    assert row["error"] == "Chunking failed: Activity task failed"


def test_completion_still_keeps_the_phase_note(dlm_db):
    """Guard the GAP-3 fix the clear_error argument was threaded through."""
    _failed_task("t-phase", status="downloading")

    _report("t-phase", {"status": "done", "phase": "3 file(s) missing — GET ..."})

    row = snapshot.get_task("t-phase")
    assert row["phase"] == "3 file(s) missing — GET ..."
    assert not row["error"]
