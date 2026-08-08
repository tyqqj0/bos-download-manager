"""T5 — PoolDownloadWorkflow: determinism replay + window-loop behavior.

Two kinds of test live here, and the first is the load-bearing one:

  1. **G1 evidence (A9).** Real histories pulled off S1 — including the
     Egocentric-100K coordinator and one of its shards, which were running
     when this branch was written and must survive its deploy — are replayed
     against the *current* workflow definitions. A NonDeterminismError here
     means deploying this branch would break a live workflow on the next
     worker restart. This is the only machine-checkable proof that G1 holds;
     reading the diff cannot establish it.

  2. **Pool loop behavior.** The window loop's four terminal shapes, the
     window=1 boundary, and the shielded cancellation cleanup, driven through
     a real workflow environment with mocked activities.

Replay is run twice under different PYTHONHASHSEED values (see
`test_replay_is_hash_seed_independent`): set iteration order is
hash-seed-dependent, so a single run can miss exactly the class of
nondeterminism the pool loop's sorted traversal exists to prevent.

Run: pytest tests/test_pool_workflow_replay.py -q   (needs temporalio)
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from temporalio.client import WorkflowFailureError
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from dlm.temporal.models import TaskInput, TaskResult
from dlm.temporal.workflows import (
    PoolDownloadWorkflow,
    ShardedDownloadWorkflow,
    ShardWorkerWorkflow,
)

HISTORY_DIR = Path(__file__).parent / "fixtures" / "histories"


# ── 1. G1 / A9: real S1 histories must still replay ─────────────────


def _history_files():
    return sorted(HISTORY_DIR.glob("*.json"))


def test_history_fixtures_are_present():
    """A silently-empty fixture dir would make the replay tests below pass
    while proving nothing — the failure mode that makes determinism suites
    worthless."""
    files = _history_files()
    assert files, f"no history fixtures in {HISTORY_DIR}"
    # The Egocentric-100K coordinator specifically: it was mid-download when
    # this branch was written and A0 forbids breaking it.
    assert any("t-20260714-b47806" in f.name for f in files), \
        "the Egocentric-100K history is the one fixture A0 requires"


@pytest.mark.parametrize("history_file", _history_files(), ids=lambda p: p.stem)
def test_real_history_replays_against_current_definitions(history_file):
    """Replay one real S1 history against today's workflow code."""
    payload = json.loads(history_file.read_text())
    replayer = Replayer(
        workflows=[
            ShardedDownloadWorkflow,
            ShardWorkerWorkflow,
            PoolDownloadWorkflow,
        ],
    )

    async def go():
        from temporalio.client import WorkflowHistory

        history = WorkflowHistory.from_json(history_file.stem, payload)
        await replayer.replay_workflow(history)

    asyncio.run(go())


def test_replay_is_hash_seed_independent():
    """Run the replay suite again in a subprocess under a different
    PYTHONHASHSEED.

    Set and dict-of-object iteration order depends on the hash seed, so a
    nondeterministic traversal (the bug `asyncio.wait`'s real sets would
    introduce) can replay cleanly under one seed and fail under another. One
    in-process run cannot see that; two seeds can.
    """
    for seed in ("0", "12345"):
        env = dict(os.environ, PYTHONHASHSEED=seed)
        proc = subprocess.run(
            [sys.executable, "-m", "pytest", __file__,
             "-k", "test_real_history_replays_against_current_definitions",
             "-q", "-p", "no:cacheprovider"],
            env=env, capture_output=True, text=True,
            cwd=Path(__file__).parent.parent,
        )
        assert proc.returncode == 0, (
            f"replay failed under PYTHONHASHSEED={seed}:\n"
            f"{proc.stdout[-3000:]}\n{proc.stderr[-2000:]}"
        )


# ── 2. Pool window loop behavior ────────────────────────────────────


def _task_input(priority=5, source="hf"):
    return TaskInput(id="t-pool-1", name="pool-task", repo_id="org/repo",
                     source=source, type="dataset", category="manipulation",
                     priority=priority)


class _PoolActivityStubs:
    """Stand-ins for every activity PoolDownloadWorkflow calls.

    Batch outcomes are scripted per (batch_index, attempt) so a test can make
    a batch fail its first dispatch round and succeed on the re-dispatch —
    the poison-worker case rule 8 exists for.
    """

    def __init__(self, num_batches=4, fail_always=(), fail_first_round=(),
                 windows=None, remaining_files=10, listing_queue="pool-test",
                 batch_seconds=0.05, missing_after_verify=0,
                 verify_raises=False):
        # The workflow pins filter/chunk to the listing worker's personal
        # queue (those activities read the filelist off its local disk), so
        # the stub must name a queue this test's worker actually polls —
        # otherwise they queue forever on a queue nobody serves.
        self.listing_queue = listing_queue
        self.batch_seconds = batch_seconds
        self.num_batches = num_batches
        self.fail_always = set(fail_always)
        self.fail_first_round = set(fail_first_round)
        self.windows = list(windows or [])
        self.remaining_files = remaining_files
        self.missing_after_verify = missing_after_verify
        self.verify_raises = verify_raises
        self.batch_calls = []          # (batch_index,) in dispatch order
        self.tolerated = []            # (batch_index, tolerate_missing) per call
        self.verified = []             # limits passed to verify_missing_files
        self.verify_calls = []         # (limit, recheck) per call
        self.recorded = []             # results lists passed to bookkeeping
        self.released = []             # task_ids passed to release
        self.dashboard = []            # (status, phase)
        self.max_concurrent = 0
        self._live = 0
        self._seen_batches = set()

    def activities(self):
        """Bound methods can't carry Temporal's activity metadata, so each is
        wrapped in a freshly-named function registered under the activity name
        the workflow calls by string."""
        from temporalio import activity

        def _stub(name, fn):
            @activity.defn(name=name)
            async def _impl(*args):
                return await fn(*args)
            return _impl

        return [
            _stub("list_repo_files", self.list_repo_files),
            _stub("filter_filelist_against_bos", self.filter_filelist_against_bos),
            _stub("report_resume_info", self.report_resume_info),
            _stub("chunk_filelist", self.chunk_filelist),
            _stub("create_pool_batches_in_db", self.create_pool_batches_in_db),
            _stub("run_pool_batch", self.run_pool_batch),
            _stub("record_batches_and_window", self.record_batches_and_window),
            _stub("release_pool_batches", self.release_pool_batches),
            _stub("verify_missing_files", self.verify_missing_files),
            _stub("aggregate_task_from_shards", self.aggregate_task_from_shards),
            _stub("report_to_dashboard", self.report_to_dashboard),
        ]

    # -- pre-loop steps ------------------------------------------------

    async def list_repo_files(self, task_input: TaskInput) -> dict:
        return {"path": "/tmp/fl.json", "count": 100, "total_bytes": 1000,
                "worker_queue": self.listing_queue}

    async def filter_filelist_against_bos(self, filelist_path: str,
                                          task_input: TaskInput) -> dict:
        return {"filtered_path": "/tmp/fl.filtered.json",
                "skipped_files": 3, "skipped_bytes": 300,
                "remaining_files": self.remaining_files, "remaining_bytes": 700}

    async def report_resume_info(self, task_id: str, skipped_files: int,
                                 skipped_gb: float):
        return None

    async def chunk_filelist(self, filelist_path: str, task_input: TaskInput) -> dict:
        n = self.num_batches
        return {"batch_keys": [f"batchlists/pool-task/batch-{i}.json" for i in range(n)],
                "counts": [5] * n, "bytes": [100] * n}

    async def create_pool_batches_in_db(self, task_id: str, batch_infos: list) -> dict:
        return {"ignored": False,
                "shard_ids": [f"s-{task_id}-{i}" for i in range(len(batch_infos))]}

    # -- the loop ------------------------------------------------------

    async def run_pool_batch(self, task_input: TaskInput, batch_index: int,
                             filelist_key: str, min_free_gb=None,
                             tolerate_missing=False) -> dict:
        self.batch_calls.append(batch_index)
        self.tolerated.append((batch_index, tolerate_missing))
        self._live += 1
        self.max_concurrent = max(self.max_concurrent, self._live)
        try:
            await asyncio.sleep(self.batch_seconds)
            first_round = batch_index not in self._seen_batches
            self._seen_batches.add(batch_index)
            if batch_index in self.fail_always or (
                    first_round and batch_index in self.fail_first_round):
                raise ApplicationError(f"batch {batch_index} boom",
                                       non_retryable=True)
            return {"ignored": False, "uploaded_files": 5,
                    "uploaded_bytes": 100, "skipped_files": 0}
        finally:
            self._live -= 1

    async def record_batches_and_window(self, task_id: str, results: list) -> dict:
        self.recorded.append(results)
        window = self.windows.pop(0) if self.windows else 2
        return {"window": window, "p": 7, "weight": 1.0, "weight_sum": 1.0}

    async def release_pool_batches(self, task_id: str) -> int:
        self.released.append(task_id)
        return 3

    async def verify_missing_files(self, task_input: TaskInput, limit: int,
                                  recheck: bool = True) -> int:
        self.verified.append(limit)
        self.verify_calls.append((limit, recheck))
        if self.verify_raises:
            raise ApplicationError("coordinator unreachable", non_retryable=True)
        return self.missing_after_verify

    async def aggregate_task_from_shards(self, task_id: str):
        return None

    async def report_to_dashboard(self, task_id, status, phase=None, progress=None,
                                  speed=None, downloaded_gb=None, extra=None,
                                  error=None):
        self.dashboard.append((status, phase, error))
        return None


async def _run_pool_workflow(stubs, task_input=None, cancel_after=None):
    """Drive PoolDownloadWorkflow in a real (time-skipping) environment."""
    task_input = task_input or _task_input()
    # Two workers, because the workflow deliberately routes to two queues: the
    # coordinator and the filelist-pinned activities on the listing worker's
    # own queue, and run_pool_batch on the shared per-source pool queue.
    pool_queue = f"pool-{'ms' if task_input.source == 'modelscope' else 'hf'}"
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="pool-test",
            workflows=[PoolDownloadWorkflow],
            activities=stubs.activities(),
        ), Worker(
            env.client,
            task_queue=pool_queue,
            activities=stubs.activities(),
        ):
            handle = await env.client.start_workflow(
                PoolDownloadWorkflow.run,
                task_input,
                id=f"pool-{task_input.id}",
                task_queue="pool-test",
            )
            if cancel_after is not None:
                await asyncio.sleep(cancel_after)
                await handle.cancel()
            return await handle.result()


def test_all_batches_done_reports_done_with_totals():
    stubs = _PoolActivityStubs(num_batches=4)
    result = asyncio.run(_run_pool_workflow(stubs))

    assert result.status == "done"
    assert result.files_uploaded == 20      # 4 batches x 5
    assert result.bytes_uploaded == 400
    assert sorted(stubs.batch_calls) == [0, 1, 2, 3]
    # every batch's terminal row was recorded, exactly once
    recorded = [r["batch_index"] for rs in stubs.recorded for r in rs]
    assert sorted(recorded) == [0, 1, 2, 3]
    assert all(r["status"] == "done" for rs in stubs.recorded for r in rs)


def test_window_of_one_serializes_batches():
    """Window=1 is the boundary the `max(1, ...)` floor produces for a task
    that is outvoted: it must still drain, one batch at a time."""
    stubs = _PoolActivityStubs(num_batches=3, windows=[1, 1, 1, 1])
    result = asyncio.run(_run_pool_workflow(stubs))

    assert result.status == "done"
    assert stubs.max_concurrent == 1
    assert stubs.batch_calls == [0, 1, 2]   # in order, never overlapping


def test_recomputed_window_is_honored():
    """The companion to the window=1 test, which on its own would pass even if
    the recomputed window were ignored (1 is also the initial value). Here the
    first wake widens the window, and concurrency must follow."""
    stubs = _PoolActivityStubs(num_batches=6, windows=[3, 3, 3, 3, 3, 3],
                               batch_seconds=0.4)
    result = asyncio.run(_run_pool_workflow(stubs))

    assert result.status == "done"
    assert stubs.max_concurrent == 3, \
        "the window returned by the bookkeeping activity was not applied"


def test_failed_batch_is_redispatched_once_and_can_succeed():
    """Rule 8: a batch whose attempts were eaten by one bad worker gets a
    whole second dispatch round before the task is called failed."""
    stubs = _PoolActivityStubs(num_batches=3, fail_first_round=[1])
    result = asyncio.run(_run_pool_workflow(stubs))

    assert result.status == "done"
    assert stubs.batch_calls.count(1) == 2          # dispatched twice
    statuses = [(r["batch_index"], r["status"]) for rs in stubs.recorded for r in rs]
    assert (1, "failed") in statuses and (1, "done") in statuses
    assert any("retrying 1 failed batches" == phase
               for _, phase, _ in stubs.dashboard if phase)


def test_batch_failing_both_rounds_fails_the_task():
    stubs = _PoolActivityStubs(num_batches=3, fail_always=[2])
    result = asyncio.run(_run_pool_workflow(stubs))

    assert result.status == "failed"
    assert "1/3 batches failed after retry" in result.error
    assert stubs.batch_calls.count(2) == 2
    assert ("failed", None, result.error) in stubs.dashboard
    # The two healthy batches' bytes are real and on BOS; a failed result that
    # reported zero would disagree with the aggregate and with the bucket.
    assert result.files_uploaded == 10      # 2 batches x 5
    assert result.bytes_uploaded == 200


def test_cancellation_releases_batch_rows():
    """A paused task must leave no row claiming a worker that has stopped.

    Scope note: this verifies the cleanup activity RUNS on cancellation. It
    does NOT prove the `asyncio.shield` around it is load-bearing — the test
    passes with the shield removed, because this harness lets a post-cancel
    activity through. The shield stays because it is the SDK's documented
    pattern for exactly this (and design rule 6 requires it); on a real
    server a cancelled workflow's unshielded cleanup activity is itself
    cancelled. Proving that needs a cluster, i.e. T10.
    """
    # Batches run long enough that the cancel lands mid-flight — cancelling
    # after the loop has already drained would prove nothing.
    stubs = _PoolActivityStubs(num_batches=6, windows=[2] * 6, batch_seconds=2.0)

    with pytest.raises(WorkflowFailureError):
        asyncio.run(_run_pool_workflow(stubs, cancel_after=0.8))

    assert stubs.released == ["t-pool-1"], \
        "release_pool_batches must run exactly once on cancellation"


def test_ignored_batch_create_stops_without_overwriting_status():
    """An operator paused the task while we were listing: the batch-create
    endpoint returns ignored, and the workflow must not report a status that
    would fight what the operator set."""
    stubs = _PoolActivityStubs(num_batches=2)

    async def ignored_create(task_id: str, batch_infos: list) -> dict:
        return {"ignored": True, "shard_ids": []}

    stubs.create_pool_batches_in_db = ignored_create
    result = asyncio.run(_run_pool_workflow(stubs))

    assert result.status == "paused"
    assert stubs.batch_calls == []
    assert not any(s in ("done", "failed") for s, _, _ in stubs.dashboard)


def test_empty_remaining_after_filter_completes_without_batches():
    stubs = _PoolActivityStubs(num_batches=0, remaining_files=0)
    result = asyncio.run(_run_pool_workflow(stubs))

    assert result.status == "done"
    assert stubs.batch_calls == []
    assert any(s == "done" for s, _, _ in stubs.dashboard)


def test_batch_reporting_ignored_stops_the_loop_without_claiming_done():
    """An operator pauses mid-window: run_pool_batch finds the parent task
    terminal and declines. Draining the rest one `ignored` at a time would
    cost a wake each and end by reporting `done` for a task that downloaded
    almost nothing — and would skip the release, leaving rows still claiming
    workers that have stopped."""
    # window 2 so batch 0 (a genuine finisher) and batch 1 (ignored) land in
    # the same wake — that is the path that records real results *and* stops.
    stubs = _PoolActivityStubs(num_batches=6, windows=[2] * 8)

    real_batch = stubs.run_pool_batch

    async def stop_after_first(task_input, batch_index, filelist_key,
                               min_free_gb=None, tolerate_missing=False):
        if batch_index == 0:
            return await real_batch(task_input, batch_index, filelist_key,
                                    min_free_gb, tolerate_missing)
        stubs.batch_calls.append(batch_index)
        return {"ignored": True}

    stubs.run_pool_batch = stop_after_first
    result = asyncio.run(_run_pool_workflow(stubs))

    assert result.status == "paused"
    # Stopped early rather than walking all six. Asserted as a bound, not an
    # exact list: with a window of 2 the order in which two activities record
    # their start is real-time dependent.
    assert len(stubs.batch_calls) < 6
    assert 5 not in stubs.batch_calls
    # batch 0 genuinely finished, so its terminal row was still recorded
    assert 0 in [r["batch_index"] for rs in stubs.recorded for r in rs]
    # nothing claimed a terminal task state on the operator's behalf
    assert not any(s in ("done", "failed") for s, _, _ in stubs.dashboard)
    # and the rows that still claimed a worker were released
    assert stubs.released == ["t-pool-1"]


def test_bookkeeping_failure_fails_the_task_instead_of_stranding_it():
    """S1 unreachable past the retry policy must not let the ActivityError
    escape the workflow: that cancels in-flight batches, writes no terminal
    report, and leaves the task row saying `downloading` behind a dead
    workflow."""
    stubs = _PoolActivityStubs(num_batches=3)

    async def always_fails(task_id, results):
        raise ApplicationError("S1 unreachable", non_retryable=True)

    stubs.record_batches_and_window = always_fails
    result = asyncio.run(_run_pool_workflow(stubs))

    assert result.status == "failed"
    assert "pool dispatch failed" in result.error
    assert any(s == "failed" for s, _, _ in stubs.dashboard)


# ── 3. T4: honest finalize (missing-file ceiling) ────────────────────
#
# The failure mode these exist for is narrow and invisible: a task reports
# `done` while files it listed are not on BOS, and nothing anywhere says so.
# Every test below is either "the verdict follows the ceiling" or "the
# re-check actually ran on this path".


def test_the_last_round_is_the_only_one_that_tolerates_missing_files():
    """Round 1 must NOT forgive: a batch that failed here still gets a whole
    second dispatch round, which is what cures a poisoned worker. Only that
    second round may write a file off as permanently missing."""
    stubs = _PoolActivityStubs(num_batches=3, fail_first_round=[1])
    result = asyncio.run(_run_pool_workflow(stubs))

    assert result.status == "done"
    assert [(i, t) for i, t in stubs.tolerated if t] == [(1, True)], \
        "only the re-dispatch round may write a file off as permanently missing"
    # batch 1 is dispatched twice: intolerant first, tolerant on the retry
    assert [t for i, t in stubs.tolerated if i == 1] == [False, True]


def test_a_task_with_no_redispatch_round_never_tolerates():
    stubs = _PoolActivityStubs(num_batches=4)
    asyncio.run(_run_pool_workflow(stubs))
    assert stubs.tolerated == [(i, False) for i, _ in stubs.tolerated]


def test_missing_files_within_the_ceiling_still_report_done():
    """The 2026-08-08 semantics: a few permanently-missing files are normal on
    this fleet, so the task completes (and the transfer that follows can run).
    Honesty is carried by the record and the phase text, not by refusing to
    finish."""
    stubs = _PoolActivityStubs(num_batches=2, missing_after_verify=3)
    result = asyncio.run(_run_pool_workflow(stubs))

    assert result.status == "done"
    done_phases = [p for s, p, _ in stubs.dashboard if s == "done"]
    assert done_phases and "3 file(s) missing" in done_phases[0]
    assert "missing-files" in done_phases[0], \
        "the phase must say where to look, not just that something is missing"


def test_missing_files_over_the_ceiling_fail_the_task():
    """The ceiling is what keeps per-batch tolerance from adding up to a
    task-level lie (5 forgiven files × 1500 batches is not "a few")."""
    stubs = _PoolActivityStubs(num_batches=2, missing_after_verify=999)
    result = asyncio.run(_run_pool_workflow(stubs))

    assert result.status == "failed"
    assert "999 missing file(s) over this task's ceiling" in result.error
    assert "missing-files" in result.error


def test_the_ceiling_comes_from_the_listing_count():
    """`task_missing_limit` is fed the LISTING count (100 in these stubs), not
    the post-resume-filter count — on a task resuming at 99% the latter would
    shrink the ratio term to nothing."""
    from dlm.temporal.models import task_missing_limit

    stubs = _PoolActivityStubs(num_batches=2)
    asyncio.run(_run_pool_workflow(stubs))

    assert stubs.verified == [task_missing_limit(100)]


def test_a_failed_recheck_never_reports_done():
    """No verdict is possible without the archive, and `done` would be exactly
    the silent report this task exists to prevent. Failing costs a retry whose
    resume filter skips everything already uploaded; it does not cost data."""
    stubs = _PoolActivityStubs(num_batches=2, verify_raises=True)
    result = asyncio.run(_run_pool_workflow(stubs))

    assert result.status == "failed"
    assert "missing-file re-check failed" in result.error
    assert not any(s == "done" for s, _, _ in stubs.dashboard)


def test_a_task_that_fails_on_batches_still_records_its_missing_files():
    """M>0 fails regardless of N — but the re-check still runs, so the ceiling
    lands in the task row and the archive is pruned of files a later round
    actually uploaded. Alerting has nothing else to work from."""
    stubs = _PoolActivityStubs(num_batches=3, fail_always=[2],
                               missing_after_verify=4)
    result = asyncio.run(_run_pool_workflow(stubs))

    assert result.status == "failed"
    assert "1/3 batches failed after retry" in result.error
    assert "4 file(s) recorded missing" in result.error
    assert len(stubs.verified) == 1


@pytest.mark.parametrize("kwargs,expected_phase", [
    ({"num_batches": 0, "remaining_files": 0}, "all 3 files already on BOS"),
    ({"num_batches": 0, "remaining_files": 5}, "nothing left to download"),
])
def test_the_trivial_done_paths_go_through_the_recheck_too(kwargs, expected_phase):
    """These three paths cannot have missing files today, so routing them
    through the re-check buys nothing now — it buys that the next person who
    adds an early `return done` does not have to know about any of this. A
    `done` reported from a branch that skipped the re-check is the exact
    failure mode, and it is invisible when it happens."""
    stubs = _PoolActivityStubs(**kwargs)
    result = asyncio.run(_run_pool_workflow(stubs))

    assert result.status == "done"
    assert len(stubs.verified) == 1
    assert any(p and expected_phase in p for _, p, _ in stubs.dashboard)


def test_the_empty_repo_path_goes_through_the_recheck_too():
    stubs = _PoolActivityStubs(num_batches=0)

    async def empty_listing(task_input):
        return {"path": "/tmp/fl.json", "count": 0, "total_bytes": 0,
                "worker_queue": stubs.listing_queue}

    stubs.list_repo_files = empty_listing
    result = asyncio.run(_run_pool_workflow(stubs))

    assert result.status == "done"
    assert len(stubs.verified) == 1
    assert any(p == "empty repo" for _, p, _ in stubs.dashboard)


# ── the recheck flag (review GAP-1) ────────────────────────────────────────
#
# The BOS scan exists to stop a task reporting `failed` over files that are
# actually in the bucket. A task already condemned by a failed batch has no
# verdict left to change, and its archive is the one large enough to make the
# scan expensive — so that path records the ceiling and skips the scan.


def test_a_task_headed_for_done_still_scans_bos():
    from dlm.temporal.models import task_missing_limit

    stubs = _PoolActivityStubs(num_batches=2)
    asyncio.run(_run_pool_workflow(stubs))

    assert stubs.verify_calls == [(task_missing_limit(100), True)]


def test_a_task_condemned_by_a_failed_batch_skips_the_scan():
    """It still needs the count for its error message and the ceiling for
    alerting — that is bookkeeping, not verification."""
    from dlm.temporal.models import task_missing_limit

    stubs = _PoolActivityStubs(num_batches=3, fail_always=[2],
                               missing_after_verify=4)
    result = asyncio.run(_run_pool_workflow(stubs))

    assert result.status == "failed"
    assert stubs.verify_calls == [(task_missing_limit(100), False)]


def test_the_batch_failure_survives_a_broken_bookkeeping_call():
    """When the bookkeeping POST itself fails on an already-doomed task, the
    error the dashboard shows must still name the real cause. Reporting
    "missing-file re-check failed" there would send the operator to look at
    dead upstream files instead of the batch that actually broke."""
    stubs = _PoolActivityStubs(num_batches=3, fail_always=[2],
                               verify_raises=True)
    result = asyncio.run(_run_pool_workflow(stubs))

    assert result.status == "failed"
    assert "1/3 batches failed after retry" in result.error
    assert "missing-file count unavailable" in result.error
    assert f"/api/tasks/{_task_input().id}/missing-files" in result.error


def test_verify_missing_files_is_registered_on_the_workers():
    """It is called only at the very end of a pool task, so an unregistered
    activity would surface hours in, on the one step that decides whether the
    task tells the truth."""
    from dlm.temporal.__main__ import ACTIVITIES

    names = [getattr(a, "__temporal_activity_definition").name for a in ACTIVITIES]
    assert "verify_missing_files" in names
