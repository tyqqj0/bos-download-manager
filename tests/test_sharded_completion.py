"""ShardedDownloadWorkflow step 8 — a task is `done` only if every shard says so.

The bug these tests pin down, observed in production on 2026-08-06:
t-20260805-460d45 (molmobot-data) was reported `done` with downloaded_gb=0.0
of 9611 GB while its single shard row read `failed` at 0/4765 files.

Mechanism: ShardWorkerWorkflow does not raise when it gives up. Insufficient
disk (workflows.py:415) and batch failure (workflows.py:525) are NORMAL
RETURNS carrying `ShardResult(status="failed")`. The coordinator's result loop
matched `isinstance(result, ShardResult)` and summed its counters without ever
reading `.status` — and `ShardResult.status` defaults to "done", so the type
alone never distinguished the two. `failed_shards` stayed empty and the task
was reported `done`.

These tests drive the REAL ShardWorkerWorkflow (not a fake child) through the
real disk-preflight path, so they exercise the actual mechanism rather than a
reconstruction of it. Everything runs on one task queue: the coordinator pins
some activities to the listing worker's queue and starts children on
`download-{worker_key}`, so the stubs name a single queue this test's worker
actually polls.

Run: pytest tests/test_sharded_completion.py -q   (needs temporalio)
"""

from __future__ import annotations

import asyncio
import uuid

import pytest

from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from dlm.temporal.models import TaskInput
from dlm.temporal.workflows import ShardedDownloadWorkflow, ShardWorkerWorkflow

# The child's queue is f"download-{worker_key}" (workflows.py:712), so the
# worker key and the queue name have to agree with each other.
WORKER_KEY = "testw"
QUEUE = f"download-{WORKER_KEY}"


class _Stubs:
    """Stand-ins for every activity the coordinator and its children call.

    `disk_ok` is what drives the case under test: False sends the real
    ShardWorkerWorkflow down its `return ShardResult(status="failed")` path
    without raising, which is exactly how molmobot-data's shard ended.
    """

    def __init__(self, num_shards=1, disk_ok=True, disk_ok_per_shard=None,
                 remaining_files=10, drop_files=0, extra_partitions=0):
        self.num_shards = num_shards
        self.disk_ok = disk_ok
        # shard_index -> bool, for the mixed case
        self.disk_ok_per_shard = dict(disk_ok_per_shard or {})
        # How many files the BOS filter leaves, and how badly the partition
        # under-reports them — see the coverage tests.
        self.remaining_files = remaining_files
        self.drop_files = drop_files
        self.extra_partitions = extra_partitions
        self.dashboard = []        # (status, phase) in call order
        self.shard_status = []     # (shard_id, status, error)
        self.aggregated = []
        self.partition_calls = []  # args, so the task_id keying is checkable

    # -- registration --------------------------------------------------

    def activities(self):
        """Bound methods cannot carry Temporal's activity metadata, so each is
        wrapped in a freshly-named function registered under the activity name
        the workflow calls by string."""

        def _stub(name, fn):
            @activity.defn(name=name)
            async def _impl(*args):
                return await fn(*args)
            return _impl

        return [_stub(name, getattr(self, name)) for name in (
            # coordinator
            "list_repo_files", "filter_filelist_against_bos",
            "report_resume_info", "query_idle_workers",
            "partition_files_greedy", "create_shards_in_db",
            "report_to_dashboard", "aggregate_task_from_shards",
            # child + shared
            "assign_shard_server", "update_shard_status", "check_disk_space",
            "load_progress", "download_shard_filelist", "read_filelist",
            "run_pipeline_batch", "save_progress", "report_shard_progress",
            "clear_progress", "cleanup_staging",
        )]

    # -- coordinator ---------------------------------------------------

    async def list_repo_files(self, task_input):
        return {"path": "/tmp/fl.json", "count": 10,
                "total_bytes": 100 * 1024 ** 3, "worker_queue": QUEUE}

    async def filter_filelist_against_bos(self, filelist_path, task_input):
        return {"filtered_path": "/tmp/fl.filtered.json",
                "skipped_files": 0, "skipped_bytes": 0,
                "remaining_files": self.remaining_files,
                "remaining_bytes": 100 * 1024 ** 3}

    async def report_resume_info(self, task_id, skipped_files, skipped_gb):
        return None

    async def query_idle_workers(self, source, exclude_task):
        return [WORKER_KEY] * self.num_shards

    async def partition_files_greedy(self, filtered_path, num_shards, staging_dir,
                                     task_id=""):
        # `total_files` is the real activity's key, and what /api/shards/create
        # reads (queue.py: info["total_files"]). The stub said "count", which no
        # production path uses — so the coordinator's coverage check saw zero
        # files partitioned. A stub that disagrees with the contract tests the
        # stub.
        self.partition_calls.append(
            {"filtered_path": filtered_path, "num_shards": num_shards,
             "staging_dir": staging_dir, "task_id": task_id})

        counts = [self.remaining_files // num_shards] * num_shards
        for i in range(self.remaining_files % num_shards):
            counts[i] += 1
        if self.drop_files:
            counts[-1] = max(0, counts[-1] - self.drop_files)
        counts += [0] * self.extra_partitions

        return [{"filelist_key": f"batchlists/x/shard-{i}.json",
                 "filelist_md5": "abc",
                 "total_files": c,
                 "total_bytes": 50 * 1024 ** 3} for i, c in enumerate(counts)]

    async def create_shards_in_db(self, task_id, partitions):
        return [f"s-{task_id}-{i}" for i in range(len(partitions))]

    async def report_to_dashboard(self, task_id, status, phase=None, *rest):
        self.dashboard.append((status, phase))
        return None

    async def aggregate_task_from_shards(self, task_id):
        self.aggregated.append(task_id)
        return None

    # -- child ---------------------------------------------------------

    async def assign_shard_server(self, shard_id, server_key):
        return None

    async def update_shard_status(self, shard_id, status, error=None):
        self.shard_status.append((shard_id, status, error))
        return None

    async def check_disk_space(self, min_gb):
        # The shard index is the trailing segment of its id (create_shards_in_db
        # above), which is how a per-shard verdict is addressed.
        idx = int(activity.info().workflow_id.rsplit("-", 1)[-1])
        return self.disk_ok_per_shard.get(idx, self.disk_ok)

    async def load_progress(self, shard_task, filelist_md5):
        return []

    async def download_shard_filelist(self, filelist_key, staging_dir,
                                      expected_md5=""):
        return "/tmp/local-shard.json"

    async def read_filelist(self, local_filelist):
        return {"count": 1, "total_bytes": 50 * 1024 ** 3}

    async def run_pipeline_batch(self, task_for_pipeline, local_filelist,
                                 start_idx, batch_size, uploaded_bytes,
                                 total_bytes):
        return {"uploaded_files": 1, "uploaded_bytes": 50 * 1024 ** 3,
                "failed_files": 0, "speed_mbps": 100}

    async def save_progress(self, shard_name, markers, md5):
        return None

    async def report_shard_progress(self, shard_id, files, byts, speed):
        return None

    async def clear_progress(self, shard_name):
        return None

    async def cleanup_staging(self, shard_name, keep):
        return None


def _run(stubs):
    """Run the coordinator to completion and return its TaskResult."""

    async def go():
        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(
                env.client,
                task_queue=QUEUE,
                workflows=[ShardedDownloadWorkflow, ShardWorkerWorkflow],
                activities=stubs.activities(),
            ):
                return await env.client.execute_workflow(
                    ShardedDownloadWorkflow.run,
                    TaskInput(id="t-sharded-1", name="sharded-task",
                              repo_id="org/repo", source="hf", type="dataset",
                              category="manipulation"),
                    id=f"wf-{uuid.uuid4()}",
                    task_queue=QUEUE,
                )

    return asyncio.run(go())


def _final_status(stubs):
    """The last status the coordinator reported to the dashboard."""
    assert stubs.dashboard, "coordinator reported nothing to the dashboard"
    return stubs.dashboard[-1][0]


def test_shard_returning_failed_status_fails_the_task():
    """The molmobot-data regression: a shard that returns (not raises)
    status="failed" must not be counted as a successful shard."""
    stubs = _Stubs(num_shards=1, disk_ok=False)
    result = _run(stubs)

    # The shard really did take the normal-return path, not a raise — without
    # this the test could pass for the wrong reason (e.g. the child crashing).
    assert ("s-t-sharded-1-0", "failed", "Insufficient disk on testw") \
        in stubs.shard_status
    assert result.status == "failed"
    assert result.error == "1/1 shards failed"
    assert _final_status(stubs) == "failed"


def test_one_failed_shard_among_several_fails_the_task():
    """Partial success is not success: 9611 GB is not delivered by 2 of 3
    shards, and the dashboard must not say `done`."""
    stubs = _Stubs(num_shards=3, disk_ok=True, disk_ok_per_shard={1: False})
    result = _run(stubs)

    assert result.status == "failed"
    assert result.error == "1/3 shards failed"
    assert _final_status(stubs) == "failed"
    # The two healthy shards' bytes are still counted — a failed task should
    # not also lose the record of what it did move.
    assert result.bytes_uploaded == 2 * 50 * 1024 ** 3


def test_all_shards_succeeding_still_reports_done():
    """The guard on the fix itself: tightening the failure check must not
    turn healthy shards into failures."""
    stubs = _Stubs(num_shards=2, disk_ok=True)
    result = _run(stubs)

    assert result.status == "done"
    assert _final_status(stubs) == "done"
    assert result.files_uploaded == 2
    assert result.bytes_uploaded == 2 * 50 * 1024 ** 3
    assert stubs.aggregated == ["t-sharded-1"]


# --- the partition must account for every file ------------------------------
#
# The coordinator reads the filtered filelist from the listing worker's disk at
# /data/staging/{task_name}/.filelist.filtered.json. Names are reused by
# requirement — a resume MUST reuse the original name, and /queue/add permits
# re-adding a repo whose previous row is terminal — so two tasks can point at
# that one path. If the partition comes back short, nothing downstream notices:
# a zero-file shard returns done immediately, step 8 sees every shard done, and
# the task is reported done at 0 bytes. Which is the false-`done` signature.

def test_a_short_partition_fails_the_task_instead_of_reporting_done():
    stubs = _Stubs(num_shards=2, remaining_files=10, drop_files=3)
    result = _run(stubs)

    assert result.status == "failed"
    assert "does not cover the filelist" in result.error
    assert "7 files" in result.error and "expected 10" in result.error
    assert _final_status(stubs) == "failed"
    assert stubs.shard_status == [], "dispatched shards despite a short partition"
    assert stubs.aggregated == [], "aggregated a task it never ran"


def test_an_empty_partition_fails_the_task():
    """A zero-file shard starts a workflow, holds a worker and returns done.
    With num_shards clamped to the file count, one can only appear if the
    partition disagrees with the filelist it was given."""
    stubs = _Stubs(num_shards=2, remaining_files=10, extra_partitions=1)
    result = _run(stubs)

    assert result.status == "failed"
    assert "empty partitions [2]" in result.error
    assert _final_status(stubs) == "failed"


def test_shard_count_is_clamped_to_the_number_of_files():
    """8 shards over 3 files used to mean 5 workflows that download nothing,
    each occupying a worker that busy_servers then reports as taken."""
    stubs = _Stubs(num_shards=8, remaining_files=3)
    result = _run(stubs)

    assert result.status == "done"
    assert stubs.partition_calls[0]["num_shards"] == 3
    assert len(stubs.shard_status) == 3 * 2  # assign + done, per shard
    assert result.files_uploaded == 3


def test_the_partition_is_keyed_by_task_id_not_name():
    """Two same-named tasks overwrote each other's shard filelists at
    download-manager/filelists/{name}/shard-{i}.json — a shard could then
    fetch another repo's file list and download it into this task's prefix."""
    stubs = _Stubs(num_shards=1)
    _run(stubs)

    call = stubs.partition_calls[0]
    assert call["task_id"] == "t-sharded-1", (
        "coordinator did not pass the task id, so the BOS filelist key falls "
        "back to the reusable task name")
    assert call["staging_dir"].endswith("sharded-task")


def test_every_stub_matches_the_real_activity_signature():
    """Same rule as the pool suite: a stub that disagrees with the activity's
    parameter list tests the stub, not the system.

    Parameter count is the load-bearing half. temporalio only applies an
    activity's argument type hints when the declared parameter count equals the
    number of payloads sent, so a call site that omits an optional argument
    makes every dataclass parameter arrive as a raw dict — which is how
    `chunk_filelist` died on the first production pool dispatch while its
    replay suite stayed green.
    """
    import inspect

    from dlm.temporal import activities as real

    stubs = _Stubs()
    mismatches = []
    for stub in stubs.activities():
        name = getattr(stub, "__temporal_activity_definition").name
        real_fn = getattr(real, name, None)
        assert real_fn is not None, f"stub {name} has no real activity"
        real_params = list(inspect.signature(real_fn).parameters)
        stub_sig = inspect.signature(getattr(stubs, name))
        stub_params = list(stub_sig.parameters)
        # A stub that takes *args absorbs any count, so it cannot disagree.
        if any(p.kind is inspect.Parameter.VAR_POSITIONAL
               for p in stub_sig.parameters.values()):
            continue
        if len(real_params) != len(stub_params):
            mismatches.append(
                f"{name}: stub takes {len(stub_params)} params {tuple(stub_params)} "
                f"but the activity declares {len(real_params)} {tuple(real_params)}"
            )
    assert not mismatches, "\n".join(mismatches)
