"""T6 — worker process registration.

Two independent regressions are the ones this task exists to prevent:

1. A worker process that never starts a Worker for the pool queue. The
   coordinator (`PoolDownloadWorkflow`) dispatches `run_pool_batch` to
   `pool-{hf,ms}` via `workflow.start_activity` with no `schedule_to_start`
   timeout — a deploy that forgets the third Worker doesn't error, the
   activity just sits scheduled until `schedule_to_close` (48h) kills it.
   Silent stall, not a crash.
2. An activity the pool coordinator calls by name that never got added to
   the registered activities list — same failure mode, different cause.

`build_worker_specs` in `dlm.temporal.__main__` is a pure function (no
Client/Worker objects) precisely so both can be asserted here without a
live Temporal connection.

Run: python3 -m pytest tests/test_worker_registration.py -q  (needs temporalio)
"""

from __future__ import annotations

import re
from pathlib import Path


def test_pool_queue_naming_for_hf_worker():
    from dlm.temporal.__main__ import build_worker_specs

    specs = build_worker_specs("w1", "download-workers")
    queues = [s["task_queue"] for s in specs]
    assert "pool-hf" in queues
    assert "pool-ms" not in queues


def test_pool_queue_naming_for_modelscope_worker():
    from dlm.temporal.__main__ import build_worker_specs

    specs = build_worker_specs("bj1", "download-bj1")
    queues = [s["task_queue"] for s in specs]
    assert "pool-ms" in queues
    assert "pool-hf" not in queues


def test_pool_worker_spec_is_workflow_empty_single_activity():
    from dlm.temporal.__main__ import build_worker_specs, run_pool_batch

    specs = build_worker_specs("w1", "download-workers")
    pool_specs = [s for s in specs if s["task_queue"] == "pool-hf"]
    assert len(pool_specs) == 1, "pool queue must be registered exactly once"
    pool_spec = pool_specs[0]

    assert pool_spec["workflows"] == [], (
        "the pool Worker must never run a workflow task — only the shared "
        "batch executor"
    )
    assert pool_spec["activities"] == [run_pool_batch]
    assert pool_spec["max_concurrent_activities"] == 1


def test_hf_worker_has_three_distinct_queues():
    """download-workers (coordinator), download-w1 (personal), pool-hf."""
    from dlm.temporal.__main__ import build_worker_specs

    specs = build_worker_specs("w1", "download-workers")
    queues = [s["task_queue"] for s in specs]
    assert queues == ["download-workers", "download-w1", "pool-hf"]
    assert len(set(queues)) == len(queues), "no duplicate Worker for the same queue"


def test_bj_worker_collapses_coordinator_and_personal_queue():
    """bj hosts run with --task-queue == their own personal queue (source
    isolation, per deploy-workers.sh's QUEUES map) — that must collapse to
    one Worker, not two Workers polling the same queue.

    The ms coordinator queue is NOT a duplicate and must be there: with only
    the personal and pool queues, a bj host polled no shared coordinator queue
    at all, so every coordinator ran on the HK-only `download-workers` and a
    ModelScope listing landed on w6 (`No module named 'modelscope'`,
    t-20260806-cbf39e). fleet.polled_queues adds it; see
    tests/test_coordinator_routing.py for the per-host table."""
    from dlm.temporal.__main__ import build_worker_specs

    specs = build_worker_specs("bj1", "download-bj1")
    queues = [s["task_queue"] for s in specs]
    assert queues == ["download-bj1", "download-ms-workers", "pool-ms"]
    assert len(set(queues)) == len(queues), "no duplicate Worker for the same queue"


def test_shared_activities_and_workflows_lists_unchanged_membership():
    """G1: the existing entries in the shared lists must still be there —
    this task only adds to them."""
    from dlm.temporal.__main__ import ACTIVITIES, WORKFLOWS
    from dlm.temporal.workflows import (
        DownloadDatasetWorkflow,
        SplitDownloadWorkflow,
        ShardedDownloadWorkflow,
        ShardWorkerWorkflow,
        PoolDownloadWorkflow,
    )
    from dlm.temporal.activities import (
        list_repo_files,
        load_progress,
        read_filelist,
        partition_filelist,
        save_progress,
        clear_progress,
        run_pipeline_batch,
        cleanup_staging,
        cleanup_all_staging,
        report_to_dashboard,
        check_disk_space,
        partition_files_greedy,
        create_shards_in_db,
        update_shard_status,
        report_shard_progress,
        query_idle_workers,
        aggregate_task_from_shards,
        assign_shard_server,
        download_shard_filelist,
        filter_filelist_against_bos,
        report_resume_info,
        pool_alive_workers,
        chunk_filelist,
        run_pool_batch,
        create_pool_batches_in_db,
        record_batches_and_window,
        release_pool_batches,
    )

    for wf in (
        DownloadDatasetWorkflow, SplitDownloadWorkflow,
        ShardedDownloadWorkflow, ShardWorkerWorkflow, PoolDownloadWorkflow,
    ):
        assert wf in WORKFLOWS

    for act in (
        list_repo_files, load_progress, read_filelist, partition_filelist,
        save_progress, clear_progress, run_pipeline_batch, cleanup_staging,
        cleanup_all_staging, report_to_dashboard, check_disk_space,
        partition_files_greedy, create_shards_in_db, update_shard_status,
        report_shard_progress, query_idle_workers, aggregate_task_from_shards,
        assign_shard_server, download_shard_filelist,
        filter_filelist_against_bos, report_resume_info, pool_alive_workers,
        chunk_filelist, run_pool_batch, create_pool_batches_in_db,
        record_batches_and_window, release_pool_batches,
    ):
        assert act in ACTIVITIES


def _activity_names_called_by(source: str, class_name: str) -> set[str]:
    """Every activity name string `class_name`'s methods pass to
    `workflow.execute_activity`/`workflow.start_activity`.

    Regex over source text, not AST: the call sites span multiple lines
    (`args=[...]` etc.) and this only needs the literal name in the first
    positional slot, which the codebase's style always puts on its own line
    right after the opening paren.
    """
    # Slice to the class body so a name used only by a different workflow
    # class doesn't get credited here.
    class_start = source.index(f"class {class_name}")
    rest = source[class_start:]
    next_class = re.search(r"\n@workflow\.defn\nclass ", rest[1:])
    body = rest if next_class is None else rest[: next_class.start() + 1]
    return set(re.findall(r'(?:execute_activity|start_activity)\(\s*\n?\s*"([a-zA-Z_]+)"', body))


def test_registered_activities_cover_every_activity_pool_workflow_calls():
    """The only cheap guard against the silent-stall failure mode: an
    activity name the pool coordinator calls that never made it into any
    registered Worker's activity list. Diffs the string literals the
    workflow source actually passes against the union of everything the
    three Workers in build_worker_specs register."""
    from dlm.temporal.__main__ import build_worker_specs, ACTIVITIES

    workflows_py = Path(__file__).resolve().parents[1] / "dlm" / "temporal" / "workflows.py"
    called = _activity_names_called_by(workflows_py.read_text(), "PoolDownloadWorkflow")
    assert called, "expected to find execute_activity/start_activity calls in PoolDownloadWorkflow"

    specs = build_worker_specs("w1", "download-workers")
    registered = {a.__name__ for spec in specs for a in spec["activities"]}
    # Also cross-check against the shared list directly, independent of
    # build_worker_specs' own wiring.
    registered |= {a.__name__ for a in ACTIVITIES}

    missing = called - registered
    assert not missing, (
        f"PoolDownloadWorkflow calls activities never registered on any "
        f"Worker: {missing}"
    )
