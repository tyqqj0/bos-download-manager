"""Temporal worker entry point.

Usage:
    python -m dlm.temporal --server-key w1
    python -m dlm.temporal --server-key w1 --temporal-host 154.85.43.52:7233
"""

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

from temporalio.client import Client
from temporalio.worker import Worker

from .workflows import (
    DownloadDatasetWorkflow,
    SplitDownloadWorkflow,
    ShardedDownloadWorkflow,
    ShardWorkerWorkflow,
    PoolDownloadWorkflow,
    pool_task_queue,
)
from .activities import (
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
    verify_missing_files,
)
# Safe at module scope: dlm.web.fleet imports only os/time and dlm/web/__init__
# is a bare docstring, so this pulls in no FastAPI. Workflow code cannot import
# it (determinism sandbox) — this file is the worker entry, not a workflow.
from ..web.fleet import polled_queues, source_for_worker


# Registered on both existing Workers (the coordinator queue and each
# worker's personal queue) — everything except the pool-batch executor
# itself, which only the third (pool) Worker below runs.
WORKFLOWS = [
    DownloadDatasetWorkflow,
    SplitDownloadWorkflow,       # keep for bj1-4 backward compat
    ShardedDownloadWorkflow,
    ShardWorkerWorkflow,
    PoolDownloadWorkflow,
]

ACTIVITIES = [
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
    verify_missing_files,
]


def _pool_source_for_worker(server_key: str) -> str:
    """Which pool queue (source) this worker's third Worker should serve.

    Delegates to `dlm.web.fleet.source_for_worker` rather than re-deriving
    `startswith("bj")` here. The pool branch duplicated the rule to keep
    `dlm.web` out of the worker process, but that reasoning does not hold for
    this one module: `dlm/web/__init__.py` is a bare docstring and fleet.py
    imports only os/time, so nothing drags in FastAPI/uvicorn — and the shared
    coordinator queue this process must poll already comes from
    `fleet.polled_queues`. Two copies of the routing rule is the failure mode
    that produced the w6 ModelScope listing, so there is one copy.
    """
    return source_for_worker(server_key)


def build_worker_specs(server_key: str, task_queue: str | None) -> list[dict]:
    """The task_queue/workflows/activities/concurrency for this process's Workers.

    Three jobs, and however many Workers `fleet.polled_queues` says it takes:
      - the shared coordinator queue(s) from `fleet.polled_queues` — where
        `Sharded`/`PoolDownloadWorkflow` coordinators and
        `DownloadDatasetWorkflow` land. `task_queue` is the raw --task-queue
        argument (None → polled_queues applies the "download-workers"
        default); polled_queues additionally adds the coordinator queue for
        this worker's own source, which is why the count is not fixed at two.
      - the worker's personal queue ("download-{server_key}"): activities
        pinned there because they read a filelist the listing worker wrote
        to its own local disk (list_repo_files, filter_filelist_against_bos,
        chunk_filelist, partition_files_greedy, ...).
      - the pool queue for this worker's source (`pool_task_queue(...)`):
        `run_pool_batch` only, `workflows=[]` — this Worker never runs a
        workflow task, only the shared-queue batch executor.

    Why the pool one matters: the pool coordinator dispatches batches to
    `pool-{hf,ms}` via `workflow.start_activity`, with no `schedule_to_start`
    timeout (by design — see workflows.py's pool section). A deploy that
    starts Workers for only the coordinator and personal queues does NOT
    error: the activity just sits scheduled on a queue nobody polls, bounded
    only by `schedule_to_close` (48h). Silent stall, not a crash — which is
    why this registration is pulled into its own testable function rather
    than left inline in `run_worker`.

    A pure function (no Client/Worker objects) so a test can assert the
    queue layout without a live Temporal connection.
    """
    personal_queue = f"download-{server_key}"
    pool_queue = pool_task_queue(_pool_source_for_worker(server_key))

    specs = []
    seen_queues = set()
    # fleet.polled_queues, not `task_queue` alone: a bj node passes its own
    # personal queue as --task-queue, which dedupes against the personal entry
    # and left it polling no shared coordinator queue at all. Coordinators then
    # only ever ran on `download-workers` (HK-only), which is how a ModelScope
    # listing landed on w6 and died with `No module named 'modelscope'`
    # (t-20260806-cbf39e). The dispatch half is fleet.coordinator_queue.
    for queue in polled_queues(server_key, task_queue):
        if queue in seen_queues:
            continue
        seen_queues.add(queue)
        specs.append({
            "task_queue": queue,
            "workflows": WORKFLOWS,
            "activities": ACTIVITIES,
            # The personal queue carries the listing/partition activities that
            # read this worker's own disk, so it gets the second slot; a shared
            # coordinator queue runs one coordinator at a time by design.
            "max_concurrent_activities": 2 if queue == personal_queue else 1,
        })

    if pool_queue not in seen_queues:
        seen_queues.add(pool_queue)
        specs.append({
            "task_queue": pool_queue,
            "workflows": [],
            "activities": [run_pool_batch],
            "max_concurrent_activities": 1,
        })

    return specs


def parse_args():
    parser = argparse.ArgumentParser(description="DLM Temporal Worker")
    parser.add_argument("--server-key", required=True, help="Worker identifier (w1-w7)")
    parser.add_argument(
        "--temporal-host",
        default=os.environ.get("TEMPORAL_HOST", "154.85.43.52:7233"),
        help="Temporal server address",
    )
    parser.add_argument("--task-queue", default=None, help="Override task queue name")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


async def _heartbeat_loop(server_key: str):
    """Report worker status to S1 dashboard every 15s."""
    import shutil
    import requests

    coordinator = os.environ.get("DLM_COORDINATOR", "http://154.85.43.52:8080")
    staging = Path("/data/staging")

    while True:
        try:
            disk_free = shutil.disk_usage(staging).free / (1024 ** 3)
            requests.post(
                f"{coordinator}/api/worker-heartbeat",
                json={
                    "server_key": server_key,
                    "hostname": f"{server_key}@temporal",
                    "disk_free_gb": round(disk_free, 1),
                    "status": "online",
                },
                timeout=5,
            )
        except Exception:
            pass
        await asyncio.sleep(15)


async def run_worker(args):
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )
    logger = logging.getLogger("dlm.temporal")

    # Load .env
    from dotenv import load_dotenv
    for env_path in [Path("/root/.env"), Path("/root/code/bos-download-manager/.env")]:
        if env_path.exists():
            load_dotenv(env_path)

    # Ensure staging exists
    Path("/data/staging").mkdir(parents=True, exist_ok=True)

    # Disable Xet (TCP stall bug xet-core#789) — use legacy LFS download
    os.environ["HF_HUB_DISABLE_XET"] = "1"
    os.environ.pop("HF_XET_HIGH_PERFORMANCE", None)
    os.environ.setdefault("HF_HUB_CACHE", "/tmp/hf_cache")

    # Connect to Temporal
    logger.info(f"Connecting to Temporal at {args.temporal_host}...")
    client = await Client.connect(args.temporal_host)

    personal_queue = f"download-{args.server_key}"

    # Export server key so activities can report it
    os.environ["DLM_SERVER_KEY"] = args.server_key
    os.environ["DLM_WORKER_QUEUE"] = personal_queue

    # Register activities, workflows, and the pool queue — see
    # build_worker_specs' docstring for why the third (pool) Worker exists.
    # args.task_queue (not a local default of "download-workers"): polled_queues
    # inside build_worker_specs applies the default AND adds the shared
    # coordinator queue for this worker's source.
    specs = build_worker_specs(args.server_key, args.task_queue)
    queues = [s["task_queue"] for s in specs]
    logger.info(f"Starting worker: server_key={args.server_key}, queues={queues}")
    logger.info(
        "Registered %d workflows, %d activities (shared); pool queue=%s activities=%s",
        len(WORKFLOWS), len(ACTIVITIES),
        pool_task_queue(_pool_source_for_worker(args.server_key)),
        [a.__name__ for a in specs[-1]["activities"]],
    )

    import uuid
    build_id = f"{args.server_key}-{uuid.uuid4().hex[:8]}"

    workers = []
    for spec in specs:
        w = Worker(
            client,
            task_queue=spec["task_queue"],
            workflows=spec["workflows"],
            activities=spec["activities"],
            max_concurrent_workflow_tasks=1,
            max_concurrent_activities=spec["max_concurrent_activities"],
            build_id=build_id,
        )
        workers.append(w)

    logger.info("Workers running. Waiting for tasks...")
    heartbeat_task = asyncio.create_task(_heartbeat_loop(args.server_key))

    from .event_buffer import init_event_buffer
    event_buf = init_event_buffer(args.server_key)
    await event_buf.start()

    try:
        await asyncio.gather(*(w.run() for w in workers))
    finally:
        heartbeat_task.cancel()
        await event_buf.stop()


def main():
    args = parse_args()
    asyncio.run(run_worker(args))


if __name__ == "__main__":
    main()
