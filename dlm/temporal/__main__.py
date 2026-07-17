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

from .workflows import DownloadDatasetWorkflow, SplitDownloadWorkflow
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
)


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

    # HF high-performance download
    os.environ.setdefault("HF_XET_HIGH_PERFORMANCE", "1")
    os.environ.setdefault("HF_HUB_CACHE", "/tmp/hf_cache")

    # Connect to Temporal
    logger.info(f"Connecting to Temporal at {args.temporal_host}...")
    client = await Client.connect(args.temporal_host)

    task_queue = args.task_queue or "download-workers"
    personal_queue = f"download-{args.server_key}"

    # Export server key so activities can report it
    os.environ["DLM_SERVER_KEY"] = args.server_key
    os.environ["DLM_WORKER_QUEUE"] = personal_queue

    # Register activities and workflows
    activities = [
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
    ]

    workflows = [
        DownloadDatasetWorkflow,
        SplitDownloadWorkflow,
    ]

    logger.info(f"Starting worker: server_key={args.server_key}, queues=[{task_queue}, {personal_queue}]")
    logger.info(f"Registered {len(workflows)} workflows, {len(activities)} activities")

    # Shared queue: picks up new workflows — limit to 1 so each worker
    # only claims one dataset at a time from the shared pool
    worker_shared = Worker(
        client,
        task_queue=task_queue,
        workflows=workflows,
        activities=activities,
        max_concurrent_workflow_tasks=1,
        max_concurrent_activities=1,
    )

    # Personal queue: pinned activities (file-local operations)
    worker_personal = Worker(
        client,
        task_queue=personal_queue,
        workflows=workflows,
        activities=activities,
    )

    logger.info("Workers running. Waiting for tasks...")
    # Start heartbeat loop and event buffer
    heartbeat_task = asyncio.create_task(_heartbeat_loop(args.server_key))

    from .event_buffer import init_event_buffer
    event_buf = init_event_buffer(args.server_key)
    await event_buf.start()

    try:
        await asyncio.gather(worker_shared.run(), worker_personal.run())
    finally:
        heartbeat_task.cancel()
        await event_buf.stop()


def main():
    args = parse_args()
    asyncio.run(run_worker(args))


if __name__ == "__main__":
    main()
