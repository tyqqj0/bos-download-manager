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
    save_progress,
    clear_progress,
    run_pipeline_batch,
    cleanup_staging,
    cleanup_all_staging,
    report_to_dashboard,
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

    # Connect to Temporal
    logger.info(f"Connecting to Temporal at {args.temporal_host}...")
    client = await Client.connect(args.temporal_host)

    task_queue = args.task_queue or f"download-{args.server_key}"

    # Register activities and workflows
    activities = [
        list_repo_files,
        load_progress,
        save_progress,
        clear_progress,
        run_pipeline_batch,
        cleanup_staging,
        cleanup_all_staging,
        report_to_dashboard,
    ]

    workflows = [
        DownloadDatasetWorkflow,
        SplitDownloadWorkflow,
    ]

    logger.info(f"Starting worker: server_key={args.server_key}, queue={task_queue}")
    logger.info(f"Registered {len(workflows)} workflows, {len(activities)} activities")

    # Run worker — polls Temporal for tasks automatically
    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=workflows,
        activities=activities,
    )

    logger.info("Worker running. Waiting for tasks...")
    await worker.run()


def main():
    args = parse_args()
    asyncio.run(run_worker(args))


if __name__ == "__main__":
    main()
