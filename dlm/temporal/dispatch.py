"""Auto-dispatch: read tasks from SQLite DB and start Temporal workflows.

Usage:
    python -m dlm.temporal.dispatch                          # dispatch all 'downloading' tasks
    python -m dlm.temporal.dispatch --task-id t-xxx --worker w1
    python -m dlm.temporal.dispatch --status downloading --strategy round-robin
"""

import asyncio
import argparse
import logging
import os
import sys
from itertools import cycle

logger = logging.getLogger(__name__)

WORKERS = ["w1", "w2", "w3", "w4", "w5", "w6", "w7"]


async def dispatch_tasks(
    task_ids: list[str] = None,
    status: str = "downloading",
    strategy: str = "round-robin",
    workers: list[str] = None,
):
    """Dispatch tasks from SQLite DB to Temporal workers.

    Args:
        task_ids: Specific task IDs to dispatch (overrides status filter)
        status: Filter tasks by this status (default: "downloading")
        strategy: "round-robin" or "single" (assign all to first worker)
        workers: Worker keys to use (default: w1-w7)
    """
    from ..queue.snapshot import init_db, get_tasks_by_status, get_task
    from ..web.temporal_client import get_client, start_download

    init_db()

    # Get tasks
    if task_ids:
        tasks = [get_task(tid) for tid in task_ids]
        tasks = [t for t in tasks if t is not None]
    else:
        tasks = get_tasks_by_status(status)

    if not tasks:
        logger.warning(f"No tasks found (status={status})")
        return []

    worker_list = workers or WORKERS
    worker_cycle = cycle(worker_list)
    dispatched = []

    for task in tasks:
        if not task.get("repo_id"):
            logger.warning(f"Skipping {task['id']} ({task['name']}): no repo_id")
            continue

        worker = next(worker_cycle)
        queue = f"download-{worker}"

        try:
            handle = await start_download(task, task_queue=queue)
            dispatched.append({
                "task_id": task["id"],
                "name": task["name"],
                "repo_id": task["repo_id"],
                "worker": worker,
                "queue": queue,
            })
            logger.info(f"Dispatched {task['name']} → {worker} (repo: {task['repo_id']})")
        except Exception as e:
            logger.error(f"Failed to dispatch {task['name']}: {e}")

    return dispatched


def main():
    parser = argparse.ArgumentParser(description="Dispatch DLM tasks to Temporal workers")
    parser.add_argument("--task-id", action="append", help="Specific task ID(s)")
    parser.add_argument("--status", default="downloading", help="Filter by status")
    parser.add_argument("--strategy", default="round-robin", choices=["round-robin", "single"])
    parser.add_argument("--worker", action="append", help="Target worker(s)")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )

    # Load .env
    from pathlib import Path
    from dotenv import load_dotenv
    for env_path in [Path("/root/.env"), Path("/root/code/bos-download-manager/.env"), Path(".env")]:
        if env_path.exists():
            load_dotenv(env_path)

    results = asyncio.run(dispatch_tasks(
        task_ids=args.task_id,
        status=args.status,
        strategy=args.strategy,
        workers=args.worker,
    ))

    print(f"\nDispatched {len(results)} tasks:")
    for r in results:
        print(f"  {r['task_id']} ({r['name']}) → {r['worker']}  [repo: {r['repo_id']}]")


if __name__ == "__main__":
    main()
