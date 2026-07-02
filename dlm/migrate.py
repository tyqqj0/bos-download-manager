"""Migration script — import BOS state.json into SQLite + re-enqueue pending tasks.

Run once during the Celery switchover:
    python3 -m dlm.migrate
"""

import os
import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

PRIORITY_MAP = {"P0": 0, "P1": 2, "P2": 5, "P3": 7}


def migrate():
    from dlm.core.state import StateManager
    from dlm.queue.snapshot import init_db, upsert_task, get_task
    from dlm.worker.download import download_dataset
    from dlm.core.models import _now

    init_db()
    logger.info("Loading state from BOS...")
    mgr = StateManager.create()
    state = mgr.load(use_cache=False)
    logger.info(f"Found {len(state.tasks)} tasks in state.json")

    imported = 0
    enqueued = 0

    for task in state.tasks:
        task_dict = task.to_dict()

        old_priority = task_dict.get("priority", "P1")
        task_dict["priority"] = PRIORITY_MAP.get(old_priority, 5)

        old_status = task_dict.get("status", "queued")
        status_map = {
            "queued": "pending",
            "dispatched": "pending",
            "downloading": "pending",
            "done": "done",
            "failed": "failed",
            "skipped": "revoked",
            "needs-auth": "failed",
        }
        task_dict["status"] = status_map.get(old_status, old_status)
        task_dict["celery_task_id"] = task_dict["id"]

        existing = get_task(task_dict["id"])
        if existing:
            logger.debug(f"  Skip existing: {task_dict['id']}")
            continue

        upsert_task(task_dict)
        imported += 1

        if task_dict["status"] == "pending":
            try:
                download_dataset.apply_async(
                    args=[task_dict],
                    priority=task_dict["priority"],
                    task_id=task_dict["id"],
                )
                enqueued += 1
                logger.info(f"  Enqueued: {task_dict['name']} (priority={task_dict['priority']})")
            except Exception as e:
                logger.warning(f"  Failed to enqueue {task_dict['name']}: {e}")

    logger.info(f"\nMigration complete: {imported} imported, {enqueued} enqueued to Celery")
    logger.info(f"Tasks already done: {sum(1 for t in state.tasks if t.status == 'done')}")
    logger.info(f"Tasks failed: {sum(1 for t in state.tasks if t.status == 'failed')}")


if __name__ == "__main__":
    if not os.environ.get("REDIS_URL"):
        print("ERROR: REDIS_URL not set. Set it before running migration.")
        print("  export REDIS_URL=redis://:password@154.85.43.52:6379/0")
        sys.exit(1)
    migrate()
