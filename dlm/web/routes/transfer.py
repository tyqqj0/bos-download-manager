"""Transfer routes — BOS→D-Robotics transfer status and controls."""

import os
import logging

from fastapi import APIRouter

from ..cache import cache
from . import run_blocking

logger = logging.getLogger("dlm.web")
router = APIRouter(tags=["transfer"])


@router.get("/transfer")
async def get_transfer_status():
    """Get transfer status for all tasks."""
    def _do():
        from ...queue.snapshot import get_all_tasks, init_db
        init_db()
        all_tasks = get_all_tasks()

        transfer_tasks = [
            {
                "id": t["id"],
                "name": t.get("name", ""),
                "category": t.get("category", ""),
                "size_gb": t.get("size_gb", 0),
                "status": t.get("status", ""),
                "transfer_status": t.get("transfer_status"),
                "transfer_task_id": t.get("transfer_task_id"),
                "transfer_error": t.get("transfer_error"),
                "bos_path": t.get("bos_path", ""),
            }
            for t in all_tasks
            if t.get("status") == "done" or t.get("transfer_status") is not None
        ]

        summary = {
            "total_done": sum(1 for t in all_tasks if t.get("status") == "done"),
            "transferred": sum(1 for t in transfer_tasks if t.get("transfer_status") == "done"),
            "transferring": sum(1 for t in transfer_tasks if t.get("transfer_status") == "transferring"),
            "failed": sum(1 for t in transfer_tasks if t.get("transfer_status") == "failed"),
            "pending": sum(1 for t in transfer_tasks if t.get("transfer_status") in (None, "queued")),
        }

        return {
            "tasks": transfer_tasks,
            "summary": summary,
            "paused": bool(cache.get("transfer_paused")),
        }

    return await run_blocking(_do)


@router.post("/transfer/trigger")
async def trigger_transfer(body: dict = None):
    """Manually trigger transfer for specific tasks or all pending done tasks."""
    task_ids = (body or {}).get("task_ids", [])

    def _do():
        from ...queue.snapshot import get_all_tasks, init_db
        from ...transfer.tasks import transfer_to_juicefs
        init_db()

        all_tasks = get_all_tasks()
        triggered = []

        for t in all_tasks:
            if task_ids and t["id"] not in task_ids:
                continue
            if t.get("status") != "done":
                continue
            if t.get("transfer_status") in ("transferring", "done"):
                continue

            transfer_to_juicefs.apply_async(
                kwargs={"task_meta": t},
                queue="transfers",
            )
            triggered.append(t.get("name", t["id"]))

        return {"triggered": triggered, "count": len(triggered)}

    return await run_blocking(_do)


@router.post("/transfer/{task_id}/retry")
async def retry_transfer(task_id: str):
    """Retry a failed transfer."""
    def _do():
        from ...queue.snapshot import get_task, init_db, _conn
        from ...transfer.tasks import transfer_to_juicefs
        init_db()

        task = get_task(task_id)
        if not task:
            return {"error": "Task not found"}
        if task.get("transfer_status") not in ("failed",):
            return {"error": f"Transfer status is {task.get('transfer_status')}, not failed"}

        conn = _conn()
        conn.execute(
            "UPDATE tasks SET transfer_status = 'queued', transfer_error = NULL WHERE id = ?",
            (task_id,),
        )
        conn.commit()

        transfer_to_juicefs.apply_async(
            kwargs={"task_meta": task},
            queue="transfers",
        )
        return {"ok": True, "name": task.get("name", "")}

    return await run_blocking(_do)


@router.post("/transfer/pause")
async def pause_transfer(body: dict = None):
    """Pause or resume auto-transfer."""
    paused = (body or {}).get("paused", True)
    cache.set("transfer_paused", paused)
    return {"paused": paused}
