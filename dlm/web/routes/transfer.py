"""Transfer routes — BOS→D-Robotics transfer status and controls."""

import os
import logging
from dataclasses import asdict

from fastapi import APIRouter

from ..cache import cache

logger = logging.getLogger("dlm.web")
router = APIRouter(tags=["transfer"])


def _run_blocking(fn, *args):
    import asyncio
    from concurrent.futures import ThreadPoolExecutor
    loop = asyncio.get_event_loop()
    pool = ThreadPoolExecutor(max_workers=1)
    return loop.run_in_executor(pool, fn, *args)


@router.get("/transfer")
async def get_transfer_status():
    """Get transfer status for all tasks."""
    tasks_data = cache.get_tasks()
    if not tasks_data:
        return {"tasks": [], "summary": {}, "paused": False}

    all_tasks = tasks_data.get("tasks", [])

    # Only include tasks that are done downloading or have transfer activity
    transfer_tasks = [
        {
            "id": t["id"],
            "name": t["name"],
            "category": t.get("category", ""),
            "size_gb": t.get("size_gb", 0),
            "status": t.get("status", ""),
            "transfer_status": t.get("transfer_status"),
            "transfer_task_id": t.get("transfer_task_id"),
            "transfer_error": t.get("transfer_error"),
            "transfer_started_at": t.get("transfer_started_at"),
            "transfer_completed_at": t.get("transfer_completed_at"),
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


@router.post("/transfer/trigger")
async def trigger_transfer(body: dict = None):
    """Manually trigger transfer for specific tasks or all pending."""
    from ...core.state import StateManager
    from ...core.models import _now
    from ...transfer.dcloud import DCloudClient

    task_ids = (body or {}).get("task_ids", [])

    dcloud_user = os.environ.get("DCLOUD_USER")
    dcloud_pass = os.environ.get("DCLOUD_PASS")
    bos_ak = os.environ.get("BAIDU_AK")
    bos_sk = os.environ.get("BAIDU_SK")

    if not all([dcloud_user, dcloud_pass, bos_ak, bos_sk]):
        return {"error": "Missing credentials (DCLOUD_USER/DCLOUD_PASS/BAIDU_AK/BAIDU_SK)"}

    def do_trigger():
        mgr = StateManager.create()
        state = mgr.load(use_cache=False)
        client = DCloudClient(dcloud_user, dcloud_pass)
        client.login()

        triggered = []
        for task in state.tasks:
            if task_ids and task.id not in task_ids:
                continue
            if task.status != "done":
                continue
            if task.transfer_status in ("transferring", "done"):
                continue

            bos_path = task.bos_path.lstrip("/")
            if task.category:
                target_path = f"/auwomo-datasets/raw-data/{task.category}/{task.name}"
            else:
                target_path = f"/auwomo-datasets/raw-data/{task.name}"

            try:
                tid = client.import_from_bos(
                    bos_ak=bos_ak,
                    bos_sk=bos_sk,
                    bos_bucket="westlake-autolab-databuilder-data",
                    bos_path=bos_path,
                    target_path=target_path,
                )
                task.transfer_status = "transferring"
                task.transfer_task_id = tid
                task.transfer_started_at = _now()
                task.transfer_error = None
                triggered.append(task.name)
            except Exception as e:
                task.transfer_status = "failed"
                task.transfer_error = str(e)

        if triggered:
            mgr.save(state)
        return triggered

    triggered = await _run_blocking(do_trigger)
    return {"triggered": triggered, "count": len(triggered)}


@router.post("/transfer/{task_id}/retry")
async def retry_transfer(task_id: str):
    """Retry a failed transfer."""
    from ...core.state import StateManager

    def do_retry():
        mgr = StateManager.create()
        state = mgr.load(use_cache=False)
        for task in state.tasks:
            if task.id == task_id:
                if task.transfer_status not in ("failed",):
                    return {"error": f"Task transfer_status is {task.transfer_status}, not failed"}
                task.transfer_status = "queued"
                task.transfer_error = None
                task.transfer_task_id = None
                mgr.save(state)
                return {"ok": True, "name": task.name}
        return {"error": "Task not found"}

    return await _run_blocking(do_retry)


@router.post("/transfer/pause")
async def pause_transfer(body: dict = None):
    """Pause or resume auto-transfer."""
    paused = (body or {}).get("paused", True)
    cache.set("transfer_paused", paused)
    return {"paused": paused}


@router.post("/transfer/sync")
async def sync_transfer():
    """Force a transfer check cycle immediately."""
    from ...core.state import StateManager
    from ..scheduler import _auto_transfer

    def do_sync():
        mgr = StateManager.create()
        state = mgr.load(use_cache=False)
        return _auto_transfer(state, mgr)

    count = await _run_blocking(do_sync)
    return {"updated": count}
