"""Queue management API — Temporal-based dispatch."""

import logging
import uuid
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

import asyncio
from fastapi import APIRouter

from ...queue import snapshot

logger = logging.getLogger("dlm.web")
router = APIRouter(tags=["queue"])

_executor = ThreadPoolExecutor(max_workers=4)


def _run_blocking(fn, *args):
    loop = asyncio.get_event_loop()
    return loop.run_in_executor(_executor, fn, *args)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _next_task_id() -> str:
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    return f"t-{today}-{uuid.uuid4().hex[:6]}"


@router.get("/queue")
async def list_queue():
    """List all tasks with their current state."""
    def do_list():
        snapshot.init_db()
        tasks = snapshot.get_all_tasks()
        workers = snapshot.get_workers()
        return {"tasks": tasks, "workers": workers}
    return await _run_blocking(do_list)


@router.get("/queue/pending")
async def list_pending():
    def do_list():
        snapshot.init_db()
        return {"tasks": snapshot.get_tasks_by_status("pending")}
    return await _run_blocking(do_list)


@router.get("/queue/active")
async def list_active():
    def do_list():
        snapshot.init_db()
        return {"tasks": snapshot.get_tasks_by_status("downloading")}
    return await _run_blocking(do_list)


@router.post("/queue/add")
async def add_to_queue(body: dict):
    """Add a new download task and start Temporal workflow.

    Body:
        repo_id: str — HuggingFace repo ID or URL
        name: str (optional)
        type: str — "dataset" or "model"
        category: str (optional)
        priority: int — 0 (highest) to 9 (lowest)
        source: str — "hf" or "modelscope"
        split_workers: int (optional) — split across N workers for large datasets
    """
    from ...core.parser import parse_repo

    repo_id = body.get("repo_id", "").strip()
    if not repo_id:
        return {"error": "repo_id is required"}

    parsed = parse_repo(repo_id)
    source = body.get("source", parsed.get("source", "hf"))
    name = body.get("name", parsed.get("name", repo_id.split("/")[-1]))
    task_type = body.get("type", parsed.get("type", "dataset"))
    category = body.get("category", "")
    priority = max(0, min(9, int(body.get("priority", 5))))
    split_workers = int(body.get("split_workers", 0))

    task_id = _next_task_id()

    task_meta = {
        "id": task_id,
        "name": name,
        "repo_id": parsed.get("repo_id", repo_id),
        "source": source,
        "type": task_type,
        "category": category,
        "status": "pending",
        "priority": priority,
        "size_gb": 0,
        "downloaded_gb": 0,
        "progress_pct": 0,
        "speed_mbps": 0,
        "created_at": _now(),
    }

    # Check for duplicates
    def check_dup():
        snapshot.init_db()
        for t in snapshot.get_all_tasks():
            if t.get("repo_id") == task_meta["repo_id"] and t.get("status") not in ("failed", "revoked", "done"):
                return t
        return None

    dup = await _run_blocking(check_dup)
    if dup:
        return {"error": f"Already exists: {dup['id']} ({dup['name']}) status={dup['status']}"}

    # Save to SQLite
    def do_save():
        snapshot.init_db()
        snapshot.upsert_task(task_meta)
    await _run_blocking(do_save)

    # Start Temporal workflow
    from ..temporal_client import start_download, start_split_download
    try:
        if split_workers >= 2:
            await start_split_download(task_meta, worker_count=split_workers)
        else:
            await start_download(task_meta)
    except Exception as e:
        logger.error(f"Failed to start workflow: {e}")
        return {"error": f"Failed to start workflow: {e}"}

    return {"ok": True, "task_id": task_id, "name": name, "priority": priority}


@router.post("/queue/pause")
async def pause_task(body: dict):
    """Pause a running task (cancels the Temporal workflow)."""
    task_id = body.get("task_id", "")
    if not task_id:
        return {"error": "task_id is required"}

    def do_update():
        snapshot.init_db()
        task = snapshot.get_task(task_id)
        if not task:
            return {"error": f"Task {task_id} not found"}
        if task["status"] not in ("downloading", "pending"):
            return {"error": f"Cannot pause task in status={task['status']}"}
        snapshot.update_task_progress(task_id, status="paused", phase=None, speed_mbps=0)
        return None

    error = await _run_blocking(do_update)
    if error:
        return error

    from ..temporal_client import cancel_workflow
    await cancel_workflow(task_id)

    return {"ok": True, "task_id": task_id, "status": "paused"}


@router.post("/queue/resume")
async def resume_task(body: dict):
    """Resume a paused/failed task (starts a new workflow)."""
    task_id = body.get("task_id", "")
    if not task_id:
        return {"error": "task_id is required"}

    def do_get():
        snapshot.init_db()
        return snapshot.get_task(task_id)
    task = await _run_blocking(do_get)

    if not task:
        return {"error": f"Task {task_id} not found"}
    if task["status"] not in ("paused", "failed", "preempted"):
        return {"error": f"Cannot resume task in status={task['status']}"}

    def do_update():
        snapshot.update_task_progress(task_id, status="pending", phase="resuming", speed_mbps=0, error=None)
    await _run_blocking(do_update)

    from ..temporal_client import start_download
    await start_download(task)

    return {"ok": True, "task_id": task_id, "status": "pending"}


@router.post("/queue/retry")
async def retry_task(body: dict):
    """Retry a failed task."""
    task_id = body.get("task_id", "")
    if not task_id:
        return {"error": "task_id is required"}

    def do_get():
        snapshot.init_db()
        return snapshot.get_task(task_id)
    task = await _run_blocking(do_get)

    if not task:
        return {"error": f"Task {task_id} not found"}
    if task["status"] not in ("failed", "revoked", "paused"):
        return {"error": f"Cannot retry task in status={task['status']}"}

    def do_update():
        retry_count = (task.get("retry_count") or 0) + 1
        snapshot.update_task_progress(
            task_id, status="pending", phase="retrying",
            speed_mbps=0, error=None,
        )
        conn = snapshot._conn()
        conn.execute("UPDATE tasks SET retry_count = ? WHERE id = ?", (retry_count, task_id))
        conn.commit()
    await _run_blocking(do_update)

    from ..temporal_client import start_download
    await start_download(task)

    return {"ok": True, "task_id": task_id, "status": "pending"}


@router.post("/queue/reorder")
async def reorder_task(body: dict):
    """Change a task's priority."""
    task_id = body.get("task_id", "")
    new_priority = max(0, min(9, int(body.get("priority", 5))))

    def do_reorder():
        snapshot.init_db()
        conn = snapshot._conn()
        conn.execute("UPDATE tasks SET priority = ? WHERE id = ?", (new_priority, task_id))
        conn.commit()
        return {"ok": True, "task_id": task_id, "priority": new_priority}
    return await _run_blocking(do_reorder)


@router.post("/queue/jump")
async def jump_queue(body: dict):
    body["priority"] = 0
    return await reorder_task(body)


@router.delete("/queue/{task_id}")
async def delete_from_queue(task_id: str):
    """Cancel workflow and delete task."""
    from ..temporal_client import cancel_workflow
    await cancel_workflow(task_id)

    def do_delete():
        snapshot.init_db()
        snapshot.delete_task(task_id)
        return {"ok": True, "task_id": task_id, "deleted": True}
    return await _run_blocking(do_delete)


@router.post("/sync")
async def sync_stub():
    return {"changes": 0, "message": "Sync not needed — Temporal manages state"}
