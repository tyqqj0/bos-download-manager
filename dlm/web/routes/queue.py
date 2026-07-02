"""Queue management API — add, list, reorder, interrupt, jump, delete tasks."""

import logging
import uuid
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

import asyncio
from fastapi import APIRouter

from ...queue.app import app
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
    """List only pending (queued) tasks."""
    def do_list():
        snapshot.init_db()
        return {"tasks": snapshot.get_tasks_by_status("pending")}

    return await _run_blocking(do_list)


@router.get("/queue/active")
async def list_active():
    """List currently downloading tasks."""
    def do_list():
        snapshot.init_db()
        return {"tasks": snapshot.get_tasks_by_status("downloading")}

    return await _run_blocking(do_list)


@router.post("/queue/add")
async def add_to_queue(body: dict):
    """Add a new download task to the queue.

    Body:
        repo_id: str — HuggingFace/ModelScope repo ID or URL
        name: str (optional) — override task name
        type: str — "dataset" or "model"
        category: str (optional)
        priority: int — 0 (highest) to 9 (lowest), default 5
        source: str — "hf" or "modelscope" or "wget"
        auto_transfer: bool — if true, chain transfer after download
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
    priority = int(body.get("priority", 5))
    priority = max(0, min(9, priority))
    auto_transfer = body.get("auto_transfer", True)

    task_id = _next_task_id()
    bos_prefix = f"auwomo-model/{name}" if task_type == "model" else f"auwomo-datasets/raw-data/{category}/{name}" if category else f"auwomo-datasets/raw-data/{name}"

    task_meta = {
        "id": task_id,
        "name": name,
        "repo_id": parsed.get("repo_id", repo_id),
        "source": source,
        "type": task_type,
        "category": category,
        "bos_path": bos_prefix + "/",
        "status": "pending",
        "priority": priority,
        "size_gb": 0,
        "downloaded_gb": 0,
        "progress_pct": 0,
        "speed_mbps": 0,
        "created_at": _now(),
    }

    def do_add():
        snapshot.init_db()

        existing = snapshot.get_all_tasks()
        for t in existing:
            if t.get("repo_id") == task_meta["repo_id"] and t.get("status") not in ("failed", "revoked"):
                return {"error": f"Already exists: {t['id']} ({t['name']}) status={t['status']}"}

        snapshot.upsert_task(task_meta)

        from ...worker.download import download_dataset
        if auto_transfer:
            from ...transfer.tasks import transfer_to_juicefs
            from celery import chain
            result = chain(
                download_dataset.s(task_meta),
                transfer_to_juicefs.s(),
            ).apply_async(priority=priority, task_id=task_id)
        else:
            result = download_dataset.apply_async(
                args=[task_meta], priority=priority, task_id=task_id,
            )

        snapshot.update_task_progress(task_id, status="pending", phase="queued")
        task_meta["celery_task_id"] = task_id
        snapshot.upsert_task(task_meta)

        return {"ok": True, "task_id": task_id, "name": name, "priority": priority}

    return await _run_blocking(do_add)


@router.post("/queue/reorder")
async def reorder_task(body: dict):
    """Change a task's priority (reorder in queue).

    Body:
        task_id: str
        priority: int (0-9)
    """
    task_id = body.get("task_id", "")
    new_priority = int(body.get("priority", 5))
    new_priority = max(0, min(9, new_priority))

    if not task_id:
        return {"error": "task_id is required"}

    def do_reorder():
        snapshot.init_db()
        task = snapshot.get_task(task_id)
        if not task:
            return {"error": f"Task {task_id} not found"}

        if task["status"] == "downloading":
            conn = snapshot._conn()
            conn.execute("UPDATE tasks SET priority = ? WHERE id = ?", (new_priority, task_id))
            conn.commit()
            return {"ok": True, "task_id": task_id, "priority": new_priority, "note": "priority updated (already running)"}

        app.control.revoke(task_id, terminate=False)

        task["priority"] = new_priority
        snapshot.upsert_task(task)

        from ...worker.download import download_dataset
        download_dataset.apply_async(
            args=[task], priority=new_priority, task_id=task_id,
        )

        return {"ok": True, "task_id": task_id, "priority": new_priority}

    return await _run_blocking(do_reorder)


@router.post("/queue/jump")
async def jump_queue(body: dict):
    """Move a task to the front of the queue (priority=0).

    Body:
        task_id: str
    """
    body["priority"] = 0
    return await reorder_task(body)


@router.post("/queue/interrupt")
async def interrupt_task(body: dict):
    """Interrupt the currently running task on a worker, preserving staging for resume.

    Body:
        task_id: str — the currently-running task to interrupt
        reason: str (optional) — why it's being interrupted
    """
    task_id = body.get("task_id", "")
    if not task_id:
        return {"error": "task_id is required"}

    def do_interrupt():
        snapshot.init_db()
        task = snapshot.get_task(task_id)
        if not task:
            return {"error": f"Task {task_id} not found"}

        if task["status"] != "downloading":
            return {"error": f"Task is not active (status={task['status']})"}

        app.control.revoke(task_id, terminate=True, signal="SIGUSR1")

        snapshot.update_task_progress(task_id, status="pending", phase="interrupted", speed_mbps=0)

        from ...worker.download import download_dataset
        download_dataset.apply_async(
            args=[task], priority=task.get("priority", 5), task_id=task_id,
            countdown=5,
        )

        return {"ok": True, "task_id": task_id, "message": "Task interrupted, will resume from staging"}

    return await _run_blocking(do_interrupt)


@router.post("/queue/pause")
async def pause_task(body: dict):
    """Pause a task — revoke it without re-enqueueing.

    Body:
        task_id: str
    """
    task_id = body.get("task_id", "")
    if not task_id:
        return {"error": "task_id is required"}

    def do_pause():
        snapshot.init_db()
        task = snapshot.get_task(task_id)
        if not task:
            return {"error": f"Task {task_id} not found"}

        if task["status"] == "downloading":
            app.control.revoke(task_id, terminate=True, signal="SIGUSR1")

        app.control.revoke(task_id, terminate=False)
        snapshot.update_task_progress(task_id, status="paused", phase=None, speed_mbps=0)

        return {"ok": True, "task_id": task_id, "status": "paused"}

    return await _run_blocking(do_pause)


@router.post("/queue/resume")
async def resume_task(body: dict):
    """Resume a paused task.

    Body:
        task_id: str
    """
    task_id = body.get("task_id", "")
    if not task_id:
        return {"error": "task_id is required"}

    def do_resume():
        snapshot.init_db()
        task = snapshot.get_task(task_id)
        if not task:
            return {"error": f"Task {task_id} not found"}

        if task["status"] not in ("paused", "failed"):
            return {"error": f"Cannot resume task in status={task['status']}"}

        snapshot.update_task_progress(task_id, status="pending", phase="queued")

        from ...worker.download import download_dataset
        download_dataset.apply_async(
            args=[task], priority=task.get("priority", 5), task_id=task_id,
        )

        return {"ok": True, "task_id": task_id, "status": "pending"}

    return await _run_blocking(do_resume)


@router.delete("/queue/{task_id}")
async def delete_from_queue(task_id: str):
    """Remove a task completely (revoke if active, delete from snapshot)."""
    def do_delete():
        snapshot.init_db()
        task = snapshot.get_task(task_id)
        if not task:
            return {"error": f"Task {task_id} not found"}

        if task["status"] == "downloading":
            app.control.revoke(task_id, terminate=True, signal="SIGTERM")

        app.control.revoke(task_id, terminate=False)
        snapshot.delete_task(task_id)

        return {"ok": True, "task_id": task_id, "deleted": True}

    return await _run_blocking(do_delete)


@router.post("/queue/retry")
async def retry_task(body: dict):
    """Retry a failed task.

    Body:
        task_id: str
        priority: int (optional, default: keep current)
    """
    task_id = body.get("task_id", "")
    if not task_id:
        return {"error": "task_id is required"}

    def do_retry():
        snapshot.init_db()
        task = snapshot.get_task(task_id)
        if not task:
            return {"error": f"Task {task_id} not found"}

        if task["status"] not in ("failed", "revoked", "paused"):
            return {"error": f"Cannot retry task in status={task['status']}"}

        priority = int(body.get("priority", task.get("priority", 5)))
        task["priority"] = priority
        task["status"] = "pending"
        task["error"] = None
        task["error_class"] = None
        task["retry_count"] = (task.get("retry_count") or 0) + 1
        snapshot.upsert_task(task)

        from ...worker.download import download_dataset
        download_dataset.apply_async(
            args=[task], priority=priority, task_id=task_id,
        )

        return {"ok": True, "task_id": task_id, "status": "pending", "retry_count": task["retry_count"]}

    return await _run_blocking(do_retry)


@router.post("/sync")
async def sync_stub():
    """Compatibility stub — sync is no longer needed with Celery."""
    return {"changes": 0, "message": "Sync not needed — Celery manages state automatically"}

