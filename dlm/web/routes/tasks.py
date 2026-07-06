"""Tasks API — compatibility layer over the new queue system.

Keeps the same endpoint signatures so the existing frontend works unchanged.
Under the hood, routes to Redis/Celery/SQLite.
"""

from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from . import run_blocking

router = APIRouter(tags=["tasks"])

PRIORITY_TO_INT = {"P0": 0, "P1": 2, "P2": 5, "P3": 7}
INT_TO_PRIORITY = {0: "P0", 1: "P0", 2: "P1", 3: "P1", 4: "P2", 5: "P2", 6: "P2", 7: "P3", 8: "P3", 9: "P3"}


def _task_for_frontend(t: dict) -> dict:
    """Map SQLite task row to frontend-expected format."""
    priority_int = t.get("priority", 5)
    if isinstance(priority_int, str):
        priority_str = priority_int
    else:
        priority_str = INT_TO_PRIORITY.get(priority_int, "P2")

    status = t.get("status", "pending")
    status_map = {"pending": "queued", "downloading": "downloading", "done": "done",
                  "failed": "failed", "revoked": "skipped", "paused": "paused",
                  "preempted": "preempted", "transferring": "done"}
    frontend_status = status_map.get(status, status)

    return {
        "id": t.get("id", ""),
        "name": t.get("name", ""),
        "repo_id": t.get("repo_id", ""),
        "source": t.get("source", ""),
        "type": t.get("type", "dataset"),
        "category": t.get("category", ""),
        "bos_path": t.get("bos_path", ""),
        "status": frontend_status,
        "server": t.get("server", ""),
        "priority": priority_str,
        "size_gb": t.get("size_gb", 0) or 0,
        "downloaded_gb": t.get("downloaded_gb", 0) or 0,
        "progress_pct": t.get("progress_pct", 0) or 0,
        "speed_mbps": t.get("speed_mbps", 0) or 0,
        "eta_seconds": None,
        "phase": t.get("phase"),
        "error": t.get("error"),
        "error_class": t.get("error_class"),
        "retry_count": t.get("retry_count", 0) or 0,
        "created_at": t.get("created_at", ""),
        "completed_at": t.get("completed_at"),
        "transfer_status": t.get("transfer_status"),
        "transfer_error": t.get("transfer_error"),
    }


@router.get("/tasks")
async def list_tasks(
    status: Optional[str] = Query(None),
    server: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    priority: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    sort: str = Query("status"),
    reverse: bool = Query(False),
):
    """List tasks with optional filters."""
    def _do():
        from ...queue.snapshot import get_all_tasks, init_db
        from ...constants import CATEGORIES
        init_db()
        raw_tasks = get_all_tasks()
        tasks = [_task_for_frontend(t) for t in raw_tasks]

        if status:
            tasks = [t for t in tasks if t["status"] == status]
        else:
            tasks = [t for t in tasks if t["status"] not in ("skipped",)]

        if server:
            tasks = [t for t in tasks if t.get("server") == server]
        if category:
            tasks = [t for t in tasks if t["category"] == category]
        if priority:
            tasks = [t for t in tasks if t["priority"] == priority]
        if search:
            q = search.lower()
            tasks = [t for t in tasks if q in t["name"].lower() or q in t.get("repo_id", "").lower()]

        STATUS_ORDER = {"downloading": 0, "dispatched": 1, "queued": 2, "failed": 3, "done": 4}
        if sort == "status":
            tasks.sort(key=lambda t: STATUS_ORDER.get(t["status"], 9), reverse=reverse)
        elif sort == "name":
            tasks.sort(key=lambda t: t["name"].lower(), reverse=reverse)
        elif sort == "size":
            tasks.sort(key=lambda t: t.get("size_gb", 0), reverse=not reverse)
        elif sort == "priority":
            tasks.sort(key=lambda t: t.get("priority", "P3"), reverse=reverse)
        elif sort == "server":
            tasks.sort(key=lambda t: t.get("server") or "ZZZ", reverse=reverse)

        return {"tasks": tasks, "total": len(tasks), "categories": CATEGORIES}

    return await run_blocking(_do)


@router.get("/tasks/{task_id}")
async def get_task(task_id: str):
    """Get a single task by ID."""
    def _do():
        from ...queue.snapshot import get_task, init_db
        init_db()
        t = get_task(task_id)
        if not t:
            return None
        return _task_for_frontend(t)

    result = await run_blocking(_do)
    if result is None:
        raise HTTPException(404, f"Task {task_id} not found")
    return result


class AddTaskRequest(BaseModel):
    url_or_repo: str
    category: str = "other"
    type: str = "dataset"
    priority: str = "P1"
    server: Optional[str] = None
    include: Optional[str] = None
    name: Optional[str] = None
    size_gb: float = 0.0
    no_dispatch: bool = False


@router.post("/tasks")
async def add_task(req: AddTaskRequest):
    """Add a new download task via the queue system."""
    def _do():
        from ...core.parser import parse_repo
        from ...queue.snapshot import get_all_tasks, upsert_task, init_db
        from ...worker.download import download_dataset
        from ...transfer.tasks import transfer_to_juicefs
        from celery import chain
        import uuid
        from datetime import datetime, timezone

        init_db()
        parsed = parse_repo(req.url_or_repo)
        if req.type:
            parsed["type"] = req.type

        if parsed["source"] == "unknown":
            return {"error": f"Cannot parse source: {req.url_or_repo}"}

        task_name = req.name or parsed["name"]
        repo_id = parsed["repo_id"]

        existing = [t for t in get_all_tasks()
                    if t.get("repo_id") == repo_id and t.get("status") not in ("failed", "revoked")]
        if existing:
            e = existing[0]
            return {"error": f"Task already exists: {e.get('name')} (status={e.get('status')}, id={e.get('id')})"}

        today = datetime.now(timezone.utc).strftime("%Y%m%d")
        task_id = f"t-{today}-{uuid.uuid4().hex[:6]}"
        priority_int = PRIORITY_TO_INT.get(req.priority, 5)

        if parsed["type"] == "model":
            bos_path = f"auwomo-model/{task_name}/"
        elif req.category and req.category != "other":
            bos_path = f"auwomo-datasets/raw-data/{req.category}/{task_name}/"
        else:
            bos_path = f"auwomo-datasets/raw-data/{task_name}/"

        task_meta = {
            "id": task_id,
            "name": task_name,
            "repo_id": repo_id,
            "source": parsed["source"],
            "type": parsed["type"],
            "category": req.category,
            "bos_path": bos_path,
            "status": "pending",
            "priority": priority_int,
            "size_gb": req.size_gb,
            "downloaded_gb": 0,
            "progress_pct": 0,
            "speed_mbps": 0,
            "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "celery_task_id": task_id,
        }

        upsert_task(task_meta)

        if not req.no_dispatch:
            chain(
                download_dataset.s(task_meta),
                transfer_to_juicefs.s(),
            ).apply_async(priority=priority_int, task_id=task_id)

        return {"task": _task_for_frontend(task_meta)}

    result = await run_blocking(_do)
    if "error" in result:
        raise HTTPException(400, result["error"])
    return result


@router.post("/tasks/{task_id}/retry")
async def retry_task(task_id: str):
    """Retry a failed task via the queue system."""
    def _do():
        from ...queue.snapshot import get_task, upsert_task, init_db
        from ...worker.download import download_dataset
        init_db()

        task = get_task(task_id)
        if not task:
            return {"error": f"Task not found: {task_id}"}
        if task.get("status") not in ("failed", "revoked", "paused", "pending"):
            return {"error": f"Cannot retry task with status: {task.get('status')}"}

        task["status"] = "pending"
        task["error"] = None
        task["error_class"] = None
        task["phase"] = "queued"
        task["speed_mbps"] = 0
        task["retry_count"] = (task.get("retry_count") or 0) + 1
        upsert_task(task)

        priority = task.get("priority", 5)
        if isinstance(priority, str):
            priority = PRIORITY_TO_INT.get(priority, 5)

        download_dataset.apply_async(
            args=[task], priority=priority, task_id=task_id,
        )

        return {"status": "dispatched", "server": "auto"}

    result = await run_blocking(_do)
    if "error" in result:
        raise HTTPException(400, result["error"])
    return result


@router.post("/tasks/{task_id}/skip")
async def skip_task(task_id: str):
    """Skip/revoke a task."""
    def _do():
        from ...queue.snapshot import get_task, update_task_progress, init_db
        from ...queue.app import app as celery_app
        init_db()

        task = get_task(task_id)
        if not task:
            return {"error": f"Task not found: {task_id}"}

        if task.get("status") == "downloading":
            celery_app.control.revoke(task_id, terminate=True, signal="SIGTERM")

        celery_app.control.revoke(task_id, terminate=False)
        update_task_progress(task_id, status="revoked", phase=None, speed_mbps=0)

        return {"id": task_id, "status": "skipped"}

    result = await run_blocking(_do)
    if "error" in result:
        raise HTTPException(400, result["error"])
    return result


@router.delete("/tasks/{task_id}")
async def delete_task(task_id: str):
    """Delete a task."""
    def _do():
        from ...queue.snapshot import get_task, delete_task, init_db
        from ...queue.app import app as celery_app
        init_db()

        task = get_task(task_id)
        if not task:
            return {"error": f"Task not found: {task_id}"}

        if task.get("status") == "downloading":
            celery_app.control.revoke(task_id, terminate=True, signal="SIGTERM")

        celery_app.control.revoke(task_id, terminate=False)
        delete_task(task_id)

        return {"id": task_id, "deleted": True}

    result = await run_blocking(_do)
    if "error" in result:
        raise HTTPException(400, result["error"])
    return result


class ParseRequest(BaseModel):
    url_or_repo: str


@router.post("/parse")
async def parse_url(req: ParseRequest):
    """Preview-parse a HuggingFace/ModelScope URL."""
    from ...core.parser import parse_repo
    return parse_repo(req.url_or_repo)


class BatchRequest(BaseModel):
    task_ids: list[str]
    action: str
    server: Optional[str] = None


@router.post("/tasks/batch")
async def batch_action(req: BatchRequest):
    """Batch operations on multiple tasks."""
    if req.action not in ("retry", "skip"):
        raise HTTPException(400, f"Invalid action: {req.action}")

    def _do():
        from ...queue.snapshot import get_task, upsert_task, update_task_progress, init_db
        from ...queue.app import app as celery_app
        from ...worker.download import download_dataset
        init_db()

        results = []
        for tid in req.task_ids:
            task = get_task(tid)
            if not task:
                results.append({"id": tid, "error": "not found"})
                continue

            if req.action == "skip":
                celery_app.control.revoke(tid, terminate=task.get("status") == "downloading")
                update_task_progress(tid, status="revoked", phase=None, speed_mbps=0)
                results.append({"id": tid, "status": "skipped"})

            elif req.action == "retry":
                if task.get("status") not in ("failed", "revoked", "pending"):
                    results.append({"id": tid, "error": f"cannot retry from {task.get('status')}"})
                    continue

                task["status"] = "pending"
                task["error"] = None
                task["retry_count"] = (task.get("retry_count") or 0) + 1
                upsert_task(task)

                priority = task.get("priority", 5)
                if isinstance(priority, str):
                    priority = PRIORITY_TO_INT.get(priority, 5)
                download_dataset.apply_async(args=[task], priority=priority, task_id=tid)
                results.append({"id": tid, "status": "dispatched"})

        return {"results": results}

    return await run_blocking(_do)
