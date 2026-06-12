"""Tasks API — CRUD operations on download tasks."""

from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from ..cache import cache

router = APIRouter(tags=["tasks"])


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
    cached = cache.get_tasks()
    if not cached:
        return {"tasks": [], "total": 0, "message": "Loading..."}

    tasks = cached["tasks"]

    if status:
        tasks = [t for t in tasks if t["status"] == status]
    else:
        tasks = [t for t in tasks if t["status"] not in ("skipped", "needs-auth")]

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

    return {
        "tasks": tasks,
        "total": len(tasks),
        "categories": cached.get("categories", []),
    }


@router.get("/tasks/{task_id}")
async def get_task(task_id: str):
    """Get a single task by ID."""
    cached = cache.get_tasks()
    if not cached:
        raise HTTPException(404, "State not loaded yet")
    for t in cached["tasks"]:
        if t["id"] == task_id:
            return t
    raise HTTPException(404, f"Task {task_id} not found")


class AddTaskRequest(BaseModel):
    url_or_repo: str
    category: str
    type: str = "dataset"
    priority: str = "P1"
    server: Optional[str] = None
    include: Optional[str] = None
    name: Optional[str] = None
    size_gb: float = 0.0
    no_dispatch: bool = False


@router.post("/tasks")
async def add_task(req: AddTaskRequest):
    """Add a new download task. Auto-dispatches unless no_dispatch=True."""
    import asyncio
    from concurrent.futures import ThreadPoolExecutor

    executor = ThreadPoolExecutor(max_workers=1)
    loop = asyncio.get_event_loop()

    def _do_add():
        from ...core.state import StateManager
        from ...core.parser import parse_repo, build_download_cmd, derive_bos_path
        from ...core.selector import select_server
        from ...core.ssh import ssh_append_queue, ssh_check_queue_contains
        from ...core.models import Task, _now

        parsed = parse_repo(req.url_or_repo)
        if req.type:
            parsed["type"] = req.type

        if parsed["source"] == "unknown":
            return {"error": f"Cannot parse source: {req.url_or_repo}"}

        task_name = req.name or parsed["name"]
        repo_id = parsed["repo_id"]

        mgr = StateManager.create()
        state = mgr.load(use_cache=False)

        existing = state.find_task(repo_id)
        if existing:
            return {"error": f"Task already exists: {existing.name} (status={existing.status}, id={existing.id})"}

        bos_path = derive_bos_path(req.category, repo_id, parsed["type"])
        task = Task(
            id=state.next_task_id(),
            name=task_name,
            repo_id=repo_id,
            source=parsed["source"],
            type=parsed["type"],
            category=req.category,
            bos_path=bos_path,
            size_gb=req.size_gb,
            priority=req.priority,
            include=req.include,
            status="queued",
        )

        if req.server:
            if req.server not in state.servers:
                return {"error": f"Unknown server: {req.server}"}
            task.server = req.server
        elif not req.no_dispatch:
            chosen = select_server(state)
            if chosen:
                task.server = chosen

        state.tasks.append(task)

        if not req.no_dispatch and task.server:
            srv = state.servers[task.server]
            cmd = build_download_cmd(
                repo_id=repo_id,
                source=parsed["source"],
                dtype=parsed["type"],
                category=req.category,
                remote_path=srv.path,
                include=req.include,
                custom_name=req.name,
            )
            if not ssh_check_queue_contains(srv, repo_id):
                ok = ssh_append_queue(srv, cmd)
                if ok:
                    task.status = "dispatched"
                    task.dispatched_at = _now()
            else:
                task.status = "dispatched"

        mgr.save(state)
        from dataclasses import asdict
        return {"task": asdict(task)}

    result = await loop.run_in_executor(executor, _do_add)
    if "error" in result:
        raise HTTPException(400, result["error"])
    return result


class RetryRequest(BaseModel):
    server: Optional[str] = None


@router.post("/tasks/{task_id}/retry")
async def retry_task(task_id: str, req: RetryRequest = None):
    """Retry a failed task."""
    import asyncio
    from concurrent.futures import ThreadPoolExecutor

    executor = ThreadPoolExecutor(max_workers=1)
    loop = asyncio.get_event_loop()

    def _do_retry():
        from ...core.state import StateManager
        from ...core.parser import build_download_cmd
        from ...core.selector import select_server
        from ...core.ssh import ssh_append_queue
        from ...core.models import _now

        mgr = StateManager.create()
        state = mgr.load(use_cache=False)
        task = state.find_task_by_id(task_id)
        if not task:
            return {"error": f"Task not found: {task_id}"}
        if task.status not in ("failed", "queued"):
            return {"error": f"Cannot retry task with status: {task.status}"}

        server_key = (req.server if req else None) or task.server
        if not server_key:
            server_key = select_server(state)
        if not server_key:
            return {"error": "No available server"}

        srv = state.servers[server_key]
        cmd = build_download_cmd(
            repo_id=task.repo_id,
            source=task.source,
            dtype=task.type,
            category=task.category,
            remote_path=srv.path,
            include=task.include,
            custom_name=None,
        )
        ok = ssh_append_queue(srv, cmd)
        if ok:
            task.status = "dispatched"
            task.server = server_key
            task.dispatched_at = _now()
            task.retry_count += 1
            task.error = None
            mgr.save(state)
            return {"status": "dispatched", "server": server_key}
        return {"error": f"SSH dispatch failed to {server_key}"}

    result = await loop.run_in_executor(executor, _do_retry)
    if "error" in result:
        raise HTTPException(400, result["error"])
    return result


class PriorityRequest(BaseModel):
    priority: str


@router.patch("/tasks/{task_id}/priority")
async def update_priority(task_id: str, req: PriorityRequest):
    """Change task priority."""
    from ...constants import PRIORITIES
    if req.priority not in PRIORITIES:
        raise HTTPException(400, f"Invalid priority: {req.priority}")

    def _do():
        from ...core.state import StateManager
        mgr = StateManager.create()
        state = mgr.load(use_cache=False)
        task = state.find_task_by_id(task_id)
        if not task:
            return {"error": f"Task not found: {task_id}"}
        task.priority = req.priority
        mgr.save(state)
        return {"id": task_id, "priority": req.priority}

    import asyncio
    from concurrent.futures import ThreadPoolExecutor
    result = await asyncio.get_event_loop().run_in_executor(
        ThreadPoolExecutor(max_workers=1), _do
    )
    if "error" in result:
        raise HTTPException(404, result["error"])
    return result


@router.post("/tasks/{task_id}/skip")
async def skip_task(task_id: str):
    """Mark task as skipped."""
    def _do():
        from ...core.state import StateManager
        mgr = StateManager.create()
        state = mgr.load(use_cache=False)
        task = state.find_task_by_id(task_id)
        if not task:
            return {"error": f"Task not found: {task_id}"}
        task.status = "skipped"
        mgr.save(state)
        return {"id": task_id, "status": "skipped"}

    import asyncio
    from concurrent.futures import ThreadPoolExecutor
    result = await asyncio.get_event_loop().run_in_executor(
        ThreadPoolExecutor(max_workers=1), _do
    )
    if "error" in result:
        raise HTTPException(404, result["error"])
    return result


class ParseRequest(BaseModel):
    url_or_repo: str


@router.post("/parse")
async def parse_url(req: ParseRequest):
    """Preview-parse a HuggingFace/ModelScope URL."""
    from ...core.parser import parse_repo
    parsed = parse_repo(req.url_or_repo)
    return parsed


class BatchRequest(BaseModel):
    task_ids: list[str]
    action: str  # "dispatch", "retry", "skip"
    server: Optional[str] = None


@router.post("/tasks/batch")
async def batch_action(req: BatchRequest):
    """Batch operations on multiple tasks."""
    if req.action not in ("dispatch", "retry", "skip"):
        raise HTTPException(400, f"Invalid action: {req.action}")

    def _do():
        from ...core.state import StateManager
        from ...core.parser import build_download_cmd
        from ...core.selector import select_server
        from ...core.ssh import ssh_append_queue
        from ...core.models import _now

        mgr = StateManager.create()
        state = mgr.load(use_cache=False)
        results = []

        for tid in req.task_ids:
            task = state.find_task_by_id(tid)
            if not task:
                results.append({"id": tid, "error": "not found"})
                continue

            if req.action == "skip":
                task.status = "skipped"
                results.append({"id": tid, "status": "skipped"})

            elif req.action in ("dispatch", "retry"):
                if task.status not in ("queued", "failed"):
                    results.append({"id": tid, "error": f"cannot {req.action} from {task.status}"})
                    continue

                server_key = req.server or task.server or select_server(state)
                if not server_key:
                    results.append({"id": tid, "error": "no server available"})
                    continue

                srv = state.servers[server_key]
                cmd = build_download_cmd(
                    repo_id=task.repo_id,
                    source=task.source,
                    dtype=task.type,
                    category=task.category,
                    remote_path=srv.path,
                    include=task.include,
                )
                ok = ssh_append_queue(srv, cmd)
                if ok:
                    task.status = "dispatched"
                    task.server = server_key
                    task.dispatched_at = _now()
                    if req.action == "retry":
                        task.retry_count += 1
                        task.error = None
                    results.append({"id": tid, "status": "dispatched", "server": server_key})
                else:
                    results.append({"id": tid, "error": f"SSH failed to {server_key}"})

        mgr.save(state)
        return {"results": results}

    import asyncio
    from concurrent.futures import ThreadPoolExecutor
    return await asyncio.get_event_loop().run_in_executor(
        ThreadPoolExecutor(max_workers=1), _do
    )
