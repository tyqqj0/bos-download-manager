"""Servers API — Celery worker status."""

from fastapi import APIRouter, HTTPException

from ..cache import cache
from . import run_blocking

router = APIRouter(tags=["servers"])


@router.get("/servers")
async def list_servers():
    """Get all workers with live status."""
    def _do():
        from ...queue.snapshot import get_workers, init_db
        init_db()
        workers = get_workers()
        return {w.get("server_key", w["hostname"]): w for w in workers}

    cached = cache.get_servers()
    if cached:
        return {"servers": cached}
    data = await run_blocking(_do)
    return {"servers": data}


@router.get("/servers/{key}")
async def get_server(key: str):
    """Get a single worker's status."""
    data = cache.get_servers()
    if not data or key not in data:
        raise HTTPException(404, f"Worker {key} not found")
    return data[key]


@router.post("/servers/{key}/ping")
async def ping_worker(key: str):
    """Ping a Celery worker."""
    def _do():
        from ...queue.app import app as celery_app
        try:
            inspect = celery_app.control.inspect(
                destination=[f"{key}@*"], timeout=5
            )
            result = inspect.ping() or {}
            if result:
                return {"key": key, "status": "alive", "response": result}
            return {"key": key, "status": "unreachable"}
        except Exception as e:
            return {"key": key, "status": "error", "error": str(e)}

    return await run_blocking(_do)


@router.post("/worker-heartbeat")
async def worker_heartbeat(body: dict):
    """Receive worker heartbeat and update dashboard snapshot."""
    def _do():
        from ...queue.snapshot import init_db, update_worker
        init_db()
        update_worker(
            hostname=body.get("hostname", ""),
            server_key=body["server_key"],
            status=body.get("status", "online"),
            current_task_id=body.get("current_task_id"),
            disk_free_gb=body.get("disk_free_gb"),
        )
        return {"ok": True}

    return await run_blocking(_do)


@router.post("/servers/{key}/cleanup")
async def cleanup_server_staging(key: str):
    """Clean staging directory on a worker for done/failed/revoked tasks.

    Only removes staging dirs whose task is done, failed, or revoked.
    Never touches staging for active (downloading/pending) tasks.
    """
    def _do():
        from ...queue.snapshot import get_all_tasks, init_db
        from ...core.servers import load_servers
        from ...core.ssh import ssh_exec

        init_db()
        tasks = get_all_tasks()

        # Find tasks that are safe to clean (done/failed/revoked)
        safe_to_clean = [
            t["name"] for t in tasks
            if t.get("status") in ("done", "failed", "revoked")
            and t.get("server") == key
        ]

        if not safe_to_clean:
            return {"cleaned": [], "message": "Nothing to clean"}

        servers = load_servers()
        server = servers.get(key)
        if not server:
            return {"error": f"Unknown server: {key}"}

        import re
        import shlex

        cleaned = []
        for name in safe_to_clean:
            if not re.match(r'^[A-Za-z0-9_.\-/]+$', name):
                continue  # skip names with shell metacharacters
            staging_dir = shlex.quote(f"/data/staging/{name}")
            try:
                ssh_exec(server.host, server.user, f"rm -rf {staging_dir}")
                cleaned.append(name)
            except Exception:
                pass  # best effort

        return {"cleaned": cleaned, "count": len(cleaned)}

    return await run_blocking(_do)


@router.post("/task-progress")
async def task_progress(body: dict):
    """Receive real-time task progress from workers."""
    def _do():
        from ...queue.snapshot import init_db, update_task_progress
        init_db()
        task_id = body.get("task_id")
        if not task_id:
            return {"error": "task_id required"}
        kwargs = {}
        for key in ("status", "speed_mbps", "progress_pct", "downloaded_gb", "server", "phase"):
            if key in body:
                kwargs[key] = body[key]
        update_task_progress(task_id, **kwargs)
        return {"ok": True}

    return await run_blocking(_do)
