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
