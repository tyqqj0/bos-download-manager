"""Servers API — status and management."""

from fastapi import APIRouter, HTTPException

from ..cache import cache

router = APIRouter(tags=["servers"])


@router.get("/servers")
async def list_servers():
    """Get all servers with live status (from cache)."""
    data = cache.get_servers()
    if not data:
        return {"servers": {}, "message": "Loading..."}
    return {"servers": data}


@router.get("/servers/{key}")
async def get_server(key: str):
    """Get a single server's status."""
    data = cache.get_servers()
    if not data or key not in data:
        raise HTTPException(404, f"Server {key} not found")
    return data[key]


@router.get("/servers/{key}/log")
async def get_server_log(key: str, lines: int = 50):
    """Get recent queue.log lines from a server."""
    import asyncio
    from concurrent.futures import ThreadPoolExecutor

    def _do():
        from ...core.servers import load_servers
        from ...core.ssh import ssh_recent_log
        from ...core.models import Server

        cfgs = load_servers()
        if key not in cfgs:
            return {"error": f"Server {key} not found"}
        cfg = cfgs[key]
        srv = Server(key=key, host=cfg.host, user=cfg.user, path=cfg.path, enabled=cfg.enabled)
        log_text = ssh_recent_log(srv, lines=lines)
        return {"key": key, "log": log_text, "lines": lines}

    result = await asyncio.get_event_loop().run_in_executor(
        ThreadPoolExecutor(max_workers=1), _do
    )
    if "error" in result:
        raise HTTPException(404, result["error"])
    return result


@router.post("/servers/{key}/restart")
async def restart_worker(key: str):
    """Restart the tmux worker on a server."""
    import asyncio
    from concurrent.futures import ThreadPoolExecutor

    def _do():
        from ...core.servers import load_servers
        from ...core.ssh import ssh_server
        from ...core.models import Server

        cfgs = load_servers()
        if key not in cfgs:
            return {"error": f"Server {key} not found"}
        cfg = cfgs[key]
        srv = Server(key=key, host=cfg.host, user=cfg.user, path=cfg.path, enabled=cfg.enabled)

        restart_cmd = (
            "tmux kill-session -t worker 2>/dev/null; "
            f"cd {cfg.path} && "
            "tmux new-session -d -s worker './queue-worker.sh' && "
            "echo OK"
        )
        out, ok = ssh_server(srv, restart_cmd, timeout=15)
        if ok and "OK" in out:
            return {"key": key, "status": "restarted"}
        return {"error": f"Restart failed: {out}"}

    result = await asyncio.get_event_loop().run_in_executor(
        ThreadPoolExecutor(max_workers=1), _do
    )
    if "error" in result:
        raise HTTPException(500, result["error"])
    return result
