"""Servers API — status and management."""

import re
from typing import List
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

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


def _extract_display_name(line: str) -> str:
    """Extract a readable name from a queue.txt command line."""
    m = re.search(r'download(?:-modelscope)?\.sh\s+(\S+)', line)
    if m:
        repo = m.group(1)
        return repo.split("/")[-1] if "/" in repo else repo
    return line.strip()[:40]


def _extract_repo_id(line: str) -> str:
    """Extract repo_id from a queue.txt command line."""
    m = re.search(r'download(?:-modelscope)?\.sh\s+(\S+)', line)
    return m.group(1) if m else ""


@router.get("/servers/{key}/queue")
async def get_server_queue(key: str):
    """Read the ordered queue.txt for a server."""
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
        out, ok = ssh_server(srv, f"cat {cfg.path}/queue.txt 2>/dev/null")
        if not ok:
            return {"queue": []}
        lines = [l for l in out.splitlines() if l.strip()]
        queue = []
        for line in lines:
            queue.append({
                "line": line,
                "repo_id": _extract_repo_id(line),
                "display": _extract_display_name(line),
            })
        return {"queue": queue}

    result = await asyncio.get_event_loop().run_in_executor(
        ThreadPoolExecutor(max_workers=1), _do
    )
    if "error" in result:
        raise HTTPException(404, result["error"])
    return result


class ReorderRequest(BaseModel):
    order: List[str]


@router.put("/servers/{key}/queue")
async def reorder_server_queue(key: str, body: ReorderRequest):
    """Reorder a server's queue.txt. body.order is the new line order."""
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

        out, ok = ssh_server(srv, f"cat {cfg.path}/queue.txt 2>/dev/null")
        current_lines = [l for l in out.splitlines() if l.strip()] if ok else []

        current_set = set(current_lines)
        ordered = [l for l in body.order if l in current_set]
        remaining = [l for l in current_lines if l not in set(ordered)]
        new_queue = ordered + remaining

        content = "\n".join(new_queue) + "\n" if new_queue else ""
        escaped = content.replace("'", "'\\''")
        write_cmd = f"printf '%s' '{escaped}' > {cfg.path}/queue.txt.tmp && mv {cfg.path}/queue.txt.tmp {cfg.path}/queue.txt"
        _, write_ok = ssh_server(srv, write_cmd, timeout=10)
        if not write_ok:
            return {"error": "Failed to write queue.txt"}

        queue = []
        for line in new_queue:
            queue.append({
                "line": line,
                "repo_id": _extract_repo_id(line),
                "display": _extract_display_name(line),
            })
        return {"queue": queue, "reordered": True}

    result = await asyncio.get_event_loop().run_in_executor(
        ThreadPoolExecutor(max_workers=1), _do
    )
    if "error" in result:
        raise HTTPException(500, result["error"])
    return result
