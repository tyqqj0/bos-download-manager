"""Actions API — sync, dispatch, and other operations."""

from fastapi import APIRouter

from . import run_blocking

router = APIRouter(tags=["actions"])


@router.post("/sync")
async def trigger_sync():
    """Manually trigger a sync cycle (normally runs every 60s in background)."""
    from ..scheduler import _load_state_fresh, _run_sync, _build_dashboard, _build_server_status
    from ..cache import cache
    from dataclasses import asdict

    def _do():
        mgr, state = _load_state_fresh()
        changes = _run_sync(state, mgr)
        if changes:
            _, state_new = _load_state_fresh()
        else:
            state_new = state

        server_data = _build_server_status(state_new)
        cache.set_servers(server_data)

        dashboard_data = _build_dashboard(state_new)
        dashboard_data["servers"] = server_data
        cache.set_dashboard(dashboard_data)

        cache.set_tasks({
            "tasks": [asdict(t) for t in state_new.tasks],
            "categories": state_new.categories,
        })
        return {"changes": changes}

    return await run_blocking(_do)


@router.post("/dispatch")
async def dispatch_queued():
    """Dispatch all queued tasks to servers."""
    def _do():
        from ...core.state import StateManager
        from ...core.parser import build_download_cmd
        from ...core.selector import select_server
        from ...core.ssh import ssh_append_queue
        from ...core.models import _now

        mgr = StateManager.create()
        state = mgr.load(use_cache=False)
        dispatched = []

        queued = [t for t in state.tasks if t.status == "queued"]
        for task in queued:
            server_key = task.server or select_server(state)
            if not server_key:
                break

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
                dispatched.append({"id": task.id, "name": task.name, "server": server_key})

        if dispatched:
            mgr.save(state)
        return {"dispatched": dispatched, "count": len(dispatched)}

    return await run_blocking(_do)
