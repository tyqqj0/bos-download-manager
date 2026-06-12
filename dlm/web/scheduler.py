"""Background scheduler — auto sync, size refresh, server status polling."""

import asyncio
import re
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict

from .cache import cache

logger = logging.getLogger("dlm.web")

_executor = ThreadPoolExecutor(max_workers=4)

SYNC_INTERVAL = 60
SIZE_INTERVAL = 300
SERVER_INTERVAL = 30


def _load_state_fresh():
    from ..core.state import StateManager
    mgr = StateManager.create()
    return mgr, mgr.load(use_cache=False)


def _load_servers_config():
    from ..core.servers import load_servers
    return load_servers()


def _build_dashboard(state) -> dict:
    tasks = state.tasks
    by_status = {}
    total_downloaded = 0.0
    total_estimated = 0.0

    for t in tasks:
        by_status[t.status] = by_status.get(t.status, 0) + 1
        total_downloaded += t.downloaded_gb
        total_estimated += t.size_gb

    recent = sorted(
        [t for t in tasks if t.status in ("done", "failed") and t.completed_at],
        key=lambda t: t.completed_at or "",
        reverse=True,
    )[:5]

    return {
        "total_tasks": len(tasks),
        "by_status": by_status,
        "total_downloaded_tb": round(total_downloaded / 1000, 2),
        "total_estimated_tb": round(total_estimated / 1000, 2),
        "recent_activity": [asdict(t) for t in recent],
        "updated_at": time.time(),
    }


def _build_server_status(state) -> dict:
    from ..core.servers import load_servers
    from ..core.ssh import ssh_parallel
    from ..core.models import Server

    server_cfgs = load_servers()
    srv_models = [
        Server(key=k, host=c.host, user=c.user, path=c.path, enabled=c.enabled)
        for k, c in server_cfgs.items() if c.enabled and not c.local
    ]

    status_cmd = (
        "tmux has-session -t worker 2>/dev/null && echo WORKER_ALIVE || echo WORKER_DEAD; "
        "cat ~/code/auwomo-tools/current.txt 2>/dev/null || echo ''; "
        "echo '---QUEUE---'; "
        "wc -l < ~/code/auwomo-tools/queue.txt 2>/dev/null || echo 0"
    )
    results = ssh_parallel(srv_models, status_cmd, timeout=10) if srv_models else {}

    servers = {}
    for key, cfg in server_cfgs.items():
        entry = {
            "key": key,
            "host": cfg.host,
            "enabled": cfg.enabled,
            "local": cfg.local,
            "worker_alive": False,
            "current_task": "",
            "queue_depth": 0,
            "active_tasks": [],
            "ssh_ok": True,
        }

        if cfg.local:
            entry["worker_alive"] = True
            entry["ssh_ok"] = True
        elif not cfg.enabled:
            entry["ssh_ok"] = False
        else:
            out, ok = results.get(key, ("", False))
            entry["ssh_ok"] = ok
            if ok:
                lines = out.split("\n")
                entry["worker_alive"] = "WORKER_ALIVE" in (lines[0] if lines else "")
                in_queue = False
                for line in lines[1:]:
                    if "---QUEUE---" in line:
                        in_queue = True
                        continue
                    if in_queue:
                        try:
                            entry["queue_depth"] = int(line.strip())
                        except ValueError:
                            pass
                    elif line.strip():
                        entry["current_task"] = line.strip()

        active = state.active_tasks_for_server(key)
        entry["active_tasks"] = [{"id": t.id, "name": t.name, "status": t.status} for t in active]
        servers[key] = entry

    return servers


def _run_sync(state, mgr) -> int:
    """Run sync logic: parse worker logs, update task statuses. Returns change count."""
    from ..core.ssh import ssh_recent_log, ssh_check_current
    from ..core.models import _now

    changes = 0
    for key, srv in state.servers.items():
        if not srv.enabled:
            continue

        try:
            log_text = ssh_recent_log(srv, lines=100)
            current = ssh_check_current(srv)
        except Exception:
            continue

        done_repos = set()
        failed_repos = {}
        for line in log_text.splitlines():
            m_done = re.search(r"DONE: .+/(download(?:-modelscope)?\.sh)\s+(\S+)", line)
            m_fail = re.search(r"FAILED \(exit (\d+)\): .+/(download(?:-modelscope)?\.sh)\s+(\S+)", line)
            if m_done:
                done_repos.add(m_done.group(2))
            if m_fail:
                failed_repos[m_fail.group(3)] = f"exit {m_fail.group(1)}"

        for task in state.tasks:
            if task.server != key:
                continue
            if task.status in ("done", "skipped", "needs-auth"):
                continue

            if task.repo_id in done_repos and task.status != "done":
                task.status = "done"
                task.completed_at = _now()
                changes += 1
            elif task.repo_id in failed_repos and task.status != "failed":
                task.status = "failed"
                task.error = failed_repos[task.repo_id]
                changes += 1
            elif current and task.repo_id in current and task.status != "downloading":
                task.status = "downloading"
                changes += 1

    if changes:
        mgr.save(state)

    return changes


def _refresh_sizes(state, mgr):
    """Refresh downloaded sizes from BOS API."""
    from ..core.size import fetch_sizes
    from ..core.config import load_config
    from ..core.bos import create_bos_client

    config = load_config()
    bos = create_bos_client(config["BAIDU_AK"], config["BAIDU_SK"], config["BOS_ENDPOINT"])
    sizes = fetch_sizes(bos, state.tasks)
    updated = 0
    for task in state.tasks:
        if task.id in sizes:
            task.downloaded_gb = sizes[task.id]
            updated += 1
    if updated:
        mgr.save(state)
    return updated


async def background_scheduler():
    """Main background loop — runs sync, refreshes sizes and server status."""
    loop = asyncio.get_event_loop()
    last_size_refresh = 0

    # Initial load
    await asyncio.sleep(2)

    while True:
        try:
            mgr, state = await loop.run_in_executor(_executor, _load_state_fresh)

            # Sync: parse worker logs → update task statuses
            changes = await loop.run_in_executor(_executor, _run_sync, state, mgr)
            if changes:
                logger.info(f"Sync: {changes} status changes")
                _, state = await loop.run_in_executor(_executor, _load_state_fresh)

            # Refresh sizes every SIZE_INTERVAL
            now = time.time()
            if now - last_size_refresh > SIZE_INTERVAL:
                count = await loop.run_in_executor(_executor, _refresh_sizes, state, mgr)
                if count:
                    logger.info(f"Size refresh: {count} tasks updated")
                    _, state = await loop.run_in_executor(_executor, _load_state_fresh)
                last_size_refresh = now

            # Update server status cache
            server_data = await loop.run_in_executor(_executor, _build_server_status, state)
            cache.set_servers(server_data)

            # Update dashboard cache
            dashboard_data = _build_dashboard(state)
            dashboard_data["servers"] = server_data
            cache.set_dashboard(dashboard_data)

            # Update tasks cache
            cache.set_tasks({
                "tasks": [asdict(t) for t in state.tasks],
                "categories": state.categories,
                "updated_at": time.time(),
            })

        except Exception as e:
            logger.error(f"Scheduler error: {e}")

        await asyncio.sleep(SERVER_INTERVAL)
