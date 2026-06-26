"""Background scheduler — auto sync, size refresh, server status polling."""

import asyncio
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
DOCTOR_INTERVAL = 300


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
    countable_statuses = {"queued", "dispatched", "downloading", "done", "failed"}
    total_downloaded = 0.0
    total_estimated = 0.0

    for t in tasks:
        by_status[t.status] = by_status.get(t.status, 0) + 1
        if t.status in countable_statuses:
            total_downloaded += t.downloaded_gb
            if t.size_gb > 0:
                total_estimated += t.size_gb

    active = [t for t in tasks if t.status == "downloading"]
    aggregate_speed = sum(t.speed_mbps for t in active)
    active_downloads = [
        {
            "id": t.id, "name": t.name, "server": t.server,
            "progress_pct": t.progress_pct, "downloaded_gb": t.downloaded_gb,
            "size_gb": t.size_gb, "speed_mbps": t.speed_mbps,
            "eta_seconds": t.eta_seconds, "phase": t.phase,
        }
        for t in active
    ]

    queue_next = [
        {"name": t.name, "server": t.server, "size_gb": t.size_gb}
        for t in sorted(
            [t for t in tasks if t.status == "dispatched"],
            key=lambda t: t.priority or "P3",
        )[:5]
    ]

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
        "aggregate_speed_mbps": round(aggregate_speed, 1),
        "active_downloads": active_downloads,
        "queue_next": queue_next,
        "recent_activity": [asdict(t) for t in recent],
        "updated_at": time.time(),
    }


def _build_server_status(state) -> dict:
    from ..core.servers import load_servers
    from datetime import datetime, timezone

    server_cfgs = load_servers()
    heartbeats = state.worker_heartbeats
    now = datetime.now(timezone.utc)

    servers = {}
    for key, cfg in server_cfgs.items():
        hb = heartbeats.get(key, {})
        alive_at = hb.get("alive_at", "")

        is_alive = False
        age_s = None
        if alive_at:
            try:
                age_s = (now - datetime.fromisoformat(alive_at)).total_seconds()
                is_alive = age_s < 180
            except (ValueError, TypeError):
                pass

        queue_depth = sum(1 for t in state.tasks if t.server == key and t.status == "dispatched")
        active = [t for t in state.tasks if t.server == key and t.status in ("downloading", "dispatched")]

        entry = {
            "key": key,
            "host": cfg.host,
            "enabled": cfg.enabled,
            "local": cfg.local,
            "worker_alive": is_alive,
            "current_task": hb.get("current_task", ""),
            "queue_depth": queue_depth,
            "active_tasks": [{"id": t.id, "name": t.name, "status": t.status} for t in active],
            "ssh_ok": is_alive or cfg.local,
            "disk_free_gb": hb.get("disk_free_gb", 0),
            "pid": hb.get("pid"),
            "alive_at": alive_at,
        }
        servers[key] = entry

    return servers


def _build_alerts(state, servers: dict) -> list:
    """Generate alerts from current state."""
    from datetime import datetime, timezone
    alerts = []
    now = datetime.now(timezone.utc)

    for key, srv in servers.items():
        if srv.get("local"):
            continue
        if not srv.get("worker_alive") and srv.get("enabled", True):
            alive_at = srv.get("alive_at", "")
            duration_min = 0
            if alive_at:
                try:
                    duration_min = int((now - datetime.fromisoformat(alive_at)).total_seconds() / 60)
                except (ValueError, TypeError):
                    pass
            alerts.append({
                "type": "worker_offline",
                "server": key,
                "duration_min": duration_min,
            })

    for t in state.tasks:
        if t.status == "failed" and t.retry_count >= 3:
            alerts.append({
                "type": "task_failed_repeat",
                "task": t.name,
                "count": t.retry_count,
                "error": t.error_class or t.error or "",
            })

    for key, srv in servers.items():
        if srv.get("worker_alive") and srv.get("disk_free_gb", 999) < 20:
            alerts.append({
                "type": "disk_low",
                "server": key,
                "free_gb": srv["disk_free_gb"],
            })

    return alerts


def _run_sync(state, mgr) -> int:
    """Sync is now a no-op: the daemon updates BOS state directly.
    Kept as a hook for future reconciliation logic."""
    return 0


def _refresh_sizes(state, mgr):
    """Refresh downloaded sizes from BOS API."""
    from ..core.size import fetch_sizes
    from ..core.config import load_config
    from ..core.bos import create_bos_client

    config = load_config()
    bos = create_bos_client(config["BAIDU_AK"], config["BAIDU_SK"], config["BOS_ENDPOINT"])
    sizes = fetch_sizes(bos, state.tasks)
    if not sizes:
        return 0
    # Re-load fresh state before saving to avoid overwriting daemon updates
    fresh_state = mgr.load(use_cache=False)
    updated = 0
    for task in fresh_state.tasks:
        if task.id in sizes and sizes[task.id] != task.downloaded_gb:
            task.downloaded_gb = sizes[task.id]
            updated += 1
    if updated:
        mgr.save(fresh_state)
    return updated


def _auto_doctor(state, mgr):
    """Auto-reset stuck downloading tasks (heartbeat stale > 180s)."""
    from .routes.doctor import _find_stuck_downloads

    stuck = _find_stuck_downloads(state)
    if not stuck:
        return []

    fresh = mgr.load(use_cache=False)
    fixed = []
    for item in stuck:
        for t in fresh.tasks:
            if t.id == item["task_id"]:
                t.status = "dispatched"
                t.phase = None
                t.speed_mbps = 0
                t.eta_seconds = None
                t.worker_pid = None
                fixed.append(item["name"])
                break
    if fixed:
        mgr.save(fresh)
    return fixed


async def background_scheduler():
    """Main background loop — runs sync, refreshes sizes and server status."""
    loop = asyncio.get_event_loop()
    last_size_refresh = 0
    last_doctor_run = 0

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

            # Auto-doctor: reset stuck downloads every DOCTOR_INTERVAL
            if now - last_doctor_run > DOCTOR_INTERVAL:
                fixed = await loop.run_in_executor(_executor, _auto_doctor, state, mgr)
                if fixed:
                    logger.info(f"Auto-doctor: reset {len(fixed)} stuck tasks: {fixed}")
                    _, state = await loop.run_in_executor(_executor, _load_state_fresh)
                last_doctor_run = now

            # Update server status cache
            server_data = await loop.run_in_executor(_executor, _build_server_status, state)
            cache.set_servers(server_data)

            # Update dashboard cache
            dashboard_data = _build_dashboard(state)
            dashboard_data["servers"] = server_data
            dashboard_data["alerts"] = _build_alerts(state, server_data)
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
