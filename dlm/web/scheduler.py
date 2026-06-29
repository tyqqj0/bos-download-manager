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
TRANSFER_INTERVAL = 60
MAX_CONCURRENT_TRANSFERS = 3


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
    from datetime import datetime, timezone

    config = load_config()
    bos = create_bos_client(config["BAIDU_AK"], config["BAIDU_SK"], config["BOS_ENDPOINT"])
    sizes = fetch_sizes(bos, state.tasks)
    if not sizes:
        return 0
    # Re-load fresh state before saving to avoid overwriting daemon updates
    fresh_state = mgr.load(use_cache=False)
    now = datetime.now(timezone.utc)
    updated = 0
    for task in fresh_state.tasks:
        if task.id in sizes:
            # Never overwrite worker-reported progress for active downloads
            if task.status == "downloading":
                continue
            # Task might be actively downloading despite dispatched status
            # (happens when retry resets status while worker is still running)
            if task.worker_heartbeat:
                try:
                    hb_age = (now - datetime.fromisoformat(task.worker_heartbeat)).total_seconds()
                    if hb_age < 600:
                        continue
                except (ValueError, TypeError):
                    pass
            if sizes[task.id] != task.downloaded_gb:
                task.downloaded_gb = sizes[task.id]
                updated += 1
    if updated:
        mgr.save(fresh_state)
    return updated


def _auto_doctor(state, mgr):
    """Auto-reset stuck downloading tasks and fix wrongly-dispatched ones."""
    from .routes.doctor import _find_stuck_downloads
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    fresh = mgr.load(use_cache=False)
    changed = False

    # Fix 1: Tasks stuck in "downloading" with stale heartbeat → reset to dispatched
    stuck = _find_stuck_downloads(state)
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
                changed = True
                break

    # Fix 2: Tasks stuck in "dispatched" but worker is still active → restore to downloading
    restored = []
    for t in fresh.tasks:
        if t.status != "dispatched" or not t.worker_heartbeat:
            continue
        try:
            hb_age = (now - datetime.fromisoformat(t.worker_heartbeat)).total_seconds()
            if hb_age < 600:
                t.status = "downloading"
                restored.append(t.name)
                changed = True
        except (ValueError, TypeError):
            pass

    if changed:
        mgr.save(fresh)
    if restored:
        logger.info(f"Auto-doctor: restored {len(restored)} active tasks: {restored}")
    return fixed


def _auto_transfer(state, mgr):
    """Auto-transfer completed tasks to D-Robotics JuiceFS.

    Uses update_task() for atomic writes to avoid race conditions with
    worker heartbeats overwriting transfer_status.
    """
    import os
    from ..core.models import _now

    if cache.get("transfer_paused"):
        return 0

    dcloud_user = os.environ.get("DCLOUD_USER")
    dcloud_pass = os.environ.get("DCLOUD_PASS")
    bos_ak = os.environ.get("BAIDU_AK")
    bos_sk = os.environ.get("BAIDU_SK")

    if not all([dcloud_user, dcloud_pass, bos_ak, bos_sk]):
        return 0

    transferring = [t for t in state.tasks if t.transfer_status == "transferring"]

    # Step B: Poll status of transferring tasks
    updated = 0
    if transferring:
        try:
            from ..transfer.dcloud import DCloudClient
            client = DCloudClient(dcloud_user, dcloud_pass)
            client.login()

            async_tasks = client.list_async_tasks(page_size=100)
            task_status_map = {t.get("task_id"): t for t in async_tasks}

            for task in transferring:
                if not task.transfer_task_id:
                    continue
                remote = task_status_map.get(task.transfer_task_id)
                if not remote:
                    continue
                status = remote.get("status", "")
                if status in ("成功", "success", "done"):
                    mgr.update_task(task.id, {
                        "transfer_status": "done",
                        "transfer_completed_at": _now(),
                        "transfer_error": None,
                    })
                    updated += 1
                    logger.info(f"Transfer done: {task.name}")
                elif status in ("失败", "failed", "error"):
                    mgr.update_task(task.id, {
                        "transfer_status": "failed",
                        "transfer_error": remote.get("error_msg", status),
                        "transfer_completed_at": _now(),
                    })
                    updated += 1
                    logger.warning(f"Transfer failed: {task.name} — {remote.get('error_msg', status)}")
        except Exception as e:
            logger.error(f"Transfer poll error: {e}")

    # Reload state after poll updates
    if updated:
        state = mgr.load(use_cache=False)
    transferring_count = sum(1 for t in state.tasks if t.transfer_status == "transferring")

    # Step A: Trigger new imports (up to MAX_CONCURRENT_TRANSFERS)
    slots = MAX_CONCURRENT_TRANSFERS - transferring_count
    if slots <= 0:
        return updated

    pending = [
        t for t in state.tasks
        if t.status == "done" and t.transfer_status in (None, "queued")
    ]
    if not pending:
        return updated

    try:
        from ..transfer.dcloud import DCloudClient
        from ..constants import DATA_BUCKET, MODEL_BUCKET
        client = DCloudClient(dcloud_user, dcloud_pass)
        client.login()

        triggered = 0
        for task in pending:
            if triggered >= slots:
                break

            bos_path = task.bos_path.lstrip("/")
            if task.type == "model":
                bos_bucket = MODEL_BUCKET
                if task.category:
                    target_path = f"/727a2f92-30c/auwomo-model/{task.category}/{task.name}"
                else:
                    target_path = f"/727a2f92-30c/auwomo-model/{task.name}"
            else:
                bos_bucket = DATA_BUCKET
                if task.category:
                    target_path = f"/727a2f92-30c/auwomo-datasets/raw-data/{task.category}/{task.name}"
                else:
                    target_path = f"/727a2f92-30c/auwomo-datasets/raw-data/{task.name}"

            try:
                if task.category:
                    try:
                        base = "/727a2f92-30c/auwomo-model/" if task.type == "model" else "/727a2f92-30c/auwomo-datasets/raw-data/"
                        client.create_folder(base, task.category)
                    except Exception:
                        pass
                task_id = client.import_from_bos(
                    bos_ak=bos_ak,
                    bos_sk=bos_sk,
                    bos_bucket=bos_bucket,
                    bos_path=bos_path,
                    target_path=target_path,
                )
                mgr.update_task(task.id, {
                    "transfer_status": "transferring",
                    "transfer_task_id": task_id,
                    "transfer_started_at": _now(),
                    "transfer_error": None,
                })
                triggered += 1
                updated += 1
                logger.info(f"Transfer started: {task.name} → {target_path} (task_id={task_id})")
            except Exception as e:
                mgr.update_task(task.id, {
                    "transfer_status": "failed",
                    "transfer_error": str(e),
                })
                updated += 1
                logger.error(f"Transfer trigger failed for {task.name}: {e}")
    except Exception as e:
        logger.error(f"Transfer trigger error: {e}")

    # Step C: Auto-retry failed (max 3 attempts)
    try:
        fresh = mgr.load(use_cache=False)
        retried = 0
        for task in fresh.tasks:
            if task.transfer_status == "failed" and task.status == "done":
                mgr.update_task(task.id, {
                    "transfer_status": "queued",
                    "transfer_error": None,
                })
                retried += 1
                if retried >= 2:
                    break
        if retried:
            updated += retried
            logger.info(f"Transfer auto-retry: queued {retried} failed tasks")
    except Exception as e:
        logger.debug(f"Transfer retry check failed: {e}")

    return updated


async def background_scheduler():
    """Main background loop — runs sync, refreshes sizes and server status."""
    loop = asyncio.get_event_loop()
    last_size_refresh = 0
    last_doctor_run = 0
    last_transfer_run = 0

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

            # Auto-transfer: push done tasks to D-Robotics
            if now - last_transfer_run > TRANSFER_INTERVAL:
                count = await loop.run_in_executor(_executor, _auto_transfer, state, mgr)
                if count:
                    logger.info(f"Auto-transfer: {count} tasks updated")
                    _, state = await loop.run_in_executor(_executor, _load_state_fresh)
                last_transfer_run = now

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
