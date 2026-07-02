"""Doctor API — cluster health check and repair.

Simplified for Celery architecture. Most issues are auto-healed by Celery (acks_late),
but we still check for:
- Workers that haven't reported in a while
- Tasks stuck in 'downloading' with no active worker
- Zombie tasks (permanently invalid repos)
"""

import time
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from . import run_blocking

router = APIRouter(tags=["doctor"])

WORKER_TIMEOUT = 180


@router.get("/doctor")
async def diagnose():
    """Run health diagnostics."""
    def _do():
        from ...queue.snapshot import get_all_tasks, get_workers, init_db
        init_db()

        tasks = get_all_tasks()
        workers = get_workers()
        now = time.time()

        offline_workers = []
        for w in workers:
            if now - (w.get("last_seen") or 0) > WORKER_TIMEOUT:
                offline_workers.append({
                    "server_key": w.get("server_key", ""),
                    "hostname": w.get("hostname", ""),
                    "last_seen_ago_s": int(now - (w.get("last_seen") or 0)),
                })

        stuck = []
        for t in tasks:
            if t.get("status") == "downloading":
                age = now - (t.get("updated_at") or 0)
                if age > 600:
                    stuck.append({
                        "task_id": t["id"],
                        "name": t.get("name", ""),
                        "server": t.get("server", ""),
                        "stale_seconds": int(age),
                    })

        failed_repeat = [
            {"task_id": t["id"], "name": t.get("name", ""),
             "retry_count": t.get("retry_count", 0), "error": t.get("error", "")}
            for t in tasks
            if t.get("status") == "failed" and (t.get("retry_count") or 0) >= 5
        ]

        findings = {
            "offline_workers": offline_workers,
            "stuck_tasks": stuck,
            "failed_repeat": failed_repeat,
            "total_issues": len(offline_workers) + len(stuck) + len(failed_repeat),
            "healthy": len(offline_workers) + len(stuck) + len(failed_repeat) == 0,
        }
        return findings

    return await run_blocking(_do)


class FixRequest(BaseModel):
    actions: list[str] = []


@router.post("/doctor")
async def fix(req: FixRequest):
    """Apply repair actions: retry_stuck, remove_zombie."""
    def _do():
        from ...queue.snapshot import get_all_tasks, update_task_progress, init_db
        from ...queue.app import app as celery_app
        init_db()

        tasks = get_all_tasks()
        now = time.time()
        results = {}
        actions = req.actions or ["retry_stuck"]

        if "retry_stuck" in actions:
            retried = []
            for t in tasks:
                if t.get("status") == "downloading" and now - (t.get("updated_at") or 0) > 600:
                    update_task_progress(t["id"], status="pending", phase="retrying")
                    from ...worker.download import download_dataset
                    download_dataset.apply_async(
                        args=[t], priority=t.get("priority", 5), task_id=t["id"],
                    )
                    retried.append(t.get("name", t["id"]))
            results["retry_stuck"] = retried

        if "remove_zombie" in actions:
            removed = []
            for t in tasks:
                if t.get("status") == "failed" and (t.get("retry_count") or 0) >= 8:
                    update_task_progress(t["id"], status="revoked", phase=None)
                    celery_app.control.revoke(t["id"], terminate=False)
                    removed.append(t.get("name", t["id"]))
            results["remove_zombie"] = removed

        return results

    return await run_blocking(_do)
