"""Transfer routes — BOS→地瓜云 transfer status and controls.

All three POST routes here used to be dead. `trigger` and `retry` posted to a
Celery broker that was decommissioned with the Celery download path on
2026-06-30; `pause` wrote the web process's in-memory `cache`, so it silently
un-paused on the next restart. The dashboard rendered all three as working
controls, which is the worst version of broken: clicking them produced no
effect and no error. They now call the same arming path the automatic trigger
uses, and `pause` persists.

`sync` was a fourth one, missed by that sweep because it fails one step
earlier: the ⟳ 立即检查 button fetches a path that no route ever served, so it
404'd rather than no-op'd. `tests/test_servers_view.py` now walks every
`/api/` literal in app.js against the registered routes, which is the check
that would have caught all four.
"""

import logging

from fastapi import APIRouter, HTTPException

from . import run_blocking

logger = logging.getLogger("dlm.web")
router = APIRouter(tags=["transfer"])


@router.get("/transfer")
async def get_transfer_status():
    """Get transfer status for all tasks."""
    def _do():
        from ...queue.snapshot import get_all_tasks, init_db
        from ...transfer.arm import transfers_paused
        init_db()
        all_tasks = get_all_tasks()

        transfer_tasks = [
            {
                "id": t["id"],
                "name": t.get("name", ""),
                "category": t.get("category", ""),
                "size_gb": t.get("size_gb", 0),
                "status": t.get("status", ""),
                "transfer_status": t.get("transfer_status"),
                "transfer_task_id": t.get("transfer_task_id"),
                "transfer_error": t.get("transfer_error"),
                "transfer_prefix": t.get("transfer_prefix"),
                "transfer_bytes": t.get("transfer_bytes", 0),
                # The verification denominator, and the only number that makes
                # `transfer_verified_bytes` mean anything. Measured 2026-08-10 on
                # molmobot-data: shard rows said 3.40 TB, the prefix held 10.32 TB.
                # Exposing the numerator without this was a number with no scale —
                # and on a `short` row it left the shortfall unreadable without
                # opening SQLite by hand.
                "transfer_bos_bytes": t.get("transfer_bos_bytes", 0),
                "transfer_bos_objects": t.get("transfer_bos_objects", 0),
                "transfer_verified_bytes": t.get("transfer_verified_bytes", 0),
                "transfer_armed_at": t.get("transfer_armed_at", 0),
                "bos_path": t.get("bos_path", ""),
            }
            for t in all_tasks
            if t.get("status") == "done" or t.get("transfer_status") is not None
        ]

        def _count(*statuses):
            return sum(1 for t in transfer_tasks
                       if t.get("transfer_status") in statuses)

        summary = {
            "total_done": sum(1 for t in all_tasks if t.get("status") == "done"),
            "transferred": _count("done"),
            "transferring": _count("transferring", "verifying"),
            "failed": _count("failed"),
            # `blocked` and `short` are counted separately rather than folded into
            # `failed`, because they need different human action: a blocked row
            # never started, a short one moved bytes but not all of them.
            "blocked": _count("blocked"),
            "short": _count("short"),
            "ready": _count("ready"),
            "pending": _count(None, "queued"),
        }

        return {
            "tasks": transfer_tasks,
            "summary": summary,
            "paused": transfers_paused(),
        }

    return await run_blocking(_do)


@router.post("/transfer/trigger")
async def trigger_transfer(body: dict = None):
    """Arm transfer for specific tasks, or for every un-armed `done` task.

    Manual arming skips only the "never armed" gate, so a `failed`, `blocked` or
    `short` row can be re-queued from the dashboard. The believability gates
    (prefix drift, shard completeness) still apply — this button cannot force a
    transfer the gates refuse — and a task whose import is already in flight is
    left alone.
    """
    task_ids = (body or {}).get("task_ids", [])

    def _do():
        from ...queue.snapshot import get_all_tasks, init_db
        from ...transfer.arm import maybe_arm_transfer
        init_db()

        armed, skipped = [], []
        for t in get_all_tasks():
            if task_ids and t["id"] not in task_ids:
                continue
            if t.get("status") != "done":
                continue
            # Without an explicit id list this means "arm the backlog", so rows
            # already queued or finished are left as they are. An explicit id is
            # taken as a deliberate re-arm.
            if not task_ids and t.get("transfer_status") is not None:
                continue
            result = maybe_arm_transfer(t["id"], manual=True)
            name = t.get("name", t["id"])
            if result["armed"]:
                armed.append(name)
            else:
                skipped.append(f"{name}: {result['reason']}")

        return {"triggered": armed, "count": len(armed), "skipped": skipped}

    return await run_blocking(_do)


@router.post("/transfer/{task_id}/retry")
async def retry_transfer(task_id: str):
    """Re-arm one task's transfer after a failure, a block, or a short result."""
    def _do():
        from ...queue.snapshot import get_task, init_db
        from ...transfer.arm import maybe_arm_transfer
        init_db()

        if not get_task(task_id):
            return {"error": "Task not found"}
        result = maybe_arm_transfer(task_id, manual=True)
        if not result["armed"]:
            return {"error": result["reason"], "transfer_status": result["status"]}
        return {"ok": True, "transfer_status": result["status"],
                "detail": result["reason"]}

    return await run_blocking(_do)


@router.post("/transfer/pause")
async def pause_transfer(body: dict = None):
    """Pause or resume automatic transfer. Persisted — survives a web restart."""
    paused = bool((body or {}).get("paused", True))

    def _do():
        from ...queue.snapshot import init_db
        from ...transfer.arm import set_transfers_paused, transfers_paused
        init_db()
        set_transfers_paused(paused)
        return {"paused": transfers_paused()}

    return await run_blocking(_do)


@router.post("/transfer/sync")
async def sync_transfer():
    """Poll in-flight transfers now, instead of waiting out the 60s cycle.

    Runs on the SCHEDULER's transfer executor, not this package's pool, and that
    is the whole point of the endpoint rather than an implementation detail:
    that pool has exactly one slot so a stage still stuck inside a BOS or DCloud
    call cannot run beside the next cycle and post the same import twice (see
    scheduler.py's comment on TRANSFER_EXECUTOR_WORKERS). A route with a thread
    of its own would reintroduce that race on demand — from the button an
    operator is most likely to press when a transfer looks stuck, i.e. exactly
    when a stage is hung. Here the click queues behind the hung stage instead.

    Poll only, no dispatch: this answers "did the far side finish?", which is
    what 立即检查 asks. Arming is 全部搬运 (`/transfer/trigger`) and posting
    happens in the automatic cycle within TRANSFER_INTERVAL.
    """
    import asyncio

    from ..scheduler import (
        TRANSFER_STAGE_TIMEOUT,
        _blocking_stage,
        _poll_transfers,
        _transfer_executor,
    )

    report = await _blocking_stage(
        asyncio.get_event_loop(), _poll_transfers, "transfer poll (manual)",
        timeout=TRANSFER_STAGE_TIMEOUT, executor=_transfer_executor)
    if report is None:
        # _blocking_stage logs the reason and returns None for both a timeout
        # and a raise. Surfacing that as `{"updated": 0}` would read as
        # "checked, nothing changed" — the opposite of what happened.
        raise HTTPException(
            503, "poll timed out or failed — see /var/log/dlm-web.log")

    # No cache write: the scheduler stashes its own report under
    # "transfer_poll_report" but nothing reads that key, and mirroring a
    # write-only key here would just look like state this route maintains.
    # The result the caller needs is in the response; the durable result is
    # the `transfer_status` columns the poll already wrote.
    updated = sum(len(report.get(k) or [])
                  for k in ("done", "short", "failed"))
    return {"ok": True, "updated": updated, "report": report,
            "errors": report.get("errors") or []}
