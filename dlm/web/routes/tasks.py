"""Tasks API — compatibility layer over the new queue system.

Keeps the same endpoint signatures so the existing frontend works unchanged.
Under the hood, routes to Redis/Celery/SQLite.
"""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from . import run_blocking

logger = logging.getLogger("dlm.web")

router = APIRouter(tags=["tasks"])

PRIORITY_TO_INT = {"P0": 0, "P1": 2, "P2": 5, "P3": 7}
INT_TO_PRIORITY = {0: "P0", 1: "P0", 2: "P1", 3: "P1", 4: "P2", 5: "P2", 6: "P2", 7: "P3", 8: "P3", 9: "P3"}


def _task_for_frontend(t: dict) -> dict:
    """Map SQLite task row to frontend-expected format."""
    priority_int = t.get("priority", 5)
    if isinstance(priority_int, str):
        priority_str = priority_int
    else:
        priority_str = INT_TO_PRIORITY.get(priority_int, "P2")

    status = t.get("status", "pending")
    status_map = {"pending": "queued", "downloading": "downloading", "done": "done",
                  "failed": "failed", "revoked": "skipped", "paused": "paused",
                  "preempted": "preempted", "transferring": "done"}
    frontend_status = status_map.get(status, status)

    return {
        "id": t.get("id", ""),
        "name": t.get("name", ""),
        "repo_id": t.get("repo_id", ""),
        "source": t.get("source", ""),
        "type": t.get("type", "dataset"),
        "category": t.get("category", ""),
        "bos_path": t.get("bos_path", ""),
        "status": frontend_status,
        "server": t.get("server", ""),
        "priority": priority_str,
        "size_gb": t.get("size_gb", 0) or 0,
        "downloaded_gb": t.get("downloaded_gb", 0) or 0,
        "progress_pct": t.get("progress_pct", 0) or 0,
        "speed_mbps": t.get("speed_mbps", 0) or 0,
        "eta_seconds": None,
        "phase": t.get("phase"),
        "error": t.get("error"),
        "error_class": t.get("error_class"),
        "retry_count": t.get("retry_count", 0) or 0,
        "created_at": t.get("created_at", ""),
        "completed_at": t.get("completed_at"),
        "transfer_status": t.get("transfer_status"),
        "transfer_error": t.get("transfer_error"),
        "total_shards": t.get("total_shards", 0) or 0,
        "done_shards": t.get("done_shards", 0) or 0,
        # The main table's row needs to tell a pool task's unit (batches)
        # from a sharded task's (shards) without a second round trip — see
        # index.html's per-task "N/M shards" button. Coalesced the same way
        # every other dispatch_mode consumer does (queue.py, doctor.py):
        # the column can be NULL on an old row, and NULL must read as the
        # pre-pool default, not as a falsy "no mode".
        "dispatch_mode": t.get("dispatch_mode") or "sharded",
        # What was ASKED for (0 = auto), as opposed to total_shards, which is
        # how many shard rows the coordinator actually created after capping
        # the request at the idle same-source workers. Sharded mode only —
        # a pool task sizes its own batches and leaves this at 0.
        "shard_count": t.get("max_workers", 0) or 0,
        # The page a human opens to look at (or request access to) the source.
        # Derived, never stored: the inputs are source/repo_id/type, all of
        # which are already on the row, and a stored copy would go stale the
        # moment any of them was corrected. Empty when the row predates
        # repo_id or names a source we have no URL scheme for.
        "source_url": _source_url(t),
        # Why a paused row is paused. NULL on an ordinary operator pause, which
        # is what lets the dashboard show an approval banner on exactly the
        # rows that need one, and lets the recheck loop leave the others alone.
        "hold_reason": t.get("hold_reason"),
        "hold_detail": t.get("hold_detail"),
        # The work a resume already skipped because it was on BOS. Sent so the
        # UI can show progress against the ORIGINAL total: after a re-list
        # `size_gb` is only the remaining work, so 25/61 batches of a resumed
        # task rendered as 38.1% when 66.2% of the dataset was actually down.
        "resume_skipped_gb": t.get("resume_skipped_gb", 0) or 0,
    }


def _source_url(t: dict) -> str:
    """The hub page for a task's repo. See `source_url` above for why derived."""
    repo_id = (t.get("repo_id") or "").strip()
    if not repo_id or "/" not in repo_id:
        return ""
    source = t.get("source") or ""
    is_model = (t.get("type") or "dataset") == "model"
    if source == "hf":
        return (f"https://huggingface.co/{repo_id}" if is_model
                else f"https://huggingface.co/datasets/{repo_id}")
    if source == "modelscope":
        return (f"https://modelscope.cn/models/{repo_id}" if is_model
                else f"https://modelscope.cn/datasets/{repo_id}")
    return ""


@router.get("/tasks")
async def list_tasks(
    status: Optional[str] = Query(None),
    server: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    priority: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    sort: str = Query("status"),
    reverse: bool = Query(False),
):
    """List tasks with optional filters."""
    def _do():
        from ...queue.snapshot import get_all_tasks, init_db
        from ...constants import CATEGORIES
        init_db()
        raw_tasks = get_all_tasks()
        tasks = [_task_for_frontend(t) for t in raw_tasks]

        if status:
            tasks = [t for t in tasks if t["status"] == status]
        else:
            tasks = [t for t in tasks if t["status"] not in ("skipped",)]

        if server:
            tasks = [t for t in tasks if t.get("server") == server]
        if category:
            tasks = [t for t in tasks if t["category"] == category]
        if priority:
            tasks = [t for t in tasks if t["priority"] == priority]
        if search:
            q = search.lower()
            tasks = [t for t in tasks if q in t["name"].lower() or q in t.get("repo_id", "").lower()]

        STATUS_ORDER = {"downloading": 0, "dispatched": 1, "queued": 2, "failed": 3, "done": 4}
        if sort == "status":
            tasks.sort(key=lambda t: STATUS_ORDER.get(t["status"], 9), reverse=reverse)
        elif sort == "name":
            tasks.sort(key=lambda t: t["name"].lower(), reverse=reverse)
        elif sort == "size":
            tasks.sort(key=lambda t: t.get("size_gb", 0), reverse=not reverse)
        elif sort == "priority":
            tasks.sort(key=lambda t: t.get("priority", "P3"), reverse=reverse)
        elif sort == "server":
            tasks.sort(key=lambda t: t.get("server") or "ZZZ", reverse=reverse)

        return {"tasks": tasks, "total": len(tasks), "categories": CATEGORIES}

    return await run_blocking(_do)


@router.get("/tasks/{task_id}")
async def get_task(task_id: str):
    """Get a single task by ID."""
    def _do():
        from ...queue.snapshot import get_task, init_db
        init_db()
        t = get_task(task_id)
        if not t:
            return None
        return _task_for_frontend(t)

    result = await run_blocking(_do)
    if result is None:
        raise HTTPException(404, f"Task {task_id} not found")
    return result


@router.get("/tasks/{task_id}/missing-files")
async def list_task_missing_files(task_id: str, limit: int | None = None):
    """Files that entered this task's filelist and never reached BOS.

    NOT a complete inventory of what the dataset is missing. Files the source
    reports as 0 bytes are dropped during listing (the HF and ModelScope list
    activities both require a positive size), so they never enter a filelist
    and cannot be recorded here — ModelScope's RoboDojo depth files are
    exactly that shape. An empty list means "nothing we tried to transfer was
    lost", not "nothing is missing".

    `limit` caps the rows returned and reports `truncated`; without it the
    whole archive comes back, which is what an operator reading the list
    wants. The finalize re-check always passes one, because an archive written
    under a systemic fault can hold six figures of rows and this handler
    serialises them on S1's blocking pool (review GAP-1).
    """
    def _do():
        from ...queue.snapshot import get_task, init_db, list_missing_files
        init_db()
        if not get_task(task_id):
            return None
        rows = list_missing_files(task_id)
        total = len(rows)
        truncated = limit is not None and total > max(0, limit)
        if truncated:
            rows = rows[:max(0, limit)]
        return {"task_id": task_id, "count": len(rows), "total": total,
                "truncated": truncated, "files": rows}

    result = await run_blocking(_do)
    if result is None:
        raise HTTPException(404, f"Task {task_id} not found")
    return result


class ClearMissingFilesRequest(BaseModel):
    paths: list[str]


@router.delete("/tasks/{task_id}/missing-files")
async def clear_task_missing_files(task_id: str, req: ClearMissingFilesRequest):
    """Drop rows a re-check proved present on BOS after all.

    Exists because the re-check runs as a worker activity, and workers cannot
    touch SQLite — every piece of worker-side state travels over HTTP. Without
    this endpoint clear_missing_files() would be reachable only from S1, and
    the activity that does the verifying could not act on its own result.
    """
    def _do():
        from ...queue.snapshot import clear_missing_files, get_task, init_db
        init_db()
        if not get_task(task_id):
            return None
        remaining = clear_missing_files(task_id, req.paths)
        return {"ok": True, "cleared": len(req.paths), "remaining": remaining}

    result = await run_blocking(_do)
    if result is None:
        raise HTTPException(404, f"Task {task_id} not found")
    return result


class MissingLimitRequest(BaseModel):
    limit: int


@router.post("/tasks/{task_id}/missing-limit")
async def set_task_missing_limit(task_id: str, req: MissingLimitRequest):
    """Record the missing-file ceiling the coordinator judged this task by.

    Separate from the DELETE above because it is a different fact with a
    different lifetime: the DELETE retracts rows a re-check disproved, while
    this stores the threshold that made `done`-with-gaps a deliberate verdict
    rather than a silent one. Alerting reads it straight off the task row (see
    the `missing_files_limit` comment in snapshot.py) — it has no other way to
    learn the listing file count the limit was derived from.
    """
    def _do():
        from ...queue.snapshot import get_task, init_db, set_missing_limit
        init_db()
        if not get_task(task_id):
            return None
        count = set_missing_limit(task_id, req.limit)
        return {"ok": True, "limit": max(0, req.limit), "missing_files_count": count}

    result = await run_blocking(_do)
    if result is None:
        raise HTTPException(404, f"Task {task_id} not found")
    return result


class AddTaskRequest(BaseModel):
    url_or_repo: str
    category: str = "other"
    type: str = "dataset"
    # P2, not P1: P1 maps to int 2, which is inside the pool weight-boost band
    # (fleet.pool_task_weight grants 1.5x the batch window at priority <= 2) and
    # is preferred by the preempt victim sort. A caller who never mentions
    # priority is not asking for either, so the default has to sit outside the
    # band — the same correction the web form got.
    priority: str = "P2"
    server: Optional[str] = None
    include: Optional[str] = None
    name: Optional[str] = None
    size_gb: float = 0.0
    no_dispatch: bool = False
    dispatch_mode: Optional[str] = None
    # 0 = auto (coordinator sizes it from total_bytes). The coordinator caps
    # whatever is asked for at the number of idle same-source workers, so a
    # request of 8 on a fleet with 4 free hosts yields 4 shards, not a stall.
    # Sharded mode only; pool mode chunks its own batches and ignores this.
    shard_count: int = 0
    # Override the source parse_repo inferred. `dlm add --source` has always
    # offered this and the body never carried it, so a bare `org/name` that
    # lives on ModelScope was silently filed as hf — and hf tasks only ever
    # dispatch to the HK fleet, which cannot reach ModelScope.
    source: Optional[str] = None


@router.post("/tasks")
async def add_task(req: AddTaskRequest):
    """Add a new download task — saves to pending, auto_dispatch picks it up.

    Two gates run before anything is stored, in this order:

      1. the parse must yield a real org/name repo (a `collections/` page or a
         bare `#dataset` used to become a task row and fail at listing);
      2. for hf sources, an authorised probe of the resolve endpoint must not
         come back 403 or 404 — see dlm/web/preflight.py for why the repo's
         own `gated` field cannot answer this.

    A 403 does not refuse the add: the repo is real and wanted, it just needs a
    human to click Agree. The row is stored `paused` with
    hold_reason='needs_approval' so the dashboard can show the reason and the
    30-minute recheck loop can release it once approval lands. Refusing instead
    would lose the operator's intent and the size estimate they typed.
    """
    from ..fleet import VALID_DISPATCH_MODES
    from .. import preflight as pf_mod
    from ...core.parser import parse_repo

    if req.dispatch_mode is not None and req.dispatch_mode not in VALID_DISPATCH_MODES:
        raise HTTPException(
            400,
            f"invalid dispatch_mode={req.dispatch_mode!r}, "
            f"expected one of {sorted(VALID_DISPATCH_MODES)}",
        )

    # Parsing is pure and cheap, so it happens out here rather than inside the
    # blocking worker: the preflight below needs its repo_id and source, and
    # both gates should reject before a DB thread is occupied.
    parsed = parse_repo(req.url_or_repo)
    if req.type:
        parsed["type"] = req.type
    if req.source:
        if req.source not in ("hf", "modelscope"):
            raise HTTPException(
                400, f"Unknown source: {req.source} (expected hf or modelscope)")
        parsed["source"] = req.source
        # An explicit source is not a guess, so a 404 below must not suggest
        # the caller "try modelscope" when modelscope is what they said.
        parsed["source_guessed"] = False
        parsed["error"] = None

    if parsed.get("error"):
        raise HTTPException(400, parsed["error"])
    if parsed["source"] == "unknown":
        raise HTTPException(400, f"Cannot parse source: {req.url_or_repo}")

    pf = await pf_mod.check_repo_access(
        parsed["repo_id"], parsed["source"], parsed["type"])
    if pf.outcome == pf_mod.NOT_FOUND:
        hint = (
            "；如果它在 ModelScope 上，请显式指定 source=modelscope"
            if parsed.get("source_guessed") else ""
        )
        raise HTTPException(400, f"{pf.detail}{hint}")

    def _do():
        from ...core.bos import bos_target
        from ...queue import snapshot
        from ...queue.snapshot import get_all_tasks, upsert_task, init_db
        from ..fleet import DEFAULT_DISPATCH_MODE
        import uuid
        from datetime import datetime, timezone
        from types import SimpleNamespace

        init_db()

        task_name = req.name or parsed["name"]
        repo_id = parsed["repo_id"]

        existing = [t for t in get_all_tasks()
                    if t.get("repo_id") == repo_id and t.get("status") not in ("failed", "revoked")]
        if existing:
            e = existing[0]
            return {"error": f"Task already exists: {e.get('name')} (status={e.get('status')}, id={e.get('id')})"}

        today = datetime.now(timezone.utc).strftime("%Y%m%d")
        task_id = f"t-{today}-{uuid.uuid4().hex[:6]}"
        priority_int = PRIORITY_TO_INT.get(req.priority, 5)
        needs_approval = pf.outcome == pf_mod.NEEDS_APPROVAL

        # Ask bos_target for the prefix rather than re-deriving it. This used to
        # build "auwomo-datasets/raw-data/{category}/{name}/" by hand — a bucket
        # name that does not exist glued to a prefix scheme the uploader
        # abandoned. Whatever this row says has to be the same prefix the
        # uploader writes to and the resume filter reads back, so there is
        # exactly one place that decides it.
        #
        # The namespace mirrors the TaskInput start_sharded_download will build
        # from this row (temporal_client.py) field for field — category passed
        # through verbatim, "other" included, since bos_target reads any
        # non-empty category as a path segment and really does write to
        # other/{name}/.
        _, bos_path = bos_target(SimpleNamespace(
            type=parsed["type"], name=task_name, category=req.category,
        ))

        task_meta = {
            "id": task_id,
            "name": task_name,
            "repo_id": repo_id,
            "source": parsed["source"],
            "type": parsed["type"],
            "category": req.category,
            "bos_path": bos_path,
            # `paused` is what "queued but not dispatched" means to the rest of
            # the system: auto_dispatch_pending() claims `pending` rows only,
            # and /api/queue/resume takes it from paused to pending when the
            # operator is ready. Writing `pending` here (what this did) meant
            # no_dispatch was decorative — the next 30s cycle claimed it.
            #
            # A needs-approval row lands here too: dispatching it would recreate
            # the failure this whole check exists to stop.
            "status": "paused" if (req.no_dispatch or needs_approval) else "pending",
            "priority": priority_int,
            "size_gb": req.size_gb,
            "downloaded_gb": 0,
            "progress_pct": 0,
            "speed_mbps": 0,
            "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            # celery_task_id is deliberately not written: nothing reads it
            # (only snapshot.py's schema/field list and one test fixture
            # mention it), and hotfix 1c5814a dropped the vestigial write.
            "dispatch_mode": req.dispatch_mode or DEFAULT_DISPATCH_MODE,
        }
        if req.shard_count > 0:
            task_meta["max_workers"] = req.shard_count

        upsert_task(task_meta)

        payload = _task_for_frontend(task_meta)
        if needs_approval:
            stamp = datetime.now(timezone.utc).strftime("%m-%d %H:%M UTC")
            detail = f"{stamp} 添加时预检：{pf.detail}"
            snapshot.set_hold(task_id, "needs_approval", detail)
            payload["hold_reason"] = "needs_approval"
            payload["hold_detail"] = detail
        return {"task": payload}

    result = await run_blocking(_do)
    if "error" in result:
        raise HTTPException(400, result["error"])
    return result


@router.post("/tasks/{task_id}/retry")
async def retry_task(task_id: str):
    """Retry a task — delegates to /api/queue/retry.

    This is the endpoint the dashboard's Retry button calls (static/app.js).
    It used to set status=pending on its own, which is the hazard /queue/retry
    was hardened against: the live coordinator and its children keep
    downloading into /data/staging while the task becomes dispatchable again,
    so the next 30s cycle stacks a second pipeline on the same hosts and the
    new coordinator collides with the old children on their deterministic
    IDs. One implementation, reachable from both paths.
    """
    from .queue import retry_task as queue_retry

    result = await queue_retry({"task_id": task_id})
    if "error" in result:
        raise HTTPException(400, result["error"])
    return {"status": "pending", "message": "Task queued for auto-dispatch",
            **{k: v for k, v in result.items() if k != "ok"}}


@router.post("/tasks/{task_id}/skip")
async def skip_task(task_id: str, force: bool = False):
    """Skip/revoke a task, terminating its workflows first.

    Order and honesty both matter here. Marking the row `revoked` while the
    pipeline is still alive is the worst outcome available: /api/task-progress
    discards reports for terminal tasks, so the dashboard shows a stopped task
    while bytes keep landing on BOS and staging keeps filling. The old code
    swallowed every cancel error (`except Exception: pass`) and returned 200
    regardless.

    So: terminate first, mark revoked only once Temporal confirms closed. If
    termination fails the state is left alone and the call reports it —
    `?force=true` marks the row anyway for the case where Temporal itself is
    down and the operator accepts an untracked pipeline.
    """
    from ..temporal_client import terminate_workflow_and_wait

    def _get():
        from ...queue.snapshot import get_task, init_db
        init_db()
        return get_task(task_id)

    task = await run_blocking(_get)
    if not task:
        raise HTTPException(400, f"Task not found: {task_id}")

    closed = False
    try:
        closed = await terminate_workflow_and_wait(task_id)
    except Exception as e:
        logger.error(f"skip {task_id}: terminate failed: {e}")
        if not force:
            raise HTTPException(502, (
                f"could not terminate workflows for {task_id}: {e} — task state "
                "unchanged. Retry, or pass ?force=true to revoke the row anyway "
                "(the pipeline may keep running untracked)."
            ))
    if not closed and not force:
        raise HTTPException(502, (
            f"workflows for {task_id} did not close within timeout — task state "
            "unchanged. Retry, or pass ?force=true to revoke the row anyway "
            "(the pipeline may keep running untracked)."
        ))

    def _do():
        from ...queue.snapshot import update_task_progress, init_db
        init_db()
        update_task_progress(task_id, status="revoked", phase=None, speed_mbps=0)
        return {"id": task_id, "status": "skipped", "workflows_closed": closed}

    return await run_blocking(_do)


@router.delete("/tasks/{task_id}")
async def delete_task(task_id: str, force: bool = False):
    """Delete a task, terminating its workflows first.

    The rows are the only handle on the running work: deleting them first
    (what this did) removes the shards from get_running_shards(), so
    busy_servers frees hosts that are still writing to /data/staging and
    terminate loses the children it would have found through those rows.
    """
    from ..temporal_client import terminate_workflow_and_wait

    def _get():
        from ...queue.snapshot import get_task, init_db
        init_db()
        return get_task(task_id)

    task = await run_blocking(_get)
    if not task:
        raise HTTPException(400, f"Task not found: {task_id}")

    closed = False
    try:
        closed = await terminate_workflow_and_wait(task_id)
    except Exception as e:
        logger.error(f"delete {task_id}: terminate failed: {e}")
        if not force:
            raise HTTPException(502, (
                f"could not terminate workflows for {task_id}: {e} — nothing "
                "deleted. Retry, or pass ?force=true."
            ))
    if not closed and not force:
        raise HTTPException(502, (
            f"workflows for {task_id} did not close within timeout — nothing "
            "deleted. Retry, or pass ?force=true."
        ))

    def _do():
        from ...queue.snapshot import delete_task as db_delete, init_db
        init_db()
        db_delete(task_id)
        return {"id": task_id, "deleted": True, "workflows_closed": closed}

    return await run_blocking(_do)


class ParseRequest(BaseModel):
    url_or_repo: str


@router.post("/parse")
async def parse_url(req: ParseRequest):
    """Preview-parse a HuggingFace/ModelScope URL."""
    from ...core.parser import parse_repo
    return parse_repo(req.url_or_repo)


class BatchRequest(BaseModel):
    task_ids: list[str]
    action: str
    server: Optional[str] = None


@router.post("/tasks/batch")
async def batch_action(req: BatchRequest):
    """Batch operations on multiple tasks."""
    if req.action not in ("retry", "skip"):
        raise HTTPException(400, f"Invalid action: {req.action}")

    from ..temporal_client import cancel_workflow

    def _do():
        from ...queue.snapshot import get_task, upsert_task, update_task_progress, init_db
        init_db()

        results = []
        for tid in req.task_ids:
            task = get_task(tid)
            if not task:
                results.append({"id": tid, "error": "not found"})
                continue

            if req.action == "skip":
                update_task_progress(tid, status="revoked", phase=None, speed_mbps=0)
                results.append({"id": tid, "status": "skipped"})

            elif req.action == "retry":
                if task.get("status") not in ("failed", "revoked", "pending"):
                    results.append({"id": tid, "error": f"cannot retry from {task.get('status')}"})
                    continue

                task["status"] = "pending"
                task["error"] = None
                task["retry_count"] = (task.get("retry_count") or 0) + 1
                upsert_task(task)
                results.append({"id": tid, "status": "pending"})

        return {"results": results}

    result = await run_blocking(_do)

    # Cancel Temporal workflows for skipped tasks (best effort)
    if req.action == "skip":
        for tid in req.task_ids:
            try:
                await cancel_workflow(tid)
            except Exception:
                pass

    return result
