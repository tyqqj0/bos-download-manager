"""Queue management API — Temporal-based dispatch."""

import logging
import time
import uuid
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

import asyncio
from fastapi import APIRouter

from ...core.naming import shard_row_id
from ...queue import snapshot
from ..fleet import TERMINAL_STATUSES

logger = logging.getLogger("dlm.web")
router = APIRouter(tags=["queue"])

_executor = ThreadPoolExecutor(max_workers=4)


def _run_blocking(fn, *args):
    loop = asyncio.get_event_loop()
    return loop.run_in_executor(_executor, fn, *args)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _next_task_id() -> str:
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    return f"t-{today}-{uuid.uuid4().hex[:6]}"


@router.get("/queue")
async def list_queue():
    """List all tasks with their current state."""
    def do_list():
        snapshot.init_db()
        tasks = snapshot.get_all_tasks()
        workers = snapshot.get_workers()
        return {"tasks": tasks, "workers": workers}
    return await _run_blocking(do_list)


@router.get("/queue/pending")
async def list_pending():
    def do_list():
        snapshot.init_db()
        return {"tasks": snapshot.get_tasks_by_status("pending")}
    return await _run_blocking(do_list)


@router.get("/queue/active")
async def list_active():
    def do_list():
        snapshot.init_db()
        return {"tasks": snapshot.get_tasks_by_status("downloading")}
    return await _run_blocking(do_list)


@router.post("/queue/add")
async def add_to_queue(body: dict):
    """Add a new download task and start Temporal workflow.

    Body:
        repo_id: str — HuggingFace repo ID or URL
        name: str (optional)
        type: str — "dataset" or "model"
        category: str (optional)
        priority: int — 0 (highest) to 9 (lowest)
        source: str — "hf" or "modelscope"
        shard_count: int (optional) — target shard count (0 = auto). Alias: split_workers.
    """
    from ...core.parser import parse_repo

    repo_id = body.get("repo_id", "").strip()
    if not repo_id:
        return {"error": "repo_id is required"}

    parsed = parse_repo(repo_id)
    source = body.get("source", parsed.get("source", "hf"))
    name = body.get("name", parsed.get("name", repo_id.split("/")[-1]))
    task_type = body.get("type", parsed.get("type", "dataset"))
    category = body.get("category", "")
    priority = max(0, min(9, int(body.get("priority", 5))))
    shard_count = int(body.get("shard_count", body.get("split_workers", 0)) or 0)

    task_id = _next_task_id()

    task_meta = {
        "id": task_id,
        "name": name,
        "repo_id": parsed.get("repo_id", repo_id),
        "source": source,
        "type": task_type,
        "category": category,
        "status": "pending",
        "priority": priority,
        "size_gb": 0,
        "downloaded_gb": 0,
        "progress_pct": 0,
        "speed_mbps": 0,
        "max_workers": shard_count,
        "created_at": _now(),
    }

    # Check for duplicates
    def check_dup():
        snapshot.init_db()
        for t in snapshot.get_all_tasks():
            if t.get("repo_id") == task_meta["repo_id"] and t.get("status") not in ("failed", "revoked", "done"):
                return t
        return None

    dup = await _run_blocking(check_dup)
    if dup:
        return {"error": f"Already exists: {dup['id']} ({dup['name']}) status={dup['status']}"}

    # Save to SQLite
    def do_save():
        snapshot.init_db()
        snapshot.upsert_task(task_meta)
    await _run_blocking(do_save)

    # Task saved as "pending" — auto_dispatch will assign to an idle worker
    return {"ok": True, "task_id": task_id, "name": name, "priority": priority}


@router.post("/queue/pause")
async def pause_task(body: dict):
    """Pause a running task (cancels the Temporal workflow).

    dispatch_mode='pool': once the cancel lands, this also resets the task's
    in-flight batch rows (running/claimed -> pending, server=NULL, speed=0)
    via release_pool_batches — reusing its SQL rather than adding a second
    reset path. That reset must stay gated on dispatch_mode == 'pool': its
    SQL clears every non-done/non-failed row for the task regardless of
    mode, and a sharded task's shard rows are still owned by the sharded
    workflow's own bookkeeping — running them through this reset would wipe
    rows pause has no business touching. `failed` rows are left alone in
    both modes; they are an operator's only per-batch attribution.
    """
    task_id = body.get("task_id", "")
    if not task_id:
        return {"error": "task_id is required"}

    def do_update():
        snapshot.init_db()
        task = snapshot.get_task(task_id)
        if not task:
            return None, {"error": f"Task {task_id} not found"}
        if task["status"] not in ("downloading", "pending"):
            return None, {"error": f"Cannot pause task in status={task['status']}"}
        snapshot.update_task_progress(task_id, status="paused", phase=None, speed_mbps=0)
        return task.get("dispatch_mode"), None

    dispatch_mode, error = await _run_blocking(do_update)
    if error:
        return error

    from ..temporal_client import cancel_workflow
    await cancel_workflow(task_id, dispatch_mode=dispatch_mode)

    if dispatch_mode == "pool":
        await release_pool_batches({"task_id": task_id})

    return {"ok": True, "task_id": task_id, "status": "paused"}


@router.post("/queue/resume")
async def resume_task(body: dict):
    """Resume a paused/failed task (starts a new workflow).

    Deletes the task's shard/batch rows as part of the pending transition.
    For a pool task this is required, not cosmetic: chunking recomputes
    batch boundaries on the next dispatch, and create_pool_batches_in_db
    rejects a request whose row set doesn't match what's already on file —
    stale rows from before the pause would make the very next dispatch
    error out. For a sharded task this is a no-op in effect (create_shards
    already deletes-then-recreates unconditionally on its own), so doing it
    here uniformly is safe.
    """
    task_id = body.get("task_id", "")
    if not task_id:
        return {"error": "task_id is required"}

    def do_get():
        snapshot.init_db()
        return snapshot.get_task(task_id)
    task = await _run_blocking(do_get)

    if not task:
        return {"error": f"Task {task_id} not found"}
    if task["status"] not in ("paused", "failed", "preempted"):
        return {"error": f"Cannot resume task in status={task['status']}"}

    def do_update():
        snapshot.update_task_progress(task_id, status="pending", phase="resuming", speed_mbps=0, error=None)
        snapshot.delete_shards_by_task(task_id)
    await _run_blocking(do_update)

    # auto_dispatch will pick this up and assign to an idle worker
    return {"ok": True, "task_id": task_id, "status": "pending"}


@router.post("/queue/retry")
async def retry_task(body: dict):
    """Retry a failed task."""
    task_id = body.get("task_id", "")
    if not task_id:
        return {"error": "task_id is required"}

    def do_get():
        snapshot.init_db()
        return snapshot.get_task(task_id)
    task = await _run_blocking(do_get)

    if not task:
        return {"error": f"Task {task_id} not found"}
    if task["status"] not in ("failed", "revoked", "paused"):
        return {"error": f"Cannot retry task in status={task['status']}"}

    def do_update():
        retry_count = (task.get("retry_count") or 0) + 1
        snapshot.update_task_progress(
            task_id, status="pending", phase="retrying",
            speed_mbps=0, error=None,
        )
        conn = snapshot._conn()
        conn.execute("UPDATE tasks SET retry_count = ? WHERE id = ?", (retry_count, task_id))
        conn.commit()
    await _run_blocking(do_update)

    # auto_dispatch will pick this up and assign to an idle worker
    return {"ok": True, "task_id": task_id, "status": "pending"}


@router.post("/queue/reorder")
async def reorder_task(body: dict):
    """Change a task's priority."""
    task_id = body.get("task_id", "")
    new_priority = max(0, min(9, int(body.get("priority", 5))))

    def do_reorder():
        snapshot.init_db()
        conn = snapshot._conn()
        conn.execute("UPDATE tasks SET priority = ? WHERE id = ?", (new_priority, task_id))
        conn.commit()
        return {"ok": True, "task_id": task_id, "priority": new_priority}
    return await _run_blocking(do_reorder)


@router.post("/queue/jump")
async def jump_queue(body: dict):
    body["priority"] = 0
    return await reorder_task(body)


@router.post("/queue/preempt")
async def preempt_for_task(body: dict):
    """Preempt: pause a running task on a worker, then dispatch the urgent task there.

    Body:
        urgent_task_id: str — the pending task to promote
        victim_task_id: str (optional) — which downloading task to pause;
            if omitted, auto-picks the lowest-priority downloading task
        target_server: str (optional) — force dispatch to this worker
    """
    urgent_id = body.get("urgent_task_id", "")
    victim_id = body.get("victim_task_id", "")
    target_server = body.get("target_server", "")

    if not urgent_id:
        return {"error": "urgent_task_id is required"}

    from ..temporal_client import cancel_workflow, start_sharded_download

    def do_read():
        snapshot.init_db()
        urgent = snapshot.get_task(urgent_id)
        if not urgent:
            return None, None, "Urgent task not found"
        if urgent["status"] not in ("pending", "paused", "failed"):
            return None, None, f"Urgent task status={urgent['status']}, expected pending/paused/failed"

        if victim_id:
            victim = snapshot.get_task(victim_id)
            if not victim:
                return urgent, None, "Victim task not found"
            if victim["status"] != "downloading":
                return urgent, None, f"Victim task status={victim['status']}, expected downloading"
            return urgent, victim, None

        downloading = snapshot.get_tasks_by_status("downloading")
        if not downloading:
            return urgent, None, "No downloading tasks to preempt — all workers are idle"

        downloading.sort(key=lambda t: (-t.get("priority", 5), t.get("created_at", "")))
        return urgent, downloading[0], None

    urgent, victim, error = await _run_blocking(do_read)
    if error:
        return {"error": error}

    server = target_server or (victim["server"] if victim else "")
    if not server:
        return {"error": "Cannot determine target server"}

    # 1) Pause the victim
    if victim:
        def do_pause_victim():
            snapshot.update_task_progress(
                victim["id"], status="preempted", phase=None, speed_mbps=0
            )
        await _run_blocking(do_pause_victim)

        try:
            await cancel_workflow(victim["id"])
        except Exception:
            pass

    # 2) Claim the urgent task. server=NULL: the sharded coordinator assigns
    #    servers per shard; a task-level claim would wrongly mark one worker
    #    busy in the idle query. target_server only influences victim choice.
    def do_claim():
        now_ts = time.time()
        conn = snapshot._conn()
        conn.execute(
            "UPDATE tasks SET status = 'downloading', server = NULL, priority = 0, "
            "updated_at = ?, claimed_at = ? "
            "WHERE id = ?",
            (now_ts, now_ts, urgent_id),
        )
        conn.commit()
        return snapshot.get_task(urgent_id)
    task = await _run_blocking(do_claim)

    # 3) Start the workflow (unified sharded path — BOS resume filter included)
    try:
        await start_sharded_download(task)
    except Exception as e:
        if "already started" not in str(e).lower():
            def do_revert():
                snapshot.update_task_progress(urgent_id, status="pending", server=None)
            await _run_blocking(do_revert)
            return {"error": f"Failed to start workflow: {e}"}

    victim_name = victim["name"] if victim else "(none)"
    return {
        "ok": True,
        "message": f"已抢占: 暂停 {victim_name}@{server}，启动 {task['name']}@{server}",
        "urgent_task_id": urgent_id,
        "victim_task_id": victim["id"] if victim else None,
        "server": server,
    }


@router.get("/tasks/{task_id}/shards")
async def list_shards(task_id: str):
    """List all shards for a task."""
    def do_list():
        snapshot.init_db()
        task = snapshot.get_task(task_id)
        if not task:
            return {"error": f"Task {task_id} not found"}
        shards = snapshot.get_shards_by_task(task_id)
        return {"task_id": task_id, "shards": shards}
    return await _run_blocking(do_list)


@router.post("/shard-progress")
async def report_shard_progress(body: dict):
    """Receive shard progress from ShardWorkerWorkflow activities."""
    shard_id = body.get("shard_id", "")
    if not shard_id:
        return {"error": "shard_id is required"}

    def do_update():
        snapshot.init_db()
        shard = snapshot.get_shard(shard_id)
        if not shard:
            return {"error": f"Shard {shard_id} not found"}
        # pause_task cancels workflows cooperatively, so in-flight shards keep
        # reporting for a while after the operator stopped the task. Writing
        # that through would re-set a paused task's speed and progress — the
        # same rule /api/task-progress already enforces.
        parent = snapshot.get_task(shard.get("task_id") or "")
        if parent and parent.get("status") in TERMINAL_STATUSES:
            return {"ok": True, "ignored": f"task is {parent['status']}"}
        snapshot.update_shard_progress(
            shard_id,
            done_files=body.get("done_files", shard.get("done_files", 0)),
            done_bytes=body.get("done_bytes", shard.get("done_bytes", 0)),
            speed_mbps=body.get("speed_mbps", 0),
        )
        # Auto-aggregate into task table so dashboard stays current
        task_id = shard.get("task_id")
        if task_id:
            _aggregate_task(task_id)
        return {"ok": True, "shard_id": shard_id}
    return await _run_blocking(do_update)


@router.post("/shards/create")
async def create_shards(body: dict):
    """Create shard rows in SQLite. Called by create_shards_in_db activity."""
    import time
    task_id = body.get("task_id", "")
    shard_infos = body.get("shard_infos", [])
    if not task_id or not shard_infos:
        return {"error": "task_id and shard_infos required"}

    def do_create():
        snapshot.init_db()
        # A re-dispatch may partition into FEWER shards (BOS filter shrinks the
        # set) — stale rows from a prior run would pin servers busy forever and
        # inflate total_shards, so clear them first.
        snapshot.delete_shards_by_task(task_id)
        shard_ids = []
        for info in shard_infos:
            idx = info["shard_index"]
            shard_id = shard_row_id(task_id, idx)
            snapshot.upsert_shard({
                "id": shard_id,
                "task_id": task_id,
                "shard_index": idx,
                "status": "pending",
                "total_files": info["total_files"],
                "total_bytes": info["total_bytes"],
                "filelist_key": info.get("filelist_key", ""),
                "updated_at": time.time(),
            })
            shard_ids.append(shard_id)
        return {"ok": True, "shard_ids": shard_ids}
    return await _run_blocking(do_create)


@router.post("/pool/batches/create")
async def create_pool_batches(body: dict):
    """Idempotent batch-row creation for the pool path (dispatch_mode='pool').

    /shards/create's delete-then-create semantics don't carry over here: the
    sharded coordinator only ever calls it once, before any shard has
    started, so blowing away stale rows first is safe. The pool coordinator
    can retry create_pool_batches_in_db (start_to_close=5min, retry x3), and
    by the time a retry lands, run_pool_batch activities may already be
    assigning/reporting against the rows the first attempt created — delete
    them and that progress is gone.

    Idempotency key: `expected_count` plus, row for row, an exact match of
    (shard_index, filelist_key, total_files, total_bytes) against what's
    already on file. A hit still resets every non-done row to
    pending/server=NULL/speed=0 — a retry can follow a crash mid-window, and
    any row a dead attempt marked running must be picked up again.

    Body: task_id, shard_infos (list of {shard_index, filelist_key,
    total_files, total_bytes}), expected_count (optional, defaults to
    len(shard_infos); a mismatch against shard_infos is a caller bug, not an
    idempotency question).
    """
    task_id = body.get("task_id", "")
    batch_infos = body.get("shard_infos", [])
    expected_count = body.get("expected_count", len(batch_infos))
    if not task_id or not batch_infos:
        return {"error": "task_id and shard_infos required"}
    if expected_count != len(batch_infos):
        return {"error": f"expected_count={expected_count} does not match "
                          f"len(shard_infos)={len(batch_infos)}"}

    def key(shard_index, filelist_key, total_files, total_bytes):
        return (shard_index, filelist_key or "", total_files, total_bytes)

    incoming = sorted(
        key(info["shard_index"], info.get("filelist_key", ""),
            info["total_files"], info["total_bytes"])
        for info in batch_infos
    )

    def matches_incoming(rows) -> bool:
        if len(rows) != expected_count:
            return False
        on_file = sorted(
            key(r["shard_index"], r.get("filelist_key"), r["total_files"], r["total_bytes"])
            for r in rows
        )
        return on_file == incoming

    def do_create():
        snapshot.init_db()
        conn = snapshot._conn()

        # A late coordinator retry after an operator pauses/revokes the task
        # must not rewrite that stopped task's shard rows back to pending —
        # same rule as the /shards/status and /shards/assign guards above.
        # Checked before any read/write below.
        task = snapshot.get_task(task_id)
        if not task:
            return {"ok": True, "ignored": True, "reason": "task not found"}
        if task.get("status") in TERMINAL_STATUSES:
            return {"ok": True, "ignored": True}

        existing = snapshot.get_shards_by_task(task_id)

        if existing and matches_incoming(existing):
            # Idempotent hit. 'done' rows are left alone — their upload
            # already landed — everything else goes back to pending so
            # the window loop re-issues it.
            with conn:
                conn.execute(
                    "UPDATE shards SET status='pending', server=NULL, speed_mbps=0 "
                    "WHERE task_id=? AND status!='done'",
                    (task_id,),
                )
            return {
                "ok": True, "idempotent": True,
                "shard_ids": [r["id"] for r in sorted(existing, key=lambda r: r["shard_index"])],
            }

        if existing:
            return {"error": f"batch rows already exist for {task_id} and do not match the requested set"}

        now = time.time()
        rows = []
        for info in batch_infos:
            idx = info["shard_index"]
            rows.append((
                shard_row_id(task_id, idx), task_id, idx, "pending",
                info["total_files"], info["total_bytes"],
                info.get("filelist_key", ""), now,
            ))
        # OR IGNORE + a spanning `with conn:` transaction: shard_row_id is
        # deterministic on (task_id, shard_index), so a concurrent retry
        # racing this same call computes identical row ids — its insert
        # collides on the primary key and is dropped instead of raising
        # IntegrityError into a 500. `with conn:` commits only if
        # executemany runs clean; on any real failure it rolls back instead
        # of leaving a stray open transaction on this thread's connection
        # for the next request on this pool thread to inherit and commit.
        # The re-read lives inside the transaction: the losing side of a race
        # needs to see the winner's rows and confirm they're the same logical
        # batch before calling it success — and if a concurrent call carried a
        # *different* batch set, OR IGNORE would have let its non-colliding
        # indices land next to the winner's. Raising here rolls those strays
        # back instead of persisting a mixed row set that would wedge the
        # task's idempotency check forever.
        class _BatchMismatch(Exception):
            pass

        try:
            with conn:
                conn.executemany(
                    "INSERT OR IGNORE INTO shards (id, task_id, shard_index, status, total_files, "
                    "total_bytes, filelist_key, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    rows,
                )
                final = snapshot.get_shards_by_task(task_id)
                if not matches_incoming(final):
                    raise _BatchMismatch()
        except _BatchMismatch:
            return {"error": f"batch rows already exist for {task_id} and do not match the requested set"}
        return {
            "ok": True, "idempotent": False,
            "shard_ids": [r["id"] for r in sorted(final, key=lambda r: r["shard_index"])],
        }
    return await _run_blocking(do_create)


@router.post("/shards/status")
async def update_shard_status_api(body: dict):
    """Update shard status. Called by update_shard_status activity."""
    shard_id = body.get("shard_id", "")
    status = body.get("status", "")
    error = body.get("error")
    if not shard_id or not status:
        return {"error": "shard_id and status required"}

    def do_update():
        snapshot.init_db()
        shard = snapshot.get_shard(shard_id)
        if not shard:
            return {"error": f"Shard {shard_id} not found"}
        # A dying workflow's late write must not resurrect a task an operator
        # already stopped — same rule as /api/shard-progress and
        # /api/task-progress, returned as 200/ignored so old workers never
        # see (and retry against) an error.
        parent = snapshot.get_task(shard.get("task_id") or "")
        if parent and parent.get("status") in TERMINAL_STATUSES:
            return {"ok": True, "ignored": True}
        if status in ("done", "failed"):
            snapshot.complete_shard(shard_id, status)
            if error:
                snapshot.update_shard_progress(shard_id, error=error)
        else:
            fields = {"status": status}
            if error:
                fields["error"] = error
            snapshot.update_shard_progress(shard_id, **fields)
        # A shard reaching a terminal status makes its last write here — if
        # the 5s debounce swallowed it, the task's aggregate would stay
        # stale until the workflow's own end-of-run aggregate call.
        if status in ("done", "failed") and shard.get("task_id"):
            _aggregate_task(shard["task_id"], force=True)
        return {"ok": True}
    return await _run_blocking(do_update)


@router.post("/shards/assign")
async def assign_shard_server_api(body: dict):
    """Assign server to shard. Called by assign_shard_server activity."""
    shard_id = body.get("shard_id", "")
    server_key = body.get("server_key", "")
    if not shard_id or not server_key:
        return {"error": "shard_id and server_key required"}

    def do_assign():
        snapshot.init_db()
        shard = snapshot.get_shard(shard_id)
        if not shard:
            return {"error": f"Shard {shard_id} not found"}
        parent = snapshot.get_task(shard.get("task_id") or "")
        if parent and parent.get("status") in TERMINAL_STATUSES:
            return {"ok": True, "ignored": True}
        snapshot.update_shard_progress(shard_id, server=server_key,
                                       started_at=_now())
        return {"ok": True}
    return await _run_blocking(do_assign)


_AGGREGATE_DEBOUNCE_SECONDS = 5
# task_id -> last time _aggregate_task actually ran (module-level: this
# process is the only writer of task-level aggregates).
_last_aggregate_ts: dict = {}


def _aggregate_task(task_id: str, force: bool = False) -> dict:
    """Aggregate shard progress into task-level fields. Must be called inside a blocking executor.

    A single SQL aggregate pass, not a fetch-all-rows-and-sum-in-Python —
    under pool-mode report volumes (hundreds of shard rows per task,
    reporting every 15s) the old approach walked O(shards) Python objects on
    every single progress ping and started queueing behind S1's SQLite write
    lock. Debounced to once per 5s per task; callers that know a shard just
    made its last possible write (reached done/failed) must pass
    force=True — otherwise that write can be the one the debounce eats, and
    nothing else will ever correct the task's totals for it.
    """
    now = time.time()
    if not force and now - _last_aggregate_ts.get(task_id, 0) < _AGGREGATE_DEBOUNCE_SECONDS:
        return {"ok": True, "skipped": "debounced"}
    _last_aggregate_ts[task_id] = now

    conn = snapshot._conn()
    row = conn.execute(
        "SELECT COUNT(*) AS n, "
        "COALESCE(SUM(done_bytes), 0) AS done_bytes, "
        "COALESCE(SUM(total_bytes), 0) AS total_bytes, "
        "COALESCE(SUM(speed_mbps), 0) AS speed, "
        "COUNT(*) FILTER (WHERE status = 'done') AS done_shards "
        "FROM shards WHERE task_id = ?",
        (task_id,),
    ).fetchone()
    if not row or row["n"] == 0:
        return {"ok": True, "shards": 0}

    done_bytes = row["done_bytes"]
    total_bytes = row["total_bytes"]
    total_shards = row["n"]
    done_shards = row["done_shards"]

    pct = round(done_bytes / total_bytes * 100, 1) if total_bytes > 0 else 0
    downloaded_gb = done_bytes / (1024 ** 3)
    size_gb = total_bytes / (1024 ** 3)

    snapshot.update_task_progress(
        task_id, speed_mbps=row["speed"], progress_pct=pct,
        downloaded_gb=round(downloaded_gb, 2),
    )
    conn.execute(
        "UPDATE tasks SET done_shards = ?, total_shards = ?, size_gb = ? WHERE id = ?",
        (done_shards, total_shards, round(size_gb, 2), task_id),
    )
    conn.commit()
    return {"ok": True, "done_shards": done_shards, "total_shards": total_shards}


@router.post("/shards/resume-info")
async def report_resume_info_api(body: dict):
    """Persist BOS resume-filter results on the task row (acceptance evidence).

    Called by the report_resume_info activity — phase messages get overwritten
    within seconds, this record survives.
    """
    task_id = body.get("task_id", "")
    if not task_id:
        return {"error": "task_id required"}

    def do_update():
        snapshot.init_db()
        conn = snapshot._conn()
        conn.execute(
            "UPDATE tasks SET resume_skipped_files = ?, resume_skipped_gb = ?, updated_at = ? "
            "WHERE id = ?",
            (int(body.get("skipped_files", 0)),
             round(float(body.get("skipped_gb", 0)), 2),
             time.time(), task_id),
        )
        conn.commit()
        return {"ok": True, "task_id": task_id}
    return await _run_blocking(do_update)


@router.post("/shards/aggregate")
async def aggregate_task_api(body: dict):
    """Aggregate shard progress into task-level. Called by aggregate_task_from_shards activity.

    This is the workflow's deliberate end-of-run aggregate, not one of the
    repeated progress pings the debounce exists for — it must always run.
    """
    task_id = body.get("task_id", "")
    if not task_id:
        return {"error": "task_id required"}

    def do_aggregate():
        snapshot.init_db()
        return _aggregate_task(task_id, force=True)
    return await _run_blocking(do_aggregate)


@router.get("/shards/idle-workers")
async def query_idle_workers_api(source: str = "hf", exclude_task: str = ""):
    """Return idle worker keys for a given source. Called by query_idle_workers activity.

    exclude_task: task_id whose own claim/shards must not count as busy —
    without it, the dispatching task's claimed server is excluded from its
    own shard pool (6 idle workers would yield only 5 shards).
    """
    import time

    from ..fleet import (
        alive_workers, busy_servers, worker_serves, MIN_SHARD_DISK_GB,
    )

    def do_query():
        snapshot.init_db()
        now = time.time()

        # The dispatching task's own claim/shards must not count as busy
        running = [s for s in snapshot.get_running_shards()
                   if s.get("task_id") != exclude_task]
        downloading = [t for t in snapshot.get_tasks_by_status("downloading")
                       if t.get("id") != exclude_task]
        busy = busy_servers(downloading, running)

        idle = []
        for w in alive_workers(snapshot.get_workers(), now):
            key = w.get("server_key") or ""
            if key in busy:
                continue
            if (w.get("disk_free_gb") or 0) < MIN_SHARD_DISK_GB:
                continue
            if not worker_serves(key, source):
                continue
            idle.append(key)
        return {"workers": idle}
    return await _run_blocking(do_query)


@router.get("/pool/alive-workers")
async def pool_alive_workers_api(source: str = "hf"):
    """Count of alive workers serving `source`. Called by the
    pool_alive_workers activity (T3).

    Deliberately NOT idle-workers: alive ∩ worker_serves, ignoring
    busy/disk. The pool workflow sizes its window from total serving
    capacity — an idle-only count would read 0 while the pool is fully
    loaded (every worker busy with other pool batches, not absent) and the
    window loop would deadlock waiting for slots that will never "open".
    """
    from ..fleet import alive_workers, worker_serves

    def do_query():
        snapshot.init_db()
        matched = [
            w.get("server_key") for w in alive_workers(snapshot.get_workers())
            if worker_serves(w.get("server_key") or "", source)
        ]
        return {"count": len(matched), "workers": matched}
    return await _run_blocking(do_query)


@router.get("/pool/window")
async def pool_window_api(task_id: str):
    """This task's current window size — how many batches it may keep in
    flight. Called once per coordinator wake by `record_batches_and_window`.

    `window = max(1, floor(P * W_self / sum(W_active)))`, P = alive workers
    serving this task's source. Computed here, not on the worker, because
    every input is S1-local state and the weights are fleet policy (fleet.py
    owns them alongside MIN_SHARD_DISK_GB).

    The floor of 1 is what keeps a task alive when it is outvoted: a task
    whose fair share rounds to zero still gets one slot and makes progress
    slowly, rather than stalling until its neighbours finish. The `sum`
    bounds total pool concurrency at ~P however many tasks are admitted.
    """
    from ..fleet import alive_workers, pool_task_weight, worker_serves

    def do_query():
        snapshot.init_db()
        task = snapshot.get_task(task_id)
        if not task:
            return {"error": f"Task {task_id} not found"}

        source = task.get("source") or "hf"
        p = len([
            w.get("server_key") for w in alive_workers(snapshot.get_workers())
            if worker_serves(w.get("server_key") or "", source)
        ])

        # Active = pool-mode tasks still downloading on the same source. A
        # task competing for a different source's workers doesn't consume
        # this task's P, so it must not dilute the share either.
        active = [
            t for t in snapshot.get_tasks_by_status("downloading")
            if (t.get("dispatch_mode") or "sharded") == "pool"
            and (t.get("source") or "hf") == source
        ]
        w_self = pool_task_weight(task.get("priority") or 0)
        w_sum = sum(pool_task_weight(t.get("priority") or 0) for t in active)
        # This task may not be in `active` yet (its own status write races the
        # first wake), so make sure it is counted.
        if not any(t.get("id") == task_id for t in active):
            w_sum += w_self

        window = max(1, int(p * w_self / w_sum)) if w_sum > 0 else 1
        return {
            "window": window, "p": p, "weight": w_self,
            "weight_sum": w_sum, "active_pool_tasks": len(active),
        }
    return await _run_blocking(do_query)


@router.post("/pool/batches/release")
async def release_pool_batches(body: dict):
    """Reset this task's in-flight batch rows to pending/server=NULL/speed=0.

    The coordinator's cancellation path calls this (shielded) so a paused or
    reshard-bound task leaves no row claiming to be running on a worker that
    has already stopped — those rows are what busy_servers, the dashboard and
    the reconciler read.

    Only rows that actually claim a worker are released. `done` rows are left
    alone (their bytes are on BOS) and so are `failed` ones: a failed batch's
    status plus its error is the only per-batch attribution an operator has
    after the fact, and resetting it to `pending` would make a task reporting
    "3/717 batches failed" show 717 rows with nothing marked failed. Getting a
    failed row back to `pending` for a genuine re-dispatch is the batch-create
    endpoint's job, which does it as part of an idempotent hit.

    No TERMINAL guard: unlike assign/status this only ever *releases* rows,
    and the task being terminal is precisely when it is called.
    """
    task_id = body.get("task_id", "")
    if not task_id:
        return {"error": "task_id is required"}

    def do_release():
        snapshot.init_db()
        conn = snapshot._conn()
        with conn:
            cur = conn.execute(
                "UPDATE shards SET status='pending', server=NULL, speed_mbps=0 "
                "WHERE task_id=? AND status NOT IN ('done', 'failed')",
                (task_id,),
            )
        return {"ok": True, "released": cur.rowcount}
    return await _run_blocking(do_release)


@router.post("/queue/reshard")
async def reshard_task(body: dict):
    """Change a task's shard count and/or dispatch_mode via lossless restart.

    Terminates the running workflows, waits for them to close, deletes the
    old shard/batch rows, and requeues the task with the new max_workers
    and/or dispatch_mode. The BOS resume filter makes the restart cheap —
    already-uploaded files are skipped.

    Validation is deliberately relaxed from "shard_count >= 1 always": a
    request needs EITHER shard_count >= 1 OR a valid dispatch_mode
    ('sharded'/'pool'); neither is an error, and an invalid dispatch_mode
    string is an error rather than a silent fallback. This lets a pure mode
    flip (sharded<->pool) or a pure pool restart — for which shard_count is
    meaningless, chunking recomputes batches — go through reshard without
    inventing a fake shard_count. A sharded->sharded call that omits
    dispatch_mode (every call site before this task existed) is unaffected.

    Body: task_id, shard_count (optional), dispatch_mode (optional, 'sharded'
    or 'pool').
    """
    from ..temporal_client import terminate_workflow_and_wait

    VALID_DISPATCH_MODES = {"sharded", "pool"}

    task_id = body.get("task_id", "")
    shard_count = int(body.get("shard_count", 0) or 0)
    requested_mode = body.get("dispatch_mode")

    if requested_mode is not None and requested_mode not in VALID_DISPATCH_MODES:
        return {"error": f"invalid dispatch_mode={requested_mode!r}, "
                          f"expected one of {sorted(VALID_DISPATCH_MODES)}"}
    if not task_id or (shard_count < 1 and requested_mode is None):
        return {"error": "task_id and (shard_count >= 1 or a valid dispatch_mode) are required"}

    def do_check():
        snapshot.init_db()
        task = snapshot.get_task(task_id)
        if not task:
            return None, f"task {task_id} not found"
        if task.get("status") not in ("downloading", "pending", "paused"):
            return None, f"task status={task.get('status')}, expected downloading/pending/paused"
        return task, None
    task, error = await _run_blocking(do_check)
    if error:
        return {"error": error}

    # Terminate against the task's CURRENT (pre-flip) dispatch_mode — that is
    # what determines whether per-shard child workflow handles exist to wait
    # on, independent of what this call is about to change it to.
    closed = await terminate_workflow_and_wait(task_id, dispatch_mode=task.get("dispatch_mode"))
    if not closed:
        return {"error": "workflows did not close within timeout — task state unchanged, retry later"}

    # New max_workers: an explicit shard_count always wins. Otherwise fall
    # back to whatever is already stored — this is what makes a pure mode
    # flip (no shard_count supplied) a no-op on max_workers, and what makes a
    # pool->sharded flip "resume with the last known shard_count" per the T8
    # brief. max_workers can be NULL in the DB (pre-existing trap: the schema
    # DEFAULT of 0 does not apply once a caller INSERTs an explicit NULL) —
    # `or 0` guards the int() conversion so a pool task that never set
    # max_workers doesn't crash the flip; 0 already means "auto-shard" to
    # start_sharded_download.
    new_max_workers = shard_count if shard_count >= 1 else int(task.get("max_workers") or 0)
    new_dispatch_mode = requested_mode if requested_mode is not None else (task.get("dispatch_mode") or "sharded")

    def do_requeue():
        conn = snapshot._conn()
        # Conditional on the status observed before termination: if
        # auto_dispatch claimed a pending task during the terminate window,
        # requeuing blindly would silently discard the new shard count.
        cur = conn.execute(
            "UPDATE tasks SET status = 'pending', server = NULL, max_workers = ?, "
            "dispatch_mode = ?, speed_mbps = 0, updated_at = ? WHERE id = ? AND status = ?",
            (new_max_workers, new_dispatch_mode, time.time(), task_id, task["status"]),
        )
        if cur.rowcount == 0:
            conn.commit()
            return {"error": "task status changed during reshard — retry"}
        conn.execute("DELETE FROM shards WHERE task_id = ?", (task_id,))
        conn.commit()
        return {
            "ok": True, "task_id": task_id, "shard_count": new_max_workers,
            "dispatch_mode": new_dispatch_mode,
            "note": "requeued; auto_dispatch restarts it with the new shard count, "
                    "BOS filter skips already-uploaded files",
        }
    return await _run_blocking(do_requeue)


@router.delete("/queue/{task_id}")
async def delete_from_queue(task_id: str):
    """Cancel workflow and delete task."""
    from ..temporal_client import cancel_workflow
    await cancel_workflow(task_id)

    def do_delete():
        snapshot.init_db()
        snapshot.delete_task(task_id)
        return {"ok": True, "task_id": task_id, "deleted": True}
    return await _run_blocking(do_delete)


@router.post("/sync")
async def sync_stub():
    return {"changes": 0, "message": "Sync not needed — Temporal manages state"}
