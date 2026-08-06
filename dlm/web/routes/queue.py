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
    """Pause a running task (cancels the Temporal workflow)."""
    task_id = body.get("task_id", "")
    if not task_id:
        return {"error": "task_id is required"}

    def do_update():
        snapshot.init_db()
        task = snapshot.get_task(task_id)
        if not task:
            return {"error": f"Task {task_id} not found"}
        if task["status"] not in ("downloading", "pending"):
            return {"error": f"Cannot pause task in status={task['status']}"}
        snapshot.update_task_progress(task_id, status="paused", phase=None, speed_mbps=0)
        return None

    error = await _run_blocking(do_update)
    if error:
        return error

    from ..temporal_client import cancel_workflow
    await cancel_workflow(task_id)

    return {"ok": True, "task_id": task_id, "status": "paused"}


@router.post("/queue/resume")
async def resume_task(body: dict):
    """Resume a paused/failed task (starts a new workflow)."""
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
    await _run_blocking(do_update)

    # auto_dispatch will pick this up and assign to an idle worker
    return {"ok": True, "task_id": task_id, "status": "pending"}


@router.post("/queue/retry")
async def retry_task(body: dict):
    """Retry a failed task.

    Also accepts a task marked `done` whose OWN shard rows contradict that
    claim — a shard row that is not `done` means the coordinator concluded
    success over a shard that did not succeed. t-20260805-460d45
    (molmobot-data) was reported done at 0 of 9611 GB on 2026-08-06 with its
    single shard row reading `failed`; the workflow bug behind it is fixed,
    but there was no supported way to correct the row it left behind, and
    hand-editing SQLite is not one.

    The gate is evidence from the task's own shard rows, not an operator
    override: a genuinely complete task (every shard `done`) is still refused,
    so this cannot be used to re-download a finished dataset by accident.
    """
    task_id = body.get("task_id", "")
    if not task_id:
        return {"error": "task_id is required"}

    def do_get():
        snapshot.init_db()
        task = snapshot.get_task(task_id)
        shards = snapshot.get_shards_by_task(task_id) if task else []
        return task, shards
    task, shards = await _run_blocking(do_get)

    if not task:
        return {"error": f"Task {task_id} not found"}
    if task["status"] not in ("failed", "revoked", "paused", "done"):
        return {"error": f"Cannot retry task in status={task['status']}"}
    contradicting = [s for s in shards if (s.get("status") or "") != "done"]
    if task["status"] == "done" and not contradicting:
        return {"error": (
            f"Task {task_id} is done and all {len(shards)} shard rows agree — "
            "refusing to re-download. Add a new task if that is really wanted."
        )}

    def do_update():
        retry_count = (task.get("retry_count") or 0) + 1
        snapshot.update_task_progress(
            task_id, status="pending", phase="retrying",
            speed_mbps=0, error=None,
        )
        conn = snapshot._conn()
        # server released and completed_at cleared for the same reason
        # /queue/reshard does it: a stale claim or completion timestamp on a
        # row auto_dispatch is about to pick up describes the previous run.
        conn.execute(
            "UPDATE tasks SET retry_count = ?, server = NULL, completed_at = NULL "
            "WHERE id = ?", (retry_count, task_id))
        # The re-dispatched coordinator calls create_shards_in_db and gets a
        # fresh set of rows. Leaving the old ones behind inflates total_shards
        # so the task can never read 100%, and keeps a `failed` row that the
        # aggregate would count against a run that is going fine. /queue/
        # reshard deletes them for exactly this reason; retry did not.
        conn.execute("DELETE FROM shards WHERE task_id = ?", (task_id,))
        conn.commit()
    await _run_blocking(do_update)

    # auto_dispatch will pick this up and assign to an idle worker
    return {
        "ok": True, "task_id": task_id, "status": "pending",
        "cleared_shard_rows": len(shards),
        "note": "auto_dispatch restarts it; the BOS resume filter skips files "
                "already uploaded under this task's prefix",
    }


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
    from ..fleet import coordinator_queue

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
        await start_sharded_download(
            task, task_queue=coordinator_queue(task.get("source", "hf")))
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
        if status in ("done", "failed"):
            snapshot.complete_shard(shard_id, status)
            if error:
                snapshot.update_shard_progress(shard_id, error=error)
        else:
            fields = {"status": status}
            if error:
                fields["error"] = error
            snapshot.update_shard_progress(shard_id, **fields)
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
        snapshot.update_shard_progress(shard_id, server=server_key,
                                       started_at=_now())
        return {"ok": True}
    return await _run_blocking(do_assign)


def _aggregate_task(task_id: str) -> dict:
    """Aggregate shard progress into task-level fields. Must be called inside a blocking executor."""
    shards = snapshot.get_shards_by_task(task_id)
    if not shards:
        return {"ok": True, "shards": 0}
    done_bytes = sum(s.get("done_bytes", 0) for s in shards)
    total_bytes = sum(s.get("total_bytes", 0) for s in shards)
    speed = sum(s.get("speed_mbps", 0) for s in shards)
    done_shards = sum(1 for s in shards if s.get("status") == "done")

    pct = round(done_bytes / total_bytes * 100, 1) if total_bytes > 0 else 0
    downloaded_gb = done_bytes / (1024 ** 3)
    size_gb = total_bytes / (1024 ** 3)

    snapshot.update_task_progress(
        task_id, speed_mbps=speed, progress_pct=pct,
        downloaded_gb=round(downloaded_gb, 2),
    )
    conn = snapshot._conn()
    conn.execute(
        "UPDATE tasks SET done_shards = ?, total_shards = ?, size_gb = ? WHERE id = ?",
        (done_shards, len(shards), round(size_gb, 2), task_id),
    )
    conn.commit()
    return {"ok": True, "done_shards": done_shards, "total_shards": len(shards)}


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
    """Aggregate shard progress into task-level. Called by aggregate_task_from_shards activity."""
    task_id = body.get("task_id", "")
    if not task_id:
        return {"error": "task_id required"}

    def do_aggregate():
        snapshot.init_db()
        return _aggregate_task(task_id)
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


@router.post("/queue/reshard")
async def reshard_task(body: dict):
    """Change a task's shard count via lossless restart.

    Terminates the running workflows, waits for them to close, deletes the old
    shard rows, and requeues the task with the new max_workers. The BOS resume
    filter makes the restart cheap — already-uploaded files are skipped.

    Body: task_id, shard_count
    """
    from ..temporal_client import terminate_workflow_and_wait

    task_id = body.get("task_id", "")
    shard_count = int(body.get("shard_count", 0) or 0)
    if not task_id or shard_count < 1:
        return {"error": "task_id and shard_count >= 1 required"}

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

    closed = await terminate_workflow_and_wait(task_id)
    if not closed:
        return {"error": "workflows did not close within timeout — task state unchanged, retry later"}

    def do_requeue():
        conn = snapshot._conn()
        # Conditional on the status observed before termination: if
        # auto_dispatch claimed a pending task during the terminate window,
        # requeuing blindly would silently discard the new shard count.
        cur = conn.execute(
            "UPDATE tasks SET status = 'pending', server = NULL, max_workers = ?, "
            "speed_mbps = 0, updated_at = ? WHERE id = ? AND status = ?",
            (shard_count, time.time(), task_id, task["status"]),
        )
        if cur.rowcount == 0:
            conn.commit()
            return {"error": "task status changed during reshard — retry"}
        conn.execute("DELETE FROM shards WHERE task_id = ?", (task_id,))
        conn.commit()
        return {
            "ok": True, "task_id": task_id, "shard_count": shard_count,
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
