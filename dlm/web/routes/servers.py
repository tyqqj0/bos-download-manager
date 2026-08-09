"""Servers API — Celery worker status."""

import logging

from fastapi import APIRouter, HTTPException

from ..fleet import TERMINAL_STATUSES
from . import run_blocking

logger = logging.getLogger("dlm.web")

router = APIRouter(tags=["servers"])


@router.get("/servers")
async def list_servers():
    """Get all workers with live status."""
    def _do():
        from ...queue.snapshot import get_workers, init_db
        from ..fleet import servers_view
        init_db()
        # merge, not last-row-wins: every worker reports under two hostnames
        # (`wN@temporal` = liveness only, `wN@sidecar` = the metrics), so a
        # dict comprehension over the raw rows keeps whichever the SQL happened
        # to yield last and drops the other's columns. The Servers tab reads
        # disk_free_gb off this payload, so that was a coin flip per request:
        # half the page loads showed "0G free" and hid the cleanup button on a
        # worker that had just reported its real free space.
        #
        # servers_view rather than merge_workers directly, so this payload and
        # /api/dashboard's `servers` key are the same dict built by the same
        # function — including `worker_alive`, which the UI needs and no route
        # used to emit.
        return servers_view(get_workers())

    return {"servers": await run_blocking(_do)}


@router.get("/servers/{key}")
async def get_server(key: str):
    """Get a single worker's status."""
    # Reads live, like /api/servers above. This used to serve
    # `cache.get_servers()` exclusively — and nothing has ever called
    # `cache.set_servers()` since the Temporal rewrite, so every request 404'd
    # on a worker that was up and reporting.
    servers = (await list_servers())["servers"]
    if key not in servers:
        raise HTTPException(404, f"Worker {key} not found")
    return servers[key]


@router.post("/servers/{key}/ping")
async def ping_worker(key: str):
    """Ping a Celery worker."""
    def _do():
        from ...queue.app import app as celery_app
        try:
            inspect = celery_app.control.inspect(
                destination=[f"{key}@*"], timeout=5
            )
            result = inspect.ping() or {}
            if result:
                return {"key": key, "status": "alive", "response": result}
            return {"key": key, "status": "unreachable"}
        except Exception as e:
            return {"key": key, "status": "error", "error": str(e)}

    return await run_blocking(_do)


@router.post("/worker-heartbeat")
async def worker_heartbeat(body: dict):
    """Receive worker heartbeat and update dashboard snapshot.

    Accepts both legacy heartbeats (hostname, server_key, disk_free_gb)
    and enhanced sidecar heartbeats (download_process_alive, https_connections, etc).
    """
    def _do():
        from ...queue.snapshot import init_db, update_worker
        init_db()
        update_worker(
            hostname=body.get("hostname", ""),
            server_key=body["server_key"],
            status=body.get("status", "online"),
            current_task_id=body.get("current_task_id"),
            disk_free_gb=body.get("disk_free_gb"),
            download_process_alive=body.get("download_process_alive"),
            download_process_pid=body.get("download_process_pid"),
            https_connections=body.get("https_connections"),
            files_last_5min=body.get("files_last_5min"),
            staging_size_mb=body.get("staging_size_mb"),
            event_buffer_pending=body.get("event_buffer_pending"),
        )
        return {"ok": True}

    return await run_blocking(_do)


@router.post("/servers/{key}/cleanup")
async def cleanup_server_staging(key: str):
    """Clean staging directory on a worker for terminal tasks.

    Staging is keyed by task name, so a directory is only safe to delete when
    NO non-terminal task shares that name — see the comment in the body.
    """
    def _do():
        from ...queue.snapshot import get_all_tasks, get_shards_by_task, init_db
        from ...core.servers import load_servers
        from ...core.ssh import ssh_exec

        init_db()
        tasks = get_all_tasks()

        TERMINAL = ("done", "failed", "revoked", "skipped")

        # Staging is keyed by task NAME, not id (STAGING_PATH / name /
        # shard-N), and a name is not unique: /queue/add permits re-adding a
        # repo whose previous row is failed/revoked/done, and CLAUDE.md
        # *requires* a resume to reuse the exact original name. So a per-row
        # "this task is terminal" check is not enough to make a path safe to
        # delete — any live task sharing the name is writing to the same
        # directory, partial files and .progress.json markers included.
        live_names = {
            (t.get("name") or "") for t in tasks
            if t.get("status") not in TERMINAL
        }

        def runs_here(t) -> bool:
            """A sharded task's row carries server = NULL; its hosts live on
            the shard rows, so the task-level server alone matched nothing
            and this endpoint quietly cleaned nothing on a sharded fleet."""
            if t.get("server") == key:
                return True
            return any(s.get("server") == key
                       for s in get_shards_by_task(t.get("id") or ""))

        safe_to_clean = sorted({
            t["name"] for t in tasks
            if t.get("status") in TERMINAL
            and (t.get("name") or "") not in live_names
            and runs_here(t)
        })

        if not safe_to_clean:
            return {"cleaned": [], "message": "Nothing to clean"}

        servers = load_servers()
        server = servers.get(key)
        if not server:
            return {"error": f"Unknown server: {key}"}

        import re
        import shlex

        cleaned = []
        skipped = []
        for name in safe_to_clean:
            # No separators, no traversal. shlex.quote stops shell
            # metacharacters but not path components: a task named `..`
            # yields `rm -rf '/data/staging/..'` — i.e. rm -rf /data — on
            # that worker, and the name comes straight from the request body
            # of /queue/add with no validation anywhere upstream.
            if not re.match(r'^[A-Za-z0-9_.\-]+$', name) or name.startswith('.'):
                skipped.append(name)
                continue
            staging_dir = shlex.quote(f"/data/staging/{name}")
            try:
                ssh_exec(server.host, server.user, f"rm -rf {staging_dir}")
                cleaned.append(name)
            except Exception as e:
                logger.warning(f"cleanup {key}:{name} failed: {e}")
                skipped.append(name)

        return {"cleaned": cleaned, "count": len(cleaned), "skipped": skipped}

    return await run_blocking(_do)


@router.post("/task-progress")
async def task_progress(body: dict):
    """Receive real-time task progress from workers."""
    def _do():
        from ...queue.snapshot import (
            init_db, update_task_progress, complete_task, get_task,
        )
        init_db()
        task_id = body.get("task_id")
        if not task_id:
            return {"error": "task_id required"}
        # Terminal/operator states are durable: a progress report from a dying
        # or stale workflow must NOT resurrect a paused/revoked task — that
        # resurrection path made the reconciler re-dispatch tasks an operator
        # had explicitly stopped (2026-07-31 incident).
        task = get_task(task_id)
        if task and task.get("status") in TERMINAL_STATUSES:
            return {"ok": True, "ignored": f"task is {task['status']}"}

        status = body.get("status")
        if status in ("done", "failed"):
            for key in ("downloaded_gb", "progress_pct", "error"):
                if key in body:
                    update_task_progress(task_id, **{key: body[key]})
            # `phase` is forwarded here too (review GAP-3). The pool
            # coordinator's finalize step puts the missing-file note in it —
            # "3 file(s) missing, within ceiling 10 — GET ..." — and this
            # branch used to drop it, so a `done` task that forgave permanently
            # missing files rendered as an unqualified `done` and the honesty
            # the verdict promised existed only in the alert. Absent or null
            # phase still clears the column, which is what every sharded and
            # legacy terminal report sends.
            complete_task(task_id, status, phase=body.get("phase"),
                          # A `done` report carrying no error means this run had
                          # none — the row must not keep the previous run's
                          # failure string. /queue/retry clears it at
                          # re-dispatch, but a task retried BEFORE that fix
                          # shipped is already downloading with a stale error,
                          # and nothing else would ever empty the column.
                          # Only `done`: a `failed` report with no error text is
                          # better served by a stale reason than by none.
                          clear_error=(status == "done" and not body.get("error")))
            # Transfer arming hangs off THIS route on purpose: it is the one
            # place a coordinator workflow reports its own completion. The
            # reconciler's inferred `done` (reconciler.py, from shard rows) must
            # never arm — that inference is how t-20260805-460d45 became a
            # `done` task with nothing downloaded, and `completed_at` cannot
            # distinguish the two afterwards. Advisory: `arm_quietly` swallows
            # its own failures so transfer bookkeeping can never fail a
            # worker's progress report.
            if status == "done":
                from ...transfer.arm import arm_quietly
                arm_quietly(task_id)
            return {"ok": True, "completed": status}

        kwargs = {}
        for key in ("status", "speed_mbps", "progress_pct", "downloaded_gb",
                    "server", "phase", "error"):
            if key in body:
                kwargs[key] = body[key]
        update_task_progress(task_id, **kwargs)
        return {"ok": True}

    return await run_blocking(_do)


MISSING_FILES_REPORT_MAX = 1000


@router.post("/missing-files")
async def report_missing_files(body: dict):
    """Worker-side report of files that failed permanently in a batch.

    Body: task_id, batch_index?, server?, files: [{path, reason, size_bytes}].

    No auth, matching every other worker→S1 endpoint. That is a deliberate
    consistency choice rather than an oversight — a lone authenticated
    endpoint here would break the uniform worker contract without closing
    the surface.
    """
    def _do():
        from ...queue.snapshot import init_db, get_task, record_missing_files
        init_db()
        task_id = body.get("task_id")
        if not task_id:
            return {"error": "task_id required"}

        # Same durability guard as /task-progress: a zombie activity belonging
        # to a task an operator revoked must not keep writing rows against it.
        task = get_task(task_id)
        if task and task.get("status") in TERMINAL_STATUSES:
            return {"ok": True, "ignored": f"task is {task['status']}"}

        files = body.get("files") or []
        if not isinstance(files, list):
            return {"error": "files must be a list"}
        # A batch caps at 500 files, so anything near this ceiling is a bug or
        # a bad actor, not a real report. Refuse rather than truncate: a
        # silently truncated archive is worse than a visible rejection.
        if len(files) > MISSING_FILES_REPORT_MAX:
            raise HTTPException(
                status_code=413,
                detail=f"too many files ({len(files)} > {MISSING_FILES_REPORT_MAX})",
            )

        rows = [dict(f, batch_index=body.get("batch_index"),
                     server=body.get("server"))
                for f in files if isinstance(f, dict)]
        total = record_missing_files(task_id, rows)
        return {"ok": True, "recorded": len(rows), "task_missing_total": total}

    return await run_blocking(_do)


@router.post("/events")
async def receive_events(body: dict):
    """Batch receive monitoring events from workers. Stores in events table and updates metrics."""
    def _do():
        from ...queue.snapshot import init_db, _conn
        import json

        init_db()
        events = body.get("events", [])
        if not events:
            return {"ok": True, "processed": 0}

        conn = _conn()

        # Ensure events table exists
        conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT,
                server_key TEXT NOT NULL,
                event_type TEXT NOT NULL,
                data TEXT,
                timestamp REAL NOT NULL,
                created_at REAL DEFAULT (strftime('%s', 'now'))
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_task_time ON events(task_id, timestamp)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_type_time ON events(event_type, timestamp)"
        )

        # Insert events
        for event in events:
            data = event.get("data", {})
            conn.execute(
                "INSERT INTO events (task_id, server_key, event_type, data, timestamp) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    data.get("task_id", ""),
                    event.get("server_key", body.get("server_key", "")),
                    event.get("type", ""),
                    json.dumps(data),
                    event.get("timestamp", 0),
                ),
            )

        # Update task speed from recent download/upload events
        import time
        now = time.time()
        window = 30  # 30 second window for speed calculation

        # Ensure upload_speed_mbps column exists
        try:
            conn.execute("ALTER TABLE tasks ADD COLUMN upload_speed_mbps REAL DEFAULT 0")
            conn.commit()
        except Exception:
            pass  # column already exists

        # Calculate per-task download speed from events
        speed_rows = conn.execute("""
            SELECT task_id,
                   SUM(json_extract(data, '$.size_bytes')) as total_bytes
            FROM events
            WHERE event_type = 'file_downloaded'
              AND timestamp > ?
              AND task_id != ''
            GROUP BY task_id
        """, (now - window,)).fetchall()

        for row in speed_rows:
            task_id = row[0]
            total_bytes = row[1] or 0
            speed_mbps = (total_bytes * 8) / (window * 1_000_000)
            if speed_mbps > 0:
                conn.execute(
                    "UPDATE tasks SET speed_mbps = ?, updated_at = ? "
                    "WHERE id = ? AND status = 'downloading'",
                    (round(speed_mbps, 1), now, task_id),
                )

        # Calculate per-task upload speed from events
        ul_rows = conn.execute("""
            SELECT task_id,
                   SUM(json_extract(data, '$.size_bytes')) as total_bytes
            FROM events
            WHERE event_type = 'file_uploaded'
              AND timestamp > ?
              AND task_id != ''
            GROUP BY task_id
        """, (now - window,)).fetchall()

        for row in ul_rows:
            task_id = row[0]
            total_bytes = row[1] or 0
            ul_speed_mbps = (total_bytes * 8) / (window * 1_000_000)
            if ul_speed_mbps > 0:
                conn.execute(
                    "UPDATE tasks SET upload_speed_mbps = ?, updated_at = ? "
                    "WHERE id = ? AND status = 'downloading'",
                    (round(ul_speed_mbps, 1), now, task_id),
                )

        conn.commit()

        # Cleanup old events (keep 24h)
        cutoff = now - 86400
        conn.execute("DELETE FROM events WHERE timestamp < ?", (cutoff,))
        conn.commit()

        return {"ok": True, "processed": len(events)}

    return await run_blocking(_do)
