"""Servers API — Celery worker status."""

from fastapi import APIRouter, HTTPException

from ..cache import cache
from . import run_blocking

router = APIRouter(tags=["servers"])


@router.get("/servers")
async def list_servers():
    """Get all workers with live status."""
    def _do():
        from ...queue.snapshot import get_workers, init_db
        init_db()
        workers = get_workers()
        return {w.get("server_key", w["hostname"]): w for w in workers}

    cached = cache.get_servers()
    if cached:
        return {"servers": cached}
    data = await run_blocking(_do)
    return {"servers": data}


@router.get("/servers/{key}")
async def get_server(key: str):
    """Get a single worker's status."""
    data = cache.get_servers()
    if not data or key not in data:
        raise HTTPException(404, f"Worker {key} not found")
    return data[key]


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
    """Clean staging directory on a worker for done/failed/revoked tasks.

    Only removes staging dirs whose task is done, failed, or revoked.
    Never touches staging for active (downloading/pending) tasks.
    """
    def _do():
        from ...queue.snapshot import get_all_tasks, init_db
        from ...core.servers import load_servers
        from ...core.ssh import ssh_exec

        init_db()
        tasks = get_all_tasks()

        # Find tasks that are safe to clean (done/failed/revoked)
        safe_to_clean = [
            t["name"] for t in tasks
            if t.get("status") in ("done", "failed", "revoked")
            and t.get("server") == key
        ]

        if not safe_to_clean:
            return {"cleaned": [], "message": "Nothing to clean"}

        servers = load_servers()
        server = servers.get(key)
        if not server:
            return {"error": f"Unknown server: {key}"}

        import re
        import shlex

        cleaned = []
        for name in safe_to_clean:
            if not re.match(r'^[A-Za-z0-9_.\-/]+$', name):
                continue  # skip names with shell metacharacters
            staging_dir = shlex.quote(f"/data/staging/{name}")
            try:
                ssh_exec(server.host, server.user, f"rm -rf {staging_dir}")
                cleaned.append(name)
            except Exception:
                pass  # best effort

        return {"cleaned": cleaned, "count": len(cleaned)}

    return await run_blocking(_do)


@router.post("/task-progress")
async def task_progress(body: dict):
    """Receive real-time task progress from workers."""
    def _do():
        from ...queue.snapshot import init_db, update_task_progress
        init_db()
        task_id = body.get("task_id")
        if not task_id:
            return {"error": "task_id required"}
        kwargs = {}
        for key in ("status", "speed_mbps", "progress_pct", "downloaded_gb", "server", "phase"):
            if key in body:
                kwargs[key] = body[key]
        update_task_progress(task_id, **kwargs)
        return {"ok": True}

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
