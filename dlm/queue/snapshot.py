"""SQLite snapshot — fast state reads for the web dashboard.

Workers report progress here; the dashboard reads from here (<10ms).
This replaces BOS state.json polling for the web layer.
"""

import logging
import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

DB_PATH = Path(os.environ.get("DLM_DB_PATH", "/data/dlm.db"))

_local = threading.local()


def _conn() -> sqlite3.Connection:
    # The cached connection is keyed by the path it was opened for. Without
    # that key, rebinding DB_PATH only affects threads that had not yet opened
    # a connection: the web layer's route executors are module-level and
    # long-lived, so a thread that opened the old path keeps serving it
    # silently. This surfaced as tests reading an earlier test's database
    # through the queue routes' ThreadPoolExecutor while believing they had
    # isolated themselves.
    path = str(DB_PATH)
    if getattr(_local, "conn", None) is None or getattr(_local, "path", None) != path:
        old = getattr(_local, "conn", None)
        if old is not None:
            try:
                old.close()
            except Exception:
                pass
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        _local.conn = sqlite3.connect(path, timeout=10)
        _local.conn.execute("PRAGMA journal_mode=WAL")
        _local.conn.execute("PRAGMA busy_timeout=5000")
        _local.conn.row_factory = sqlite3.Row
        _local.path = path
    return _local.conn


def init_db():
    """Create tables if they don't exist."""
    conn = _conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS tasks (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            repo_id TEXT,
            source TEXT,
            type TEXT,
            category TEXT,
            bos_path TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            server TEXT,
            priority INTEGER NOT NULL DEFAULT 5,
            size_gb REAL DEFAULT 0,
            downloaded_gb REAL DEFAULT 0,
            progress_pct REAL DEFAULT 0,
            speed_mbps REAL DEFAULT 0,
            phase TEXT,
            error TEXT,
            error_class TEXT,
            retry_count INTEGER DEFAULT 0,
            celery_task_id TEXT,
            transfer_status TEXT,
            transfer_task_id TEXT,
            transfer_error TEXT,
            created_at TEXT,
            started_at TEXT,
            completed_at TEXT,
            updated_at REAL
        );

        CREATE TABLE IF NOT EXISTS workers (
            hostname TEXT PRIMARY KEY,
            server_key TEXT,
            status TEXT DEFAULT 'offline',
            current_task_id TEXT,
            disk_free_gb REAL,
            last_seen REAL
        );

        CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
        CREATE INDEX IF NOT EXISTS idx_tasks_celery ON tasks(celery_task_id);

        CREATE TABLE IF NOT EXISTS shards (
            id           TEXT PRIMARY KEY,
            task_id      TEXT NOT NULL,
            shard_index  INTEGER NOT NULL,
            server       TEXT,
            status       TEXT NOT NULL DEFAULT 'pending',
            total_files  INTEGER DEFAULT 0,
            done_files   INTEGER DEFAULT 0,
            total_bytes  INTEGER DEFAULT 0,
            done_bytes   INTEGER DEFAULT 0,
            speed_mbps   REAL DEFAULT 0,
            error        TEXT,
            filelist_key TEXT,
            started_at   TEXT,
            completed_at TEXT,
            updated_at   REAL
        );

        CREATE INDEX IF NOT EXISTS idx_shards_task ON shards(task_id);
        CREATE INDEX IF NOT EXISTS idx_shards_server ON shards(server);
        CREATE INDEX IF NOT EXISTS idx_shards_status ON shards(status);
    """)
    conn.commit()

    # Add shard-related columns to tasks table (safe migration)
    for col, coltype, default in [
        ("total_shards", "INTEGER", "1"),
        ("done_shards", "INTEGER", "0"),
        ("max_workers", "INTEGER", "0"),
        ("shard_strategy", "TEXT", "'auto'"),
        ("resume_skipped_files", "INTEGER", "0"),
        ("resume_skipped_gb", "REAL", "0"),
        ("claimed_at", "REAL", "0"),
        # Pool dispatch (work-stealing) groundwork — dispatch_mode picks the
        # coordinator a task runs under, coordinator_phase is the listing
        # guard's replacement for the NOT EXISTS(shards) probe once pool
        # tasks can have batch rows before dispatching starts.
        # Both are COALESCE-preserved in upsert_task, so a phase moves between
        # named values only: passing None leaves the stored phase alone rather
        # than clearing it. Use a named terminal phase, not None, to say "done".
        ("dispatch_mode", "TEXT", "'sharded'"),
        ("coordinator_phase", "TEXT", "NULL"),
    ]:
        try:
            conn.execute(f"ALTER TABLE tasks ADD COLUMN {col} {coltype} DEFAULT {default}")
        except Exception:
            pass


# Assignment fragment every route that (re)starts a coordinator must include
# in its claim UPDATE, so the listing guard reads a phase belonging to the
# CURRENT coordinator run.
#
# Nothing clears coordinator_phase: resume and reshard put a task back to
# `pending` and delete its batch rows while leaving the column at
# 'dispatching'. A later claim that only refreshed status/claimed_at would
# then present a listing coordinator as "past listing" and the guard would
# let a second coordinator onto the same source — the exact double dispatch
# it exists to prevent. Written as one CASE rather than a per-mode branch
# because there are two claim sites a pool task can reach (auto_dispatch and
# /queue/preempt) and a third is a matter of time; sharded rows write their
# own value back, so their behaviour is unchanged. reconcile()'s orphan
# re-dispatch was a third site until decision C stopped re-dispatching pool
# tasks at all — see the comment at that UPDATE for why the fragment is
# deliberately absent there.
CLAIM_RESET_PHASE_SQL = (
    "coordinator_phase = CASE WHEN dispatch_mode = 'pool' "
    "THEN 'listing' ELSE coordinator_phase END"
)


def upsert_task(task: dict):
    """Insert or update a task record."""
    conn = _conn()
    task["updated_at"] = time.time()
    columns = [
        "id", "name", "repo_id", "source", "type", "category", "bos_path",
        "status", "server", "priority", "size_gb", "downloaded_gb",
        "progress_pct", "speed_mbps", "phase", "error", "error_class",
        "retry_count", "celery_task_id", "transfer_status", "transfer_task_id",
        "transfer_error", "created_at", "started_at", "completed_at", "updated_at",
        "max_workers", "dispatch_mode", "coordinator_phase",
    ]
    values = [task.get(c) for c in columns]
    placeholders = ", ".join(["?"] * len(columns))
    col_str = ", ".join(columns)
    # COALESCE for the columns most callers don't know about: an update that
    # omits them must not blank them. dispatch_mode in particular decides
    # which coordinator a task gets — losing it silently reverts a pool task
    # to sharded on the next progress write.
    _preserve_if_omitted = ("max_workers", "dispatch_mode", "coordinator_phase")
    updates = ", ".join(
        f"{c}=COALESCE(excluded.{c}, tasks.{c})" if c in _preserve_if_omitted
        else f"{c}=excluded.{c}"
        for c in columns if c != "id"
    )

    conn.execute(
        f"INSERT INTO tasks ({col_str}) VALUES ({placeholders}) "
        f"ON CONFLICT(id) DO UPDATE SET {updates}",
        values,
    )
    # Naming dispatch_mode in the column list means an omitted key INSERTs an
    # explicit NULL, which overrides the schema DEFAULT — so a task created by
    # any of the callers that don't know about the column would have no mode at
    # all. Defaulting in Python instead would break the COALESCE above: an
    # update that supplied 'sharded' would revert a running pool task. Applying
    # it here covers both — on insert the NULL becomes 'sharded', on update
    # COALESCE has already preserved whatever was stored.
    conn.execute(
        "UPDATE tasks SET dispatch_mode='sharded' "
        "WHERE id=? AND dispatch_mode IS NULL",
        (task.get("id"),),
    )
    conn.commit()


def update_task_progress(task_id: str, progress_pct: float = None,
                         speed_mbps: float = None, downloaded_gb: float = None,
                         phase: str = None, server: str = None,
                         status: str = None, error: str = None,
                         error_class: str = None, **extra):
    """Update progress fields for a running task (called by workers)."""
    conn = _conn()
    sets = ["updated_at = ?"]
    vals = [time.time()]

    if progress_pct is not None:
        sets.append("progress_pct = ?")
        vals.append(progress_pct)
    if speed_mbps is not None:
        sets.append("speed_mbps = ?")
        vals.append(speed_mbps)
    if downloaded_gb is not None:
        sets.append("downloaded_gb = ?")
        vals.append(downloaded_gb)
    if phase is not None:
        sets.append("phase = ?")
        vals.append(phase)
    if server is not None:
        sets.append("server = ?")
        vals.append(server)
    if status is not None:
        sets.append("status = ?")
        vals.append(status)
    if error is not None:
        sets.append("error = ?")
        vals.append(error)
    if error_class is not None:
        sets.append("error_class = ?")
        vals.append(error_class)

    vals.append(task_id)
    conn.execute(f"UPDATE tasks SET {', '.join(sets)} WHERE id = ?", vals)
    conn.commit()


def complete_task(task_id: str, status: str = "done"):
    """Mark task as completed or failed."""
    conn = _conn()
    now = time.time()
    from datetime import datetime, timezone
    completed_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    conn.execute(
        "UPDATE tasks SET status = ?, completed_at = ?, updated_at = ?, "
        "speed_mbps = 0, phase = NULL WHERE id = ?",
        (status, completed_at, now, task_id),
    )
    conn.commit()


def get_task(task_id: str) -> Optional[dict]:
    """Get a single task by ID."""
    conn = _conn()
    row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
    if row:
        return dict(row)
    return None


def get_all_tasks() -> list:
    """Get all tasks ordered by status priority then creation time."""
    conn = _conn()
    rows = conn.execute(
        "SELECT * FROM tasks ORDER BY "
        "CASE status "
        "  WHEN 'downloading' THEN 0 "
        "  WHEN 'pending' THEN 1 "
        "  WHEN 'failed' THEN 2 "
        "  WHEN 'done' THEN 3 "
        "  WHEN 'revoked' THEN 4 "
        "  ELSE 5 END, "
        "priority ASC, created_at ASC"
    ).fetchall()
    return [dict(r) for r in rows]


def get_tasks_by_status(status: str) -> list:
    """Get tasks filtered by status."""
    conn = _conn()
    rows = conn.execute(
        "SELECT * FROM tasks WHERE status = ? ORDER BY priority ASC, created_at ASC",
        (status,),
    ).fetchall()
    return [dict(r) for r in rows]


def update_worker(hostname: str, server_key: str, status: str = "online",
                  current_task_id: str = None, disk_free_gb: float = None,
                  download_process_alive: bool = None, download_process_pid: int = None,
                  https_connections: int = None, files_last_5min: int = None,
                  staging_size_mb: int = None, event_buffer_pending: int = None):
    """Update worker heartbeat in snapshot (supports sidecar extra fields)."""
    conn = _conn()

    # Ensure extended columns exist. Keyed by database path, not a bare
    # once-per-thread flag: a thread that migrated one database would otherwise
    # skip the ALTERs after DB_PATH is rebound and insert into a table without
    # the columns (the same hazard as the connection cache above).
    if getattr(_local, "workers_schema_migrated", None) != str(DB_PATH):
        for col, col_type in [
            ("download_process_alive", "INTEGER"),
            ("download_process_pid", "INTEGER"),
            ("https_connections", "INTEGER"),
            ("files_last_5min", "INTEGER"),
            ("staging_size_mb", "INTEGER"),
            ("event_buffer_pending", "INTEGER"),
        ]:
            try:
                conn.execute(f"ALTER TABLE workers ADD COLUMN {col} {col_type}")
            except Exception:
                pass  # column already exists
        _local.workers_schema_migrated = str(DB_PATH)

    conn.execute(
        "INSERT INTO workers (hostname, server_key, status, current_task_id, "
        "disk_free_gb, download_process_alive, download_process_pid, "
        "https_connections, files_last_5min, staging_size_mb, event_buffer_pending, last_seen) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(hostname) DO UPDATE SET "
        "server_key=excluded.server_key, status=excluded.status, "
        "current_task_id=excluded.current_task_id, disk_free_gb=excluded.disk_free_gb, "
        "download_process_alive=excluded.download_process_alive, "
        "download_process_pid=excluded.download_process_pid, "
        "https_connections=excluded.https_connections, "
        "files_last_5min=excluded.files_last_5min, "
        "staging_size_mb=excluded.staging_size_mb, "
        "event_buffer_pending=excluded.event_buffer_pending, "
        "last_seen=excluded.last_seen",
        (hostname, server_key, status, current_task_id, disk_free_gb,
         1 if download_process_alive else (0 if download_process_alive is False else None),
         download_process_pid, https_connections, files_last_5min,
         staging_size_mb, event_buffer_pending, time.time()),
    )
    conn.commit()


def get_workers() -> list:
    """Get all workers."""
    conn = _conn()
    rows = conn.execute("SELECT * FROM workers ORDER BY server_key").fetchall()
    return [dict(r) for r in rows]


def get_dashboard_summary() -> dict:
    """Build dashboard summary from SQLite (fast)."""
    conn = _conn()

    total = conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
    by_status = {}
    for row in conn.execute("SELECT status, COUNT(*) as cnt FROM tasks GROUP BY status"):
        by_status[row["status"]] = row["cnt"]

    total_downloaded = conn.execute(
        "SELECT COALESCE(SUM(CASE "
        "  WHEN status = 'done' THEN COALESCE(NULLIF(size_gb, 0), downloaded_gb) "
        "  ELSE downloaded_gb END), 0) FROM tasks"
    ).fetchone()[0]
    total_estimated = conn.execute(
        "SELECT COALESCE(SUM(size_gb), 0) FROM tasks WHERE size_gb > 0"
    ).fetchone()[0]

    # Check if upload_speed_mbps column exists
    cols = [r[1] for r in conn.execute("PRAGMA table_info(tasks)").fetchall()]
    has_upload_speed = "upload_speed_mbps" in cols

    if has_upload_speed:
        active = conn.execute(
            "SELECT id, name, server, progress_pct, downloaded_gb, size_gb, "
            "speed_mbps, upload_speed_mbps, phase FROM tasks WHERE status = 'downloading' "
            "ORDER BY updated_at DESC"
        ).fetchall()
    else:
        active = conn.execute(
            "SELECT id, name, server, progress_pct, downloaded_gb, size_gb, "
            "speed_mbps, phase FROM tasks WHERE status = 'downloading' "
            "ORDER BY updated_at DESC"
        ).fetchall()

    aggregate_dl_speed = sum(r["speed_mbps"] or 0 for r in active)
    aggregate_ul_speed = sum(
        (r["upload_speed_mbps"] or 0) for r in active
    ) if has_upload_speed else 0.0

    return {
        "total_tasks": total,
        "by_status": by_status,
        "total_downloaded_tb": round(total_downloaded / 1000, 2),
        "total_estimated_tb": round(total_estimated / 1000, 2),
        "aggregate_speed_mbps": round(aggregate_dl_speed, 1),
        "aggregate_download_speed_mbps": round(aggregate_dl_speed, 1),
        "aggregate_upload_speed_mbps": round(aggregate_ul_speed, 1),
        "active_downloads": [dict(r) for r in active],
        "updated_at": time.time(),
    }


def delete_task(task_id: str):
    """Remove a task from the snapshot."""
    conn = _conn()
    conn.execute("DELETE FROM shards WHERE task_id = ?", (task_id,))
    conn.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
    conn.commit()


# ── Shard CRUD ──────────────────────────────────────────────


def upsert_shard(shard: dict):
    conn = _conn()
    shard.setdefault("updated_at", time.time())
    keys = list(shard.keys())
    placeholders = ", ".join(["?"] * len(keys))
    cols = ", ".join(keys)
    updates = ", ".join(f"{k} = excluded.{k}" for k in keys if k != "id")
    conn.execute(
        f"INSERT INTO shards ({cols}) VALUES ({placeholders}) "
        f"ON CONFLICT(id) DO UPDATE SET {updates}",
        [shard[k] for k in keys],
    )
    conn.commit()


def get_shard(shard_id: str) -> Optional[dict]:
    conn = _conn()
    row = conn.execute("SELECT * FROM shards WHERE id = ?", (shard_id,)).fetchone()
    return dict(row) if row else None


def get_shards_by_task(task_id: str) -> list:
    conn = _conn()
    rows = conn.execute(
        "SELECT * FROM shards WHERE task_id = ? ORDER BY shard_index", (task_id,)
    ).fetchall()
    return [dict(r) for r in rows]


def get_shards_by_status(status: str) -> list:
    conn = _conn()
    rows = conn.execute("SELECT * FROM shards WHERE status = ?", (status,)).fetchall()
    return [dict(r) for r in rows]


# Mirror of dlm.web.fleet.TERMINAL_STATUSES. snapshot.py is imported by the
# web layer, the workers, and the CLI — importing dlm.web.fleet here to save
# one tuple would invert that dependency, so the tuple is replicated instead.
_TERMINAL_TASK_STATUSES = ("paused", "preempted", "revoked", "skipped", "failed", "done")


def get_running_shards(include_stopped_tasks: bool = False) -> list:
    """Shard rows still running, whose parent task hasn't been stopped.

    A cancelled/paused/revoked task's in-flight shard rows used to stay
    `running` forever — nothing rewrites them once the workflow that would
    have completed them is gone — which pinned their servers "busy" for
    every caller (auto_dispatch, idle-worker queries, doctor) until the row
    was deleted by a resume/reshard. The JOIN excludes those stale rows at
    the source instead of requiring every caller to filter them out.

    `include_stopped_tasks=True` drops the JOIN and returns every running row,
    including ones whose parent is terminal or missing. Exactly one caller
    wants that: reconciler.reclaim_orphaned_shards, which does not read these
    rows to decide who is busy — it REWRITES them. Terminating a task's
    workflows is precisely what leaves its children unable to report, so the
    rows the JOIN hides are the ones most in need of being written back to
    `failed`; hiding them from the reclaim would leave them `running` in the
    database forever, visible in /api/tasks/{id}/shards, and dependent on the
    JOIN staying in every future read path.
    """
    conn = _conn()
    if include_stopped_tasks:
        rows = conn.execute("SELECT * FROM shards WHERE status = 'running'").fetchall()
        return [dict(r) for r in rows]
    placeholders = ", ".join("?" * len(_TERMINAL_TASK_STATUSES))
    rows = conn.execute(
        f"SELECT s.* FROM shards s JOIN tasks t ON t.id = s.task_id "
        f"WHERE s.status = 'running' AND t.status NOT IN ({placeholders})",
        _TERMINAL_TASK_STATUSES,
    ).fetchall()
    return [dict(r) for r in rows]


def get_task_servers(task_id: str) -> list:
    """Machines currently running work for a task, in both dispatch modes.

    Pool batch rows and sharded shard rows share this table; the parent
    task's dispatch_mode is the only discriminator, and neither mode needs
    a different question asked here. `tasks.server` cannot answer it: a
    claim deliberately writes NULL there because the coordinator assigns
    servers per shard/batch.
    """
    conn = _conn()
    rows = conn.execute(
        "SELECT DISTINCT server FROM shards WHERE task_id = ? "
        "AND status = 'running' AND server IS NOT NULL "
        "ORDER BY server",
        (task_id,),
    ).fetchall()
    return [r["server"] for r in rows]


def update_shard_progress(shard_id: str, **fields):
    conn = _conn()
    fields["updated_at"] = time.time()
    sets = ", ".join(f"{k} = ?" for k in fields)
    conn.execute(
        f"UPDATE shards SET {sets} WHERE id = ?",
        [*fields.values(), shard_id],
    )
    conn.commit()


def complete_shard(shard_id: str, status: str = "done"):
    conn = _conn()
    from datetime import datetime, timezone
    conn.execute(
        "UPDATE shards SET status = ?, speed_mbps = 0, completed_at = ?, updated_at = ? WHERE id = ?",
        (status, datetime.now(timezone.utc).isoformat(), time.time(), shard_id),
    )
    conn.commit()


def delete_shards_by_task(task_id: str):
    conn = _conn()
    conn.execute("DELETE FROM shards WHERE task_id = ?", (task_id,))
    conn.commit()
