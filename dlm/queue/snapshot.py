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

        -- Archive of files that entered a task's filelist but never made it
        -- to BOS. Deliberately NOT the `events` table: that one is a 24-hour
        -- rolling monitoring buffer (servers.py DELETEs rows older than 86400
        -- on every event), so a task that runs 10 hours and then sits failed
        -- for two days would have no record left. This is the only answer to
        -- "which files is dataset X missing", so it outlives the task's run.
        --
        -- Keyed on (task_id, file_path) rather than a rowid: the same file
        -- fails repeatedly across a batch's attempts and its re-dispatch
        -- round, and the useful fact is "this file is missing, tried N times",
        -- not N duplicate rows.
        CREATE TABLE IF NOT EXISTS missing_files (
            task_id     TEXT NOT NULL,
            file_path   TEXT NOT NULL,
            batch_index INTEGER,
            server      TEXT,
            reason      TEXT,
            size_bytes  INTEGER DEFAULT 0,
            attempts    INTEGER DEFAULT 1,
            first_seen  REAL,
            last_seen   REAL,
            PRIMARY KEY (task_id, file_path)
        );

        CREATE INDEX IF NOT EXISTS idx_missing_task ON missing_files(task_id);

        -- Process-lifetime-independent flags. The one that forced this table
        -- into existence is `transfer_paused`: it lived in the web process's
        -- in-memory `cache`, so the pause button worked until the next web
        -- restart and then silently un-paused itself. Anything an operator
        -- toggles and expects to stay toggled belongs here, not in `cache`.
        CREATE TABLE IF NOT EXISTS settings (
            key        TEXT PRIMARY KEY,
            value      TEXT,
            updated_at REAL
        );
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
        # Denormalised COUNT(*) of this task's missing_files rows. Not
        # redundant: alerts are purely DB-derived — check_alerts(tasks, workers)
        # runs every 10s on get_all_tasks(), which is a bare SELECT * FROM
        # tasks, so a joined count is not something it can see. As a column,
        # SELECT * carries it for free at zero extra queries per tick. The one
        # shape to avoid is a per-task count_missing_files() inside
        # check_alerts: get_all_tasks() returns every historical task, making
        # that O(all tasks) queries every 10 seconds.
        # Every writer below keeps this in step with the actual row count.
        ("missing_files_count", "INTEGER", "0"),
        # The ceiling the coordinator judged this task's missing files by
        # (`task_missing_limit(listed_files)` — see dlm/temporal/models.py),
        # written once at finalize. Stored rather than recomputed because the
        # inputs are gone by then: it is a function of the LISTING file count,
        # which lives on a worker's disk during the run and nowhere after it,
        # while alerting runs off `SELECT * FROM tasks` every 10s. Without the
        # column, alerts would need a second, necessarily different, threshold
        # — and the absurd window that follows is the whole reason it exists: a
        # 300-file task missing 50 is already `failed` by the coordinator's
        # max(10, 0.5%), but a hardcoded 100 in alerts would call it a WARNING.
        # 0 means "not finalized yet" ⇒ no CRITICAL verdict is possible.
        ("missing_files_limit", "INTEGER", "0"),
        # ── transfer (BOS → 地瓜云) bookkeeping ──────────────────────────────
        # `{bucket}/{prefix}` as computed at the moment the download was
        # dispatched (temporal_client.start_task_download, the single funnel for
        # both dispatch modes). This is the ONLY trustworthy record of where the
        # uploader actually wrote: `bos_path` is historically dirty (measured
        # 2026-08-10 — molmobot's column holds a 地瓜云 destination path) and
        # recomputing `bos_target()` at transfer time reads the CURRENT
        # name/category, so an operator renaming a finished task silently moves
        # the prefix. NULL on every row that predates this column ⇒ the drift
        # gate has nothing to compare and skips itself; it must not reject.
        ("dispatch_prefix", "TEXT", "NULL"),
        # The source `{bucket}/{prefix}` frozen at arm time. The dispatcher uses
        # this verbatim and never re-derives — same rename hazard as above.
        ("transfer_prefix", "TEXT", "NULL"),
        # `SUM(shards.total_bytes)` at arm time: the bytes THIS round dispatched.
        # Not `size_gb` (measured 3 accurate / 9 short) and never plus
        # `resume_skipped_gb` (accumulates across rounds — RoboDojo reads 715.7
        # vs 5518.2 GB). Used as the ratio denominator, so a resumed task's
        # ratio is legitimately >1 and the bands are deliberately one-sided.
        ("transfer_bytes", "INTEGER", "0"),
        # Arm timestamp: FIFO order for the dispatcher, and the only way to see
        # a row that has been `ready` for a suspiciously long time.
        ("transfer_armed_at", "REAL", "0"),
        # Bytes actually measured on the far side after the remote import
        # reported success, so a short transfer is queryable rather than living
        # only inside an error string. 0 = not verified yet.
        ("transfer_verified_bytes", "INTEGER", "0"),
        # What the BOS prefix measured at dispatch time — bytes and object
        # count. Two jobs, neither of which `transfer_bytes` can do:
        #
        #   1. the verification denominator. `transfer_bytes` counts only the
        #      bytes THIS round dispatched, so verifying against it would pass
        #      a resumed task that moved a fraction of its data (RoboDojo:
        #      715.7 GB dispatched against 6189.9 GB actually on BOS) — exactly
        #      the false-done class this feature exists to catch.
        #   2. the "BOS was not modified" check. The transfer only ever READS
        #      BOS, so a byte or object count that differs after the import
        #      means another actor wrote the prefix mid-transfer. Recorded and
        #      alerted, never used to block: it is information about the world,
        #      not a fault in the transfer.
        #
        # 0/0 = never measured (every row armed before the dispatcher existed).
        ("transfer_bos_bytes", "INTEGER", "0"),
        ("transfer_bos_objects", "INTEGER", "0"),
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


def get_setting(key: str, default=None):
    """Read a persisted operator flag. Returns `default` when unset."""
    conn = _conn()
    row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def set_setting(key: str, value):
    """Persist an operator flag. Stored as TEXT — callers coerce."""
    conn = _conn()
    conn.execute(
        "INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value, "
        "updated_at = excluded.updated_at",
        (key, str(value), time.time()),
    )
    conn.commit()


def set_dispatch_prefix(task_id: str, prefix: str):
    """Record where this dispatch will upload to (`{bucket}/{prefix}`).

    Written at dispatch, read by the transfer drift gate. Overwrites on every
    re-dispatch on purpose: the newest dispatch is the one whose uploader is
    running, so it is the prefix a later transfer must match.
    """
    conn = _conn()
    conn.execute(
        "UPDATE tasks SET dispatch_prefix = ?, updated_at = ? WHERE id = ?",
        (prefix, time.time(), task_id),
    )
    conn.commit()


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
                         error_class: str = None, clear_error: bool = False,
                         **extra):
    """Update progress fields for a running task (called by workers).

    Every field is skipped when None, so a caller can update one column
    without knowing the rest — which leaves no way to say "set error back to
    empty": `error=None` is indistinguishable from "not passing error". Both
    /queue/retry and /queue/resume passed `error=None` intending to clear,
    and silently didn't, so a re-dispatched task carried the failure string
    from its previous run while downloading fine. That makes the dashboard
    lie, which is worse than the original failure. `clear_error=True` is the
    explicit form; it also clears error_class, since the two are written
    together and a stale class keeps alerting on a run that has no error.
    """
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
    if clear_error:
        if error is not None or error_class is not None:
            raise ValueError(
                "update_task_progress: clear_error=True together with "
                f"error={error!r}/error_class={error_class!r} — the caller is "
                "asking to both set and clear the failure. Pick one."
            )
        sets.append("error = NULL")
        sets.append("error_class = NULL")

    vals.append(task_id)
    conn.execute(f"UPDATE tasks SET {', '.join(sets)} WHERE id = ?", vals)
    conn.commit()


def complete_task(task_id: str, status: str = "done", phase: Optional[str] = None,
                  clear_error: bool = False):
    """Mark task as completed or failed.

    `phase` defaults to clearing the column: a finished task must not keep
    "downloading batch 12/40" on the dashboard. But a caller may pass the one
    thing worth keeping past completion — the pool coordinator's missing-file
    note ("3 file(s) missing, within ceiling 10 — GET ...") is written for a
    `done` row and used to be wiped by this UPDATE, so the second tier of the
    missing-file verdict was honest only in the alert channel (review GAP-3).

    `clear_error` empties error/error_class. It is a caller's decision rather
    than an automatic consequence of status="done", because a `done` report MAY
    legitimately carry an error string (the T4 verdict forgives a few missing
    files and still says done), and this function runs after the route has
    already written it. Callers that know the run succeeded on its own terms —
    a completion report with no error, the reconciler's all-shards-done
    inference — pass True so the row does not keep the previous run's failure.
    """
    conn = _conn()
    now = time.time()
    from datetime import datetime, timezone
    completed_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    error_sql = ", error = NULL, error_class = NULL" if clear_error else ""
    conn.execute(
        "UPDATE tasks SET status = ?, completed_at = ?, updated_at = ?, "
        f"speed_mbps = 0, phase = ?{error_sql} WHERE id = ?",
        (status, completed_at, now, phase, task_id),
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
    # The ONLY path allowed to drop missing_files rows wholesale. Keeping them
    # past the task they point at would leave orphans keyed on a dead id, and
    # nothing can act on those. Every other cleanup path — complete_task,
    # staging gc, reconcile, reshard, resume, retry, doctor — must leave this
    # table alone: it is the archive of which files a dataset is missing, and
    # the task reaching a terminal state is precisely when someone wants it.
    conn.execute("DELETE FROM missing_files WHERE task_id = ?", (task_id,))
    conn.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
    conn.commit()


# ── Missing-file archive ────────────────────────────────────
#
# Scope boundary, worth knowing before treating a query result as complete:
# these are files that entered a task's filelist and then failed to reach BOS.
# Files the SOURCE reports as 0 bytes never enter a filelist at all — the
# listing activities require a truthy/positive size — so they cannot appear
# here. An empty result therefore does not mean "nothing is missing".


def record_missing_files(task_id: str, rows: list) -> int:
    """Upsert missing-file rows for a task; returns the task's new total.

    Each row: {path, reason?, size_bytes?, batch_index?, server?}. Idempotent
    per (task_id, path) — a repeat sighting bumps `attempts` and refreshes
    `last_seen`/`reason`/`server`/`batch_index` (latest attempt is the useful
    one) while preserving `first_seen`. Callers report on every attempt
    unconditionally, so re-upserting the same file up to six times over a
    batch's retry budget is the expected traffic, not a bug.
    """
    conn = _conn()
    now = time.time()
    for row in rows:
        path = row.get("path") or row.get("file_path")
        if not path:
            continue
        conn.execute(
            "INSERT INTO missing_files "
            "(task_id, file_path, batch_index, server, reason, size_bytes, "
            " attempts, first_seen, last_seen) "
            "VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?) "
            "ON CONFLICT(task_id, file_path) DO UPDATE SET "
            "  attempts = attempts + 1, "
            "  last_seen = excluded.last_seen, "
            "  reason = excluded.reason, "
            "  server = excluded.server, "
            "  batch_index = excluded.batch_index, "
            "  size_bytes = MAX(size_bytes, excluded.size_bytes)",
            (task_id, path, row.get("batch_index"), row.get("server"),
             row.get("reason"), int(row.get("size_bytes") or 0), now, now),
        )
    total = _sync_missing_count(conn, task_id)
    conn.commit()
    return total


def list_missing_files(task_id: str) -> list:
    conn = _conn()
    rows = conn.execute(
        "SELECT * FROM missing_files WHERE task_id = ? ORDER BY file_path",
        (task_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def count_missing_files(task_id: str) -> int:
    conn = _conn()
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM missing_files WHERE task_id = ?", (task_id,)
    ).fetchone()
    return int(row["n"]) if row else 0


def clear_missing_files(task_id: str, paths: list) -> int:
    """Drop specific rows a re-check proved present on BOS; returns new total.

    Path-scoped by design: the re-check verifies files one by one, and a
    wholesale clear would erase the rows it could not verify along with the
    ones it could.
    """
    conn = _conn()
    for path in paths:
        conn.execute(
            "DELETE FROM missing_files WHERE task_id = ? AND file_path = ?",
            (task_id, path),
        )
    total = _sync_missing_count(conn, task_id)
    conn.commit()
    return total


def set_missing_limit(task_id: str, limit: int) -> int:
    """Record the missing-file ceiling this task was judged by; returns its
    current missing count.

    Written by the coordinator at finalize (over HTTP — the workflow's
    verifying activity runs on a worker, which cannot touch SQLite). Alerting
    compares `missing_files_count > missing_files_limit > 0`, so a task that
    never finalized keeps 0 and is never called CRITICAL on a stale threshold.
    """
    conn = _conn()
    conn.execute(
        "UPDATE tasks SET missing_files_limit = ? WHERE id = ?",
        (max(0, int(limit)), task_id),
    )
    total = _sync_missing_count(conn, task_id)
    conn.commit()
    return total


def _sync_missing_count(conn, task_id: str) -> int:
    """Point tasks.missing_files_count at the real row count. Caller commits."""
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM missing_files WHERE task_id = ?", (task_id,)
    ).fetchone()
    total = int(row["n"]) if row else 0
    conn.execute(
        "UPDATE tasks SET missing_files_count = ? WHERE id = ?", (total, task_id)
    )
    return total


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

    That branch also carries the parent's mode as `task_dispatch_mode` (LEFT
    JOIN, so a row whose task is missing still comes back, with None). The
    reclaim needs it: a pool batch is an activity, not a child workflow, so
    the "no running shard-{id} execution" test is never true for a pool row,
    and its retry backoff legitimately exceeds the quiet grace. Without the
    mode, the reclaim writes healthy backing-off batches to `failed`. It is
    deliberately NOT added to the JOINed branch — those callers ask "who is
    busy", a question with the same answer in both modes.
    """
    conn = _conn()
    if include_stopped_tasks:
        rows = conn.execute(
            "SELECT s.*, t.dispatch_mode AS task_dispatch_mode "
            "FROM shards s LEFT JOIN tasks t ON t.id = s.task_id "
            "WHERE s.status = 'running'"
        ).fetchall()
        return [dict(r) for r in rows]
    placeholders = ", ".join("?" * len(_TERMINAL_TASK_STATUSES))
    rows = conn.execute(
        f"SELECT s.* FROM shards s JOIN tasks t ON t.id = s.task_id "
        f"WHERE s.status = 'running' AND t.status NOT IN ({placeholders})",
        _TERMINAL_TASK_STATUSES,
    ).fetchall()
    return [dict(r) for r in rows]


def count_live_pool_batches(queue_source: str, since: float) -> int:
    """Running pool batch rows on `queue_source`'s pool queue, reported since `since`.

    Evidence that some worker process is alive and draining that pool
    queue, for the two callers that cannot get that answer from
    DescribeTaskQueue: a pool worker runs one batch at a time, so a fully-busy
    fleet stops polling and reads identically to a fleet that never registered
    the queue (see fleet.POOL_LIVE_BATCH_WINDOW_S).

    `queue_source` is `modelscope` or `hf` — a QUEUE, not a task's raw source
    column. One queue is one worker pool, and workflows.pool_task_queue sends
    every non-modelscope source to pool-hf, so matching `t.source` exactly
    would count a subset: with an `hf` task saturating HK and a `huggingface`
    task asking, the answer would be 0 and the live fleet would read as dead.
    Pass fleet.pool_queue_source(task_source); the WHERE clause below mirrors
    pool_task_queue's rule and nothing else may be assumed about the column.

    All three conditions are worker-originated writes, which is what makes the
    count evidence rather than bookkeeping:
      - `server` is only ever set by POST /api/shards/assign, which
        `run_pool_batch` calls from the worker that claimed the batch. A
        dispatched-but-unclaimed row has a fresh `updated_at` (upsert_shard
        stamps it) and no server, and must not count.
      - `status = 'running'` arrives with the POST /api/shards/status that
        immediately follows the assign, so a row counts from the start of
        preflight — but only once that second POST lands. If it fails, the row
        keeps `pending` and this correctly declines to vouch for it.
      - `updated_at` is bumped by POST /api/shard-progress, which the same
        activity sends on a 15s throttle for as long as the pipeline runs. The
        one non-worker writer of pool rows,
        reconciler.reclaim_orphaned_shards, skips pool rows outright and
        writes `failed` (terminal) when it does write; zero_stale_speeds
        touches `speed_mbps` only. Neither can fake freshness here.

    The parent task's status is deliberately NOT filtered. It would be nearly
    redundant — /api/shard-progress and /api/shards/status both refuse to
    write once the parent is terminal, and pause releases the rows — but the
    question here is "is a worker process alive", and if one somehow does
    report, that answer is yes regardless of what the operator did to the
    task. Rows of a stopped task age out of the window on their own.
    """
    conn = _conn()
    # Mirrors workflows.pool_task_queue: pool-ms is exactly `modelscope`,
    # pool-hf is everything else (including NULL and '').
    source_clause = (
        "t.source = 'modelscope'" if queue_source == "modelscope"
        else "COALESCE(t.source, '') != 'modelscope'"
    )
    row = conn.execute(
        "SELECT COUNT(*) FROM shards s JOIN tasks t ON t.id = s.task_id "
        "WHERE s.status = 'running' AND t.dispatch_mode = 'pool' "
        f"AND {source_clause} "
        "AND s.server IS NOT NULL AND s.server != '' "
        "AND COALESCE(s.updated_at, 0) > ?",
        (since,),
    ).fetchone()
    return int(row[0]) if row else 0


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
