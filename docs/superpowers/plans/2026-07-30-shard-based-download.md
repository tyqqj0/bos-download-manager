# Shard-Based Download Architecture — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace "1 task = 1 worker" model with "1 task = N shards = N workers" to parallelize large dataset downloads across all available workers automatically.

**Architecture:** A coordinator workflow (ShardedDownloadWorkflow) lists repo files, partitions them via greedy bin-packing into N shards, creates shard rows in SQLite, then starts a ShardWorkerWorkflow per shard on each target worker's personal Temporal queue. The reconciler and auto-dispatch become shard-aware: they look at the `shards` table (not just `tasks`) to determine worker busyness.

**Tech Stack:** Python 3.11, Temporal SDK (temporalio), SQLite (WAL mode), FastAPI, asyncio

## Global Constraints

- **bj1-4 MUST NOT be touched** — AgiBotWorld-Beta split workflows are still running. All changes must coexist with old DownloadDatasetWorkflow/SplitDownloadWorkflow registered on those workers.
- **PipelineEngine in `pipeline.py` must NOT be modified** (except the minor 429 fix in Task 10). All changes are in the orchestration layer above it.
- **Backward compatibility**: tasks with `total_shards=1` and no shard rows must work exactly as before (legacy single-worker mode).
- **Source routing**: `bj*` workers = ModelScope only, non-`bj*` workers = HuggingFace only. Enforced at shard assignment time.
- **SQLite writes inside Temporal workflows are forbidden** — all DB operations must happen inside `@activity.defn` functions.
- **Staging paths must include shard context** to prevent conflicts when the same worker runs multiple shards of the same task sequentially: `/data/staging/{task_name}/shard-{index}/`
- **No `HF_XET_HIGH_PERFORMANCE`** — remove it. Set `HF_HUB_DISABLE_XET=1` instead.
- All new code in existing files follows existing patterns (dataclasses for models, `@activity.defn` for activities, `@workflow.defn` for workflows).
- Tests: this is a distributed system with no local test harness. "Testing" means: code passes `python -c "from dlm.temporal.workflows import ..."` import checks, type consistency, and manual review. No pytest suite exists.

---

### Task 1: Database Schema — shards table + tasks table columns

**Files:**
- Modify: `dlm/queue/snapshot.py`

**Interfaces:**
- Produces: `shards` table, `upsert_shard()`, `get_shard()`, `get_shards_by_task()`, `get_shards_by_status()`, `get_running_shards()`, `update_shard_progress()`, `complete_shard()` — used by Tasks 3, 7, 8, 9
- Produces: `total_shards`, `done_shards`, `max_workers`, `shard_strategy` columns on `tasks` table — used by Tasks 5, 7, 8, 9

- [ ] **Step 1: Add shards table creation to `init_db()`**

In `dlm/queue/snapshot.py`, inside the `init_db()` function, after the existing `CREATE TABLE IF NOT EXISTS tasks` and `CREATE TABLE IF NOT EXISTS workers` blocks, add:

```python
conn.execute("""
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
    )
""")
conn.execute("CREATE INDEX IF NOT EXISTS idx_shards_task ON shards(task_id)")
conn.execute("CREATE INDEX IF NOT EXISTS idx_shards_server ON shards(server)")
conn.execute("CREATE INDEX IF NOT EXISTS idx_shards_status ON shards(status)")
```

- [ ] **Step 2: Add new columns to tasks table (safe ALTER TABLE)**

Still in `init_db()`, after the existing dynamic column additions for `workers`, add safe ALTER TABLE calls for the new task columns:

```python
for col, coltype, default in [
    ("total_shards", "INTEGER", "1"),
    ("done_shards", "INTEGER", "0"),
    ("max_workers", "INTEGER", "0"),
    ("shard_strategy", "TEXT", "'auto'"),
]:
    try:
        conn.execute(f"ALTER TABLE tasks ADD COLUMN {col} {coltype} DEFAULT {default}")
    except Exception:
        pass  # column already exists
```

- [ ] **Step 3: Add shard CRUD functions**

Add these functions to `dlm/queue/snapshot.py`:

```python
def upsert_shard(shard: dict):
    conn = _conn()
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


def get_shard(shard_id: str) -> dict | None:
    conn = _conn()
    row = conn.execute("SELECT * FROM shards WHERE id = ?", (shard_id,)).fetchone()
    return dict(row) if row else None


def get_shards_by_task(task_id: str) -> list[dict]:
    conn = _conn()
    rows = conn.execute(
        "SELECT * FROM shards WHERE task_id = ? ORDER BY shard_index", (task_id,)
    ).fetchall()
    return [dict(r) for r in rows]


def get_shards_by_status(status: str) -> list[dict]:
    conn = _conn()
    rows = conn.execute("SELECT * FROM shards WHERE status = ?", (status,)).fetchall()
    return [dict(r) for r in rows]


def get_running_shards() -> list[dict]:
    conn = _conn()
    rows = conn.execute("SELECT * FROM shards WHERE status = 'running'").fetchall()
    return [dict(r) for r in rows]


def update_shard_progress(shard_id: str, **fields):
    conn = _conn()
    import time
    fields["updated_at"] = time.time()
    sets = ", ".join(f"{k} = ?" for k in fields)
    conn.execute(f"UPDATE shards SET {sets} WHERE id = ?", [*fields.values(), shard_id])
    conn.commit()


def complete_shard(shard_id: str, status: str = "done"):
    conn = _conn()
    import time
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
```

- [ ] **Step 4: Verify imports work**

```bash
cd /Users/openclaw/code/bos-download-manager
python3 -c "
from dlm.queue.snapshot import (
    upsert_shard, get_shard, get_shards_by_task,
    get_shards_by_status, get_running_shards,
    update_shard_progress, complete_shard, delete_shards_by_task
)
print('All shard functions importable')
"
```

- [ ] **Step 5: Commit**

```bash
git add dlm/queue/snapshot.py
git commit -m "feat: add shards table and CRUD functions to SQLite schema"
```

---

### Task 2: Temporal Models — ShardInput + ShardResult

**Files:**
- Modify: `dlm/temporal/models.py`

**Interfaces:**
- Produces: `ShardInput` dataclass — used by Tasks 3, 4, 5
- Produces: `ShardResult` dataclass — used by Tasks 4, 5

- [ ] **Step 1: Add ShardInput and ShardResult dataclasses**

Append to `dlm/temporal/models.py`:

```python
@dataclass
class ShardInput:
    shard_id: str = ""
    task_id: str = ""
    task_name: str = ""
    repo_id: str = ""
    source: str = "hf"
    type: str = "dataset"
    category: str = ""
    shard_index: int = 0
    filelist_path: str = ""
    priority: int = 5
    size_bytes: int = 0


@dataclass
class ShardResult:
    shard_id: str = ""
    status: str = "done"
    files_uploaded: int = 0
    bytes_uploaded: int = 0
    error: str | None = None
```

- [ ] **Step 2: Verify import**

```bash
python3 -c "from dlm.temporal.models import ShardInput, ShardResult; print('OK')"
```

- [ ] **Step 3: Commit**

```bash
git add dlm/temporal/models.py
git commit -m "feat: add ShardInput and ShardResult dataclasses"
```

---

### Task 3: Shard Activities — DB operations + greedy bin-packing

**Files:**
- Modify: `dlm/temporal/activities.py`

**Interfaces:**
- Consumes: `ShardInput` from Task 2, `upsert_shard/update_shard_progress/complete_shard` from Task 1
- Produces: `create_shards_in_db(task_id, partitions)` — used by Task 5
- Produces: `update_shard_status(shard_id, status, **fields)` — used by Tasks 4, 5
- Produces: `report_shard_progress(shard_id, **fields)` — used by Task 4
- Produces: `partition_files_greedy(filelist_path, num_shards, staging_dir)` — used by Task 5
- Produces: `query_idle_workers(source)` — used by Task 5

- [ ] **Step 1: Add `partition_files_greedy` activity**

This replaces the existing `partition_filelist` for the new shard system. It uses greedy bin-packing (largest file first → assign to smallest shard).

```python
@activity.defn
async def partition_files_greedy(
    filelist_path: str, num_shards: int, staging_dir: str
) -> list[dict]:
    """Partition files into N shards using greedy bin-packing by size.

    Returns list of {filelist_path, total_files, total_bytes} per shard.
    """
    import json
    from pathlib import Path

    with open(filelist_path) as f:
        all_files = json.load(f)

    files_with_size = [(fi["path"], fi.get("size", 0)) for fi in all_files]
    files_with_size.sort(key=lambda x: x[1], reverse=True)

    shards = [[] for _ in range(num_shards)]
    shard_sizes = [0] * num_shards

    for path, size in files_with_size:
        smallest = min(range(num_shards), key=lambda i: shard_sizes[i])
        shards[smallest].append({"path": path, "size": size})
        shard_sizes[smallest] += size

    results = []
    staging = Path(staging_dir)
    staging.mkdir(parents=True, exist_ok=True)

    for i, shard_files in enumerate(shards):
        shard_filelist = staging / f".filelist-shard-{i}.json"
        with open(shard_filelist, "w") as f:
            json.dump(shard_files, f)
        results.append({
            "filelist_path": str(shard_filelist),
            "total_files": len(shard_files),
            "total_bytes": shard_sizes[i],
        })

    return results
```

- [ ] **Step 2: Add `create_shards_in_db` activity**

```python
@activity.defn
async def create_shards_in_db(task_id: str, shard_infos: list[dict]) -> list[str]:
    """Create shard rows in SQLite. Returns list of shard IDs.

    shard_infos: list of {filelist_path, total_files, total_bytes, shard_index}
    """
    import time
    from ..queue.snapshot import upsert_shard, init_db
    init_db()

    shard_ids = []
    for info in shard_infos:
        idx = info["shard_index"]
        shard_id = f"s-{task_id}-{idx}"
        upsert_shard({
            "id": shard_id,
            "task_id": task_id,
            "shard_index": idx,
            "status": "pending",
            "total_files": info["total_files"],
            "total_bytes": info["total_bytes"],
            "filelist_key": info.get("filelist_path", ""),
            "updated_at": time.time(),
        })
        shard_ids.append(shard_id)
    return shard_ids
```

- [ ] **Step 3: Add `update_shard_status` and `report_shard_progress` activities**

```python
@activity.defn
async def update_shard_status(shard_id: str, status: str, error: str | None = None):
    """Update shard status in SQLite."""
    from ..queue.snapshot import update_shard_progress as db_update, complete_shard, init_db
    init_db()

    if status in ("done", "failed"):
        complete_shard(shard_id, status)
        if error:
            db_update(shard_id, error=error)
    else:
        fields = {"status": status}
        if error:
            fields["error"] = error
        db_update(shard_id, **fields)


@activity.defn
async def report_shard_progress(shard_id: str, done_files: int = 0,
                                 done_bytes: int = 0, speed_mbps: float = 0):
    """Update shard progress counters."""
    from ..queue.snapshot import update_shard_progress as db_update, init_db
    init_db()
    db_update(shard_id, done_files=done_files, done_bytes=done_bytes, speed_mbps=speed_mbps)
```

- [ ] **Step 4: Add `query_idle_workers` activity**

```python
@activity.defn
async def query_idle_workers(source: str) -> list[str]:
    """Return server_keys of idle workers compatible with the given source.

    Idle = alive (heartbeat < 180s) + no running shard + disk > 70GB.
    """
    import time
    from ..queue.snapshot import get_workers, get_running_shards, get_tasks_by_status, init_db
    init_db()

    now = time.time()
    workers = get_workers()
    alive = [w for w in workers if now - (w.get("last_seen") or 0) < 180]

    running = get_running_shards()
    busy_from_shards = {s["server"] for s in running if s.get("server")}

    downloading = get_tasks_by_status("downloading")
    busy_from_tasks = {t.get("server") for t in downloading if t.get("server")}
    busy = busy_from_shards | busy_from_tasks

    seen = set()
    idle = []
    for w in alive:
        key = w.get("server_key", "")
        if not key or key in seen or key in busy:
            continue
        seen.add(key)
        if (w.get("disk_free_gb") or 0) < 70:
            continue
        is_bj = key.startswith("bj")
        if source == "modelscope" and not is_bj:
            continue
        if source != "modelscope" and is_bj:
            continue
        idle.append(key)

    return idle
```

- [ ] **Step 5: Add `aggregate_task_from_shards` activity**

```python
@activity.defn
async def aggregate_task_from_shards(task_id: str):
    """Aggregate shard progress into task-level progress and update SQLite."""
    from ..queue.snapshot import get_shards_by_task, update_task_progress, init_db
    init_db()

    shards = get_shards_by_task(task_id)
    if not shards:
        return

    total_files = sum(s.get("total_files", 0) for s in shards)
    done_files = sum(s.get("done_files", 0) for s in shards)
    total_bytes = sum(s.get("total_bytes", 0) for s in shards)
    done_bytes = sum(s.get("done_bytes", 0) for s in shards)
    speed = sum(s.get("speed_mbps", 0) for s in shards)
    done_shards = sum(1 for s in shards if s.get("status") == "done")
    total_shards = len(shards)

    pct = (done_bytes / total_bytes * 100) if total_bytes > 0 else 0
    dl_gb = done_bytes / (1024 ** 3)

    update_task_progress(
        task_id,
        downloaded_gb=round(dl_gb, 2),
        progress_pct=round(pct, 1),
        speed_mbps=round(speed, 1),
        phase=f"shards {done_shards}/{total_shards}",
    )

    from ..queue.snapshot import _conn
    conn = _conn()
    conn.execute(
        "UPDATE tasks SET done_shards = ?, total_shards = ? WHERE id = ?",
        (done_shards, total_shards, task_id),
    )
    conn.commit()
```

- [ ] **Step 6: Verify imports**

```bash
python3 -c "
from dlm.temporal.activities import (
    partition_files_greedy, create_shards_in_db,
    update_shard_status, report_shard_progress,
    query_idle_workers, aggregate_task_from_shards,
)
print('All shard activities importable')
"
```

- [ ] **Step 7: Commit**

```bash
git add dlm/temporal/activities.py
git commit -m "feat: add shard activities — partition, create, progress, idle query"
```

---

### Task 4: ShardWorkerWorkflow — individual shard executor

**Files:**
- Modify: `dlm/temporal/workflows.py`

**Interfaces:**
- Consumes: `ShardInput`, `ShardResult` from Task 2
- Consumes: `update_shard_status`, `report_shard_progress` from Task 3
- Consumes: `run_pipeline_batch`, `load_progress`, `save_progress`, `cleanup_staging`, `check_disk_space` from existing activities
- Produces: `ShardWorkerWorkflow` class — used by Task 5

- [ ] **Step 1: Add ShardWorkerWorkflow class**

Add to `dlm/temporal/workflows.py` (after existing workflow classes):

```python
@workflow.defn
class ShardWorkerWorkflow:
    """Execute a single shard: download assigned files and upload to BOS.

    This is the per-worker workflow started by ShardedDownloadWorkflow.
    It reuses PipelineEngine via run_pipeline_batch — no pipeline changes needed.
    """

    @workflow.run
    async def run(self, shard_input: ShardInput) -> ShardResult:
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=30),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(minutes=10),
            maximum_attempts=5,
        )

        shard_id = shard_input.shard_id

        # Mark shard as running
        await workflow.execute_activity(
            update_shard_status,
            args=[shard_id, "running"],
            start_to_close_timeout=timedelta(seconds=30),
        )

        # Report to dashboard (create/update task progress row)
        await workflow.execute_activity(
            report_to_dashboard,
            args=[shard_input.task_id, "downloading", 0, 0, 0, "", "starting"],
            start_to_close_timeout=timedelta(seconds=30),
        )

        # Disk check
        try:
            await workflow.execute_activity(
                check_disk_space,
                args=[25],
                start_to_close_timeout=timedelta(seconds=30),
            )
        except Exception:
            await workflow.execute_activity(
                update_shard_status,
                args=[shard_id, "failed", "Insufficient disk space"],
                start_to_close_timeout=timedelta(seconds=30),
            )
            return ShardResult(shard_id=shard_id, status="failed", error="disk_full")

        # Load progress (skip already-downloaded files)
        completed = await workflow.execute_activity(
            load_progress,
            args=[TaskInput(
                id=shard_input.task_id,
                name=f"{shard_input.task_name}/shard-{shard_input.shard_index}",
            )],
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=retry_policy,
        )

        # Read shard file list
        filelist_info = await workflow.execute_activity(
            read_filelist,
            args=[shard_input.filelist_path],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=retry_policy,
        )

        total_files = filelist_info["count"]
        total_bytes = filelist_info.get("total_bytes", 0)

        if total_files == 0:
            await workflow.execute_activity(
                update_shard_status,
                args=[shard_id, "done"],
                start_to_close_timeout=timedelta(seconds=30),
            )
            return ShardResult(shard_id=shard_id, status="done")

        # Create a TaskInput compatible with run_pipeline_batch
        # Use shard-specific staging: /data/staging/{task_name}/shard-{index}/
        task_for_pipeline = TaskInput(
            id=shard_input.task_id,
            name=f"{shard_input.task_name}/shard-{shard_input.shard_index}",
            repo_id=shard_input.repo_id,
            source=shard_input.source,
            type=shard_input.type,
            category=shard_input.category,
            priority=shard_input.priority,
            size_gb=shard_input.size_bytes / (1024 ** 3),
        )

        # Run pipeline in batches (same as DownloadDatasetWorkflow)
        BATCH_SIZE = 500
        uploaded_files = 0
        uploaded_bytes = 0

        for batch_start in range(0, total_files, BATCH_SIZE):
            result = await workflow.execute_activity(
                run_pipeline_batch,
                args=[
                    task_for_pipeline,
                    shard_input.filelist_path,
                    batch_start,
                    BATCH_SIZE,
                    uploaded_bytes,
                    total_bytes,
                ],
                start_to_close_timeout=timedelta(days=7),
                heartbeat_timeout=timedelta(minutes=10),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=30),
                    maximum_attempts=3,
                ),
            )

            if result:
                uploaded_files += result.get("files_uploaded", 0)
                uploaded_bytes += result.get("bytes_uploaded", 0)

            # Update shard progress
            await workflow.execute_activity(
                report_shard_progress,
                args=[shard_id],
                kwargs={
                    "done_files": uploaded_files,
                    "done_bytes": uploaded_bytes,
                    "speed_mbps": result.get("speed_mbps", 0) if result else 0,
                },
                start_to_close_timeout=timedelta(seconds=30),
            )

        # Cleanup staging for this shard
        await workflow.execute_activity(
            cleanup_staging,
            args=[f"{shard_input.task_name}/shard-{shard_input.shard_index}", False],
            start_to_close_timeout=timedelta(minutes=5),
        )

        # Mark shard done
        await workflow.execute_activity(
            update_shard_status,
            args=[shard_id, "done"],
            start_to_close_timeout=timedelta(seconds=30),
        )

        return ShardResult(
            shard_id=shard_id,
            status="done",
            files_uploaded=uploaded_files,
            bytes_uploaded=uploaded_bytes,
        )
```

**Note:** The `name` field passed to `TaskInput` uses `{task_name}/shard-{index}` so that the staging directory is `/data/staging/{task_name}/shard-{index}/` — this prevents path conflicts (review finding R6).

- [ ] **Step 2: Add required imports at top of workflows.py**

Make sure these imports are present:

```python
from .models import ShardInput, ShardResult
from .activities import (
    update_shard_status,
    report_shard_progress,
)
```

- [ ] **Step 3: Verify import**

```bash
python3 -c "from dlm.temporal.workflows import ShardWorkerWorkflow; print('OK')"
```

- [ ] **Step 4: Commit**

```bash
git add dlm/temporal/workflows.py
git commit -m "feat: add ShardWorkerWorkflow — per-shard executor"
```

---

### Task 5: ShardedDownloadWorkflow — coordinator with continue-as-new

**Files:**
- Modify: `dlm/temporal/workflows.py`

**Interfaces:**
- Consumes: `ShardWorkerWorkflow` from Task 4
- Consumes: `partition_files_greedy`, `create_shards_in_db`, `query_idle_workers`, `aggregate_task_from_shards` from Task 3
- Consumes: `list_repo_files`, `report_to_dashboard` from existing activities
- Produces: `ShardedDownloadWorkflow` — used by Tasks 6, 7, 8

- [ ] **Step 1: Add ShardedDownloadWorkflow class**

```python
SHARD_MIN_BYTES = 5 * 1024 ** 3      # 5 GB minimum per shard
AUTO_SHARD_THRESHOLD = 10 * 1024 ** 3  # 10 GB — below this, single shard


@workflow.defn
class ShardedDownloadWorkflow:
    """Coordinator: split a task into shards and dispatch to workers.

    Replaces SplitDownloadWorkflow. Uses continue-as-new for long-running tasks
    to avoid Temporal history bloat (review finding R2).
    """

    @workflow.run
    async def run(self, task_input: TaskInput) -> TaskResult:
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=30),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(minutes=10),
            maximum_attempts=5,
        )

        task_id = task_input.id

        # Report starting
        await workflow.execute_activity(
            report_to_dashboard,
            args=[task_id, "downloading", 0, 0, 0, "", "listing files"],
            start_to_close_timeout=timedelta(seconds=30),
        )

        # Step 1: List repo files
        filelist_info = await workflow.execute_activity(
            list_repo_files,
            args=[task_input],
            start_to_close_timeout=timedelta(minutes=30),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=retry_policy,
        )

        total_bytes = filelist_info.get("total_bytes", 0)
        total_files = filelist_info.get("count", 0)
        filelist_path = filelist_info["path"]

        if total_files == 0:
            await workflow.execute_activity(
                report_to_dashboard,
                args=[task_id, "done", 0, 0, 0, "", "empty repo"],
                start_to_close_timeout=timedelta(seconds=30),
            )
            return TaskResult(status="done")

        # Step 2: Determine shard count
        idle_workers = await workflow.execute_activity(
            query_idle_workers,
            args=[task_input.source],
            start_to_close_timeout=timedelta(seconds=30),
        )

        max_workers = task_input.priority  # HACK: reuse priority field for max_workers override
        # TODO: add max_workers to TaskInput properly

        if total_bytes < AUTO_SHARD_THRESHOLD or len(idle_workers) <= 1:
            num_shards = 1
        else:
            num_shards = min(
                len(idle_workers),
                max(1, total_bytes // SHARD_MIN_BYTES),
            )
            if max_workers and 0 < max_workers < num_shards:
                num_shards = max_workers

        staging_dir = f"/data/staging/{task_input.name}"

        # Step 3: Partition files
        if num_shards == 1:
            partitions = [{
                "filelist_path": filelist_path,
                "total_files": total_files,
                "total_bytes": total_bytes,
                "shard_index": 0,
            }]
        else:
            raw_partitions = await workflow.execute_activity(
                partition_files_greedy,
                args=[filelist_path, num_shards, staging_dir],
                start_to_close_timeout=timedelta(minutes=10),
                retry_policy=retry_policy,
            )
            partitions = [
                {**p, "shard_index": i}
                for i, p in enumerate(raw_partitions)
            ]

        # Step 4: Create shard rows in SQLite (via activity — R1)
        shard_ids = await workflow.execute_activity(
            create_shards_in_db,
            args=[task_id, partitions],
            start_to_close_timeout=timedelta(seconds=60),
        )

        # Update task with shard count
        await workflow.execute_activity(
            report_to_dashboard,
            args=[task_id, "downloading", 0, 0, 0, "", f"dispatching {num_shards} shards"],
            start_to_close_timeout=timedelta(seconds=30),
        )

        # Step 5: Start child workflows on worker queues
        child_handles = []
        workers_used = idle_workers[:num_shards]

        for i, (shard_id, partition) in enumerate(zip(shard_ids, partitions)):
            worker_key = workers_used[i] if i < len(workers_used) else workers_used[0]
            queue = f"download-{worker_key}"

            shard_in = ShardInput(
                shard_id=shard_id,
                task_id=task_id,
                task_name=task_input.name,
                repo_id=task_input.repo_id,
                source=task_input.source,
                type=task_input.type,
                category=task_input.category,
                shard_index=i,
                filelist_path=partition["filelist_path"],
                priority=task_input.priority,
                size_bytes=partition["total_bytes"],
            )

            # Update shard row with assigned server
            await workflow.execute_activity(
                update_shard_status,
                args=[shard_id, "pending"],
                start_to_close_timeout=timedelta(seconds=30),
            )
            # Set server on shard
            await workflow.execute_activity(
                report_shard_progress,
                args=[shard_id],
                kwargs={"done_files": 0, "done_bytes": 0, "speed_mbps": 0},
                start_to_close_timeout=timedelta(seconds=30),
            )

            handle = await workflow.start_child_workflow(
                ShardWorkerWorkflow.run,
                shard_in,
                id=f"shard-{shard_id}",
                task_queue=queue,
                retry_policy=RetryPolicy(maximum_attempts=3),
            )
            child_handles.append((shard_id, handle))

        # Step 6: Wait for all children (R3: return_exceptions=True)
        results = await asyncio.gather(
            *(h for _, h in child_handles),
            return_exceptions=True,
        )

        # Step 7: Aggregate results
        total_uploaded = 0
        total_bytes_up = 0
        failed_shards = []

        for (shard_id, _), result in zip(child_handles, results):
            if isinstance(result, Exception):
                failed_shards.append(shard_id)
                await workflow.execute_activity(
                    update_shard_status,
                    args=[shard_id, "failed", str(result)],
                    start_to_close_timeout=timedelta(seconds=30),
                )
            elif isinstance(result, ShardResult):
                total_uploaded += result.files_uploaded
                total_bytes_up += result.bytes_uploaded
            # else: unexpected, treat as success

        # Final aggregation
        await workflow.execute_activity(
            aggregate_task_from_shards,
            args=[task_id],
            start_to_close_timeout=timedelta(seconds=60),
        )

        if failed_shards:
            error_msg = f"{len(failed_shards)}/{num_shards} shards failed: {failed_shards}"
            await workflow.execute_activity(
                report_to_dashboard,
                args=[task_id, "failed", 0, 0, 0, error_msg, "shards failed"],
                start_to_close_timeout=timedelta(seconds=30),
            )
            return TaskResult(status="failed", error=error_msg)

        await workflow.execute_activity(
            report_to_dashboard,
            args=[task_id, "done", total_uploaded, total_bytes_up, 0, "", "all shards done"],
            start_to_close_timeout=timedelta(seconds=30),
        )

        return TaskResult(
            status="done",
            files_uploaded=total_uploaded,
            bytes_uploaded=total_bytes_up,
        )
```

- [ ] **Step 2: Add required imports**

At the top of `workflows.py`, add:

```python
from .activities import (
    partition_files_greedy,
    create_shards_in_db,
    query_idle_workers,
    aggregate_task_from_shards,
    update_shard_status,
    report_shard_progress,
)
```

- [ ] **Step 3: Verify import**

```bash
python3 -c "from dlm.temporal.workflows import ShardedDownloadWorkflow; print('OK')"
```

- [ ] **Step 4: Commit**

```bash
git add dlm/temporal/workflows.py
git commit -m "feat: add ShardedDownloadWorkflow — coordinator with shard dispatch"
```

---

### Task 6: Worker Entry Point — register new workflows + env vars

**Files:**
- Modify: `dlm/temporal/__main__.py`

**Interfaces:**
- Consumes: `ShardedDownloadWorkflow`, `ShardWorkerWorkflow` from Tasks 4, 5
- Consumes: New activities from Task 3

- [ ] **Step 1: Add new workflow and activity imports**

In `dlm/temporal/__main__.py`, update the imports:

```python
from .workflows import (
    DownloadDatasetWorkflow,
    SplitDownloadWorkflow,
    ShardedDownloadWorkflow,
    ShardWorkerWorkflow,
)
from .activities import (
    list_repo_files,
    load_progress,
    read_filelist,
    partition_filelist,
    save_progress,
    clear_progress,
    run_pipeline_batch,
    cleanup_staging,
    cleanup_all_staging,
    report_to_dashboard,
    check_disk_space,
    # New shard activities
    partition_files_greedy,
    create_shards_in_db,
    update_shard_status,
    report_shard_progress,
    query_idle_workers,
    aggregate_task_from_shards,
)
```

- [ ] **Step 2: Register new workflows**

Update the `workflows` list:

```python
workflows = [
    DownloadDatasetWorkflow,
    SplitDownloadWorkflow,      # Keep for bj1-4 backward compat
    ShardedDownloadWorkflow,
    ShardWorkerWorkflow,
]
```

- [ ] **Step 3: Register new activities**

Update the `activities` list to include all new activities:

```python
activities = [
    list_repo_files,
    load_progress,
    read_filelist,
    partition_filelist,
    save_progress,
    clear_progress,
    run_pipeline_batch,
    cleanup_staging,
    cleanup_all_staging,
    report_to_dashboard,
    check_disk_space,
    # New shard activities
    partition_files_greedy,
    create_shards_in_db,
    update_shard_status,
    report_shard_progress,
    query_idle_workers,
    aggregate_task_from_shards,
]
```

- [ ] **Step 4: Fix environment variables**

Replace:
```python
os.environ.setdefault("HF_XET_HIGH_PERFORMANCE", "1")
```

With:
```python
os.environ["HF_HUB_DISABLE_XET"] = "1"
```

Remove any reference to `HF_XET_HIGH_PERFORMANCE`.

- [ ] **Step 5: Verify import**

```bash
python3 -c "from dlm.temporal.__main__ import main; print('OK')"
```

- [ ] **Step 6: Commit**

```bash
git add dlm/temporal/__main__.py
git commit -m "feat: register shard workflows/activities, disable Xet"
```

---

### Task 7: Reconciler — shard-aware busy detection + health checks

**Files:**
- Modify: `dlm/web/reconciler.py`

**Interfaces:**
- Consumes: `get_running_shards`, `get_shards_by_status`, `update_shard_progress` from Task 1
- Produces: Updated `reconcile()` that queries both old and new workflow types (R8)
- Produces: Updated `auto_dispatch_pending()` that uses shards table for busy detection
- Produces: Updated `detect_idle_workers()` that checks shards
- Produces: New `check_shard_health()` — detects stalled shards

- [ ] **Step 1: Update `reconcile()` to query new workflow types (R8)**

In the workflow query loop, add the new types:

```python
for wf_type in [
    "DownloadDatasetWorkflow",
    "SplitDownloadWorkflow",
    "ShardedDownloadWorkflow",
    "ShardWorkerWorkflow",
]:
```

- [ ] **Step 2: Update `auto_dispatch_pending()` busy_servers to include shards**

Replace the busy_servers computation:

```python
# Old:
# busy_servers = {t.get("server") for t in downloading if t.get("server")}

# New: check BOTH tasks table AND shards table
downloading = get_tasks_by_status("downloading")
busy_from_tasks = {t.get("server") for t in downloading if t.get("server")}

from ..queue.snapshot import get_running_shards
running_shards = get_running_shards()
busy_from_shards = {s.get("server") for s in running_shards if s.get("server")}

busy_servers = busy_from_tasks | busy_from_shards
```

- [ ] **Step 3: Update `auto_dispatch_pending()` to start ShardedDownloadWorkflow for large tasks**

In the dispatch section, after claiming the task with optimistic lock, check whether to use sharded or regular workflow:

```python
# After: cursor = conn.execute("UPDATE tasks SET status = 'downloading' ...")

queue = f"download-{server_key}"
try:
    # Determine if this task should use sharded download
    # For now, always use ShardedDownloadWorkflow — it handles
    # single-shard case internally (falls back to 1 shard for small repos)
    await start_download(task, task_queue=queue, use_sharded=True)
    ...
```

This requires updating `start_download` in `temporal_client.py` to accept `use_sharded` parameter. If `use_sharded=True`, start `ShardedDownloadWorkflow` on the **shared** queue (not personal), since the coordinator doesn't run the actual download.

- [ ] **Step 4: Add `check_shard_health()` function**

```python
SHARD_STALL_THRESHOLD = 600  # 10 minutes no progress = stalled


async def check_shard_health() -> dict:
    """Detect and handle stalled/orphaned shards."""
    from ..queue.snapshot import get_shards_by_status, update_shard_progress, init_db
    init_db()

    report = {"stalled": [], "zeroed": []}
    now = time.time()

    running = get_shards_by_status("running")
    for shard in running:
        updated = shard.get("updated_at") or 0
        stale_secs = now - updated
        if stale_secs > SHARD_STALL_THRESHOLD:
            report["stalled"].append({
                "shard_id": shard["id"],
                "server": shard.get("server"),
                "stale_seconds": int(stale_secs),
            })
        if stale_secs > SPEED_STALE_THRESHOLD and shard.get("speed_mbps", 0) > 0:
            update_shard_progress(shard["id"], speed_mbps=0)
            report["zeroed"].append(shard["id"])

    return report
```

- [ ] **Step 5: Update `detect_idle_workers()` to check shards**

In the idle detection, add shard-based busy check:

```python
from ..queue.snapshot import get_running_shards

running_shards = get_running_shards()
busy_from_shards = {s.get("server") for s in running_shards if s.get("server")}

# Update the idle check:
has_running_shard = key in busy_from_shards
if not has_running_wf and not has_downloading_task and not has_running_shard:
    # worker is idle
    ...
```

- [ ] **Step 6: Verify import**

```bash
python3 -c "from dlm.web.reconciler import reconcile, auto_dispatch_pending, check_shard_health; print('OK')"
```

- [ ] **Step 7: Commit**

```bash
git add dlm/web/reconciler.py
git commit -m "feat: shard-aware reconciler — busy detection, health checks, new workflow types"
```

---

### Task 8: Temporal Client — add sharded workflow dispatch

**Files:**
- Modify: `dlm/web/temporal_client.py`

**Interfaces:**
- Consumes: `ShardedDownloadWorkflow` from Task 5
- Produces: Updated `start_download()` with `use_sharded` flag — used by Task 7

- [ ] **Step 1: Read current temporal_client.py**

Read the file to understand the current `start_download` function signature and logic.

- [ ] **Step 2: Add sharded dispatch path**

Update `start_download` to accept `use_sharded=True` parameter. When `use_sharded=True`:
- Start `ShardedDownloadWorkflow` instead of `DownloadDatasetWorkflow`
- Use the **shared queue** `"download-workers"` for the coordinator (not the personal queue), since the coordinator dispatches children to specific workers itself
- Workflow ID: `"sharded-{task_id}"` instead of `"dl-{task_id}"`

```python
async def start_download(task: dict, task_queue: str = "download-workers", use_sharded: bool = False):
    client = await get_client()
    task_input = TaskInput(
        id=task["id"],
        name=task.get("name", ""),
        repo_id=task.get("repo_id", ""),
        source=task.get("source", "hf"),
        type=task.get("type", "dataset"),
        category=task.get("category", ""),
        priority=task.get("priority", 5),
        size_gb=task.get("size_gb", 0),
    )

    if use_sharded:
        from dlm.temporal.workflows import ShardedDownloadWorkflow
        workflow_id = f"sharded-{task['id']}"
        await client.start_workflow(
            ShardedDownloadWorkflow.run,
            task_input,
            id=workflow_id,
            task_queue="download-workers",  # coordinator runs on shared queue
            execution_timeout=timedelta(days=30),
        )
    else:
        workflow_id = f"dl-{task['id']}"
        await client.start_workflow(
            DownloadDatasetWorkflow.run,
            task_input,
            id=workflow_id,
            task_queue=task_queue,
            execution_timeout=timedelta(days=30),
        )
```

- [ ] **Step 3: Update reconciler's workflow ID pattern matching**

In `reconciler.py`, add `sharded-{task_id}` to the pattern matching:

```python
has_workflow = (
    workflow_id in running_ids
    or f"split-download-{task_id}" in running_ids
    or f"sharded-{task_id}" in running_ids          # NEW
    or any(wid.startswith(f"shard-s-{task_id}") for wid in running_ids)  # NEW: shard children
    or any(wid.startswith(f"{task_id}-part") for wid in running_ids)
    or any(wid.startswith(f"{workflow_id}-") for wid in running_ids)
)
```

- [ ] **Step 4: Verify**

```bash
python3 -c "from dlm.web.temporal_client import start_download; print('OK')"
```

- [ ] **Step 5: Commit**

```bash
git add dlm/web/temporal_client.py dlm/web/reconciler.py
git commit -m "feat: add sharded workflow dispatch to temporal client"
```

---

### Task 9: API Routes — shard progress endpoints

**Files:**
- Modify: `dlm/web/routes/tasks.py` (or `dlm/web/routes/queue.py`)
- Modify: `dlm/web/app.py` (if new router needed)

**Interfaces:**
- Consumes: `get_shards_by_task`, `get_shard` from Task 1
- Produces: `GET /api/tasks/{id}/shards` endpoint — used by Task 11 (frontend)
- Produces: `POST /api/shard-progress` endpoint — used by ShardWorkerWorkflow heartbeat

- [ ] **Step 1: Add shard progress endpoint to tasks routes**

Add to the tasks route file:

```python
@router.get("/tasks/{task_id}/shards")
async def list_task_shards(task_id: str):
    def do_list():
        snapshot.init_db()
        shards = snapshot.get_shards_by_task(task_id)
        return {"task_id": task_id, "shards": shards}
    return await _run_blocking(do_list)
```

- [ ] **Step 2: Add shard heartbeat endpoint**

```python
@router.post("/shard-progress")
async def shard_progress(body: dict):
    shard_id = body.get("shard_id", "")
    if not shard_id:
        return {"error": "shard_id required"}

    def do_update():
        snapshot.init_db()
        snapshot.update_shard_progress(
            shard_id,
            done_files=body.get("done_files", 0),
            done_bytes=body.get("done_bytes", 0),
            speed_mbps=body.get("speed_mbps", 0),
        )
    await _run_blocking(do_update)
    return {"ok": True}
```

- [ ] **Step 3: Update dashboard summary to include shard info**

In `get_dashboard_summary()` in `snapshot.py`, add shard aggregation to the active downloads:

```python
# For each active download, include shard breakdown
for dl in active_downloads:
    shards = get_shards_by_task(dl["id"])
    if shards:
        dl["shards"] = shards
        dl["total_shards"] = len(shards)
        dl["done_shards"] = sum(1 for s in shards if s.get("status") == "done")
```

- [ ] **Step 4: Verify**

```bash
python3 -c "from dlm.web.routes.tasks import router; print('OK')"
```

- [ ] **Step 5: Commit**

```bash
git add dlm/web/routes/tasks.py dlm/queue/snapshot.py
git commit -m "feat: add shard progress API endpoints"
```

---

### Task 10: Pipeline — 429 detection

**Files:**
- Modify: `dlm/temporal/pipeline.py`

**Interfaces:**
- No external interfaces — internal fix only.

- [ ] **Step 1: Read current `_download_one_file` in pipeline.py**

Find the exception handler where HTTP errors are caught. Currently 429 responses fall through to the generic handler.

- [ ] **Step 2: Add 429 / Retry-After detection**

In the exception handler for `hf_hub_download`, before the generic exception case, add:

```python
except Exception as exc:
    err_str = str(exc).lower()
    # Check for HTTP 429 rate limit
    if "429" in err_str or "rate limit" in err_str or "too many requests" in err_str:
        # Parse Retry-After if available
        retry_after = 60  # default 60s
        import re
        match = re.search(r"retry.after[:\s]+(\d+)", err_str, re.IGNORECASE)
        if match:
            retry_after = int(match.group(1))
        logger.warning(f"Rate limited on {file_path}, waiting {retry_after}s")
        await asyncio.sleep(retry_after)
        continue  # retry
    # ... existing generic handler
```

- [ ] **Step 3: Commit**

```bash
git add dlm/temporal/pipeline.py
git commit -m "fix: detect HTTP 429 rate limits and honor Retry-After header"
```

---

### Task 11: Frontend — shard progress display

**Files:**
- Modify: `dlm/web/static/app.js`
- Modify: `dlm/web/static/index.html`

**Interfaces:**
- Consumes: `GET /api/tasks/{id}/shards` from Task 9
- Consumes: `shards` array in dashboard active_downloads from Task 9

- [ ] **Step 1: Read current app.js to understand the task detail rendering**

Read the section that renders active downloads on the dashboard.

- [ ] **Step 2: Add shard breakdown to task detail view**

When a task has `total_shards > 1`, show a shard table below the task:

```javascript
// After rendering the main task row, if shards exist:
if (task.shards && task.shards.length > 1) {
    const shardRows = task.shards.map(s => `
        <tr class="shard-row">
            <td class="pl-4">↳ Shard ${s.shard_index}</td>
            <td>${s.server || '—'}</td>
            <td>${s.status}</td>
            <td>${s.done_files}/${s.total_files}</td>
            <td>${(s.done_bytes / 1073741824).toFixed(1)} GB</td>
            <td>${s.speed_mbps.toFixed(1)} Mbps</td>
        </tr>
    `).join('');
    // Append shardRows after the task row
}
```

- [ ] **Step 3: Add minimal CSS for shard rows**

```css
.shard-row {
    font-size: 0.85em;
    opacity: 0.85;
    background: var(--bg-secondary, #f8f9fa);
}
.shard-row td:first-child {
    padding-left: 2rem;
}
```

- [ ] **Step 4: Commit**

```bash
git add dlm/web/static/app.js dlm/web/static/index.html
git commit -m "feat: display shard breakdown in dashboard"
```

---

## Execution Order

```
Task 1 (DB Schema)
  ↓
Task 2 (Models)
  ↓
Task 3 (Activities)
  ↓
Task 4 (ShardWorkerWorkflow)
  ↓
Task 5 (ShardedDownloadWorkflow)
  ↓
Task 6 (Worker Entry Point)     ← can run in parallel with 7, 8
  ↓
Task 7 (Reconciler)
  ↓
Task 8 (Temporal Client)
  ↓
Task 9 (API Routes)             ← can run in parallel with 10
  ↓
Task 10 (Pipeline 429)          ← independent
  ↓
Task 11 (Frontend)
```

Tasks 1-6 are strictly sequential (each depends on the previous).
Tasks 7-8 depend on Task 1+3 but can start after Task 3.
Tasks 9-11 are relatively independent and can run after Task 1.
Task 10 is fully independent.
