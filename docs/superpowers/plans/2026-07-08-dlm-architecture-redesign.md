# DLM Architecture Redesign — High-Throughput Parallel Download Platform

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the fundamental architecture so 7 workers can saturate their network bandwidth (~16 MB/s each to HF = 112 MB/s aggregate = ~9 TB/day), with automatic task dispatch, progress persistence, and no manual intervention.

**Architecture:** Replace the broken Celery-based dispatch with a pull-based coordinator: workers poll a central scheduler for work, the scheduler assigns tasks based on disk space and affinity. Downloads use `aria2c` for multi-connection parallelism on large files. Progress checkpoints to local disk (fast) with BOS backup (durable).

**Tech Stack:** Python 3.10, FastAPI (coordinator), SQLite (state), Redis (signals only), aria2c (multi-connection downloads), BOS SDK (uploads), huggingface_hub (file listing/auth)

## Global Constraints

- Python 3.10 (installed on all workers)
- No new system dependencies beyond `aria2c` (apt install)
- Must handle 9+ TB datasets with <200GB worker disk
- Workers are stateless — can die and recover without data loss
- BOS upload speed is fine (43 MB/s) — don't touch upload path
- All workers share one Redis on S1 (154.85.43.52)
- Backward compatible: existing task DB (SQLite) must be preserved

---

## Root Cause Analysis

### Why only 100GB/day across 7 workers?

| Problem | Impact | Root Cause |
|---------|--------|------------|
| **No auto-dispatch** | Idle workers sit forever | Celery requires explicit `apply_async()` — no loop checks for idle workers |
| **Task shuffling** | Progress lost on every move | Celery round-robins messages to any available worker; no affinity |
| **Disk full → stall** | Workers download into full disk, fail, retry from zero | No pre-flight disk check before dispatch |
| **Ghost detection kills running tasks** | Tasks marked as "ghost" and reset while still running | Celery task_id vs DB task_id mismatch |
| **Single-connection download** | 16 MB/s per file even when bandwidth allows more | `hf download` uses one HTTP connection per file; XET protocol also single-stream |
| **Stale staging accumulates** | Disk fills with orphaned data from shuffled tasks | No cleanup when task moves to different worker |
| **Celery revoke memory** | Re-dispatched tasks silently discarded | Celery remembers revoked IDs globally |

### Why Celery is wrong for this

Celery is designed for **short-lived tasks** (ms to minutes). Using it for **multi-day downloads** creates:
1. No task affinity → messages go to random workers
2. No disk-awareness → can't route to worker with space
3. Task ID management nightmare → revoke/re-dispatch cycles
4. No built-in progress persistence → worker death = lost state
5. Broker memory bloat → thousands of stale messages in Redis
6. Ghost detection complexity → active task != Celery active

---

## Proposed Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   S1 (Coordinator)                        │
│                                                          │
│  ┌──────────┐  ┌──────────┐  ┌───────────────────────┐ │
│  │ FastAPI  │  │ Scheduler│  │ SQLite DB             │ │
│  │ Web UI   │  │ (30s loop)│ │ (tasks, workers,     │ │
│  └──────────┘  └──────────┘  │  progress)            │ │
│                               └───────────────────────┘ │
│  Scheduler loop:                                         │
│    1. Workers heartbeat every 15s (POST /worker/heartbeat)
│    2. Scheduler matches pending tasks → idle workers     │
│    3. Worker pulls assignment (GET /worker/next-task)    │
│    4. Worker reports progress (POST /worker/progress)    │
│    5. Worker reports done/failed (POST /worker/complete) │
└─────────────────────────────────────────────────────────┘
         ▲ HTTP (simple, debuggable, no broker)
         │
    ┌────┴────┬────────┬────────┬────────┬────────┬────────┐
    │   w1    │   w2   │   w3   │   w4   │   w5   │  w6/w7 │
    │         │        │        │        │        │        │
    │ Worker Agent:                                         │
    │  - Heartbeat (15s): disk_free, speed, current_task   │
    │  - Pull task from coordinator                        │
    │  - Download (aria2c multi-connection OR hf download)  │
    │  - Upload to BOS (existing SDK, works well)          │
    │  - Checkpoint progress after each batch              │
    └──────────────────────────────────────────────────────┘
```

### Key Design Decisions

1. **Pull model, not push** — Workers ask "what should I do?" instead of getting messages pushed. Eliminates ghost tasks, stale messages, task shuffling.

2. **Coordinator assigns with disk awareness** — Only assigns task to worker with enough free space. Large datasets go to workers with most space.

3. **Worker affinity** — Once a task starts on a worker, it STAYS there. Coordinator won't reassign unless worker is dead (no heartbeat for 5 min).

4. **aria2c for large files** — Multi-connection download (16 connections per file) can saturate bandwidth. Falls back to `hf download` for repos with many small files.

5. **Local progress file** — Each worker writes `{staging}/.progress.json` after every batch. On restart, reads it and skips completed files. No BOS dependency for progress.

6. **Staging cleanup on assign** — When coordinator assigns a task, worker first cleans ALL staging dirs except the assigned task.

---

## Reference Projects

| Project | What to learn |
|---------|--------------|
| **aria2** (aria2.github.io) | Multi-connection HTTP/FTP download. `-x 16 -s 16` splits a file into 16 segments downloaded in parallel. Can 4-5x single-connection speed. |
| **rclone** (rclone.org) | Mature parallel file transfer. Good model for chunked upload with progress tracking. |
| **Dask Distributed** (distributed.dask.org) | Worker-scheduler protocol. Workers heartbeat to scheduler, scheduler assigns tasks based on resource availability. |
| **Buildkite Agent** (buildkite.com/docs/agent) | Pull-based CI agent model. Agents poll for work, report status. Simple and reliable. |
| **HuggingFace hub** `hf_xet` | XET protocol already does block-level parallelism internally. Can't be sped up externally. |

### aria2c speed improvement estimate

Current: 16 MB/s (single connection to HF CDN)
With aria2c `-x 16`: Expect 50-80 MB/s per worker (CDN allows parallel connections)
Cluster total: 7 × 60 MB/s = 420 MB/s = **~35 TB/day** (vs current 100 GB/day)

Note: Only works for regular HF downloads. XET-protocol repos (like Sekai) won't benefit — they need the `hf_xet` library which does its own parallelism.

---

## Implementation Plan

### Task 1: Worker Agent (Pull-Based)

Replace Celery worker with a simple HTTP-polling agent.

**Files:**
- Create: `dlm/agent/worker.py` — Main worker agent loop
- Create: `dlm/agent/downloader.py` — Download orchestrator (aria2c + hf fallback)
- Create: `dlm/agent/checkpoint.py` — Local progress persistence
- Modify: `dlm/worker/movers/bos_sdk.py` — Keep as-is (upload works)
- Test: manual integration test on one worker

**Interfaces:**
- Consumes: Coordinator HTTP API (Task 2)
- Produces: Heartbeat data, progress reports, completion signals

- [ ] **Step 1: Create worker agent skeleton**

```python
# dlm/agent/worker.py
"""Pull-based worker agent. Replaces Celery worker."""

import logging
import os
import time
import socket
import requests
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)

COORDINATOR_URL = os.environ.get("DLM_COORDINATOR", "http://154.85.43.52:8080")
HEARTBEAT_INTERVAL = 15
STAGING_PATH = Path("/data/staging")


class WorkerAgent:
    def __init__(self, server_key: str):
        self.server_key = server_key
        self.hostname = socket.gethostname()
        self.current_task = None
        self.running = True

    def run(self):
        logger.info(f"Worker agent starting: {self.server_key}")
        self._cleanup_stale_staging()

        while self.running:
            try:
                self._heartbeat()

                if self.current_task is None:
                    task = self._pull_task()
                    if task:
                        self._execute_task(task)
                    else:
                        time.sleep(HEARTBEAT_INTERVAL)
                else:
                    time.sleep(HEARTBEAT_INTERVAL)
            except KeyboardInterrupt:
                self.running = False
            except Exception as e:
                logger.error(f"Agent error: {e}", exc_info=True)
                time.sleep(30)

    def _heartbeat(self):
        """Report status to coordinator."""
        disk_free = shutil.disk_usage(STAGING_PATH).free / (1024**3)
        data = {
            "server_key": self.server_key,
            "hostname": self.hostname,
            "disk_free_gb": round(disk_free, 1),
            "current_task_id": self.current_task["id"] if self.current_task else None,
            "status": "busy" if self.current_task else "idle",
        }
        try:
            requests.post(f"{COORDINATOR_URL}/api/agent/heartbeat", json=data, timeout=5)
        except Exception as e:
            logger.debug(f"Heartbeat failed: {e}")

    def _pull_task(self):
        """Ask coordinator for next task."""
        disk_free = shutil.disk_usage(STAGING_PATH).free / (1024**3)
        try:
            resp = requests.get(
                f"{COORDINATOR_URL}/api/agent/next-task",
                params={"server_key": self.server_key, "disk_free_gb": disk_free},
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                if data.get("task"):
                    return data["task"]
            return None
        except Exception as e:
            logger.debug(f"Pull failed: {e}")
            return None

    def _execute_task(self, task: dict):
        """Download + upload a task."""
        from .downloader import Downloader
        from .checkpoint import Checkpoint

        self.current_task = task
        task_name = task["name"]
        staging_dir = STAGING_PATH / task_name

        # Clean other staging dirs to maximize space
        self._cleanup_except(task_name)
        staging_dir.mkdir(parents=True, exist_ok=True)

        checkpoint = Checkpoint(staging_dir)
        downloader = Downloader(task, staging_dir, checkpoint)

        try:
            self._report_progress(task["id"], status="downloading", phase="starting")
            downloader.run(progress_callback=self._make_progress_cb(task["id"]))
            self._report_complete(task["id"], status="done")
        except Exception as e:
            logger.error(f"Task {task_name} failed: {e}")
            self._report_complete(task["id"], status="failed", error=str(e))
        finally:
            self.current_task = None

    def _report_progress(self, task_id, **kwargs):
        try:
            requests.post(
                f"{COORDINATOR_URL}/api/agent/progress",
                json={"task_id": task_id, "server_key": self.server_key, **kwargs},
                timeout=5,
            )
        except Exception:
            pass

    def _report_complete(self, task_id, **kwargs):
        try:
            requests.post(
                f"{COORDINATOR_URL}/api/agent/complete",
                json={"task_id": task_id, "server_key": self.server_key, **kwargs},
                timeout=10,
            )
        except Exception:
            pass

    def _make_progress_cb(self, task_id):
        last_report = [0]
        def cb(downloaded_bytes, total_bytes, speed_bps):
            now = time.time()
            if now - last_report[0] < 15:
                return
            last_report[0] = now
            pct = (downloaded_bytes / total_bytes * 100) if total_bytes > 0 else 0
            self._report_progress(
                task_id, status="downloading", phase="downloading",
                progress_pct=round(pct, 1),
                speed_mbps=round(speed_bps / 1024 / 1024, 1),
                downloaded_gb=round(downloaded_bytes / 1024**3, 2),
            )
        return cb

    def _cleanup_stale_staging(self):
        if not STAGING_PATH.exists():
            STAGING_PATH.mkdir(parents=True, exist_ok=True)
            return
        for d in STAGING_PATH.iterdir():
            if d.is_dir():
                shutil.rmtree(d, ignore_errors=True)
                logger.info(f"Cleaned stale staging: {d.name}")

    def _cleanup_except(self, keep_name: str):
        for d in STAGING_PATH.iterdir():
            if d.is_dir() and d.name != keep_name:
                shutil.rmtree(d, ignore_errors=True)
                logger.info(f"Cleaned staging: {d.name}")
```

- [ ] **Step 2: Create download orchestrator with aria2c**

```python
# dlm/agent/downloader.py
"""Download orchestrator — uses aria2c for large files, hf download for many small files."""

import json
import logging
import os
import subprocess
import time
from pathlib import Path
from typing import Callable, Optional

logger = logging.getLogger(__name__)

ARIA2C_CONNECTIONS = 16  # parallel connections per file
SMALL_FILE_BATCH_THRESHOLD = 100  # if >100 files in a batch, use hf download


class Downloader:
    def __init__(self, task: dict, staging_dir: Path, checkpoint):
        self.task = task
        self.staging_dir = staging_dir
        self.checkpoint = checkpoint
        self.repo_id = task["repo_id"]
        self.repo_type = task.get("type", "dataset")

    def run(self, progress_callback: Optional[Callable] = None):
        """Main download + upload loop."""
        from ..worker.movers.bos_sdk import BOSSDKMover
        from ..worker.disk import DiskManager
        from threading import Event

        files = self._list_repo_files()
        if not files:
            raise RuntimeError(f"Could not list files for {self.repo_id}")

        total_bytes = sum(f["size"] for f in files)

        # Resume: skip already-uploaded files
        completed = self.checkpoint.load()
        remaining = [f for f in files if f["path"] not in completed]

        if completed:
            skipped_bytes = sum(f["size"] for f in files if f["path"] in completed)
            logger.info(f"Resume: skipping {len(completed)} files ({skipped_bytes/1024**3:.1f}GB)")
            uploaded_bytes = skipped_bytes
        else:
            uploaded_bytes = 0

        # Sort largest first for better disk utilization
        remaining.sort(key=lambda f: f["size"], reverse=True)

        disk = DiskManager()
        mover = BOSSDKMover()
        cancel = Event()
        batch_num = 0

        while remaining:
            batch_num += 1
            avail_gb = disk.available_gb()
            max_batch_bytes = int(avail_gb * 0.6 * 1024**3)

            # Build batch
            batch = []
            batch_size = 0
            leftover = []
            for f in remaining:
                if batch_size + f["size"] <= max_batch_bytes or not batch:
                    batch.append(f)
                    batch_size += f["size"]
                else:
                    leftover.append(f)
            remaining = leftover

            batch_paths = [f["path"] for f in batch]
            logger.info(f"Batch {batch_num}: {len(batch)} files, {batch_size/1024**3:.1f}GB")

            # Download batch — choose strategy
            if len(batch) <= 3 and all(f["size"] > 100*1024*1024 for f in batch):
                # Large files: use aria2c for multi-connection speed
                self._download_aria2c(batch, self.staging_dir)
            else:
                # Many files: use hf download for parallelism across files
                self._download_hf(batch_paths, self.staging_dir)

            # Upload to BOS
            mover.move(self.staging_dir, self._make_task_obj(),
                      progress_callback=lambda u, t: None, cancel_event=cancel)

            # Checkpoint
            completed.update(batch_paths)
            self.checkpoint.save(completed)

            uploaded_bytes += batch_size
            if progress_callback and total_bytes > 0:
                progress_callback(uploaded_bytes, total_bytes, 0)

            logger.info(f"Batch {batch_num} done, {uploaded_bytes/1024**3:.1f}GB total")

        # Done
        self.checkpoint.clear()

    def _download_aria2c(self, files: list, staging_dir: Path):
        """Download files using aria2c with multi-connection."""
        token = os.environ.get("HF_TOKEN", "")

        for f in files:
            url = self._resolve_download_url(f["path"])
            if not url:
                # Fallback to hf download for this file
                self._download_hf([f["path"]], staging_dir)
                continue

            output_path = staging_dir / f["path"]
            output_path.parent.mkdir(parents=True, exist_ok=True)

            cmd = [
                "aria2c",
                "--max-connection-per-server", str(ARIA2C_CONNECTIONS),
                "--split", str(ARIA2C_CONNECTIONS),
                "--min-split-size", "20M",
                "--dir", str(output_path.parent),
                "--out", output_path.name,
                "--continue=true",  # resume partial downloads
                "--auto-file-renaming=false",
                "--allow-overwrite=true",
            ]
            if token:
                cmd.extend(["--header", f"Authorization: Bearer {token}"])
            cmd.append(url)

            logger.info(f"aria2c: {f['path']} ({f['size']/1024**3:.1f}GB, {ARIA2C_CONNECTIONS} connections)")

            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=7200)
            if proc.returncode != 0:
                raise RuntimeError(f"aria2c failed for {f['path']}: {proc.stderr[:200]}")

    def _download_hf(self, file_paths: list, staging_dir: Path):
        """Download using hf CLI (good for many small files)."""
        MAX_ARGS = 500
        if len(file_paths) > MAX_ARGS:
            for i in range(0, len(file_paths), MAX_ARGS):
                self._download_hf(file_paths[i:i+MAX_ARGS], staging_dir)
            return

        cmd = [
            "hf", "download", self.repo_id,
            "--local-dir", str(staging_dir),
            "--repo-type", self.repo_type,
            "--max-workers", "32",
        ]
        cmd.extend(file_paths)

        env = os.environ.copy()
        env["HF_HUB_ENABLE_HF_TRANSFER"] = "1"
        if os.environ.get("HF_TOKEN"):
            env["HF_TOKEN"] = os.environ["HF_TOKEN"]

        proc = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=7200)
        if proc.returncode != 0:
            raise RuntimeError(f"hf download failed: {proc.stderr[:300]}")

    def _resolve_download_url(self, file_path: str) -> Optional[str]:
        """Get direct download URL for a file (for aria2c)."""
        try:
            from huggingface_hub import hf_hub_url, get_token
            url = hf_hub_url(self.repo_id, file_path, repo_type=self.repo_type)
            # Check if it's XET (aria2c won't work with XET)
            import requests
            token = os.environ.get("HF_TOKEN", "")
            headers = {"Authorization": f"Bearer {token}"} if token else {}
            resp = requests.head(url, headers=headers, allow_redirects=False, timeout=10)
            if resp.status_code in (301, 302):
                location = resp.headers.get("location", "")
                if "xethub" in location or "xet-bridge" in location:
                    return None  # XET protocol — can't use aria2c
                return location
            return url
        except Exception:
            return None

    def _list_repo_files(self) -> list:
        """List all files in the HF repo."""
        try:
            from huggingface_hub import HfApi
            api = HfApi(token=os.environ.get("HF_TOKEN"))
            repo_type = "dataset" if self.repo_type == "dataset" else "model"
            files = []
            for item in api.list_repo_tree(self.repo_id, repo_type=repo_type, recursive=True):
                if hasattr(item, "size") and item.size and hasattr(item, "rfilename"):
                    files.append({"path": item.rfilename, "size": item.size})
            return files
        except Exception as e:
            logger.error(f"Failed to list repo: {e}")
            return []

    def _make_task_obj(self):
        """Create a minimal task object for the mover."""
        class TaskObj:
            pass
        t = TaskObj()
        t.name = self.task["name"]
        t.type = self.task.get("type", "dataset")
        t.category = self.task.get("category", "")
        return t
```

- [ ] **Step 3: Create checkpoint module**

```python
# dlm/agent/checkpoint.py
"""Local progress checkpoint — survives worker restarts."""

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class Checkpoint:
    def __init__(self, staging_dir: Path):
        self.path = staging_dir / ".progress.json"

    def load(self) -> set:
        try:
            if self.path.exists():
                data = json.loads(self.path.read_text())
                return set(data) if isinstance(data, list) else set()
        except Exception:
            pass
        return set()

    def save(self, completed: set):
        try:
            self.path.write_text(json.dumps(sorted(completed)))
        except Exception as e:
            logger.warning(f"Failed to save checkpoint: {e}")

    def clear(self):
        try:
            self.path.unlink(missing_ok=True)
        except Exception:
            pass
```

- [ ] **Step 4: Create agent __init__ and entry point**

```python
# dlm/agent/__init__.py
"""Pull-based worker agent."""
```

```python
# dlm/agent/__main__.py
"""Entry point: python -m dlm.agent --server-key w1"""
import argparse
import logging

from .worker import WorkerAgent

def main():
    parser = argparse.ArgumentParser(description="DLM Worker Agent")
    parser.add_argument("--server-key", required=True)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    agent = WorkerAgent(server_key=args.server_key)
    agent.run()

if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Install aria2c on all workers**

```bash
# Run from S1:
for ip in 156.240.120.209 154.85.53.152 154.85.49.95 154.85.40.244 154.85.54.251 154.85.50.210 156.240.121.60; do
  ssh root@$ip "apt-get install -y aria2 2>/dev/null | tail -1"
done
```

- [ ] **Step 6: Commit**

```bash
git add dlm/agent/
git commit -m "feat: add pull-based worker agent with aria2c multi-connection downloads"
```

---

### Task 2: Coordinator API (Auto-Dispatch)

Add coordinator endpoints that workers poll for task assignment.

**Files:**
- Create: `dlm/web/routes/agent.py` — Worker agent API endpoints
- Modify: `dlm/web/app.py` — Register new router
- Modify: `dlm/web/scheduler.py` — Add auto-dispatch logic
- Test: manual curl tests

**Interfaces:**
- Consumes: SQLite task DB
- Produces: HTTP API for worker agents (heartbeat, next-task, progress, complete)

- [ ] **Step 1: Create coordinator agent API**

```python
# dlm/web/routes/agent.py
"""Coordinator API for pull-based worker agents."""

import logging
import time
from concurrent.futures import ThreadPoolExecutor

import asyncio
from fastapi import APIRouter

from ...queue import snapshot

logger = logging.getLogger("dlm.web")
router = APIRouter(prefix="/agent", tags=["agent"])

_executor = ThreadPoolExecutor(max_workers=4)

# Worker affinity: once a task starts on a worker, keep it there
# Maps task_id -> server_key
_task_affinity = {}


def _run_blocking(fn, *args):
    loop = asyncio.get_event_loop()
    return loop.run_in_executor(_executor, fn, *args)


@router.post("/heartbeat")
async def worker_heartbeat(body: dict):
    """Worker reports its status every 15s."""
    def do_heartbeat():
        snapshot.init_db()
        snapshot.update_worker(
            hostname=body.get("hostname", ""),
            server_key=body["server_key"],
            status=body.get("status", "idle"),
            current_task_id=body.get("current_task_id"),
            disk_free_gb=body.get("disk_free_gb"),
        )
        return {"ok": True}
    return await _run_blocking(do_heartbeat)


@router.get("/next-task")
async def get_next_task(server_key: str, disk_free_gb: float = 0):
    """Worker asks for its next task. Coordinator assigns based on priority + disk space."""
    def do_assign():
        snapshot.init_db()
        conn = snapshot._conn()

        # Check if this worker already has an assigned task
        existing = conn.execute(
            "SELECT * FROM tasks WHERE server = ? AND status IN ('downloading', 'assigned')",
            (server_key,),
        ).fetchone()
        if existing:
            return {"task": dict(existing)}

        # Find best pending task for this worker
        pending = conn.execute(
            "SELECT * FROM tasks WHERE status = 'pending' ORDER BY priority ASC, created_at ASC"
        ).fetchall()

        for row in pending:
            task = dict(row)
            est_size = task.get("size_gb") or 0

            # Skip if task is too big for this worker's disk
            # Need at least size * 0.3 free (chunked mode handles the rest)
            min_required = max(est_size * 0.1, 20)  # at least 20GB free
            if disk_free_gb < min_required:
                continue

            # Check affinity: if task was previously on this worker, prefer it
            # If task was on a different worker, skip (let that worker reclaim it)
            if task.get("id") in _task_affinity:
                if _task_affinity[task["id"]] != server_key:
                    continue

            # Assign task to this worker
            conn.execute(
                "UPDATE tasks SET status = 'assigned', server = ?, updated_at = ? WHERE id = ?",
                (server_key, time.time(), task["id"]),
            )
            conn.commit()
            _task_affinity[task["id"]] = server_key
            logger.info(f"Assigned {task['name']} to {server_key} (disk_free={disk_free_gb:.0f}GB)")
            return {"task": task}

        return {"task": None}

    return await _run_blocking(do_assign)


@router.post("/progress")
async def report_progress(body: dict):
    """Worker reports download progress."""
    def do_progress():
        snapshot.init_db()
        task_id = body["task_id"]
        kwargs = {k: v for k, v in body.items() if k in (
            "status", "phase", "progress_pct", "speed_mbps", "downloaded_gb"
        )}
        kwargs["server"] = body.get("server_key")
        snapshot.update_task_progress(task_id, **kwargs)
        return {"ok": True}
    return await _run_blocking(do_progress)


@router.post("/complete")
async def report_complete(body: dict):
    """Worker reports task completion (done or failed)."""
    def do_complete():
        snapshot.init_db()
        task_id = body["task_id"]
        status = body.get("status", "done")
        conn = snapshot._conn()

        if status == "done":
            conn.execute(
                "UPDATE tasks SET status = 'done', phase = NULL, progress_pct = 100, "
                "speed_mbps = 0, completed_at = ?, updated_at = ? WHERE id = ?",
                (time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()), time.time(), task_id),
            )
            logger.info(f"Task completed: {task_id}")
        else:
            error = body.get("error", "unknown")
            retry = conn.execute("SELECT retry_count FROM tasks WHERE id = ?", (task_id,)).fetchone()
            retry_count = (retry[0] or 0) + 1 if retry else 1

            if retry_count < 5:
                conn.execute(
                    "UPDATE tasks SET status = 'pending', phase = 'retry_waiting', "
                    "error = ?, retry_count = ?, updated_at = ? WHERE id = ?",
                    (error, retry_count, time.time(), task_id),
                )
            else:
                conn.execute(
                    "UPDATE tasks SET status = 'failed', phase = NULL, "
                    "error = ?, retry_count = ?, updated_at = ? WHERE id = ?",
                    (error, retry_count, time.time(), task_id),
                )
            logger.warning(f"Task failed: {task_id} ({error})")

        conn.commit()
        _task_affinity.pop(task_id, None)
        return {"ok": True}
    return await _run_blocking(do_complete)
```

- [ ] **Step 2: Register agent router in app.py**

Add to `dlm/web/app.py`:
```python
from .routes.agent import router as agent_router
app.include_router(agent_router, prefix="/api")
```

- [ ] **Step 3: Add dead-worker detection to scheduler**

In `dlm/web/scheduler.py`, add to the main loop:
```python
def _check_dead_workers():
    """If a worker hasn't heartbeated in 5 min, release its tasks back to pending."""
    conn = snapshot._conn()
    threshold = time.time() - 300  # 5 minutes

    dead_workers = conn.execute(
        "SELECT server_key FROM workers WHERE last_seen < ? AND status != 'offline'",
        (threshold,),
    ).fetchall()

    for row in dead_workers:
        server = row[0]
        # Release tasks assigned to dead worker
        conn.execute(
            "UPDATE tasks SET status = 'pending', server = NULL, phase = 'worker_died' "
            "WHERE server = ? AND status IN ('assigned', 'downloading')",
            (server,),
        )
        conn.execute(
            "UPDATE workers SET status = 'offline' WHERE server_key = ?", (server,),
        )
        logger.warning(f"Worker {server} dead — released its tasks")

    if dead_workers:
        conn.commit()
```

- [ ] **Step 4: Commit**

```bash
git add dlm/web/routes/agent.py dlm/web/app.py dlm/web/scheduler.py
git commit -m "feat: add coordinator API with auto-dispatch and dead-worker recovery"
```

---

### Task 3: Multi-Worker Parallel Download for Large Datasets

Split a single large dataset across multiple workers for faster completion.

**Files:**
- Modify: `dlm/web/routes/agent.py` — Add split-task logic
- Modify: `dlm/agent/worker.py` — Handle split-task assignments
- Test: manual test with Sekai split across 2 workers

**Interfaces:**
- Consumes: Task DB with `split_config` field
- Produces: Multiple sub-tasks that independently download portions of a dataset

- [ ] **Step 1: Add task splitting to coordinator**

When a task is very large (>1TB) and multiple workers are idle, split it:

```python
# Add to dlm/web/routes/agent.py

@router.post("/split-task")
async def split_task(body: dict):
    """Split a large task across multiple workers.

    Body:
        task_id: str — the large task to split
        worker_count: int — how many workers to use (2-4)
    """
    def do_split():
        snapshot.init_db()
        conn = snapshot._conn()

        task = dict(conn.execute("SELECT * FROM tasks WHERE id = ?", (body["task_id"],)).fetchone())
        worker_count = min(body.get("worker_count", 2), 4)

        # List all files in the repo to divide them
        from ..agent.downloader import Downloader
        # We'll need a helper that just lists files
        from huggingface_hub import HfApi
        api = HfApi(token=os.environ.get("HF_TOKEN"))
        repo_type = "dataset" if task["type"] == "dataset" else "model"

        files = []
        for item in api.list_repo_tree(task["repo_id"], repo_type=repo_type, recursive=True):
            if hasattr(item, "size") and item.size:
                files.append({"path": item.rfilename, "size": item.size})

        # Sort and divide into N roughly-equal chunks by total size
        files.sort(key=lambda f: f["size"], reverse=True)
        chunks = [[] for _ in range(worker_count)]
        chunk_sizes = [0] * worker_count

        for f in files:
            # Put file in the chunk with least total size (greedy balancing)
            min_idx = chunk_sizes.index(min(chunk_sizes))
            chunks[min_idx].append(f["path"])
            chunk_sizes[min_idx] += f["size"]

        # Create sub-tasks
        sub_tasks = []
        for i, chunk in enumerate(chunks):
            sub_id = f"{task['id']}-part{i+1}"
            sub_task = dict(task)
            sub_task["id"] = sub_id
            sub_task["name"] = f"{task['name']}-part{i+1}"
            sub_task["status"] = "pending"
            sub_task["phase"] = "split"
            sub_task["split_parent"] = task["id"]
            sub_task["split_files"] = json.dumps(chunk)
            sub_task["size_gb"] = chunk_sizes[i] / (1024**3)
            snapshot.upsert_task(sub_task)
            sub_tasks.append({"id": sub_id, "files": len(chunk), "size_gb": round(chunk_sizes[i]/1024**3, 1)})

        # Mark original as "split"
        conn.execute("UPDATE tasks SET status = 'split', phase = ? WHERE id = ?",
                     (f"split into {worker_count} parts", task["id"]))
        conn.commit()

        return {"ok": True, "sub_tasks": sub_tasks}
    return await _run_blocking(do_split)
```

- [ ] **Step 2: Handle split assignments in worker**

In `dlm/agent/downloader.py`, check if task has `split_files`:
```python
def run(self, progress_callback=None):
    if self.task.get("split_files"):
        # Only download assigned files
        all_paths = json.loads(self.task["split_files"])
        files = [{"path": p, "size": self._get_file_size(p)} for p in all_paths]
    else:
        files = self._list_repo_files()
    # ... rest of existing logic
```

- [ ] **Step 3: Commit**

```bash
git add dlm/web/routes/agent.py dlm/agent/downloader.py
git commit -m "feat: add multi-worker parallel download for large datasets"
```

---

### Task 4: Migration & Deployment

Deploy the new agent alongside existing Celery workers (can coexist during transition).

**Files:**
- Create: `scripts/start-agent.sh` — Worker startup script
- Create: `scripts/deploy-agents.sh` — Deploy to all workers from S1
- Modify: `dlm/web/app.py` — Ensure both old and new routes work

- [ ] **Step 1: Create worker start script**

```bash
#!/bin/bash
# scripts/start-agent.sh — Run on each worker
set -a
source /root/.env 2>/dev/null
source /root/code/bos-download-manager/.env 2>/dev/null
set +a

SERVER_KEY=${DLM_SERVER_KEY:-$(hostname)}
cd /root/code/bos-download-manager

exec python3 -m dlm.agent --server-key "$SERVER_KEY" --log-level INFO
```

- [ ] **Step 2: Create deploy script**

```bash
#!/bin/bash
# scripts/deploy-agents.sh — Deploy and start agents on all workers
WORKERS="156.240.120.209 154.85.53.152 154.85.49.95 154.85.40.244 154.85.54.251 154.85.50.210 156.240.121.60"
KEYS=(w1 w2 w3 w4 w5 w6 w7)

i=0
for ip in $WORKERS; do
    key=${KEYS[$i]}
    echo "=== Deploying to $key ($ip) ==="

    # Sync code
    rsync -az /root/code/bos-download-manager/dlm/ root@$ip:/root/code/bos-download-manager/dlm/
    rsync -az /root/code/bos-download-manager/scripts/ root@$ip:/root/code/bos-download-manager/scripts/

    # Kill old celery, start new agent
    ssh root@$ip "
        pkill -f celery 2>/dev/null
        pkill -f 'dlm.agent' 2>/dev/null
        sleep 2
        export DLM_SERVER_KEY=$key
        tmux kill-session -t dlm-worker 2>/dev/null
        tmux new-session -d -s dlm-worker 'bash /root/code/bos-download-manager/scripts/start-agent.sh'
    "

    echo "  $key started"
    ((i++))
done
```

- [ ] **Step 3: Install aria2 on all workers**

```bash
for ip in 156.240.120.209 154.85.53.152 154.85.49.95 154.85.40.244 154.85.54.251 154.85.50.210 156.240.121.60; do
    ssh root@$ip "apt-get install -y aria2 2>&1 | tail -1"
done
```

- [ ] **Step 4: Restart web server with new routes**

```bash
# On S1:
pkill -f "dlm web"
cd /root/code/bos-download-manager
nohup python3 -m dlm web --port 8080 > /tmp/dlm-web.log 2>&1 &
```

- [ ] **Step 5: Commit**

```bash
git add scripts/ dlm/web/app.py
git commit -m "feat: add deployment scripts for new agent architecture"
```

---

## Expected Performance After Migration

| Metric | Before | After |
|--------|--------|-------|
| Auto-dispatch | Manual `apply_async` | Automatic pull every 15s |
| Download speed (regular HF) | 16 MB/s (1 conn) | 50-80 MB/s (aria2c 16 conn) |
| Download speed (XET repos) | 16 MB/s | 16 MB/s (can't improve) |
| Large dataset (Sekai 9.3TB) | 10 days on 1 worker | 3-4 days split across 3 workers |
| Cluster throughput | 100 GB/day | 5-15 TB/day |
| Progress persistence | Lost on restart | Local file, survives restart |
| Idle workers | Stay idle forever | Auto-assigned in 15s |
| Ghost tasks | Common | Impossible (no message broker) |
| Disk full crashes | Common | Pre-flight check before assign |

## Rollback Plan

If the new agent has issues, revert to Celery:
```bash
# On each worker:
pkill -f 'dlm.agent'
tmux new-session -d -s dlm-worker 'celery -A dlm.queue.app worker ...'
```

Both systems use the same SQLite DB, so state is preserved.
