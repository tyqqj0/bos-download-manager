# DLM Temporal Migration — Pipeline Download Platform

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the broken Celery-based dispatch with Temporal durable workflows, implementing a pipeline download+upload architecture with disk backpressure so 7 workers can sustain ~5-15 TB/day with zero manual intervention.

**Architecture:** Workers connect to Temporal Server (on S1) and poll for workflow tasks. Each download workflow runs a pipeline: download threads feed files to upload threads concurrently. A disk monitor pauses downloads when disk exceeds 80%, resumes at 50%. Temporal persists workflow state in Postgres — crash recovery is automatic with no progress loss.

**Tech Stack:** Python 3.10, Temporal (temporalio SDK), FastAPI (dashboard), SQLite (catalog + dashboard), Postgres (Temporal state), aria2c (multi-connection download), BOS SDK (upload), huggingface_hub (file listing)

## Global Constraints

- Python >=3.9 (already installed on all workers)
- Temporal Server via docker-compose on S1 (154.85.43.52)
- Workers connect to Temporal at `S1:7233` (gRPC)
- Reuse existing `BOSSDKMover._upload_one()` and `upload_file()` for BOS uploads
- Reuse existing SQLite schema in `/data/dlm.db` (add columns, don't break)
- `aria2c` must be installed on all workers (`apt-get install -y aria2`)
- Staging path: `/data/staging/` on all workers
- BOS buckets: `auwomo-data` (datasets), `auwomo-model-open` (models)
- Environment: `BAIDU_AK`, `BAIDU_SK`, `BOS_ENDPOINT`, `HF_TOKEN` in `.env`

---

## File Structure

```
dlm/
├── temporal/                    # NEW — Temporal workflow package
│   ├── __init__.py
│   ├── __main__.py             # Worker entry: python -m dlm.temporal --server-key w1
│   ├── workflows.py            # Workflow definitions (DownloadDataset, LargeDatasetSplit)
│   ├── activities.py           # Activity implementations (download, upload, disk ops)
│   ├── pipeline.py             # Core pipeline engine (concurrent download+upload+backpressure)
│   ├── downloader.py           # Download strategies (aria2c, hf_download)
│   └── models.py               # Dataclasses for workflow I/O
├── web/
│   ├── app.py                  # MODIFY — remove Celery, add Temporal client
│   ├── scheduler.py            # MODIFY — replace Celery polling with Temporal queries
│   └── routes/
│       └── queue.py            # MODIFY — dispatch via Temporal instead of Celery
├── queue/
│   └── snapshot.py             # MODIFY — add workflow_run_id column
└── pyproject.toml              # MODIFY — replace celery dep with temporalio
```

---

### Task 1: Temporal Server Deployment (docker-compose on S1)

**Files:**
- Create: `deploy/docker-compose.yml`
- Create: `deploy/README.md`

**Interfaces:**
- Produces: Temporal Server at `154.85.43.52:7233` (gRPC) and Web UI at `154.85.43.52:8233`

- [ ] **Step 1: Create docker-compose.yml**

```yaml
# deploy/docker-compose.yml
version: "3.8"
services:
  postgresql:
    image: postgres:15
    environment:
      POSTGRES_USER: temporal
      POSTGRES_PASSWORD: temporal
      POSTGRES_DB: temporal
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    restart: unless-stopped

  temporal:
    image: temporalio/auto-setup:latest
    depends_on:
      - postgresql
    environment:
      - DB=postgres12
      - DB_PORT=5432
      - POSTGRES_USER=temporal
      - POSTGRES_PWD=temporal
      - POSTGRES_SEEDS=postgresql
      - DYNAMIC_CONFIG_FILE_PATH=config/dynamicconfig/development-sql.yaml
    ports:
      - "7233:7233"
    restart: unless-stopped

  temporal-ui:
    image: temporalio/ui:latest
    depends_on:
      - temporal
    environment:
      - TEMPORAL_ADDRESS=temporal:7233
      - TEMPORAL_CORS_ORIGINS=http://localhost:3000
    ports:
      - "8233:8080"
    restart: unless-stopped

volumes:
  postgres_data:
```

- [ ] **Step 2: Deploy on S1**

```bash
ssh root@154.85.43.52 "mkdir -p /root/code/bos-download-manager/deploy"
scp deploy/docker-compose.yml root@154.85.43.52:/root/code/bos-download-manager/deploy/
ssh root@154.85.43.52 "cd /root/code/bos-download-manager/deploy && docker-compose up -d"
```

- [ ] **Step 3: Verify Temporal is running**

```bash
ssh root@154.85.43.52 "curl -s http://localhost:7233/health | head -1"
```

Expected: gRPC health response (or connection accepted).
Temporal UI: open `http://154.85.43.52:8233` in browser.

- [ ] **Step 4: Commit**

```bash
git add deploy/
git commit -m "infra: add Temporal Server docker-compose for S1"
```

---

### Task 2: Temporal Models and Pipeline Engine

The core innovation: a pipeline that downloads and uploads concurrently with disk backpressure.

**Files:**
- Create: `dlm/temporal/__init__.py`
- Create: `dlm/temporal/models.py`
- Create: `dlm/temporal/pipeline.py`

**Interfaces:**
- Produces:
  - `TaskInput` dataclass — workflow input
  - `TaskResult` dataclass — workflow output
  - `PipelineEngine.run(files, task_input, heartbeat_fn)` — runs the download+upload pipeline
  - `PipelineStats` dataclass — real-time stats

- [ ] **Step 1: Create package init**

```python
# dlm/temporal/__init__.py
"""Temporal-based download workflow engine."""
```

- [ ] **Step 2: Create models**

```python
# dlm/temporal/models.py
"""Dataclasses for Temporal workflow I/O."""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class TaskInput:
    """Input to the download workflow."""
    id: str
    name: str
    repo_id: str
    source: str = "hf"           # "hf" or "modelscope"
    type: str = "dataset"        # "dataset" or "model"
    category: str = ""
    priority: int = 5
    size_gb: float = 0
    assigned_files: list = field(default_factory=list)  # for split tasks


@dataclass
class FileInfo:
    """A single file in a repo."""
    path: str
    size: int  # bytes


@dataclass
class PipelineStats:
    """Real-time pipeline statistics."""
    total_files: int = 0
    downloaded_files: int = 0
    uploaded_files: int = 0
    total_bytes: int = 0
    uploaded_bytes: int = 0
    speed_mbps: float = 0
    phase: str = "starting"
    paused: bool = False


@dataclass
class TaskResult:
    """Output from the download workflow."""
    status: str  # "done", "failed"
    files_uploaded: int = 0
    bytes_uploaded: int = 0
    error: Optional[str] = None
```

- [ ] **Step 3: Create pipeline engine**

```python
# dlm/temporal/pipeline.py
"""Pipeline engine — concurrent download + upload with disk backpressure.

Architecture:
  Download threads → [disk buffer] → Upload threads
                         ↕
                    Disk Monitor
                    (>80% → pause downloads)
                    (<50% → resume downloads)
"""

import asyncio
import logging
import os
import shutil
import time
from pathlib import Path
from typing import Callable, Optional

from .models import TaskInput, FileInfo, PipelineStats

logger = logging.getLogger(__name__)

STAGING_PATH = Path("/data/staging")
DISK_PAUSE_THRESHOLD = 0.80   # pause downloads at 80% disk usage
DISK_RESUME_THRESHOLD = 0.50  # resume downloads at 50% disk usage
DOWNLOAD_CONCURRENCY = 4      # parallel download tasks
UPLOAD_CONCURRENCY = 8        # parallel upload tasks


def _disk_usage_pct() -> float:
    stat = shutil.disk_usage(STAGING_PATH)
    return stat.used / stat.total


def _disk_free_gb() -> float:
    stat = shutil.disk_usage(STAGING_PATH)
    return stat.free / (1024 ** 3)


class PipelineEngine:
    """Runs download+upload pipeline with backpressure."""

    def __init__(
        self,
        task_input: TaskInput,
        staging_dir: Path,
        heartbeat_fn: Callable[[str], None],
    ):
        self.task = task_input
        self.staging_dir = staging_dir
        self.heartbeat_fn = heartbeat_fn
        self.stats = PipelineStats()
        self._cancel = False
        self._download_paused = asyncio.Event()
        self._download_paused.set()  # not paused initially

    async def run(self, files: list[FileInfo]) -> PipelineStats:
        """Execute the pipeline. Returns final stats."""
        self.stats.total_files = len(files)
        self.stats.total_bytes = sum(f.size for f in files)
        self.stats.phase = "downloading"

        upload_queue: asyncio.Queue = asyncio.Queue(maxsize=100)

        # Start disk monitor
        monitor_task = asyncio.create_task(self._disk_monitor())

        # Start uploaders
        upload_tasks = [
            asyncio.create_task(self._uploader(upload_queue, i))
            for i in range(UPLOAD_CONCURRENCY)
        ]

        # Start downloaders
        file_queue: asyncio.Queue = asyncio.Queue()
        for f in files:
            await file_queue.put(f)

        download_tasks = [
            asyncio.create_task(self._downloader(file_queue, upload_queue, i))
            for i in range(DOWNLOAD_CONCURRENCY)
        ]

        # Wait for all downloads to finish
        await asyncio.gather(*download_tasks)

        # Signal uploaders to stop
        for _ in upload_tasks:
            await upload_queue.put(None)

        # Wait for all uploads to finish
        self.stats.phase = "uploading_remaining"
        await asyncio.gather(*upload_tasks)

        # Stop monitor
        self._cancel = True
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass

        self.stats.phase = "done"
        return self.stats

    async def _downloader(self, file_queue: asyncio.Queue,
                          upload_queue: asyncio.Queue, worker_id: int):
        """Download files from queue, respecting backpressure."""
        from .downloader import download_file

        while True:
            try:
                file_info = file_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            # Backpressure: wait if disk is too full
            while not self._download_paused.is_set():
                self.stats.paused = True
                self.heartbeat_fn(
                    f"⏸ disk {_disk_free_gb():.0f}GB free, waiting for uploads... "
                    f"↓{self.stats.downloaded_files}/{self.stats.total_files} "
                    f"↑{self.stats.uploaded_files}/{self.stats.total_files}"
                )
                await asyncio.sleep(3)
            self.stats.paused = False

            if self._cancel:
                break

            # Download the file
            local_path = self.staging_dir / file_info.path
            local_path.parent.mkdir(parents=True, exist_ok=True)

            try:
                await download_file(
                    self.task, file_info, local_path,
                )
                self.stats.downloaded_files += 1

                # Pass to upload queue
                await upload_queue.put((file_info, local_path))

                self.heartbeat_fn(
                    f"↓{self.stats.downloaded_files}/{self.stats.total_files} "
                    f"↑{self.stats.uploaded_files}/{self.stats.total_files} "
                    f"({self.stats.uploaded_bytes / 1024**3:.1f}GB done)"
                )
            except Exception as e:
                logger.error(f"Download failed {file_info.path}: {e}")
                # Put back in queue for retry
                await file_queue.put(file_info)
                await asyncio.sleep(5)

    async def _uploader(self, upload_queue: asyncio.Queue, worker_id: int):
        """Upload files to BOS, delete local after success."""
        from ..core.config import load_config
        from ..core.bos import create_bos_client, upload_file
        from ..constants import DATA_BUCKET, MODEL_BUCKET

        config = load_config()
        client = create_bos_client(
            config["BAIDU_AK"], config["BAIDU_SK"], config["BOS_ENDPOINT"]
        )

        if self.task.type == "model":
            bucket = MODEL_BUCKET
            prefix = f"{self.task.name}/"
        else:
            bucket = DATA_BUCKET
            prefix = f"{self.task.category}/{self.task.name}/" if self.task.category else f"{self.task.name}/"

        while True:
            item = await upload_queue.get()
            if item is None:
                break

            file_info, local_path = item
            key = prefix + file_info.path

            try:
                # Run blocking upload in thread pool
                await asyncio.to_thread(
                    upload_file, client, bucket, key, str(local_path)
                )

                # Delete local file immediately after upload
                try:
                    local_path.unlink()
                except OSError:
                    pass

                self.stats.uploaded_files += 1
                self.stats.uploaded_bytes += file_info.size

            except Exception as e:
                logger.error(f"Upload failed {file_info.path}: {e}")
                # Re-queue for retry
                await upload_queue.put((file_info, local_path))
                await asyncio.sleep(5)

    async def _disk_monitor(self):
        """Monitor disk usage, pause/resume downloads."""
        while not self._cancel:
            usage = _disk_usage_pct()

            if usage > DISK_PAUSE_THRESHOLD and self._download_paused.is_set():
                self._download_paused.clear()
                logger.warning(
                    f"Disk at {usage:.0%}, pausing downloads "
                    f"(free: {_disk_free_gb():.0f}GB)"
                )
            elif usage < DISK_RESUME_THRESHOLD and not self._download_paused.is_set():
                self._download_paused.set()
                logger.info(
                    f"Disk at {usage:.0%}, resuming downloads "
                    f"(free: {_disk_free_gb():.0f}GB)"
                )

            await asyncio.sleep(5)
```

- [ ] **Step 4: Commit**

```bash
git add dlm/temporal/__init__.py dlm/temporal/models.py dlm/temporal/pipeline.py
git commit -m "feat: add Temporal pipeline engine with disk backpressure"
```

---

### Task 3: Download Strategies (aria2c + hf)

**Files:**
- Create: `dlm/temporal/downloader.py`

**Interfaces:**
- Consumes: `TaskInput`, `FileInfo` from `dlm/temporal/models.py`
- Produces: `download_file(task, file_info, local_path)` — async function that downloads a single file using the best strategy

- [ ] **Step 1: Create downloader module**

```python
# dlm/temporal/downloader.py
"""Download strategies — picks aria2c or hf_download based on file characteristics."""

import asyncio
import logging
import os
import subprocess
from pathlib import Path
from typing import Optional

from .models import TaskInput, FileInfo

logger = logging.getLogger(__name__)

ARIA2C_CONNECTIONS = 16
ARIA2C_MIN_FILE_SIZE = 100 * 1024 * 1024  # 100MB — below this, hf download is fine


async def download_file(task: TaskInput, file_info: FileInfo, local_path: Path):
    """Download a single file using the best strategy.

    Strategy selection:
    - Large file (>100MB) + non-XET: aria2c with 16 connections
    - Everything else: hf download (handles XET, auth, small files)
    """
    if local_path.exists() and local_path.stat().st_size == file_info.size:
        logger.debug(f"Already exists: {file_info.path}")
        return

    if file_info.size > ARIA2C_MIN_FILE_SIZE:
        url = await _resolve_hf_url(task.repo_id, file_info.path, task.type)
        if url:
            await _download_aria2c(url, local_path, file_info.path)
            return

    # Fallback: hf download
    await _download_hf(task, [file_info.path], local_path.parent)


async def download_batch_hf(task: TaskInput, file_paths: list[str], staging_dir: Path):
    """Download multiple files at once using hf download (efficient for many small files)."""
    MAX_ARGS = 500
    for i in range(0, len(file_paths), MAX_ARGS):
        chunk = file_paths[i:i + MAX_ARGS]
        await _download_hf(task, chunk, staging_dir)


async def _resolve_hf_url(repo_id: str, file_path: str, repo_type: str) -> Optional[str]:
    """Resolve direct download URL. Returns None if XET protocol (can't use aria2c)."""
    def _resolve():
        try:
            from huggingface_hub import hf_hub_url
            import requests

            rtype = "dataset" if repo_type == "dataset" else "model"
            url = hf_hub_url(repo_id, file_path, repo_type=rtype)

            token = os.environ.get("HF_TOKEN", "")
            headers = {"Authorization": f"Bearer {token}"} if token else {}
            resp = requests.head(url, headers=headers, allow_redirects=False, timeout=10)

            if resp.status_code in (301, 302):
                location = resp.headers.get("location", "")
                if "xet" in location.lower():
                    return None  # XET — aria2c won't work
                return location
            return url
        except Exception:
            return None

    return await asyncio.to_thread(_resolve)


async def _download_aria2c(url: str, local_path: Path, display_name: str):
    """Download a file with aria2c multi-connection."""
    token = os.environ.get("HF_TOKEN", "")
    local_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        "aria2c",
        "--max-connection-per-server", str(ARIA2C_CONNECTIONS),
        "--split", str(ARIA2C_CONNECTIONS),
        "--min-split-size", "20M",
        "--dir", str(local_path.parent),
        "--out", local_path.name,
        "--continue=true",
        "--auto-file-renaming=false",
        "--allow-overwrite=true",
        "--max-tries", "5",
        "--retry-wait", "10",
        "--timeout", "300",
        "--connect-timeout", "30",
    ]
    if token:
        cmd.extend(["--header", f"Authorization: Bearer {token}"])
    cmd.append(url)

    logger.info(f"aria2c [{ARIA2C_CONNECTIONS} conn]: {display_name}")

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    stdout, _ = await proc.communicate()

    if proc.returncode != 0:
        output = stdout.decode(errors="replace")[:300] if stdout else ""
        raise RuntimeError(f"aria2c failed for {display_name}: {output}")


async def _download_hf(task: TaskInput, file_paths: list[str], staging_dir: Path):
    """Download files using hf CLI."""
    rtype = "dataset" if task.type == "dataset" else "model"
    cmd = [
        "hf", "download", task.repo_id,
        "--local-dir", str(staging_dir),
        "--repo-type", rtype,
        "--max-workers", "32",
    ]
    cmd.extend(file_paths)

    env = os.environ.copy()
    env["HF_HUB_ENABLE_HF_TRANSFER"] = "1"
    env["HF_HUB_CACHE"] = "/tmp/hf_cache"
    if os.environ.get("HF_TOKEN"):
        env["HF_TOKEN"] = os.environ["HF_TOKEN"]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    stdout, _ = await proc.communicate()

    if proc.returncode != 0:
        output = stdout.decode(errors="replace")[:500] if stdout else ""
        raise RuntimeError(f"hf download failed: {output}")
```

- [ ] **Step 2: Commit**

```bash
git add dlm/temporal/downloader.py
git commit -m "feat: add download strategies (aria2c multi-conn + hf fallback)"
```

---

### Task 4: Temporal Workflows and Activities

**Files:**
- Create: `dlm/temporal/workflows.py`
- Create: `dlm/temporal/activities.py`

**Interfaces:**
- Consumes: `PipelineEngine` from Task 2, `download_file` from Task 3, `TaskInput`/`TaskResult` from Task 2
- Produces:
  - `DownloadDatasetWorkflow.run(input: TaskInput) -> TaskResult`
  - `SplitDownloadWorkflow.run(input: TaskInput) -> TaskResult`
  - Activities: `list_repo_files`, `run_pipeline_batch`, `cleanup_staging`, `save_progress`, `load_progress`

- [ ] **Step 1: Create activities**

```python
# dlm/temporal/activities.py
"""Temporal activities — the actual work units that run on workers."""

import asyncio
import json
import logging
import os
import shutil
from pathlib import Path
from typing import Optional

from temporalio import activity

from .models import TaskInput, FileInfo, PipelineStats

logger = logging.getLogger(__name__)

STAGING_PATH = Path("/data/staging")


@activity.defn
async def list_repo_files(task_input: TaskInput) -> list[dict]:
    """List all files in the HF repo. Returns list of {path, size} dicts."""
    def _list():
        from huggingface_hub import HfApi

        api = HfApi(token=os.environ.get("HF_TOKEN"))
        repo_type = "dataset" if task_input.type == "dataset" else "model"

        files = []
        for item in api.list_repo_tree(
            task_input.repo_id, repo_type=repo_type, recursive=True
        ):
            if hasattr(item, "size") and item.size and hasattr(item, "rfilename"):
                files.append({"path": item.rfilename, "size": item.size})

        return files

    activity.heartbeat("listing repo files...")
    result = await asyncio.to_thread(_list)
    activity.heartbeat(f"found {len(result)} files")
    return result


@activity.defn
async def load_progress(task_input: TaskInput) -> list[str]:
    """Load list of already-uploaded file paths from local progress file."""
    progress_file = STAGING_PATH / task_input.name / ".progress.json"
    try:
        if progress_file.exists():
            data = json.loads(progress_file.read_text())
            if isinstance(data, list):
                return data
    except Exception:
        pass
    return []


@activity.defn
async def save_progress(task_name: str, completed_paths: list[str]):
    """Save completed file paths to local progress file."""
    progress_file = STAGING_PATH / task_name / ".progress.json"
    progress_file.parent.mkdir(parents=True, exist_ok=True)
    progress_file.write_text(json.dumps(completed_paths))


@activity.defn
async def clear_progress(task_name: str):
    """Remove progress file after task completes."""
    progress_file = STAGING_PATH / task_name / ".progress.json"
    try:
        progress_file.unlink(missing_ok=True)
    except Exception:
        pass


@activity.defn
async def run_pipeline_batch(task_input: TaskInput, file_dicts: list[dict]) -> dict:
    """Run the download+upload pipeline for a batch of files.

    This is the core activity — it handles concurrent download and upload
    with disk backpressure. Sends heartbeats with progress.

    Returns dict with stats.
    """
    from .pipeline import PipelineEngine

    files = [FileInfo(path=f["path"], size=f["size"]) for f in file_dicts]
    staging_dir = STAGING_PATH / task_input.name
    staging_dir.mkdir(parents=True, exist_ok=True)

    def heartbeat_fn(msg: str):
        activity.heartbeat(msg)

    engine = PipelineEngine(task_input, staging_dir, heartbeat_fn)
    stats = await engine.run(files)

    return {
        "downloaded_files": stats.downloaded_files,
        "uploaded_files": stats.uploaded_files,
        "uploaded_bytes": stats.uploaded_bytes,
        "total_bytes": stats.total_bytes,
    }


@activity.defn
async def cleanup_staging(task_name: str, keep_progress: bool = False):
    """Clean staging directory for a task."""
    staging_dir = STAGING_PATH / task_name
    if not staging_dir.exists():
        return

    progress_file = staging_dir / ".progress.json"
    progress_data = None
    if keep_progress and progress_file.exists():
        progress_data = progress_file.read_text()

    shutil.rmtree(staging_dir, ignore_errors=True)

    if progress_data:
        staging_dir.mkdir(parents=True, exist_ok=True)
        progress_file.write_text(progress_data)

    activity.heartbeat(f"cleaned staging for {task_name}")


@activity.defn
async def cleanup_all_staging(except_task: Optional[str] = None):
    """Clean ALL staging dirs except the specified task."""
    if not STAGING_PATH.exists():
        STAGING_PATH.mkdir(parents=True, exist_ok=True)
        return

    for d in STAGING_PATH.iterdir():
        if not d.is_dir():
            continue
        if except_task and d.name == except_task:
            continue
        shutil.rmtree(d, ignore_errors=True)
        logger.info(f"Cleaned staging: {d.name}")


@activity.defn
async def report_to_dashboard(task_id: str, status: str, phase: str = None,
                              progress_pct: float = None, speed_mbps: float = None,
                              downloaded_gb: float = None, server: str = None,
                              error: str = None):
    """Update the SQLite dashboard snapshot (for web UI)."""
    def _update():
        from ..queue.snapshot import init_db, update_task_progress, complete_task
        init_db()

        if status in ("done", "failed"):
            complete_task(task_id, status)
        else:
            kwargs = {"status": status}
            if phase is not None:
                kwargs["phase"] = phase
            if progress_pct is not None:
                kwargs["progress_pct"] = progress_pct
            if speed_mbps is not None:
                kwargs["speed_mbps"] = speed_mbps
            if downloaded_gb is not None:
                kwargs["downloaded_gb"] = downloaded_gb
            if server is not None:
                kwargs["server"] = server
            if error is not None:
                kwargs["error"] = error
            update_task_progress(task_id, **kwargs)

    await asyncio.to_thread(_update)
```

- [ ] **Step 2: Create workflows**

```python
# dlm/temporal/workflows.py
"""Temporal workflow definitions — orchestrate the download lifecycle."""

import asyncio
from datetime import timedelta
from typing import Optional

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from .models import TaskInput, TaskResult


BATCH_SIZE = 500  # files per pipeline batch (Temporal checkpoint boundary)

NON_RETRYABLE_ERRORS = [
    "NotFoundError",
    "GatedRepoError",
    "AuthError",
]

ACTIVITY_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=30),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=10),
    maximum_attempts=5,
    non_retryable_error_types=NON_RETRYABLE_ERRORS,
)


@workflow.defn
class DownloadDatasetWorkflow:
    """Download a dataset: list files → pipeline download+upload in batches.

    Each batch is a checkpoint. If the worker dies, Temporal restarts
    from the last completed batch. Within a batch, the local .progress.json
    tracks individual files for sub-batch resume.
    """

    @workflow.run
    async def run(self, task_input: TaskInput) -> TaskResult:
        server_key = workflow.info().task_queue.removeprefix("download-")

        # 1. Report starting
        await workflow.execute_activity(
            "report_to_dashboard",
            args=[task_input.id, "downloading", "starting", 0, 0, 0, server_key, None],
            start_to_close_timeout=timedelta(seconds=30),
        )

        # 2. Clean staging (keep only this task)
        await workflow.execute_activity(
            "cleanup_all_staging",
            args=[task_input.name],
            start_to_close_timeout=timedelta(minutes=5),
        )

        # 3. List files
        try:
            file_dicts = await workflow.execute_activity(
                "list_repo_files",
                args=[task_input],
                start_to_close_timeout=timedelta(minutes=10),
                heartbeat_timeout=timedelta(minutes=3),
                retry_policy=ACTIVITY_RETRY,
            )
        except Exception as e:
            error_msg = str(e)
            await workflow.execute_activity(
                "report_to_dashboard",
                args=[task_input.id, "failed", None, None, None, None, server_key, error_msg],
                start_to_close_timeout=timedelta(seconds=30),
            )
            return TaskResult(status="failed", error=error_msg)

        if not file_dicts:
            await workflow.execute_activity(
                "report_to_dashboard",
                args=[task_input.id, "failed", None, None, None, None, server_key, "No files found in repo"],
                start_to_close_timeout=timedelta(seconds=30),
            )
            return TaskResult(status="failed", error="No files found in repo")

        # 4. Filter to assigned files (for split tasks)
        if task_input.assigned_files:
            assigned_set = set(task_input.assigned_files)
            file_dicts = [f for f in file_dicts if f["path"] in assigned_set]

        total_bytes = sum(f["size"] for f in file_dicts)
        total_gb = total_bytes / (1024 ** 3)

        # 5. Load progress (resume support)
        completed_paths = await workflow.execute_activity(
            "load_progress",
            args=[task_input],
            start_to_close_timeout=timedelta(seconds=30),
        )

        if completed_paths:
            completed_set = set(completed_paths)
            file_dicts = [f for f in file_dicts if f["path"] not in completed_set]
            uploaded_bytes = sum(
                f["size"] for f in file_dicts
                if f["path"] in completed_set
            )
            # recalculate from original
            uploaded_bytes = total_bytes - sum(f["size"] for f in file_dicts)
        else:
            uploaded_bytes = 0

        # 6. Process in batches
        batch_num = 0
        all_completed = list(completed_paths) if completed_paths else []

        # Sort: largest files first for better disk utilization
        file_dicts.sort(key=lambda f: f["size"], reverse=True)

        while file_dicts:
            batch_num += 1
            batch = file_dicts[:BATCH_SIZE]
            file_dicts = file_dicts[BATCH_SIZE:]

            batch_bytes = sum(f["size"] for f in batch)
            pct = (uploaded_bytes / total_bytes * 100) if total_bytes > 0 else 0

            await workflow.execute_activity(
                "report_to_dashboard",
                args=[task_input.id, "downloading", f"batch {batch_num}",
                      round(pct, 1), None, round(uploaded_bytes / 1024**3, 2), server_key, None],
                start_to_close_timeout=timedelta(seconds=30),
            )

            # Run pipeline for this batch
            result = await workflow.execute_activity(
                "run_pipeline_batch",
                args=[task_input, batch],
                start_to_close_timeout=timedelta(hours=24),
                heartbeat_timeout=timedelta(minutes=10),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(minutes=1),
                    backoff_coefficient=2.0,
                    maximum_interval=timedelta(minutes=30),
                    maximum_attempts=3,
                    non_retryable_error_types=NON_RETRYABLE_ERRORS,
                ),
            )

            # Save progress checkpoint
            uploaded_bytes += result["uploaded_bytes"]
            batch_paths = [f["path"] for f in batch]
            all_completed.extend(batch_paths)

            await workflow.execute_activity(
                "save_progress",
                args=[task_input.name, all_completed],
                start_to_close_timeout=timedelta(seconds=30),
            )

        # 7. Done!
        await workflow.execute_activity(
            "clear_progress",
            args=[task_input.name],
            start_to_close_timeout=timedelta(seconds=30),
        )
        await workflow.execute_activity(
            "cleanup_staging",
            args=[task_input.name, False],
            start_to_close_timeout=timedelta(minutes=5),
        )
        await workflow.execute_activity(
            "report_to_dashboard",
            args=[task_input.id, "done", None, 100, 0, round(total_gb, 2), server_key, None],
            start_to_close_timeout=timedelta(seconds=30),
        )

        return TaskResult(
            status="done",
            files_uploaded=len(all_completed),
            bytes_uploaded=uploaded_bytes,
        )


@workflow.defn
class SplitDownloadWorkflow:
    """Split a large dataset across multiple workers.

    Divides files into N chunks (greedy by size) and runs
    DownloadDatasetWorkflow as child workflows — one per chunk.
    Each child runs on a different worker's task queue.
    """

    @workflow.run
    async def run(self, task_input: TaskInput, worker_count: int = 2) -> TaskResult:
        # List all files
        file_dicts = await workflow.execute_activity(
            "list_repo_files",
            args=[task_input],
            start_to_close_timeout=timedelta(minutes=10),
            heartbeat_timeout=timedelta(minutes=3),
            retry_policy=ACTIVITY_RETRY,
        )

        if not file_dicts:
            return TaskResult(status="failed", error="No files found")

        # Greedy partition by size
        file_dicts.sort(key=lambda f: f["size"], reverse=True)
        chunks: list[list] = [[] for _ in range(worker_count)]
        chunk_sizes = [0] * worker_count

        for f in file_dicts:
            min_idx = chunk_sizes.index(min(chunk_sizes))
            chunks[min_idx].append(f["path"])
            chunk_sizes[min_idx] += f["size"]

        # Launch child workflows
        child_handles = []
        for i, chunk in enumerate(chunks):
            child_input = TaskInput(
                id=f"{task_input.id}-part{i+1}",
                name=f"{task_input.name}",  # same name — same BOS prefix
                repo_id=task_input.repo_id,
                source=task_input.source,
                type=task_input.type,
                category=task_input.category,
                priority=task_input.priority,
                size_gb=chunk_sizes[i] / (1024 ** 3),
                assigned_files=chunk,
            )
            handle = await workflow.start_child_workflow(
                DownloadDatasetWorkflow.run,
                args=[child_input],
                id=f"{task_input.id}-part{i+1}",
                task_queue=f"download-workers",  # any available worker
            )
            child_handles.append(handle)

        # Wait for all children
        results = await asyncio.gather(*child_handles)
        total_files = sum(r.files_uploaded for r in results)
        total_bytes = sum(r.bytes_uploaded for r in results)
        failed = [r for r in results if r.status == "failed"]

        if failed:
            return TaskResult(
                status="failed",
                files_uploaded=total_files,
                bytes_uploaded=total_bytes,
                error=f"{len(failed)}/{worker_count} parts failed",
            )

        return TaskResult(
            status="done",
            files_uploaded=total_files,
            bytes_uploaded=total_bytes,
        )
```

- [ ] **Step 3: Commit**

```bash
git add dlm/temporal/workflows.py dlm/temporal/activities.py
git commit -m "feat: add Temporal workflows (DownloadDataset + SplitDownload)"
```

---

### Task 5: Temporal Worker Entry Point

**Files:**
- Create: `dlm/temporal/__main__.py`

**Interfaces:**
- Consumes: All activities from Task 4, all workflows from Task 4
- Produces: CLI entry point `python -m dlm.temporal --server-key w1`

- [ ] **Step 1: Create worker entry point**

```python
# dlm/temporal/__main__.py
"""Temporal worker entry point.

Usage:
    python -m dlm.temporal --server-key w1
    python -m dlm.temporal --server-key w1 --temporal-host 154.85.43.52:7233
"""

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

from temporalio.client import Client
from temporalio.worker import Worker

from .workflows import DownloadDatasetWorkflow, SplitDownloadWorkflow
from .activities import (
    list_repo_files,
    load_progress,
    save_progress,
    clear_progress,
    run_pipeline_batch,
    cleanup_staging,
    cleanup_all_staging,
    report_to_dashboard,
)


def parse_args():
    parser = argparse.ArgumentParser(description="DLM Temporal Worker")
    parser.add_argument("--server-key", required=True, help="Worker identifier (w1-w7)")
    parser.add_argument(
        "--temporal-host",
        default=os.environ.get("TEMPORAL_HOST", "154.85.43.52:7233"),
        help="Temporal server address",
    )
    parser.add_argument("--task-queue", default=None, help="Override task queue name")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


async def run_worker(args):
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )
    logger = logging.getLogger("dlm.temporal")

    # Load .env
    from dotenv import load_dotenv
    for env_path in [Path("/root/.env"), Path("/root/code/bos-download-manager/.env")]:
        if env_path.exists():
            load_dotenv(env_path)

    # Ensure staging exists
    Path("/data/staging").mkdir(parents=True, exist_ok=True)

    # Connect to Temporal
    logger.info(f"Connecting to Temporal at {args.temporal_host}...")
    client = await Client.connect(args.temporal_host)

    task_queue = args.task_queue or f"download-{args.server_key}"

    # Register activities and workflows
    activities = [
        list_repo_files,
        load_progress,
        save_progress,
        clear_progress,
        run_pipeline_batch,
        cleanup_staging,
        cleanup_all_staging,
        report_to_dashboard,
    ]

    workflows = [
        DownloadDatasetWorkflow,
        SplitDownloadWorkflow,
    ]

    logger.info(f"Starting worker: server_key={args.server_key}, queue={task_queue}")
    logger.info(f"Registered {len(workflows)} workflows, {len(activities)} activities")

    # Run worker — polls Temporal for tasks automatically
    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=workflows,
        activities=activities,
    )

    logger.info("Worker running. Waiting for tasks...")
    await worker.run()


def main():
    args = parse_args()
    asyncio.run(run_worker(args))


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Commit**

```bash
git add dlm/temporal/__main__.py
git commit -m "feat: add Temporal worker entry point (python -m dlm.temporal)"
```

---

### Task 6: Web API — Dispatch via Temporal

Replace Celery dispatch with Temporal workflow start.

**Files:**
- Modify: `dlm/web/app.py`
- Create: `dlm/web/temporal_client.py`
- Modify: `dlm/web/routes/queue.py`
- Modify: `dlm/web/scheduler.py`

**Interfaces:**
- Consumes: `DownloadDatasetWorkflow`, `SplitDownloadWorkflow`, `TaskInput`
- Produces: API endpoints that start Temporal workflows instead of Celery tasks

- [ ] **Step 1: Create Temporal client singleton**

```python
# dlm/web/temporal_client.py
"""Temporal client singleton for the web server."""

import asyncio
import logging
import os
from typing import Optional

from temporalio.client import Client

logger = logging.getLogger("dlm.web")

_client: Optional[Client] = None


async def get_client() -> Client:
    """Get or create the Temporal client connection."""
    global _client
    if _client is None:
        host = os.environ.get("TEMPORAL_HOST", "154.85.43.52:7233")
        logger.info(f"Connecting to Temporal at {host}...")
        _client = await Client.connect(host)
    return _client


async def start_download(task_dict: dict, task_queue: str = "download-workers"):
    """Start a DownloadDatasetWorkflow for a task."""
    from ..temporal.models import TaskInput
    from ..temporal.workflows import DownloadDatasetWorkflow

    client = await get_client()
    task_input = TaskInput(
        id=task_dict["id"],
        name=task_dict["name"],
        repo_id=task_dict["repo_id"],
        source=task_dict.get("source", "hf"),
        type=task_dict.get("type", "dataset"),
        category=task_dict.get("category", ""),
        priority=task_dict.get("priority", 5),
        size_gb=task_dict.get("size_gb", 0),
    )

    handle = await client.start_workflow(
        DownloadDatasetWorkflow.run,
        args=[task_input],
        id=f"download-{task_dict['id']}",
        task_queue=task_queue,
    )
    logger.info(f"Started workflow download-{task_dict['id']} on queue {task_queue}")
    return handle


async def start_split_download(task_dict: dict, worker_count: int = 2):
    """Start a SplitDownloadWorkflow for a large dataset."""
    from ..temporal.models import TaskInput
    from ..temporal.workflows import SplitDownloadWorkflow

    client = await get_client()
    task_input = TaskInput(
        id=task_dict["id"],
        name=task_dict["name"],
        repo_id=task_dict["repo_id"],
        source=task_dict.get("source", "hf"),
        type=task_dict.get("type", "dataset"),
        category=task_dict.get("category", ""),
        priority=task_dict.get("priority", 5),
        size_gb=task_dict.get("size_gb", 0),
    )

    handle = await client.start_workflow(
        SplitDownloadWorkflow.run,
        args=[task_input, worker_count],
        id=f"split-download-{task_dict['id']}",
        task_queue="download-workers",
    )
    logger.info(f"Started split workflow for {task_dict['name']} ({worker_count} workers)")
    return handle


async def cancel_workflow(task_id: str):
    """Cancel a running workflow."""
    client = await get_client()
    handle = client.get_workflow_handle(f"download-{task_id}")
    try:
        await handle.cancel()
    except Exception as e:
        logger.warning(f"Cancel failed for {task_id}: {e}")


async def list_running_workflows() -> list:
    """List all running download workflows."""
    client = await get_client()
    workflows = []
    async for wf in client.list_workflows('WorkflowType="DownloadDatasetWorkflow" AND ExecutionStatus="Running"'):
        workflows.append({
            "workflow_id": wf.id,
            "status": wf.status.name,
            "start_time": str(wf.start_time) if wf.start_time else None,
        })
    return workflows
```

- [ ] **Step 2: Modify queue routes to use Temporal**

Replace the Celery dispatch in `dlm/web/routes/queue.py`. Key changes:
- `add_to_queue`: call `start_download()` instead of `download_dataset.apply_async()`
- `pause_task`: call `cancel_workflow()` instead of Celery revoke
- `resume_task`: call `start_download()` again
- `delete_from_queue`: call `cancel_workflow()` + delete from SQLite

```python
# dlm/web/routes/queue.py — FULL REPLACEMENT
"""Queue management API — Temporal-based dispatch."""

import logging
import uuid
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

import asyncio
from fastapi import APIRouter

from ...queue import snapshot

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
        split_workers: int (optional) — split across N workers for large datasets
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
    split_workers = int(body.get("split_workers", 0))

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

    # Start Temporal workflow
    from ..temporal_client import start_download, start_split_download
    try:
        if split_workers >= 2:
            await start_split_download(task_meta, worker_count=split_workers)
        else:
            await start_download(task_meta)
    except Exception as e:
        logger.error(f"Failed to start workflow: {e}")
        return {"error": f"Failed to start workflow: {e}"}

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

    from ..temporal_client import start_download
    await start_download(task)

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

    from ..temporal_client import start_download
    await start_download(task)

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
```

- [ ] **Step 3: Simplify scheduler (remove Celery polling)**

```python
# dlm/web/scheduler.py — FULL REPLACEMENT
"""Background scheduler — dashboard refresh and Temporal workflow status sync."""

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor

from .cache import cache

logger = logging.getLogger("dlm.web")

_executor = ThreadPoolExecutor(max_workers=4)

DASHBOARD_INTERVAL = 10
WORKFLOW_SYNC_INTERVAL = 30
TRANSFER_INTERVAL = 60


def _build_dashboard() -> dict:
    """Build dashboard from SQLite snapshot."""
    from ..queue.snapshot import get_dashboard_summary, get_all_tasks, get_workers
    summary = get_dashboard_summary()
    workers = get_workers()

    now = time.time()
    active_workers = [w for w in workers if now - (w.get("last_seen") or 0) < 180]

    summary["workers"] = workers
    summary["active_worker_count"] = len(active_workers)

    all_tasks = get_all_tasks()
    recent = sorted(
        [t for t in all_tasks if t.get("status") in ("done", "failed") and t.get("completed_at")],
        key=lambda t: t.get("completed_at", ""),
        reverse=True,
    )[:10]
    summary["recent_activity"] = recent

    queue_next = [t for t in all_tasks if t.get("status") == "pending"][:5]
    summary["queue_next"] = queue_next

    alerts = _build_alerts(all_tasks, workers)
    summary["alerts"] = alerts

    return summary


def _build_alerts(tasks: list, workers: list) -> list:
    alerts = []
    now = time.time()

    for w in workers:
        if now - (w.get("last_seen") or 0) > 180 and w.get("status") != "offline":
            alerts.append({
                "type": "worker_offline",
                "server": w.get("server_key", w.get("hostname", "")),
                "duration_min": int((now - (w.get("last_seen") or now)) / 60),
            })

    for t in tasks:
        if t.get("status") == "failed" and (t.get("retry_count") or 0) >= 5:
            alerts.append({
                "type": "task_failed_repeat",
                "task": t.get("name", ""),
                "count": t.get("retry_count", 0),
                "error": t.get("error_class") or t.get("error") or "",
            })

    return alerts


def _poll_transfers():
    """Check status of in-progress D-Robotics transfers."""
    import os
    from ..queue.snapshot import _conn

    conn = _conn()
    rows = conn.execute(
        "SELECT id, name, transfer_task_id FROM tasks WHERE transfer_status = 'transferring'"
    ).fetchall()
    transferring = [dict(r) for r in rows]

    if not transferring:
        return 0

    dcloud_user = os.environ.get("DCLOUD_USER")
    dcloud_pass = os.environ.get("DCLOUD_PASS")
    if not dcloud_user or not dcloud_pass:
        return 0

    try:
        from ..transfer.dcloud import DCloudClient
        client = DCloudClient(dcloud_user, dcloud_pass)
        client.login()

        async_tasks = client.list_async_tasks(page_size=100)
        task_status_map = {t.get("task_id"): t for t in async_tasks}

        updated = 0
        from datetime import datetime, timezone
        now_ts = time.time()

        for task in transferring:
            if not task.get("transfer_task_id"):
                continue
            remote = task_status_map.get(task["transfer_task_id"])
            if not remote:
                continue
            status = remote.get("status", "")
            if status in ("成功", "success", "done"):
                conn.execute(
                    "UPDATE tasks SET transfer_status = ?, transfer_error = NULL, updated_at = ? WHERE id = ?",
                    ("done", now_ts, task["id"]),
                )
                updated += 1
            elif status in ("失败", "failed", "error"):
                conn.execute(
                    "UPDATE tasks SET transfer_status = ?, transfer_error = ?, updated_at = ? WHERE id = ?",
                    ("failed", remote.get("error_msg", status), now_ts, task["id"]),
                )
                updated += 1

        if updated:
            conn.commit()
        return updated
    except Exception as e:
        logger.error(f"Transfer poll error: {e}")
        return 0


async def background_scheduler():
    """Main background loop — refresh dashboard and poll transfers."""
    loop = asyncio.get_event_loop()
    last_transfer_poll = 0

    await asyncio.sleep(2)

    while True:
        try:
            dashboard = await loop.run_in_executor(_executor, _build_dashboard)
            cache.set_dashboard(dashboard)

            now = time.time()
            if now - last_transfer_poll > TRANSFER_INTERVAL:
                await loop.run_in_executor(_executor, _poll_transfers)
                last_transfer_poll = now

        except Exception as e:
            logger.error(f"Scheduler error: {e}")

        await asyncio.sleep(DASHBOARD_INTERVAL)
```

- [ ] **Step 4: Update app.py**

```python
# dlm/web/app.py — FULL REPLACEMENT
"""FastAPI application factory — Temporal workflow architecture."""

import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

logger = logging.getLogger("dlm.web")

STATIC_DIR = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    from .scheduler import background_scheduler
    from ..queue.snapshot import init_db
    init_db()
    logger.info("Starting background scheduler...")
    task = asyncio.create_task(background_scheduler())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def create_app() -> FastAPI:
    app = FastAPI(
        title="DLM Dashboard",
        description="Dataset Download Manager — Temporal Workflows",
        version="3.0.0",
        lifespan=lifespan,
    )

    from .routes.dashboard import router as dashboard_router
    from .routes.tasks import router as tasks_router
    from .routes.queue import router as queue_router
    from .routes.servers import router as servers_router
    from .routes.doctor import router as doctor_router
    from .routes.transfer import router as transfer_router
    from .routes.storage import router as storage_router

    app.include_router(queue_router, prefix="/api")
    app.include_router(tasks_router, prefix="/api")
    app.include_router(dashboard_router, prefix="/api")
    app.include_router(servers_router, prefix="/api")
    app.include_router(doctor_router, prefix="/api")
    app.include_router(transfer_router, prefix="/api")
    app.include_router(storage_router, prefix="/api")

    STATIC_DIR.mkdir(parents=True, exist_ok=True)
    app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")

    return app
```

- [ ] **Step 5: Update pyproject.toml dependencies**

Replace `celery[redis]>=5.3` with `temporalio>=1.5`:

```toml
dependencies = [
    "click>=8.0",
    "python-dotenv>=1.0",
    "bce-python-sdk>=0.9",
    "pyyaml>=6.0",
    "huggingface_hub>=0.20",
    "temporalio>=1.5",
    "redis>=5.0",
]
```

- [ ] **Step 6: Commit**

```bash
git add dlm/web/temporal_client.py dlm/web/routes/queue.py dlm/web/scheduler.py dlm/web/app.py pyproject.toml
git commit -m "feat: replace Celery with Temporal for workflow dispatch and scheduling"
```

---

### Task 7: Deployment Scripts and Worker Setup

**Files:**
- Create: `scripts/start-temporal-worker.sh`
- Create: `scripts/deploy-all.sh`
- Create: `scripts/install-deps.sh`

**Interfaces:**
- Consumes: Temporal Server (Task 1), Worker entry point (Task 5)
- Produces: Running workers on all 7 machines

- [ ] **Step 1: Create worker start script**

```bash
#!/bin/bash
# scripts/start-temporal-worker.sh
# Usage: DLM_SERVER_KEY=w1 bash scripts/start-temporal-worker.sh
set -euo pipefail

SERVER_KEY="${DLM_SERVER_KEY:?Must set DLM_SERVER_KEY (e.g. w1)}"
TEMPORAL_HOST="${TEMPORAL_HOST:-154.85.43.52:7233}"

cd /root/code/bos-download-manager

# Load environment
set -a
[ -f /root/.env ] && source /root/.env
[ -f .env ] && source .env
set +a

export TEMPORAL_HOST="$TEMPORAL_HOST"

echo "[$(date)] Starting Temporal worker: $SERVER_KEY -> $TEMPORAL_HOST"
exec python3 -m dlm.temporal --server-key "$SERVER_KEY" --temporal-host "$TEMPORAL_HOST"
```

- [ ] **Step 2: Create install script**

```bash
#!/bin/bash
# scripts/install-deps.sh — Run on each worker to install dependencies
set -euo pipefail

echo "Installing system deps..."
apt-get update -qq && apt-get install -y -qq aria2

echo "Installing Python deps..."
cd /root/code/bos-download-manager
pip install -e ".[web]" 2>&1 | tail -3

echo "Verifying..."
python3 -c "import temporalio; print(f'temporalio {temporalio.__version__}')"
aria2c --version | head -1
echo "Done."
```

- [ ] **Step 3: Create deployment script**

```bash
#!/bin/bash
# scripts/deploy-all.sh — Deploy from S1 to all workers
set -euo pipefail

WORKERS=(
    "w1:156.240.120.209"
    "w2:154.85.53.152"
    "w3:154.85.49.95"
    "w4:154.85.40.244"
    "w5:154.85.54.251"
    "w6:154.85.50.210"
    "w7:156.240.121.60"
)

echo "=== Syncing code to workers ==="
for entry in "${WORKERS[@]}"; do
    key="${entry%%:*}"
    ip="${entry##*:}"
    echo "  → $key ($ip)"
    rsync -az --delete \
        --exclude='.git' --exclude='__pycache__' --exclude='.env' \
        /root/code/bos-download-manager/ \
        root@$ip:/root/code/bos-download-manager/ &
done
wait
echo "  Sync complete."

echo ""
echo "=== Installing deps on workers ==="
for entry in "${WORKERS[@]}"; do
    key="${entry%%:*}"
    ip="${entry##*:}"
    echo "  → $key ($ip)"
    ssh root@$ip "bash /root/code/bos-download-manager/scripts/install-deps.sh" 2>&1 | tail -2 &
done
wait
echo "  Install complete."

echo ""
echo "=== Starting workers ==="
for entry in "${WORKERS[@]}"; do
    key="${entry%%:*}"
    ip="${entry##*:}"
    echo "  → $key ($ip)"
    ssh root@$ip "
        # Kill old celery/worker processes
        pkill -f 'celery' 2>/dev/null || true
        pkill -f 'dlm.temporal' 2>/dev/null || true
        pkill -f 'dlm.worker' 2>/dev/null || true
        sleep 2

        # Start new Temporal worker in tmux
        tmux kill-session -t dlm-worker 2>/dev/null || true
        export DLM_SERVER_KEY=$key
        tmux new-session -d -s dlm-worker \
            'DLM_SERVER_KEY=$key bash /root/code/bos-download-manager/scripts/start-temporal-worker.sh'
    "
    echo "    $key started"
done

echo ""
echo "=== All workers deployed ==="
echo "Check Temporal UI: http://154.85.43.52:8233"
echo "Check Dashboard:   http://154.85.43.52:8080"
```

- [ ] **Step 4: Commit**

```bash
chmod +x scripts/start-temporal-worker.sh scripts/deploy-all.sh scripts/install-deps.sh
git add scripts/
git commit -m "feat: add deployment scripts for Temporal workers"
```

---

### Task 8: Worker Heartbeat Integration

Workers should report their status to the dashboard SQLite so the web UI shows live worker info.

**Files:**
- Modify: `dlm/temporal/activities.py` — add `worker_heartbeat` activity
- Modify: `dlm/temporal/workflows.py` — call heartbeat periodically
- Modify: `dlm/temporal/__main__.py` — background heartbeat task

**Interfaces:**
- Consumes: `snapshot.update_worker()` from `dlm/queue/snapshot.py`
- Produces: Live worker status in dashboard (hostname, disk_free, current_task, status)

- [ ] **Step 1: Add heartbeat to worker main loop**

Add a background asyncio task in `dlm/temporal/__main__.py` that reports worker status every 15 seconds:

```python
# Add to dlm/temporal/__main__.py, inside run_worker() before worker.run():

async def _heartbeat_loop(server_key: str):
    """Report worker status to S1 dashboard every 15s."""
    import shutil
    import requests
    from pathlib import Path

    coordinator = os.environ.get("DLM_COORDINATOR", "http://154.85.43.52:8080")
    staging = Path("/data/staging")

    while True:
        try:
            disk_free = shutil.disk_usage(staging).free / (1024 ** 3)
            requests.post(
                f"{coordinator}/api/worker-heartbeat",
                json={
                    "server_key": server_key,
                    "hostname": f"{server_key}@temporal",
                    "disk_free_gb": round(disk_free, 1),
                    "status": "online",
                },
                timeout=5,
            )
        except Exception:
            pass
        await asyncio.sleep(15)

# Start heartbeat before worker.run()
heartbeat_task = asyncio.create_task(_heartbeat_loop(args.server_key))
try:
    await worker.run()
finally:
    heartbeat_task.cancel()
```

- [ ] **Step 2: Add heartbeat API endpoint**

Add to `dlm/web/routes/servers.py` or create a simple endpoint:

```python
# Add to dlm/web/routes/servers.py
@router.post("/worker-heartbeat")
async def worker_heartbeat(body: dict):
    def do_hb():
        snapshot.init_db()
        snapshot.update_worker(
            hostname=body.get("hostname", ""),
            server_key=body["server_key"],
            status=body.get("status", "online"),
            current_task_id=body.get("current_task_id"),
            disk_free_gb=body.get("disk_free_gb"),
        )
        return {"ok": True}
    return await _run_blocking(do_hb)
```

- [ ] **Step 3: Commit**

```bash
git add dlm/temporal/__main__.py dlm/web/routes/servers.py
git commit -m "feat: add worker heartbeat reporting to dashboard"
```

---

### Task 9: Migration — Move Existing Tasks to Temporal

**Files:**
- Create: `scripts/migrate-tasks.py`

**Interfaces:**
- Consumes: Existing SQLite task DB, Temporal client
- Produces: Running workflows for all pending/failed tasks

- [ ] **Step 1: Create migration script**

```python
#!/usr/bin/env python3
"""scripts/migrate-tasks.py — Migrate pending/failed tasks from SQLite to Temporal workflows.

Run on S1 after Temporal is deployed and workers are running.
Usage: python3 scripts/migrate-tasks.py
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dlm.queue.snapshot import init_db, get_all_tasks, update_task_progress


async def main():
    from dlm.web.temporal_client import start_download

    init_db()
    tasks = get_all_tasks()

    # Reset all "downloading" tasks to pending (they're zombies from old Celery)
    downloading = [t for t in tasks if t["status"] == "downloading"]
    for t in downloading:
        update_task_progress(t["id"], status="pending", phase="migrated", speed_mbps=0)
        print(f"  Reset zombie: {t['name']}")

    # Start workflows for all pending tasks
    pending = [t for t in tasks if t["status"] in ("pending", "paused", "preempted")]
    pending.sort(key=lambda t: t.get("priority", 5))

    print(f"\nStarting {len(pending)} workflows...")
    for t in pending:
        try:
            await start_download(t)
            print(f"  ✓ {t['name']} (priority={t.get('priority', 5)})")
        except Exception as e:
            if "already running" in str(e).lower() or "already started" in str(e).lower():
                print(f"  ⊘ {t['name']} (already running)")
            else:
                print(f"  ✗ {t['name']}: {e}")

    # Optionally retry failed tasks
    failed = [t for t in tasks if t["status"] == "failed" and (t.get("retry_count") or 0) < 5]
    if failed:
        print(f"\n{len(failed)} failed tasks eligible for retry:")
        for t in failed[:10]:
            print(f"  - {t['name']} (error: {t.get('error', '')[:50]})")
        print("  Run with --retry-failed to restart them")

    if "--retry-failed" in sys.argv:
        for t in failed:
            try:
                update_task_progress(t["id"], status="pending", phase="retrying", error=None)
                await start_download(t)
                print(f"  ✓ Retrying: {t['name']}")
            except Exception as e:
                print(f"  ✗ {t['name']}: {e}")

    print("\nDone! Check Temporal UI: http://154.85.43.52:8233")


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 2: Commit**

```bash
git add scripts/migrate-tasks.py
git commit -m "feat: add task migration script (Celery → Temporal)"
```

---

## Deployment Sequence (全部步骤)

```bash
# === Phase 1: Deploy Temporal Server (on S1) ===
cd /root/code/bos-download-manager/deploy
docker-compose up -d
# Wait 30s for Temporal to be ready
sleep 30
curl -s http://localhost:7233 || echo "Temporal running"

# === Phase 2: Stop old system ===
# Kill old web server
pkill -f "dlm web" || true
# Kill old Celery workers on all servers (deploy script handles this)

# === Phase 3: Deploy new code + start workers ===
cd /root/code/bos-download-manager
bash scripts/deploy-all.sh

# === Phase 4: Start new web server ===
nohup python3 -m dlm web --port 8080 > /tmp/dlm-web.log 2>&1 &

# === Phase 5: Migrate existing tasks ===
python3 scripts/migrate-tasks.py

# === Phase 6: Verify ===
curl -s http://localhost:8080/api/queue | python3 -m json.tool | head -20
# Open Temporal UI: http://154.85.43.52:8233
```

---

## Expected Results

| Metric | Before (Celery) | After (Temporal) |
|--------|-----------------|------------------|
| Throughput | 100 GB/day | 5-15 TB/day |
| Auto-dispatch | None (manual) | Automatic (workers poll) |
| Crash recovery | Manual restart | Automatic (Temporal retry) |
| Progress persistence | Lost on crash | Checkpoint per batch |
| Disk full handling | Crash | Backpressure (pause download) |
| Worker visibility | Stale/wrong | Real-time heartbeat |
| Ghost tasks | Common | Impossible |
| Download speed (non-XET) | 16 MB/s | 50-80 MB/s (aria2c) |
| Large dataset | 1 worker only | Split across N workers |
