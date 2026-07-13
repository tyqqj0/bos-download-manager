"""Temporal activities — the actual work units that run on workers."""

from __future__ import annotations

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
async def list_repo_files(task_input: TaskInput) -> dict:
    """List all files in the HF repo. Saves to disk, returns metadata.

    Returns {path, count, total_bytes, worker_queue} — the file list stays on
    disk to avoid gRPC limits. worker_queue pins subsequent activities to this worker.
    """
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

        # Save to local file instead of returning via gRPC
        staging_dir = STAGING_PATH / task_input.name
        staging_dir.mkdir(parents=True, exist_ok=True)
        filelist_path = staging_dir / ".filelist.json"
        filelist_path.write_text(json.dumps(files))

        total_bytes = sum(f["size"] for f in files)
        return str(filelist_path), len(files), total_bytes

    activity.heartbeat("listing repo files...")
    path, count, total_bytes = await asyncio.to_thread(_list)
    activity.heartbeat(f"found {count} files, saved to {path}")

    worker_queue = os.environ.get("DLM_WORKER_QUEUE", "download-workers")
    return {
        "path": path,
        "count": count,
        "total_bytes": total_bytes,
        "worker_queue": worker_queue,
    }


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
async def read_filelist(filelist_path: str) -> dict:
    """Read file list metadata from local JSON file (written by list_repo_files).

    Returns {count, total_bytes} — NOT the full list (would exceed gRPC limits).
    Activities that need the file list read it directly from disk.
    """
    path = Path(filelist_path)
    if not path.exists():
        return {"count": 0, "total_bytes": 0}
    data = json.loads(path.read_text())
    files = data if isinstance(data, list) else []
    total_bytes = sum(f.get("size", 0) for f in files)
    return {"count": len(files), "total_bytes": total_bytes}


@activity.defn
async def partition_filelist(filelist_path: str, num_chunks: int) -> list:
    """Partition a file list into N chunks balanced by size.

    Writes separate filelist files for each chunk and returns metadata:
    [{path, count, total_bytes}, ...]
    """
    path = Path(filelist_path)
    if not path.exists():
        return []

    all_files = json.loads(path.read_text())
    if not all_files:
        return []

    # Sort largest first for greedy partition
    all_files.sort(key=lambda f: f.get("size", 0), reverse=True)

    chunks: list[list] = [[] for _ in range(num_chunks)]
    chunk_sizes = [0] * num_chunks

    for f in all_files:
        min_idx = chunk_sizes.index(min(chunk_sizes))
        chunks[min_idx].append(f)
        chunk_sizes[min_idx] += f.get("size", 0)

    # Write each chunk to its own file
    results = []
    parent = path.parent
    for i, chunk in enumerate(chunks):
        chunk_path = parent / f".filelist-part{i+1}.json"
        chunk_path.write_text(json.dumps(chunk))
        results.append({
            "path": str(chunk_path),
            "count": len(chunk),
            "total_bytes": chunk_sizes[i],
        })

    return results


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
async def run_pipeline_batch(task_input: TaskInput, filelist_path: str,
                              start_idx: int, batch_size: int) -> dict:
    """Run the download+upload pipeline for a batch of files.

    Reads the file list from disk (filelist_path) and processes files
    from start_idx to start_idx + batch_size.

    Returns dict with stats.
    """
    import time
    from .pipeline import PipelineEngine

    # Read file list from disk (avoids gRPC size limits)
    filelist = Path(filelist_path)
    all_files = json.loads(filelist.read_text())
    batch_dicts = all_files[start_idx:start_idx + batch_size]

    files = [FileInfo(path=f["path"], size=f["size"]) for f in batch_dicts]
    staging_dir = STAGING_PATH / task_input.name
    staging_dir.mkdir(parents=True, exist_ok=True)

    server_key = os.environ.get("DLM_SERVER_KEY", "")
    last_report_time = [0.0]

    def heartbeat_fn(msg: str):
        activity.heartbeat(msg)

    def progress_fn(downloaded_bytes: int, total_bytes: int, speed_bps: float):
        """Report speed to S1 dashboard via HTTP every 15s."""
        now = time.time()
        if now - last_report_time[0] < 15:
            return
        last_report_time[0] = now

        try:
            import requests
            coordinator = os.environ.get("DLM_COORDINATOR", "http://154.85.43.52:8080")
            speed_mbps = speed_bps * 8 / 1_000_000
            pct = downloaded_bytes / total_bytes * 100 if total_bytes > 0 else 0
            dl_gb = downloaded_bytes / (1024 ** 3)
            requests.post(
                f"{coordinator}/api/task-progress",
                json={
                    "task_id": task_input.id,
                    "status": "downloading",
                    "speed_mbps": round(speed_mbps, 1),
                    "progress_pct": round(pct, 1),
                    "downloaded_gb": round(dl_gb, 2),
                    "server": server_key,
                    "phase": f"batch ({speed_mbps:.0f}Mbps)",
                },
                timeout=5,
            )
        except Exception as e:
            logger.debug(f"Progress report failed: {e}")

    engine = PipelineEngine(task_input, staging_dir, heartbeat_fn, progress_fn)
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
            if downloaded_gb is not None:
                update_task_progress(task_id, downloaded_gb=downloaded_gb)
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
