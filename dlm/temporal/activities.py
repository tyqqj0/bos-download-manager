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
