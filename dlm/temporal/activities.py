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

from .models import TaskInput, FileInfo, PipelineStats, ShardInput

logger = logging.getLogger(__name__)

STAGING_PATH = Path("/data/staging")


@activity.defn
async def list_repo_files(task_input: TaskInput) -> dict:
    """List all files in a HF or ModelScope repo. Saves to disk, returns metadata.

    Returns {path, count, total_bytes, worker_queue} — the file list stays on
    disk to avoid gRPC limits. worker_queue pins subsequent activities to this worker.

    Sends periodic heartbeats while listing to avoid Temporal timeout on large repos.
    """
    file_count = [0]

    def _list_hf():
        from huggingface_hub import HfApi

        api = HfApi(token=os.environ.get("HF_TOKEN"))
        repo_type = "dataset" if task_input.type == "dataset" else "model"

        files = []
        for item in api.list_repo_tree(
            task_input.repo_id, repo_type=repo_type, recursive=True
        ):
            if hasattr(item, "size") and item.size and hasattr(item, "rfilename"):
                files.append({"path": item.rfilename, "size": item.size})
                file_count[0] = len(files)
        return files

    def _list_modelscope():
        from modelscope.hub.api import HubApi

        api = HubApi()
        token = os.environ.get("MODELSCOPE_API_TOKEN") or os.environ.get("MS_TOKEN")

        files = []
        page = 1
        while True:
            page_files = api.get_dataset_files(
                repo_id=task_input.repo_id,
                recursive=True,
                page_number=page,
                page_size=100,
                token=token,
            )
            if not page_files:
                break
            for item in page_files:
                if isinstance(item, dict) and item.get("Type") == "blob":
                    size = item.get("Size", 0) or 0
                    path = item.get("Path", "")
                    if path and size > 0:
                        files.append({"path": path, "size": size})
                        file_count[0] = len(files)
            if len(page_files) < 100:
                break
            page += 1
        return files

    def _list():
        if task_input.source == "modelscope":
            files = _list_modelscope()
        else:
            files = _list_hf()

        staging_dir = STAGING_PATH / task_input.name
        staging_dir.mkdir(parents=True, exist_ok=True)
        filelist_path = staging_dir / ".filelist.json"
        filelist_path.write_text(json.dumps(files))

        total_bytes = sum(f["size"] for f in files)
        return str(filelist_path), len(files), total_bytes

    async def _heartbeat_while_listing():
        while True:
            await asyncio.sleep(30)
            activity.heartbeat(f"listing: {file_count[0]} files found...")

    activity.heartbeat("listing repo files...")
    heartbeat_task = asyncio.create_task(_heartbeat_while_listing())
    try:
        path, count, total_bytes = await asyncio.to_thread(_list)
    finally:
        heartbeat_task.cancel()
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
                              start_idx: int, batch_size: int,
                              prior_bytes: int = 0, total_task_bytes: int = 0) -> dict:
    """Run the download+upload pipeline for a batch of files.

    Reads the file list from disk (filelist_path) and processes files
    from start_idx to start_idx + batch_size.
    prior_bytes: cumulative bytes uploaded in earlier batches (for progress reporting).
    total_task_bytes: total bytes across ALL batches (for percentage calculation).

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
    task_total = total_task_bytes if total_task_bytes > 0 else sum(f.size for f in files)

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
            cumulative = prior_bytes + downloaded_bytes
            pct = cumulative / task_total * 100 if task_total > 0 else 0
            dl_gb = cumulative / (1024 ** 3)
            requests.post(
                f"{coordinator}/api/task-progress",
                json={
                    "task_id": task_input.id,
                    "status": "downloading",
                    "speed_mbps": round(speed_mbps, 1),
                    "progress_pct": round(min(pct, 100), 1),
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

    if stats.failed_files > 0:
        raise RuntimeError(
            f"Batch incomplete: {stats.failed_files}/{stats.total_files} files failed "
            f"(downloaded={stats.downloaded_files}, uploaded={stats.uploaded_files})"
        )

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

    preserved = {}
    if keep_progress:
        for fname in [".progress.json", ".filelist.json"]:
            fpath = staging_dir / fname
            if fpath.exists():
                preserved[fname] = fpath.read_text()

    shutil.rmtree(staging_dir, ignore_errors=True)

    if preserved:
        staging_dir.mkdir(parents=True, exist_ok=True)
        for fname, content in preserved.items():
            (staging_dir / fname).write_text(content)

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


@activity.defn
async def check_disk_space(min_free_gb: int = 25) -> bool:
    """Preflight check: verify worker has enough disk space to start a pipeline."""
    free_gb = shutil.disk_usage(STAGING_PATH).free / (1024 ** 3)
    if free_gb < min_free_gb:
        logger.warning(f"Disk preflight failed: {free_gb:.1f}GB free < {min_free_gb}GB required")
        return False
    return True


# ── Shard activities ────────────────────────────────────────


@activity.defn
async def partition_files_greedy(
    filelist_path: str, num_shards: int, staging_dir: str
) -> list[dict]:
    """Partition files into N shards using greedy bin-packing by size.

    Returns list of {filelist_path, total_files, total_bytes} per shard.
    """
    path = Path(filelist_path)
    all_files = json.loads(path.read_text())

    files_with_size = [(fi["path"], fi.get("size", 0)) for fi in all_files]
    files_with_size.sort(key=lambda x: x[1], reverse=True)

    shards: list[list] = [[] for _ in range(num_shards)]
    shard_sizes = [0] * num_shards

    for fpath, size in files_with_size:
        smallest = min(range(num_shards), key=lambda i: shard_sizes[i])
        shards[smallest].append({"path": fpath, "size": size})
        shard_sizes[smallest] += size

    results = []
    sdir = Path(staging_dir)
    sdir.mkdir(parents=True, exist_ok=True)

    # Upload shard filelists to BOS so all workers can access them
    from ..core.bos import create_bos_client, upload_file
    from ..constants import META_BUCKET
    ak = os.environ.get("BAIDU_AK", "")
    sk = os.environ.get("BAIDU_SK", "")
    endpoint = os.environ.get("BOS_ENDPOINT", "https://bj.bcebos.com")
    bos = create_bos_client(ak, sk, endpoint)

    task_name = Path(staging_dir).name
    for i, shard_files in enumerate(shards):
        shard_filelist = sdir / f".filelist-shard-{i}.json"
        shard_filelist.write_text(json.dumps(shard_files))

        bos_key = f"download-manager/filelists/{task_name}/shard-{i}.json"
        upload_file(bos, META_BUCKET, bos_key, str(shard_filelist))

        results.append({
            "filelist_key": bos_key,
            "total_files": len(shard_files),
            "total_bytes": shard_sizes[i],
        })

    activity.heartbeat(f"partitioned {len(all_files)} files into {num_shards} shards, uploaded to BOS")
    return results


@activity.defn
async def download_shard_filelist(filelist_key: str, staging_dir: str) -> str:
    """Download a shard filelist from BOS to local disk. Returns local path."""
    from ..core.bos import create_bos_client
    from ..constants import META_BUCKET

    ak = os.environ.get("BAIDU_AK", "")
    sk = os.environ.get("BAIDU_SK", "")
    endpoint = os.environ.get("BOS_ENDPOINT", "https://bj.bcebos.com")
    bos = create_bos_client(ak, sk, endpoint)

    sdir = Path(staging_dir)
    sdir.mkdir(parents=True, exist_ok=True)
    local_path = sdir / f".filelist-{Path(filelist_key).stem}.json"

    response = bos.get_object(META_BUCKET, filelist_key)
    local_path.write_bytes(response.data.read())

    return str(local_path)


_COORDINATOR_URL = None

def _coordinator():
    global _COORDINATOR_URL
    if _COORDINATOR_URL is None:
        _COORDINATOR_URL = os.environ.get("DLM_COORDINATOR", "http://154.85.43.52:8080")
    return _COORDINATOR_URL


@activity.defn
async def create_shards_in_db(task_id: str, shard_infos: list[dict]) -> list[str]:
    """Create shard rows via S1 API. Returns shard IDs."""
    import requests
    resp = requests.post(
        f"{_coordinator()}/api/shards/create",
        json={"task_id": task_id, "shard_infos": shard_infos},
        timeout=30,
    )
    data = resp.json()
    if data.get("error"):
        raise RuntimeError(data["error"])
    return data["shard_ids"]


@activity.defn
async def update_shard_status(shard_id: str, status: str, error: str = None):
    """Update shard status via S1 API."""
    import requests
    requests.post(
        f"{_coordinator()}/api/shards/status",
        json={"shard_id": shard_id, "status": status, "error": error},
        timeout=30,
    )


@activity.defn
async def report_shard_progress(shard_id: str, done_files: int = 0,
                                done_bytes: int = 0, speed_mbps: float = 0):
    """Update shard progress via S1 API."""
    import requests
    requests.post(
        f"{_coordinator()}/api/shard-progress",
        json={"shard_id": shard_id, "done_files": done_files,
              "done_bytes": done_bytes, "speed_mbps": speed_mbps},
        timeout=30,
    )


@activity.defn
async def query_idle_workers(source: str) -> list[str]:
    """Query idle workers via S1 API."""
    import requests
    resp = requests.get(
        f"{_coordinator()}/api/shards/idle-workers",
        params={"source": source},
        timeout=30,
    )
    return resp.json().get("workers", [])


@activity.defn
async def aggregate_task_from_shards(task_id: str):
    """Aggregate shard progress into task-level via S1 API."""
    import requests
    requests.post(
        f"{_coordinator()}/api/shards/aggregate",
        json={"task_id": task_id},
        timeout=30,
    )


@activity.defn
async def assign_shard_server(shard_id: str, server_key: str):
    """Record shard-to-server assignment via S1 API."""
    import requests
    requests.post(
        f"{_coordinator()}/api/shards/assign",
        json={"shard_id": shard_id, "server_key": server_key},
        timeout=30,
    )
