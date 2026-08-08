"""Temporal activities — the actual work units that run on workers."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional

from temporalio import activity
from temporalio.exceptions import ApplicationError

from ..core.naming import shard_row_id, split_shard_name
from .models import (
    ARCHIVABLE_FAIL_REASONS,
    POOL_BATCH_FAIL_MAX,
    POOL_BATCH_MAX_ATTEMPTS,
    FileInfo,
    PipelineStats,
    ShardInput,
    TaskInput,
)

logger = logging.getLogger(__name__)

STAGING_PATH = Path("/data/staging")


class FilelistMismatchError(Exception):
    """The bytes at a shard's filelist key are not the bytes the coordinator
    uploaded there.

    Permanent by nature — the object was overwritten, so every retry re-reads
    the same wrong content. Listed in workflows.NON_RETRYABLE_ERRORS by class
    name so eight shards don't each burn five attempts on it.
    """


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
async def load_progress(task_input: TaskInput, filelist_md5: str = "") -> list[str]:
    """Load batch-progress markers from the local progress file.

    Markers are positional (batch index based), so they are only valid for the
    exact filelist that produced them. When filelist_md5 is given, a stored
    hash mismatch — or the legacy bare-list format, which carries no hash —
    invalidates the markers (file removed, empty list returned).
    """
    progress_file = STAGING_PATH / task_input.name / ".progress.json"
    try:
        if progress_file.exists():
            data = json.loads(progress_file.read_text())
            if isinstance(data, dict):
                if not filelist_md5 or data.get("filelist_md5") == filelist_md5:
                    batches = data.get("batches", [])
                    return batches if isinstance(batches, list) else []
                progress_file.unlink(missing_ok=True)
                return []
            if isinstance(data, list):
                if filelist_md5:
                    # legacy format cannot be trusted against a hashed filelist
                    progress_file.unlink(missing_ok=True)
                    return []
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
async def filter_filelist_against_bos(filelist_path: str, task_input: TaskInput) -> dict:
    """Remove files already uploaded to BOS (same key + size) from the filelist.

    One paginated list of the task's target prefix — never per-file requests.
    Writes the filtered list to a NEW file (.filelist.filtered.json) so a stale
    original can never be mistaken for a filtered one. Must run on the worker
    that holds filelist_path (pin via task_queue).
    """
    from ..core.config import load_config
    from ..core.bos import bos_target, create_bos_client

    def _filter():
        config = load_config()
        bos = create_bos_client(
            config["BAIDU_AK"], config["BAIDU_SK"], config["BOS_ENDPOINT"]
        )
        # Same target the uploader writes to — see bos_target()
        bucket, prefix = bos_target(task_input)

        existing = {}
        marker = ""
        while True:
            resp = bos.list_objects(
                bucket, prefix=prefix, marker=marker, max_keys=1000
            )
            for obj in getattr(resp, "contents", None) or []:
                existing[obj.key[len(prefix):]] = obj.size
            if not getattr(resp, "is_truncated", False):
                break
            marker = resp.next_marker

        path = Path(filelist_path)
        files = json.loads(path.read_text())
        remaining = []
        skipped_bytes = 0
        for f in files:
            if existing.get(f.get("path")) == f.get("size"):
                skipped_bytes += f.get("size", 0)
            else:
                remaining.append(f)

        filtered_path = path.with_name(".filelist.filtered.json")
        filtered_path.write_text(json.dumps(remaining))
        return {
            "filtered_path": str(filtered_path),
            "skipped_files": len(files) - len(remaining),
            "skipped_bytes": skipped_bytes,
            "remaining_files": len(remaining),
            "remaining_bytes": sum(f.get("size", 0) for f in remaining),
        }

    # Heartbeat concurrently: with tens of thousands of objects already at
    # the target prefix the paginated listing outlives the heartbeat timeout
    # (same failure mode list_repo_files had on large repos).
    async def _heartbeat_while_filtering():
        while True:
            activity.heartbeat("listing BOS objects for resume filter...")
            await asyncio.sleep(30)

    heartbeat_task = asyncio.create_task(_heartbeat_while_filtering())
    try:
        result = await asyncio.to_thread(_filter)
    finally:
        heartbeat_task.cancel()
    logger.info(
        "BOS resume filter for %s: skipped %d files (%.1f GB), %d remaining (%.1f GB)",
        task_input.name, result["skipped_files"],
        result["skipped_bytes"] / (1024 ** 3),
        result["remaining_files"], result["remaining_bytes"] / (1024 ** 3),
    )
    activity.heartbeat(
        f"skipped {result['skipped_files']} files already on BOS, "
        f"{result['remaining_files']} remaining"
    )
    return result


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
async def save_progress(task_name: str, completed_paths: list[str], filelist_md5: str = ""):
    """Save progress markers. With filelist_md5, writes the hash-guarded format."""
    progress_file = STAGING_PATH / task_name / ".progress.json"
    progress_file.parent.mkdir(parents=True, exist_ok=True)
    if filelist_md5:
        progress_file.write_text(json.dumps(
            {"filelist_md5": filelist_md5, "batches": completed_paths}
        ))
    else:
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

    # Detect shard mode: shard workers use "TaskName/shard-N" naming
    base_name, shard_index = split_shard_name(task_input.name)
    is_shard = shard_index is not None
    shard_id = shard_row_id(task_input.id, shard_index) if is_shard else None

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

            if shard_id:
                # Shard mode: report to shard-progress (auto-aggregates to task)
                requests.post(
                    f"{coordinator}/api/shard-progress",
                    json={
                        "shard_id": shard_id,
                        "done_bytes": cumulative,
                        "speed_mbps": round(speed_mbps, 1),
                    },
                    timeout=5,
                )
            else:
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

    # Shard runs must upload to the TASK's flat BOS prefix, not a shard-N/
    # subprefix — the pipeline derives the upload prefix from task.name, so
    # hand it the base name while keeping staging shard-scoped for isolation.
    # (A shard-N/ prefix would be invisible to the BOS resume filter and
    # would scatter the dataset across per-shard subdirectories.)
    import dataclasses
    engine_task = (
        dataclasses.replace(task_input, name=base_name) if is_shard else task_input
    )
    engine = PipelineEngine(engine_task, staging_dir, heartbeat_fn, progress_fn)
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
    """Report task status to S1 via the coordinator API.

    MUST go over HTTP: this activity runs on a worker, and the SQLite DB is
    S1-local. Writing directly would land every status transition — including
    'done' — in the worker's own throwaway DB, leaving the real task stuck in
    'downloading' forever (observed 2026-07-31).
    """
    import requests

    payload = {"task_id": task_id, "status": status}
    for key, value in (
        ("phase", phase),
        ("progress_pct", progress_pct),
        ("speed_mbps", speed_mbps),
        ("downloaded_gb", downloaded_gb),
        ("server", server),
        ("error", error),
    ):
        if value is not None:
            payload[key] = value

    def _post():
        requests.post(
            f"{_coordinator()}/api/task-progress", json=payload, timeout=30
        ).raise_for_status()

    await asyncio.to_thread(_post)


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
    filelist_path: str, num_shards: int, staging_dir: str, task_id: str = ""
) -> list[dict]:
    """Partition files into N shards using greedy bin-packing by size.

    Returns list of {filelist_key, filelist_md5, total_files, total_bytes}
    per shard. `total_files` across the result must equal the input length —
    the coordinator checks it, because a partition that silently loses files
    produces shards that report done having transferred nothing.

    `task_id` keys the BOS filelist objects. It used to be the task NAME,
    taken from the staging path, and names are not unique: a resume MUST
    reuse the original name and /queue/add permits re-adding a repo whose
    previous row is terminal. Two same-named tasks therefore overwrote each
    other's shard filelists at
    `download-manager/filelists/{name}/shard-{i}.json`, and a shard that
    fetched the loser's copy would download another repo's files into this
    task's BOS prefix. Defaulted for replay of executions that started before
    this argument existed; falls back to the name those runs used.
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

    import hashlib

    scope = task_id or Path(staging_dir).name
    for i, shard_files in enumerate(shards):
        shard_filelist = sdir / f".filelist-shard-{i}.json"
        content = json.dumps(shard_files)
        shard_filelist.write_text(content)

        bos_key = f"download-manager/filelists/{scope}/shard-{i}.json"
        upload_file(bos, META_BUCKET, bos_key, str(shard_filelist))

        results.append({
            "filelist_key": bos_key,
            "filelist_md5": hashlib.md5(content.encode()).hexdigest(),
            "total_files": len(shard_files),
            "total_bytes": shard_sizes[i],
        })

    partitioned = sum(r["total_files"] for r in results)
    if partitioned != len(all_files):
        raise RuntimeError(
            f"partition lost files: {partitioned} of {len(all_files)} from "
            f"{filelist_path} — refusing to return a short partition"
        )

    activity.heartbeat(f"partitioned {len(all_files)} files into {num_shards} shards, uploaded to BOS")
    return results


@activity.defn
async def download_shard_filelist(filelist_key: str, staging_dir: str,
                                  expected_md5: str = "") -> str:
    """Download a shard filelist from BOS to local disk. Returns local path.

    `expected_md5` is the hash the coordinator computed over the bytes it
    uploaded. It was carried in ShardInput.filelist_md5 all along and only
    ever used to validate resume markers — never the filelist itself. So a
    shard fetched whatever was at the key and trusted it, which is precisely
    the hole the name-keyed BOS path opened: a same-named task overwriting
    `download-manager/filelists/{name}/shard-{i}.json` would have this shard
    download another repo's files into this task's prefix, and everything
    downstream would report success. Empty means an execution that started
    before this argument existed; those replay unchecked rather than fail.
    """
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
    payload = response.data.read()

    if expected_md5:
        import hashlib

        got = hashlib.md5(payload).hexdigest()
        if got != expected_md5:
            raise FilelistMismatchError(
                f"shard filelist at bos://{META_BUCKET}/{filelist_key} does not "
                f"match the partition that produced this shard "
                f"(md5 {got} != {expected_md5}) — another task likely overwrote "
                f"it; refusing to download an unknown filelist"
            )

    local_path.write_bytes(payload)

    return str(local_path)


_COORDINATOR_URL = None

def _coordinator():
    global _COORDINATOR_URL
    if _COORDINATOR_URL is None:
        _COORDINATOR_URL = os.environ.get("DLM_COORDINATOR", "http://154.85.43.52:8080")
    return _COORDINATOR_URL


def _report_missing_files(
    task_id: str, batch_index: int, server_key: str, details: list
) -> bool:
    """Archive permanently-failed files against the task. Returns whether it
    landed — never raises.

    Workers cannot touch SQLite (that is S1's, and `activities.py` imports
    nothing from `dlm.queue.snapshot`), so the archive is written over HTTP
    like every other worker→S1 write.

    Returning a bool rather than raising, and the caller's use of it, is the
    whole point: the caller only forgives a batch when this returned True.
    Tolerating a batch whose dropped files were never recorded is precisely
    the silent loss the archive exists to prevent — worse than today's
    behaviour, because today the batch at least fails loudly. So a coordinator
    hiccup costs us the forgiveness, not the record.

    Idempotent at the far end on (task_id, file_path): reporting on every
    attempt bumps `attempts` instead of duplicating rows, which is why this is
    called unconditionally rather than only when the batch is about to be
    forgiven.
    """
    if not details:
        return True
    import requests

    try:
        resp = requests.post(
            f"{_coordinator()}/api/missing-files",
            json={
                "task_id": task_id,
                "batch_index": batch_index,
                "server": server_key,
                "files": [
                    {"path": d.get("path"), "reason": d.get("reason"),
                     "size_bytes": d.get("size_bytes") or 0}
                    for d in details
                ],
            },
            timeout=30,
        )
        resp.raise_for_status()
        body = resp.json()
    except Exception as e:
        logger.error(
            "Missing-file archive POST failed for %s batch %s (%d files): %s: %s",
            task_id, batch_index, len(details), type(e).__name__, e,
        )
        return False

    if body.get("error"):
        logger.error(
            "Missing-file archive rejected for %s batch %s: %s",
            task_id, batch_index, body["error"],
        )
        return False
    if body.get("ignored"):
        # The parent task is paused/revoked/done. Nothing was written and
        # nothing should be — but the caller must not read that as "recorded".
        logger.warning(
            "Missing-file archive ignored for %s batch %s: %s",
            task_id, batch_index, body["ignored"],
        )
        return False
    return True


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
    if "shard_ids" not in data:
        # Neither key: not our route answering. A proxy error page or an S1
        # 500 would otherwise surface as `KeyError: 'shard_ids'`, which reads
        # like a coordinator bug instead of "the request never landed".
        raise RuntimeError(
            f"/api/shards/create returned no shard_ids (HTTP {resp.status_code}): "
            f"{str(data)[:200]}"
        )
    return data["shard_ids"]


def _log_write_result(resp, what: str) -> None:
    """Say it out loud when an S1 state write did not actually happen.

    These routes answer **HTTP 200 with `{"error": ...}`** on a rejected write
    — a missing shard row, a parent task already terminal. Every caller below
    used to discard the response entirely, so the one observable trace of a
    lost status write was its absence from SQLite: nothing in the worker log,
    nothing in the workflow history, and a shard that stays `running` forever
    in the dashboard while its download has long finished.

    Deliberately log-only, no raise. These calls are fire-and-forget in the
    workflow (`report_resume_info`, `aggregate_task_from_shards` and friends
    are dispatched WITHOUT an explicit retry_policy, so they inherit
    Temporal's unlimited-retry default): raising here would put a coordinator
    into an endless retry loop over a rejection that will never clear —
    trading a silent miss for a stalled task. Visibility is the fix; the
    verdict stays with the operator.
    """
    try:
        resp.raise_for_status()
        body = resp.json()
    except Exception as e:
        logger.error("%s: S1 write failed: %s: %s", what, type(e).__name__, e)
        return
    if not isinstance(body, dict):
        logger.error("%s: unexpected S1 response: %s", what, str(body)[:200])
        return
    if body.get("error"):
        logger.error("%s: S1 rejected the write: %s", what, body["error"])
    elif body.get("ignored"):
        # Expected whenever a paused/revoked task's shards report in late.
        logger.warning("%s: S1 ignored the write (task terminal?)", what)


@activity.defn
async def update_shard_status(shard_id: str, status: str, error: str = None):
    """Update shard status via S1 API."""
    import requests
    resp = requests.post(
        f"{_coordinator()}/api/shards/status",
        json={"shard_id": shard_id, "status": status, "error": error},
        timeout=30,
    )
    _log_write_result(resp, f"shard {shard_id} -> {status}")


@activity.defn
async def report_shard_progress(shard_id: str, done_files: int = 0,
                                done_bytes: int = 0, speed_mbps: float = 0):
    """Update shard progress via S1 API."""
    import requests
    resp = requests.post(
        f"{_coordinator()}/api/shard-progress",
        json={"shard_id": shard_id, "done_files": done_files,
              "done_bytes": done_bytes, "speed_mbps": speed_mbps},
        timeout=30,
    )
    _log_write_result(resp, f"shard {shard_id} progress")


@activity.defn
async def query_idle_workers(source: str, exclude_task: str = "") -> list[str]:
    """Query idle workers via S1 API.

    exclude_task: the calling task's own id — its claim/shards must not
    count as busy, or the dispatching worker excludes itself from the pool.
    """
    import requests
    resp = requests.get(
        f"{_coordinator()}/api/shards/idle-workers",
        params={"source": source, "exclude_task": exclude_task},
        timeout=30,
    )
    return resp.json().get("workers", [])


@activity.defn
async def report_resume_info(task_id: str, skipped_files: int, skipped_gb: float):
    """Persist BOS resume-filter results on the task row via S1 API.

    This row is the acceptance evidence for "we did not re-download what BOS
    already has" (requirements A4), so a write that vanishes costs us the
    ability to tell a working resume filter from a broken one.
    """
    import requests
    resp = requests.post(
        f"{_coordinator()}/api/shards/resume-info",
        json={"task_id": task_id, "skipped_files": skipped_files,
              "skipped_gb": skipped_gb},
        timeout=30,
    )
    _log_write_result(
        resp, f"task {task_id} resume-info ({skipped_files} files, {skipped_gb:.1f} GB)"
    )


@activity.defn
async def aggregate_task_from_shards(task_id: str):
    """Aggregate shard progress into task-level via S1 API.

    No-op for pool tasks, which have no shard rows — the pool coordinator
    still calls it, and S1 answers `{"shards": 0}`. Kept rather than made
    conditional: the call is in recorded pool histories, and dropping it
    would break replay for no gain (see requirements R5).
    """
    import requests
    resp = requests.post(
        f"{_coordinator()}/api/shards/aggregate",
        json={"task_id": task_id},
        timeout=30,
    )
    _log_write_result(resp, f"task {task_id} aggregate")


@activity.defn
async def assign_shard_server(shard_id: str, server_key: str):
    """Record shard-to-server assignment via S1 API."""
    import requests
    resp = requests.post(
        f"{_coordinator()}/api/shards/assign",
        json={"shard_id": shard_id, "server_key": server_key},
        timeout=30,
    )
    _log_write_result(resp, f"shard {shard_id} -> server {server_key}")


# ── Pool activities (T3) ────────────────────────────────────


@activity.defn
async def pool_alive_workers(source: str) -> int:
    """Count of alive workers that serve `source` ("hf" | "modelscope").

    "Alive" is a fresh heartbeat (`dlm.web.fleet.WORKER_TIMEOUT`), NOT idle.
    The pool workflow computes its window size from total serving capacity
    (P) — if this only counted idle workers, a fully-loaded pool would
    report P=0 and the window loop would deadlock waiting for slots that
    aren't "gone", just busy with other pool batches.

    Queries the coordinator's `/api/pool/alive-workers` — activities run on
    workers, and the workers table is S1-local SQLite (same reason
    `query_idle_workers` goes over HTTP instead of importing `dlm.queue`).
    """
    import requests
    resp = requests.get(
        f"{_coordinator()}/api/pool/alive-workers",
        params={"source": source},
        timeout=30,
    )
    resp.raise_for_status()
    return int(resp.json().get("count", 0))


# "BatchLimitExceededError" — the Temporal error `type` chunk_filelist raises
# (as an `ApplicationError(..., type="BatchLimitExceededError", non_retryable=True)`)
# when a filelist would produce more batches than the runtime cap allows. The
# same filelist always chunks into the same batch count, so retrying
# chunk_filelist cannot fix this — the task needs to be split or re-sharded.
# `non_retryable=True` on the raise makes this a property of the error itself,
# not something callers must remember to list in
# `RetryPolicy.non_retryable_error_types` (though listing the type name there
# too is harmless belt-and-suspenders).

# Per-batch limits (spec-fixed, not env/fleet-config — unlike POOL_MAX_BATCHES
# these aren't an operator dial, they're the shape of a Temporal-payload-sized
# batch manifest).
BATCH_MAX_FILES = 500
BATCH_MAX_BYTES = 32 * 1024 ** 3  # 32 GiB

# Mirrors dlm.web.fleet.POOL_MAX_BATCHES. Activities run on workers and must
# not import dlm.web (coordinator-only, wrong dependency direction) — kept
# as a chunk_filelist parameter instead of a duplicated worker-side env
# read, so there is exactly one place (fleet.py) that owns the number and
# the workflow (which already knows fleet policy) is free to pass it
# through explicitly. deploy-workers.sh's md5 manifest gate keeps worker and
# coordinator code — and therefore this default and fleet.py's — in sync.
POOL_MAX_BATCHES_DEFAULT = 1500


def _chunk_files(
    files: list[dict], max_files: int = BATCH_MAX_FILES, max_bytes: int = BATCH_MAX_BYTES
) -> list[list[dict]]:
    """Split files into batches of <= max_files and <= max_bytes each.

    A single file over max_bytes gets its own singleton batch instead of
    blocking every other file behind it ("big files isolated").

    First-fit-decreasing: stable-sort by size descending (ties keep input
    order, so identical input always yields identical output), then place
    each file in the first open batch it still fits in, else open a new one.
    Deterministic — required so a retried chunk_filelist call reproduces the
    exact same batch numbering/contents.
    """
    ordered = sorted(files, key=lambda f: f.get("size", 0), reverse=True)

    big_batches: list[list[dict]] = []
    normal: list[dict] = []
    for f in ordered:
        if f.get("size", 0) > max_bytes:
            big_batches.append([f])
        else:
            normal.append(f)

    # `open_bins` holds indices of bins that haven't hit max_files yet — the
    # only ones first-fit can still place a file in. A bin that reaches
    # max_files is removed permanently (files only ever get added, never
    # removed, so a closed bin never re-opens). This does not change which
    # bin any file lands in — it only skips rescanning bins the plain
    # `len(bins[i]) < max_files` check would have rejected anyway — but it
    # bounds the rescan cost for the common case of many small files filling
    # up max_files-sized bins (bare first-fit is O(files * bins), which was
    # ~8.6s at 500k files and ~20s near the batch-count cap).
    bins: list[list[dict]] = []
    bin_bytes: list[int] = []
    open_bins: list[int] = []
    for f in normal:
        size = f.get("size", 0)
        for idx in open_bins:
            if bin_bytes[idx] + size <= max_bytes:
                bins[idx].append(f)
                bin_bytes[idx] += size
                if len(bins[idx]) >= max_files:
                    open_bins.remove(idx)
                break
        else:
            bins.append([f])
            bin_bytes.append(size)
            if len(bins[-1]) < max_files:
                open_bins.append(len(bins) - 1)

    return big_batches + bins


@activity.defn
async def chunk_filelist(
    filtered_path: str,
    task_input: TaskInput,
    max_batches: int = POOL_MAX_BATCHES_DEFAULT,
) -> dict:
    """Chunk a (BOS-filtered) filelist into pool batches, uploading each
    batch's own file list to BOS so any worker can pick it up.

    Must run on the listing worker — filtered_path lives on its local disk
    (same pinning reason as list_repo_files/filter_filelist_against_bos/
    partition_files_greedy). A MISSING file is treated as lost state (the
    filter step's output can vanish to a staging wipe or disk pressure), not
    as an empty filelist — it raises RuntimeError rather than silently
    returning zero batches, which would otherwise read as "already fully
    filtered, nothing to download" and complete the task with zero bytes
    downloaded. A present-but-empty `[]` file is the legitimate no-op and
    still returns zero batches without raising.

    Batching: see `_chunk_files` (<=BATCH_MAX_FILES files, <=BATCH_MAX_BYTES
    bytes per batch, oversized files isolated, deterministic). The read of
    filtered_path and the chunking are heartbeated together with the upload
    phase (one concurrent heartbeat task spanning all three) — `_chunk_files`
    is quadratic in file count and can itself run for many seconds, which
    must not go unheartbeated on the worker's event loop (same failure mode
    filter_filelist_against_bos guards against).

    max_batches is a runtime guard, not just a test assertion: a task that
    would chunk past it needs to be split or re-sharded, not scheduled. On
    overflow raises `temporalio.exceptions.ApplicationError` with
    `type="BatchLimitExceededError"` and `non_retryable=True` — non-retryability
    is carried by the raise itself, so callers don't need to remember to list
    the type in `RetryPolicy.non_retryable_error_types`. The coordinator
    surfaces this as the batch's failure, which the existing "task failed"
    alert path picks up once wired to a terminal write (activities only
    raise; G4).

    Uploads each batch to
    `META_BUCKET:download-manager/batchlists/{task_input.name}/batch-{i}.json`.

    Returns {batch_keys, counts, bytes} — three lists, all indexed by batch
    number i:
      batch_keys[i]: BOS key of batch i's file list
      counts[i]:     file count in batch i
      bytes[i]:      total bytes in batch i
    A present-but-empty filelist returns {"batch_keys": [], "counts": [], "bytes": []}
    with no BOS calls.
    """
    path = Path(filtered_path)

    def _read_and_chunk():
        if not path.exists():
            raise RuntimeError(
                f"chunk_filelist: filtered filelist missing at "
                f"{filtered_path} for {task_input.name} — this is lost "
                f"state, not an empty filelist; re-run the BOS resume "
                f"filter step before chunking"
            )
        files = json.loads(path.read_text())
        return files, _chunk_files(files)

    from ..core.bos import create_bos_client, upload_file
    from ..constants import META_BUCKET

    def _write_and_upload(batches):
        ak = os.environ.get("BAIDU_AK", "")
        sk = os.environ.get("BAIDU_SK", "")
        endpoint = os.environ.get("BOS_ENDPOINT", "https://bj.bcebos.com")
        bos = create_bos_client(ak, sk, endpoint)

        parent = path.parent
        batch_keys, counts, byte_totals = [], [], []
        for i, batch in enumerate(batches):
            batch_bytes = sum(f.get("size", 0) for f in batch)
            local = parent / f".batch-{i}.json"
            local.write_text(json.dumps(batch))

            bos_key = f"download-manager/batchlists/{task_input.name}/batch-{i}.json"
            upload_file(bos, META_BUCKET, bos_key, str(local))

            batch_keys.append(bos_key)
            counts.append(len(batch))
            byte_totals.append(batch_bytes)
        return batch_keys, counts, byte_totals

    # Heartbeat concurrently across read + chunk + upload (started before any
    # of that work begins) — see the docstring note on why the read/chunk
    # phase alone can be slow enough to need it.
    async def _heartbeat_while_chunking():
        while True:
            activity.heartbeat(f"chunking filelist for {task_input.name}...")
            await asyncio.sleep(30)

    heartbeat_task = asyncio.create_task(_heartbeat_while_chunking())
    try:
        all_files, batches = await asyncio.to_thread(_read_and_chunk)

        if len(batches) > max_batches:
            raise ApplicationError(
                f"{task_input.name}: {len(all_files)} files chunk into "
                f"{len(batches)} batches, over the cap of {max_batches} — "
                f"split the task or re-shard instead of scheduling",
                type="BatchLimitExceededError",
                non_retryable=True,
            )

        if not batches:
            return {"batch_keys": [], "counts": [], "bytes": []}

        activity.heartbeat(
            f"uploading {len(batches)} batch lists for {task_input.name}..."
        )
        batch_keys, counts, byte_totals = await asyncio.to_thread(
            _write_and_upload, batches
        )
    finally:
        heartbeat_task.cancel()

    return {"batch_keys": batch_keys, "counts": counts, "bytes": byte_totals}


# ── Pool execution (T4) ─────────────────────────────────────────


class _RetryableDiskLow(Exception):
    """Insufficient disk to safely start a pool batch on this worker.

    Deliberately a plain Exception (retryable — no ApplicationError /
    non_retryable marker): the pool workflow's batch retry policy backs off
    (~5min per the design doc) and re-attempts, by which time this worker
    may have freed space from other pipelines finishing, or a later
    schedule tick sends the retry to a different worker entirely.
    """


# A worker can run several PipelineEngine instances against the same
# /data/staging volume at once: up to 2 from its personal queue (see
# dlm/temporal/__main__.py's max_concurrent_activities=2 for personal_queue)
# + 1 from the shared per-source queue + 1 from the pool queue (this
# activity). What actually bounds a running pipeline's disk use, though, is
# the engine's own backpressure line — a *relative* threshold, not a fixed
# margin — so the coexistence floor is derived from that same line rather
# than from a multiple of the single-shard start gate.
#
# The two constants come straight from the engine (they are public module
# names; only `_disk_free_threshold_gb` itself is private, and G1 forbids
# widening pipeline.py's interface to export it). Only the formula *shape* is
# restated here, and the drift-guard test asserts it still matches.


def _pool_disk_floor_gb() -> tuple[int, float, float]:
    """Default coexistence floor: the engine's refuse-to-accumulate line plus
    room for one full batch.

    Returns `(floor_gb, free_gb, total_gb)` — the caller reports all three so
    a rejected batch says why, on which volume, and what to override.

    Below `max(30% of volume, 20GB)` the engine stops accumulating and aborts
    outright (pipeline.py:373-374), so starting a batch without that much
    headroom *plus* one batch's worth (BATCH_MAX_BYTES) means the batch
    cannot finish on this worker. Capped at half the volume so the floor is
    always satisfiable in principle: on a volume too small for that sum, a
    batch should fail with a real disk error mid-run, not spin forever in a
    preflight that no amount of freed space can pass.
    """
    from .pipeline import DISK_FREE_ABSOLUTE_MIN_GB, DISK_FREE_MIN_PCT

    stat = shutil.disk_usage(STAGING_PATH)
    total_gb = stat.total / (1024 ** 3)
    free_gb = stat.free / (1024 ** 3)
    engine_line = max(total_gb * DISK_FREE_MIN_PCT, DISK_FREE_ABSOLUTE_MIN_GB)
    floor = min(engine_line + BATCH_MAX_BYTES / (1024 ** 3), total_gb * 0.5)
    return int(floor), free_gb, total_gb


def _pool_batch_staging_dir(task_name: str, batch_index: int) -> Path:
    """Staging path for one pool batch — isolated per batch so a retried or
    concurrently-running batch of the same task never collides with another
    batch's files (mirrors the sharded path's per-shard staging isolation,
    `{task}/shard-N/`, generalized to `{task}/pool-batch-N/`)."""
    return STAGING_PATH / task_name / f"pool-batch-{batch_index}"


def _head_skip_filter(files: list[dict], task_input: TaskInput) -> tuple[list[dict], int, int]:
    """Unconditional per-file HEAD check against the task's flat BOS prefix.

    The pool path has no positional progress marker (`.progress.json`) —
    unlike a shard's ordered filelist, a batch has no stable "how far did I
    get" offset, and a retried batch can land on a completely different
    worker with an empty local staging dir. This HEAD check IS the resume
    mechanism: a file already on BOS under the task's flat prefix with the
    exact expected size is dropped before it is ever re-downloaded.

    Deliberately one HEAD per file rather than a bulk `list_objects` pass
    like `filter_filelist_against_bos` uses: a batch is capped at
    BATCH_MAX_FILES (500) files, small enough that checking exactly the
    files this batch cares about costs far fewer round trips than paginating
    the whole task prefix (which can hold hundreds of thousands of objects
    for a large dataset) on every single batch.

    Returns (remaining_files, skipped_files, skipped_bytes).
    """
    from ..core.config import load_config
    from ..core.bos import bos_target, create_bos_client

    config = load_config()
    bos = create_bos_client(
        config["BAIDU_AK"], config["BAIDU_SK"], config["BOS_ENDPOINT"]
    )
    bucket, prefix = bos_target(task_input)

    def _check(f: dict) -> tuple[dict, bool]:
        key = prefix + f.get("path", "")
        try:
            meta = bos.get_object_meta_data(bucket, key)
            existing_size = int(meta.metadata.content_length)
        except Exception:
            return f, False
        return f, existing_size == f.get("size")

    remaining: list[dict] = []
    skipped_files = 0
    skipped_bytes = 0
    if files:
        with ThreadPoolExecutor(max_workers=16) as pool:
            for f, already_present in pool.map(_check, files):
                if already_present:
                    skipped_files += 1
                    skipped_bytes += f.get("size", 0)
                else:
                    remaining.append(f)
    return remaining, skipped_files, skipped_bytes


@activity.defn
async def run_pool_batch(
    task_input: TaskInput,
    batch_index: int,
    filelist_key: str,
    min_free_gb: Optional[int] = None,
    tolerate_missing: bool = False,
) -> dict:
    """Download+upload one pool batch's files to the task's flat BOS prefix.

    Self-contained — unlike a shard, no per-batch child workflow drives this
    (the pool window loop just fires one `run_pool_batch` call per batch
    slot via `workflow.start_activity` and moves on), so every step a shard
    spreads across ShardWorkerWorkflow + run_pipeline_batch lives in this
    one activity: server assignment, running/done status, disk preflight,
    the download+upload pipeline, and progress reporting.

    Batch row id: `shard_row_id(task_input.id, batch_index)` — the same
    `shards` table row T1's `/api/pool/batches/create` created; `filelist_key`
    is one of T3 `chunk_filelist`'s returned `batch_keys[i]`
    (`download-manager/batchlists/{task_input.name}/batch-{i}.json`).

    Sequence (spec T4, order is the contract):
      1. Wipe this batch's own staging dir first — an idempotent restart
         starts clean rather than trying to resume a half-downloaded local
         file a previous attempt (on this worker or another) left behind.
         BOS-side resume is HEAD-skip's job, not local disk state.
      2. POST directly to `/api/shards/assign` (server=this worker) and
         `/api/shards/status` (status=running) via `requests` — NOT the
         `assign_shard_server`/`update_shard_status` *activities*: those are
         Temporal activities themselves, and an activity cannot invoke
         another activity through Temporal's execution machinery; going
         straight to HTTP also keeps this call's behavior independent of
         theirs per G1 (their return semantics must not change on the
         sharded path's behalf). Either response coming back
         `{"ignored": true}` means an operator already stopped the parent
         task (paused/revoked/etc, see TERMINAL_STATUSES) — stop
         immediately, download nothing, return `{"ignored": True}` cleanly.
      3. Disk preflight against a coexistence floor — `min_free_gb` when the
         caller passes one (the coordinator owns the fleet-wide value), else
         `_pool_disk_floor_gb()`'s relative default — raising
         `_RetryableDiskLow` (retryable; no status write) rather than
         starting a download that cannot finish on this worker.
      4. Pull the batch manifest from BOS (`download_shard_filelist`,
         reused verbatim), apply the unconditional HEAD-skip filter, then
         hand the remainder to `PipelineEngine.run()` exactly the way
         `run_pipeline_batch` does (same heartbeat_fn/progress_fn shape).
      5. Success: clean staging, report final stats — POST
         `/api/shard-progress` with the true totals (skipped + downloaded)
         BEFORE `/api/shards/status` status=done, because `/shards/status`
         itself only reads `status`/`error` from the body and does not
         persist byte/file counts (see that route) — then return stats.
      6. Any other exception: clean staging, raise (never POST
         status=failed — G4, only the coordinator writes that terminal
         state); the raised message includes `server_key` for triage.
      7. `asyncio.CancelledError` (Temporal activity cancellation, e.g. the
         task was paused mid-batch): re-raised immediately with no cleanup
         and no status POST of any kind. `PipelineEngine.run()` already
         handles the cancellation itself (stops in-flight downloads,
         preserves staging for a possible resume) before its own re-raise —
         this mirrors `run_pipeline_batch`, which adds no CancelledError
         handling of its own either; that behavior lives in the engine.

    `tolerate_missing` (T3) relaxes step 5's all-or-nothing rule, and ONLY on
    the last attempt of the last re-dispatch round: a batch carrying at most
    POOL_BATCH_FAIL_MAX permanently-failed files, all of them already written
    to the `missing_files` archive, is judged complete instead of failing the
    task. The coordinator owns this flag — the worker cannot know whether
    another round is coming. Default False, which is byte-for-byte today's
    behaviour.

    Returns (on a real run) `{"ignored": False, downloaded_files,
    uploaded_files, uploaded_bytes, skipped_files, skipped_bytes,
    total_files, total_bytes}`; on an ignored (stopped-task) short-circuit,
    just `{"ignored": True}`.
    """
    import time
    import requests
    from .pipeline import PipelineEngine

    server_key = os.environ.get("DLM_SERVER_KEY", "")
    batch_id = shard_row_id(task_input.id, batch_index)
    staging_dir = _pool_batch_staging_dir(task_input.name, batch_index)

    shutil.rmtree(staging_dir, ignore_errors=True)
    staging_dir.mkdir(parents=True, exist_ok=True)

    def _post(path: str, payload: dict) -> dict:
        try:
            resp = requests.post(f"{_coordinator()}{path}", json=payload, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            # Bare HTTPError/timeouts carry no task context; triage from a
            # Temporal failure message needs to know which worker, batch and
            # endpoint were involved.
            raise RuntimeError(
                f"{server_key}: POST {path} failed for batch {batch_id} "
                f"of {task_input.name}: {type(e).__name__}: {e}"
            ) from e

    # Everything up to engine.run() is network-bound and can outlive a
    # heartbeat timeout on its own: two coordinator POSTs at 30s each (S1
    # wedging is a known failure mode), the manifest fetch from BOS, and up
    # to BATCH_MAX_FILES HEADs. The engine phase heartbeats via heartbeat_fn,
    # so this concurrent beater is cancelled right before handing over —
    # same pattern as filter_filelist_against_bos / chunk_filelist.
    preflight_done = False

    async def _heartbeat_while_preflight():
        while not preflight_done:
            activity.heartbeat(f"pool batch {batch_index}: preflight (assign/manifest/HEAD sweep)")
            await asyncio.sleep(30)

    heartbeat_task = asyncio.create_task(_heartbeat_while_preflight())
    try:
        assign_resp = await asyncio.to_thread(
            _post, "/api/shards/assign", {"shard_id": batch_id, "server_key": server_key}
        )
        if assign_resp.get("error"):
            raise RuntimeError(f"{server_key}: assign failed for batch {batch_id}: {assign_resp['error']}")
        if assign_resp.get("ignored"):
            return {"ignored": True}

        status_resp = await asyncio.to_thread(
            _post, "/api/shards/status", {"shard_id": batch_id, "status": "running"}
        )
        if status_resp.get("error"):
            raise RuntimeError(f"{server_key}: status=running failed for batch {batch_id}: {status_resp['error']}")
        if status_resp.get("ignored"):
            return {"ignored": True}

        floor_gb, free_gb, total_gb = await asyncio.to_thread(_pool_disk_floor_gb)
        if min_free_gb is not None:
            floor_gb = min_free_gb
        if free_gb < floor_gb:
            raise _RetryableDiskLow(
                f"{server_key}: {free_gb:.1f}GB free < {floor_gb}GB floor for pool batch "
                f"{batch_index} of {task_input.name} on a {total_gb:.0f}GB staging volume "
                f"(default floor = engine backpressure line + one batch; override with "
                f"run_pool_batch's min_free_gb)"
            )

        local_filelist = await download_shard_filelist(filelist_key, str(staging_dir))
        raw_files = json.loads(Path(local_filelist).read_text())

        remaining, skipped_files, skipped_bytes = await asyncio.to_thread(
            _head_skip_filter, raw_files, task_input
        )
    except asyncio.CancelledError:
        raise
    except (_RetryableDiskLow, RuntimeError):
        # Already carries its own context (disk floor detail, or _post's
        # server_key/endpoint wrapper) — re-raise untouched.
        raise
    except Exception as e:
        # A BOS outage on the manifest fetch, a corrupt manifest, or a config
        # failure inside the HEAD sweep would otherwise propagate raw: same
        # server_key context and staging cleanup the post-engine handler gives.
        shutil.rmtree(staging_dir, ignore_errors=True)
        raise RuntimeError(
            f"{server_key}: pool batch {batch_index} of {task_input.name} "
            f"preflight failed: {type(e).__name__}: {e}"
        ) from e
    finally:
        preflight_done = True
        heartbeat_task.cancel()

    try:
        activity.heartbeat(
            f"pool batch {batch_index}: {skipped_files} already on BOS, "
            f"{len(remaining)} to download"
        )

        files = [FileInfo(path=f["path"], size=f["size"]) for f in remaining]

        def heartbeat_fn(msg: str):
            activity.heartbeat(msg)

        last_report_time = [0.0]

        def progress_fn(downloaded_bytes: int, total_bytes: int, speed_bps: float):
            """Report cumulative batch progress to S1 every 15s.

            Wrapped in try/except so a coordinator hiccup never breaks the
            engine's `_speed_reporter` loop — same self-protection
            `run_pipeline_batch`'s own progress_fn applies (spec item 7).
            """
            now = time.time()
            if now - last_report_time[0] < 15:
                return
            last_report_time[0] = now
            try:
                speed_mbps = speed_bps * 8 / 1_000_000
                requests.post(
                    f"{_coordinator()}/api/shard-progress",
                    json={
                        "shard_id": batch_id,
                        # done_files is a coarse mid-run signal (skipped files
                        # already known-complete; in-flight files aren't
                        # individually tracked here) — done_bytes is what the
                        # task-level aggregate actually sums, and it's exact.
                        "done_files": skipped_files,
                        "done_bytes": skipped_bytes + downloaded_bytes,
                        "speed_mbps": round(speed_mbps, 1),
                    },
                    timeout=5,
                )
            except Exception as e:
                logger.debug(f"Pool batch progress report failed: {e}")

        engine = PipelineEngine(task_input, staging_dir, heartbeat_fn, progress_fn)
        stats = await engine.run(files)

        if stats.failed_files > 0:
            # (1) Archive first, unconditionally — decoupled from the tolerance
            #     decision on purpose. Putting this inside the "we are about to
            #     forgive it" branch would mean the two cases that most need
            #     evidence — a first-attempt failure, and a batch over the
            #     ceiling that ends up failing the whole task — leave no record
            #     at all. "Which files are missing" is the only useful thing to
            #     know about a task that ends `failed`.
            archivable = [d for d in stats.failed_details
                          if d.get("reason") in ARCHIVABLE_FAIL_REASONS]
            recorded = await asyncio.to_thread(
                _report_missing_files, task_input.id, batch_index, server_key,
                archivable,
            )

            # (2) Then decide. Four conditions, each closing a different hole:
            #
            #   tolerate_missing  — the coordinator's call, not the worker's:
            #       it is only true on the final re-dispatch round, so a
            #       first-round batch still fails and still gets its second
            #       round on (probably) a different worker. A poisoned worker
            #       is exactly what that round cures, and forgiving early
            #       throws it away.
            #   final_attempt     — same argument one level down, across this
            #       round's own retries.
            #   fully archivable  — every failure is a file-level fact we just
            #       wrote down. Cancelled uploads and missing staged files are
            #       deliberately NOT archivable (they are orchestration and
            #       local disk state), so a batch whose failures are those
            #       cannot be forgiven: forgiving it would drop files with no
            #       record anywhere, which is worse than failing loudly.
            #   <= ceiling        — the tolerance must not degenerate into
            #       "never fails". A systemic fault takes out far more than
            #       POOL_BATCH_FAIL_MAX files and still raises.
            final_attempt = activity.info().attempt >= POOL_BATCH_MAX_ATTEMPTS
            fully_archivable = len(archivable) == stats.failed_files
            tolerable = (
                tolerate_missing
                and final_attempt
                and recorded
                and fully_archivable
                and stats.failed_files <= POOL_BATCH_FAIL_MAX
            )

            if not tolerable:
                # Message names which condition held it back — from a Temporal
                # failure list, "incomplete: 3/500 failed" alone cannot
                # distinguish "will retry" from "over the ceiling, task is
                # doomed", and those call for opposite responses.
                raise RuntimeError(
                    f"{server_key}: pool batch {batch_index} of {task_input.name} "
                    f"incomplete: {stats.failed_files}/{stats.total_files} files "
                    f"failed (downloaded={stats.downloaded_files}, "
                    f"uploaded={stats.uploaded_files}); not tolerated "
                    f"[tolerate_missing={tolerate_missing}, "
                    f"attempt={activity.info().attempt}/{POOL_BATCH_MAX_ATTEMPTS}, "
                    f"archived={recorded}, "
                    f"archivable={len(archivable)}/{stats.failed_files}, "
                    f"ceiling={POOL_BATCH_FAIL_MAX}]"
                )

            # Forgiven: the batch is judged complete and the coordinator moves
            # on. Every file we gave up on is in `missing_files` and shows up
            # in the task's alert and its /missing-files listing — the batch is
            # passed, the loss is not hidden.
            logger.warning(
                "Pool batch %s of %s tolerated with %d permanently-failed "
                "file(s) on attempt %d/%d (archived; reasons: %s)",
                batch_index, task_input.name, stats.failed_files,
                activity.info().attempt, POOL_BATCH_MAX_ATTEMPTS,
                sorted({d.get("reason") for d in archivable}),
            )
    except asyncio.CancelledError:
        raise
    except Exception as e:
        shutil.rmtree(staging_dir, ignore_errors=True)
        raise RuntimeError(
            f"{server_key}: pool batch {batch_index} of {task_input.name} "
            f"failed: {e}"
        ) from e

    shutil.rmtree(staging_dir, ignore_errors=True)

    total_files = skipped_files + stats.uploaded_files
    total_bytes = skipped_bytes + stats.uploaded_bytes

    final_resp = await asyncio.to_thread(
        _post, "/api/shard-progress",
        {"shard_id": batch_id, "done_files": total_files,
         "done_bytes": total_bytes, "speed_mbps": 0},
    )
    if final_resp.get("error"):
        raise RuntimeError(
            f"{server_key}: final progress report failed for batch {batch_id}: "
            f"{final_resp['error']}"
        )

    done_resp = await asyncio.to_thread(
        _post, "/api/shards/status", {"shard_id": batch_id, "status": "done"}
    )
    if done_resp.get("error"):
        raise RuntimeError(
            f"{server_key}: status=done failed for batch {batch_id}: "
            f"{done_resp['error']}"
        )

    return {
        "ignored": False,
        "downloaded_files": stats.downloaded_files,
        "uploaded_files": stats.uploaded_files,
        "uploaded_bytes": stats.uploaded_bytes,
        "skipped_files": skipped_files,
        "skipped_bytes": skipped_bytes,
        "total_files": total_files,
        "total_bytes": total_bytes,
    }


# ── Pool coordinator bookkeeping (T5) ───────────────────────────────


@activity.defn
async def create_pool_batches_in_db(task_id: str, batch_infos: list[dict]) -> dict:
    """Create this task's batch rows via T1's idempotent pool endpoint.

    Returns `{"ignored": bool, "shard_ids": [...]}`. `ignored` means an
    operator stopped the task (or it no longer exists) — that response
    carries no `shard_ids`, so the coordinator must check it before reading
    them. Blind-indexing `data["shard_ids"]` the way `create_shards_in_db`
    does would KeyError here and burn every retry against a task that was
    deliberately paused.

    A mismatch (rows on file whose chunking differs from what we just
    computed) is raised non-retryable: the endpoint deliberately does not
    delete rows that in-flight batches may already be reporting against, so
    retrying re-sends the identical body and fails identically. Recovery is
    an operator action — reshard the task, which requeues it and clears the
    old rows.
    """
    import requests
    resp = requests.post(
        f"{_coordinator()}/api/pool/batches/create",
        json={
            "task_id": task_id,
            "shard_infos": batch_infos,
            "expected_count": len(batch_infos),
        },
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("ignored"):
        return {"ignored": True, "shard_ids": []}
    if data.get("error"):
        raise ApplicationError(
            f"pool batch rows for {task_id} do not match this chunking "
            f"({data['error']}); reshard the task to clear them",
            type="PoolBatchMismatch",
            non_retryable=True,
        )
    return {"ignored": False, "shard_ids": data["shard_ids"]}


@activity.defn
async def record_batches_and_window(task_id: str, results: list[dict]) -> dict:
    """Write finished batches' terminal rows, then read the new window size.

    One activity per coordinator wake, not one per batch: the window loop
    wakes on every batch completion, and separate bookkeeping activities
    would roughly double each batch's history footprint (~12 events vs ~6),
    putting a 1,500-batch task near Temporal's 10k-event warning line.

    `results` is `[{"batch_index": int, "shard_id": str, "status":
    "done"|"failed", "error": str|None}, ...]` — only batches that finished
    since the last wake.

    `failed` is written only from here, i.e. only by the coordinator (G4:
    `run_pool_batch` raises rather than marking itself failed). `done` is NOT
    coordinator-exclusive — a successful `run_pool_batch` already posts its
    own `done` before returning, so this write is a redundant confirmation of
    a row that is already terminal. Both are idempotent on the same row and
    value; a row whose parent task is already terminal is skipped by the
    endpoint's guard, so a late wake cannot resurrect a stopped task.

    Returns `{"window": int, "p": int, ...}` — the window this task may keep
    in flight until the next wake.
    """
    import requests

    for r in sorted(results, key=lambda r: r.get("batch_index", 0)):
        payload = {"shard_id": r["shard_id"], "status": r["status"]}
        if r.get("error"):
            payload["error"] = str(r["error"])[:500]
        resp = requests.post(
            f"{_coordinator()}/api/shards/status", json=payload, timeout=30
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("error"):
            raise RuntimeError(
                f"recording batch {r.get('batch_index')} of {task_id} "
                f"as {r['status']} failed: {data['error']}"
            )
        activity.heartbeat(f"recorded batch {r.get('batch_index')} as {r['status']}")

    resp = requests.get(
        f"{_coordinator()}/api/pool/window", params={"task_id": task_id}, timeout=30
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("error"):
        raise RuntimeError(f"window query for {task_id} failed: {data['error']}")
    return data


@activity.defn
async def release_pool_batches(task_id: str) -> int:
    """Release this task's non-done batch rows back to pending.

    Called from the coordinator's cancellation path (shielded) so a paused or
    terminated task leaves no row claiming a worker that has stopped working
    on it. Returns how many rows were released.
    """
    import requests
    resp = requests.post(
        f"{_coordinator()}/api/pool/batches/release",
        json={"task_id": task_id},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("error"):
        raise RuntimeError(f"releasing batches of {task_id} failed: {data['error']}")
    return int(data.get("released", 0))


@activity.defn
async def verify_missing_files(
    task_input: TaskInput, limit: int, recheck: bool = True
) -> int:
    """Re-check this task's archived missing files against BOS; return what is
    still missing, and record the ceiling the coordinator will judge that by.

    Called from the pool coordinator's finalize step, before it decides between
    `done` and `failed`. BOS is the only truth about what landed, and the
    archive is deliberately written optimistically — every failed attempt
    upserts a row, including attempts whose file a LATER batch or round then
    uploaded successfully. Without this pass a task could report `failed` over
    files that are sitting in the bucket.

    `recheck=False` records the ceiling and returns the count WITHOUT touching
    BOS. The coordinator passes it when the verdict is already `failed` on
    batch failures (review GAP-1): that task needs the count for its error
    message and the ceiling for alerting, but a BOS scan cannot change its
    outcome, and the archive of a task that failed systemically is exactly the
    one large enough to hurt.

    Fail-open toward "still missing", in every direction:
      * A row is cleared ONLY if BOS has that key with the exact recorded
        size. Existence alone would accept a truncated object left by an
        interrupted upload as proof of delivery — the one way this activity
        could erase a real missing-file record. Both resume filters compare
        key + size for the same reason.
      * A row whose recorded `size_bytes` is <= 0 cannot be size-checked, so
        it is kept.
      * A HEAD that raises keeps its row.
      * A dataset task with no `category` is skipped wholesale: `bos_target`
        silently drops a path segment for a falsy category (bos.py:29), so
        every HEAD would run against a prefix that is probably not this
        task's. (Model tasks are unaffected — their prefix is `{name}/` and
        does not involve `category` at all.)
      * An archive larger than MISSING_VERIFY_MAX is not scanned at all (see
        that constant): the rows are kept, so the task fails on its ceiling
        with every path still queryable.

    Returns the task's remaining missing-file count as SQLite sees it, so the
    coordinator's verdict and the row the dashboard shows cannot disagree.
    """
    import requests
    from ..core.bos import bos_target, create_bos_client
    from ..core.config import load_config
    from .models import MISSING_VERIFY_CHUNK, MISSING_VERIFY_MAX

    base = _coordinator()
    task_id = task_input.id

    def _record_limit() -> int:
        r = requests.post(
            f"{base}/api/tasks/{task_id}/missing-limit",
            json={"limit": int(limit)},
            timeout=30,
        )
        r.raise_for_status()
        body = r.json()
        if body.get("error"):
            raise RuntimeError(
                f"recording missing-file limit for {task_id} failed: {body['error']}"
            )
        return int(body.get("missing_files_count", 0))

    if not recheck:
        return await asyncio.to_thread(_record_limit)

    # One more than the cap, so an over-cap archive is detectable without
    # transferring (or JSON-serialising on S1) the whole thing. The endpoint
    # returns everything when no limit is given — that is for operators.
    resp = await asyncio.to_thread(
        lambda: requests.get(
            f"{base}/api/tasks/{task_id}/missing-files",
            params={"limit": MISSING_VERIFY_MAX + 1},
            timeout=60,
        )
    )
    resp.raise_for_status()
    listing = resp.json()
    rows = listing.get("files") or []

    if len(rows) > MISSING_VERIFY_MAX:
        logger.error(
            "Skipping missing-file re-check for %s: %d+ archived row(s) exceeds "
            "the %d-row scan cap, so every row is kept and this task will fail "
            "on its ceiling. An archive this large is a systemic fault, not a "
            "few dead upstream files — GET /api/tasks/%s/missing-files and look "
            "for one shared source, worker, or credential.",
            task_id, len(rows), MISSING_VERIFY_MAX, task_id,
        )
        return await asyncio.to_thread(_record_limit)

    checkable = bool(rows) and (
        task_input.type == "model" or bool(task_input.category)
    )
    if not checkable:
        if rows:
            logger.warning(
                "Skipping missing-file re-check for %s: dataset task with no "
                "category, so its BOS prefix cannot be computed safely; "
                "keeping all %d row(s)",
                task_id, len(rows),
            )
        return await asyncio.to_thread(_record_limit)

    state: dict = {}

    def _setup():
        config = load_config()
        state["bos"] = create_bos_client(
            config["BAIDU_AK"], config["BAIDU_SK"], config["BOS_ENDPOINT"]
        )
        state["bucket"], state["prefix"] = bos_target(task_input)

    def _verify_chunk(chunk: list[dict]) -> list[str]:
        bos = state["bos"]
        bucket, prefix = state["bucket"], state["prefix"]

        def _present(row: dict) -> tuple[str, bool]:
            path = row.get("file_path") or ""
            expected = int(row.get("size_bytes") or 0)
            if not path or expected <= 0:
                return path, False
            try:
                meta = bos.get_object_meta_data(bucket, prefix + path)
                return path, int(meta.metadata.content_length) == expected
            except Exception:
                return path, False

        found: list[str] = []
        with ThreadPoolExecutor(max_workers=16) as pool:
            for path, ok in pool.map(_present, chunk):
                if ok:
                    found.append(path)
        return found

    await asyncio.to_thread(_setup)

    # Heartbeat per chunk, not once before the whole scan: the activity's
    # heartbeat_timeout is 5 minutes and a large archive takes longer than that,
    # so a single up-front heartbeat guaranteed a timeout-and-retry loop over
    # the same rows (review GAP-1).
    present: list[str] = []
    for start in range(0, len(rows), MISSING_VERIFY_CHUNK):
        activity.heartbeat(
            f"re-checking missing files of {task_id}: {start}/{len(rows)}"
        )
        present.extend(
            await asyncio.to_thread(
                _verify_chunk, rows[start:start + MISSING_VERIFY_CHUNK]
            )
        )

    if present:
        logger.info(
            "Missing-file re-check for %s: %d/%d row(s) are on BOS after all",
            task_id, len(present), len(rows),
        )

        def _clear() -> int:
            r = requests.delete(
                f"{base}/api/tasks/{task_id}/missing-files",
                json={"paths": present},
                timeout=60,
            )
            r.raise_for_status()
            body = r.json()
            if body.get("error"):
                raise RuntimeError(
                    f"clearing verified files of {task_id} failed: {body['error']}"
                )
            return int(body.get("remaining", 0))

        await asyncio.to_thread(_clear)

    return await asyncio.to_thread(_record_limit)

