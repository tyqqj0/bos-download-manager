"""Download strategies — picks aria2c or hf_download based on file characteristics."""

from __future__ import annotations

import asyncio
import logging
import os
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
