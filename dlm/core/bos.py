"""BOS client wrapper for DLM."""

import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from baidubce.bce_client_configuration import BceClientConfiguration
from baidubce.auth.bce_credentials import BceCredentials
from baidubce.services.bos.bos_client import BosClient


MULTIPART_THRESHOLD = 100 * 1024 * 1024  # 100MB


def create_bos_client(ak: str, sk: str, endpoint: str) -> BosClient:
    config = BceClientConfiguration(
        credentials=BceCredentials(ak, sk),
        endpoint=endpoint,
    )
    return BosClient(config)


def list_prefixes(client: BosClient, bucket: str, prefix: str = "", delimiter: str = "/"):
    """List sub-directories and files directly under a prefix.

    Returns (dirs, files) where dirs are prefix strings and files are (key, size) tuples.
    """
    dirs = []
    files = []
    marker = ""
    while True:
        response = client.list_objects(
            bucket, prefix=prefix, delimiter=delimiter, marker=marker, max_keys=1000
        )
        if hasattr(response, "common_prefixes") and response.common_prefixes:
            for p in response.common_prefixes:
                dirs.append(p.prefix)
        if response.contents:
            for obj in response.contents:
                if obj.key != prefix:
                    files.append((obj.key, obj.size))
        if not response.is_truncated:
            break
        marker = response.next_marker
    return dirs, files


def get_prefix_size(client: BosClient, bucket: str, prefix: str) -> int:
    """Sum total bytes of all objects under a prefix (paginated)."""
    total = 0
    marker = ""
    while True:
        response = client.list_objects(bucket, prefix=prefix, marker=marker, max_keys=1000)
        if response.contents:
            for obj in response.contents:
                total += obj.size
        if not response.is_truncated:
            break
        marker = response.next_marker
    return total


def upload_file(client: BosClient, bucket: str, key: str, local_path: str):
    """Upload a single file to BOS. Uses multipart for large files."""
    size = os.path.getsize(local_path)
    if size > MULTIPART_THRESHOLD:
        client.put_super_object_from_file(bucket, key, local_path)
    else:
        client.put_object_from_file(bucket, key, local_path)


def upload_directory(
    client: BosClient,
    bucket: str,
    prefix: str,
    local_dir: str,
    workers: int = 8,
    exclude: tuple = (".incomplete", ".huggingface", ".cache"),
    progress_callback=None,
):
    """Upload all files in a directory to BOS in parallel.

    Args:
        client: BOS client
        bucket: Target bucket name
        prefix: Key prefix in bucket (e.g. "manipulation/DROID/")
        local_dir: Local directory to upload
        workers: Number of parallel upload threads
        exclude: Filename/dirname patterns to skip
        progress_callback: Called with (bytes_uploaded, total_bytes) after each file

    Returns:
        (files_uploaded, bytes_uploaded)
    """
    local_path = Path(local_dir)
    files = []
    for f in local_path.rglob("*"):
        if not f.is_file():
            continue
        if any(exc in f.parts for exc in exclude):
            continue
        if any(f.name.endswith(exc) for exc in exclude):
            continue
        files.append(f)

    total_bytes = sum(f.stat().st_size for f in files)
    uploaded_bytes = 0
    uploaded_count = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {}
        for f in files:
            rel = f.relative_to(local_path)
            key = prefix.rstrip("/") + "/" + rel.as_posix()
            futures[pool.submit(upload_file, client, bucket, key, str(f))] = f

        for future in as_completed(futures):
            f = futures[future]
            future.result()
            size = f.stat().st_size
            uploaded_bytes += size
            uploaded_count += 1
            f.unlink()
            if progress_callback:
                progress_callback(uploaded_bytes, total_bytes)

    # Clean up empty directories
    for d in sorted(local_path.rglob("*"), reverse=True):
        if d.is_dir():
            try:
                d.rmdir()
            except OSError:
                pass

    return uploaded_count, uploaded_bytes
