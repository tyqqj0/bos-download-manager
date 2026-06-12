"""Size checking — parallel BOS API queries + HuggingFace total sizes."""

import click
from concurrent.futures import ThreadPoolExecutor, as_completed

from .bos import get_prefix_size
from ..constants import DATA_BUCKET


def fetch_sizes(bos_client, tasks, max_workers=10, verbose=False) -> dict[str, float]:
    """Query BOS for actual downloaded size of each task (parallel).

    Only checks tasks with status in (downloading, dispatched, done).
    Returns {task_id: downloaded_gb}.
    """
    eligible = [
        t for t in tasks
        if t.status in ("downloading", "dispatched", "done") and t.bos_path
    ]

    if not eligible:
        return {}

    results = {}
    errors = []

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(_get_task_size, bos_client, t.bos_path): t
            for t in eligible
        }
        for future in as_completed(futures):
            task = futures[future]
            try:
                size_bytes = future.result()
                if size_bytes > 0:
                    results[task.id] = round(size_bytes / (1024 ** 3), 2)
                elif verbose:
                    errors.append(f"  {task.name}: BOS 前缀 '{task.bos_path}' 下无文件")
            except Exception as e:
                errors.append(f"  {task.name}: {type(e).__name__}: {str(e)[:60]}")

    if verbose and errors:
        click.echo(f"\n⚠ {len(errors)} 个任务大小查询失败:")
        for err in errors[:10]:
            click.echo(err)

    return results


def fetch_hf_total_sizes(tasks, hf_token=None, max_workers=6) -> dict[str, float]:
    """Query HuggingFace API for total repo sizes (parallel).

    Only checks HF-source tasks. Returns {task_id: total_gb}.
    """
    eligible = [t for t in tasks if t.source == "hf" and t.repo_id]

    if not eligible:
        return {}

    results = {}

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(_get_hf_repo_size, t.repo_id, t.type, hf_token): t.id
            for t in eligible
        }
        for future in as_completed(futures):
            task_id = futures[future]
            try:
                size_bytes = future.result()
                if size_bytes > 0:
                    results[task_id] = round(size_bytes / (1024 ** 3), 2)
            except Exception:
                pass

    return results


def _get_task_size(bos_client, bos_path: str) -> int:
    """Get total bytes for a single task's BOS prefix."""
    prefix = bos_path.rstrip("/") + "/"
    return get_prefix_size(bos_client, DATA_BUCKET, prefix)


def _get_hf_repo_size(repo_id: str, repo_type: str, token: str = None) -> int:
    """Get total size of a HuggingFace repo via list_repo_tree."""
    from huggingface_hub import HfApi

    api = HfApi(token=token)
    hf_type = "dataset" if repo_type == "dataset" else "model"
    total = 0
    try:
        for item in api.list_repo_tree(repo_id, repo_type=hf_type, recursive=True):
            if hasattr(item, "size") and item.size:
                total += item.size
    except Exception:
        pass
    return total
