"""Size checking — parallel BOS API queries for actual download sizes."""

from concurrent.futures import ThreadPoolExecutor, as_completed

from .bos import get_prefix_size
from ..constants import DATA_BUCKET


def fetch_sizes(bos_client, tasks, max_workers=10) -> dict[str, float]:
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

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(_get_task_size, bos_client, t.bos_path): t.id
            for t in eligible
        }
        for future in as_completed(futures):
            task_id = futures[future]
            try:
                size_bytes = future.result()
                results[task_id] = round(size_bytes / (1024 ** 3), 2)
            except Exception:
                pass

    return results


def _get_task_size(bos_client, bos_path: str) -> int:
    """Get total bytes for a single task's BOS prefix."""
    prefix = bos_path.rstrip("/") + "/"
    return get_prefix_size(bos_client, DATA_BUCKET, prefix)
