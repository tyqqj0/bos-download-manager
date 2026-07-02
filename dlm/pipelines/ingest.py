"""Ingest pipeline — download → transfer chain.

Usage:
    from dlm.pipelines.ingest import ingest
    result = ingest({"id": "t-001", "name": "my-dataset", "repo_id": "org/repo", ...})
"""

from celery import chain

from ..worker.download import download_dataset
from ..transfer.tasks import transfer_to_juicefs


def ingest(task_meta: dict, priority: int = 5):
    """Create and dispatch a full ingest pipeline: download → transfer.

    Args:
        task_meta: Task metadata dict.
        priority: Queue priority (0=highest, 9=lowest).

    Returns:
        AsyncResult for the chain.
    """
    return chain(
        download_dataset.s(task_meta),
        transfer_to_juicefs.s(),
    ).apply_async(priority=priority, task_id=task_meta["id"])


def download_only(task_meta: dict, priority: int = 5):
    """Dispatch download without transfer."""
    return download_dataset.apply_async(
        args=[task_meta], priority=priority, task_id=task_meta["id"],
    )
