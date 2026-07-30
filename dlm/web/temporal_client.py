"""Temporal client singleton for the web server."""

import asyncio
import logging
import os
from typing import Optional

from temporalio.client import Client

logger = logging.getLogger("dlm.web")

_client: Optional[Client] = None
_client_loop: Optional[asyncio.AbstractEventLoop] = None


async def get_client() -> Client:
    """Get or create the Temporal client connection.

    Creates a new client if the event loop has changed (e.g., called from
    a different context than where the client was first created).
    """
    global _client, _client_loop
    current_loop = asyncio.get_running_loop()

    if _client is None or _client_loop is not current_loop:
        host = os.environ.get("TEMPORAL_HOST", "localhost:7233")
        logger.info(f"Connecting to Temporal at {host}...")
        _client = await Client.connect(host)
        _client_loop = current_loop

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

    workflow_id = f"dl-{task_dict['id']}"
    handle = await client.start_workflow(
        DownloadDatasetWorkflow.run,
        args=[task_input],
        id=workflow_id,
        task_queue=task_queue,
    )
    logger.info(f"Started workflow {workflow_id} on queue {task_queue}")
    return handle


async def start_split_download(task_dict: dict, worker_count: int = 2):
    """Start a SplitDownloadWorkflow for a large dataset (legacy — kept for bj1-4)."""
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


async def start_sharded_download(task_dict: dict):
    """Start a ShardedDownloadWorkflow — auto-sharding coordinator."""
    from ..temporal.models import TaskInput
    from ..temporal.workflows import ShardedDownloadWorkflow

    client = await get_client()
    task_input = TaskInput(
        id=task_dict["id"],
        name=task_dict.get("name", ""),
        repo_id=task_dict.get("repo_id", ""),
        source=task_dict.get("source", "hf"),
        type=task_dict.get("type", "dataset"),
        category=task_dict.get("category", ""),
        priority=task_dict.get("priority", 5),
        size_gb=task_dict.get("size_gb", 0),
        shard_count=int(task_dict.get("max_workers") or 0),
    )

    workflow_id = f"sharded-{task_dict['id']}"
    handle = await client.start_workflow(
        ShardedDownloadWorkflow.run,
        task_input,
        id=workflow_id,
        task_queue="download-workers",
    )
    logger.info(f"Started sharded workflow {workflow_id}")
    return handle


async def cancel_workflow(task_id: str):
    """Cancel running workflow(s) for a task — handles all ID patterns."""
    client = await get_client()
    patterns = [
        f"dl-{task_id}",
        f"split-download-{task_id}",
        f"sharded-{task_id}",
    ]
    for wf_id in patterns:
        try:
            handle = client.get_workflow_handle(wf_id)
            await handle.cancel()
            logger.info(f"Cancelled workflow {wf_id}")
        except Exception:
            pass

    # Also cancel shard child workflows
    try:
        from ..queue.snapshot import get_shards_by_task, init_db
        init_db()
        shards = get_shards_by_task(task_id)
        for shard in shards:
            try:
                handle = client.get_workflow_handle(f"shard-{shard['id']}")
                await handle.cancel()
            except Exception:
                pass
    except Exception:
        pass


async def terminate_workflow_and_wait(task_id: str, timeout_s: int = 120) -> bool:
    """Terminate all workflows for a task and wait until they are closed.

    Unlike cancel_workflow (async cancel, returns immediately), this blocks
    until Temporal reports every handle closed — required before requeuing a
    task under the same workflow ID (e.g. /queue/reshard). Returns True when
    everything closed within the timeout.
    """
    import time as _time
    from temporalio.client import WorkflowExecutionStatus

    client = await get_client()
    handles = []
    for wf_id in (f"dl-{task_id}", f"split-download-{task_id}", f"sharded-{task_id}"):
        handles.append(client.get_workflow_handle(wf_id))
    try:
        from ..queue.snapshot import get_shards_by_task, init_db
        init_db()
        for shard in get_shards_by_task(task_id):
            handles.append(client.get_workflow_handle(f"shard-{shard['id']}"))
    except Exception:
        pass

    for handle in handles:
        try:
            await handle.terminate(reason=f"reshard/requeue of {task_id}")
        except Exception:
            pass  # not found / already closed

    deadline = _time.monotonic() + timeout_s
    open_statuses = {WorkflowExecutionStatus.RUNNING}
    while _time.monotonic() < deadline:
        still_open = 0
        for handle in handles:
            try:
                desc = await handle.describe()
                if desc.status in open_statuses:
                    still_open += 1
            except Exception:
                pass  # not found = closed
        if still_open == 0:
            return True
        await asyncio.sleep(2)
    return False


async def list_running_workflows() -> list:
    """List all running download workflows (all types)."""
    client = await get_client()
    workflows = []
    for wf_type in [
        "DownloadDatasetWorkflow",
        "SplitDownloadWorkflow",
        "ShardedDownloadWorkflow",
        "ShardWorkerWorkflow",
    ]:
        try:
            async for wf in client.list_workflows(
                f'WorkflowType="{wf_type}" AND ExecutionStatus="Running"'
            ):
                workflows.append({
                    "workflow_id": wf.id,
                    "workflow_type": wf_type,
                    "status": wf.status.name,
                    "start_time": str(wf.start_time) if wf.start_time else None,
                })
        except Exception:
            pass
    return workflows
