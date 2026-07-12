"""Temporal client singleton for the web server."""

import asyncio
import logging
import os
from typing import Optional

from temporalio.client import Client

logger = logging.getLogger("dlm.web")

_client: Optional[Client] = None


async def get_client() -> Client:
    """Get or create the Temporal client connection."""
    global _client
    if _client is None:
        host = os.environ.get("TEMPORAL_HOST", "154.85.43.52:7233")
        logger.info(f"Connecting to Temporal at {host}...")
        _client = await Client.connect(host)
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
    """Start a SplitDownloadWorkflow for a large dataset."""
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


async def cancel_workflow(task_id: str):
    """Cancel a running workflow."""
    client = await get_client()
    handle = client.get_workflow_handle(f"dl-{task_id}")
    try:
        await handle.cancel()
    except Exception as e:
        logger.warning(f"Cancel failed for {task_id}: {e}")


async def list_running_workflows() -> list:
    """List all running download workflows."""
    client = await get_client()
    workflows = []
    async for wf in client.list_workflows('WorkflowType="DownloadDatasetWorkflow" AND ExecutionStatus="Running"'):
        workflows.append({
            "workflow_id": wf.id,
            "status": wf.status.name,
            "start_time": str(wf.start_time) if wf.start_time else None,
        })
    return workflows
