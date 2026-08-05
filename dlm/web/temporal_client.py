"""Temporal client singleton for the web server.

Every call out to Temporal from here runs on the uvicorn event loop, so
every one of them needs a deadline. An await that never returns is not a
slow request — it stops the loop iteration it belongs to. The scheduler
loop reached through this module drives auto-dispatch, reconcile and
health verification, and a hang there leaves a process that answers HTTP
normally while nothing is dispatched or reconciled ever again.
"""

import asyncio
import logging
import os
from datetime import timedelta
from typing import Optional

from temporalio.client import Client

logger = logging.getLogger("dlm.web")

_client: Optional[Client] = None
_client_loop: Optional[asyncio.AbstractEventLoop] = None

# The download workflow types, in one place. Six sites used to inline this
# same list, and each was a place a new workflow type could be forgotten.
# `PoolDownloadWorkflow` is listed ahead of its class existing (T5 adds it) —
# tests/test_workflow_registry.py asserts the defn classes in workflows.py
# are a *subset* of this tuple, so listing it early is safe.
WORKFLOW_TYPES = (
    "DownloadDatasetWorkflow",
    "SplitDownloadWorkflow",
    "ShardedDownloadWorkflow",
    "ShardWorkerWorkflow",
    "PoolDownloadWorkflow",
)

# The coordinator/parent types — what fleet-wide cancel operates on. Children
# (ShardWorkerWorkflow) are NOT cancelled directly: they die with their parent
# via ParentClosePolicy=TERMINATE, which skips the child's failure handler.
# Cancelling a child directly runs that handler, the shard reports "failed",
# the coordinator marks the whole task `failed` — a terminal status the
# reconciler never re-dispatches. Parent-only cancel leaves the task in
# `downloading`, where orphan re-dispatch reclaims it losslessly.
PARENT_WORKFLOW_TYPES = tuple(t for t in WORKFLOW_TYPES if t != "ShardWorkerWorkflow")

# Workflow-ID prefix for each type — the second half of the registry. Three
# call sites (fleet.has_live_workflow, cancel_workflow,
# terminate_workflow_and_wait) used to hand-copy these same prefix literals,
# and each copy could drift independently (that history is why this table
# exists instead of a fourth copy). ShardWorkerWorkflow's IDs are built from
# a *shard* id, not a task id (`shard-` + naming.shard_row_id(task_id, idx)
# = f"shard-s-{task_id}-{idx}") — callers that need the task-id-keyed IDs
# should iterate PARENT_WORKFLOW_TYPES, not WORKFLOW_TYPES, against this map.
WORKFLOW_ID_PREFIXES = {
    "DownloadDatasetWorkflow": "dl-",
    "SplitDownloadWorkflow": "split-download-",
    "ShardedDownloadWorkflow": "sharded-",
    "ShardWorkerWorkflow": "shard-",
    "PoolDownloadWorkflow": "pool-",
}

# Named aliases for the two prefixes fleet.has_live_workflow needs outside a
# type-list iteration (the legacy single-node ID and the shard-child prefix).
# Importing these by name — instead of indexing WORKFLOW_ID_PREFIXES with a
# literal type-name string — keeps fleet.py free of workflow-type literals,
# which test_event_loop_safety.py's AST scan treats as a re-inlined copy of
# WORKFLOW_TYPES.
LEGACY_DOWNLOAD_ID_PREFIX = WORKFLOW_ID_PREFIXES["DownloadDatasetWorkflow"]
SHARD_WORKER_ID_PREFIX = WORKFLOW_ID_PREFIXES["ShardWorkerWorkflow"]

CONNECT_TIMEOUT = 5  # seconds to reach the frontend; connect() has no deadline
QUERY_TIMEOUT = timedelta(seconds=15)  # per list/describe RPC


async def get_client() -> Client:
    """Get or create the Temporal client connection.

    Creates a new client if the event loop has changed (e.g., called from
    a different context than where the client was first created).

    Prefer `connected_client()` — `Client.connect` takes no deadline of its
    own, so if the frontend completes the TCP handshake and then goes quiet
    this await never returns.
    """
    global _client, _client_loop
    current_loop = asyncio.get_running_loop()

    if _client is None or _client_loop is not current_loop:
        host = os.environ.get("TEMPORAL_HOST", "localhost:7233")
        logger.info(f"Connecting to Temporal at {host}...")
        _client = await Client.connect(host)
        _client_loop = current_loop

    return _client


async def connected_client(timeout: float = CONNECT_TIMEOUT) -> Client:
    """`get_client()` that cannot hang forever."""
    return await asyncio.wait_for(get_client(), timeout=timeout)


async def running_workflows(client: Optional[Client] = None) -> dict:
    """Every RUNNING download workflow, as `{workflow_id: task_queue}`.

    The callers want three different views of this — a set of ids, the set
    of busy task queues, and the id→queue mapping — so it returns the
    mapping and lets them narrow it. Every scan carries an rpc_timeout;
    none of the six hand-rolled copies did.
    """
    client = client or await connected_client()
    found: dict[str, str] = {}
    for wf_type in WORKFLOW_TYPES:
        async for wf in client.list_workflows(
            f'WorkflowType="{wf_type}" AND ExecutionStatus="Running"',
            rpc_timeout=QUERY_TIMEOUT,
        ):
            found[wf.id] = wf.task_queue
    return found


async def start_download(task_dict: dict, task_queue: str = "download-workers"):
    """Start a DownloadDatasetWorkflow for a task."""
    from ..temporal.models import TaskInput
    from ..temporal.workflows import DownloadDatasetWorkflow

    client = await connected_client()
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

    workflow_id = f"{WORKFLOW_ID_PREFIXES['DownloadDatasetWorkflow']}{task_dict['id']}"
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

    client = await connected_client()
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
        id=f"{WORKFLOW_ID_PREFIXES['SplitDownloadWorkflow']}{task_dict['id']}",
        task_queue="download-workers",
    )
    logger.info(f"Started split workflow for {task_dict['name']} ({worker_count} workers)")
    return handle


async def start_sharded_download(task_dict: dict):
    """Start a ShardedDownloadWorkflow — auto-sharding coordinator."""
    from ..temporal.models import TaskInput
    from ..temporal.workflows import ShardedDownloadWorkflow

    client = await connected_client()
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

    workflow_id = f"{WORKFLOW_ID_PREFIXES['ShardedDownloadWorkflow']}{task_dict['id']}"
    handle = await client.start_workflow(
        ShardedDownloadWorkflow.run,
        task_input,
        id=workflow_id,
        task_queue="download-workers",
    )
    logger.info(f"Started sharded workflow {workflow_id}")
    return handle


async def _find_running_workflow_ids(client, task_id: str) -> list:
    """All RUNNING workflow IDs containing task_id — catches suffixed legacy
    IDs (e.g. dl-{task_id}-bjN-v3) that fixed patterns miss."""
    try:
        return [wf_id for wf_id in await running_workflows(client) if task_id in wf_id]
    except Exception:
        # Best effort: the caller falls back to the fixed ID patterns. Swallowing
        # this is only safe because the scan is now bounded — an untimed one
        # would hang /queue/pause and /tasks/{id}/skip instead of failing.
        return []


async def cancel_workflow(task_id: str):
    """Cancel running workflow(s) for a task — handles all ID patterns."""
    client = await connected_client()
    wf_ids = {f"{WORKFLOW_ID_PREFIXES[t]}{task_id}" for t in PARENT_WORKFLOW_TYPES}
    wf_ids.update(await _find_running_workflow_ids(client, task_id))

    for wf_id in wf_ids:
        try:
            handle = client.get_workflow_handle(wf_id)
            await handle.cancel(rpc_timeout=QUERY_TIMEOUT)
            logger.info(f"Cancelled workflow {wf_id}")
        except Exception:
            pass

    # Also cancel shard child workflows
    try:
        from ..queue.snapshot import get_shards_by_task, init_db
        init_db()
        shards = get_shards_by_task(task_id)
        shard_prefix = WORKFLOW_ID_PREFIXES["ShardWorkerWorkflow"]
        for shard in shards:
            try:
                handle = client.get_workflow_handle(f"{shard_prefix}{shard['id']}")
                await handle.cancel(rpc_timeout=QUERY_TIMEOUT)
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

    client = await connected_client()
    wf_ids = {f"{WORKFLOW_ID_PREFIXES[t]}{task_id}" for t in PARENT_WORKFLOW_TYPES}
    wf_ids.update(await _find_running_workflow_ids(client, task_id))
    handles = [client.get_workflow_handle(wf_id) for wf_id in wf_ids]
    try:
        from ..queue.snapshot import get_shards_by_task, init_db
        init_db()
        shard_prefix = WORKFLOW_ID_PREFIXES["ShardWorkerWorkflow"]
        for shard in get_shards_by_task(task_id):
            handles.append(client.get_workflow_handle(f"{shard_prefix}{shard['id']}"))
    except Exception:
        pass

    for handle in handles:
        try:
            await handle.terminate(
                reason=f"reshard/requeue of {task_id}", rpc_timeout=QUERY_TIMEOUT
            )
        except Exception:
            pass  # not found / already closed

    deadline = _time.monotonic() + timeout_s
    open_statuses = {WorkflowExecutionStatus.RUNNING}
    while _time.monotonic() < deadline:
        still_open = 0
        for handle in handles:
            try:
                # Without a deadline a single hung describe() outlives
                # `timeout_s` entirely — the loop only re-checks between
                # iterations, so it never gets to notice the deadline passed.
                desc = await handle.describe(rpc_timeout=QUERY_TIMEOUT)
                if desc.status in open_statuses:
                    still_open += 1
            except Exception:
                pass  # not found = closed
        if still_open == 0:
            return True
        await asyncio.sleep(2)
    return False
