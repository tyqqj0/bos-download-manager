"""Workflow management API — list, cancel, and inspect Temporal workflows.

Every Temporal call here carries a deadline. `wait_for` around `get_client()`
covers the connect only — the `async for` that follows is a separate RPC and
needs its own `rpc_timeout`, or the request hangs after a successful connect.
"""

import asyncio
from fastapi import APIRouter

router = APIRouter(tags=["workflows"])


@router.get("/workflows")
async def list_workflows():
    """List all running Temporal workflows with metadata."""
    from ..temporal_client import QUERY_TIMEOUT, connected_client

    try:
        client = await connected_client()
    except Exception as e:
        return {"workflows": [], "count": 0, "error": str(e)}

    workflows = []
    try:
        async for wf in client.list_workflows(
            'ExecutionStatus="Running"', rpc_timeout=QUERY_TIMEOUT
        ):
            workflows.append({
                "id": wf.id,
                "type": wf.workflow_type,
                "status": wf.status.name if wf.status else "RUNNING",
                "start_time": str(wf.start_time) if wf.start_time else None,
            })
    except asyncio.TimeoutError:
        return {"workflows": workflows, "count": len(workflows), "error": "Query timed out"}
    except Exception as e:
        return {"workflows": workflows, "count": len(workflows), "error": str(e)}

    return {"workflows": workflows, "count": len(workflows)}


@router.delete("/workflows/{workflow_id}")
async def cancel_workflow(workflow_id: str):
    """Cancel a specific workflow by ID."""
    from ..temporal_client import QUERY_TIMEOUT, connected_client

    try:
        client = await connected_client()
        handle = client.get_workflow_handle(workflow_id)
        await handle.cancel(rpc_timeout=QUERY_TIMEOUT)
        return {"cancelled": workflow_id, "success": True}
    except Exception as e:
        return {"cancelled": workflow_id, "success": False, "error": str(e)}


@router.post("/cancel-all-workflows")
async def cancel_all():
    """Cancel all running download workflows. Used by safe-deploy script."""
    from ..temporal_client import QUERY_TIMEOUT, connected_client

    try:
        client = await connected_client()
    except Exception as e:
        return {"cancelled": [], "count": 0, "error": str(e)}

    cancelled = []
    try:
        # NOTE: this filter predates sharding and matches neither
        # ShardedDownloadWorkflow nor ShardWorkerWorkflow, so under the current
        # architecture it cancels nothing and reports count=0. safe-deploy.sh
        # Phase 1 relies on it and therefore deploys believing it stopped work
        # it did not stop. Left as-is deliberately: widening it to
        # WORKFLOW_TYPES would make the next safe-deploy run cancel every live
        # download, which is a call for the operator, not a silent fix.
        query = (
            '(WorkflowType="DownloadDatasetWorkflow" OR WorkflowType="SplitDownloadWorkflow")'
            ' AND ExecutionStatus="Running"'
        )
        async for wf in client.list_workflows(query, rpc_timeout=QUERY_TIMEOUT):
            handle = client.get_workflow_handle(wf.id)
            try:
                await handle.cancel(rpc_timeout=QUERY_TIMEOUT)
                cancelled.append(wf.id)
            except Exception:
                pass
    except Exception as e:
        return {"cancelled": cancelled, "count": len(cancelled), "error": str(e)}

    return {"cancelled": cancelled, "count": len(cancelled)}
