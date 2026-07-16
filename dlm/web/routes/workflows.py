"""Workflow management API — list, cancel, and inspect Temporal workflows."""

import asyncio
from fastapi import APIRouter

router = APIRouter(tags=["workflows"])


@router.get("/workflows")
async def list_workflows():
    """List all running Temporal workflows with metadata."""
    from ..temporal_client import get_client

    try:
        client = await asyncio.wait_for(get_client(), timeout=5)
    except Exception as e:
        return {"workflows": [], "count": 0, "error": str(e)}

    workflows = []
    try:
        async for wf in client.list_workflows('ExecutionStatus="Running"'):
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
    from ..temporal_client import get_client

    try:
        client = await asyncio.wait_for(get_client(), timeout=5)
        handle = client.get_workflow_handle(workflow_id)
        await handle.cancel()
        return {"cancelled": workflow_id, "success": True}
    except Exception as e:
        return {"cancelled": workflow_id, "success": False, "error": str(e)}


@router.post("/cancel-all-workflows")
async def cancel_all():
    """Cancel all running download workflows. Used by safe-deploy script."""
    from ..temporal_client import get_client

    try:
        client = await asyncio.wait_for(get_client(), timeout=5)
    except Exception as e:
        return {"cancelled": [], "count": 0, "error": str(e)}

    cancelled = []
    try:
        query = (
            '(WorkflowType="DownloadDatasetWorkflow" OR WorkflowType="SplitDownloadWorkflow")'
            ' AND ExecutionStatus="Running"'
        )
        async for wf in client.list_workflows(query):
            handle = client.get_workflow_handle(wf.id)
            try:
                await handle.cancel()
                cancelled.append(wf.id)
            except Exception:
                pass
    except Exception as e:
        return {"cancelled": cancelled, "count": len(cancelled), "error": str(e)}

    return {"cancelled": cancelled, "count": len(cancelled)}
