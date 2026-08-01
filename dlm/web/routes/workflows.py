"""Workflow management API — list, cancel, and inspect Temporal workflows.

Every Temporal call here carries a deadline. `wait_for` around `get_client()`
covers the connect only — the `async for` that follows is a separate RPC and
needs its own `rpc_timeout`, or the request hangs after a successful connect.
"""

import asyncio
from fastapi import APIRouter, Body

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
async def cancel_all(body: dict | None = Body(default=None)):
    """Cancel every running download COORDINATOR workflow. Fleet-wide.

    Without `{"confirm": true}` this is a dry run: it returns what would be
    cancelled and touches nothing. The gate exists because the old version of
    this endpoint filtered on pre-sharding workflow types, matched nothing,
    and reported count=0 — so safe-deploy ran for a month believing it had
    stopped work it never stopped. An always-armed fleet-wide cancel is the
    opposite failure; the confirm requirement removes both.

    The body must stay optional: a required body turns the legacy bare-POST
    caller into a silent 422 swallowed by `curl -sf`, recreating the exact
    false "cancelled 0" this rewrite removes.

    Parent types only (PARENT_WORKFLOW_TYPES): children are terminated with
    their parent by ParentClosePolicy, without running the child-side failure
    handler that would mark tasks terminally `failed`. Tasks stay
    `downloading`, so orphan re-dispatch reclaims them losslessly.
    """
    from ..temporal_client import PARENT_WORKFLOW_TYPES, QUERY_TIMEOUT, connected_client

    confirm = bool(body and body.get("confirm") is True)

    try:
        client = await connected_client()
    except Exception as e:
        return {"cancelled": [], "count": 0, "dry_run": not confirm, "error": str(e)}

    targets = []
    try:
        for wf_type in PARENT_WORKFLOW_TYPES:
            async for wf in client.list_workflows(
                f'WorkflowType="{wf_type}" AND ExecutionStatus="Running"',
                rpc_timeout=QUERY_TIMEOUT,
            ):
                targets.append(wf.id)
    except Exception as e:
        return {"cancelled": [], "count": 0, "dry_run": not confirm, "error": str(e)}

    if not confirm:
        return {"dry_run": True, "would_cancel": targets, "count": len(targets)}

    cancelled = []
    errors = []
    for wf_id in targets:
        try:
            await client.get_workflow_handle(wf_id).cancel(rpc_timeout=QUERY_TIMEOUT)
            cancelled.append(wf_id)
        except Exception as e:
            errors.append(f"{wf_id}: {e}")

    result = {"dry_run": False, "cancelled": cancelled, "count": len(cancelled)}
    if errors:
        result["errors"] = errors
    return result
