"""The fleet-wide cancel endpoint: dry-run by default, parents only.

Two histories meet here. The old endpoint filtered on pre-sharding workflow
types, matched nothing, and reported count=0 — safe-deploy trusted it for a
month. The fix must not create the opposite hazard: an always-armed
fleet-wide cancel one bare curl away. Hence the confirm gate, and hence
these tests exercising the actual route function, not its source text.

Parents only: cancelling a ShardWorkerWorkflow directly runs its failure
handler, the shard reports failed, the coordinator marks the task `failed` —
terminal, never re-dispatched. Killing parents lets ParentClosePolicy
terminate children without that handler, so tasks stay reclaimable.

Run: python3 -m pytest tests/test_cancel_all.py -q   (needs temporalio → S1)
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace


RUNNING = {
    "DownloadDatasetWorkflow": ["dl-t-legacy"],
    "SplitDownloadWorkflow": [],
    "ShardedDownloadWorkflow": ["sharded-t-aaa", "sharded-t-bbb"],
    "ShardWorkerWorkflow": ["shard-s-t-aaa-0", "shard-s-t-aaa-1"],
}


class _FakeClient:
    def __init__(self, cancelled):
        self._cancelled = cancelled

    def list_workflows(self, query, rpc_timeout=None):
        async def gen():
            for wf_type, ids in RUNNING.items():
                if f'WorkflowType="{wf_type}"' in query:
                    for wf_id in ids:
                        yield SimpleNamespace(id=wf_id)
        return gen()

    def get_workflow_handle(self, wf_id):
        cancelled = self._cancelled

        class _Handle:
            async def cancel(self, rpc_timeout=None):
                cancelled.append(wf_id)

        return _Handle()


def _call(body):
    """Invoke the real route function against the fake client."""
    from dlm.web import temporal_client
    from dlm.web.routes.workflows import cancel_all

    cancelled = []

    async def fake_connected_client(timeout=None):
        return _FakeClient(cancelled)

    original = temporal_client.connected_client
    temporal_client.connected_client = fake_connected_client
    try:
        result = asyncio.run(cancel_all(body))
    finally:
        temporal_client.connected_client = original
    return result, cancelled


def test_bare_post_is_a_dry_run_that_cancels_nothing():
    """The legacy caller sent no body at all; it must preview, not fire."""
    result, cancelled = _call(None)

    assert result["dry_run"] is True
    assert sorted(result["would_cancel"]) == ["dl-t-legacy", "sharded-t-aaa", "sharded-t-bbb"]
    assert result["count"] == 3
    assert cancelled == []


def test_confirm_true_cancels_exactly_the_previewed_parents():
    result, cancelled = _call({"confirm": True})

    assert result["dry_run"] is False
    assert sorted(cancelled) == ["dl-t-legacy", "sharded-t-aaa", "sharded-t-bbb"]
    assert result["count"] == 3


def test_shard_children_are_never_cancelled_directly():
    """Their failure handler would mark tasks terminally failed."""
    _, cancelled = _call({"confirm": True})

    assert not any(wf_id.startswith("shard-s-") for wf_id in cancelled)


def test_confirm_must_be_the_boolean_true():
    """Truthy strings ("yes", "1") must not arm a fleet-wide cancel."""
    for body in ({"confirm": "yes"}, {"confirm": 1}, {"confirm": "true"}, {}):
        result, cancelled = _call(body)
        assert result["dry_run"] is True, body
        assert cancelled == []


def test_parent_types_are_derived_from_the_single_definition():
    from dlm.web.temporal_client import PARENT_WORKFLOW_TYPES, WORKFLOW_TYPES

    assert set(PARENT_WORKFLOW_TYPES) == set(WORKFLOW_TYPES) - {"ShardWorkerWorkflow"}
