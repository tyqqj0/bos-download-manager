"""Consistency test for the workflow-type registry.

Three sites used to hand-copy the download workflow types: adding a fourth
(T5's `PoolDownloadWorkflow`) drifted a hardcode independently every time —
one omission left a workflow type invisible to pause/skip/reshard/reconciler/
doctor. `WORKFLOW_TYPES` and `WORKFLOW_ID_PREFIXES` in
`dlm.web.temporal_client` are now the single source; this test locks that
every `@workflow.defn` class in `dlm.temporal.workflows` is a subset of
`WORKFLOW_TYPES`, and every parent type has an ID-prefix entry.

Run: python3 -m pytest tests/test_workflow_registry.py -q   (needs temporalio)
"""

from __future__ import annotations

import inspect


def _is_workflow_defn(cls) -> bool:
    """Whether `cls` was decorated with `@temporalio.workflow.defn`.

    `_Definition.from_class` is the documented way to answer this (it
    returns None for a plain class); `__temporal_workflow_definition` is
    the attribute the decorator actually stamps on the class and is kept
    as a fallback in case the private `_Definition` API moves again.
    """
    from temporalio import workflow

    definition_cls = getattr(workflow, "_Definition", None)
    if definition_cls is not None and hasattr(definition_cls, "from_class"):
        return definition_cls.from_class(cls) is not None
    return hasattr(cls, "__temporal_workflow_definition")


def _defn_class_names() -> set[str]:
    from dlm.temporal import workflows

    return {
        name
        for name, obj in inspect.getmembers(workflows, inspect.isclass)
        if obj.__module__ == workflows.__name__ and _is_workflow_defn(obj)
    }


def test_every_workflow_defn_class_is_in_workflow_types():
    """A new `@workflow.defn` class in workflows.py must be registered.

    Subset, not equality: WORKFLOW_TYPES may (and does, ahead of T5) list a
    type — `PoolDownloadWorkflow` — before its class exists.
    """
    from dlm.web.temporal_client import WORKFLOW_TYPES

    defn_names = _defn_class_names()
    assert defn_names, "expected at least one @workflow.defn class in workflows.py"
    missing = defn_names - set(WORKFLOW_TYPES)
    assert not missing, f"workflow.defn classes missing from WORKFLOW_TYPES: {missing}"


def test_every_parent_workflow_type_has_an_id_prefix():
    from dlm.web.temporal_client import PARENT_WORKFLOW_TYPES, WORKFLOW_ID_PREFIXES

    missing = [t for t in PARENT_WORKFLOW_TYPES if t not in WORKFLOW_ID_PREFIXES]
    assert not missing, f"parent workflow types with no WORKFLOW_ID_PREFIXES entry: {missing}"


def test_every_workflow_type_has_an_id_prefix():
    """Not just parents — ShardWorkerWorkflow's own prefix must be present
    too, since fleet.has_live_workflow and the cancel/terminate sweeps both
    need it to recognize shard-child IDs."""
    from dlm.web.temporal_client import WORKFLOW_ID_PREFIXES, WORKFLOW_TYPES

    missing = [t for t in WORKFLOW_TYPES if t not in WORKFLOW_ID_PREFIXES]
    assert not missing, f"workflow types with no WORKFLOW_ID_PREFIXES entry: {missing}"
