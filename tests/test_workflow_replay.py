"""Replay the captured production histories against the current workflow code.

Workflow code is REPLAYED: Temporal re-executes `workflows.py` from event one
and compares the commands it issues against the ones recorded in history. A
change that issues a *different* command at a point history already recorded is
a non-determinism error — the workflow task then fails and retries forever, the
execution stays RUNNING, and `reconcile()` will not re-dispatch it because
`has_live_workflow()` is true. Nothing is lost, but the task never finishes and
only a manual terminate clears it.

Changed activity *arguments* are safe (replay compares command type and id, not
input payloads). What is not safe is a new conditional that issues a command on
a path an in-flight execution has already passed — e.g. adding a gate that
reports the task failed where history recorded `create_shards_in_db`.

The histories in fixtures/histories/ are real, captured 2026-08-06/07 from the
live cluster:
  * sharded-t-20260806-319c55  — coordinator, 7 shards, 532943 remaining files
  * shard-s-t-20260806-319c55-0 — a shard child mid-`run_pipeline_batch`
  * sharded-t-20260806-cbf39e  — coordinator stopped at `list_repo_files`

They cover the three shapes that were in flight across the revision boundary.
Any future edit to workflows.py that would have wedged them fails here instead.

Run: pytest tests/test_workflow_replay.py -q   (needs temporalio)
"""

from __future__ import annotations

import asyncio
import pathlib

import pytest

from temporalio.client import WorkflowHistory
from temporalio.worker import Replayer

from dlm.temporal.workflows import (
    DownloadDatasetWorkflow,
    ShardedDownloadWorkflow,
    ShardWorkerWorkflow,
    SplitDownloadWorkflow,
)

HISTORY_DIR = pathlib.Path(__file__).parent / "fixtures" / "histories"

HISTORIES = sorted(p.name for p in HISTORY_DIR.glob("*.json"))


def test_the_fixtures_are_actually_present():
    """A silently empty glob would make every replay test below vacuous —
    green because nothing ran. The captured histories sat unreferenced by any
    test once already."""
    assert HISTORIES, f"no history fixtures found in {HISTORY_DIR}"


@pytest.mark.parametrize("filename", HISTORIES)
def test_history_replays_against_current_workflow_code(filename):
    path = HISTORY_DIR / filename
    # The workflow id is the filename — Temporal's JSON export does not carry
    # it outside the started-event attributes.
    history = WorkflowHistory.from_json(path.stem, path.read_text())

    replayer = Replayer(workflows=[
        ShardedDownloadWorkflow,
        ShardWorkerWorkflow,
        DownloadDatasetWorkflow,
        SplitDownloadWorkflow,
    ])
    # Raises on non-determinism. Activities are never executed during replay,
    # so this needs no stubs, no network and no Temporal server.
    asyncio.run(replayer.replay_workflow(history))
