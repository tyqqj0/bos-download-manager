"""The staging-cleanup endpoint is the only `rm -rf` in the web layer.

Staging is keyed by task NAME, and names are deliberately reused: /queue/add
permits re-adding a repo whose previous row is failed/revoked/done, and a
resume MUST reuse the exact original name or the BOS resume filter matches
nothing. So "this task row is terminal" does not make its staging path safe —
a live task with the same name is writing to that exact directory.

Run: pytest tests/test_staging_cleanup.py -q
"""

from __future__ import annotations

import asyncio
import time

import pytest

from dlm.queue import snapshot
from dlm.web.routes import servers as servers_routes


def _task(task_id, name, status, server=None):
    snapshot.upsert_task({
        "id": task_id, "name": name, "repo_id": f"org/{name}",
        "source": "modelscope", "type": "dataset", "category": "manipulation",
        "status": status, "server": server, "priority": 0, "size_gb": 10.0,
        "downloaded_gb": 0.0, "progress_pct": 0, "speed_mbps": 0,
        "retry_count": 0, "max_workers": 2,
    })


def _shard(shard_id, task_id, server, status="running", index=0):
    snapshot.upsert_shard({
        "id": shard_id, "task_id": task_id, "shard_index": index,
        "status": status, "server": server, "total_files": 5,
        "done_files": 0, "updated_at": time.time(),
    })


class _Server:
    host = "1.2.3.4"
    user = "root"


@pytest.fixture
def rm_calls(monkeypatch):
    """Capture the commands that would run on the worker."""
    calls = []
    monkeypatch.setattr(
        "dlm.core.ssh.ssh_exec",
        lambda host, user, cmd: calls.append(cmd) or "",
    )
    monkeypatch.setattr(
        "dlm.core.servers.load_servers", lambda: {"bj1": _Server(), "w3": _Server()})
    return calls


def _cleanup(key="bj1"):
    return asyncio.run(servers_routes.cleanup_server_staging(key))


def test_a_live_task_sharing_the_name_blocks_the_delete(dlm_db, rm_calls):
    """The dangerous case, reachable through documented usage: a failed run and
    its resume carry the same name by requirement."""
    _task("t-old", "RoboDojo", "failed", server="bj1")
    _task("t-new", "RoboDojo", "downloading")
    _shard("s-t-new-0", "t-new", "bj1")

    result = _cleanup("bj1")

    assert rm_calls == [], "would have deleted the live run's partials and resume markers"
    assert result["cleaned"] == []


def test_terminal_task_with_no_live_namesake_is_cleaned(dlm_db, rm_calls):
    _task("t-old", "OldSet", "done", server="bj1")

    result = _cleanup("bj1")

    assert rm_calls == ["rm -rf /data/staging/OldSet"]
    assert result["cleaned"] == ["OldSet"]


def test_sharded_task_is_found_through_its_shard_rows(dlm_db, rm_calls):
    """A sharded task's row carries server = NULL, so the task-level check
    alone cleaned nothing on a sharded fleet and staging grew until the host
    fell below MIN_SHARD_DISK_GB and left the dispatch pool."""
    _task("t-sharded", "ShardedSet", "done", server=None)
    _shard("s-t-sharded-0", "t-sharded", "bj1", status="done")

    assert _cleanup("bj1")["cleaned"] == ["ShardedSet"]
    assert rm_calls == ["rm -rf /data/staging/ShardedSet"]


def test_a_task_on_another_host_is_not_cleaned_here(dlm_db, rm_calls):
    _task("t-elsewhere", "ElsewhereSet", "done", server="w3")
    _shard("s-t-elsewhere-0", "t-elsewhere", "w3", status="done")

    assert _cleanup("bj1")["cleaned"] == []
    assert rm_calls == []


@pytest.mark.parametrize("name", ["..", "../../root", "a/b", ".hidden", "x;rm -rf /"])
def test_traversal_and_separator_names_are_refused(dlm_db, rm_calls, name):
    """`shlex.quote` blocks shell metacharacters but not path components:
    `rm -rf '/data/staging/..'` is `rm -rf /data` on that worker."""
    _task("t-bad", name, "done", server="bj1")

    result = _cleanup("bj1")

    assert rm_calls == []
    assert result["cleaned"] == []
    assert result["skipped"] == [name]
