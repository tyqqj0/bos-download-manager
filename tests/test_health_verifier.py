"""Layer 3 correlation, and the invariant that it never forks.

The SSH fan-out this module used to run took the whole control plane down
for 24 hours on 2026-07-31: a fork from the event-loop thread of a
23-thread process produced a child that deadlocked before exec(), so the
loop never called accept() again. The no-subprocess test below is the
guard against that regressing — it is the only test here that is about a
past outage rather than about behaviour.

Run: python3 -m pytest tests/test_health_verifier.py -q
"""

from __future__ import annotations

import time

from dlm.web.health_verifier import correlate_layers, work_by_server


def _worker(key, *, last_seen=None, files5=None, conns=None, alive=None):
    return {
        "server_key": key,
        "last_seen": time.time() if last_seen is None else last_seen,
        "files_last_5min": files5,
        "https_connections": conns,
        "download_process_alive": alive,
    }


def _types(anomalies):
    return {a["type"] for a in anomalies}


def test_module_never_spawns_a_process():
    """The outage was a fork on the event loop. Nothing here may fork again.

    Grepping for `subprocess` alone is not enough: every helper in
    `dlm.core.ssh` reaches `subprocess.run` one frame down (ssh.py:42, :182),
    so `from ..core.ssh import ssh_exec` forks from the loop thread while
    this module's own source stays clean. The 2026-07-31 hang came from a
    fork in the web process; where the `subprocess` token was typed does not
    change that. So the ssh surface is named explicitly.
    """
    import ast
    import inspect

    from dlm.web import health_verifier

    source = inspect.getsource(health_verifier)
    body = source.split('"""', 2)[-1]  # skip the docstring that explains why
    for forbidden in ("subprocess", "os.system", "os.fork", "os.popen", "os.spawn"):
        assert forbidden not in body, f"{forbidden} reintroduces the 2026-07-31 hang"

    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            module = node.module or ""
            assert not module.endswith("core.ssh") and module != "ssh", (
                f"imports from {module} — every ssh helper forks via "
                f"subprocess.run, which is the hang this file exists to prevent"
            )
            for alias in node.names:
                assert not alias.name.startswith("ssh_"), (
                    f"imports {alias.name}: forks via subprocess.run one frame down"
                )
        if isinstance(node, ast.Import):
            for alias in node.names:
                assert "core.ssh" not in alias.name, (
                    f"imports {alias.name}: every helper in it forks"
                )


def test_sharded_work_is_found_through_the_shard_row():
    """A sharded task's own row has server=NULL — the shard carries the server."""
    tasks = [{"id": "t-1", "name": "Beta", "status": "downloading", "server": None}]
    shards = [{"id": "s-t-1-0", "task_id": "t-1", "server": "w3", "updated_at": 100.0}]

    held = work_by_server(tasks, shards)

    assert held["w3"]["name"] == "Beta"


def test_legacy_single_node_task_is_still_found():
    tasks = [{"id": "t-1", "name": "Old", "status": "downloading",
              "server": "w1", "updated_at": 100.0}]

    assert work_by_server(tasks, [])["w1"]["name"] == "Old"


def test_offline_worker_is_left_to_the_doctor():
    """Offline is already `offline_workers`; repeating it double-alerts one fact."""
    now = time.time()
    workers = [_worker("w1", last_seen=now - 9999, files5=0, conns=0)]

    assert correlate_layers(workers, [], [], now) == []


def test_worker_without_sidecar_metrics_reports_blindness_not_a_stall():
    now = time.time()
    workers = [_worker("bj1")]  # basic heartbeat only: every metric is None
    tasks = [{"id": "t-1", "name": "MS", "status": "downloading", "server": None}]
    shards = [{"id": "s-t-1-0", "task_id": "t-1", "server": "bj1", "updated_at": 0}]

    types = _types(correlate_layers(workers, tasks, shards, now))

    assert types == {"sidecar_missing"}


def test_stall_needs_both_no_files_and_stale_progress():
    """Fresh progress is not a stall, whatever the file counter says.

    This replaces the old large-file exemption, which needed an SSH `find`
    to size the in-flight file.
    """
    now = time.time()
    workers = [_worker("w1", files5=0, conns=8, alive=1)]
    tasks = [{"id": "t-1", "name": "Big", "status": "downloading", "server": None}]
    fresh = [{"id": "s-t-1-0", "task_id": "t-1", "server": "w1", "updated_at": now - 5}]

    assert correlate_layers(workers, tasks, fresh, now) == []

    stale = [{"id": "s-t-1-0", "task_id": "t-1", "server": "w1", "updated_at": now - 3600}]
    assert _types(correlate_layers(workers, tasks, stale, now)) == {"possible_stall"}


def test_no_files_no_connections_and_stale_is_a_confirmed_stall():
    now = time.time()
    workers = [_worker("w1", files5=0, conns=0, alive=1)]
    tasks = [{"id": "t-1", "name": "Dead", "status": "downloading", "server": None}]
    shards = [{"id": "s-t-1-0", "task_id": "t-1", "server": "w1", "updated_at": now - 3600}]

    assert _types(correlate_layers(workers, tasks, shards, now)) == {
        "download_stalled_confirmed"
    }


def test_dead_download_process_is_flagged_even_while_idle():
    now = time.time()
    workers = [_worker("w1", files5=0, conns=0, alive=0)]

    assert _types(correlate_layers(workers, [], [], now)) == {
        "process_dead_undetected"
    }
