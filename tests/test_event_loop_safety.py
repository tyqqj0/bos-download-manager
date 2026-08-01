"""Invariants that keep the control plane from silently going dead.

Two outages of the same shape motivate this file. The web process stays
alive, keeps its port open and answers `ps` — while the event loop, or the
one scheduler loop that drives dispatch and reconciliation, has stopped
advancing. Nothing in the fleet notices, because every liveness check that
exists asks whether the process is running.

    2026-07-31: `asyncio.create_subprocess_shell` forked the 23-thread web
    process from the loop thread. The child deadlocked before exec(); the
    parent blocked in pipe_read on the errpipe. 24 hours offline.

The audit that followed found the same outcome reachable a second way: the
scheduler awaited Temporal with no deadline anywhere in the chain, so a
frontend that accepts the connection and never answers freezes the loop
body permanently — with HTTP still serving normally.

Run: python3 -m pytest tests/test_event_loop_safety.py -q
"""

from __future__ import annotations

import ast
import inspect
import pathlib

WEB = pathlib.Path(__file__).resolve().parent.parent / "dlm" / "web"


def _tree(relpath: str) -> ast.AST:
    return ast.parse((WEB / relpath).read_text())


def _own_body(fn: ast.AST):
    """Walk `fn` without descending into functions nested inside it.

    The nesting is the whole point: a blocking call inside a closure that
    gets handed to `run_blocking` runs on a worker thread, which is exactly
    what we want. Only code in the function's own body is on the loop.
    """
    stack = list(ast.iter_child_nodes(fn))
    while stack:
        node = stack.pop()
        yield node
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            continue
        stack.extend(ast.iter_child_nodes(node))


def _calls(nodes) -> list[str]:
    """Dotted names of every call among these nodes, e.g. `asyncio.wait_for`."""
    out = []
    for n in nodes:
        if isinstance(n, ast.Call):
            f = n.func
            parts = []
            while isinstance(f, ast.Attribute):
                parts.append(f.attr)
                f = f.value
            if isinstance(f, ast.Name):
                parts.append(f.id)
            if parts:
                out.append(".".join(reversed(parts)))
    return out


def _function(tree: ast.AST, name: str) -> ast.AST:
    for n in ast.walk(tree):
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name:
            return n
    raise AssertionError(f"{name} not found — did it get renamed?")


def test_every_scheduler_stage_has_a_deadline():
    """A hang in any stage stops the loop for good; try/except does not catch it."""
    loop = _function(_tree("scheduler.py"), "background_scheduler")

    awaited = [
        n for n in _own_body(loop)
        if isinstance(n, ast.Await) and isinstance(n.value, ast.Call)
    ]
    bare = []
    for a in awaited:
        called = _calls([a.value])[:1]
        if called and called[0] in ("asyncio.wait_for", "loop.run_in_executor",
                                    "asyncio.sleep"):
            continue
        bare.append(called[0] if called else "?")

    assert not bare, f"scheduler awaits without a deadline: {bare}"


def test_workflow_type_list_is_defined_once():
    """Six inlined copies is how the rpc_timeout got missed in five of them."""
    from dlm.web import temporal_client

    hits = []
    for path in WEB.rglob("*.py"):
        text = path.read_text()
        if "ShardWorkerWorkflow" in text and path.name != "temporal_client.py":
            hits.append(str(path.relative_to(WEB)))

    assert not hits, f"workflow types re-listed outside temporal_client: {hits}"
    assert "ShardWorkerWorkflow" in temporal_client.WORKFLOW_TYPES


def test_shared_workflow_scan_passes_an_rpc_timeout():
    from dlm.web.temporal_client import running_workflows

    source = inspect.getsource(running_workflows)
    assert "rpc_timeout=QUERY_TIMEOUT" in source


def test_connected_client_bounds_the_untimed_connect():
    """`Client.connect` takes no deadline of its own — wait_for is the only guard."""
    from dlm.web.temporal_client import connected_client

    assert "asyncio.wait_for" in inspect.getsource(connected_client)


def test_doctor_does_not_touch_sqlite_on_the_loop():
    """init_db takes the write lock; on the loop that is an accept() gap."""
    tree = _tree("routes/doctor.py")
    for name in ("diagnose", "fix"):
        called = _calls(_own_body(_function(tree, name)))
        for db_call in ("init_db", "get_all_tasks", "get_workers",
                        "get_running_shards", "update_task_progress"):
            assert db_call not in called, f"{name}() calls {db_call} on the event loop"


def test_doctor_orphan_report_has_no_undefined_reference():
    """A NameError here is swallowed by the broad except and reported as
    'Cannot check Temporal', which also suppresses the idle-worker check —
    so orphan detection died exactly when an orphan existed."""
    tree = _tree("routes/doctor.py")
    fn = _function(tree, "diagnose")

    defined = {"self"}
    for n in ast.walk(fn):
        if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Store):
            defined.add(n.id)
        elif isinstance(n, (ast.Import, ast.ImportFrom)):
            for alias in n.names:
                defined.add(alias.asname or alias.name.split(".")[0])
        elif isinstance(n, ast.comprehension) and isinstance(n.target, ast.Name):
            defined.add(n.target.id)
        elif isinstance(n, ast.ExceptHandler) and n.name:
            defined.add(n.name)

    used = {n.id for n in ast.walk(fn)
            if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)}

    import builtins
    unresolved = used - defined - set(dir(builtins)) - set(globals())
    # Module-level names of doctor.py are legitimately visible.
    unresolved -= {n.name for n in ast.walk(tree)
                   if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    unresolved -= {"router", "time", "asyncio", "run_blocking", "DEAD_THRESHOLD",
                   "STALE_THRESHOLD", "WORKER_TIMEOUT", "has_live_workflow"}

    assert not unresolved, f"undefined names in diagnose(): {sorted(unresolved)}"
