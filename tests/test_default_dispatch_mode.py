"""The global default is pool — and stays wired to the constant, not a literal.

Two things are pinned here, and they fail in different ways:

  * **Behaviour**: both *dispatchable* add endpoints stamp a new row with
    `fleet.DEFAULT_DISPATCH_MODE`. Parametrised over BOTH values of the
    constant, because now that the default IS "pool" a test that monkeypatches
    it to "pool" and asserts "pool" passes against a hardcoded literal — the
    older tests in test_pool_dispatch.py were written when the default was
    "sharded" and have quietly lost their teeth.
  * **Structure**: every `upsert_task` call site under `dlm/web/` is either
    passing `dispatch_mode` or named in an exemption list with a reason. A
    third dispatchable entry point that forgets the key would otherwise write
    rows with SQLite's column default — 'sharded' — and no behavioural test
    that nobody wrote can catch that.

Plus `scripts/backfill_dispatch_mode.py`, which is the only thing that can
move the existing backlog: ALTER TABLE ... ADD COLUMN ... DEFAULT materialised
'sharded' onto every row that already existed, so the Python-side default
above is structurally incapable of reaching them.

Run: pytest tests/test_default_dispatch_mode.py -q
"""

from __future__ import annotations

import ast
import asyncio
import importlib.util
import pathlib

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent


def _call(coro):
    return asyncio.run(coro)


def _queue_add(**over):
    from dlm.web.routes.queue import add_to_queue

    body = {"repo_id": "org/default-mode-probe"}
    body.update(over)
    out = _call(add_to_queue(body))
    assert out.get("ok") is True, out
    return out["task_id"]


def _api_tasks_add(**over):
    from dlm.web.routes.tasks import AddTaskRequest, add_task

    body = {"url_or_repo": "org/default-mode-probe", "category": "manipulation"}
    body.update(over)
    out = _call(add_task(AddTaskRequest(**body)))
    return out["task"]["id"]


# Both *dispatchable* entry points. /storage/register also creates rows but
# creates them `done`, so it never dispatches — see the structural test below.
DISPATCHABLE_ADDS = {"queue/add": _queue_add, "api/tasks": _api_tasks_add}


# ── behaviour: the row follows the constant, whichever way it points ────────


@pytest.mark.parametrize("entry", sorted(DISPATCHABLE_ADDS))
@pytest.mark.parametrize("configured", ["pool", "sharded"])
def test_a_new_task_gets_the_configured_default(db, monkeypatch, entry, configured):
    """Both directions, so the assertion cannot pass by accident.

    Testing only `configured == "pool"` would be satisfied by a route with
    `dispatch_mode: "pool"` hardcoded — and hardcoding is exactly the failure
    mode that makes DLM_DEFAULT_DISPATCH_MODE=sharded (the documented rollback,
    fleet.py:36) a no-op on the day someone needs it.
    """
    import dlm.web.fleet as fleet

    monkeypatch.setattr(fleet, "DEFAULT_DISPATCH_MODE", configured)
    task_id = DISPATCHABLE_ADDS[entry]()

    assert db.get_task(task_id)["dispatch_mode"] == configured


@pytest.mark.parametrize("entry", sorted(DISPATCHABLE_ADDS))
def test_the_shipped_default_is_pool(db, entry):
    """No monkeypatch: what a real request to a real dlm-web writes today.

    This is the T6 deliverable itself — "the global default is pool" — read
    through the endpoints rather than off the constant, so an entry point that
    reads some other source of truth shows up here.
    """
    from dlm.web.fleet import DEFAULT_DISPATCH_MODE

    assert DEFAULT_DISPATCH_MODE == "pool"
    assert db.get_task(DISPATCHABLE_ADDS[entry]())["dispatch_mode"] == "pool"


@pytest.mark.parametrize("entry", sorted(DISPATCHABLE_ADDS))
@pytest.mark.parametrize("requested", ["pool", "sharded"])
def test_an_explicit_mode_still_wins_over_the_default(db, monkeypatch, entry, requested):
    """The default is a default. `--dispatch-mode sharded` must remain a way to
    opt one task out of pool without touching the fleet-wide setting."""
    import dlm.web.fleet as fleet

    monkeypatch.setattr(
        fleet, "DEFAULT_DISPATCH_MODE", "sharded" if requested == "pool" else "pool"
    )
    task_id = DISPATCHABLE_ADDS[entry](dispatch_mode=requested)

    assert db.get_task(task_id)["dispatch_mode"] == requested


# ── structure: no unaccounted-for task-creating call site ──────────────────

# Keyed by the enclosing *route* function, which is stable in a way line
# numbers are not. A new entry here needs a reason in this comment block, and
# "I didn't want the test to be red" is not one.
#
#   register_bos_data — /api/storage/register files data ALREADY on BOS as a
#       `done` row. It never dispatches, so a mode on it would be fiction.
#   batch_action — the retry path round-trips the EXISTING row dict, so it
#       preserves whatever mode that task already ran under. Deliberate: a
#       task with sharded shard rows and sharded staging on a worker should be
#       retried the way it was partitioned, not silently re-moded mid-life.
#       The backlog it would otherwise strand is handled once, up front, by
#       scripts/backfill_dispatch_mode.py covering `failed` and `paused`.
EXEMPT_FROM_DISPATCH_MODE = {"register_bos_data", "batch_action"}


def _writes_dispatch_mode(route: ast.AST, arg: ast.expr) -> bool:
    """Whether the row this call persists carries a `dispatch_mode` key.

    Resolved structurally, not by grepping the route for the string: every one
    of these routes mentions `dispatch_mode` several times (validating it,
    echoing it back in the response), so a substring check would stay green
    after the one line that actually persists it was deleted.

    Both real call sites build a dict literal and pass it by name, so: follow
    the name to its assignment in this route and read that dict's keys, plus
    any `row["dispatch_mode"] = ...` written afterwards. An argument that
    cannot be resolved to a literal — e.g. a row read back out of the DB —
    counts as NOT writing the key, which is the safe direction: it forces an
    explicit exemption with a reason rather than passing on a guess.
    """
    if isinstance(arg, ast.Dict):
        dicts = [arg]
        name = None
    elif isinstance(arg, ast.Name):
        name = arg.id
        dicts = [
            n.value for n in ast.walk(route)
            if isinstance(n, ast.Assign) and isinstance(n.value, ast.Dict)
            and any(isinstance(t, ast.Name) and t.id == name for t in n.targets)
        ]
    else:
        return False

    for d in dicts:
        for k in d.keys:
            if isinstance(k, ast.Constant) and k.value == "dispatch_mode":
                return True

    # row["dispatch_mode"] = ... after the literal was built.
    if name:
        for n in ast.walk(route):
            if not isinstance(n, ast.Assign):
                continue
            for t in n.targets:
                if (isinstance(t, ast.Subscript) and isinstance(t.value, ast.Name)
                        and t.value.id == name
                        and isinstance(t.slice, ast.Constant)
                        and t.slice.value == "dispatch_mode"):
                    return True
    return False


def _upsert_task_sites():
    """(file, lineno, route function, writes dispatch_mode?) per call site."""
    sites = []
    for path in sorted((REPO_ROOT / "dlm" / "web").rglob("*.py")):
        tree = ast.parse(path.read_text())
        funcs = [
            n for n in ast.walk(tree)
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
        ]
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            name = getattr(node.func, "attr", None) or getattr(node.func, "id", None)
            if name != "upsert_task" or not node.args:
                continue
            enclosing = sorted(
                (f for f in funcs if f.lineno <= node.lineno <= (f.end_lineno or 0)),
                key=lambda f: f.lineno,
            )
            # Outermost is the route; the inner one is the _run_blocking thunk.
            route = enclosing[0] if enclosing else tree
            sites.append((
                path.relative_to(REPO_ROOT).as_posix(),
                node.lineno,
                getattr(route, "name", "<module>"),
                _writes_dispatch_mode(route, node.args[0]),
            ))
    return sites


def test_every_task_creating_call_site_writes_the_mode_or_is_exempt():
    """The invariant that survives a fourth entry point being added.

    A row written without `dispatch_mode` does not get the Python default —
    it gets SQLite's column default, 'sharded'. So "forgot to pass the key"
    and "deliberately chose sharded" produce byte-identical rows, and only
    this test can tell them apart.
    """
    sites = _upsert_task_sites()
    assert sites, "found no upsert_task call sites — the AST walk is broken"

    unaccounted = [
        s for s in sites if not s[3] and s[2] not in EXEMPT_FROM_DISPATCH_MODE
    ]
    assert not unaccounted, (
        "these upsert_task call sites neither write dispatch_mode nor appear in "
        f"EXEMPT_FROM_DISPATCH_MODE: {unaccounted}"
    )


def test_the_exemption_list_has_no_dead_entries():
    """An exemption for a call site that no longer exists is a stale licence:
    it would silently cover a *different* future function of the same name."""
    routes = {s[2] for s in _upsert_task_sites()}
    stale = EXEMPT_FROM_DISPATCH_MODE - routes
    assert not stale, f"EXEMPT_FROM_DISPATCH_MODE names non-existent routes: {stale}"


# ── the backfill ───────────────────────────────────────────────────────────


@pytest.fixture
def backfill():
    """scripts/ is not a package, so load the file directly."""
    path = REPO_ROOT / "scripts" / "backfill_dispatch_mode.py"
    spec = importlib.util.spec_from_file_location("dlm_backfill_dispatch_mode", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _seed(db, task_id, status, mode="sharded"):
    db.upsert_task({
        "id": task_id,
        "name": task_id,
        "source": "hf",
        "repo_id": f"org/{task_id}",
        "status": status,
        "category": "manipulation",
        "type": "dataset",
        "priority": 5,
        "dispatch_mode": mode,
    })


def _mode(db, task_id):
    return db.get_task(task_id)["dispatch_mode"]


def _run_backfill(module, apply: bool):
    """Drive the script the way an operator does — through argv."""
    import sys

    argv = ["backfill_dispatch_mode.py"] + (["--apply"] if apply else [])
    old = sys.argv
    sys.argv = argv
    try:
        return module.main()
    finally:
        sys.argv = old


def test_the_backfill_moves_pending_paused_and_failed(db, backfill):
    for status in ("pending", "paused", "failed"):
        _seed(db, f"t-{status}", status)

    assert _run_backfill(backfill, apply=True) == 0

    for status in ("pending", "paused", "failed"):
        assert _mode(db, f"t-{status}") == "pool"


def test_the_backfill_never_touches_a_downloading_row(db, backfill):
    """A3. A running task's mode describes the coordinator currently driving
    it; rewriting it under a live ShardedDownloadWorkflow would make every
    later read — reconciler, doctor, the shard popup — lie about what is
    actually running on the fleet."""
    _seed(db, "t-live", "downloading")
    _seed(db, "t-queued", "pending")

    assert _run_backfill(backfill, apply=True) == 0

    assert _mode(db, "t-live") == "sharded"
    assert _mode(db, "t-queued") == "pool"


@pytest.mark.parametrize("status", ["done", "revoked", "skipped"])
def test_the_backfill_leaves_terminal_history_alone(db, backfill, status):
    """Those rows will never dispatch again. Rewriting them only makes the
    audit trail wrong about how the download actually ran."""
    _seed(db, f"t-{status}", status)

    assert _run_backfill(backfill, apply=True) == 0

    assert _mode(db, f"t-{status}") == "sharded"


def test_a_dry_run_writes_nothing(db, backfill, capsys):
    _seed(db, "t-pending", "pending")

    assert _run_backfill(backfill, apply=False) == 0

    assert _mode(db, "t-pending") == "sharded"
    out = capsys.readouterr().out
    assert "DRY RUN" in out
    assert "t-pending" in out  # and it still shows what it WOULD do


def test_a_null_dispatch_mode_row_is_backfilled_too(db, backfill):
    """`NULL <> 'pool'` is NULL under SQL's three-valued logic — not true — so
    a bare comparison would silently skip exactly the rows most in need of
    backfilling. This is what the COALESCE in the script is for."""
    _seed(db, "t-null", "pending")
    conn = db._conn()
    conn.execute("UPDATE tasks SET dispatch_mode = NULL WHERE id = 't-null'")
    conn.commit()
    assert _mode(db, "t-null") is None

    assert _run_backfill(backfill, apply=True) == 0

    assert _mode(db, "t-null") == "pool"


def test_the_backfill_is_idempotent(db, backfill, capsys):
    """It runs on S1 by hand, so a second run is a matter of when, not if."""
    _seed(db, "t-pending", "pending")
    assert _run_backfill(backfill, apply=True) == 0
    capsys.readouterr()

    assert _run_backfill(backfill, apply=True) == 0

    assert "0 row(s) to move to pool" in capsys.readouterr().out
    assert _mode(db, "t-pending") == "pool"


def test_the_backfill_reports_the_rows_it_moved(db, backfill, capsys):
    """The operator's only record of what happened. A silent UPDATE against
    the single state source is not reviewable after the fact."""
    _seed(db, "t-pending", "pending")
    _seed(db, "t-failed", "failed")

    assert _run_backfill(backfill, apply=True) == 0

    out = capsys.readouterr().out
    assert "2 row(s) to move to pool" in out
    assert "t-pending" in out and "t-failed" in out
    assert "UPDATE affected 2 row(s)" in out
