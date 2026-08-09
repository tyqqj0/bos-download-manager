"""Deliverable 2 (task-preempt-brief.md) — the UI stops hardcoding 'sharded'.

app.js:22/269 hardcoded `dispatch_mode: 'sharded'` in the add-form state and
:261 submitted it unconditionally, so flipping DLM_DEFAULT_DISPATCH_MODE to
'pool' on the cluster would silently defeat itself for every task created
through the web UI (API-created tasks would follow the flip; UI-created ones
would not, and nothing would say so). Covers:

1. GET /api/dashboard exposes fleet.DEFAULT_DISPATCH_MODE, both through the
   scheduler's 10s cache and the cold-cache live fallback dashboard.py itself
   reads directly (get_dashboard_summary(), not scheduler._build_dashboard —
   a different function that must be checked separately, see the two tests
   below).
2. GET /api/tasks (_task_for_frontend) carries dispatch_mode per row — the
   brief assumed this already existed ("queue.py:390 coalesces it"); it
   didn't, for the *list* endpoint (tasks.py), only for the shard-detail one
   (queue.py). Without it, index.html's per-row "N/M shards|batches" button
   has nothing to branch on and a pool task's row always says "shards".
   Documented as a judged deviation in the task report.
3. Text-level checks on app.js/index.html for what there is no browser in CI
   to exercise — same convention as test_pool_observability.py's hero-card
   tests (see that file's docstring on why: adding a node-dependent test
   would make the deploy gate depend on node being installed on S1).

Also covers review finding M5: doctorFix's toast dropped skipped_pool_tasks
and summed a `restart_dead` key doctor.py's fix() never returns.

Run: python3 -m pytest tests/test_dispatch_mode_ui.py -q
"""

from __future__ import annotations

import asyncio
from pathlib import Path

STATIC = Path(__file__).resolve().parents[1] / "dlm" / "web" / "static"


def _call(coro):
    return asyncio.run(coro)


def _task(db, task_id, *, mode="pool", status="downloading"):
    row = {"id": task_id, "name": task_id, "repo_id": "org/x",
           "status": status, "priority": 5}
    if mode is not None:
        row["dispatch_mode"] = mode
    db.upsert_task(row)


# ═══════════════════════════════════════════════════════════════════════
# 1. GET /api/dashboard exposes fleet.DEFAULT_DISPATCH_MODE
# ═══════════════════════════════════════════════════════════════════════


def test_dashboard_cold_cache_reports_live_default_dispatch_mode(db, monkeypatch):
    """Cold-cache path: cache.dashboard.data is empty, so dashboard.py falls
    back to its own `_live` closure (get_dashboard_summary directly) rather
    than scheduler._build_dashboard. That closure builds a dict with no
    default_dispatch_mode key of its own — the route must add it on the way
    out regardless of which path produced `data`."""
    from dlm.web.cache import cache
    from dlm.web.routes import dashboard as dashboard_route
    import dlm.web.fleet as fleet

    monkeypatch.setattr(cache.dashboard, "data", {})
    monkeypatch.setattr(fleet, "DEFAULT_DISPATCH_MODE", "pool")

    out = _call(dashboard_route.get_dashboard())
    assert out["default_dispatch_mode"] == "pool"


def test_dashboard_cached_path_also_merges_default_dispatch_mode(monkeypatch):
    """The scheduler's 10s refresh populates cache.dashboard.data; that is
    the path a running server serves almost all the time (the cold-cache
    fallback above only fires for ~10s after a restart), so it needs the
    field too — and merged, not replacing the rest of the cached payload."""
    from dlm.web.cache import cache
    from dlm.web.routes import dashboard as dashboard_route
    import dlm.web.fleet as fleet

    monkeypatch.setattr(cache.dashboard, "data", {"total_tasks": 3})
    monkeypatch.setattr(fleet, "DEFAULT_DISPATCH_MODE", "pool")

    out = _call(dashboard_route.get_dashboard())
    assert out["default_dispatch_mode"] == "pool"
    assert out["total_tasks"] == 3


def test_dashboard_default_dispatch_mode_follows_the_env_default_too():
    """Sanity check against the *real* fleet constant (no monkeypatch) so a
    future default flip in fleet.py is reflected here without editing this
    test — matches the brief's literal ask: 'equal to
    fleet.DEFAULT_DISPATCH_MODE'."""
    from dlm.web.routes import dashboard as dashboard_route
    import dlm.web.fleet as fleet

    out = _call(dashboard_route.get_dashboard())
    assert out["default_dispatch_mode"] == fleet.DEFAULT_DISPATCH_MODE


# ═══════════════════════════════════════════════════════════════════════
# 2. GET /api/tasks carries dispatch_mode per row (tasks.py, not brief-listed
#    — the brief's assumption that this already existed was wrong; see the
#    module docstring above)
# ═══════════════════════════════════════════════════════════════════════


def test_task_for_frontend_carries_dispatch_mode_pool_sharded_and_legacy_null():
    """index.html's per-row shard/batch button branches on t.dispatch_mode.
    A pre-pool row has dispatch_mode=NULL in SQLite (get_all_tasks() surfaces
    that as the key being absent or None) and must still read as 'sharded',
    not as a falsy 'no mode' the ternary can't branch on."""
    from dlm.web.routes.tasks import _task_for_frontend

    assert _task_for_frontend({"id": "a", "dispatch_mode": "pool"})["dispatch_mode"] == "pool"
    assert _task_for_frontend({"id": "b", "dispatch_mode": "sharded"})["dispatch_mode"] == "sharded"
    assert _task_for_frontend({"id": "c"})["dispatch_mode"] == "sharded"
    assert _task_for_frontend({"id": "d", "dispatch_mode": None})["dispatch_mode"] == "sharded"


# ═══════════════════════════════════════════════════════════════════════
# 3. Text-level checks — no browser in CI (same convention as
#    test_pool_observability.py's hero-card tests, see its docstring)
# ═══════════════════════════════════════════════════════════════════════


def test_app_js_no_longer_hardcodes_sharded_dispatch_mode():
    """Crude, but it is the only thing standing between a future edit and
    silently re-breaking the DLM_DEFAULT_DISPATCH_MODE flip: app.js must
    never again write a literal 'sharded' into addForm.dispatch_mode."""
    appjs = (STATIC / "app.js").read_text()
    assert "dispatch_mode: 'sharded'" not in appjs


def test_submit_add_omits_dispatch_mode_key_when_form_value_is_empty():
    """submitAdd must build the POST body so an empty addForm.dispatch_mode
    ('' = server default) leaves the key out entirely, not send '' explicitly
    — /api/tasks's VALID_DISPATCH_MODES check would 400 on an explicit ''
    instead of falling through to DEFAULT_DISPATCH_MODE."""
    appjs = (STATIC / "app.js").read_text()
    start = appjs.index("async submitAdd()")
    end = appjs.index("confirmDelete(", start)
    body = appjs[start:end]
    assert "body.dispatch_mode = this.addForm.dispatch_mode" in body
    assert "dispatch_mode: this.addForm.dispatch_mode," not in body


def test_add_form_reset_uses_empty_dispatch_mode_not_sharded_literal():
    """The post-submit reset (old app.js:269) must reset to the same '' sentinel
    as the initial state, not silently reintroduce the hardcoded literal."""
    appjs = (STATIC / "app.js").read_text()
    start = appjs.index("async submitAdd()")
    end = appjs.index("confirmDelete(", start)
    body = appjs[start:end]
    assert "dispatch_mode: ''" in body
    assert "dispatch_mode: 'sharded'" not in body


def test_dispatch_mode_select_has_server_default_option_first():
    """index.html:996-1001 — third option, first and selected by default,
    value="", labelled with the live server default; explicit sharded/pool
    remain as manual overrides with no stale '(default)' suffix."""
    index = (STATIC / "index.html").read_text()
    start = index.index('Dispatch mode')
    select = index[start:index.index("</select>", start)]
    assert 'value=""' in select
    assert "defaultDispatchMode" in select
    assert '<option value="sharded">Sharded</option>' in select
    assert '<option value="pool">Pool (work-stealing)</option>' in select
    assert "(default)" not in select
    # first, selected by default
    assert select.index('value=""') < select.index('value="sharded"')
    assert "selected" in select[:select.index('value="sharded"')]


def test_row_shard_button_reads_batches_for_pool_and_byte_identical_for_sharded():
    """index.html:275 — the per-task table row button. G1 (frontend half):
    for a sharded task (t.dispatch_mode !== 'pool') the rendered string must
    stay exactly what it was before this branch existed."""
    index = (STATIC / "index.html").read_text()
    assert "t.dispatch_mode === 'pool' ? ' batches' : ' shards'" in index
    # byte-identical G1 regression guard: the sharded half of the ternary is
    # the exact old unconditional string.
    assert "t.done_shards + '/' + t.total_shards + (t.dispatch_mode === 'pool' ? ' batches' : ' shards')" in index


def test_shard_modal_heading_branches_pool_vs_sharded():
    """index.html:398 — the detail modal heading. Sharded default stays the
    literal text 'Shards' (G1); pool renders 'Batches'."""
    index = (STATIC / "index.html").read_text()
    assert "shardDispatchMode === 'pool' ? 'Batches' : 'Shards'" in index
    assert "<h3 class=\"font-bold\">Shards</h3>" not in index


# ═══════════════════════════════════════════════════════════════════════
# 4. The shard-count input is sharded-only (pool has shipped)
# ═══════════════════════════════════════════════════════════════════════
#
# The input was left visible and unconditionally submitted while pool was
# still landing, with a note in app.js saying the mode-awareness would follow.
# Pool is now the server default, so the form's most prominent numeric field
# is one that does nothing for the mode almost every new task uses: someone
# types "6 workers", the task recruits whatever the dispatch window allows,
# and the number they set is never read (tasks.py:347 only stores it, and only
# the sharded coordinator reads it).


def test_effective_dispatch_mode_falls_back_to_the_server_default():
    """The gate has to key off the mode the task will ACTUALLY get. The form's
    own value is '' whenever the user has not overridden it, so reading
    addForm.dispatch_mode alone would show the sharded-only input on a pool
    cluster — the exact bug this closes."""
    appjs = (STATIC / "app.js").read_text()
    start = appjs.index("effectiveDispatchMode()")
    body = appjs[start:appjs.index("},", start)]
    assert "this.addForm.dispatch_mode || this.defaultDispatchMode" in body


def test_shard_count_input_is_hidden_unless_the_task_will_be_sharded():
    index = (STATIC / "index.html").read_text()
    start = index.index("Shards (workers)")
    # walk back to the enclosing row that carries the gate
    row = index[index.rindex("<div", 0, index.rindex("<div", 0, start)):start]
    assert "effectiveDispatchMode() === 'sharded'" in row
    assert "x-show" in row


def test_submit_add_sends_shard_count_only_for_sharded():
    """A pool task must not carry a shard_count at all. Sending 0 would be
    harmless today (tasks.py:347 gates on > 0) but it re-establishes the field
    as part of every pool submission, which is how the stale max_workers got
    written in the first place."""
    appjs = (STATIC / "app.js").read_text()
    start = appjs.index("async submitAdd()")
    body = appjs[start:appjs.index("confirmDelete(", start)]
    assert "if (this.effectiveDispatchMode() === 'sharded')" in body
    assert "body.shard_count" in body
    # not an unconditional key in the body literal
    assert "shard_count: Number(" not in body


def test_the_pool_ships_later_comment_is_gone():
    """The note that deferred this ("the form's mode-awareness lands after pool
    ships") is now false, and a false comment about a gate is worse than none."""
    appjs = (STATIC / "app.js").read_text()
    assert "lands after pool ships" not in appjs


# ═══════════════════════════════════════════════════════════════════════
# 5. Review finding M5 — doctorFix's toast drops skipped_pool_tasks
# ═══════════════════════════════════════════════════════════════════════


def test_doctor_fix_toast_surfaces_skipped_pool_tasks_and_drops_dead_key():
    """doctor.py's fix() never returns a `restart_dead` key ("There is
    deliberately no restart worker action" per its docstring) — the old sum
    counted it anyway (always 0) and never read skipped_pool_tasks, so "Fix
    All" on a pool orphan showed "Fixed 0 issues" and dropped the response's
    explanation of why."""
    appjs = (STATIC / "app.js").read_text()
    start = appjs.index("async doctorFix(actions)")
    end = appjs.index("async cleanServer", start)
    body = appjs[start:end]
    assert "data.skipped_pool_tasks" in body
    assert "data.restart_dead" not in body
    # the real fix-result keys doctor.py's fix() can return, all counted now
    assert "data.redispatch_orphaned" in body
    assert "data.redispatch_pool" in body
    assert "data.reset_stuck" in body
    assert "data.skip_zombie" in body
