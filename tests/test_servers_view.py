"""#111 — the Servers/Workers views rendered from a payload key that did not exist.

`app.js` derives both `serverKeys` and `workersOnly` from
`this.dashboard.servers`, and `index.html` consumes them in three places (the
Workers chip strip, the task table's server filter dropdown, and the whole
Servers tab). `/api/dashboard` carried no `servers` key: the Celery-era
scheduler injected `dashboard_data["servers"]` (commits 8a814dc / b4c36bd) and
the Temporal rewrite dropped that line, so all three regions have been
iterating `{}` — silently, because Alpine renders an empty x-for as nothing.

Two more things had drifted with it, which is why this file also pins the
field vocabulary rather than just the presence of the key:

  - `worker_alive` and `local` existed ONLY in index.html. No Python payload has
    emitted either since 58dc63d, so wiring the fetch alone would have drawn
    every node's dot red and labelled a live worker "offline".
  - `srv.host` / `srv.current_task` / `srv.alive_at` / `srv.queue_depth` /
    `srv.active_tasks` are Celery-era names. Today's columns are `hostname`,
    `current_task_id` and `last_seen`; the last two have no successor at all.

And `GET /api/servers/{key}` served `cache.get_servers()` exclusively while
nothing has called `cache.set_servers()` since the rewrite, so it 404'd on every
worker that was up and reporting.

Text-level checks on the static files follow the convention in
test_dispatch_mode_ui.py (see its docstring: no browser in CI, and adding a
node-dependent test would make the deploy gate depend on node being on S1).

Run: python3 -m pytest tests/test_servers_view.py -q
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path

STATIC = Path(__file__).resolve().parents[1] / "dlm" / "web" / "static"


def _call(coro):
    return asyncio.run(coro)


def _rows(now: float):
    """The two-hostnames-per-node shape the `workers` table really holds:
    `wN@temporal` sends liveness only, `wN@sidecar` sends the metrics."""
    return [
        {"hostname": "w1@temporal", "server_key": "w1", "status": "online",
         "last_seen": now - 5, "current_task_id": "t-abc"},
        {"hostname": "w1@sidecar", "server_key": "w1", "status": "online",
         "last_seen": now - 40, "disk_free_gb": 181, "staging_size_mb": 4096},
        {"hostname": "bj9@temporal", "server_key": "bj9", "status": "online",
         "last_seen": now - 9999},
    ]


def _seed(db, now: float):
    """Write `_rows` through the real `update_worker`, then backdate.

    `update_worker` stamps `last_seen = time.time()` itself and takes no
    override, so a test that needs a stale node has to age the row afterwards.
    """
    for row in _rows(now):
        fields = {k: v for k, v in row.items() if k != "last_seen"}
        db.update_worker(**fields)
        db._conn().execute("UPDATE workers SET last_seen = ? WHERE hostname = ?",
                           (row["last_seen"], row["hostname"]))
    db._conn().commit()


# ═══════════════════════════════════════════════════════════════════════
# 1. fleet.servers_view — the one definition both surfaces serve
# ═══════════════════════════════════════════════════════════════════════


def test_servers_view_is_keyed_by_server_key_and_merges_both_hostnames():
    """One entry per node, carrying the union of what its hostnames reported —
    not the freshest row whole, which would drop every sidecar metric because
    the temporal row is almost always fresher."""
    from dlm.web.fleet import servers_view

    now = time.time()
    view = servers_view(_rows(now), now)

    assert sorted(view) == ["bj9", "w1"]
    assert view["w1"]["disk_free_gb"] == 181       # from @sidecar
    assert view["w1"]["current_task_id"] == "t-abc"  # from @temporal
    assert view["w1"]["server_key"] == "w1"


def test_worker_alive_is_heartbeat_freshness_not_process_liveness():
    """The UI turns a false `worker_alive` into an offline dot and a Restart
    button, so it must mean "stopped reporting" — the `alive_workers`
    predicate — and never `download_process_alive`, which is the sidecar's
    separate claim about the temporal worker process."""
    from dlm.web.fleet import WORKER_TIMEOUT, servers_view

    now = time.time()
    view = servers_view(_rows(now), now)

    assert view["w1"]["worker_alive"] is True
    assert view["bj9"]["worker_alive"] is False   # last_seen 9999s ago

    # A node reporting on time whose download process is down is still ALIVE:
    # a restart is not the answer, and the offline dot would be a lie.
    quiet_process = [{"hostname": "w2@temporal", "server_key": "w2",
                      "last_seen": now - 1, "download_process_alive": False}]
    assert servers_view(quiet_process, now)["w2"]["worker_alive"] is True

    # Exactly at the boundary is not alive (strict <, same as alive_workers).
    edge = [{"hostname": "w3@temporal", "server_key": "w3",
             "last_seen": now - WORKER_TIMEOUT}]
    assert servers_view(edge, now)["w3"]["worker_alive"] is False


def test_servers_view_agrees_with_alive_workers_on_who_is_alive():
    """Two definitions of "alive" on the same page is how this module's
    docstring says the earlier false alarms happened. Pin them together."""
    from dlm.web.fleet import alive_workers, servers_view

    now = time.time()
    rows = _rows(now)
    from_view = {k for k, v in servers_view(rows, now).items() if v["worker_alive"]}
    from_fleet = {w["server_key"] for w in alive_workers(rows, now)}
    assert from_view == from_fleet


def test_servers_view_emits_no_local_flag():
    """S1 owns no `workers` row, so nothing in this dict is the master. The
    template's `srv.local` branches were deleted rather than fed a fabricated
    flag — if a future edit invents one, this test says where to look."""
    from dlm.web.fleet import servers_view

    now = time.time()
    for entry in servers_view(_rows(now), now).values():
        assert "local" not in entry


def test_servers_view_survives_a_row_with_no_server_key():
    """merge_workers drops keyless rows; the view must not KeyError on them."""
    from dlm.web.fleet import servers_view

    now = time.time()
    assert servers_view([{"hostname": "orphan", "last_seen": now}], now) == {}


# ═══════════════════════════════════════════════════════════════════════
# 2. GET /api/dashboard carries `servers`
# ═══════════════════════════════════════════════════════════════════════


def test_dashboard_payload_carries_servers_derived_from_its_own_worker_rows(monkeypatch):
    """The bug itself: `servers` absent while three UI regions render from it.
    Derived from the `workers` rows already in the payload, so it costs no
    extra query and cannot go stale against them."""
    from dlm.web.cache import cache
    from dlm.web.routes import dashboard as dashboard_route

    now = time.time()
    monkeypatch.setattr(cache.dashboard, "data",
                        {"total_tasks": 2, "workers": _rows(now)})

    out = _call(dashboard_route.get_dashboard())

    assert sorted(out["servers"]) == ["bj9", "w1"]
    assert out["servers"]["w1"]["worker_alive"] is True
    assert out["servers"]["w1"]["disk_free_gb"] == 181
    assert out["total_tasks"] == 2          # merged, not replaced


def test_dashboard_servers_is_empty_dict_not_missing_when_no_workers(monkeypatch):
    """`serverKeys` and `workersOnly` must find a mapping to iterate even on a
    fleet that has never reported — a missing key and an empty one render the
    same, but only one of them survives `Object.keys` on undefined upstream."""
    from dlm.web.cache import cache
    from dlm.web.routes import dashboard as dashboard_route

    monkeypatch.setattr(cache.dashboard, "data", {"total_tasks": 0})
    out = _call(dashboard_route.get_dashboard())
    assert out["servers"] == {}


def test_dashboard_does_not_mutate_the_cached_payload(monkeypatch):
    """The scheduler's cached dict is shared across requests; the route builds
    a new one. Writing `servers` into the cache would let it outlive the
    worker rows it was derived from."""
    from dlm.web.cache import cache
    from dlm.web.routes import dashboard as dashboard_route

    cached = {"total_tasks": 1, "workers": _rows(time.time())}
    monkeypatch.setattr(cache.dashboard, "data", cached)

    _call(dashboard_route.get_dashboard())
    assert "servers" not in cached


# ═══════════════════════════════════════════════════════════════════════
# 3. GET /api/servers and /api/servers/{key}
# ═══════════════════════════════════════════════════════════════════════


def test_list_servers_carries_worker_alive_like_the_dashboard_does(db):
    """Both surfaces go through servers_view, so the Servers tab and the
    Workers strip cannot disagree about who is up."""
    from dlm.web.routes import servers as servers_route

    _seed(db, time.time())

    out = _call(servers_route.list_servers())["servers"]
    assert out["w1"]["worker_alive"] is True
    assert out["w1"]["disk_free_gb"] == 181
    assert out["bj9"]["worker_alive"] is False


def test_get_single_server_reads_live_instead_of_a_cache_nobody_fills(db):
    """This 404'd on every live worker: it served `cache.get_servers()` only,
    and `cache.set_servers()` has had no caller since the Temporal rewrite."""
    from dlm.web.routes import servers as servers_route

    _seed(db, time.time())

    entry = _call(servers_route.get_server("w1"))
    assert entry["server_key"] == "w1"
    assert entry["worker_alive"] is True


def test_get_single_server_still_404s_for_an_unknown_key(db):
    import pytest
    from fastapi import HTTPException

    from dlm.web.routes import servers as servers_route

    _seed(db, time.time())
    with pytest.raises(HTTPException) as exc:
        _call(servers_route.get_server("w99"))
    assert exc.value.status_code == 404


def test_the_dead_servers_cache_accessors_are_gone():
    """Deleted rather than left as a trap: a `get_servers()` that always
    returns `{}` reads like a cache miss and hid the 404 above for weeks."""
    from dlm.web.cache import cache

    assert not hasattr(cache, "get_servers")
    assert not hasattr(cache, "set_servers")


# ═══════════════════════════════════════════════════════════════════════
# 4. The template's field vocabulary matches the payload (no browser in CI)
# ═══════════════════════════════════════════════════════════════════════


def test_servers_tab_reads_only_fields_the_payload_actually_emits():
    """The check that would have caught #111 on the day it landed: every
    `srv.<field>` in index.html must be a key servers_view emits."""
    import re

    from dlm.web.fleet import servers_view

    index = (STATIC / "index.html").read_text()
    now = time.time()
    emitted = set()
    for entry in servers_view(_rows(now), now).values():
        emitted |= set(entry)
    # Sidecar-only columns are absent from a node that never sent them, so
    # take the union with the raw column names those rows carry.
    emitted |= {"event_buffer_pending", "files_last_5min", "https_connections",
                "download_process_alive", "download_process_pid"}

    referenced = set(re.findall(r"\bsrv\.([a-z_]+)", index))
    assert referenced <= emitted, f"index.html reads absent fields: {referenced - emitted}"


def test_workers_strip_reads_only_fields_the_payload_actually_emits():
    """`workersOnly` maps to `{key, ...srv}`, so `w.key` is legitimate; every
    other `w.<field>` in the strip must come from the payload."""
    import re

    index = (STATIC / "index.html").read_text()
    strip = index[index.index('x-for="w in workersOnly"'):]
    strip = strip[:strip.index("</template>")]

    referenced = set(re.findall(r"\bw\.([a-z_]+)", strip))
    assert referenced <= {"key", "worker_alive", "disk_free_gb"}


def test_the_retired_celery_field_names_are_gone_from_the_servers_tab():
    """Named individually so a future revert points at the right line."""
    index = (STATIC / "index.html").read_text()
    for dead in ("srv.host\"", "srv.current_task\"", "srv.alive_at",
                 "srv.queue_depth", "srv.active_tasks", "srv.local"):
        assert dead not in index, f"{dead} is a Celery-era field no payload emits"


def test_workers_only_no_longer_filters_on_the_nonexistent_local_flag():
    appjs = (STATIC / "app.js").read_text()
    body = appjs[appjs.index("get workersOnly()"):]
    body = body[:body.index("},")]
    assert "local" in body            # the comment explaining why there is no filter
    assert ".filter(" not in body


def test_time_ago_accepts_epoch_seconds_so_last_seen_renders():
    """`last_seen` is a float epoch, and `new Date(1754800000)` is 1970. The
    Servers tab passes it straight in, so the helper has to handle numbers."""
    appjs = (STATIC / "app.js").read_text()
    body = appjs[appjs.index("timeAgo(isoStr)"):]
    body = body[:body.index("},")]
    assert "typeof isoStr === 'number'" in body
    assert "isoStr * 1000" in body


# --- the guard that would have caught this whole bug family -----------------
#
# #111 was one of four dead controls in the same dashboard, found one at a time
# by clicking them: three transfer buttons posted to a decommissioned Celery
# broker, and the Servers tab's Restart / View Log fetched routes deleted with
# that daemon. `fetch` on a missing path rejects into a `catch` that shows a
# generic toast (or nothing), so a dead button is indistinguishable from a
# working one until someone checks the server. This test makes the whole family
# a build failure instead of a discovery.


def _registered_route_patterns():
    """Compiled matchers for every path the app actually serves.

    Via `app.openapi()["paths"]` rather than a walk over `app.routes`: the
    routers are mounted with `prefix="/api"` and the walk has to reassemble
    that, which is how a first attempt at this test reported five routes and
    "everything is dead" — a false negative in the guard is worse than no
    guard.
    """
    import re

    from dlm.web.app import create_app

    paths = create_app().openapi()["paths"]
    return [
        (p, re.compile("^" + re.sub(r"\{[^}]+\}", "[^/]+", p) + "$"))
        for p in paths
    ]


def _api_urls_in_app_js():
    """Every `/api/...` literal in app.js, normalised for matching.

    Template interpolations (`${taskId}`) collapse to one path segment and the
    query string is dropped — a route is identified by its path.
    """
    import re

    js = (STATIC / "app.js").read_text()
    urls = set()
    for m in re.finditer(r"""fetch\(\s*['"`](/api/[^'"`]*)['"`]""", js):
        urls.add(m.group(1))
    return urls


def test_every_api_literal_in_app_js_is_inside_a_fetch_call():
    """The audit below only reads `fetch(...)` arguments, so a URL built some
    other way would slip past it silently. Pin that they are all fetch args,
    and this stays a complete audit rather than a partial one."""
    import re

    js = (STATIC / "app.js").read_text()
    all_literals = set(re.findall(r"""['"`](/api/[^'"`]*)['"`]""", js))
    assert all_literals == _api_urls_in_app_js(), (
        "an /api/ literal is reached some way other than fetch(); the dead-URL "
        "guard below cannot see it"
    )


def test_no_button_in_the_dashboard_fetches_a_route_that_does_not_exist():
    import re

    patterns = _registered_route_patterns()
    dead = []
    for url in sorted(_api_urls_in_app_js()):
        path = re.sub(r"\$\{[^}]*\}", "X", url.split("?")[0])
        if not any(rx.match(path) for _, rx in patterns):
            dead.append(url)
    assert not dead, (
        f"app.js fetches {dead} — no route serves them, so the control looks "
        f"like it works and does nothing"
    )


def test_the_two_servers_tab_controls_with_no_route_are_gone_not_just_hidden():
    """Removed rather than left in place: making the Servers tab render is what
    turned these from unreachable code into clickable dead buttons.

    Restart gets no replacement route on purpose — worker restart goes through
    scripts/deploy-workers.sh, which brings the fleet back under one md5
    version manifest; a per-host web button is the mixed-code bypass that
    constraint exists to prevent.
    """
    appjs = (STATIC / "app.js").read_text()
    index = (STATIC / "index.html").read_text()

    # Nothing in the template names them any more — the replacement comments
    # describe the routes, not the handlers.
    for gone in ("restartWorker", "viewLog", "showLogModal", "logServer",
                 "logContent"):
        assert gone not in index, f"{gone} is still wired up in index.html"

    # app.js may still name the two handlers inside the comment explaining
    # their absence, but must not define or call them, and the log modal's
    # state fields are gone outright.
    assert "restartWorker(" not in appjs
    assert "viewLog(" not in appjs
    for gone in ("showLogModal", "logServer", "logContent"):
        assert gone not in appjs, f"{gone} is dead state for a removed modal"


def test_transfer_sync_route_exists_and_shares_the_schedulers_single_slot():
    """The ⟳ 立即检查 button. Its path had never existed; the fix is a route,
    not a removal, because forcing a poll is a real operator need.

    It must run on the scheduler's own one-slot transfer executor: two threads
    polling at once is how the same import gets posted twice, which is the
    reason that pool has one slot (scheduler.py, TRANSFER_EXECUTOR_WORKERS).
    """
    from pathlib import Path

    src = (Path(__file__).resolve().parents[1]
           / "dlm" / "web" / "routes" / "transfer.py").read_text()
    body = src[src.index('@router.post("/transfer/sync")'):]
    assert "_transfer_executor" in body
    assert "run_blocking" not in body, (
        "the routes' own 4-slot pool would let a manual sync run beside a "
        "wedged scheduler stage"
    )
    assert "TRANSFER_STAGE_TIMEOUT" in body


def test_transfer_sync_reports_a_failed_poll_instead_of_zero_updates():
    """`_blocking_stage` returns None for both a timeout and a raise. Reporting
    that as `updated: 0` reads as "checked, nothing changed"."""
    from pathlib import Path

    src = (Path(__file__).resolve().parents[1]
           / "dlm" / "web" / "routes" / "transfer.py").read_text()
    body = src[src.index('@router.post("/transfer/sync")'):]
    assert "if report is None" in body
    assert "503" in body
