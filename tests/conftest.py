"""Shared fixtures for the pytest suite.

Run: pytest tests/ -q  (pyproject sets pythonpath=["."], so bare pytest works
without a package install; python3 -m pytest works too)

`snapshot._conn()` caches its connection in a thread-local keyed by the path it
opened, so rebinding `snapshot.DB_PATH` is enough to redirect every thread —
including the web routes' long-lived module-level executors, which is what broke
two suites on 2026-08-07 (test_retry_false_done passed alone and failed after
test_coordinator_routing with "Task t-failed not found"). Use one of the DB
fixtures below rather than patching DB_PATH by hand, so the schema exists and
the calling thread's connection is dropped on the way out.
"""

from __future__ import annotations

import threading

import pytest


@pytest.fixture(autouse=True)
def _never_touch_the_production_alert_log(monkeypatch, tmp_path):
    """Structural version of a guard review finding I5 first pinned only in
    tests/test_pool_observability.py, moved here per review finding M4 so a
    future test file that starts calling check_alerts is covered without
    remembering to copy it.

    check_alerts unconditionally calls _get_alert_logger(), which opens
    logging.FileHandler(alerts.ALERT_LOG_PATH) in append mode and swallows
    only OSError/PermissionError. On a dev box /data is absent so nothing
    happens — but scripts/deploy-workers.sh runs `pytest tests/ -q` as its
    deploy gate on S1, where /data exists, so any test that reaches
    check_alerts without this guard appends fabricated alerts (and RESOLVED
    churn) to the live incident log a human greps.

    Two independent guards, because one monkeypatch is "unlikely", not
    "impossible" (see tests/test_pool_observability.py's pinning tests for
    what dropping either one costs):

    1. the module's cached logger is replaced with a NullHandler-only one, so
       _get_alert_logger() short-circuits and never builds a FileHandler;
    2. ALERT_LOG_PATH points at this test's tmp_path, so anything that DOES
       rebuild the logger — including a test that deliberately resets the
       cache — lands in the test's own directory.

    _active_alerts is reset too: it is module-global de-dupe state, and a leak
    across tests turns one test's alerts into another's RESOLVED lines.
    """
    import logging as _logging
    from dlm.web import alerts as alerts_mod

    monkeypatch.setattr(alerts_mod, "ALERT_LOG_PATH", tmp_path / "dlm-alerts.log")

    null = _logging.getLogger("dlm.alerts.pytest-null")
    null.handlers = [_logging.NullHandler()]
    null.propagate = False
    monkeypatch.setattr(alerts_mod, "_alert_logger", null)
    monkeypatch.setattr(alerts_mod, "_active_alerts", {})


@pytest.fixture(autouse=True)
def _the_hosts_free_disk_is_not_a_test_input(monkeypatch):
    """Make ambient free disk irrelevant to what the suite reports.

    `pipeline._disk_free_gb()` stats a real filesystem, and two call sites
    branch on it:

    1. `_producer`'s backpressure loop (`pipeline.py:459`) —
       `while _disk_free_gb() < threshold: await asyncio.sleep(10)`, where
       nothing in a unit test will ever free a byte. Below the threshold this
       does not fail, it **hangs forever**.
    2. `_wait_with_growth_check`'s emergency check (`pipeline.py:683`) —
       under `DISK_FREE_ABSOLUTE_MIN_GB` it raises `_StallDetected`, so a
       download test silently takes the disk-abort branch instead of the one
       it meant to exercise.

    The threshold is `max(30% of total, 20GB)` and both terms come from
    whichever machine runs pytest, so the suite's outcome depended on the
    host. Found the hard way on 2026-08-08: S1 sits at 81% used of a 40G
    `/dev/vda2` — 7.4G free against a 20G threshold — and
    `scripts/deploy-workers.sh` gate G7 runs `pytest tests/ -q` on S1 and
    refuses to deploy until it passes. The suite parked in
    test_failed_details.py's first `_producer` test at load 0.02 with no
    output: a deploy gate that hangs forever and prints nothing to explain
    itself. Locally the same suite is 13.5s.

    Patching `STAGING_PATH` to `tmp_path` — which both pipeline test files
    already do, for `/data` being absent on a dev box — does not help here:
    on S1 `/tmp` is that same `/dev/vda2`.

    A test that genuinely wants to exercise low disk re-patches
    `_disk_free_gb` itself; the test body runs after this fixture, so its
    value wins (pinned below in tests/test_disk_is_not_a_test_input.py).
    """
    from dlm.temporal import pipeline

    monkeypatch.setattr(pipeline, "_disk_free_gb", lambda: 10_000.0)


@pytest.fixture(autouse=True)
def _the_add_time_preflight_never_hits_the_network(monkeypatch):
    """Keep HuggingFace out of the suite.

    Both add routes now await `preflight.check_repo_access()`, which for an hf
    source issues a real authorised HEAD to huggingface.co. Every existing test
    that adds a task would therefore make a network call — and the outcome
    would depend on whether the machine running pytest happens to have
    HF_TOKEN exported, which is exactly the host-dependence the disk fixture
    above exists to eliminate. On S1 it is worse than flaky:
    scripts/deploy-workers.sh runs `pytest tests/ -q` as its deploy gate, and
    S1 does have a token, so the gate would spend a network round trip per
    add test and fail closed whenever HF is unreachable.

    Patched at `probe_hf_repo` rather than `check_repo_access` on purpose: the
    async wrapper carries the source routing and the thread hand-off, both of
    which tests should still exercise. This only replaces the socket.

    A test that wants a specific verdict re-patches `probe_hf_repo` itself; the
    test body runs after this fixture, so its value wins.
    """
    from dlm.web import preflight

    monkeypatch.setattr(
        preflight, "probe_hf_repo",
        lambda repo_id, dtype="dataset", token=None: preflight.PreflightResult(
            preflight.UNKNOWN, "preflight disabled in tests"),
    )


@pytest.fixture
def db(tmp_path, monkeypatch):
    """A fresh, isolated SQLite snapshot DB for one test.

    `dlm.queue.snapshot` keeps its connection in a thread-local and reads
    its path from a module-level constant set at import time, so a test
    can't just pass a path around — it has to redirect the module itself
    and throw away any cached connection so the next `_conn()` call opens
    the redirected file instead of a stale handle to the last test's.
    """
    from dlm.queue import snapshot

    db_path = tmp_path / "dlm-test.db"
    monkeypatch.setattr(snapshot, "DB_PATH", db_path)
    snapshot._local = threading.local()
    snapshot.init_db()
    yield snapshot
    snapshot._local = threading.local()


@pytest.fixture(scope="session", autouse=True)
def _never_touch_the_production_db(tmp_path_factory):
    """Redirect snapshot.DB_PATH away from production for the WHOLE session.

    `snapshot.DB_PATH` defaults to `/data/dlm.db` (snapshot.py:17) — the live
    state source — and is only overridden by the `DLM_DB_PATH` env var.
    `scripts/deploy-workers.sh:99` runs `python3 -m pytest tests/ -q` on S1 as
    the deploy gate and does NOT set that variable, so on S1 any test that
    reaches `snapshot.init_db()` without requesting the `db` fixture opens the
    production database. Today that is prevented only by validation ordering
    inside the endpoints under test — one reordered guard turns the deploy gate
    into a writer against live state.

    This is session-scoped and autouse so the redirect is in force before the
    first test runs, whether or not that test asks for `db`. The per-test `db`
    fixture still overrides it with its own isolated file; this is the floor,
    not a replacement.
    """
    from dlm.queue import snapshot

    fallback = tmp_path_factory.mktemp("dlm-session-db") / "fallback.db"
    real_default = snapshot.DB_PATH
    snapshot.DB_PATH = fallback
    snapshot._local = threading.local()
    assert "/data/dlm.db" not in str(snapshot.DB_PATH), (
        f"refusing to run the suite against {real_default}"
    )
    yield
    snapshot.DB_PATH = real_default
    snapshot._local = threading.local()


def _drop_cached_conn(snapshot):
    conn = getattr(snapshot._local, "conn", None)
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass
    snapshot._local.conn = None


@pytest.fixture()
def dlm_db(tmp_path, monkeypatch):
    """A real, empty SQLite database at a temp path. Returns the snapshot
    module, so tests write through the same API production does."""
    from dlm.queue import snapshot

    monkeypatch.setattr(snapshot, "DB_PATH", tmp_path / "dlm.db")
    _drop_cached_conn(snapshot)
    snapshot.init_db()
    yield snapshot
    _drop_cached_conn(snapshot)
