"""Shared fixtures for the pytest suite.

Run: pytest tests/ -q  (pyproject sets pythonpath=["."], so bare pytest works
without a package install; python3 -m pytest works too)
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
