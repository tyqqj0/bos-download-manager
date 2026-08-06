"""Shared fixtures for the pytest suite.

Run: pytest tests/ -q  (pyproject sets pythonpath=["."], so bare pytest works
without a package install; python3 -m pytest works too)
"""

from __future__ import annotations

import threading

import pytest


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
