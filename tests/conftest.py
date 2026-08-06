"""Shared test fixtures.

`snapshot._conn()` caches its connection in a thread-local keyed by the path it
opened, so rebinding `snapshot.DB_PATH` is enough to redirect every thread —
including the web routes' long-lived module-level executors, which is what broke
two suites on 2026-08-07 (test_retry_false_done passed alone and failed after
test_coordinator_routing with "Task t-failed not found"). Use the `dlm_db`
fixture rather than patching DB_PATH by hand, so the schema exists and the
main thread's connection is dropped on the way out.
"""

from __future__ import annotations

import pytest


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
