"""Shared fixtures for the pytest suite.

Run: python3 -m pytest tests/ -q   (python -m, not bare pytest — dlm isn't
installed, and -m prepends the repo root running dlm.queue.snapshot etc.
importable without a package install)
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
