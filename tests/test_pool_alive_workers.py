"""T3 — pool_alive_workers: alive ∩ worker_serves worker count for the pool
window loop, plus the /api/pool/alive-workers endpoint it queries.

Run: python3 -m pytest tests/test_pool_alive_workers.py -q   (needs temporalio)
"""

from __future__ import annotations

import asyncio
import time

import pytest

from dlm.web.fleet import WORKER_TIMEOUT


def _call(coro):
    return asyncio.run(coro)


# ── GET /api/pool/alive-workers ─────────────────────────────────────────


def test_alive_workers_endpoint_counts_by_source(db):
    from dlm.web.routes.queue import pool_alive_workers_api

    db.update_worker(hostname="w1@temporal", server_key="w1")
    db.update_worker(hostname="w2@temporal", server_key="w2")
    db.update_worker(hostname="bj1@temporal", server_key="bj1")

    hf = _call(pool_alive_workers_api(source="hf"))
    ms = _call(pool_alive_workers_api(source="modelscope"))

    assert hf["count"] == 2
    assert sorted(hf["workers"]) == ["w1", "w2"]
    assert ms["count"] == 1
    assert ms["workers"] == ["bj1"]


def test_alive_workers_endpoint_ignores_busy_and_disk(db):
    """Deliberately not idle-workers: a busy, disk-starved worker still
    counts — the pool sizes its window from total serving capacity."""
    from dlm.web.routes.queue import pool_alive_workers_api

    db.update_worker(hostname="w1@temporal", server_key="w1", disk_free_gb=0.1)
    db.upsert_task({"id": "t1", "name": "t1", "status": "downloading",
                     "server": "w1", "priority": 5, "created_at": "now"})

    result = _call(pool_alive_workers_api(source="hf"))

    assert result["count"] == 1
    assert result["workers"] == ["w1"]


def test_alive_workers_endpoint_excludes_stale_heartbeat(db):
    from dlm.web.routes.queue import pool_alive_workers_api

    db.update_worker(hostname="w1@temporal", server_key="w1")
    conn = db._conn()
    conn.execute(
        "UPDATE workers SET last_seen = ? WHERE server_key = 'w1'",
        (time.time() - WORKER_TIMEOUT - 1,),
    )
    conn.commit()

    result = _call(pool_alive_workers_api(source="hf"))

    assert result == {"count": 0, "workers": []}


def test_alive_workers_endpoint_dedupes_multi_hostname_worker(db):
    """Same worker reporting under two hostnames (temporal + sidecar) must
    count once, not twice — the exact bug dedupe_workers exists to prevent."""
    from dlm.web.routes.queue import pool_alive_workers_api

    db.update_worker(hostname="w1@temporal", server_key="w1")
    db.update_worker(hostname="w1@sidecar", server_key="w1")

    result = _call(pool_alive_workers_api(source="hf"))

    assert result["count"] == 1


# ── pool_alive_workers activity (HTTP call mocked) ──────────────────────


def _run_activity(coro_fn, *args, **kwargs):
    from temporalio.testing import ActivityEnvironment

    env = ActivityEnvironment()

    async def main():
        return await env.run(coro_fn, *args, **kwargs)

    return asyncio.run(main())


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


def test_pool_alive_workers_activity_calls_the_right_endpoint(monkeypatch):
    from dlm.temporal import activities

    calls = []

    def fake_get(url, params=None, timeout=None):
        calls.append((url, params))
        return _FakeResponse({"count": 5, "workers": ["bj1", "bj2"]})

    monkeypatch.setattr("requests.get", fake_get)

    result = _run_activity(activities.pool_alive_workers, "modelscope")

    assert result == 5
    assert len(calls) == 1
    url, params = calls[0]
    assert url.endswith("/api/pool/alive-workers")
    assert params == {"source": "modelscope"}


def test_pool_alive_workers_activity_returns_zero_on_empty_count(monkeypatch):
    from dlm.temporal import activities

    monkeypatch.setattr("requests.get", lambda *a, **k: _FakeResponse({"count": 0, "workers": []}))

    result = _run_activity(activities.pool_alive_workers, "hf")

    assert result == 0


def test_pool_alive_workers_activity_raises_on_http_error(monkeypatch):
    from dlm.temporal import activities

    class _Failing(_FakeResponse):
        def raise_for_status(self):
            raise RuntimeError("503")

    monkeypatch.setattr("requests.get", lambda *a, **k: _Failing({}))

    with pytest.raises(RuntimeError):
        _run_activity(activities.pool_alive_workers, "hf")
