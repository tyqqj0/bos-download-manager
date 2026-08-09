"""Layer 3 correlation, and the invariant that it never forks.

The SSH fan-out this module used to run took the whole control plane down
for 24 hours on 2026-07-31: a fork from the event-loop thread of a
23-thread process produced a child that deadlocked before exec(), so the
loop never called accept() again. The no-subprocess test below is the
guard against that regressing — it is the only test here that is about a
past outage rather than about behaviour.

Run: python3 -m pytest tests/test_health_verifier.py -q
"""

from __future__ import annotations

import time

from dlm.web.health_verifier import correlate_layers, work_by_server


def _worker(key, *, last_seen=None, files5=None, conns=None, alive=None,
            backlog=None):
    """`backlog` is event_buffer_pending. None is what a worker on older code
    or with an unreadable status file reports, and it is deliberately NOT 0."""
    return {
        "server_key": key,
        "last_seen": time.time() if last_seen is None else last_seen,
        "files_last_5min": files5,
        "https_connections": conns,
        "download_process_alive": alive,
        "event_buffer_pending": backlog,
    }


def _types(anomalies):
    return {a["type"] for a in anomalies}


def test_module_never_spawns_a_process():
    """The outage was a fork on the event loop. Nothing here may fork again.

    Grepping for `subprocess` alone is not enough: every helper in
    `dlm.core.ssh` reaches `subprocess.run` one frame down (ssh.py:42, :182),
    so `from ..core.ssh import ssh_exec` forks from the loop thread while
    this module's own source stays clean. The 2026-07-31 hang came from a
    fork in the web process; where the `subprocess` token was typed does not
    change that. So the ssh surface is named explicitly.
    """
    import ast
    import inspect

    from dlm.web import health_verifier

    source = inspect.getsource(health_verifier)
    body = source.split('"""', 2)[-1]  # skip the docstring that explains why
    for forbidden in ("subprocess", "os.system", "os.fork", "os.popen", "os.spawn"):
        assert forbidden not in body, f"{forbidden} reintroduces the 2026-07-31 hang"

    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            module = node.module or ""
            assert not module.endswith("core.ssh") and module != "ssh", (
                f"imports from {module} — every ssh helper forks via "
                f"subprocess.run, which is the hang this file exists to prevent"
            )
            for alias in node.names:
                assert not alias.name.startswith("ssh_"), (
                    f"imports {alias.name}: forks via subprocess.run one frame down"
                )
        if isinstance(node, ast.Import):
            for alias in node.names:
                assert "core.ssh" not in alias.name, (
                    f"imports {alias.name}: every helper in it forks"
                )


def test_sharded_work_is_found_through_the_shard_row():
    """A sharded task's own row has server=NULL — the shard carries the server."""
    tasks = [{"id": "t-1", "name": "Beta", "status": "downloading", "server": None}]
    shards = [{"id": "s-t-1-0", "task_id": "t-1", "server": "w3", "updated_at": 100.0}]

    held = work_by_server(tasks, shards)

    assert held["w3"]["name"] == "Beta"


def test_legacy_single_node_task_is_still_found():
    tasks = [{"id": "t-1", "name": "Old", "status": "downloading",
              "server": "w1", "updated_at": 100.0}]

    assert work_by_server(tasks, [])["w1"]["name"] == "Old"


def test_offline_worker_is_left_to_the_doctor():
    """Offline is already `offline_workers`; repeating it double-alerts one fact."""
    now = time.time()
    workers = [_worker("w1", last_seen=now - 9999, files5=0, conns=0)]

    assert correlate_layers(workers, [], [], now) == []


def test_worker_without_sidecar_metrics_reports_blindness_not_a_stall():
    now = time.time()
    workers = [_worker("bj1")]  # basic heartbeat only: every metric is None
    tasks = [{"id": "t-1", "name": "MS", "status": "downloading", "server": None}]
    shards = [{"id": "s-t-1-0", "task_id": "t-1", "server": "bj1", "updated_at": 0}]

    types = _types(correlate_layers(workers, tasks, shards, now))

    assert types == {"sidecar_missing"}


def test_stall_needs_both_no_files_and_stale_progress():
    """Fresh progress is not a stall, whatever the file counter says.

    This replaces the old large-file exemption, which needed an SSH `find`
    to size the in-flight file.
    """
    now = time.time()
    workers = [_worker("w1", files5=0, conns=8, alive=1)]
    tasks = [{"id": "t-1", "name": "Big", "status": "downloading", "server": None}]
    fresh = [{"id": "s-t-1-0", "task_id": "t-1", "server": "w1", "updated_at": now - 5}]

    assert correlate_layers(workers, tasks, fresh, now) == []

    stale = [{"id": "s-t-1-0", "task_id": "t-1", "server": "w1", "updated_at": now - 3600}]
    assert _types(correlate_layers(workers, tasks, stale, now)) == {"possible_stall"}


def test_no_files_no_connections_and_stale_is_a_confirmed_stall():
    now = time.time()
    workers = [_worker("w1", files5=0, conns=0, alive=1)]
    tasks = [{"id": "t-1", "name": "Dead", "status": "downloading", "server": None}]
    shards = [{"id": "s-t-1-0", "task_id": "t-1", "server": "w1", "updated_at": now - 3600}]

    assert _types(correlate_layers(workers, tasks, shards, now)) == {
        "download_stalled_confirmed"
    }


def test_dead_download_process_is_flagged_even_while_idle():
    now = time.time()
    workers = [_worker("w1", files5=0, conns=0, alive=0)]

    assert _types(correlate_layers(workers, [], [], now)) == {
        "process_dead_undetected"
    }


# ── layer2_delivery_broken: silence vs. not-yet-spoken ───────────────────
#
# This alert had no test and, under pool mode, no accuracy. It compares "files
# arrived in the last 5 min" against "events arrived in the last 10 min", which
# holds for sharded mode — a worker took one shard and kept it for hours, so it
# was only ever fresh right after a deploy. Pool mode recruits a worker the
# instant the dispatch window widens. On 2026-08-09 molmobot's window went 1→4
# and all four new workers were flagged 6 minutes into their first batch, while
# a direct read of the events table showed their events arriving seconds later.
# A warning that fires on healthy routine trains everyone to ignore it, which
# costs exactly what the alert was built to buy.

from dlm.queue import snapshot  # noqa: E402
from dlm.web.health_verifier import (  # noqa: E402
    EVENT_BACKLOG_TOLERANCE, EVENT_SILENCE_TOLERANCE, _as_epoch,
)


def _live_task(task_id="t-live"):
    snapshot.upsert_task({
        "id": task_id, "name": "X", "repo_id": "org/r", "source": "hf",
        "type": "dataset", "category": "other", "status": "downloading",
        "server": None, "priority": 0, "size_gb": 1.0, "downloaded_gb": 0.0,
        "progress_pct": 0, "speed_mbps": 0, "retry_count": 0,
    })
    return task_id


def _batch(shard_id, task_id, server, started_ago, status="running"):
    """A pool batch row. started_at is ISO TEXT in this table — the column next
    to it, updated_at, is an epoch float, so the type is worth pinning."""
    from datetime import datetime, timezone
    started = datetime.fromtimestamp(
        time.time() - started_ago, tz=timezone.utc).isoformat(timespec="seconds")
    snapshot.upsert_shard({
        "id": shard_id, "task_id": task_id, "shard_index": 0, "server": server,
        "status": status, "total_files": 10, "done_files": 0,
        "started_at": started, "updated_at": time.time(),
    })


def _busy(key, backlog=None):
    """A worker whose Layer 1 activity is above the floor."""
    return _worker(key, files5=50, conns=8, alive=1, backlog=backlog)


def _events_table():
    """The events table is created lazily by POST /api/events, so a DB no
    worker has ever reported to does not have one — and correlate_layers then
    reads recent_events as -1 and stays silent. These tests are about the
    grace, not about that fallback, so they start from a table that exists."""
    conn = snapshot._conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id TEXT,
            server_key TEXT NOT NULL,
            event_type TEXT NOT NULL,
            data TEXT,
            timestamp REAL NOT NULL,
            created_at REAL DEFAULT (strftime('%s', 'now'))
        )
    """)
    conn.commit()


def test_freshly_recruited_worker_is_not_reported_as_broken(dlm_db):
    _events_table()
    task_id = _live_task()
    _batch("s-fresh", task_id, "w1", started_ago=120)

    anomalies = correlate_layers([_busy("w1")], [snapshot.get_task(task_id)], [])

    assert "layer2_delivery_broken" not in _types(anomalies), anomalies


def test_long_running_silent_worker_is_still_reported(dlm_db):
    """The grace must not become an amnesty."""
    _events_table()
    task_id = _live_task()
    _batch("s-old", task_id, "w1", started_ago=EVENT_SILENCE_TOLERANCE * 3)

    anomalies = correlate_layers([_busy("w1")], [snapshot.get_task(task_id)], [])

    assert "layer2_delivery_broken" in _types(anomalies), anomalies


def test_a_worker_cycling_short_batches_gets_no_permanent_excuse(dlm_db):
    """Why the grace reads the OLDEST row, not the current one.

    Pool hands out one batch at a time. Judging freshness by the batch a worker
    happens to hold now would reset the clock every few minutes, so a worker
    whose delivery is permanently broken would be permanently excused. Its
    completed rows are the evidence that it has been here long enough.
    """
    _events_table()
    task_id = _live_task()
    _batch("s-done-1", task_id, "w1",
           started_ago=EVENT_SILENCE_TOLERANCE * 2, status="done")
    _batch("s-now", task_id, "w1", started_ago=30)

    anomalies = correlate_layers([_busy("w1")], [snapshot.get_task(task_id)], [])

    assert "layer2_delivery_broken" in _types(anomalies), anomalies


def test_delivered_events_clear_it_regardless_of_age(dlm_db):
    _events_table()
    task_id = _live_task()
    _batch("s-old2", task_id, "w1", started_ago=EVENT_SILENCE_TOLERANCE * 3)
    conn = snapshot._conn()
    conn.execute(
        "INSERT INTO events (task_id, server_key, event_type, data, timestamp) "
        "VALUES (?, ?, ?, ?, ?)",
        (task_id, "w1", "file_done", "{}", time.time() - 30))
    conn.commit()

    anomalies = correlate_layers([_busy("w1")], [snapshot.get_task(task_id)], [])

    assert "layer2_delivery_broken" not in _types(anomalies), anomalies


def test_worker_with_no_work_row_gets_no_grace(dlm_db):
    """Absent evidence that it only just started, silence is judged as before —
    a grace that defaults to ON would silently disable the alert fleet-wide."""
    _events_table()
    _live_task()

    anomalies = correlate_layers([_busy("w9")], [], [])

    assert "layer2_delivery_broken" in _types(anomalies), anomalies


def test_started_at_accepts_both_column_types():
    """shards.started_at is ISO text; sharded-era rows and updated_at are epoch
    floats. Comparing a str to a float raises, so both must decode."""
    from datetime import datetime, timezone

    now = time.time()
    assert abs(_as_epoch(now) - now) < 1

    iso = "2026-08-09T01:52:07+00:00"
    expected = datetime(2026, 8, 9, 1, 52, 7, tzinfo=timezone.utc).timestamp()
    assert _as_epoch(iso) == expected

    assert _as_epoch(None) is None
    assert _as_epoch("not a date") is None


def test_a_long_gap_between_finished_files_is_not_broken_delivery(dlm_db):
    """The w5 case, and why the tolerance is hours rather than minutes.

    The trigger (files_last_5min = `find -mmin -5`) counts files being WRITTEN;
    the events counted against it are emitted only when a file FINISHES. w5 was
    flagged on 2026-08-09 at done_bytes 8.7/34.3 GB and 176 Mbps with four
    ~1.4 GB files in flight — nothing had finished for 16 minutes, its buffer
    flushes every 5s, and its last events matched its previous batch's last
    completion to the second. Nothing was late; there was nothing to send.

    shards.done_files cannot narrow this: mid-run it holds only skipped_files
    (activities.py:1406), so a "did a file complete recently" gate reads 0 on
    every running batch and silently disables the alert fleet-wide. The
    tolerance is the honest instrument instead.
    """
    _events_table()
    task_id = _live_task()
    _batch("s-bigfiles", task_id, "w5", started_ago=1800)  # 30 min in, silent

    anomalies = correlate_layers([_busy("w5")], [snapshot.get_task(task_id)], [])

    assert "layer2_delivery_broken" not in _types(anomalies), anomalies


def test_silence_past_the_tolerance_is_still_reported(dlm_db):
    """The tolerance must not become an amnesty: over two hours even a very
    large file completes, so total silence is evidence of a dead channel."""
    _events_table()
    task_id = _live_task()
    _batch("s-mute", task_id, "w5", started_ago=EVENT_SILENCE_TOLERANCE + 600)

    anomalies = correlate_layers([_busy("w5")], [snapshot.get_task(task_id)], [])

    assert "layer2_delivery_broken" in _types(anomalies), anomalies


# ── the direct signal: event_buffer_pending (#87) ────────────────────
#
# The two-hour tolerance above is an inference with a known cost — a channel
# that dies for under two hours goes unreported. `event_buffer_pending` now
# carries the buffer's real backlog (events held past event_buffer.STUCK_AFTER),
# which is direct evidence rather than inference, so where it exists the wait
# drops to EVENT_BACKLOG_TOLERANCE. Where it does not exist it must change
# nothing: -1/None means the sidecar could not read the file, and unknown is
# not zero.


def test_a_reported_backlog_shortens_the_wait(dlm_db):
    """25 minutes of silence with events piling up in the buffer is reported —
    under the old inference-only rule this worker had another 95 minutes of
    grace before anyone would hear about it."""
    _events_table()
    task_id = _live_task()
    _batch("s-backed-up", task_id, "w5",
           started_ago=EVENT_BACKLOG_TOLERANCE + 600)

    anomalies = correlate_layers(
        [_busy("w5", backlog=1200)], [snapshot.get_task(task_id)], [])

    types = _types(anomalies)
    assert "layer2_delivery_broken" in types, anomalies
    assert "1200 events backlogged" in next(
        a["message"] for a in anomalies if a["type"] == "layer2_delivery_broken")


def test_an_unknown_backlog_keeps_the_long_tolerance(dlm_db):
    """Same 25 minutes of silence, but the worker reports -1: its sidecar could
    not read the status file, or it is on older code. Judging it on a signal
    nobody sent would re-create the false alarm on TB-scale files."""
    _events_table()
    task_id = _live_task()
    _batch("s-unknown", task_id, "w5",
           started_ago=EVENT_BACKLOG_TOLERANCE + 600)

    for reported in (-1, None):
        anomalies = correlate_layers(
            [_busy("w5", backlog=reported)], [snapshot.get_task(task_id)], [])
        assert "layer2_delivery_broken" not in _types(anomalies), (reported, anomalies)


def test_an_empty_backlog_is_evidence_of_a_working_channel(dlm_db):
    """backlog == 0 means everything emitted was delivered, so silence is real
    idleness — a big file in flight, nothing finished. This is the w5 case, and
    it must stay quiet even though 0 is a known value rather than unknown."""
    _events_table()
    task_id = _live_task()
    _batch("s-quiet", task_id, "w5",
           started_ago=EVENT_BACKLOG_TOLERANCE + 600)

    anomalies = correlate_layers(
        [_busy("w5", backlog=0)], [snapshot.get_task(task_id)], [])

    assert "layer2_delivery_broken" not in _types(anomalies), anomalies


def test_delivered_events_clear_it_even_with_a_backlog(dlm_db):
    """A backlog alone is not breakage — a burst can outrun one flush cycle.
    Events actually arriving is the answer to "is the channel alive"."""
    _events_table()
    task_id = _live_task()
    _batch("s-burst", task_id, "w5",
           started_ago=EVENT_BACKLOG_TOLERANCE + 600)
    conn = snapshot._conn()
    conn.execute(
        "INSERT INTO events (task_id, server_key, event_type, data, timestamp) "
        "VALUES (?, ?, ?, ?, ?)",
        (task_id, "w5", "file_done", "{}", time.time() - 60))
    conn.commit()

    anomalies = correlate_layers(
        [_busy("w5", backlog=900)], [snapshot.get_task(task_id)], [])

    assert "layer2_delivery_broken" not in _types(anomalies), anomalies


def test_a_backlogged_worker_that_just_started_still_gets_its_grace(dlm_db):
    """The tenure grace applies to the short window too: a worker 3 minutes into
    its first batch has not been silent, it has not had time to speak."""
    _events_table()
    task_id = _live_task()
    _batch("s-new-backlog", task_id, "w5", started_ago=180)

    anomalies = correlate_layers(
        [_busy("w5", backlog=50)], [snapshot.get_task(task_id)], [])

    assert "layer2_delivery_broken" not in _types(anomalies), anomalies
