"""The event-delivery health signal, end to end across two processes.

`workers.event_buffer_pending` existed, was reported by the sidecar, and read
-1 on all 16 workers for months: nothing ever wrote the file the sidecar reads.
So the only instrument for "is this worker's event channel alive" was an
inference — Layer 1 sees files being written, Layer 2 sees no events — and that
inference is wrong on TB-scale files, because files are counted while being
WRITTEN and events are emitted only when a file FINISHES. The workaround was a
two-hour tolerance (health_verifier.EVENT_SILENCE_TOLERANCE), which is honest
but blind to any channel that dies for less than two hours.

These tests pin the three things that make the direct signal trustworthy:

  1. It reports BACKLOG, not buffer occupancy. Every flush cycle holds events
     for a few seconds by design; Layer 3 samples every 300s, so a raw count
     would call a healthy buffer broken most of the time.
  2. Unknown never collapses to zero. A dead worker leaves the status file
     frozen; a frozen 0 would read as "channel fine" forever.
  3. Both ends resolve the same path, from one definition.

Run: pytest tests/test_event_buffer_status.py -q
"""

from __future__ import annotations

import asyncio
import json
import time

from dlm.sidecar import monitor
from dlm.temporal import event_buffer
from dlm.temporal.event_buffer import STUCK_AFTER, EventBuffer


def _redirect(tmp_path, monkeypatch):
    """Point both ends at a temp file instead of /data/staging."""
    path = tmp_path / ".event_buffer_status"
    monkeypatch.setattr(event_buffer, "STATUS_PATH", path)
    monkeypatch.setattr(monitor, "STATUS_PATH", path)
    return path


# ── the two ends agree on one file ──────────────────────────────────


def test_writer_and_reader_resolve_the_same_path():
    """A divergence here fails nothing at runtime — it just makes the signal
    read `unknown` forever, which is indistinguishable from the bug this
    replaces. So it is pinned rather than left to review."""
    from dlm.constants import EVENT_BUFFER_STATUS_FILE

    assert event_buffer.STATUS_PATH == monitor.STATUS_PATH
    assert event_buffer.STATUS_PATH == EVENT_BUFFER_STATUS_FILE


# ── backlog, not occupancy ──────────────────────────────────────────


def test_freshly_emitted_events_are_not_a_backlog():
    """The normal state between two flushes. If this counted, the alert would
    fire on healthy routine — which is how a real alert gets ignored."""
    buf = EventBuffer("w1")
    for i in range(50):
        buf.emit("file_downloaded", {"i": i})

    assert buf.pending_count == 50
    assert buf.stuck_count == 0


def test_events_held_past_the_flush_window_are_a_backlog():
    buf = EventBuffer("w1")
    buf.emit("file_downloaded", {"i": 1})
    buf.emit("file_downloaded", {"i": 2})
    # Age them by hand rather than sleeping STUCK_AFTER seconds.
    for e in buf._buffer:
        e["timestamp"] = time.time() - (STUCK_AFTER + 1)
    buf.emit("file_downloaded", {"i": 3})       # this one is fresh

    assert buf.pending_count == 3
    assert buf.stuck_count == 2


def test_status_file_carries_the_backlog_and_the_sidecar_reports_it(
        tmp_path, monkeypatch):
    _redirect(tmp_path, monkeypatch)
    buf = EventBuffer("w1")
    buf.emit("file_downloaded", {"i": 1})
    for e in buf._buffer:
        e["timestamp"] = time.time() - (STUCK_AFTER + 1)

    buf.write_status()

    payload = json.loads(event_buffer.STATUS_PATH.read_text())
    assert payload["stuck"] == 1
    assert payload["pending"] == 1
    assert payload["server_key"] == "w1"
    assert monitor.get_event_buffer_pending() == 1


def test_a_delivered_buffer_reports_zero_not_unknown(tmp_path, monkeypatch):
    """0 and -1 must be distinguishable: 0 is evidence the channel works, -1 is
    absence of evidence, and the health check treats them very differently."""
    _redirect(tmp_path, monkeypatch)
    buf = EventBuffer("w1")

    buf.write_status()

    assert monitor.get_event_buffer_pending() == 0


def test_last_success_starts_unset_rather_than_now(tmp_path, monkeypatch):
    """Seeding it with now() would be a lie that survives into the first real
    failure — the buffer has delivered nothing yet."""
    _redirect(tmp_path, monkeypatch)
    buf = EventBuffer("w1")

    buf.write_status()

    assert json.loads(event_buffer.STATUS_PATH.read_text())["last_success"] is None


# ── unknown is not zero ─────────────────────────────────────────────


def test_missing_file_is_unknown(tmp_path, monkeypatch):
    _redirect(tmp_path, monkeypatch)
    assert monitor.get_event_buffer_pending() == -1


def test_a_stale_file_is_unknown_not_healthy(tmp_path, monkeypatch):
    """The worker process died. The file it left behind says 0 backlog, and
    that 0 is now meaningless — reading it as healthy is the exact
    false-negative this signal exists to remove."""
    path = _redirect(tmp_path, monkeypatch)
    EventBuffer("w1").write_status()
    assert monitor.get_event_buffer_pending() == 0        # fresh: believed

    old = time.time() - (monitor.STATUS_MAX_AGE + 60)
    import os
    os.utime(path, (old, old))

    assert monitor.get_event_buffer_pending() == -1       # stale: not believed


def test_unparseable_or_empty_file_is_unknown(tmp_path, monkeypatch):
    path = _redirect(tmp_path, monkeypatch)

    path.write_text("")
    assert monitor.get_event_buffer_pending() == -1

    path.write_text("{not json")
    assert monitor.get_event_buffer_pending() == -1


def test_a_bare_integer_is_still_accepted(tmp_path, monkeypatch):
    """The documented format before this change was a plain int. A worker on
    older code mid-deploy must not read as garbage."""
    path = _redirect(tmp_path, monkeypatch)
    path.write_text("7")

    assert monitor.get_event_buffer_pending() == 7


def test_write_status_never_raises_on_an_unwritable_path(tmp_path, monkeypatch):
    """This is an observability side channel. A full or read-only staging
    volume must not be able to stop event delivery."""
    monkeypatch.setattr(
        event_buffer, "STATUS_PATH", tmp_path / "nope" / "\0bad" / "status")
    EventBuffer("w1").write_status()      # must not raise


# ── the flush loop publishes a failing channel's own backlog ─────────


def test_the_flush_loop_publishes_status_even_when_the_post_fails(
        tmp_path, monkeypatch):
    """The case the whole signal exists for. The status write is in the loop's
    `finally`, outside the try that swallows POST errors, so a worker whose
    delivery is broken still reports its growing backlog."""
    _redirect(tmp_path, monkeypatch)
    monkeypatch.setattr(event_buffer, "FLUSH_INTERVAL", 0.01)
    monkeypatch.setattr(event_buffer, "RETRY_BACKOFF", [0.01])

    buf = EventBuffer("w1")

    async def always_fail(events):
        return False

    monkeypatch.setattr(buf, "_post_events", always_fail)

    async def run():
        await buf.start()
        buf.emit("file_downloaded", {"i": 1})
        for e in buf._buffer:
            e["timestamp"] = time.time() - (STUCK_AFTER + 1)
        await asyncio.sleep(0.2)
        await buf.stop()

    asyncio.run(run())

    # The event is back in the buffer (retained for retry) and reported as
    # backlog, not silently dropped.
    assert monitor.get_event_buffer_pending() == 1
    assert json.loads(event_buffer.STATUS_PATH.read_text())["last_success"] is None
