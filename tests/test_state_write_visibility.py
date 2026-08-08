"""A worker→S1 state write that S1 refused must not look like it succeeded.

These five activities are the whole worker→S1 write channel for shard and
task state. Workers cannot touch SQLite, so if one of these POSTs is rejected
the state simply never changes — and every one of them used to throw the
response away.

The trap is that rejection here is **HTTP 200**. `/api/shards/status` answers
`{"error": "Shard ... not found"}` with a 200 when a resume or reshard has
already replaced the shard rows an older workflow is still reporting against,
and `{"ignored": true}` when the parent task is terminal. So neither
`raise_for_status()` nor "did the request throw" can see it: the only signal
is the body. Without reading it, a lost `status -> done` write leaves a shard
`running` in the dashboard forever, with nothing in the worker log and
nothing in the workflow history to explain it.

What is pinned here is *visibility*, not control flow: these calls are
dispatched from the workflow without an explicit retry_policy, so they
inherit Temporal's unlimited-retry default and a raise would convert a
permanent rejection into a coordinator that retries forever. So the contract
is: log at ERROR, return normally. Both halves are asserted — a future
"improvement" that raises instead would stall a task.

Run: pytest tests/test_state_write_visibility.py -q
"""

from __future__ import annotations

import asyncio
import logging

import pytest

from dlm.temporal import activities


def _call(coro):
    return asyncio.run(coro)


class _Resp:
    """Minimal stand-in for requests.Response."""

    def __init__(self, body, status_code=200):
        self._body = body
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        if isinstance(self._body, Exception):
            raise self._body
        return self._body


@pytest.fixture
def post(monkeypatch):
    """Capture outgoing POSTs and script the response body."""
    import requests

    sent = []
    box = {"resp": _Resp({"ok": True})}

    def fake_post(url, json=None, timeout=None):
        sent.append((url, json))
        return box["resp"]

    monkeypatch.setattr(requests, "post", fake_post)
    return type("P", (), {"sent": sent, "box": box})()


# Every fire-and-forget write, with the arguments the workflow passes.
WRITES = {
    "update_shard_status": lambda: activities.update_shard_status("s-1", "done"),
    "report_shard_progress": lambda: activities.report_shard_progress("s-1", 3, 4096, 1.5),
    "report_resume_info": lambda: activities.report_resume_info("t-1", 120, 45.5),
    "aggregate_task_from_shards": lambda: activities.aggregate_task_from_shards("t-1"),
    "assign_shard_server": lambda: activities.assign_shard_server("s-1", "w3"),
}


@pytest.mark.parametrize("name", sorted(WRITES))
def test_a_rejected_write_is_logged_at_error(post, caplog, name):
    """The 200-with-error case: the only trace this write was lost."""
    post.box["resp"] = _Resp({"error": "Shard s-1 not found"})

    with caplog.at_level(logging.ERROR, logger=activities.logger.name):
        _call(WRITES[name]())

    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert errors, f"{name} swallowed a rejected write"
    assert "Shard s-1 not found" in errors[0].getMessage()


@pytest.mark.parametrize("name", sorted(WRITES))
def test_a_rejected_write_does_not_raise(post, name):
    """A raise would be retried forever: these are dispatched without a
    retry_policy, so they inherit Temporal's unlimited-retry default, and a
    "shard not found" rejection never clears on its own. A stalled
    coordinator is worse than a logged miss."""
    post.box["resp"] = _Resp({"error": "Shard s-1 not found"})

    _call(WRITES[name]())  # must return normally


@pytest.mark.parametrize("name", sorted(WRITES))
def test_the_identifier_is_in_the_message(post, caplog, name):
    """A log line that says "write rejected" without saying which row is not
    actionable on a 16-host fleet reporting continuously."""
    post.box["resp"] = _Resp({"error": "nope"})

    with caplog.at_level(logging.ERROR, logger=activities.logger.name):
        _call(WRITES[name]())

    msg = caplog.records[0].getMessage()
    assert ("s-1" in msg) or ("t-1" in msg)


@pytest.mark.parametrize("name", sorted(WRITES))
def test_a_successful_write_logs_nothing(post, caplog, name):
    """The happy path runs on every progress ping from every shard. Logging
    there would bury the rejections this module exists to surface."""
    post.box["resp"] = _Resp({"ok": True})

    with caplog.at_level(logging.DEBUG, logger=activities.logger.name):
        _call(WRITES[name]())

    assert not caplog.records


@pytest.mark.parametrize("name", sorted(WRITES))
def test_an_ignored_write_is_a_warning_not_an_error(post, caplog, name):
    """`ignored` means S1 deliberately declined: the parent task is
    paused/revoked/done and a late report must not resurrect it. That is
    correct behaviour, so it must not page anyone — but it is still the
    reason a state change did not land, so it is not silent either."""
    post.box["resp"] = _Resp({"ok": True, "ignored": True})

    with caplog.at_level(logging.DEBUG, logger=activities.logger.name):
        _call(WRITES[name]())

    assert [r.levelno for r in caplog.records] == [logging.WARNING]


@pytest.mark.parametrize("name", sorted(WRITES))
def test_a_transport_failure_is_logged_too(post, caplog, name):
    """An HTTP 502 from a proxy in front of S1 loses the write just as
    thoroughly as a rejection, and `raise_for_status` is the only thing that
    can see it."""
    post.box["resp"] = _Resp({}, status_code=502)

    with caplog.at_level(logging.ERROR, logger=activities.logger.name):
        _call(WRITES[name]())

    assert any(r.levelno >= logging.ERROR for r in caplog.records)


@pytest.mark.parametrize("name", sorted(WRITES))
def test_an_unparseable_body_is_logged_not_raised(post, caplog, name):
    """An HTML error page answers 200 with a body `.json()` cannot parse. That
    must not become an exception inside a fire-and-forget activity."""
    post.box["resp"] = _Resp(ValueError("Expecting value: line 1 column 1"))

    with caplog.at_level(logging.ERROR, logger=activities.logger.name):
        _call(WRITES[name]())

    assert any(r.levelno >= logging.ERROR for r in caplog.records)


@pytest.mark.parametrize("name", sorted(WRITES))
def test_a_non_dict_body_does_not_crash_the_check(post, caplog, name):
    """`.get()` on a list is an AttributeError. Reached if a route ever
    returns a bare array."""
    post.box["resp"] = _Resp([1, 2, 3])

    with caplog.at_level(logging.ERROR, logger=activities.logger.name):
        _call(WRITES[name]())

    assert any(r.levelno >= logging.ERROR for r in caplog.records)


def test_the_resume_info_message_carries_the_numbers(post, caplog):
    """A4 verifies "we did not re-download what BOS already has" from this
    row. When the write is lost, the log has to hold the figures the row
    should have had, or the evidence is gone for good."""
    post.box["resp"] = _Resp({"error": "no such task"})

    with caplog.at_level(logging.ERROR, logger=activities.logger.name):
        _call(activities.report_resume_info("t-1", 120, 45.5))

    msg = caplog.records[0].getMessage()
    assert "120" in msg and "45.5" in msg


# ── create_shards_in_db keeps raising: it has a return value ────────────────


@pytest.fixture
def shard_create(monkeypatch):
    import requests

    box = {"resp": _Resp({"shard_ids": ["s-1"]})}
    monkeypatch.setattr(requests, "post",
                        lambda url, json=None, timeout=None: box["resp"])
    return box


def test_shard_creation_still_raises_on_a_rejection(shard_create):
    """Unlike the writes above this one has a return value the workflow
    partitions against, so failing loudly is right — there is no shard id to
    carry on with."""
    shard_create["resp"] = _Resp({"error": "task not found"})

    with pytest.raises(RuntimeError, match="task not found"):
        _call(activities.create_shards_in_db("t-1", [{}]))


def test_a_body_with_neither_key_names_the_real_problem(shard_create):
    """A proxy error page or an S1 500 has no `error` key either. Indexing
    straight into `shard_ids` made that a bare KeyError, which reads like a
    coordinator bug rather than "the request never reached the route"."""
    shard_create["resp"] = _Resp({"detail": "Internal Server Error"}, status_code=500)

    with pytest.raises(RuntimeError) as exc:
        _call(activities.create_shards_in_db("t-1", [{}]))

    msg = str(exc.value)
    assert "shard_ids" in msg and "500" in msg


def test_the_happy_path_still_returns_the_ids(shard_create):
    shard_create["resp"] = _Resp({"shard_ids": ["s-1", "s-2"]})

    assert _call(activities.create_shards_in_db("t-1", [{}, {}])) == ["s-1", "s-2"]
