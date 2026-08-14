"""R1-R9: the gated-repo approval hold, end to end.

The failure this feature exists to stop, measured 2026-08-11/12: a gated HF
repo was accepted, chunked into pool batches, and each batch discovered its own
403 and burned its 3 Temporal attempts — and a pool batch that exhausts its
attempts is permanently `failed` and never re-dispatched. assembly101 lost 20
of 113 batches inside one ~6-hour approval window; Franka-Dataset had started
the same slide (1 of 696 in 8 hours).

So the tests here are mostly about *not* letting a task run: the add-time
probe, the hold that stops a task already running, the self-clearing recheck,
and the release button. The parts worth pinning hardest are the ones where a
plausible-looking implementation is silently wrong:

  * `README.md` as the probe target (gated repos serve it anonymously with
    200 — it reports success for exactly the repos this catches);
  * releasing a hold on `OK` from a source the probe never looked at
    (`check_repo_access` returns OK for every non-hf source, meaning "no
    opinion", and releasing on it re-enters the burn loop every 30 minutes);
  * a progress bar reading `downloaded_gb / size_gb` on a resumed task
    (after a resume `size_gb` is only what was LEFT — assembly101 showed
    38.1% when it was 66.2% done).

Run: python3 -m pytest tests/test_approval_hold.py -q
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from dlm.web import preflight as _preflight_at_import

# The real, unstubbed probe, captured at import time.
#
# conftest's autouse `_the_add_time_preflight_never_hits_the_network` fixture
# replaces `preflight.probe_hf_repo` with a canned UNKNOWN for every test in the
# suite — which is right for the add-route tests below and fatal for the ones in
# section A, whose whole subject IS that function. Module import happens at
# collection time, before any fixture runs, so this binding is the genuine one;
# the tests in section A stub `requests` instead and call through this.
_REAL_PROBE = _preflight_at_import.probe_hf_repo

STATIC = Path(__file__).resolve().parents[1] / "dlm" / "web" / "static"


def _call(coro):
    return asyncio.run(coro) if asyncio.iscoroutine(coro) else coro


def _task(db, task_id, *, status="downloading", mode="pool", source="hf",
          repo_id="org/gated", priority=5):
    db.upsert_task({"id": task_id, "name": task_id, "repo_id": repo_id,
                    "status": status, "priority": priority, "source": source,
                    "type": "dataset", "dispatch_mode": mode,
                    "created_at": "2026-01-01T00:00:00+00:00"})


class _Resp:
    """Minimal stand-in for a requests response. Only the two attributes
    probe_hf_repo actually reads."""

    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


def _stub_requests(monkeypatch, *, head=None, get=None, calls=None):
    """Replace requests.head/get for preflight's own module import.

    preflight imports `requests` inside probe_hf_repo, so patching the
    `requests` module itself is what reaches it. `head`/`get` are callables of
    (url) -> _Resp, or a dict keyed by url suffix.
    """
    import requests

    def _resolve(spec, url):
        if callable(spec):
            return spec(url)
        for suffix, resp in (spec or {}).items():
            if url.endswith(suffix):
                return resp
        raise AssertionError(f"unstubbed URL: {url}")

    def fake_head(url, **kw):
        if calls is not None:
            calls.append(("head", url, kw))
        return _resolve(head, url)

    def fake_get(url, **kw):
        if calls is not None:
            calls.append(("get", url, kw))
        return _resolve(get, url)

    monkeypatch.setattr(requests, "head", fake_head)
    monkeypatch.setattr(requests, "get", fake_get)
    monkeypatch.setenv("HF_TOKEN", "test-token-not-a-real-one")


# ── A. the probe itself ──────────────────────────────────────────────────


def test_403_on_the_resolve_url_is_needs_approval(monkeypatch):
    from dlm.web import preflight

    _stub_requests(monkeypatch, head=lambda url: _Resp(403))
    result = _REAL_PROBE("org/gated")

    assert result.outcome == preflight.NEEDS_APPROVAL
    assert result.blocks_add is True
    assert result.status_code == 403


@pytest.mark.parametrize("status", [200, 302, 307])
def test_authorised_2xx_or_redirect_is_ok(monkeypatch, status):
    """allow_redirects=False is deliberate — a 302 to the CDN already proves
    authorisation, and following it would download bytes and log a multi-KB
    signed URL. So a 3xx must read as OK, not as "inconclusive"."""
    from dlm.web import preflight

    calls = []
    _stub_requests(monkeypatch, head=lambda url: _Resp(status), calls=calls)
    result = _REAL_PROBE("org/open")

    assert result.outcome == preflight.OK
    assert result.blocks_add is False
    assert calls[0][2]["allow_redirects"] is False


def test_401_is_our_token_problem_and_never_blocks(monkeypatch):
    """With a token attached, 401 is a bad token, not a gate. Blaming the repo
    for our own misconfiguration would refuse adds fleet-wide the moment
    HF_TOKEN expired."""
    from dlm.web import preflight

    _stub_requests(monkeypatch, head=lambda url: _Resp(401))
    result = _REAL_PROBE("org/whatever")

    assert result.outcome == preflight.UNKNOWN
    assert result.blocks_add is False


def test_network_failure_is_unknown_and_never_blocks(monkeypatch):
    from dlm.web import preflight

    def boom(url):
        raise OSError("connection reset")

    _stub_requests(monkeypatch, head=boom)
    result = _REAL_PROBE("org/whatever")

    assert result.outcome == preflight.UNKNOWN
    assert result.blocks_add is False


def test_probe_never_targets_readme(monkeypatch):
    """The single most important assertion in this file. Gated repos serve
    README.md anonymously with 200, so a probe pointed at it reports success
    for precisely the repos this check exists to catch."""
    from dlm.web import preflight

    calls = []
    _stub_requests(monkeypatch, head=lambda url: _Resp(403), calls=calls)
    _REAL_PROBE("org/gated")

    assert calls, "probe made no request at all"
    for _, url, _ in calls:
        assert "README" not in url.upper(), url
    assert preflight.PRIMARY_PROBE_PATH == ".gitattributes"


def test_gitattributes_404_falls_back_to_the_tree_api(monkeypatch):
    """.gitattributes missing says nothing about the repo, so the tree API
    decides — and then the smallest real file is re-probed, because the tree
    API answers "does it exist", not "may we download it"."""
    from dlm.web import preflight

    calls = []
    _stub_requests(
        monkeypatch,
        head={".gitattributes": _Resp(404), "data/small.txt": _Resp(403)},
        get={"/tree/main": _Resp(200, [
            {"type": "file", "path": "data/big.bin", "size": 900},
            {"type": "file", "path": "data/small.txt", "size": 12},
            {"type": "directory", "path": "data"},
        ])},
        calls=calls,
    )
    result = _REAL_PROBE("org/no-gitattributes")

    assert result.outcome == preflight.NEEDS_APPROVAL
    probed = [url for kind, url, _ in calls if kind == "head"]
    assert probed[-1].endswith("data/small.txt"), probed


def test_repo_absent_everywhere_is_not_found(monkeypatch):
    from dlm.web import preflight

    _stub_requests(monkeypatch,
                   head=lambda url: _Resp(404),
                   get=lambda url: _Resp(404))
    result = _REAL_PROBE("org/typo")

    assert result.outcome == preflight.NOT_FOUND
    assert result.blocks_add is True


def test_empty_repo_is_reachable_not_gated(monkeypatch):
    """fastumi100k is a real one: README + a PNG and nothing else. There is
    nothing gated about having no files."""
    from dlm.web import preflight

    _stub_requests(monkeypatch,
                   head={".gitattributes": _Resp(404)},
                   get={"/tree/main": _Resp(200, [])})
    result = _REAL_PROBE("org/empty")

    assert result.outcome == preflight.OK


def test_no_token_skips_the_probe_rather_than_guessing(monkeypatch):
    from dlm.web import preflight

    monkeypatch.delenv("HF_TOKEN", raising=False)
    result = _REAL_PROBE("org/gated")

    assert result.outcome == preflight.UNKNOWN
    assert "HF_TOKEN" in result.detail


def test_non_hf_source_is_not_probed(monkeypatch):
    """And the OK it returns means "no opinion", which is why PROBED_SOURCE
    exists for callers that act on OK."""
    from dlm.web import preflight

    def boom(*a, **kw):
        raise AssertionError("ModelScope must not be probed")

    monkeypatch.setattr(preflight, "probe_hf_repo", boom)
    result = _call(preflight.check_repo_access("org/x", "modelscope"))

    assert result.outcome == preflight.OK
    assert preflight.PROBED_SOURCE == "hf"


def test_dataset_and_model_probe_different_urls():
    from dlm.web import preflight

    assert preflight.hf_repo_url("org/x", "dataset") == \
        "https://huggingface.co/datasets/org/x"
    assert preflight.hf_repo_url("org/x", "model") == "https://huggingface.co/org/x"


# ── B. the hold columns ──────────────────────────────────────────────────


def test_set_and_clear_hold_round_trip(db):
    _task(db, "t-1", status="paused")
    db.set_hold("t-1", "needs_approval", "点同意")

    row = db.get_task("t-1")
    assert row["hold_reason"] == "needs_approval"
    assert row["hold_detail"] == "点同意"
    assert row["hold_checked_at"] > 0

    db.clear_hold("t-1")
    row = db.get_task("t-1")
    assert row["hold_reason"] is None
    assert row["hold_detail"] is None


def test_get_held_tasks_filters_on_reason_not_just_paused(db):
    _task(db, "t-held", status="paused")
    db.set_hold("t-held", "needs_approval", "d")
    _task(db, "t-just-paused", status="paused")
    _task(db, "t-running", status="downloading")
    db.set_hold("t-running", "needs_approval", "d")  # wrong status

    ids = [t["id"] for t in db.get_held_tasks("needs_approval", 50)]
    assert ids == ["t-held"]


def test_get_held_tasks_returns_oldest_check_first(db):
    """This ordering is what makes recheck_holds' per-cycle cap of 5 rotate
    instead of re-probing the same five for ever while the tail starves."""
    for tid, checked in (("t-a", 300.0), ("t-b", 100.0), ("t-c", 200.0)):
        _task(db, tid, status="paused")
        db.set_hold(tid, "needs_approval", "d")
        conn = db._conn()
        conn.execute("UPDATE tasks SET hold_checked_at=? WHERE id=?", (checked, tid))
        conn.commit()

    assert [t["id"] for t in db.get_held_tasks("needs_approval", 50)] == \
        ["t-b", "t-c", "t-a"]


def test_touch_hold_check_moves_a_task_to_the_back_of_the_queue(db):
    for tid in ("t-a", "t-b"):
        _task(db, tid, status="paused")
        db.set_hold(tid, "needs_approval", "d")
        conn = db._conn()
        conn.execute("UPDATE tasks SET hold_checked_at=? WHERE id=?", (1.0, tid))
        conn.commit()

    db.touch_hold_check("t-a")
    assert [t["id"] for t in db.get_held_tasks("needs_approval", 50)][0] == "t-b"


# ── C. holding a task that is already running ────────────────────────────


def _stub_hold_deps(monkeypatch, *, cancel_calls, release_calls,
                    cancel_raises=None, release_raises=None):
    import dlm.web.temporal_client as tc
    import dlm.web.routes.queue as qroutes

    async def fake_cancel(task_id, dispatch_mode=None):
        cancel_calls.append((task_id, dispatch_mode))
        if cancel_raises is not None:
            raise cancel_raises

    async def fake_release(body):
        release_calls.append(body.get("task_id"))
        if release_raises is not None:
            raise release_raises
        return {"ok": True}

    monkeypatch.setattr(tc, "cancel_workflow", fake_cancel)
    monkeypatch.setattr(qroutes, "release_pool_batches", fake_release)


def test_hold_pauses_cancels_and_releases_batches(db, monkeypatch):
    from dlm.web.hold import hold_for_approval

    cancel_calls, release_calls = [], []
    _stub_hold_deps(monkeypatch, cancel_calls=cancel_calls,
                    release_calls=release_calls)
    _task(db, "t-1", status="downloading", mode="pool")

    assert _call(hold_for_approval("t-1", "403 in batch 7")) is True

    row = db.get_task("t-1")
    assert row["status"] == "paused"
    assert row["hold_reason"] == "needs_approval"
    assert row["hold_detail"] == "403 in batch 7"
    assert row["speed_mbps"] == 0
    assert cancel_calls == [("t-1", "pool")]
    assert release_calls == ["t-1"]


def test_sharded_hold_skips_the_batch_release(db, monkeypatch):
    from dlm.web.hold import hold_for_approval

    cancel_calls, release_calls = [], []
    _stub_hold_deps(monkeypatch, cancel_calls=cancel_calls,
                    release_calls=release_calls)
    _task(db, "t-1", status="downloading", mode="sharded")

    assert _call(hold_for_approval("t-1", "d")) is True
    assert cancel_calls == [("t-1", "sharded")]
    assert release_calls == []


@pytest.mark.parametrize("status", ["done", "failed", "revoked", "skipped",
                                    "paused"])
def test_hold_refuses_a_task_it_must_not_touch(db, monkeypatch, status):
    """`done`/`failed`/`revoked` are history and a report against one is a
    zombie activity; an already-`paused` row is either held already or paused
    by an operator, and neither wants this overwriting it."""
    from dlm.web.hold import hold_for_approval

    cancel_calls, release_calls = [], []
    _stub_hold_deps(monkeypatch, cancel_calls=cancel_calls,
                    release_calls=release_calls)
    _task(db, "t-1", status=status)

    assert _call(hold_for_approval("t-1", "d")) is False
    assert db.get_task("t-1")["status"] == status
    assert db.get_task("t-1")["hold_reason"] is None
    assert cancel_calls == []
    assert release_calls == []


def test_cancel_failure_does_not_undo_the_hold(db, monkeypatch):
    """Leaving the task `downloading` because Temporal hiccuped is the worst
    of the three outcomes — the row being `paused` is what stops the next
    dispatch cycle."""
    from dlm.web.hold import hold_for_approval

    cancel_calls, release_calls = [], []
    _stub_hold_deps(monkeypatch, cancel_calls=cancel_calls,
                    release_calls=release_calls,
                    cancel_raises=RuntimeError("temporal down"))
    _task(db, "t-1", status="downloading", mode="pool")

    assert _call(hold_for_approval("t-1", "d")) is True
    row = db.get_task("t-1")
    assert row["status"] == "paused"
    assert row["hold_reason"] == "needs_approval"
    # …and the batch release still runs: the cancel is not its precondition.
    assert release_calls == ["t-1"]


def test_batch_release_failure_does_not_undo_the_hold(db, monkeypatch):
    from dlm.web.hold import hold_for_approval

    cancel_calls, release_calls = [], []
    _stub_hold_deps(monkeypatch, cancel_calls=cancel_calls,
                    release_calls=release_calls,
                    release_raises=RuntimeError("db busy"))
    _task(db, "t-1", status="downloading", mode="pool")

    assert _call(hold_for_approval("t-1", "d")) is True
    assert db.get_task("t-1")["hold_reason"] == "needs_approval"


# ── D. the worker report that catches a gate closing mid-run ─────────────


def _report(body):
    from dlm.web.routes.servers import report_missing_files
    return _call(report_missing_files(body))


def test_access_denied_report_holds_the_task(db, monkeypatch):
    from dlm.temporal.models import FAIL_ACCESS_DENIED

    cancel_calls, release_calls = [], []
    _stub_hold_deps(monkeypatch, cancel_calls=cancel_calls,
                    release_calls=release_calls)
    _task(db, "t-1", status="downloading", mode="pool")

    out = _report({"task_id": "t-1", "batch_index": 7, "server": "w3",
                   "files": [{"path": "a.bin", "reason": FAIL_ACCESS_DENIED},
                             {"path": "b.bin", "reason": "timeout"}]})

    assert out["held_for_approval"] is True
    assert out["recorded"] == 2, "the archive is recorded either way"
    row = db.get_task("t-1")
    assert row["status"] == "paused"
    assert "批次 7" in row["hold_detail"]
    assert "403" in row["hold_detail"]


def test_other_failure_reasons_do_not_hold(db, monkeypatch):
    cancel_calls, release_calls = [], []
    _stub_hold_deps(monkeypatch, cancel_calls=cancel_calls,
                    release_calls=release_calls)
    _task(db, "t-1", status="downloading", mode="pool")

    out = _report({"task_id": "t-1", "batch_index": 1,
                   "files": [{"path": "a.bin", "reason": "timeout"},
                             {"path": "b.bin", "reason": "checksum"}]})

    assert "held_for_approval" not in out
    assert db.get_task("t-1")["status"] == "downloading"
    assert cancel_calls == []


def test_zombie_report_against_a_revoked_task_neither_records_nor_holds(db, monkeypatch):
    """Holding a task on the strength of a report we just declined to record
    would be a hold with nothing behind it."""
    from dlm.temporal.models import FAIL_ACCESS_DENIED

    cancel_calls, release_calls = [], []
    _stub_hold_deps(monkeypatch, cancel_calls=cancel_calls,
                    release_calls=release_calls)
    _task(db, "t-1", status="revoked")

    out = _report({"task_id": "t-1",
                   "files": [{"path": "a.bin", "reason": FAIL_ACCESS_DENIED}]})

    assert "ignored" in out
    assert "held_for_approval" not in out
    assert db.get_task("t-1")["status"] == "revoked"
    assert cancel_calls == []


def test_report_without_task_id_does_not_hold(db, monkeypatch):
    from dlm.temporal.models import FAIL_ACCESS_DENIED

    cancel_calls, release_calls = [], []
    _stub_hold_deps(monkeypatch, cancel_calls=cancel_calls,
                    release_calls=release_calls)

    out = _report({"files": [{"path": "a", "reason": FAIL_ACCESS_DENIED}]})

    assert "error" in out
    assert cancel_calls == []


# ── E. add-time refusal and hold ─────────────────────────────────────────


def _stub_verdict(monkeypatch, outcome, detail="d"):
    from dlm.web import preflight

    monkeypatch.setattr(
        preflight, "probe_hf_repo",
        lambda repo_id, dtype="dataset", token=None:
            preflight.PreflightResult(outcome, detail))


def test_queue_add_stores_a_gated_repo_paused_and_held(db, monkeypatch):
    from dlm.web import preflight
    from dlm.web.routes.queue import add_to_queue

    _stub_verdict(monkeypatch, preflight.NEEDS_APPROVAL, "需要点同意")
    out = _call(add_to_queue({"repo_id": "org/gated", "name": "G",
                              "category": "manipulation", "source": "hf"}))

    assert out["ok"] is True
    assert out["status"] == "paused"
    assert out["hold_reason"] == "needs_approval"
    row = db.get_task(out["task_id"])
    assert row["status"] == "paused"
    assert row["hold_reason"] == "needs_approval"
    assert "需要点同意" in row["hold_detail"]


def test_queue_add_rejects_a_repo_that_is_not_there(db, monkeypatch):
    from dlm.web import preflight
    from dlm.web.routes.queue import add_to_queue

    _stub_verdict(monkeypatch, preflight.NOT_FOUND, "找不到 org/typo（404）")
    out = _call(add_to_queue({"repo_id": "org/typo", "name": "T",
                              "source": "hf"}))

    assert "error" in out
    assert db.get_all_tasks() == [], "a rejected add must store nothing"


def test_not_found_on_a_guessed_source_suggests_modelscope(db, monkeypatch):
    """R3's intent, not its letter: a bare `org/name` is *guessed* to be hf, so
    a 404 there is as likely to mean "wrong source" as "wrong name". The guess
    is what gets flagged — the add is not silently retried elsewhere."""
    from dlm.web import preflight
    from dlm.web.routes.queue import add_to_queue

    _stub_verdict(monkeypatch, preflight.NOT_FOUND, "找不到")
    out = _call(add_to_queue({"repo_id": "org/only-on-ms", "name": "M"}))

    assert "modelscope" in out["error"]


def test_explicit_source_gets_no_modelscope_hint(db, monkeypatch):
    """The hint exists because the source was a guess. Told explicitly, it
    would be noise on top of a plain 404."""
    from dlm.web import preflight
    from dlm.web.routes.queue import add_to_queue

    _stub_verdict(monkeypatch, preflight.NOT_FOUND, "找不到")
    out = _call(add_to_queue({"repo_id": "org/x", "name": "X", "source": "hf"}))

    assert "modelscope" not in out["error"]


@pytest.mark.parametrize("outcome", ["ok", "unknown"])
def test_ok_and_unknown_both_add_normally(db, monkeypatch, outcome):
    """UNKNOWN never blocks: a check that turns an HF hiccup into a refused
    task is worse than no check."""
    from dlm.web.routes.queue import add_to_queue

    _stub_verdict(monkeypatch, outcome)
    out = _call(add_to_queue({"repo_id": f"org/{outcome}", "name": outcome,
                              "source": "hf"}))

    assert out["ok"] is True
    assert "hold_reason" not in out
    row = db.get_task(out["task_id"])
    assert row["status"] == "pending"
    assert row["hold_reason"] is None


def test_tasks_route_also_holds_a_gated_repo(db, monkeypatch):
    from dlm.web import preflight
    from dlm.web.routes.tasks import AddTaskRequest, add_task

    _stub_verdict(monkeypatch, preflight.NEEDS_APPROVAL, "需要点同意")
    out = _call(add_task(AddTaskRequest(url_or_repo="org/gated2",
                                        category="manipulation")))

    payload = out["task"]
    assert payload["hold_reason"] == "needs_approval"
    row = db.get_task(payload["id"])
    assert row["status"] == "paused"
    assert row["hold_reason"] == "needs_approval"


def test_tasks_route_default_priority_stays_out_of_the_boost_band(db, monkeypatch):
    """P1 maps to int 2, which fleet.pool_task_weight boosts and the preempt
    victim sort prefers. A caller who never mentioned priority asked for
    neither."""
    from dlm.web.routes.tasks import PRIORITY_TO_INT, AddTaskRequest, add_task
    from dlm.web.fleet import POOL_P0_MAX_PRIORITY

    _stub_verdict(monkeypatch, "ok")
    out = _call(add_task(AddTaskRequest(url_or_repo="org/plain")))

    priority = db.get_task(out["task"]["id"])["priority"]
    assert priority == PRIORITY_TO_INT["P2"]
    assert priority > POOL_P0_MAX_PRIORITY


# ── F. taking the hold off ───────────────────────────────────────────────


def test_resume_releases_the_hold_and_drops_batch_rows(db, monkeypatch):
    """The "已审批，继续" button. It does NOT re-run the preflight: a probe can
    come back UNKNOWN on an HF hiccup, and a button whose effect depends on a
    flaky measurement is a button that sometimes does nothing silently."""
    from dlm.web.routes.queue import resume_task

    _task(db, "t-1", status="paused", mode="pool")
    db.set_hold("t-1", "needs_approval", "d")
    db.upsert_shard({"id": "b-0", "task_id": "t-1", "shard_index": 0,
                     "status": "failed", "server": "w1"})

    out = _call(resume_task({"task_id": "t-1"}))

    assert out["ok"] is True
    assert out["released_hold"] == "needs_approval"
    row = db.get_task("t-1")
    assert row["status"] == "pending"
    assert row["hold_reason"] is None
    assert db.get_shards_by_task("t-1") == []


def test_resume_of_an_unheld_task_reports_no_released_hold(db):
    from dlm.web.routes.queue import resume_task

    _task(db, "t-1", status="paused", mode="pool")
    out = _call(resume_task({"task_id": "t-1"}))

    assert out["ok"] is True
    assert out["released_hold"] is None


def test_release_to_pending_respects_expect_status(db):
    """The reconciler's candidate list is a whole cycle old by the time it
    writes, so it passes the status it read. A row that moved on in between
    must not be flipped."""
    from dlm.web.hold import release_to_pending

    _task(db, "t-1", status="downloading")
    assert release_to_pending("t-1", expect_status="preempted") is False
    assert db.get_task("t-1")["status"] == "downloading"

    _task(db, "t-2", status="preempted")
    assert release_to_pending("t-2", expect_status="preempted") is True
    assert db.get_task("t-2")["status"] == "pending"


# ── G. the recheck loop that makes the hold self-clearing ────────────────


def _stub_check(monkeypatch, verdicts):
    """verdicts: repo_id -> outcome. Records what was probed."""
    from dlm.web import preflight

    probed = []

    async def fake_check(repo_id, source, dtype="dataset"):
        probed.append(repo_id)
        return preflight.PreflightResult(verdicts[repo_id], "stub")

    monkeypatch.setattr(preflight, "check_repo_access", fake_check)
    return probed


def test_recheck_releases_a_task_whose_gate_opened(db, monkeypatch):
    from dlm.web.hold import recheck_holds

    _task(db, "t-1", status="paused", repo_id="org/now-open")
    db.set_hold("t-1", "needs_approval", "d")
    _stub_check(monkeypatch, {"org/now-open": "ok"})

    report = _call(recheck_holds())

    assert report["released"] == ["t-1"]
    row = db.get_task("t-1")
    assert row["status"] == "pending"
    assert row["hold_reason"] is None


@pytest.mark.parametrize("outcome", ["needs_approval", "unknown", "not_found"])
def test_recheck_only_releases_on_ok(db, monkeypatch, outcome):
    """NEEDS_APPROVAL is the expected steady state; UNKNOWN and NOT_FOUND are
    refusals to conclude. Releasing on either hands a task back to the fleet on
    no evidence — straight back into the batch-burning loop."""
    from dlm.web.hold import recheck_holds

    _task(db, "t-1", status="paused", repo_id="org/still-shut")
    db.set_hold("t-1", "needs_approval", "d")
    conn = db._conn()
    conn.execute("UPDATE tasks SET hold_checked_at=0 WHERE id='t-1'")
    conn.commit()
    _stub_check(monkeypatch, {"org/still-shut": outcome})

    report = _call(recheck_holds())

    assert report["released"] == []
    assert report["still_held"] == 1
    row = db.get_task("t-1")
    assert row["status"] == "paused"
    assert row["hold_reason"] == "needs_approval"
    assert row["hold_checked_at"] > 0, "an unreleased hold must be touched"


def test_recheck_never_releases_a_source_it_cannot_probe(db, monkeypatch):
    """The bug this pins: check_repo_access answers OK for every source it does
    not probe, and OK is the release condition. A ModelScope task held by the
    403 path (which is source-blind, and right to be) would be handed back
    every 30 minutes, rediscover the same refusal, and burn a batch's three
    attempts each time — the exact loop the hold exists to stop, on a timer."""
    from dlm.web.hold import recheck_holds

    _task(db, "t-ms", status="paused", source="modelscope", repo_id="org/ms")
    db.set_hold("t-ms", "needs_approval", "d")
    probed = _stub_check(monkeypatch, {})  # a probe here would KeyError

    report = _call(recheck_holds())

    assert probed == [], "a non-hf hold must not be probed at all"
    assert report["released"] == []
    assert report["still_held"] == 1
    assert report["unprobeable"] == ["t-ms"]
    assert db.get_task("t-ms")["status"] == "paused"
    assert db.get_task("t-ms")["hold_checked_at"] > 0


def test_recheck_reports_what_the_per_cycle_cap_left_out(db, monkeypatch):
    """A capped sweep that reads as a complete one is how a task sits held for
    hours with nothing in the log to explain it."""
    from dlm.web.hold import recheck_holds

    verdicts = {}
    for i in range(4):
        _task(db, f"t-{i}", status="paused", repo_id=f"org/r{i}")
        db.set_hold(f"t-{i}", "needs_approval", "d")
        verdicts[f"org/r{i}"] = "needs_approval"
    _stub_check(monkeypatch, verdicts)

    report = _call(recheck_holds(limit=2))

    assert report["checked"] == 2
    assert report["truncated"] == 2


def test_recheck_with_nothing_held_is_a_no_op(db, monkeypatch):
    from dlm.web.hold import recheck_holds

    probed = _stub_check(monkeypatch, {})
    report = _call(recheck_holds())

    assert report["released"] == []
    assert report["checked"] == 0
    assert probed == []


# ── H. what an operator actually sees ────────────────────────────────────


def test_dashboard_active_rows_carry_resume_skipped_gb(db):
    """The progress bar needs it: without it the UI can only compute
    downloaded/size, which on a resumed task is progress through THIS ROUND,
    not through the dataset."""
    _task(db, "t-1", status="downloading")
    conn = db._conn()
    conn.execute("UPDATE tasks SET downloaded_gb=10.0, size_gb=20.0, "
                 "resume_skipped_gb=80.0 WHERE id='t-1'")
    conn.commit()

    row = db.get_dashboard_summary()["active_downloads"][0]

    assert row["resume_skipped_gb"] == 80.0
    assert (row["downloaded_gb"] + row["resume_skipped_gb"]) / \
        (row["size_gb"] + row["resume_skipped_gb"]) == pytest.approx(0.9)


def test_held_tasks_drive_a_banner_with_a_release_button():
    """Text-level assertions on the static assets, per the convention
    tests/test_dispatch_mode_ui.py established: these files are shipped
    verbatim and have no other test surface."""
    html = (STATIC / "index.html").read_text()
    js = (STATIC / "app.js").read_text()

    assert "heldTasks.length > 0" in html, "the banner must be bound to the getter"
    assert "resumeTask(t.id)" in html, "the banner needs its own release button"
    assert "t.hold_detail" in html, "the banner must say what a human has to do"
    assert "hold_reason === 'needs_approval'" in js


def test_progress_bars_use_the_whole_dataset_denominator():
    """assembly101 read 38.1% when it was 66.2% done, which looks like a task
    in trouble rather than one two thirds finished. Both the dashboard bar and
    the task table must go through the helpers, not raw columns."""
    html = (STATIC / "index.html").read_text()
    js = (STATIC / "app.js").read_text()

    assert html.count("taskProgressPct(") >= 3
    assert "taskTotalGb(t) > 0" in html
    assert "(t.size_gb || 0) + (t.resume_skipped_gb || 0)" in js
    assert "(t.downloaded_gb || 0) + (t.resume_skipped_gb || 0)" in js


def test_priority_labels_match_what_the_ints_actually_do():
    """P1 was labelled "Normal" while mapping to int 2 — inside the pool
    weight-boost band. An operator picking the label that reads as the default
    was silently buying weighted preemption."""
    html = (STATIC / "index.html").read_text()
    js = (STATIC / "app.js").read_text()

    assert "P1 (Normal)" not in html
    assert "P0 (Urgent · 加权抢占)" in html
    assert "P1 (High · 加权抢占)" in html
    assert "P2 (Normal)" in html
    assert "priority: 'P1'" not in js, "the form default must sit outside the band"
    assert js.count("priority: 'P2'") == 3
