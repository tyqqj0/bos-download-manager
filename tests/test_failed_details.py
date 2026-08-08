"""T2 — the pipeline records WHICH files failed, not just how many.

`failed_files` told a batch it had lost N files. Which N existed only in a
log line on one worker's disk, so nothing downstream could archive it, alert
on it, or judge a transfer against it — R4's "缺件不静默" has nothing to write
without this. Nine sites incremented the counter; six of them emitted no
event at all, and one could not even name the file it was counting.

The tests here pin: every one of the nine populates failed_details, the
invariant len(failed_details) == failed_files holds at each, reasons are
short classifiers rather than exception text (exceptions on this fleet carry
KB-scale xet CDN URLs, which must never reach the database), and — the one
site with real risk — the asyncio.gather catch-all recovers identity by
position.

Run: python3 -m pytest tests/test_failed_details.py -q
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor

import pytest

from dlm.temporal import models, pipeline
from dlm.temporal.models import FileInfo, TaskInput


def _engine(tmp_path, monkeypatch, source="hf") -> pipeline.PipelineEngine:
    monkeypatch.setattr(pipeline, "STAGING_PATH", tmp_path)
    task = TaskInput(id="t1", name="task", repo_id="org/name", source=source)
    return pipeline.PipelineEngine(task, tmp_path, heartbeat_fn=lambda *_: None)


def _assert_invariant(engine):
    assert len(engine.stats.failed_details) == engine.stats.failed_files, (
        "every counted failure must carry its identity — a count without a "
        "name is what T2 exists to remove"
    )


def _only(engine) -> dict:
    _assert_invariant(engine)
    assert len(engine.stats.failed_details) == 1
    return engine.stats.failed_details[0]


# ── the shared helper ───────────────────────────────────────────────────


def test_stats_starts_with_an_empty_detail_list(tmp_path, monkeypatch):
    engine = _engine(tmp_path, monkeypatch)
    assert engine.stats.failed_details == []
    assert engine.stats.failed_files == 0


def test_fail_file_moves_both_the_count_and_the_list(tmp_path, monkeypatch):
    engine = _engine(tmp_path, monkeypatch)
    engine._fail_file("a/b.bin", models.FAIL_ACCESS_DENIED, 42)
    assert engine.stats.failed_files == 1
    assert engine.stats.failed_details == [
        {"path": "a/b.bin", "reason": models.FAIL_ACCESS_DENIED, "size_bytes": 42}
    ]


def test_a_missing_size_becomes_zero_not_none(tmp_path, monkeypatch):
    """The archive column is INTEGER DEFAULT 0; None would land as NULL and
    make every consumer coalesce."""
    engine = _engine(tmp_path, monkeypatch)
    engine._fail_file("a/b.bin", models.FAIL_UPLOAD_FAILED, None)
    assert engine.stats.failed_details[0]["size_bytes"] == 0


def test_every_pipeline_failure_site_uses_the_helper(tmp_path, monkeypatch):
    """Structural guard: a tenth site that increments the counter directly
    would break the invariant silently, and no behavioural test can see a
    site nobody wrote a test for yet.
    """
    import inspect

    src = inspect.getsource(pipeline.PipelineEngine)
    hits = [ln.strip() for ln in src.splitlines()
            if ln.strip().startswith("self.stats.failed_files += 1")]
    assert len(hits) == 1, (
        f"failed_files must only be incremented inside _fail_file; found {hits}"
    )


def test_the_reason_classifiers_are_short_and_stable(tmp_path, monkeypatch):
    """Reasons go into the database and get grouped on. Anything long enough
    to be exception text — or a URL — is the failure mode this guards."""
    names = [n for n in dir(models) if n.startswith("FAIL_")]
    assert len(names) >= 9
    for name in names:
        value = getattr(models, name)
        assert value.islower() and " " not in value and len(value) <= 40
        assert "http" not in value


# ── download-side sites ─────────────────────────────────────────────────


def test_access_denied_records_the_file(tmp_path, monkeypatch):
    engine = _engine(tmp_path, monkeypatch)
    engine._executor = ThreadPoolExecutor(max_workers=1)
    engine._concurrency = 1

    monkeypatch.setattr(engine, "_download_one_file", lambda *a, **k: None)

    async def denied(*args, **kwargs):
        raise pipeline._AccessDenied("403 for a/gated.bin")

    monkeypatch.setattr(engine, "_wait_with_growth_check", denied)

    asyncio.run(engine._producer([FileInfo(path="a/gated.bin", size=7)],
                                 asyncio.Queue()))

    assert _only(engine) == {"path": "a/gated.bin",
                             "reason": models.FAIL_ACCESS_DENIED,
                             "size_bytes": 7}


def test_an_upstream_empty_response_records_the_file(tmp_path, monkeypatch):
    """The source declared a size and then served nothing — ModelScope's
    RoboDojo depth files. Precisely what the archive is for: it is permanent
    at the source, so no amount of retrying or re-dispatching fixes it, and
    the only useful outcome is knowing which files they were.
    """
    engine = _engine(tmp_path, monkeypatch, source="modelscope")
    engine._executor = ThreadPoolExecutor(max_workers=1)
    engine._concurrency = 1

    monkeypatch.setattr(engine, "_download_one_file", lambda *a, **k: None)

    async def empty(*args, **kwargs):
        raise pipeline._UpstreamEmpty("0 bytes for a/depth.mp4")

    monkeypatch.setattr(engine, "_wait_with_growth_check", empty)

    asyncio.run(engine._producer([FileInfo(path="a/depth.mp4", size=900)],
                                 asyncio.Queue()))

    assert _only(engine)["reason"] == models.FAIL_UPSTREAM_EMPTY


def test_retries_exhausted_records_the_file(tmp_path, monkeypatch):
    """The most common poison-file path, and one of the six that emitted
    nothing at all before."""
    engine = _engine(tmp_path, monkeypatch)
    engine._executor = ThreadPoolExecutor(max_workers=1)
    engine._concurrency = 1
    monkeypatch.setattr(pipeline, "MAX_FILE_RETRIES", 1)

    monkeypatch.setattr(engine, "_download_one_file", lambda *a, **k: None)

    async def all_mirrors_fail(*args, **kwargs):
        return None

    monkeypatch.setattr(engine, "_wait_with_growth_check", all_mirrors_fail)

    asyncio.run(engine._producer([FileInfo(path="a/big.bin", size=11)],
                                 asyncio.Queue()))

    assert _only(engine) == {"path": "a/big.bin",
                             "reason": models.FAIL_DOWNLOAD_RETRIES_EXHAUSTED,
                             "size_bytes": 11}


def test_the_gather_catch_all_recovers_identity_by_position(tmp_path, monkeypatch):
    """The one site with real risk in T2.

    `results` from asyncio.gather carries only outcome objects, so this site
    counted a failure it could not name. `tasks` is built one-per-entry of
    `files` in order and gather preserves that order, so zip(files, results)
    is what recovers the identity — and this test is what proves the pairing
    is actually aligned rather than merely plausible.
    """
    engine = _engine(tmp_path, monkeypatch)
    engine._executor = ThreadPoolExecutor(max_workers=3)
    engine._concurrency = 3

    files = [FileInfo(path=f"a/{i}.bin", size=i + 1) for i in range(3)]

    monkeypatch.setattr(engine, "_download_one_file", lambda *a, **k: None)

    async def blow_up_on_the_middle_one(download_future, file_info, *a, **k):
        if file_info.path == "a/1.bin":
            raise RuntimeError("something outside the handled classes")
        return None

    monkeypatch.setattr(pipeline, "MAX_FILE_RETRIES", 1)
    monkeypatch.setattr(engine, "_wait_with_growth_check",
                        blow_up_on_the_middle_one)

    asyncio.run(engine._producer(files, asyncio.Queue()))

    _assert_invariant(engine)
    by_reason = {d["reason"]: d for d in engine.stats.failed_details}
    unhandled = by_reason[models.FAIL_UNHANDLED_DOWNLOAD_ERROR]
    assert unhandled["path"] == "a/1.bin", (
        "the unhandled failure must name the file that actually raised, "
        "not whichever entry happened to be first"
    )
    assert unhandled["size_bytes"] == 2


def test_a_cancelled_download_is_named_too(tmp_path, monkeypatch):
    """CancelledError is a BaseException, so it bypasses both except clauses
    and the retries-exhausted tail — the gather site is the only place that
    sees it, which is why it needed a name."""
    engine = _engine(tmp_path, monkeypatch)
    engine._executor = ThreadPoolExecutor(max_workers=1)
    engine._concurrency = 1

    monkeypatch.setattr(engine, "_download_one_file", lambda *a, **k: None)

    async def cancelled(*args, **kwargs):
        raise asyncio.CancelledError()

    monkeypatch.setattr(engine, "_wait_with_growth_check", cancelled)

    asyncio.run(engine._producer([FileInfo(path="a/c.bin", size=5)],
                                 asyncio.Queue()))

    assert _only(engine)["path"] == "a/c.bin"


# ── upload-side sites ───────────────────────────────────────────────────


def test_a_cancelled_upload_is_named_and_classified_as_cancellation(
        tmp_path, monkeypatch):
    """Cancellation is orchestration — pause, preempt, reshard — not "the
    source lost this file". It is still counted (a batch with cancelled
    uploads is not clean), but T3 keeps this reason out of the archive so a
    pause does not fabricate missing-file records.
    """
    engine = _engine(tmp_path, monkeypatch)

    async def fake_upload_one(fi, sem):
        if fi.path == "cancel_me.bin":
            raise asyncio.CancelledError()

    engine._upload_one = fake_upload_one

    queue = asyncio.Queue()

    async def run_it():
        await queue.put(FileInfo(path="cancel_me.bin", size=10))
        await queue.put(FileInfo(path="ok.bin", size=10))
        await queue.put(None)
        await engine._consumer(queue)

    asyncio.run(run_it())

    assert _only(engine) == {"path": "cancel_me.bin",
                             "reason": models.FAIL_UPLOAD_CANCELLED,
                             "size_bytes": 10}


def test_an_upload_task_raising_is_named(tmp_path, monkeypatch):
    engine = _engine(tmp_path, monkeypatch)

    async def fake_upload_one(fi, sem):
        raise RuntimeError("bug outside _upload_one's try block")

    engine._upload_one = fake_upload_one

    queue = asyncio.Queue()

    async def run_it():
        await queue.put(FileInfo(path="boom.bin", size=3))
        await queue.put(None)
        await engine._consumer(queue)

    asyncio.run(run_it())

    assert _only(engine)["reason"] == models.FAIL_UPLOAD_FAILED


def test_the_owner_map_does_not_grow_with_the_batch(tmp_path, monkeypatch):
    """Entries are popped as tasks are counted. A 500-file batch holding 500
    FileInfo objects would be a leak, and the backpressure cap is what makes
    popping sufficient."""
    engine = _engine(tmp_path, monkeypatch)
    seen_sizes = []
    real = engine._count_upload_task_failures

    def spy(done, owners):
        real(done, owners)
        seen_sizes.append(len(owners))

    engine._count_upload_task_failures = spy

    async def fake_upload_one(fi, sem):
        return None

    engine._upload_one = fake_upload_one

    queue = asyncio.Queue()

    async def run_it():
        for i in range(6):
            await queue.put(FileInfo(path=f"f{i}.bin", size=1))
        await queue.put(None)
        await engine._consumer(queue)

    asyncio.run(run_it())

    assert seen_sizes, "the drain path must have counted at least once"
    assert seen_sizes[-1] == 0, f"owners left populated: {seen_sizes}"


def test_a_task_missing_from_the_owner_map_still_counts(tmp_path, monkeypatch):
    """Losing the name is a bug; losing the count would make a lossy batch
    look clean. Degrade to a placeholder rather than dropping either."""
    engine = _engine(tmp_path, monkeypatch)

    async def boom():
        raise RuntimeError("x")

    async def run_it():
        task = asyncio.create_task(boom())
        await asyncio.gather(task, return_exceptions=True)
        engine._count_upload_task_failures({task}, {})

    asyncio.run(run_it())

    detail = _only(engine)
    assert detail["reason"] == models.FAIL_UPLOAD_FAILED
    assert detail["path"] == "<unknown-upload>"


def test_a_staged_file_that_vanished_is_named(tmp_path, monkeypatch):
    engine = _engine(tmp_path, monkeypatch)
    engine._bos_client = object()
    engine._bucket = "bucket"
    engine._prefix = "prefix/"

    asyncio.run(engine._upload_one(FileInfo(path="gone.bin", size=10),
                                   asyncio.Semaphore(1)))

    assert _only(engine) == {"path": "gone.bin",
                             "reason": models.FAIL_STAGED_FILE_MISSING,
                             "size_bytes": 10}


def test_a_pre_upload_size_mismatch_is_named(tmp_path, monkeypatch):
    """The RoboDojo shape: on disk at 0 bytes while the source claimed 900MB.
    Permanent at the source, so it belongs in the archive."""
    import dlm.core.bos as bos_mod

    engine = _engine(tmp_path, monkeypatch)
    engine._bos_client = object()
    engine._bucket = "bucket"
    engine._prefix = "prefix/"
    (tmp_path / "f.bin").write_bytes(b"")

    monkeypatch.setattr(bos_mod, "upload_file", lambda *a, **k: None)

    asyncio.run(engine._upload_one(FileInfo(path="f.bin", size=900_000_000),
                                   asyncio.Semaphore(1)))

    assert _only(engine)["reason"] == models.FAIL_SIZE_MISMATCH


def test_upload_retries_exhausted_is_named(tmp_path, monkeypatch):
    import dlm.core.bos as bos_mod

    engine = _engine(tmp_path, monkeypatch)
    engine._bos_client = object()
    engine._bucket = "bucket"
    engine._prefix = "prefix/"
    (tmp_path / "f.bin").write_bytes(b"xxxx")

    def always_fail(*a, **k):
        raise RuntimeError("bos said no")

    monkeypatch.setattr(bos_mod, "upload_file", always_fail)

    async def no_sleep(_):
        return None

    monkeypatch.setattr(asyncio, "sleep", no_sleep)

    asyncio.run(engine._upload_one(FileInfo(path="f.bin", size=4),
                                   asyncio.Semaphore(1)))

    assert _only(engine)["reason"] == models.FAIL_UPLOAD_RETRIES_EXHAUSTED


def test_the_exception_text_never_reaches_the_detail(tmp_path, monkeypatch):
    """An exception here can carry a KB-scale xet CDN URL — the project has
    been bitten by those in logs already, and the database is worse."""
    import dlm.core.bos as bos_mod

    engine = _engine(tmp_path, monkeypatch)
    engine._bos_client = object()
    engine._bucket = "bucket"
    engine._prefix = "prefix/"
    (tmp_path / "f.bin").write_bytes(b"xxxx")

    secret = "https://transfer.xethub.hf.co/xorbs/" + "A" * 2000

    def always_fail(*a, **k):
        raise RuntimeError(secret)

    monkeypatch.setattr(bos_mod, "upload_file", always_fail)

    async def no_sleep(_):
        return None

    monkeypatch.setattr(asyncio, "sleep", no_sleep)

    asyncio.run(engine._upload_one(FileInfo(path="f.bin", size=4),
                                   asyncio.Semaphore(1)))

    blob = repr(engine.stats.failed_details)
    assert "xethub" not in blob and "AAAA" not in blob


# ── replay / serialisation ──────────────────────────────────────────────


def test_old_activity_payloads_without_the_field_still_deserialise(tmp_path):
    """Activity results cross the Temporal boundary, so histories recorded
    before this change carry no failed_details key. default_factory is what
    keeps those replayable — a required field would break replay of every
    in-flight batch."""
    from dataclasses import fields

    stats = models.PipelineStats(**{"total_files": 3, "failed_files": 1})
    assert stats.failed_details == []

    field = next(f for f in fields(models.PipelineStats)
                 if f.name == "failed_details")
    assert field.default_factory is list


def test_two_stats_objects_do_not_share_one_list(tmp_path):
    a = models.PipelineStats()
    b = models.PipelineStats()
    a.failed_details.append({"path": "x"})
    assert b.failed_details == []
