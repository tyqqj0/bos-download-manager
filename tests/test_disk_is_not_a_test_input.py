"""The host's free disk must not decide what this suite reports.

`scripts/deploy-workers.sh` gate G7 runs `pytest tests/ -q` **on S1** and
refuses to deploy until it passes. S1 runs at 81% of a 40G `/dev/vda2`, so
free space (7.4G) sits below the pipeline's backpressure threshold
(`max(30% of total, 20GB)` = 20G). `_producer`'s backpressure loop is
`while _disk_free_gb() < threshold: await asyncio.sleep(10)` with no bound
and nothing in a unit test that could free a byte — so on S1 the suite did
not fail, it **hung**, at load 0.02, with no output. A deploy gate that
blocks forever and prints nothing.

conftest's `_the_hosts_free_disk_is_not_a_test_input` fixture removes disk
from the inputs. These tests pin that it is in force and that a test can
still opt out, because the failure it prevents is invisible: on any dev box
with room to spare, deleting the fixture changes nothing locally and breaks
only the deploy.

Run: python3 -m pytest tests/test_disk_is_not_a_test_input.py -q
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from dlm.temporal import pipeline
from dlm.temporal.models import FileInfo, TaskInput


def _engine(tmp_path, monkeypatch) -> pipeline.PipelineEngine:
    monkeypatch.setattr(pipeline, "STAGING_PATH", tmp_path)
    task = TaskInput(id="t1", name="task", repo_id="org/name", source="hf")
    return pipeline.PipelineEngine(task, tmp_path, heartbeat_fn=lambda *_: None)


def test_the_suite_does_not_depend_on_the_hosts_free_disk(monkeypatch):
    """Reported free space is a fixed, generous constant — not a measurement.

    Asserting only `>= 1000` would be vacuous on a roomy dev box: this
    machine has 1,385GB free, so the bare threshold passes with the fixture
    deleted and the regression ships to S1 anyway. So the discriminator is
    *independence from the filesystem*, which holds on every host: point
    `STAGING_PATH` at a path that does not exist. The real implementation
    calls `shutil.disk_usage` on it and raises FileNotFoundError; a constant
    does not care.

    Either way this test fails if the autouse guard is gone — by error
    rather than by assert, which is fine, and better than the alternative of
    a suite that quietly resumes hanging on any host near the threshold.
    Every worker in this fleet lives near that threshold by design; that is
    what backpressure is for.
    """
    monkeypatch.setattr(pipeline, "STAGING_PATH",
                        Path("/nonexistent-so-a-real-stat-would-raise"))

    assert pipeline._disk_free_gb() >= 1000.0


def test_the_real_threshold_is_still_something_a_host_could_fall_under():
    """Guards the guard: the reason disk had to be neutralised is that the
    threshold is high enough for a real machine to sit below it. If someone
    lowers these constants to "fix" the hang instead, this fails and points
    at the fixture as the correct place, rather than letting production lose
    its disk protection.
    """
    assert pipeline.DISK_FREE_ABSOLUTE_MIN_GB == 20
    assert pipeline.DISK_FREE_MIN_PCT == 0.30


def test_a_test_can_still_opt_in_to_low_disk(monkeypatch):
    """The escape hatch: a test body's own patch runs after the autouse
    fixture, so a future backpressure test needs no special mechanism. If
    fixture ordering ever changed, that test would silently exercise a
    10,000GB disk and pass while asserting nothing.
    """
    monkeypatch.setattr(pipeline, "_disk_free_gb", lambda: 1.0)

    assert pipeline._disk_free_gb() == 1.0


def test_a_producer_run_never_parks_in_backpressure(tmp_path, monkeypatch):
    """The end-to-end shape of the S1 hang, as a test that terminates.

    `stats.paused` is set True only inside the backpressure loop, so
    asserting it is False after a completed run pins that the loop was never
    entered — the specific thing that made the deploy gate hang rather than
    fail.
    """
    engine = _engine(tmp_path, monkeypatch)
    engine._executor = ThreadPoolExecutor(max_workers=1)
    engine._concurrency = 1

    monkeypatch.setattr(engine, "_download_one_file", lambda *a, **k: None)

    async def arrived(*args, **kwargs):
        return tmp_path / "a.bin"

    monkeypatch.setattr(engine, "_wait_with_growth_check", arrived)

    queue: asyncio.Queue = asyncio.Queue()
    asyncio.run(engine._producer([FileInfo(path="a.bin", size=7)], queue))

    assert engine.stats.paused is False
    assert engine.stats.downloaded_files == 1
