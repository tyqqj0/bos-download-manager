"""T3 — chunk_filelist: pool batch chunking.

Covers the batching rule matrix (file-count limit, byte limit, big-file
isolation, determinism), the batch-manifest round trip via the chunk_filelist
activity, and the runtime batch-count cap.

Run: python3 -m pytest tests/test_chunk_filelist.py -q   (needs temporalio)
"""

from __future__ import annotations

import asyncio
import json

import pytest

from dlm.temporal.activities import (
    BATCH_MAX_BYTES,
    BATCH_MAX_FILES,
    BatchLimitExceededError,
    _chunk_files,
    chunk_filelist,
)
from dlm.temporal.models import TaskInput


def _files(n, size=1):
    return [{"path": f"f{i}", "size": size} for i in range(n)]


# ── _chunk_files: the pure batching-rule matrix ────────────────────────


def test_empty_filelist_yields_no_batches():
    assert _chunk_files([]) == []


def test_small_filelist_is_one_batch():
    files = _files(10, size=1024)
    batches = _chunk_files(files)
    assert len(batches) == 1
    assert batches[0] == files


def test_file_count_limit_splits_batches():
    files = _files(BATCH_MAX_FILES + 1, size=1)
    batches = _chunk_files(files)
    assert len(batches) == 2
    assert all(len(b) <= BATCH_MAX_FILES for b in batches)
    assert sum(len(b) for b in batches) == BATCH_MAX_FILES + 1


def test_byte_limit_splits_batches_even_under_file_count_limit():
    # 3 files at half the byte cap each: two fill a batch exactly, the
    # third must spill to a new batch even though the file-count limit
    # (500) is nowhere close.
    half = BATCH_MAX_BYTES // 2
    files = [{"path": "a", "size": half}, {"path": "b", "size": half},
             {"path": "c", "size": half}]
    batches = _chunk_files(files)
    assert len(batches) == 2
    assert sorted(len(b) for b in batches) == [1, 2]
    for b in batches:
        assert sum(f["size"] for f in b) <= BATCH_MAX_BYTES


def test_big_file_over_cap_gets_its_own_singleton_batch():
    huge = {"path": "huge", "size": BATCH_MAX_BYTES + 1}
    small = {"path": "small", "size": 1}
    batches = _chunk_files([huge, small])
    assert [huge] in batches
    singleton = next(b for b in batches if b == [huge])
    assert len(singleton) == 1
    # the small file is not blocked behind the huge one — it gets its own
    # normal batch, not force-merged or dropped
    assert sum(len(b) for b in batches) == 2


def test_multiple_big_files_each_get_isolated():
    huge1 = {"path": "h1", "size": BATCH_MAX_BYTES + 100}
    huge2 = {"path": "h2", "size": BATCH_MAX_BYTES + 200}
    batches = _chunk_files([huge1, huge2])
    assert len(batches) == 2
    assert all(len(b) == 1 for b in batches)


def test_chunking_is_deterministic_across_repeated_calls():
    files = _files(1200, size=17) + [{"path": "big", "size": BATCH_MAX_BYTES + 1}]
    first = _chunk_files(files)
    second = _chunk_files(files)
    assert first == second


def test_chunking_is_deterministic_given_same_input_order_with_ties():
    # Same-size files: stable sort must keep them in original relative
    # order every time, not an arbitrary sort-dependent shuffle.
    files = [{"path": f"f{i}", "size": 1} for i in range(50)]
    first = _chunk_files(files)
    second = _chunk_files(list(files))  # fresh list, same order/content
    assert first == second


def test_every_input_file_appears_exactly_once():
    files = _files(1000, size=1) + [
        {"path": "big", "size": BATCH_MAX_BYTES + 1},
    ]
    batches = _chunk_files(files)
    flat = [f["path"] for b in batches for f in b]
    assert sorted(flat) == sorted(f["path"] for f in files)
    assert len(flat) == len(set(flat))


# ── chunk_filelist activity: manifest round trip + cap ─────────────────


def _run(coro_fn, *args, **kwargs):
    from temporalio.testing import ActivityEnvironment

    env = ActivityEnvironment()

    async def main():
        return await env.run(coro_fn, *args, **kwargs)

    return asyncio.run(main())


@pytest.fixture
def fake_bos(monkeypatch):
    """Capture BOS uploads instead of hitting the network."""
    uploaded: dict[str, str] = {}

    def fake_create_bos_client(ak, sk, endpoint):
        return object()

    def fake_upload_file(client, bucket, key, local_path):
        with open(local_path) as f:
            uploaded[(bucket, key)] = f.read()

    monkeypatch.setattr("dlm.core.bos.create_bos_client", fake_create_bos_client)
    monkeypatch.setattr("dlm.core.bos.upload_file", fake_upload_file)
    return uploaded


def _task_input(name="pool-task"):
    return TaskInput(id="t-1", name=name, repo_id="org/repo", source="hf")


def test_chunk_filelist_uploads_one_manifest_per_batch(tmp_path, fake_bos):
    files = _files(BATCH_MAX_FILES + 5, size=1)
    filtered = tmp_path / ".filelist.filtered.json"
    filtered.write_text(json.dumps(files))

    result = _run(chunk_filelist, str(filtered), _task_input("pool-task"))

    assert len(result["batch_keys"]) == 2
    assert len(result["counts"]) == 2
    assert len(result["bytes"]) == 2
    assert sum(result["counts"]) == len(files)
    assert sum(result["bytes"]) == sum(f["size"] for f in files)

    for key, count in zip(result["batch_keys"], result["counts"]):
        assert key.startswith("download-manager/batchlists/pool-task/batch-")
        manifest = json.loads(fake_bos[("westlake-autolab-databuilder-meta", key)])
        assert len(manifest) == count


def test_chunk_filelist_empty_input_makes_no_bos_calls(tmp_path, fake_bos):
    filtered = tmp_path / ".filelist.filtered.json"
    filtered.write_text(json.dumps([]))

    result = _run(chunk_filelist, str(filtered), _task_input())

    assert result == {"batch_keys": [], "counts": [], "bytes": []}
    assert fake_bos == {}


def test_chunk_filelist_missing_file_treated_as_empty(tmp_path, fake_bos):
    missing = tmp_path / "does-not-exist.json"

    result = _run(chunk_filelist, str(missing), _task_input())

    assert result == {"batch_keys": [], "counts": [], "bytes": []}


def test_chunk_filelist_over_cap_raises_non_retryable(tmp_path, fake_bos):
    # 3 oversized files, each isolated into its own singleton batch — 3
    # batches against a cap of 2.
    files = [{"path": f"big{i}", "size": BATCH_MAX_BYTES + 1} for i in range(3)]
    filtered = tmp_path / ".filelist.filtered.json"
    filtered.write_text(json.dumps(files))

    with pytest.raises(BatchLimitExceededError):
        _run(chunk_filelist, str(filtered), _task_input(), max_batches=2)

    # The cap check must happen before any BOS upload — a rejected task
    # must not leave partial batch manifests behind.
    assert fake_bos == {}


def test_chunk_filelist_at_cap_exactly_does_not_raise(tmp_path, fake_bos):
    files = _files(BATCH_MAX_FILES, size=1)
    filtered = tmp_path / ".filelist.filtered.json"
    filtered.write_text(json.dumps(files))

    result = _run(chunk_filelist, str(filtered), _task_input(), max_batches=1)
    assert len(result["batch_keys"]) == 1
