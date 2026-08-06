"""The shard filelist is the only thing telling a worker what to download.

It travels through BOS at `download-manager/filelists/{scope}/shard-{i}.json`,
and `scope` used to be the task NAME. Names are reused *by requirement*: a
resume MUST reuse the original name for the BOS resume filter to match, and
/queue/add permits re-adding a repo whose previous row is terminal. So two live
tasks could share that key — one overwrote the other's list, and the loser's
shard downloaded another repo's files into this task's prefix, reporting
success the whole way.

Two guards, tested here:
  * the key is scoped by task id, which is unique
  * the shard verifies the bytes it fetched against the md5 the coordinator
    computed over the bytes it uploaded, and refuses the download otherwise

Run: pytest tests/test_filelist_integrity.py -q   (needs temporalio)
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import pathlib

import pytest

from temporalio.testing import ActivityEnvironment

from dlm.temporal import activities
from dlm.temporal.activities import (
    FilelistMismatchError,
    download_shard_filelist,
    partition_files_greedy,
)


class _FakeBos:
    """Records uploads by key and serves them back, like the real bucket."""

    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def get_object(self, bucket, key):
        class _Resp:
            def __init__(self, payload):
                self.data = _Data(payload)

        class _Data:
            def __init__(self, payload):
                self._payload = payload

            def read(self):
                return self._payload

        return _Resp(self.objects[key])


@pytest.fixture
def bos(monkeypatch):
    """Both activities build their client inside the function body, from
    dlm.core.bos — patch there, not on the activities module."""
    fake = _FakeBos()

    def _upload_file(client, bucket, key, local_path):
        fake.objects[key] = pathlib.Path(local_path).read_bytes()
        return True

    monkeypatch.setattr("dlm.core.bos.create_bos_client",
                        lambda *a, **k: fake)
    monkeypatch.setattr("dlm.core.bos.upload_file", _upload_file)
    return fake


def _partition(filelist_path, num_shards, staging_dir, task_id=""):
    """Activities that heartbeat need an activity context to run in."""
    env = ActivityEnvironment()
    return asyncio.run(env.run(partition_files_greedy, str(filelist_path),
                               num_shards, str(staging_dir), task_id))


def _write_filelist(tmp_path, n):
    files = [{"path": f"data/f{i}.tar", "size": (i + 1) * 1000} for i in range(n)]
    p = tmp_path / "filtered.json"
    p.write_text(json.dumps(files))
    return p, files


# --- partition_files_greedy -------------------------------------------------

def test_the_bos_key_is_scoped_by_task_id(tmp_path, bos):
    fl, _ = _write_filelist(tmp_path, 6)
    results = _partition(fl, 3, tmp_path / "staging" / "reused-name",
                         task_id="t-20260807-abc")

    keys = [r["filelist_key"] for r in results]
    assert keys == [
        f"download-manager/filelists/t-20260807-abc/shard-{i}.json"
        for i in range(3)
    ]
    assert sorted(bos.objects) == sorted(keys)
    assert not any("reused-name" in k for k in keys), (
        "task name in the key: two same-named tasks collide")


def test_an_absent_task_id_falls_back_to_the_staging_name(tmp_path, bos):
    """Executions that started before the argument existed replay with it
    empty; they must land on the path their own earlier steps used."""
    fl, _ = _write_filelist(tmp_path, 2)
    results = _partition(fl, 1, tmp_path / "staging" / "old-run")

    assert results[0]["filelist_key"] == \
        "download-manager/filelists/old-run/shard-0.json"


@pytest.mark.parametrize("n_files,num_shards", [(1, 1), (10, 3), (7, 7), (100, 8)])
def test_every_file_lands_in_exactly_one_shard(tmp_path, bos, n_files, num_shards):
    """The invariant the coordinator's coverage gate depends on. A partition
    that drops files produces shards that report done having moved nothing —
    the false-`done` signature (see tests/test_sharded_completion.py)."""
    fl, files = _write_filelist(tmp_path, n_files)
    results = _partition(fl, num_shards, tmp_path / "s", task_id="t-x")

    assert sum(r["total_files"] for r in results) == n_files
    assert sum(r["total_bytes"] for r in results) == sum(f["size"] for f in files)

    placed = [entry["path"]
              for r in results
              for entry in json.loads(bos.objects[r["filelist_key"]])]
    assert sorted(placed) == sorted(f["path"] for f in files), \
        "a path was dropped or duplicated across shards"


def test_the_reported_md5_is_over_the_bytes_actually_uploaded(tmp_path, bos):
    """The md5 is what the shard checks its download against, so it has to be
    computed over the uploaded object — not over some other rendering of the
    same list."""
    fl, _ = _write_filelist(tmp_path, 9)
    results = _partition(fl, 4, tmp_path / "s", task_id="t-x")

    for r in results:
        uploaded = bos.objects[r["filelist_key"]]
        assert hashlib.md5(uploaded).hexdigest() == r["filelist_md5"]


# --- download_shard_filelist ------------------------------------------------

def _download(key, staging_dir, expected_md5=""):
    env = ActivityEnvironment()
    return asyncio.run(env.run(download_shard_filelist, key, str(staging_dir),
                               expected_md5))


def test_a_matching_filelist_is_written_to_disk(tmp_path, bos):
    fl, files = _write_filelist(tmp_path, 4)
    results = _partition(fl, 2, tmp_path / "s", task_id="t-x")
    r = results[0]

    local = _download(r["filelist_key"], tmp_path / "shard", r["filelist_md5"])

    assert pathlib.Path(local).read_bytes() == bos.objects[r["filelist_key"]]


def test_an_overwritten_filelist_is_refused_before_it_reaches_disk(tmp_path, bos):
    """The collision, end to end: task A partitions, task B overwrites the
    key, task A's shard fetches it. Without the check that shard downloads
    B's repo into A's BOS prefix and every counter says success."""
    fl_a, _ = _write_filelist(tmp_path, 4)
    a = _partition(fl_a, 2, tmp_path / "sa", task_id="t-a")[0]

    bos.objects[a["filelist_key"]] = json.dumps(
        [{"path": "someone/elses/repo.tar", "size": 99}]).encode()

    staging = tmp_path / "shard"
    with pytest.raises(FilelistMismatchError) as e:
        _download(a["filelist_key"], staging, a["filelist_md5"])

    assert a["filelist_md5"] in str(e.value)
    assert not list(staging.glob(".filelist-*.json")), \
        "wrote the wrong filelist to disk before checking it"


def test_the_mismatch_error_is_non_retryable_in_the_workflow(tmp_path):
    """Permanent by nature — the object was overwritten, so all five attempts
    re-read the same wrong bytes, 30s apart, once per shard. Matched by class
    name because importing activities into workflows.py would break
    determinism."""
    from dlm.temporal.workflows import NON_RETRYABLE_ERRORS

    assert FilelistMismatchError.__name__ in NON_RETRYABLE_ERRORS


def test_an_empty_expected_md5_skips_the_check(tmp_path, bos):
    """Shards dispatched before the argument existed carry no md5; they
    replay unchecked rather than fail on a field their coordinator never
    sent."""
    fl, _ = _write_filelist(tmp_path, 2)
    r = _partition(fl, 1, tmp_path / "s", task_id="t-x")[0]
    bos.objects[r["filelist_key"]] = b'[{"path": "other.tar", "size": 1}]'

    local = _download(r["filelist_key"], tmp_path / "shard")

    assert json.loads(pathlib.Path(local).read_text())[0]["path"] == "other.tar"
