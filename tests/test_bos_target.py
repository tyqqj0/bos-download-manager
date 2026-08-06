"""The BOS target contract.

The uploader and the resume filter both call bos_target(). If they ever
disagree the filter inspects a prefix nothing was uploaded to, sees an empty
listing, and re-downloads the entire dataset — which is why this is one
function with tests rather than three hand-synced copies.

Run: python3 -m pytest tests/test_bos_target.py -q
"""

from __future__ import annotations

from dataclasses import dataclass

from dlm.constants import DATA_BUCKET, MODEL_BUCKET
import pytest

from dlm.core.bos import MULTIPART_THRESHOLD, bos_target, upload_file


@dataclass
class T:
    name: str
    category: str | None = None
    type: str = "dataset"


def test_dataset_prefix_is_category_then_name():
    assert bos_target(T("AgiBotWorld-Beta-BJ", "manipulation")) == (
        DATA_BUCKET, "manipulation/AgiBotWorld-Beta-BJ/"
    )


def test_model_ignores_category_and_uses_the_model_bucket():
    assert bos_target(T("Wan2.2-I2V", "manipulation", type="model")) == (
        MODEL_BUCKET, "Wan2.2-I2V/"
    )


def test_dataset_without_category_does_not_emit_a_none_segment():
    # The old bos_sdk copy produced "None/name/" here.
    assert bos_target(T("Loose-Dataset")) == (DATA_BUCKET, "Loose-Dataset/")
    assert bos_target(T("Loose-Dataset", "")) == (DATA_BUCKET, "Loose-Dataset/")


def test_prefix_always_ends_in_a_slash():
    # Callers strip it off keys with prefix-length slicing; a missing slash
    # would shift every relative key by one character.
    for task in (T("a", "b"), T("a"), T("a", type="model")):
        assert bos_target(task)[1].endswith("/")


# ---------------------------------------------------------------------------
# The multipart upload's silent failure.
# ---------------------------------------------------------------------------

class _FakeBos:
    """Stands in for BosClient. `multipart_ok=False` reproduces the driver's
    abort-and-return-False path."""

    def __init__(self, multipart_ok=True):
        self.multipart_ok = multipart_ok
        self.super_calls = []
        self.plain_calls = []

    def put_super_object_from_file(self, bucket, key, local_path, **kw):
        self.super_calls.append(key)
        return True if self.multipart_ok else False

    def put_object_from_file(self, bucket, key, local_path, **kw):
        self.plain_calls.append(key)
        return object()


def _big(tmp_path):
    p = tmp_path / "big.bin"
    p.write_bytes(b"x" * (MULTIPART_THRESHOLD + 1))
    return p


def test_upload_file_raises_when_multipart_returns_false(tmp_path):
    """`put_super_object_from_file` aborts the upload and returns False on a
    failed part — no exception. Callers delete their local copy once
    upload_file returns, so this must raise or the bytes are lost while the
    task reports success."""
    client = _FakeBos(multipart_ok=False)
    with pytest.raises(RuntimeError, match="multipart upload failed"):
        upload_file(client, "auwomo-data", "manipulation/X/big.bin", str(_big(tmp_path)))
    assert client.super_calls == ["manipulation/X/big.bin"]


def test_upload_file_returns_normally_when_multipart_succeeds(tmp_path):
    client = _FakeBos(multipart_ok=True)
    upload_file(client, "auwomo-data", "manipulation/X/big.bin", str(_big(tmp_path)))
    assert client.plain_calls == []


def test_small_files_still_take_the_single_put_path(tmp_path):
    p = tmp_path / "small.bin"
    p.write_bytes(b"y" * 1024)
    client = _FakeBos(multipart_ok=False)  # would fail if routed to multipart
    upload_file(client, "auwomo-data", "manipulation/X/small.bin", str(p))
    assert client.plain_calls == ["manipulation/X/small.bin"]
    assert client.super_calls == []
