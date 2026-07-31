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
from dlm.core.bos import bos_target


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
