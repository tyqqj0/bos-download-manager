"""Shard identifier round-trip.

The workflow builds the shard run name, the activity parses it back to get
both the shard row id and the base task name. If those two disagree the
symptom is not an error: progress POSTs hit a shard id that does not exist,
the endpoint answers "not found" to a fire-and-forget request, and a
perfectly healthy shard reads as stalled on the dashboard.

Run: python3 -m pytest tests/test_naming.py -q
"""

from __future__ import annotations

from dlm.core.naming import shard_row_id, shard_task_name, split_shard_name


def test_shard_name_round_trips():
    name = shard_task_name("AgiBotWorld-Beta-BJ", 3)
    assert name == "AgiBotWorld-Beta-BJ/shard-3"
    assert split_shard_name(name) == ("AgiBotWorld-Beta-BJ", "3")


def test_parsed_index_rebuilds_the_row_id_the_web_side_created():
    # web side, at shard-row creation
    created = shard_row_id("t-20260730-c4caf4", 3)
    # worker side, from the run name alone
    _, idx = split_shard_name(shard_task_name("AgiBotWorld-Beta-BJ", 3))
    assert shard_row_id("t-20260730-c4caf4", idx) == created


def test_unsharded_name_parses_as_no_shard():
    assert split_shard_name("MolmoAct-Dataset") == ("MolmoAct-Dataset", None)


def test_a_dataset_whose_own_name_contains_shard_is_not_misparsed():
    assert split_shard_name("weird/shard-thing") == ("weird/shard-thing", None)


def test_only_the_last_segment_is_treated_as_the_index():
    # A base name may itself contain the separator; the shard suffix is last.
    name = shard_task_name("set/shard-1", 2)
    assert split_shard_name(name) == ("set/shard-1", "2")
