"""Fleet primitives — these encode the rules that four call sites used to
re-derive independently, and every drift produced a production false alarm.

Run: python3 -m pytest tests/test_fleet.py -q
"""

import time

from dlm.web.fleet import (
    WORKER_TIMEOUT,
    alive_workers,
    busy_servers,
    dedupe_workers,
    idle_workers,
    source_for_worker,
    worker_serves,
)

NOW = 1_000_000.0


def w(key, hostname, seen_ago, disk=200):
    return {
        "server_key": key,
        "hostname": hostname,
        "last_seen": NOW - seen_ago,
        "disk_free_gb": disk,
    }


def test_dedupe_keeps_freshest_row_per_worker():
    # w1 reports twice: temporal is live, sidecar died 15h ago
    rows = [w("w1", "w1@sidecar", 55_000), w("w1", "w1@temporal", 5)]
    out = dedupe_workers(rows)
    assert len(out) == 1
    assert out[0]["hostname"] == "w1@temporal"


def test_stale_sidecar_row_does_not_mark_worker_offline():
    rows = [w("w1", "w1@sidecar", 55_000), w("w1", "w1@temporal", 5)]
    assert [x["server_key"] for x in alive_workers(rows, NOW)] == ["w1"]


def test_worker_past_timeout_is_not_alive():
    assert alive_workers([w("w1", "w1@temporal", WORKER_TIMEOUT + 1)], NOW) == []


def test_shard_ownership_makes_a_worker_busy():
    # A sharded task's own row has server=NULL — the servers are on the shards
    tasks = [{"id": "t1", "status": "downloading", "server": None}]
    shards = [{"task_id": "t1", "server": "bj1"}, {"task_id": "t1", "server": "bj2"}]
    assert busy_servers(tasks, shards) == {"bj1", "bj2"}


def test_legacy_task_level_server_still_counts_as_busy():
    tasks = [{"id": "t1", "status": "downloading", "server": "w4"}]
    assert busy_servers(tasks, []) == {"w4"}


def test_non_downloading_tasks_do_not_hold_a_worker():
    tasks = [{"id": "t1", "status": "paused", "server": "bj9"}]
    assert busy_servers(tasks, []) == set()


def test_routing_modelscope_to_bj_everything_else_to_hk():
    assert source_for_worker("bj3") == "modelscope"
    assert source_for_worker("w3") == "hf"
    assert worker_serves("bj3", "modelscope")
    assert not worker_serves("bj3", "hf")
    assert worker_serves("w3", "hf")
    assert not worker_serves("w3", "modelscope")
    # any other source routes to HK, not nowhere
    assert worker_serves("w3", "wget")
    assert not worker_serves("bj3", "wget")


def test_worker_running_a_shard_is_not_idle():
    tasks = [{"id": "t1", "status": "downloading", "server": None}]
    shards = [{"task_id": "t1", "server": "bj1"}]
    workers = [w("bj1", "bj1@temporal", 5), w("bj2", "bj2@temporal", 5)]
    idle = idle_workers(tasks, workers, shards, NOW)
    assert [i["server_key"] for i in idle] == ["bj2"]


def test_idle_with_empty_queue_is_not_starved():
    workers = [w("bj2", "bj2@temporal", 5)]
    idle = idle_workers([], workers, [], NOW)
    assert len(idle) == 1
    assert idle[0]["starved"] is False


def test_idle_while_matching_work_waits_is_starved():
    tasks = [{"id": "t2", "status": "pending", "source": "modelscope"}]
    workers = [w("bj2", "bj2@temporal", 5), w("w2", "w2@temporal", 5)]
    idle = {i["server_key"]: i for i in idle_workers(tasks, workers, [], NOW)}
    assert idle["bj2"]["starved"] is True   # ModelScope work is queued for it
    assert idle["w2"]["starved"] is False   # nothing it can serve is queued
