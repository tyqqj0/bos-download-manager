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


def test_live_workflow_matches_every_id_scheme():
    from dlm.web.fleet import has_live_workflow

    tid = "t-20260730-c4caf4"
    for wid in (
        f"dl-{tid}",                 # legacy single-node
        f"split-download-{tid}",     # legacy split parent
        f"sharded-{tid}",            # current coordinator
        f"shard-s-{tid}-3",          # current shard child
        f"{tid}-part1",              # legacy split child
        f"dl-{tid}-v3",              # suffixed retry of the legacy id
    ):
        assert has_live_workflow(tid, {wid}), wid


def test_live_workflow_does_not_match_a_different_task():
    from dlm.web.fleet import has_live_workflow

    assert not has_live_workflow("t-aaa", {"sharded-t-bbb", "shard-s-t-bbb-0"})
    assert not has_live_workflow("t-aaa", set())


def test_merge_keeps_sidecar_metrics_the_fresher_temporal_row_lacks():
    """The false-alarm shape: freshest-wins drops every metric a worker sends.

    `wN@temporal` heartbeats more often than `wN@sidecar` but carries no
    metrics, so dedupe_workers picked it and Layer 3 reported every busy
    HK worker as having no sidecar.
    """
    from dlm.web.fleet import merge_workers

    now = 1000.0
    rows = [
        {"server_key": "w1", "hostname": "w1@temporal", "last_seen": now,
         "files_last_5min": None, "https_connections": None},
        {"server_key": "w1", "hostname": "w1@sidecar", "last_seen": now - 20,
         "files_last_5min": 1225, "https_connections": 21},
    ]

    merged, = merge_workers(rows, now)

    assert merged["last_seen"] == now          # liveness from the freshest row
    assert merged["files_last_5min"] == 1225   # metrics from the sidecar row
    assert merged["https_connections"] == 21


def test_merge_drops_metrics_from_a_hostname_that_went_quiet():
    """A dead sidecar must stop contributing, not look healthy forever."""
    from dlm.web.fleet import merge_workers

    now = 1000.0
    rows = [
        {"server_key": "w1", "hostname": "w1@temporal", "last_seen": now},
        {"server_key": "w1", "hostname": "w1@sidecar", "last_seen": now - 7200,
         "files_last_5min": 1225, "download_process_alive": 1},
    ]

    merged, = merge_workers(rows, now)

    assert merged.get("files_last_5min") is None
    assert merged.get("download_process_alive") is None
