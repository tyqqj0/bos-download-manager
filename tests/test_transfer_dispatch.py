"""Phase three: posting the imports, and believing them only after measuring.

Run: python3 -m pytest tests/test_transfer_dispatch.py -q

Two properties, both learned the hard way:

1. **Never two importers on one directory.** The far side's async task list is
   consulted before every post, and the `transferring` write is committed the
   instant the post returns — so a lost write re-attaches next cycle instead of
   posting again. The 2026-08-04 DL3DV run is what this is for: our side gave up
   at its poll cap while the remote task ran on for two more days.
2. **"The far side said 成功" is not "the bytes are there."** The old poller
   wrote `done` on the remote status alone. Here a remote success only earns
   `verifying`; `done` requires the size and scope checks to pass, and a short
   result is `short`, which is a different word on purpose.

The fakes below stand in for BOS and 地瓜云. Both real clients are HTTP, and the
point of these tests is the state machine, not their wire format. The one piece
of wire format that the state machine depends on is pinned by the last test in
this file: `inflight.endpoint_source` must reproduce exactly the `endpoint`
string `DCloudClient.import_from_bos` posts, because that string is what the
re-attach check matches on. Every fake here builds its `source` field by calling
`endpoint_source`, so nothing else in the suite would notice if the two drifted.
"""

from __future__ import annotations

import pytest

from dlm.constants import DATA_BUCKET
from dlm.transfer import dispatch as dispatch_mod
from dlm.transfer.dispatch import (
    MAX_IN_FLIGHT, MAX_PER_CYCLE, UNKNOWN_VERIFY_PER_CYCLE,
    dispatch_ready_transfers, plan_for_row, poll_transfers,
)
from dlm.transfer.measure import bos_stats, bos_top_children
from dlm.transfer.verify import verify_transfer

PREFIX = f"{DATA_BUCKET}/other/molmobot-data/"
TARGET = "/727a2f92-30c/auwomo-datasets/raw-data/other/molmobot-data"


# --- fakes -------------------------------------------------------------------

class _Obj:
    def __init__(self, key, size):
        self.key, self.size = key, size


class _CommonPrefix:
    def __init__(self, prefix):
        self.prefix = prefix


class _Resp:
    def __init__(self, contents, common_prefixes, is_truncated, next_marker):
        self.contents = contents
        self.common_prefixes = common_prefixes
        self.is_truncated = is_truncated
        self.next_marker = next_marker


class FakeBos:
    """`list_objects` over an in-memory `{key: size}` map, with paging."""

    def __init__(self, keys: dict, page_size: int = 1000):
        self.keys = dict(keys)
        self.page_size = page_size
        self.calls = 0

    def list_objects(self, bucket, prefix="", delimiter=None, marker="",
                     max_keys=1000):
        self.calls += 1
        matching = sorted(k for k in self.keys if k.startswith(prefix))
        if delimiter:
            groups, files = set(), []
            for key in matching:
                rest = key[len(prefix):]
                head, sep, _ = rest.partition(delimiter)
                if sep:
                    groups.add(prefix + head + delimiter)
                else:
                    files.append(_Obj(key, self.keys[key]))
            return _Resp(files, [_CommonPrefix(g) for g in sorted(groups)],
                         False, "")
        after = [k for k in matching if k > marker] if marker else matching
        page = after[:self.page_size]
        truncated = len(after) > len(page)
        return _Resp([_Obj(k, self.keys[k]) for k in page], [], truncated,
                     page[-1] if page else "")


class FakeDCloud:
    """`list_files` over a `{path: [{"name","size"}]}` tree, plus the async task
    list and the import call, both recorded."""

    def __init__(self, tree=None, tasks=None, next_id="remote-1"):
        self.tree = tree or {}
        self.tasks = list(tasks or [])
        self.next_id = next_id
        self.imports = []
        self.logins = 0
        self.folders = []

    def login(self):
        self.logins += 1

    def list_async_tasks(self, page=1, page_size=50):
        return self.tasks if page == 1 else []

    def list_files(self, path, page=1, page_size=50):
        entries = self.tree.get(path, []) if page == 1 else []
        return {"data": {"files": entries}}

    def create_folder(self, path, name):
        self.folders.append((path, name))

    def import_from_bos(self, **kwargs):
        self.imports.append(kwargs)
        return f"{self.next_id}-{len(self.imports)}"


def _install(monkeypatch, bos=None, dcloud=None):
    bos = bos if bos is not None else FakeBos({})
    dcloud = dcloud if dcloud is not None else FakeDCloud()
    monkeypatch.setattr(dispatch_mod, "_clients",
                        lambda: (bos, dcloud, {"BAIDU_AK": "ak", "BAIDU_SK": "sk"}))
    return bos, dcloud


# --- rows --------------------------------------------------------------------

def _task(db, task_id="t-1", **over):
    row = {
        "id": task_id, "name": "molmobot-data", "repo_id": "org/molmobot",
        "source": "hf", "type": "dataset", "category": "other",
        "status": "done", "priority": 0, "dispatch_mode": "pool",
    }
    row.update(over)
    db.upsert_task(row)
    return row


def _transfer(db, task_id, **cols):
    """Set transfer columns directly — the arming path is phase two's subject."""
    assignments = ", ".join(f"{k} = ?" for k in cols)
    conn = db._conn()
    conn.execute(f"UPDATE tasks SET {assignments} WHERE id = ?",
                 (*cols.values(), task_id))
    conn.commit()


def _ready(db, task_id="t-1", armed_at=1000.0, dispatched=1_000_000,
           name="molmobot-data", category="other"):
    """An armed row, exactly as `arm._write` leaves one.

    The prefix is the derived one on purpose: arming always stores
    `plan.source`, so a stored prefix that disagrees with `name`/`category` means
    a rename — which is what the drift test constructs by hand.
    """
    _task(db, task_id, name=name, category=category)
    _transfer(db, task_id, transfer_status="ready",
              transfer_prefix=f"{DATA_BUCKET}/{category}/{name}/",
              transfer_bytes=dispatched, transfer_armed_at=armed_at)
    return task_id


def _in_flight(db, n, status="transferring"):
    for i in range(n):
        _task(db, f"t-flight-{i}", name=f"flight-{i}")
        _transfer(db, f"t-flight-{i}", transfer_status=status,
                  transfer_task_id=f"remote-{i}")


def _status(db, task_id="t-1"):
    return db.get_task(task_id)


# --- the quota ---------------------------------------------------------------

def test_sixteen_in_flight_posts_nothing_new(db, monkeypatch):
    """The user's cap (2026-08-10): 16 concurrent imports, no more."""
    _in_flight(db, MAX_IN_FLIGHT)
    _ready(db)
    bos, dcloud = _install(monkeypatch, FakeBos({"other/molmobot-data/a": 10 ** 6}))

    report = dispatch_ready_transfers()

    assert (report["in_flight"], report["quota"]) == (MAX_IN_FLIGHT, 0)
    assert dcloud.imports == []
    assert _status(db)["transfer_status"] == "ready"


def test_fifteen_in_flight_posts_exactly_one(db, monkeypatch):
    _in_flight(db, MAX_IN_FLIGHT - 1)
    for i in range(4):
        _ready(db, f"t-r{i}", armed_at=1000.0 + i, name=f"ds-{i}")
    bos, dcloud = _install(monkeypatch, FakeBos({f"other/ds-{i}/a": 10 ** 6
                                                 for i in range(4)}))

    report = dispatch_ready_transfers()

    assert report["quota"] == 1
    assert len(dcloud.imports) == 1
    assert len(report["dispatched"]) == 1


def test_the_verifying_state_counts_against_the_cap(db, monkeypatch):
    """`verifying` is still our transfer occupying the far side's directory —
    counting only `transferring` would let a 17th import through."""
    _in_flight(db, MAX_IN_FLIGHT, status="verifying")
    _ready(db)
    _, dcloud = _install(monkeypatch)

    report = dispatch_ready_transfers()

    assert report["in_flight"] == MAX_IN_FLIGHT
    assert dcloud.imports == []


def test_at_most_four_per_cycle_even_with_fifty_ready(db, monkeypatch):
    """Each post costs a full BOS prefix scan; a cycle that posted all 16 would
    hold its thread for minutes. Four a minute still saturates in four minutes."""
    for i in range(50):
        _ready(db, f"t-r{i}", armed_at=1000.0 + i, name=f"ds-{i}")
    bos, dcloud = _install(monkeypatch, FakeBos({f"other/ds-{i}/a": 10 ** 6
                                                 for i in range(50)}))

    report = dispatch_ready_transfers()

    assert len(dcloud.imports) == MAX_PER_CYCLE == 4
    assert len(report["dispatched"]) == 4


def test_paused_posts_nothing(db, monkeypatch):
    from dlm.transfer.arm import set_transfers_paused
    _ready(db)
    _, dcloud = _install(monkeypatch)
    set_transfers_paused(True)

    report = dispatch_ready_transfers()

    assert report["skipped"] == "transfers are paused"
    assert dcloud.imports == []
    assert _status(db)["transfer_status"] == "ready"


def test_fifo_by_armed_at(db, monkeypatch):
    """Oldest armed row goes first — otherwise a busy fleet can starve the task
    that finished first behind ones that finished later."""
    _ready(db, "t-late", armed_at=3000.0, name="late")
    _ready(db, "t-early", armed_at=1000.0, name="early")
    _ready(db, "t-mid", armed_at=2000.0, name="mid")
    bos, dcloud = _install(monkeypatch, FakeBos({
        "other/late/a": 10 ** 6, "other/early/a": 10 ** 6, "other/mid/a": 10 ** 6}))
    monkeypatch.setattr(dispatch_mod, "MAX_PER_CYCLE", 1)

    dispatch_ready_transfers()

    assert [i["bos_path"] for i in dcloud.imports] == ["other/early/"]


# --- what gets posted --------------------------------------------------------

def test_dispatch_records_transferring_and_the_remote_task_id(db, monkeypatch):
    _ready(db)
    bos, dcloud = _install(monkeypatch, FakeBos({"other/molmobot-data/a": 10 ** 6}))

    dispatch_ready_transfers()

    row = _status(db)
    assert row["transfer_status"] == "transferring"
    assert row["transfer_task_id"] == "remote-1-1"
    assert row["transfer_bos_bytes"] == 10 ** 6
    assert row["transfer_bos_objects"] == 1
    assert row["transfer_error"] is None
    assert dcloud.imports[0]["bos_bucket"] == DATA_BUCKET
    assert dcloud.imports[0]["bos_path"] == "other/molmobot-data/"
    assert dcloud.imports[0]["target_path"] == TARGET


def test_a_model_goes_from_the_model_bucket_to_the_model_root(db, monkeypatch):
    """The manual script hardcoded `auwomo-data` and the dataset layout, so not
    one of the six `done` models (272.7 GB) could be addressed at all. Both ends
    come from `bos_target`/`plan_transfer` here, so a model is just a task."""
    from dlm.constants import MODEL_BUCKET
    _task(db, "t-m", name="Qwen3-VL-30B-A3B-Thinking", category="multimodal",
          type="model")
    _transfer(db, "t-m", transfer_status="ready", transfer_bytes=10 ** 6,
              transfer_armed_at=1.0,
              transfer_prefix=f"{MODEL_BUCKET}/Qwen3-VL-30B-A3B-Thinking/")
    bos, dcloud = _install(
        monkeypatch, FakeBos({"Qwen3-VL-30B-A3B-Thinking/model.safetensors": 10 ** 6}))

    dispatch_ready_transfers()

    assert _status(db, "t-m")["transfer_status"] == "transferring"
    assert dcloud.imports[0]["bos_bucket"] == MODEL_BUCKET
    assert dcloud.imports[0]["bos_path"] == "Qwen3-VL-30B-A3B-Thinking/"
    assert dcloud.imports[0]["target_path"] == (
        "/727a2f92-30c/auwomo-model/multimodal/Qwen3-VL-30B-A3B-Thinking")


def test_a_rename_after_arming_blocks_instead_of_importing_elsewhere(db, monkeypatch):
    _ready(db)
    _transfer(db, "t-1", name="renamed-after-arming")
    _, dcloud = _install(monkeypatch)

    report = dispatch_ready_transfers()

    row = _status(db)
    assert row["transfer_status"] == "blocked"
    assert "prefix drift" in row["transfer_error"]
    assert dcloud.imports == []
    assert report["blocked"][0]["task_id"] == "t-1"


def test_an_empty_bos_prefix_blocks_and_posts_nothing(db, monkeypatch):
    """The ratio bands run at dispatch time, where the measurement exists. An
    empty prefix is the `t-20260805-460d45` shape: a `done` with no data."""
    _ready(db)
    bos, dcloud = _install(monkeypatch, FakeBos({}))

    dispatch_ready_transfers()

    row = _status(db)
    assert row["transfer_status"] == "blocked"
    assert "empty" in row["transfer_error"]
    assert dcloud.imports == []


def test_a_middle_band_ratio_still_transfers(db, monkeypatch):
    """0.50–0.95 is a warning, not a refusal: the far side's import skips what is
    already there, so topping up later is cheap, while stalling a real
    completion needs a human."""
    _ready(db, dispatched=1_000_000)
    bos, dcloud = _install(monkeypatch, FakeBos({"other/molmobot-data/a": 700_000}))

    report = dispatch_ready_transfers()

    assert _status(db)["transfer_status"] == "transferring"
    assert len(dcloud.imports) == 1
    assert "ratio" in report["dispatched"][0]["note"]


def test_an_import_already_running_on_the_far_side_is_re_attached(db, monkeypatch):
    """Two importers writing one directory is the failure this whole check
    exists to prevent."""
    from dlm.transfer.inflight import endpoint_source
    _ready(db)
    remote = {"task_id": "remote-existing", "status": "运行中",
              "source": endpoint_source(DATA_BUCKET, "other/molmobot-data/"),
              "target": TARGET}
    bos, dcloud = _install(monkeypatch,
                           FakeBos({"other/molmobot-data/a": 10 ** 6}),
                           FakeDCloud(tasks=[remote]))

    report = dispatch_ready_transfers()

    assert dcloud.imports == []
    assert _status(db)["transfer_task_id"] == "remote-existing"
    assert report["dispatched"][0]["reattached"] is True


def test_a_failed_remote_listing_posts_nothing_at_all(db, monkeypatch):
    """Without the remote list we cannot tell a first post from a second one."""
    _ready(db)

    class Blind(FakeDCloud):
        def list_async_tasks(self, page=1, page_size=50):
            raise RuntimeError("gateway timeout")

    bos, dcloud = _install(monkeypatch, FakeBos({"other/molmobot-data/a": 10 ** 6}),
                           Blind())

    report = dispatch_ready_transfers()

    assert dcloud.imports == []
    assert _status(db)["transfer_status"] == "ready"
    assert any("remote async tasks" in e for e in report["errors"])


def test_a_dispatch_failure_leaves_the_row_ready_with_the_reason(db, monkeypatch):
    _ready(db)

    class Refuses(FakeDCloud):
        def import_from_bos(self, **kwargs):
            raise RuntimeError("500 from the import API")

    bos, dcloud = _install(monkeypatch, FakeBos({"other/molmobot-data/a": 10 ** 6}),
                           Refuses())

    report = dispatch_ready_transfers()

    row = _status(db)
    # Nothing was posted, so nothing is inconsistent — the next cycle retries.
    assert row["transfer_status"] == "ready"
    assert "500 from the import API" in row["transfer_error"]
    assert report["errors"]


def test_two_consecutive_failures_stop_the_cycle(db, monkeypatch):
    """The far side refusing us twice means it is refusing us; six more attempts
    would only make noise. Same rule as scripts/transfer_import.py."""
    for i in range(4):
        _ready(db, f"t-r{i}", armed_at=1000.0 + i, name=f"ds-{i}")

    class Refuses(FakeDCloud):
        def import_from_bos(self, **kwargs):
            self.imports.append(kwargs)
            raise RuntimeError("nope")

    bos, dcloud = _install(monkeypatch,
                           FakeBos({f"other/ds-{i}/a": 10 ** 6 for i in range(4)}),
                           Refuses())

    report = dispatch_ready_transfers()

    assert len(dcloud.imports) == 2
    assert any("in a row" in e for e in report["errors"])
    assert _status(db, "t-r3")["transfer_status"] == "ready"


def test_missing_credentials_are_reported_not_raised(db, monkeypatch):
    _ready(db)
    monkeypatch.setattr(dispatch_mod, "_clients", lambda: None)

    report = dispatch_ready_transfers()

    assert any("DCLOUD_USER" in e for e in report["errors"])
    assert _status(db)["transfer_status"] == "ready"


# --- polling and verification ------------------------------------------------

def _transferring(db, task_id="t-1", bos_bytes=1000, bos_objects=1,
                  remote_id="remote-1", status="transferring"):
    _task(db, task_id)
    _transfer(db, task_id, transfer_status=status, transfer_prefix=PREFIX,
              transfer_bytes=bos_bytes, transfer_bos_bytes=bos_bytes,
              transfer_bos_objects=bos_objects, transfer_task_id=remote_id,
              transfer_armed_at=1.0)


def _tree(jfs_bytes, children=("episodes",)):
    parent = TARGET.rsplit("/", 1)[0]
    return {
        parent: [{"name": "molmobot-data", "size": jfs_bytes}],
        TARGET: [{"name": c, "size": 1} for c in children],
    }


def test_remote_success_with_matching_bytes_becomes_done(db, monkeypatch):
    _transferring(db, bos_bytes=1000)
    bos, dcloud = _install(
        monkeypatch,
        FakeBos({"other/molmobot-data/episodes/a": 1000}),
        FakeDCloud(tree=_tree(1000), tasks=[{"task_id": "remote-1", "status": "成功"}]))

    report = poll_transfers()

    row = _status(db)
    assert row["transfer_status"] == "done"
    assert row["transfer_verified_bytes"] == 1000
    assert row["transfer_error"] is None
    assert report["done"][0]["task_id"] == "t-1"


def test_remote_success_but_short_bytes_is_short_not_done(db, monkeypatch):
    """The old poller wrote `done` on the remote status alone. This is the whole
    reason the `verifying` step exists."""
    _transferring(db, bos_bytes=1000)
    bos, dcloud = _install(
        monkeypatch,
        FakeBos({"other/molmobot-data/episodes/a": 1000}),
        FakeDCloud(tree=_tree(400), tasks=[{"task_id": "remote-1", "status": "成功"}]))

    report = poll_transfers()

    row = _status(db)
    assert row["transfer_status"] == "short"
    assert row["transfer_verified_bytes"] == 400
    assert "600 short" in row["transfer_error"]
    assert report["short"][0]["task_id"] == "t-1"


def test_a_missing_target_folder_is_short(db, monkeypatch):
    _transferring(db, bos_bytes=1000)
    bos, dcloud = _install(
        monkeypatch,
        FakeBos({"other/molmobot-data/episodes/a": 1000}),
        FakeDCloud(tree={}, tasks=[{"task_id": "remote-1", "status": "成功"}]))

    poll_transfers()

    row = _status(db)
    assert row["transfer_status"] == "short"
    assert "does not exist" in row["transfer_error"]


def test_extra_scope_children_are_short_even_when_the_size_passes(db, monkeypatch):
    """Oversize passes the byte check happily. Only the scope check catches
    prefix bleed — a slash-less `RDT-1B` import drags in `RDT-1B-repair/`."""
    _transferring(db, bos_bytes=1000)
    bos, dcloud = _install(
        monkeypatch,
        FakeBos({"other/molmobot-data/episodes/a": 1000}),
        FakeDCloud(tree=_tree(9000, children=("episodes", "somebody-elses-data")),
                   tasks=[{"task_id": "remote-1", "status": "成功"}]))

    poll_transfers()

    row = _status(db)
    assert row["transfer_status"] == "short"
    assert "scope check failed" in row["transfer_error"]
    assert "somebody-elses-data" in row["transfer_error"]


def test_a_changed_bos_prefix_is_recorded_on_an_otherwise_done_row(db, monkeypatch):
    """We only ever read BOS, so a change is another actor writing the prefix
    mid-transfer. Recorded, never used to fail the transfer."""
    _transferring(db, bos_bytes=1000, bos_objects=1)
    bos, dcloud = _install(
        monkeypatch,
        FakeBos({"other/molmobot-data/episodes/a": 1000,
                 "other/molmobot-data/episodes/b": 7}),
        FakeDCloud(tree=_tree(1007), tasks=[{"task_id": "remote-1", "status": "成功"}]))

    poll_transfers()

    row = _status(db)
    assert row["transfer_status"] == "done"
    assert "BOS prefix changed" in row["transfer_error"]


def test_remote_failure_becomes_failed_with_the_remote_reason(db, monkeypatch):
    _transferring(db)
    bos, dcloud = _install(monkeypatch, FakeBos({}), FakeDCloud(
        tasks=[{"task_id": "remote-1", "status": "失败",
                "error_msg": "juicefs <FATAL>: failed to handle 2 objects"}]))

    report = poll_transfers()

    row = _status(db)
    assert row["transfer_status"] == "failed"
    assert "failed to handle 2 objects" in row["transfer_error"]
    assert report["failed"][0]["task_id"] == "t-1"
    # A failed remote import is NOT re-verified — there is nothing to measure.
    assert row["transfer_verified_bytes"] == 0


def test_a_running_remote_task_is_left_alone(db, monkeypatch):
    _transferring(db)
    bos, dcloud = _install(monkeypatch, FakeBos({}), FakeDCloud(
        tasks=[{"task_id": "remote-1", "status": "运行中"}]))

    report = poll_transfers()

    assert _status(db)["transfer_status"] == "transferring"
    assert report["running"] == 1


def test_a_verifying_row_re_verifies_without_the_remote_record(db, monkeypatch):
    """Verification is minutes of listing; a process that dies in the middle of
    it must recover. `verifying` says "the far side finished, we still owe a
    measurement" — and needs no remote lookup, which is what makes it survive
    the remote record aging off the list."""
    _transferring(db, bos_bytes=1000, status="verifying")
    bos, dcloud = _install(
        monkeypatch,
        FakeBos({"other/molmobot-data/episodes/a": 1000}),
        FakeDCloud(tree=_tree(1000), tasks=[]))  # no remote record at all

    poll_transfers()

    assert _status(db)["transfer_status"] == "done"


def test_a_remote_record_that_aged_off_the_list_is_measured_not_guessed(db, monkeypatch):
    """`fetch_tasks` reads the newest 500 records and the far side holds 672, so
    a long import WILL vanish from the list. An incomplete target is not proof
    the import died — "still copying" measures identically — so the row keeps
    its `transferring` slot and records what the measurement saw. Writing
    `short` here would invite a human to post a second importer onto a
    directory a live import is still writing."""
    _transferring(db)
    bos, dcloud = _install(monkeypatch, FakeBos({}), FakeDCloud(tasks=[]))

    report = poll_transfers()

    assert report["unknown_remote"] == 1
    row = _status(db)
    assert row["transfer_status"] == "transferring"
    assert "remote record not found" in row["transfer_error"]


def test_an_aged_off_record_whose_target_is_complete_lands_done(db, monkeypatch):
    """The escape hatch. A complete target IS proof, whatever the list says —
    without this the row holds one of the 16 slots forever and alerts nothing."""
    _transferring(db, bos_bytes=1000)
    bos, dcloud = _install(
        monkeypatch,
        FakeBos({"other/molmobot-data/episodes/a": 1000}),
        FakeDCloud(tree=_tree(1000), tasks=[]))

    report = poll_transfers()

    row = _status(db)
    assert row["transfer_status"] == "done"
    assert row["transfer_verified_bytes"] == 1000
    assert report["unknown_remote"] == 1
    assert report["done"][0]["task_id"] == "t-1"
    assert "remote record not found" in report["done"][0]["detail"]


def test_only_two_aged_off_rows_are_measured_per_pass(db, monkeypatch):
    """Each measurement walks both ends; the stage has 600s. All of them are
    still counted — the bound is on the work, not on the reporting."""
    for i in range(4):
        _transferring(db, task_id=f"t-{i}", remote_id=f"remote-{i}")
    bos, dcloud = _install(monkeypatch, FakeBos({}), FakeDCloud(tasks=[]))

    report = poll_transfers()

    assert report["unknown_remote"] == 4
    measured = [i for i in range(4)
                if _status(db, f"t-{i}")["transfer_error"] is not None]
    assert len(measured) == UNKNOWN_VERIFY_PER_CYCLE


def test_a_row_with_no_dispatch_measurement_cannot_verify_clean(db, monkeypatch):
    """`jfs_bytes >= 0` is true for every folder. A row armed before the
    dispatcher existed must not verify by default."""
    _transferring(db, bos_bytes=0)
    _transfer(db, "t-1", transfer_bos_bytes=0)
    bos, dcloud = _install(
        monkeypatch, FakeBos({}),
        FakeDCloud(tree=_tree(0), tasks=[{"task_id": "remote-1", "status": "成功"}]))

    poll_transfers()

    row = _status(db)
    assert row["transfer_status"] == "short"
    assert "no dispatch-time BOS measurement" in row["transfer_error"]


def test_one_bad_row_does_not_stop_the_others(db, monkeypatch):
    """Sixteen rows share one poll pass; a far side that fails to list one
    target must not cost the other fifteen their verdict."""
    from dlm.transfer import measure
    monkeypatch.setattr(measure.time, "sleep", lambda _s: None)  # 3 retries × 30s

    good_parent = "/727a2f92-30c/auwomo-datasets/raw-data/other"
    bad_parent = "/727a2f92-30c/auwomo-datasets/raw-data/manipulation"

    _task(db, "t-good", name="good-one", category="other")
    _transfer(db, "t-good", transfer_status="transferring", transfer_armed_at=1.0,
              transfer_prefix=f"{DATA_BUCKET}/other/good-one/",
              transfer_bos_bytes=1000, transfer_bos_objects=1,
              transfer_task_id="remote-good")
    _task(db, "t-bad", name="bad-one", category="manipulation")
    _transfer(db, "t-bad", transfer_status="transferring", transfer_armed_at=2.0,
              transfer_prefix=f"{DATA_BUCKET}/manipulation/bad-one/",
              transfer_bos_bytes=1000, transfer_bos_objects=1,
              transfer_task_id="remote-bad")

    class HalfDown(FakeDCloud):
        def list_files(self, path, page=1, page_size=50):
            if path.startswith(bad_parent):
                raise RuntimeError("files API is down")
            return super().list_files(path, page, page_size)

    bos, dcloud = _install(
        monkeypatch,
        FakeBos({"other/good-one/a": 1000, "manipulation/bad-one/a": 1000}),
        HalfDown(tree={good_parent: [{"name": "good-one", "size": 1000}],
                       f"{good_parent}/good-one": [{"name": "a", "size": 1000}]},
                 tasks=[{"task_id": "remote-good", "status": "成功"},
                        {"task_id": "remote-bad", "status": "成功"}]))

    report = poll_transfers()

    assert report["done"][0]["task_id"] == "t-good"
    assert _status(db, "t-good")["transfer_status"] == "done"
    assert any("bad-one" in e for e in report["errors"]), \
        "the failing row must be reported, not swallowed"
    # The failed row stays `verifying`: the far side finished, we still owe a
    # measurement, and the next cycle takes it without the remote record.
    assert _status(db, "t-bad")["transfer_status"] == "verifying"


# --- the four alerts ---------------------------------------------------------

def test_blocked_raises_a_critical_alert_naming_the_retry_route(db):
    from dlm.web.alerts import CRITICAL, check_alerts
    _task(db, "t-1")
    _transfer(db, "t-1", transfer_status="blocked",
              transfer_error="0 shard rows — nothing proves what was downloaded")

    alerts = {a["type"]: a for a in check_alerts(db.get_all_tasks(), [])}

    assert alerts["transfer_blocked"]["severity"] == CRITICAL
    assert "0 shard rows" in alerts["transfer_blocked"]["message"]
    assert "/api/transfer/t-1/retry" in alerts["transfer_blocked"]["message"]


@pytest.mark.parametrize("status", ["short", "failed"])
def test_short_and_failed_raise_warnings(db, status):
    """WARNING, not CRITICAL: the bytes are still on BOS and a retry fixes it."""
    from dlm.web.alerts import WARNING, check_alerts
    _task(db, "t-1")
    _transfer(db, "t-1", transfer_status=status, transfer_error="because")

    alerts = {a["type"]: a for a in check_alerts(db.get_all_tasks(), [])}

    assert alerts[f"transfer_{status}"]["severity"] == WARNING
    assert "because" in alerts[f"transfer_{status}"]["message"]


def test_healthy_transfer_states_raise_nothing(db):
    """An alert channel that fires on normal operation stops being read."""
    from dlm.web.alerts import check_alerts
    for i, status in enumerate(["ready", "transferring", "verifying", "done", None]):
        _task(db, f"t-{i}", name=f"ds-{i}")
        _transfer(db, f"t-{i}", transfer_status=status)

    alerts = check_alerts(db.get_all_tasks(), [])

    assert [a for a in alerts if a["type"].startswith("transfer_")] == []


def _stale_ready(db, task_id="t-1", age_s=7200):
    """A `ready` row armed `age_s` ago, relative to real now."""
    import time
    _ready(db, task_id, armed_at=time.time() - age_s)
    return task_id


def test_a_ready_row_nobody_dispatches_raises_a_stalled_warning(db):
    """The only transfer failure the far side cannot see: nothing was posted.

    Without this, a dead web process, a transfer stage wedged past its 600s
    deadline, or CONSECUTIVE_FAIL_LIMIT stopping the dispatch loop all leave
    the row sitting in `ready` forever and raise nothing at all.
    """
    from dlm.web.alerts import WARNING, check_alerts
    _stale_ready(db, "t-1")

    alerts = {a["type"]: a for a in check_alerts(db.get_all_tasks(), [])}

    assert alerts["transfer_stalled"]["severity"] == WARNING
    assert alerts["transfer_stalled"]["task_id"] == "t-1"
    assert "120 min" in alerts["transfer_stalled"]["message"]
    # It must point at our own process, not at the far side — the whole point
    # of this alert is that the far side has nothing to look at.
    assert "dlm-web" in alerts["transfer_stalled"]["message"]


def test_a_stale_ready_row_raises_nothing_while_transfers_are_paused(db):
    """Paused means "stop posting new ones" — a waiting row is then correct."""
    from dlm.transfer.arm import set_transfers_paused
    from dlm.web.alerts import check_alerts
    _stale_ready(db, "t-1")
    set_transfers_paused(True)
    try:
        alerts = check_alerts(db.get_all_tasks(), [])
    finally:
        set_transfers_paused(False)

    assert [a for a in alerts if a["type"] == "transfer_stalled"] == []


def test_a_stale_ready_row_raises_nothing_when_every_slot_is_busy(db):
    """16 in flight is the quota working, not a stall. Alerting here would fire
    on exactly the busiest, most normal state the system has."""
    from dlm.transfer.dispatch import MAX_IN_FLIGHT
    from dlm.web.alerts import check_alerts
    _stale_ready(db, "t-1")
    _in_flight(db, MAX_IN_FLIGHT)

    alerts = check_alerts(db.get_all_tasks(), [])

    assert [a for a in alerts if a["type"] == "transfer_stalled"] == []


def test_a_freshly_armed_ready_row_raises_nothing(db):
    """A row armed a minute ago is waiting its turn, by design."""
    from dlm.web.alerts import check_alerts
    _stale_ready(db, "t-1", age_s=60)

    alerts = check_alerts(db.get_all_tasks(), [])

    assert [a for a in alerts if a["type"] == "transfer_stalled"] == []


def test_a_ready_row_armed_before_the_column_existed_raises_nothing(db):
    """transfer_armed_at defaults to 0. Treating that as an epoch timestamp
    would report every such row as stalled for 56 years."""
    from dlm.web.alerts import check_alerts
    _task(db, "t-1")
    _transfer(db, "t-1", transfer_status="ready", transfer_armed_at=0)

    alerts = check_alerts(db.get_all_tasks(), [])

    assert [a for a in alerts if a["type"] == "transfer_stalled"] == []


# --- the measurement primitives ---------------------------------------------

def test_bos_stats_walks_every_page():
    """The move out of scripts/transfer_import.py must not have dropped
    truncation handling — a 3.4 TB dataset is ~50 pages."""
    bos = FakeBos({f"p/{i:03d}": 10 for i in range(250)}, page_size=100)

    assert bos_stats(bos, "b", "p/") == (2500, 250)
    assert bos.calls == 3


def test_bos_top_children_groups_by_delimiter():
    bos = FakeBos({"p/a/1": 1, "p/a/2": 1, "p/b/1": 1, "p/top.txt": 1})

    assert bos_top_children(bos, "b", "p/") == {"a", "b", "top.txt"}


def test_verify_treats_an_unlistable_scope_as_unknown_not_as_extras():
    """A failed listing must not read as "no extras found" — nor as extras."""
    class NoFiles(FakeDCloud):
        def list_files(self, path, page=1, page_size=50):
            if path == TARGET:
                raise RuntimeError("listing failed")
            return super().list_files(path, page, page_size)

    plan = plan_for_row({"name": "molmobot-data", "category": "other",
                         "type": "dataset", "transfer_prefix": PREFIX})
    verdict = verify_transfer(FakeBos({"other/molmobot-data/a": 1000}),
                              NoFiles(tree=_tree(1000)), plan, 1000, 1)

    assert verdict["status"] == "done"
    assert verdict["extra_children"] is None
    assert "scope check skipped" in verdict["detail"]


# --- the wire format the re-attach check depends on --------------------------

def test_the_posted_endpoint_is_exactly_what_endpoint_source_reconstructs():
    """Two importers on one directory is the failure this pins shut.

    Re-attach works by matching `inflight.endpoint_source(bucket, prefix)`
    against the `source` field the far side echoes back — which is the
    `endpoint` string `import_from_bos` posted. Nothing else ties those two
    f-strings together: they live in different modules, and every fake in this
    file builds its `source` by calling `endpoint_source`, so a drift in either
    one would keep the whole suite green while the live re-attach silently never
    matched and posted a second import onto a live directory.
    """
    from dlm.transfer import inflight
    from dlm.transfer.dcloud import DCloudClient

    posted = {}

    class _R:
        @staticmethod
        def raise_for_status():
            pass

        @staticmethod
        def json():
            return {"status": 0, "data": "remote-42"}

    client = DCloudClient("u", "p")
    client._http.post = lambda url, json=None, timeout=None: (
        posted.update(json), _R)[1]

    task_id = client.import_from_bos(
        bos_ak="ak", bos_sk="sk", bos_bucket=DATA_BUCKET,
        bos_path="other/molmobot-data/", target_path=TARGET)

    assert task_id == "remote-42"
    assert posted["endpoint"] == inflight.endpoint_source(
        DATA_BUCKET, "other/molmobot-data/")
    # And the default endpoint domain is the one endpoint_source assumes.
    assert posted["endpoint"].endswith("bj.bcebos.com/other/molmobot-data/")
