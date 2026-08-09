"""Phase-one transfer hardening: destination mapping, in-flight detection, and
the manual script's per-item failure isolation.

Run: python3 -m pytest tests/test_transfer_targets.py -q

Three regressions are pinned here, all of them real:

1. `transfer_import.py` hardcoded `auwomo-data` in three places, so a model
   could not be transferred at all.
2. Any single item's failure `sys.exit`'d the whole round — on 2026-08-04 a 72h
   poll timeout on item 4 took items 5-9 down unrun.
3. Nothing checked whether the remote import was still running before posting a
   new one. The DL3DV task our side abandoned on 08-07 kept writing until
   08-09 19:26.
"""

from __future__ import annotations

import importlib.util
import json
from dataclasses import dataclass
from pathlib import Path

import pytest

from dlm.constants import DATA_BUCKET, MODEL_BUCKET
from dlm.transfer import inflight
from dlm.transfer.targets import (
    DATASET_ROOT,
    MODEL_ROOT,
    plan_from_mapping,
    plan_transfer,
)

REPO_ROOT = Path(__file__).resolve().parents[1]


@dataclass
class T:
    name: str
    category: str | None = None
    type: str = "dataset"


# ── destination mapping ────────────────────────────────────────────────────


def test_dataset_lands_under_raw_data_category_name():
    p = plan_transfer(T("molmobot-data", "other"))
    assert (p.bucket, p.prefix) == (DATA_BUCKET, "other/molmobot-data/")
    assert p.target == f"{DATASET_ROOT}/other/molmobot-data"


def test_model_reads_the_model_bucket_and_keeps_the_category_in_the_target():
    # BOS has no category segment for models; the JuiceFS destination does.
    # Proven by a successful remote import: auwomo-model-open.bj.bcebos.com/
    # Qwen3-VL-30B-A3B-Thinking/ -> /auwomo-model/multimodal/Qwen3-VL-...
    p = plan_transfer(T("Qwen3-VL-30B-A3B-Thinking", "multimodal", type="model"))
    assert (p.bucket, p.prefix) == (MODEL_BUCKET, "Qwen3-VL-30B-A3B-Thinking/")
    assert p.target == f"{MODEL_ROOT}/multimodal/Qwen3-VL-30B-A3B-Thinking"


def test_missing_category_does_not_emit_an_empty_or_none_segment():
    for entry in (T("Loose"), T("Loose", ""), T("Loose", None)):
        p = plan_transfer(entry)
        assert p.target == f"{DATASET_ROOT}/Loose"
    p = plan_transfer(T("LooseModel", "", type="model"))
    assert p.target == f"{MODEL_ROOT}/LooseModel"


def test_source_is_bucket_and_prefix_so_same_shaped_keys_stay_distinct():
    # A category-less dataset and a model both have the key shape `{name}/`.
    dataset = plan_transfer(T("X"))
    model = plan_transfer(T("X", type="model"))
    assert dataset.prefix == model.prefix
    assert dataset.source != model.source


def test_explicit_src_overrides_the_derived_prefix_but_not_the_target():
    # DL3DV is a first-generation prefix: BOS holds it under datasets/ while
    # its category is multimodal. Both facts are correct; only the source is
    # unusual.
    p = plan_from_mapping({
        "name": "DL3DV-ALL-4K",
        "src": "datasets/DL3DV-ALL-4K/",
        "category": "multimodal",
    })
    assert (p.bucket, p.prefix) == (DATA_BUCKET, "datasets/DL3DV-ALL-4K/")
    assert p.target == f"{DATASET_ROOT}/multimodal/DL3DV-ALL-4K"


def test_mapping_without_src_derives_it():
    p = plan_from_mapping({"name": "molmobot-data", "category": "other"})
    assert p.prefix == "other/molmobot-data/"


def test_mapping_without_type_is_a_dataset():
    assert plan_from_mapping({"name": "n", "category": "other"}).bucket == DATA_BUCKET


def test_mapping_accepts_a_sqlite_row_shape():
    class Row(dict):
        """sqlite3.Row-like: no .get is the point of _AttrView, but a real Row
        does support mapping access, so both paths must work."""

    p = plan_from_mapping(Row(name="m", category="manipulation", type="model"))
    assert p.target == f"{MODEL_ROOT}/manipulation/m"


def test_prefix_always_ends_in_a_slash():
    # The remote endpoint string is a key prefix: a missing slash also matches
    # sibling prefixes (manipulation/ holds RDT-1B/, RDT-1B-repair/ and
    # RDT-1B_extracted/).
    for entry in (T("a", "b"), T("a"), T("a", "b", type="model")):
        assert plan_transfer(entry).prefix.endswith("/")


# ── in-flight detection ───────────────────────────────────────────────────


def _task(task_id, status, source, target, **extra):
    return {"task_id": task_id, "status": status, "source": source,
            "target": target, **extra}


SRC = inflight.endpoint_source(DATA_BUCKET, "other/molmobot-data/")
DST = f"{DATASET_ROOT}/other/molmobot-data"


def test_endpoint_source_matches_what_the_client_posts():
    assert SRC == "auwomo-data.bj.bcebos.com/other/molmobot-data/"


def test_unknown_status_counts_as_running():
    # Only 成功 / 失败 have ever been observed. Reading an unknown status as
    # finished is the dangerous direction: it posts a second importer.
    assert inflight.classify("进行中") == "running"
    assert inflight.classify("") == "running"
    assert inflight.classify(None) == "running"
    assert inflight.classify("成功") == "ok"
    assert inflight.classify("失败") == "failed"


def test_finds_a_running_task_for_the_same_source_and_target():
    tasks = [_task("t1", "进行中", SRC, DST)]
    assert inflight.find_running(tasks, SRC, DST)["task_id"] == "t1"


def test_ignores_terminal_tasks_for_the_same_source_and_target():
    tasks = [_task("t1", "成功", SRC, DST), _task("t2", "失败", SRC, DST)]
    assert inflight.find_running(tasks, SRC, DST) is None


def test_trailing_slash_and_scheme_do_not_defeat_the_match():
    tasks = [_task("t1", "进行中", "https://" + SRC.rstrip("/"), DST + "/")]
    assert inflight.find_running(tasks, SRC, DST)["task_id"] == "t1"


def test_a_different_prefix_is_not_a_match():
    other = inflight.endpoint_source(DATA_BUCKET, "other/molmobot-data-v2/")
    assert inflight.find_running([_task("t1", "进行中", other, DST)], SRC, DST) is None


def test_case_matters_because_bos_keys_are_case_sensitive():
    # multimodal/WebVid-10M/ and multimodal/webvid-10M/ are two real prefixes
    # holding different data.
    a = inflight.endpoint_source(DATA_BUCKET, "multimodal/WebVid-10M/")
    b = inflight.endpoint_source(DATA_BUCKET, "multimodal/webvid-10M/")
    assert inflight.find_running([_task("t1", "进行中", b, DST)], a, DST) is None


def test_a_known_task_id_wins_over_a_path_match():
    tasks = [_task("other", "进行中", SRC, DST), _task("mine", "进行中", SRC, DST)]
    assert inflight.find_running(tasks, SRC, DST, task_id="mine")["task_id"] == "mine"


def test_a_known_task_id_that_finished_is_not_reused():
    tasks = [_task("mine", "成功", SRC, DST)]
    assert inflight.find_running(tasks, SRC, DST, task_id="mine") is None


def test_fetch_tasks_stops_at_a_short_page():
    calls = []

    class C:
        def list_async_tasks(self, page, page_size):
            calls.append(page)
            return [_task(f"t{page}", "成功", SRC, DST)] if page == 1 else []

    assert len(inflight.fetch_tasks(C(), page_size=100)) == 1
    assert calls == [1]


# ── the manual script's failure isolation ─────────────────────────────────


@pytest.fixture
def script(monkeypatch):
    """scripts/ is not a package, so load the file directly. Poll timings are
    shrunk so a timeout test finishes in milliseconds."""
    path = REPO_ROOT / "scripts" / "transfer_import.py"
    spec = importlib.util.spec_from_file_location("dlm_transfer_import", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "POLL_S", 0)
    monkeypatch.setattr(module, "PROGRESS_EVERY_S", 10_000)
    monkeypatch.setattr(module, "ITEM_TIMEOUT_S", 0.05)
    return module


class FakeBos:
    """list_objects for one prefix per bucket. Only ever read."""

    def __init__(self, size=100, count=2):
        self.size, self.count = size, count

    def list_objects(self, bucket, prefix="", delimiter=None, marker="", max_keys=1000):
        class Obj:
            def __init__(self, key, size):
                self.key, self.size = key, size

        class Resp:
            is_truncated = False
            next_marker = ""

        resp = Resp()
        if delimiter:
            resp.common_prefixes = [type("P", (), {"prefix": prefix + "part/"})()]
            resp.contents = []
        else:
            resp.common_prefixes = []
            per = self.size // self.count
            resp.contents = [Obj(f"{prefix}part/f{i}", per) for i in range(self.count)]
        return resp


class FakeDCloud:
    """Records every import posted; serves whatever async-task list it is told
    to. `import_fails` makes the import CALL raise."""

    def __init__(self, statuses=None, import_fails=0, jfs_bytes=100, children=("part",)):
        self.posted = []
        self.statuses = statuses or {}          # task_id -> status
        self.import_fails = import_fails
        self.jfs_bytes = jfs_bytes
        self.children = list(children)
        self.extra_tasks = []
        self._n = 0

    # -- import side
    def create_folder(self, path, name):
        return {}

    def import_from_bos(self, bos_ak, bos_sk, bos_bucket, bos_path, target_path,
                        bos_endpoint="bj.bcebos.com"):
        if self.import_fails > 0:
            self.import_fails -= 1
            raise RuntimeError("Import failed: quota")
        self._n += 1
        task_id = f"posted-{self._n}"
        self.posted.append({"task_id": task_id, "bucket": bos_bucket,
                            "path": bos_path, "target": target_path})
        self.statuses.setdefault(task_id, "成功")
        return task_id

    def list_async_tasks(self, page=1, page_size=50):
        if page > 1:
            return []
        out = [{**t, "status": self.statuses.get(t["task_id"], t["status"])}
               for t in self.extra_tasks]
        for t in self.posted:
            out.append({"task_id": t["task_id"],
                        "status": self.statuses.get(t["task_id"], "成功"),
                        "source": inflight.endpoint_source(t["bucket"], t["path"]),
                        "target": t["target"]})
        return out

    # -- filesystem side
    def list_files(self, path="", page=1, page_size=50):
        if page > 1:
            return {"data": {"files": []}}
        name = path.rsplit("/", 1)[-1]
        if path.endswith(name) and name in ("molmobot-data", "A", "B", "C"):
            return {"data": {"files": [{"name": c, "size": self.jfs_bytes}
                                       for c in self.children]}}
        # listing a parent directory: report the target folder's size
        return {"data": {"files": [{"name": n, "size": self.jfs_bytes}
                                   for n in ("A", "B", "C", "molmobot-data")]}}


def _item(name, category="other", **extra):
    p = plan_from_mapping({"name": name, "category": category, **extra})
    return {"name": name, "category": category, "src": p.prefix,
            "bucket": p.bucket, "parent": p.parent, "target": p.target,
            "bos_bytes": 100, "bos_objects": 2, **extra}


def _run(script, monkeypatch, dcloud, plan, state=None):
    monkeypatch.setattr(script, "save_state", lambda s: None)
    state = {} if state is None else state
    failed = script.execute_plan(FakeBos(), dcloud, {"BAIDU_AK": "a", "BAIDU_SK": "s"},
                                 plan, state)
    return failed, state


def test_a_poll_timeout_does_not_cancel_the_following_items(script, monkeypatch):
    # The 2026-08-04 regression: item A timed out and items B, C never ran.
    dcloud = FakeDCloud(statuses={"posted-1": "进行中"})
    failed, state = _run(script, monkeypatch, dcloud,
                         [_item("A"), _item("B"), _item("C")])
    assert failed == ["A"]
    assert [p["target"].rsplit("/", 1)[-1] for p in dcloud.posted] == ["A", "B", "C"]
    assert state["A"]["status"] == "timeout_polling"
    assert state["B"]["status"] == "verified"
    assert state["C"]["status"] == "verified"


def test_a_failed_item_makes_the_run_exit_non_zero(script):
    with pytest.raises(SystemExit) as e:
        script.finish([], ["A"])
    assert e.value.code == 1


def test_a_clean_run_exits_zero(script):
    assert script.finish([], []) is None


def test_two_consecutive_import_call_failures_stop_the_round(script, monkeypatch):
    dcloud = FakeDCloud(import_fails=2)
    failed, state = _run(script, monkeypatch, dcloud,
                         [_item("A"), _item("B"), _item("C")])
    assert dcloud.posted == []
    # A and B were refused; C never got a chance and must not read as untouched.
    assert failed == ["A", "B", "C"]
    assert state["A"]["status"] == "failed"
    assert "C" not in state


def test_one_import_call_failure_does_not_stop_the_round(script, monkeypatch):
    dcloud = FakeDCloud(import_fails=1)
    failed, state = _run(script, monkeypatch, dcloud, [_item("A"), _item("B")])
    assert failed == ["A"]
    assert state["B"]["status"] == "verified"


def test_an_inflight_remote_task_is_reused_instead_of_posting_a_second_import(
        script, monkeypatch):
    item = _item("molmobot-data")
    dcloud = FakeDCloud()
    dcloud.extra_tasks = [{
        "task_id": "theirs-1", "status": "进行中",
        "source": inflight.endpoint_source(item["bucket"], item["src"]),
        "target": item["target"],
    }]
    # It finishes on the next poll, so the item still verifies.
    dcloud.statuses["theirs-1"] = "进行中"

    original = script.poll_until_done

    def poll(dc, task_id, it):
        dc.statuses[task_id] = "成功"
        return original(dc, task_id, it)

    monkeypatch.setattr(script, "poll_until_done", poll)
    failed, state = _run(script, monkeypatch, dcloud, [item])
    assert dcloud.posted == [], "posted a second import over a live one"
    assert failed == []
    assert state["molmobot-data"]["dcloud_task_id"] == "theirs-1"


def test_the_far_side_refusing_to_answer_skips_the_item_instead_of_posting_blind(
        script, monkeypatch):
    """The one path left that could put two importers on one directory.

    The re-attach check above is what prevents that, and it needs the remote
    task list to work. When the list call fails we cannot know whether an
    import is already running here, so posting anyway would defeat the check
    entirely — on a directory that may be mid-write. Skip, and let the next
    run re-check.
    """
    dcloud = FakeDCloud()

    def boom(page=1, page_size=50):
        raise RuntimeError("502 Bad Gateway")

    dcloud.list_async_tasks = boom
    failed, state = _run(script, monkeypatch, dcloud,
                         [_item("A"), _item("B")])

    assert dcloud.posted == [], "posted blind with the re-attach check broken"
    assert failed == ["A", "B"], "a skipped item must make the run exit non-zero"
    for name in ("A", "B"):
        assert state[name]["status"] == "skipped"
        assert "could not list remote async tasks" in state[name]["error"]


def test_a_recorded_task_id_still_running_is_reused(script, monkeypatch):
    item = _item("molmobot-data")
    dcloud = FakeDCloud()
    dcloud.extra_tasks = [{
        "task_id": "mine-1", "status": "进行中",
        "source": inflight.endpoint_source(item["bucket"], item["src"]),
        "target": item["target"],
    }]
    state = {"molmobot-data": {"status": "timeout_polling",
                               "dcloud_task_id": "mine-1"}}
    original = script.poll_until_done

    def poll(dc, task_id, it):
        dc.statuses[task_id] = "成功"
        return original(dc, task_id, it)

    monkeypatch.setattr(script, "poll_until_done", poll)
    failed, state = _run(script, monkeypatch, dcloud, [item], state)
    assert dcloud.posted == []
    assert failed == []


def test_a_finished_remote_task_does_not_block_a_fresh_import(script, monkeypatch):
    item = _item("molmobot-data")
    dcloud = FakeDCloud()
    dcloud.extra_tasks = [{
        "task_id": "old-1", "status": "失败",
        "source": inflight.endpoint_source(item["bucket"], item["src"]),
        "target": item["target"],
    }]
    failed, _ = _run(script, monkeypatch, dcloud, [item])
    assert [p["task_id"] for p in dcloud.posted] == ["posted-1"]
    assert failed == []


def test_a_model_item_imports_from_the_model_bucket(script, monkeypatch):
    item = _item("molmobot-data", "other", type="model")
    dcloud = FakeDCloud()
    _run(script, monkeypatch, dcloud, [item])
    assert dcloud.posted[0]["bucket"] == MODEL_BUCKET
    assert dcloud.posted[0]["path"] == "molmobot-data/"
    assert dcloud.posted[0]["target"] == f"{MODEL_ROOT}/other/molmobot-data"


def test_a_short_target_fails_only_its_own_item(script, monkeypatch):
    dcloud = FakeDCloud(jfs_bytes=1)   # < bos_bytes=100
    failed, state = _run(script, monkeypatch, dcloud, [_item("A"), _item("B")])
    assert failed == ["A", "B"]        # both short — but both were attempted
    assert len(dcloud.posted) == 2
    assert "size check failed" in state["A"]["error"]


# ── manifest validation ───────────────────────────────────────────────────


def test_manifest_type_typo_is_refused(script, tmp_path, monkeypatch):
    bad = tmp_path / "m.json"
    bad.write_text(json.dumps([{"name": "n", "category": "other", "type": "models"}]))
    monkeypatch.setattr("sys.argv", ["transfer_import.py", "--manifest", str(bad)])
    with pytest.raises(AssertionError):
        script.main()


def test_manifest_may_omit_src(script, tmp_path, monkeypatch):
    """No `src` is legal now — it is derived. The run gets as far as needing
    credentials, which is past validation."""
    ok = tmp_path / "m.json"
    ok.write_text(json.dumps([{"name": "n", "category": "other"}]))
    monkeypatch.setattr("sys.argv", ["transfer_import.py", "--manifest", str(ok)])
    monkeypatch.delenv("DCLOUD_USER", raising=False)
    monkeypatch.delenv("DCLOUD_PASS", raising=False)
    monkeypatch.setattr(script, "load_config", lambda: {"BAIDU_AK": "a", "BAIDU_SK": "s"})
    with pytest.raises(SystemExit) as e:
        script.main()
    assert "DCLOUD_USER" in str(e.value)


def test_the_builtin_manifest_still_maps_the_way_it_did(script):
    """Every built-in entry keeps its first-generation source prefix and lands
    under raw-data/{category}/{name}."""
    for entry in script.MANIFEST:
        p = plan_from_mapping(entry)
        assert p.prefix == entry["src"]
        assert p.target == f"{DATASET_ROOT}/{entry['category']}/{entry['name']}"
