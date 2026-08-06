#!/usr/bin/env python3
"""One-shot: batch-import verified-complete datasets from BOS to the
D-Robotics (地瓜云) JuiceFS. 2026-08-03, user-approved, Opus-reviewed.

Safety contract (the whole point of this script):
  - BOS is READ-ONLY here: the only BOS calls are list_objects. The import
    itself is a server-side copy performed by D-Robotics' infrastructure
    (they read the bucket with the AK/SK we pass); nothing in this process
    can write to or delete from BOS.
  - JuiceFS side only ever gains data at the listed target paths. No
    delete/move API is called anywhere.
  - Serial (concurrency 1), stop-on-failure: the first dataset that fails
    halts the run with its reason in the state file; nothing cascades.
  - Idempotent: a dataset whose JuiceFS folder already covers the BOS bytes
    is skipped, so the script can be re-run after any interruption.
  - Source prefixes MUST end with "/" — review evidence: all 672 prior
    imports in the D-Robotics async history use trailing-slash sources, and
    a slash-less prefix would also match sibling prefixes (manipulation/
    holds RDT-1B/, RDT-1B-repair/ AND RDT-1B_extracted/ — a slash-less
    "RDT-1B" import would silently drag in ~1 TiB of neighbors).
  - Post-import SCOPE check: the target's top-level child names must be a
    subset of the source prefix's top-level children. Catches both prefix
    bleed (extra children) and pathological nesting; the size check alone
    passes happily on oversize and would miss both.

The transfer list is PREFIX-driven from the 2026-08-03 reconciliation
(scripts/reconcile_transfers.py v2), not task-table-driven —
AgiBotWorld-Alpha has no `done` task row and task bos_path fields are
unreliable (see that script's docstring).

Run on S1, detached via systemd (survives SSH disconnects). NOTE:
-p WorkingDirectory= is load-bearing for CREDENTIALS, not just imports:
a transient unit does not inherit the shell env; DCLOUD_USER/PASS reach
the process only because load_config() runs load_dotenv() upward from cwd.
  dry-run:  python3 scripts/transfer_import.py
  execute:  systemd-run --unit=dlm-transfer-oneshot --collect \
              -p WorkingDirectory=/root/code/bos-download-manager \
              -p StandardOutput=append:/root/transfer-20260803.log \
              -p StandardError=append:/root/transfer-20260803.log \
              /usr/bin/python3 -u /root/code/bos-download-manager/scripts/transfer_import.py --execute
State:  /root/transfer_state-20260803.json (per-dataset, atomically updated)
Re-run after fixing one dataset: add --only NAME to touch nothing else.

Expected wall-clock (from observed server-side throughput ~470-540 MB/s,
object-bound sets ~30 obj/s): Alpha ~6h, Beta-BJ ~28h, DL3DV ~26h,
InternData ~4h, rest ~3h — roughly 65-70h serial for the full manifest.
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dlm.core.bos import create_bos_client  # noqa: E402
from dlm.core.config import load_config  # noqa: E402
from dlm.constants import DATA_BUCKET  # noqa: E402
from dlm.transfer.dcloud import DCloudClient  # noqa: E402

ROOT = "/727a2f92-30c"
RAW = f"{ROOT}/auwomo-datasets/raw-data"
STATE_PATH = "/root/transfer_state-20260803.json"

POLL_S = 60
ITEM_TIMEOUT_S = 72 * 3600
PROGRESS_EVERY_S = 30 * 60        # probe target size as a progress signal
CONSECUTIVE_ERR_LIMIT = 5         # poll errors in a row (after re-login) -> item fails
SKIP_STATUSES = ("verified", "verified_bos_drift", "verified_scope_warn")

# Order matters: Alpha first (small, freshly file-level-verified — proves the
# pipe), then the Beta-BJ heavyweight, then the rest by value/size.
# Every entry was NOT_TRANSFERRED in the v2 reconciliation with a clean
# (absent) target. PARTIAL datasets are deliberately excluded: the import
# API's merge semantics onto an existing folder are unverified.
MANIFEST = [
    {"name": "AgiBotWorld-Alpha",
     "src": "manipulation/AgiBotWorld-Alpha/", "category": "manipulation"},
    {"name": "AgiBotWorld-Beta-BJ",
     "src": "manipulation/AgiBotWorld-Beta-BJ/", "category": "manipulation"},
    {"name": "InternData-A1",
     "src": "other/InternData-A1/", "category": "other"},
    {"name": "DL3DV-ALL-4K",
     "src": "datasets/DL3DV-ALL-4K/", "category": "multimodal"},
    {"name": "PhysicalAI-Robotics-Open-H-Embodiment",
     "src": "other/PhysicalAI-Robotics-Open-H-Embodiment/", "category": "other"},
    {"name": "MolmoAct-Dataset",
     "src": "other/MolmoAct-Dataset/", "category": "other"},
    {"name": "RDT-1B",
     "src": "manipulation/RDT-1B/", "category": "manipulation"},
    {"name": "PhysicalAI-Robotics-Locomanipulation-GRAIL",
     "src": "whole-body/PhysicalAI-Robotics-Locomanipulation-GRAIL/",
     "category": "whole-body"},
]


def log(msg):
    print(f"{datetime.now(timezone.utc).isoformat(timespec='seconds')} {msg}", flush=True)


def load_state():
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH) as f:
            return json.load(f)
    return {}


def save_state(state):
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, ensure_ascii=False, indent=1)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, STATE_PATH)


def bos_stats(bos, prefix):
    total, count, marker = 0, 0, ""
    while True:
        resp = bos.list_objects(DATA_BUCKET, prefix=prefix, marker=marker, max_keys=1000)
        for obj in getattr(resp, "contents", None) or []:
            total += obj.size
            count += 1
        if not getattr(resp, "is_truncated", False):
            break
        marker = resp.next_marker
    return total, count


def bos_top_children(bos, prefix):
    """Top-level child names (dirs without trailing /, plus direct files)."""
    names, marker = set(), ""
    while True:
        resp = bos.list_objects(DATA_BUCKET, prefix=prefix, delimiter="/",
                                marker=marker, max_keys=1000)
        for p in getattr(resp, "common_prefixes", None) or []:
            names.add(p.prefix[len(prefix):].rstrip("/"))
        for obj in getattr(resp, "contents", None) or []:
            rel = obj.key[len(prefix):]
            if rel:
                names.add(rel)
        if not getattr(resp, "is_truncated", False):
            break
        marker = resp.next_marker
    return names


def jfs_folder_size(dcloud, parent, name, retries=3):
    """Recursive size of parent/name per the files API, None if absent."""
    for attempt in range(retries):
        try:
            page = 1
            while True:
                resp = dcloud.list_files(path=parent, page=page, page_size=50)
                files = (resp.get("data") or {}).get("files") or []
                for f in files:
                    if f["name"] == name:
                        return int(f.get("size") or 0)
                if len(files) < 50:
                    return None
                page += 1
        except Exception as e:
            if attempt == retries - 1:
                raise
            log(f"WARN jfs list {parent} attempt {attempt + 1} failed: {e}")
            time.sleep(30)


def jfs_children(dcloud, path):
    names, page = set(), 1
    while True:
        resp = dcloud.list_files(path=path, page=page, page_size=50)
        files = (resp.get("data") or {}).get("files") or []
        names |= {f["name"] for f in files}
        if len(files) < 50:
            return names
        page += 1


def find_async_task(dcloud, task_id, max_pages=5):
    for page in range(1, max_pages + 1):
        tasks = dcloud.list_async_tasks(page=page, page_size=100)
        if not tasks:
            return None
        for t in tasks:
            if t.get("task_id") == task_id:
                return t
    return None


def poll_until_done(dcloud, task_id, item):
    """Poll to terminal state. Re-logins on errors; probes the target's
    size every PROGRESS_EVERY_S both as a progress log and as a fallback
    completion signal (review finding: the async list carries no progress
    field, and a task that ages off the list must not poll to the 72h cap
    when the target demonstrably holds all the bytes)."""
    name, parent, bos_bytes = item["name"], item["parent"], item["bos_bytes"]
    started = time.time()
    consecutive_errors = 0
    last_probe = 0.0
    while time.time() - started < ITEM_TIMEOUT_S:
        try:
            t = find_async_task(dcloud, task_id)
            consecutive_errors = 0
            status = str(t.get("status", "")) if t else "(absent from list)"
            if t and status in ("成功", "success", "done"):
                return True, status
            if t and status in ("失败", "failed", "error"):
                return False, f"{status}: {t.get('error_msg', '')}"
            if time.time() - last_probe >= PROGRESS_EVERY_S:
                last_probe = time.time()
                try:
                    size = jfs_folder_size(dcloud, parent, name) or 0
                except Exception as e:
                    size = -1
                    log(f"WARN [{name}] progress probe failed: {e}")
                elapsed = int(time.time() - started)
                log(f"  [{name}] status={status} elapsed={elapsed // 60}min "
                    f"target={size:,} / {bos_bytes:,} B "
                    f"({100 * max(size, 0) / bos_bytes:.1f}%)")
                if t is None and size >= bos_bytes:
                    return True, "inferred: absent from async list but target complete"
        except Exception as e:
            consecutive_errors += 1
            log(f"WARN [{name}] poll error {consecutive_errors}/"
                f"{CONSECUTIVE_ERR_LIMIT}: {e} — re-login and retry")
            if consecutive_errors >= CONSECUTIVE_ERR_LIMIT:
                return False, f"poll failed {consecutive_errors} times: {e}"
            try:
                dcloud.login()
            except Exception as le:
                log(f"WARN [{name}] re-login failed: {le}")
            time.sleep(min(60 * consecutive_errors, 300))
        time.sleep(POLL_S)
    return False, f"timeout_polling after {ITEM_TIMEOUT_S}s"


def main():
    global STATE_PATH
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true",
                        help="actually import (default: dry-run)")
    parser.add_argument("--only", default="",
                        help="run only the named dataset (re-run escape hatch)")
    parser.add_argument("--manifest", default="",
                        help="JSON file with the transfer list (overrides built-in)")
    parser.add_argument("--allow-topup", action="store_true",
                        help="import onto an existing smaller target instead of "
                             "aborting. ONLY safe because the 2026-08-03 rh20t "
                             "experiment proved overwrite/skip-same semantics "
                             "(partial 268MB -> exactly BOS size, no duplication)")
    parser.add_argument("--state", default=STATE_PATH,
                        help="state/resume file. A second concurrent lane MUST "
                             "pass its own: save_state rewrites the whole JSON, "
                             "so two processes sharing one file lose each "
                             "other's records (the default file belongs to the "
                             "round started 2026-08-03)")
    args = parser.parse_args()

    STATE_PATH = args.state

    manifest = MANIFEST
    if args.manifest:
        with open(args.manifest) as f:
            manifest = json.load(f)
        for m in manifest:
            assert m.get("name") and m.get("src") and m.get("category"), m
    if args.only:
        manifest = [m for m in MANIFEST if m["name"] == args.only]
        if not manifest:
            sys.exit(f"--only {args.only!r} matches nothing in the manifest")

    cfg = load_config()
    user, pw = os.environ.get("DCLOUD_USER"), os.environ.get("DCLOUD_PASS")
    if not user or not pw:
        sys.exit("DCLOUD_USER/DCLOUD_PASS not set")
    bos = create_bos_client(cfg["BAIDU_AK"], cfg["BAIDU_SK"],
                            cfg.get("BOS_ENDPOINT") or "https://bj.bcebos.com")
    dcloud = DCloudClient(user, pw)
    dcloud.login()
    log(f"logged in; mode={'EXECUTE' if args.execute else 'dry-run'}"
        + (f"; only={args.only}" if args.only else ""))

    state = load_state()

    # ---- Pre-flight every item first: fail fast before moving any bytes.
    plan = []
    for item in manifest:
        name, src, category = item["name"], item["src"], item["category"]
        assert src.endswith("/"), f"source prefix must end with '/': {src}"
        st = state.get(name, {})
        if st.get("status") in SKIP_STATUSES:
            log(f"  skip (already {st['status']}): {name}")
            continue
        bos_bytes, bos_objects = bos_stats(bos, src)
        if bos_bytes == 0:
            sys.exit(f"ABORT pre-flight: BOS source {src} is empty — "
                     f"world changed since reconciliation")
        parent = f"{RAW}/{category}"
        jfs = jfs_folder_size(dcloud, parent, name)
        if jfs and jfs >= bos_bytes and not item.get("force"):
            log(f"  skip (target already complete): {name} jfs={jfs:,} >= bos={bos_bytes:,}")
            state[name] = {"status": "verified", "jfs_bytes": jfs,
                           "bos_bytes": bos_bytes, "note": "pre-existing"}
            continue
        if jfs and not args.allow_topup and not item.get("force"):
            sys.exit(f"ABORT pre-flight: {parent}/{name} exists with {jfs:,} B "
                     f"< {bos_bytes:,} B — partial target; re-run with "
                     f"--allow-topup (overwrite/skip-same semantics proven "
                     f"2026-08-03) or resolve by hand")
        if jfs:
            log(f"  top-up: {name} target has {jfs:,} B < bos {bos_bytes:,} B")
        plan.append({**item, "bos_bytes": bos_bytes, "bos_objects": bos_objects,
                     "parent": parent, "target": f"{parent}/{name}"})
        log(f"  plan: {src} ({bos_bytes:,} B / {bos_objects} obj) -> {parent}/{name}")

    if args.execute:
        save_state(state)
    if not plan:
        log("nothing to do")
        return
    if not args.execute:
        log(f"dry-run only — {len(plan)} imports pending. Re-run with --execute.")
        return

    # ---- Serial execution, stop on first failure.
    for item in plan:
        name, src = item["name"], item["src"]
        log(f"=== {name}: import starting ({item['bos_bytes']:,} B)")
        state[name] = {"status": "importing", "bos_bytes": item["bos_bytes"],
                       "bos_objects": item["bos_objects"],
                       "started_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
        save_state(state)
        try:
            try:
                dcloud.create_folder(RAW + "/", item["category"])
            except Exception:
                pass  # folder may already exist; import creates paths anyway
            # src keeps its trailing slash: the D-Robotics endpoint string is
            # a key prefix, and every one of the 672 prior imports ends in
            # "/" — see the safety contract above.
            task_id = dcloud.import_from_bos(
                bos_ak=cfg["BAIDU_AK"], bos_sk=cfg["BAIDU_SK"],
                bos_bucket=DATA_BUCKET, bos_path=src,
                target_path=item["target"],
            )
        except Exception as e:
            state[name].update(status="failed", error=f"import call failed: {e}")
            save_state(state)
            sys.exit(f"STOP: {name} import call failed: {e}")
        state[name]["dcloud_task_id"] = task_id
        save_state(state)
        log(f"  [{name}] dcloud task {task_id}, polling every {POLL_S}s")

        ok, detail = poll_until_done(dcloud, task_id, item)
        if not ok:
            status = "timeout_polling" if detail.startswith("timeout_polling") else "failed"
            state[name].update(status=status, error=detail)
            save_state(state)
            sys.exit(f"STOP: {name} import {status}: {detail}")

        # ---- Verify 1: size — target folder covers the bytes.
        jfs = jfs_folder_size(dcloud, item["parent"], name)
        # ---- Verify 2: scope — target children ⊆ source children. Catches
        # prefix bleed and nesting, which the size check cannot see.
        try:
            # A top-up target may legitimately hold children from an earlier
            # import of a sibling copy of the same dataset — manifest items
            # can list every legitimate source under "scope_srcs".
            src_children = set()
            for sp in item.get("scope_srcs") or [src]:
                src_children |= bos_top_children(bos, sp)
            dst_children = jfs_children(dcloud, item["target"])
        except Exception as e:
            src_children, dst_children = None, None
            log(f"WARN [{name}] scope check listing failed: {e}")
        scope_ok = None
        if src_children is not None:
            extra = dst_children - src_children
            scope_ok = not extra
            if extra:
                log(f"ERROR [{name}] target has children not in source: "
                    f"{sorted(extra)[:10]}")
        # ---- Verify 3: BOS untouched.
        bos_bytes2, bos_objects2 = bos_stats(bos, src)
        readonly_ok = (bos_bytes2 == item["bos_bytes"]
                       and bos_objects2 == item["bos_objects"])
        state[name].update(
            jfs_bytes=jfs or 0,
            bos_bytes_after=bos_bytes2, bos_objects_after=bos_objects2,
            scope_ok=scope_ok,
            finished_at=datetime.now(timezone.utc).isoformat(timespec="seconds"))
        if not readonly_ok:
            # Not something this script could cause (it never writes BOS) —
            # but new uploads/other actors could land mid-import. Surface it.
            log(f"WARN [{name}] BOS prefix changed during import: "
                f"{item['bos_bytes']:,}/{item['bos_objects']} -> "
                f"{bos_bytes2:,}/{bos_objects2} — flagging for review")
        if jfs is None or jfs < item["bos_bytes"]:
            state[name].update(status="failed",
                               error=f"size check failed: jfs={jfs} < bos={item['bos_bytes']}")
            save_state(state)
            sys.exit(f"STOP: {name} completed but size check failed "
                     f"(jfs={jfs}, bos={item['bos_bytes']:,})")
        if scope_ok is False:
            state[name].update(status="failed",
                               error="scope check failed: target holds children "
                                     "not present under the source prefix")
            save_state(state)
            sys.exit(f"STOP: {name} completed but SCOPE check failed — "
                     f"see log for the extra children")
        if scope_ok is None:
            state[name]["status"] = "verified_scope_warn"
        elif not readonly_ok:
            state[name]["status"] = "verified_bos_drift"
        else:
            state[name]["status"] = "verified"
        save_state(state)
        log(f"=== {name}: {state[name]['status'].upper()} "
            f"jfs={jfs:,} >= bos={item['bos_bytes']:,}, scope_ok={scope_ok}")

    log("ALL DONE — every dataset imported and verified")


if __name__ == "__main__":
    main()
