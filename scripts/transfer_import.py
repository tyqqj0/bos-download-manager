#!/usr/bin/env python3
"""Batch-import verified-complete datasets and models from BOS to the
D-Robotics (地瓜云) JuiceFS. 2026-08-03, user-approved, Opus-reviewed;
hardened 2026-08-10 (per-item failures, in-flight reuse, model bucket).

Safety contract (the whole point of this script):
  - BOS is READ-ONLY here: the only BOS calls are list_objects. The import
    itself is a server-side copy performed by D-Robotics' infrastructure
    (they read the bucket with the AK/SK we pass); nothing in this process
    can write to or delete from BOS.
  - JuiceFS side only ever gains data at the listed target paths. No
    delete/move API is called anywhere.
  - Serial (concurrency 1). One item's failure — a refused pre-flight, a poll
    timeout, a failed verification — is recorded in the state file and the run
    MOVES ON to the next item; the process exits non-zero at the end so a
    supervisor still sees it. Two CONSECUTIVE import-call failures do stop the
    round: that is the far side refusing us, and seven more attempts would only
    make noise. (Before 2026-08-10 any single failure sys.exit'd the whole
    round, so a 72h poll timeout on item 4 silently cancelled items 5-9.)
  - Never posts an import that is already in flight: the remote async task list
    carries (source, target), so an item whose import is still running there
    gets re-attached to that task_id and polled instead of re-posted. That is
    the hole the 2026-08-04 DL3DV run fell through — our side gave up at the
    72h cap on 08-07 while the remote task ran on until 08-09 19:26.
  - Idempotent: a dataset whose JuiceFS folder already covers the BOS bytes
    is skipped, so the script can be re-run after any interruption.
  - Bucket and destination are NOT hardcoded: both come from
    `dlm.transfer.targets`, the same module the automatic dispatcher uses. A
    manifest item with `"type": "model"` therefore reads `auwomo-model-open`
    and lands under `/auwomo-model/{category}/{name}`.
  - A manifest item may also carry `"bucket"` and `"root"` to address a bucket
    or destination root that is neither of the two built-in pairs (see
    `docs/runbooks/transfer-processed-bucket.md`). Those two keys are readable
    only from a manifest — a `sqlite3.Row` has no `.get`, so the automatic
    dispatcher cannot be redirected by them.
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
import logging
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dlm.core.bos import create_bos_client  # noqa: E402
from dlm.core.config import load_config  # noqa: E402
from dlm.transfer import inflight  # noqa: E402
from dlm.transfer.dcloud import DCloudClient  # noqa: E402
# The four measurement primitives used to live in this file. They moved to
# dlm/transfer/measure.py when the automatic dispatcher needed the same numbers:
# two definitions of "how many bytes are under this prefix" is how this script
# came to disagree with the rest of the codebase about which bucket a model
# lives in.
from dlm.transfer.measure import (  # noqa: E402
    bos_stats, bos_top_children, jfs_children, jfs_folder_size,
)
from dlm.transfer.targets import plan_from_mapping  # noqa: E402

# measure.py reports its retries through the logging module; without this they
# would vanish, and a systemd run's stderr goes to the same log file as log().
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")

STATE_PATH = "/root/transfer_state-20260803.json"

POLL_S = 60
ITEM_TIMEOUT_S = 72 * 3600
PROGRESS_EVERY_S = 30 * 60        # probe target size as a progress signal
CONSECUTIVE_ERR_LIMIT = 5         # poll errors in a row (after re-login) -> item fails
CONSECUTIVE_ITEM_FAIL_LIMIT = 2   # import CALLS refused in a row -> stop the round
SKIP_STATUSES = ("verified", "verified_bos_drift", "verified_scope_warn")

# Order matters: Alpha first (small, freshly file-level-verified — proves the
# pipe), then the Beta-BJ heavyweight, then the rest by value/size.
# Every entry was NOT_TRANSFERRED in the v2 reconciliation with a clean
# (absent) target. PARTIAL datasets are deliberately excluded: the import
# API's merge semantics onto an existing folder are unverified.
#
# `src` is optional — omitted, it is derived from (type, category, name) by
# `dlm.transfer.targets`. It is spelled out on every entry below because these
# are first-generation prefixes that predate the current naming rule and
# genuinely differ from the derived one (DL3DV lives under `datasets/` while
# its category is `multimodal`).
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


def find_async_task(dcloud, task_id, max_pages=5):
    return inflight.find_by_id(
        inflight.fetch_tasks(dcloud, max_pages=max_pages), task_id)


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
            verdict = inflight.classify(t.get("status")) if t else "running"
            if verdict == "ok":
                return True, status
            if verdict == "failed":
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


def select_manifest(builtin, custom, only):
    """The transfer list a run operates on.

    `--only` filters whatever manifest is in play. Filtering the built-in list
    instead meant `--manifest x.json --only NAME` imported the built-in entry's
    prefix (same name, different src) or exited "matches nothing" for a name
    only the custom file has.
    """
    manifest = builtin if custom is None else custom
    if not only:
        return manifest
    source = "--manifest file" if custom is not None else "built-in manifest"
    picked = [m for m in manifest if m["name"] == only]
    if not picked:
        raise ValueError(f"--only {only!r} matches nothing in the {source}")
    return picked


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

    custom = None
    if args.manifest:
        with open(args.manifest) as f:
            custom = json.load(f)
        for m in custom:
            # `src` stays optional: targets.plan_from_mapping derives it from
            # (type, category, name) when absent. `type` must be spelled
            # correctly if present — a typo would silently read the data bucket
            # for a model and find nothing there.
            assert m.get("name") and m.get("category"), m
            assert m.get("type", "dataset") in ("dataset", "model"), m
    try:
        manifest = select_manifest(MANIFEST, custom, args.only)
    except ValueError as e:
        sys.exit(str(e))

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

    # ---- Pre-flight every item first: refuse the bad ones before moving any
    # bytes. A refusal is per-item (recorded + counted), not a round abort.
    plan, refused = [], []
    for item in manifest:
        p = plan_from_mapping(item)
        name, src = p.name, p.prefix
        assert src.endswith("/"), f"source prefix must end with '/': {src}"
        st = state.get(name, {})
        if st.get("status") in SKIP_STATUSES:
            log(f"  skip (already {st['status']}): {name}")
            continue
        bos_bytes, bos_objects = bos_stats(bos, p.bucket, src)
        if bos_bytes == 0:
            log(f"  REFUSE: BOS source {p.bucket}/{src} is empty — world "
                f"changed since reconciliation")
            state[name] = {"status": "blocked", "bos_bytes": 0,
                           "error": f"BOS source {p.bucket}/{src} is empty"}
            refused.append(name)
            continue
        jfs = jfs_folder_size(dcloud, p.parent, name)
        if jfs and jfs >= bos_bytes and not item.get("force"):
            log(f"  skip (target already complete): {name} jfs={jfs:,} >= bos={bos_bytes:,}")
            state[name] = {"status": "verified", "jfs_bytes": jfs,
                           "bos_bytes": bos_bytes, "note": "pre-existing"}
            continue
        if jfs and not args.allow_topup and not item.get("force"):
            log(f"  REFUSE: {p.target} exists with {jfs:,} B < {bos_bytes:,} B "
                f"— partial target; re-run with --allow-topup (overwrite/"
                f"skip-same semantics proven 2026-08-03) or resolve by hand")
            state[name] = {"status": "blocked", "jfs_bytes": jfs,
                           "bos_bytes": bos_bytes,
                           "error": "partial target, --allow-topup not given"}
            refused.append(name)
            continue
        if jfs:
            log(f"  top-up: {name} target has {jfs:,} B < bos {bos_bytes:,} B")
        plan.append({**item, "name": name, "src": src, "bucket": p.bucket,
                     "bos_bytes": bos_bytes, "bos_objects": bos_objects,
                     "parent": p.parent, "target": p.target})
        log(f"  plan: {p.bucket}/{src} ({bos_bytes:,} B / {bos_objects} obj) "
            f"-> {p.target}")

    if args.execute:
        save_state(state)
    if not plan:
        log("nothing to do")
        return finish(refused, [])
    if not args.execute:
        log(f"dry-run only — {len(plan)} imports pending. Re-run with --execute.")
        return finish(refused, [])

    return finish(refused, execute_plan(bos, dcloud, cfg, plan, state))


def finish(refused, failed):
    """Exit non-zero if anything needs attention.

    A skipped item must not look like success to whatever supervises the run —
    that is the price of no longer aborting the round on the first failure.
    """
    bad = list(refused) + list(failed)
    if not bad:
        log("ALL DONE — every dataset imported and verified")
        return
    log(f"FINISHED WITH {len(bad)} PROBLEM ITEM(S): {', '.join(bad)} "
        f"— reasons in {STATE_PATH}")
    sys.exit(1)


def execute_plan(bos, dcloud, cfg, plan, state):
    """Import every planned item serially. Returns the failed items' names.

    One item's failure does NOT cancel its successors: on 2026-08-04 item 4 hit
    the 72h poll cap and `sys.exit` took items 5-9 down with it, unrun and
    unrecorded. The single exception is CONSECUTIVE_ITEM_FAIL_LIMIT import
    CALLS refused in a row — that is the far side rejecting us, and the
    remaining items would only add identical failures.
    """
    failed = []
    consecutive_call_failures = 0
    for index, item in enumerate(plan):
        name, src = item["name"], item["src"]
        log(f"=== {name}: import starting ({item['bos_bytes']:,} B)")
        prior_task_id = (state.get(name) or {}).get("dcloud_task_id")
        state[name] = {"status": "importing", "bos_bytes": item["bos_bytes"],
                       "bos_objects": item["bos_objects"],
                       "started_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
        save_state(state)

        # ---- Re-attach instead of re-posting. A remote import still running
        # against this (source, target) means a previous run of ours gave up
        # early — posting a second one would have two importers writing the
        # same directory.
        try:
            running = inflight.find_running(
                inflight.fetch_tasks(dcloud),
                source=inflight.endpoint_source(item["bucket"], src),
                target=item["target"],
                task_id=prior_task_id,
            )
        except Exception as e:
            # We could not find out whether an import is already running here.
            # Posting anyway is the one path left in the system that can put two
            # importers on one directory — which is exactly what the re-attach
            # check above exists to prevent, and what this file's header
            # promises not to do. Skip the item; the next run re-checks.
            log(f"SKIP [{name}] could not list remote async tasks ({e}) — "
                f"refusing to post blind (a second importer on one directory "
                f"is worse than a delay). Re-run when the far side answers.")
            failed.append(name)
            state[name].update(status="skipped",
                               error=f"could not list remote async tasks: {e}")
            save_state(state)
            continue
        if running is not None:
            task_id = running.get("task_id")
            consecutive_call_failures = 0
            log(f"  [{name}] re-attaching to in-flight remote task {task_id} "
                f"(status={running.get('status')!r}, "
                f"created={running.get('created_at')}) — no new import posted")
        else:
            try:
                try:
                    root, leaf = item["parent"].rsplit("/", 1)
                    dcloud.create_folder(root + "/", leaf)
                except Exception:
                    pass  # folder may already exist; import creates paths anyway
                # src keeps its trailing slash: the D-Robotics endpoint string
                # is a key prefix, and every one of the 672 prior imports ends
                # in "/" — see the safety contract above.
                task_id = dcloud.import_from_bos(
                    bos_ak=cfg["BAIDU_AK"], bos_sk=cfg["BAIDU_SK"],
                    bos_bucket=item["bucket"], bos_path=src,
                    target_path=item["target"],
                )
            except Exception as e:
                consecutive_call_failures += 1
                state[name].update(status="failed", error=f"import call failed: {e}")
                save_state(state)
                failed.append(name)
                log(f"FAIL [{name}] import call failed "
                    f"({consecutive_call_failures}/{CONSECUTIVE_ITEM_FAIL_LIMIT}): {e}")
                if consecutive_call_failures >= CONSECUTIVE_ITEM_FAIL_LIMIT:
                    skipped = [it["name"] for it in plan[index + 1:]]
                    log(f"STOP: {consecutive_call_failures} import calls refused "
                        f"in a row — the far side is not taking work. Skipping "
                        f"{len(skipped)} remaining item(s): {', '.join(skipped) or '(none)'}")
                    failed.extend(skipped)
                    break
                continue
            consecutive_call_failures = 0
        state[name]["dcloud_task_id"] = task_id
        save_state(state)
        log(f"  [{name}] dcloud task {task_id}, polling every {POLL_S}s")

        ok, detail = poll_until_done(dcloud, task_id, item)
        if not ok:
            status = "timeout_polling" if detail.startswith("timeout_polling") else "failed"
            state[name].update(status=status, error=detail)
            save_state(state)
            failed.append(name)
            log(f"FAIL [{name}] import {status}: {detail} — remote task "
                f"{task_id} is NOT cancelled and may still be running; a "
                f"re-run re-attaches to it. Continuing with the next item.")
            continue

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
                src_children |= bos_top_children(bos, item["bucket"], sp)
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
        bos_bytes2, bos_objects2 = bos_stats(bos, item["bucket"], src)
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
            failed.append(name)
            log(f"FAIL [{name}] import completed but size check failed "
                f"(jfs={jfs}, bos={item['bos_bytes']:,})")
            continue
        if scope_ok is False:
            state[name].update(status="failed",
                               error="scope check failed: target holds children "
                                     "not present under the source prefix")
            save_state(state)
            failed.append(name)
            log(f"FAIL [{name}] import completed but SCOPE check failed — "
                f"see the extra children above")
            continue
        if scope_ok is None:
            state[name]["status"] = "verified_scope_warn"
        elif not readonly_ok:
            state[name]["status"] = "verified_bos_drift"
        else:
            state[name]["status"] = "verified"
        save_state(state)
        log(f"=== {name}: {state[name]['status'].upper()} "
            f"jfs={jfs:,} >= bos={item['bos_bytes']:,}, scope_ok={scope_ok}")

    return failed


if __name__ == "__main__":
    main()
