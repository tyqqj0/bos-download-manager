#!/usr/bin/env python3
"""READ-ONLY reconciliation: which done datasets actually reached the
D-Robotics (地瓜云) JuiceFS, and completely?

Ground truths (nothing else is trusted):

  BOS side:     list_objects over every prefix the dataset actually lives
                under (bytes + count)
  JuiceFS side: the folder's recursive size as returned by the D-Robotics
                files API (each dir entry carries its total size)

Explicitly NOT trusted, with reasons learned on 2026-08-03:
  - tasks.transfer_status  — self-reported by a Celery pipeline dead since
    2026-06-30
  - tasks.size_gb          — only the increment one task downloaded; a
    resumed task under-reports its prefix
  - tasks.bos_path         — stale/garbage for many rows (NULL for
    AgiBotWorld-Beta-BJ; a JuiceFS-style path for InternData-A1). The
    uploader ignores it: bos_target() always writes {category}/{name}/
    (datasets) or {name}/ (models).

BOS prefix resolution per dataset, all non-empty candidates aggregated:
  1. canonical bos_target(): {category}/{name}/
  2. legacy June-era layout: datasets/{name}/
  3. the task row's bos_path (when set and different)
  4. fallback probe of {top-level-dir}/{name}/ across every top-level dir
     (cheap: max_keys=1 per probe) — catches category drift like
     InternData-A1 living under manipulation/ with category='other'

Expected JuiceFS layout (dlm/transfer/tasks.py):
  datasets: /727a2f92-30c/auwomo-datasets/raw-data/{category}/{name}
  models:   /727a2f92-30c/auwomo-model[/{category}]/{name}
Early manual imports also landed directly under /727a2f92-30c/auwomo-datasets/
so that legacy location is checked as a fallback.

Verdicts (JuiceFS vs the LARGEST single BOS location):
  VERIFIED         jfs_size >= bos_size (JuiceFS dir sizes carry fs overhead
                   of ~0-4%, so >= is the completeness signal; gross
                   oversize is reported in the delta column for eyeballing)
  PARTIAL          folder exists but smaller than BOS
  NOT_TRANSFERRED  no folder found (or 0 bytes)
  BOS_EMPTY        no BOS location holds any objects (metadata-only task /
                   data lost — needs source-repo audit, not a transfer)

Usage (on S1): python3 scripts/reconcile_transfers.py [--json OUT.json]
Makes no writes anywhere (SQLite opened read-only, only GET/list APIs).
"""

import argparse
import json
import os
import sqlite3
import sys
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dlm.core.bos import create_bos_client  # noqa: E402
from dlm.core.config import load_config  # noqa: E402
from dlm.constants import DATA_BUCKET, MODEL_BUCKET  # noqa: E402
from dlm.transfer.dcloud import DCloudClient  # noqa: E402

DB_PATH = os.environ.get("DLM_DB_PATH", "/data/dlm.db")
ROOT = "/727a2f92-30c"
RAW_DATA = f"{ROOT}/auwomo-datasets/raw-data"
LEGACY_DATA = f"{ROOT}/auwomo-datasets"
MODEL_ROOT = f"{ROOT}/auwomo-model"


def bos_prefix_stats(client, bucket, prefix):
    total, count, marker = 0, 0, ""
    while True:
        resp = client.list_objects(bucket, prefix=prefix, marker=marker, max_keys=1000)
        for obj in getattr(resp, "contents", None) or []:
            total += obj.size
            count += 1
        if not getattr(resp, "is_truncated", False):
            break
        marker = resp.next_marker
    return total, count


def top_level_dirs(client, bucket):
    dirs, marker = [], ""
    while True:
        resp = client.list_objects(bucket, prefix="", delimiter="/",
                                   marker=marker, max_keys=1000)
        dirs += [p.prefix for p in (getattr(resp, "common_prefixes", None) or [])]
        if not getattr(resp, "is_truncated", False):
            break
        marker = resp.next_marker
    return dirs


def prefix_nonempty(client, bucket, prefix):
    resp = client.list_objects(bucket, prefix=prefix, max_keys=1)
    return bool(getattr(resp, "contents", None))


def list_folder(dcloud, path):
    """name -> recursive size for every entry directly under path."""
    out, page = {}, 1
    while True:
        resp = dcloud.list_files(path=path, page=page, page_size=50)
        files = (resp.get("data") or {}).get("files") or []
        for f in files:
            out[f["name"]] = int(f.get("size") or 0)
        if len(files) < 50:
            break
        page += 1
    return out


def fmt_tb(n):
    return f"{n / 1024**4:.2f}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", default="", help="also write full results to this path")
    args = parser.parse_args()

    cfg = load_config()
    user, pw = os.environ.get("DCLOUD_USER"), os.environ.get("DCLOUD_PASS")
    if not user or not pw:
        sys.exit("DCLOUD_USER/DCLOUD_PASS not set")

    bos = create_bos_client(cfg["BAIDU_AK"], cfg["BAIDU_SK"],
                            cfg.get("BOS_ENDPOINT") or "https://bj.bcebos.com")
    dcloud = DCloudClient(user, pw)
    dcloud.login()

    db = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    tasks = [dict(r) for r in db.execute(
        "SELECT id, name, type, category, bos_path, transfer_status "
        "FROM tasks WHERE status = 'done'")]

    # Multiple task rows can share a dataset (resumed / re-created tasks).
    # The unit of reconciliation is (type, name).
    by_dataset = {}
    for t in tasks:
        by_dataset.setdefault(((t["type"] or "dataset"), t["name"]), []).append(t)

    data_dirs = top_level_dirs(bos, DATA_BUCKET)
    juicefs_cache = {}

    def folder_sizes(parent):
        if parent not in juicefs_cache:
            try:
                juicefs_cache[parent] = list_folder(dcloud, parent)
            except Exception as e:
                print(f"  WARN: listing {parent} failed: {e}", file=sys.stderr)
                juicefs_cache[parent] = {}
        return juicefs_cache[parent]

    results = []
    for (task_type, name), rows in sorted(by_dataset.items(), key=lambda kv: kv[0][1]):
        categories = sorted({r["category"] for r in rows if r["category"]})
        bucket = MODEL_BUCKET if task_type == "model" else DATA_BUCKET

        # -- BOS side: every location this dataset actually occupies
        candidates = []
        if task_type == "model":
            candidates.append(f"{name}/")
        else:
            candidates += [f"{c}/{name}/" for c in categories]
            candidates.append(f"datasets/{name}/")
            for d in data_dirs:
                candidates.append(f"{d}{name}/")
        for r in rows:
            bp = (r["bos_path"] or "").lstrip("/")
            if bp:
                candidates.append(bp if bp.endswith("/") else bp + "/")
        seen, locations = set(), []
        for cand in candidates:
            if cand in seen:
                continue
            seen.add(cand)
            if prefix_nonempty(bos, bucket, cand):
                b, n = bos_prefix_stats(bos, bucket, cand)
                if b:
                    locations.append({"prefix": cand, "bytes": b, "objects": n})
        locations.sort(key=lambda l: -l["bytes"])
        primary = locations[0] if locations else None

        # -- JuiceFS side
        if task_type == "model":
            jfs_parents = [f"{MODEL_ROOT}/{c}" for c in categories] + [MODEL_ROOT]
        else:
            jfs_parents = [f"{RAW_DATA}/{c}" for c in categories] + [RAW_DATA, LEGACY_DATA]
        jfs_bytes, jfs_where = 0, ""
        for parent in jfs_parents:
            size = folder_sizes(parent).get(name)
            if size:
                jfs_bytes, jfs_where = size, f"{parent}/{name}"
                break

        if not primary:
            verdict = "BOS_EMPTY"
        elif not jfs_bytes:
            verdict = "NOT_TRANSFERRED"
        elif jfs_bytes >= primary["bytes"]:
            verdict = "VERIFIED"
        else:
            verdict = "PARTIAL"

        results.append({
            "name": name, "type": task_type, "categories": categories,
            "bos_locations": locations,
            "bos_primary_bytes": primary["bytes"] if primary else 0,
            "jfs_bytes": jfs_bytes, "jfs_path": jfs_where,
            "verdict": verdict,
            "claimed": sorted({r["transfer_status"] or "none" for r in rows}),
            "task_ids": [r["id"] for r in rows],
        })
        pb = primary["bytes"] if primary else 0
        pct = f"{100 * jfs_bytes / pb:6.1f}%" if pb else "   n/a"
        extra = f" (+{len(locations) - 1} more locations)" if len(locations) > 1 else ""
        loc = primary["prefix"] if primary else "-"
        print(f"  {verdict:15} {pct}  {name}  [{loc} {fmt_tb(pb)} TB]{extra}")

    print("\n=== Summary (unit = dataset) ===")
    agg = defaultdict(lambda: [0, 0])
    for r in results:
        agg[r["verdict"]][0] += 1
        agg[r["verdict"]][1] += r["bos_primary_bytes"]
    for v, (n, b) in sorted(agg.items()):
        print(f"  {v:15} {n:3} datasets  {fmt_tb(b)} TB")
    todo = [r for r in results if r["verdict"] in ("NOT_TRANSFERRED", "PARTIAL")]
    print(f"\n  TO TRANSFER: {len(todo)} datasets, "
          f"{fmt_tb(sum(r['bos_primary_bytes'] - r['jfs_bytes'] for r in todo))} TB remaining")
    multi = [r for r in results if len(r["bos_locations"]) > 1]
    if multi:
        print(f"  MULTI-LOCATION (duplicate storage worth reviewing): "
              f"{', '.join(r['name'] for r in multi)}")

    if args.json:
        with open(args.json, "w") as f:
            json.dump(results, f, ensure_ascii=False, indent=1)
        print(f"\nFull results: {args.json}")


if __name__ == "__main__":
    main()
