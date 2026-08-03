#!/usr/bin/env python3
"""READ-ONLY: build a deletion-candidate manifest for duplicate BOS storage.

For every multi-location dataset from the 2026-08-03 reconciliation, plus
the known standalone duplicates, verify object-by-object (relative key +
exact size) that the secondary location is a SUBSET of the primary. Only a
fully-redundant secondary is eligible for deletion; anything with unique
content is reported and excluded.

This script DELETES NOTHING and contains no delete call. It emits a
manifest for human sign-off; actual deletion is a separate, explicitly
approved step.

Usage (S1): python3 scripts/verify_duplicates.py --json /root/dup_manifest.json
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dlm.core.bos import create_bos_client  # noqa: E402
from dlm.core.config import load_config  # noqa: E402
from dlm.constants import DATA_BUCKET  # noqa: E402

RECONCILE_JSON = "/root/transfer_reconcile-20260803-v2.json"
# Standalone duplicates known outside the reconciliation's per-task view.
EXTRA_PAIRS = [
    # (secondary-to-consider-deleting, primary-that-must-cover-it)
    ("other/AgiBotWorld-Alpha/", "manipulation/AgiBotWorld-Alpha/"),
    ("manipulation/InternData-A1/", "other/InternData-A1/"),
    ("manipulation/AgiBotWorld-Beta/", "manipulation/AgiBotWorld-Beta-BJ/"),
]


def listing(bos, prefix):
    out, marker = {}, ""
    while True:
        resp = bos.list_objects(DATA_BUCKET, prefix=prefix, marker=marker, max_keys=1000)
        for obj in getattr(resp, "contents", None) or []:
            rel = obj.key[len(prefix):]
            if rel:
                out[rel] = obj.size
        if not getattr(resp, "is_truncated", False):
            break
        marker = resp.next_marker
    return out


def compare(bos, secondary, primary, cache):
    if primary not in cache:
        cache[primary] = listing(bos, primary)
    if secondary not in cache:
        cache[secondary] = listing(bos, secondary)
    prim, sec = cache[primary], cache[secondary]
    unique = {k: v for k, v in sec.items() if prim.get(k) != v}
    return {
        "secondary": secondary,
        "primary": primary,
        "secondary_bytes": sum(sec.values()),
        "secondary_objects": len(sec),
        "unique_objects": len(unique),
        "unique_bytes": sum(unique.values()),
        "unique_sample": sorted(unique)[:10],
        "fully_redundant": not unique,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", required=True)
    args = parser.parse_args()

    cfg = load_config()
    bos = create_bos_client(cfg["BAIDU_AK"], cfg["BAIDU_SK"],
                            cfg.get("BOS_ENDPOINT") or "https://bj.bcebos.com")

    pairs = list(EXTRA_PAIRS)
    with open(RECONCILE_JSON) as f:
        recon = json.load(f)
    for r in recon:
        locs = r.get("bos_locations") or []
        if len(locs) > 1:
            primary = locs[0]["prefix"]
            for sec in locs[1:]:
                pairs.append((sec["prefix"], primary))

    cache, results = {}, []
    seen = set()
    for secondary, primary in pairs:
        if secondary in seen or secondary == primary:
            continue
        seen.add(secondary)
        print(f"checking {secondary} against {primary} ...", flush=True)
        try:
            results.append(compare(bos, secondary, primary, cache))
        except Exception as e:
            results.append({"secondary": secondary, "primary": primary,
                            "error": str(e)})

    deletable = [r for r in results if r.get("fully_redundant")]
    blocked = [r for r in results if r.get("fully_redundant") is False]
    errors = [r for r in results if "error" in r]

    print("\n=== DELETABLE (every object present in primary at same size) ===")
    total = 0
    for r in deletable:
        total += r["secondary_bytes"]
        print(f"  {r['secondary']:55} {r['secondary_bytes'] / 1024**4:7.2f} TiB "
              f"({r['secondary_objects']} obj)  primary={r['primary']}")
    print(f"  TOTAL RECLAIMABLE: {total / 1024**4:.2f} TiB")

    print("\n=== BLOCKED (unique content — NOT deletable as-is) ===")
    for r in blocked:
        print(f"  {r['secondary']:55} unique {r['unique_objects']} obj / "
              f"{r['unique_bytes'] / 1024**3:.2f} GiB  e.g. {r['unique_sample'][:3]}")
    for r in errors:
        print(f"  ERROR {r['secondary']}: {r['error']}")

    with open(args.json, "w") as f:
        json.dump(results, f, ensure_ascii=False, indent=1)
    print(f"\nManifest: {args.json}")


if __name__ == "__main__":
    main()
