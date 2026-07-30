#!/usr/bin/env python3
"""Merge AgiBotWorld-Beta/ objects into AgiBotWorld-Beta-BJ/ on BOS.

Server-side copy only — never deletes anything. Designed to run on S1.

Rules (from the 2026-07-31 investigation):
- Copy only keys that exist under Beta/ but NOT under Beta-BJ/.
- Skip .cache/* junk (HF download artifacts).
- Skip known conflicts (.gitattributes, README.md, and the size-mismatched
  observations/709/864860-911899.tar) — the BJ copy wins.
- ModelScope size gate: if the ModelScope repo lists the file with a
  DIFFERENT size, copying is pointless (the resume filter would re-download
  and overwrite it) — skip and report.
- Pre-flight namespace check: the existing Beta-BJ keys must join against
  the ModelScope listing at high rate, else relative-key namespaces differ
  and the whole merge (and resume filter) would be misaligned — abort.
- Idempotent: objects already present at the target with matching size are
  skipped, so the script can be re-run safely.

Usage:
    python3 scripts/merge_beta_to_bj.py                 # dry-run (default)
    python3 scripts/merge_beta_to_bj.py --execute       # actually copy
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dlm.core.bos import create_bos_client  # noqa: E402
from dlm.core.config import load_config  # noqa: E402

BUCKET = "auwomo-data"
SRC_PREFIX = "manipulation/AgiBotWorld-Beta/"
DST_PREFIX = "manipulation/AgiBotWorld-Beta-BJ/"
MS_REPO = None  # resolved from --repo-id or the DLM task record

# Conflicts where the BJ copy wins (verified 2026-07-31)
CONFLICT_SKIP = {
    ".gitattributes",
    "README.md",
    "observations/709/864860-911899.tar",
}

COPY_OBJECT_LIMIT = 5 * 1024 ** 3  # BOS single CopyObject cap
PART_SIZE = 1024 ** 3  # 1 GB parts for multipart copy
MIN_NAMESPACE_JOIN = 0.95  # abort if fewer than 95% of BJ keys join MS listing


def list_objects(client, prefix):
    """Full paginated listing → {relative_key: size}."""
    out = {}
    marker = ""
    while True:
        resp = client.list_objects(BUCKET, prefix=prefix, marker=marker, max_keys=1000)
        for obj in getattr(resp, "contents", None) or []:
            out[obj.key[len(prefix):]] = obj.size
        if not getattr(resp, "is_truncated", False):
            break
        marker = resp.next_marker
    return out


def list_modelscope(repo_id):
    """Full ModelScope dataset file listing → {path: size}."""
    import requests

    out = {}
    page = 1
    while True:
        resp = requests.get(
            f"https://modelscope.cn/api/v1/datasets/{repo_id}/repo/files",
            params={"Recursive": "true", "PageNumber": page, "PageSize": 100},
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json().get("Data", {}) or {}
        files = data.get("Files", []) or []
        if not files:
            break
        for item in files:
            if item.get("Type") == "blob":
                path, size = item.get("Path", ""), item.get("Size", 0) or 0
                if path and size > 0:
                    out[path] = size
        if len(files) < 100:
            break
        page += 1
    return out


def multipart_copy(client, src_key, dst_key, size):
    upload_id = client.initiate_multipart_upload(BUCKET, dst_key).upload_id
    try:
        parts = []
        offset = 0
        part_number = 1
        while offset < size:
            part_size = min(PART_SIZE, size - offset)
            resp = client.upload_part_copy(
                BUCKET, src_key, BUCKET, dst_key,
                upload_id, part_number, part_size, offset,
            )
            parts.append({"partNumber": part_number, "eTag": resp.etag})
            offset += part_size
            part_number += 1
        client.complete_multipart_upload(BUCKET, dst_key, upload_id, parts)
    except Exception:
        try:
            client.abort_multipart_upload(BUCKET, dst_key, upload_id=upload_id)
        except Exception:
            pass
        raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true",
                        help="actually copy (default: dry-run)")
    parser.add_argument("--repo-id", required=True,
                        help="ModelScope repo id for the size gate (e.g. agibot-world/AgiBotWorld-Beta)")
    args = parser.parse_args()

    config = load_config()
    client = create_bos_client(
        config["BAIDU_AK"], config["BAIDU_SK"], config["BOS_ENDPOINT"]
    )

    print(f"Listing {DST_PREFIX} ...")
    dst = list_objects(client, DST_PREFIX)
    print(f"  {len(dst)} objects, {sum(dst.values()) / 1024**4:.2f} TB")

    print(f"Listing {SRC_PREFIX} ...")
    src = list_objects(client, SRC_PREFIX)
    print(f"  {len(src)} objects, {sum(src.values()) / 1024**4:.2f} TB")

    print(f"Listing ModelScope {args.repo_id} ...")
    ms = list_modelscope(args.repo_id)
    print(f"  {len(ms)} files, {sum(ms.values()) / 1024**4:.2f} TB")

    # Pre-flight: BJ keys must join the MS namespace, or everything is misaligned
    if dst:
        joined = sum(1 for k in dst if k in ms)
        rate = joined / len(dst)
        print(f"Namespace join: {joined}/{len(dst)} BJ keys in MS listing ({rate:.1%})")
        if rate < MIN_NAMESPACE_JOIN:
            print("ABORT: BJ keys do not join the ModelScope namespace — "
                  "relative-key layouts differ, merging would misalign the resume filter.")
            sys.exit(1)

    will_copy, junk, conflicts, size_mismatch, already = [], [], [], [], []
    for rel, size in sorted(src.items()):
        if rel.startswith(".cache/"):
            junk.append(rel)
            continue
        if rel in CONFLICT_SKIP:
            conflicts.append(rel)
            continue
        if dst.get(rel) == size:
            already.append(rel)
            continue
        if rel in dst:
            # target exists with a different size — BJ wins, do not overwrite
            conflicts.append(rel)
            continue
        if rel in ms and ms[rel] != size:
            size_mismatch.append((rel, size, ms[rel]))
            continue
        will_copy.append((rel, size))

    total_copy_bytes = sum(s for _, s in will_copy)
    print("\n=== Plan ===")
    print(f"  will copy      : {len(will_copy)} objects, {total_copy_bytes / 1024**4:.2f} TB")
    print(f"  already at dst : {len(already)}")
    print(f"  junk skipped   : {len(junk)}")
    print(f"  conflict skip  : {len(conflicts)} (BJ copy wins)")
    print(f"  MS-size skip   : {len(size_mismatch)} (would be re-downloaded anyway)")
    for rel, hf_size, ms_size in size_mismatch[:10]:
        print(f"    {rel}: HF={hf_size} MS={ms_size}")

    # Expected resume-filter skip count after merge (acceptance A2 reference)
    post_merge = dict(dst)
    for rel, size in will_copy:
        post_merge[rel] = size
    expected_skip = sum(1 for p, s in ms.items() if post_merge.get(p) == s)
    print(f"\nExpected resume-filter skips after merge: {expected_skip}/{len(ms)} MS files")

    if not args.execute:
        print("\nDry-run only. Re-run with --execute to copy.")
        return

    print("\n=== Copying ===")
    done_bytes = 0
    for i, (rel, size) in enumerate(will_copy, 1):
        src_key = SRC_PREFIX + rel
        dst_key = DST_PREFIX + rel
        try:
            if size > COPY_OBJECT_LIMIT:
                multipart_copy(client, src_key, dst_key, size)
            else:
                client.copy_object(BUCKET, src_key, BUCKET, dst_key)
            done_bytes += size
            print(f"[{i}/{len(will_copy)}] {rel} ({size / 1024**3:.1f} GB) "
                  f"— {done_bytes / 1024**4:.2f} TB done", flush=True)
        except Exception as e:
            print(f"[{i}/{len(will_copy)}] FAILED {rel}: {e}", flush=True)

    print("\nVerifying ...")
    dst_after = list_objects(client, DST_PREFIX)
    print(f"  {DST_PREFIX}: {len(dst_after)} objects, "
          f"{sum(dst_after.values()) / 1024**4:.2f} TB "
          f"(was {len(dst)}, expected ≈ {len(dst) + len(will_copy)})")


if __name__ == "__main__":
    main()
