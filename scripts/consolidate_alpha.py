#!/usr/bin/env python3
"""One-shot: make manipulation/AgiBotWorld-Alpha/ the complete Alpha copy.

Server-side copy only — never deletes anything. Designed to run on S1.

Background (reconciliation of 2026-08-02, see
docs/superpowers/plans/2026-08-02-control-plane-hardening-and-alpha.md):
Alpha (258 files / 9000.8 GiB on ModelScope agibot_world/AgiBotWorld-Alpha)
is fully present on BOS, split across two prefixes. manipulation/ lacks
exactly two objects, both present with exact size in other/:

  - observations/352/648544-655345.tar   45.2 GB   (absent — pure addition)
  - .gitattributes                       20,361 B  (manipulation/ holds a
    stale 2,461 B version; it is backed up server-side to
    .gitattributes.bak-20260802 before being overwritten)

This script verifies each fact against live BOS before acting, is dry-run by
default (--execute to copy), and re-verifies completeness afterwards.

Usage (on S1):
    python3 scripts/consolidate_alpha.py             # dry-run
    python3 scripts/consolidate_alpha.py --execute   # actually copy
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dlm.core.bos import create_bos_client  # noqa: E402
from dlm.core.config import load_config  # noqa: E402
from dlm.constants import DATA_BUCKET as BUCKET  # noqa: E402

SRC_PREFIX = "other/AgiBotWorld-Alpha/"
DST_PREFIX = "manipulation/AgiBotWorld-Alpha/"
# Backups live OUTSIDE the dataset prefix — the canonical prefix must hold
# repo files only, and the resume filter must never see a .bak key.
BACKUP_PREFIX = "download-manager/backups/AgiBotWorld-Alpha/"
REPO_ID = "agibot_world/AgiBotWorld-Alpha"

COPY_OBJECT_LIMIT = 5 * 1024 ** 3  # BOS single CopyObject cap
PART_SIZE = 1024 ** 3

# The two objects the reconciliation identified. Verified against live BOS
# before any copy; a mismatch aborts rather than "fixing" a drifted world.
PLANNED = [
    {
        "rel": "observations/352/648544-655345.tar",
        "size": 48_561_940_480,   # exact size from the 2026-08-02 filelist; re-checked at runtime
        "overwrites": False,
        "backup": None,
    },
    {
        "rel": ".gitattributes",
        "size": 20_361,
        "overwrites": True,       # dst holds a stale 2,461 B version
        "backup": "gitattributes.bak-20260802",
    },
]


def head_size(client, key):
    try:
        meta = client.get_object_meta_data(BUCKET, key)
        return int(meta.metadata.content_length)
    except Exception:
        return None


def multipart_copy(client, src_key, dst_key, size, part_workers=8):
    """Server-side multipart copy (same pattern as merge_beta_to_bj.py)."""
    from concurrent.futures import ThreadPoolExecutor

    upload_id = client.initiate_multipart_upload(BUCKET, dst_key).upload_id
    try:
        ranges = []
        offset = 0
        part_number = 1
        while offset < size:
            ranges.append((part_number, min(PART_SIZE, size - offset), offset))
            offset += min(PART_SIZE, size - offset)
            part_number += 1

        def copy_part(args):
            pn, psize, poff = args
            resp = client.upload_part_copy(
                BUCKET, src_key, BUCKET, dst_key, upload_id, pn, psize, poff,
            )
            return {"partNumber": pn, "eTag": resp.etag}

        with ThreadPoolExecutor(max_workers=part_workers) as pool:
            parts = list(pool.map(copy_part, ranges))
        parts.sort(key=lambda p: p["partNumber"])
        client.complete_multipart_upload(BUCKET, dst_key, upload_id, parts)
    except Exception:
        try:
            client.abort_multipart_upload(BUCKET, dst_key, upload_id=upload_id)
        except Exception:
            pass
        raise


def copy_object(client, src_key, dst_key, size):
    if size > COPY_OBJECT_LIMIT:
        multipart_copy(client, src_key, dst_key, size)
    else:
        client.copy_object(BUCKET, src_key, BUCKET, dst_key)


def list_bos(client, prefix):
    out, marker = {}, ""
    while True:
        resp = client.list_objects(BUCKET, prefix=prefix, marker=marker, max_keys=1000)
        for obj in getattr(resp, "contents", None) or []:
            rel = obj.key[len(prefix):]
            if rel:
                out[rel] = obj.size
        if not getattr(resp, "is_truncated", False):
            break
        marker = resp.next_marker
    return out


def list_repo():
    """Full ModelScope listing, same code path as the download pipeline."""
    from modelscope.hub.api import HubApi

    api = HubApi()
    token = os.environ.get("MODELSCOPE_API_TOKEN") or os.environ.get("MS_TOKEN")
    out, page = {}, 1
    while True:
        page_files = api.get_dataset_files(
            repo_id=REPO_ID, recursive=True,
            page_number=page, page_size=100, token=token,
        )
        if not page_files:
            break
        for item in page_files:
            if isinstance(item, dict) and item.get("Type") == "blob":
                path, size = item.get("Path", ""), item.get("Size", 0) or 0
                if path and size > 0:
                    out[path] = size
        if len(page_files) < 100:
            break
        page += 1
    return out


def verify_complete(client, repo):
    dst = list_bos(client, DST_PREFIX)
    missing = [p for p, s in repo.items() if dst.get(p) != s]
    return missing, len(repo)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true",
                        help="actually copy (default: dry-run)")
    args = parser.parse_args()

    config = load_config()
    client = create_bos_client(
        config["BAIDU_AK"], config["BAIDU_SK"],
        config.get("BOS_ENDPOINT") or "https://bj.bcebos.com",
    )

    # Pre-flight: the world must still match the reconciliation this plan
    # was approved against. A dst already holding the exact target size is
    # "already done" — the script is safely re-runnable (A6 re-verifies).
    todo = []
    for item in PLANNED:
        src_key = SRC_PREFIX + item["rel"]
        dst_key = DST_PREFIX + item["rel"]
        dst_size = head_size(client, dst_key)
        if dst_size == item["size"]:
            print(f"  skip: {dst_key} already complete ({dst_size:,} B)")
            continue
        src_size = head_size(client, src_key)
        if src_size != item["size"]:
            sys.exit(f"ABORT: {src_key} size {src_size} != expected {item['size']} "
                     f"— world changed since reconciliation, re-run the analysis")
        if item["overwrites"] and dst_size is None:
            sys.exit(f"ABORT: expected a stale {dst_key} to overwrite, found none")
        if not item["overwrites"] and dst_size is not None:
            sys.exit(f"ABORT: {dst_key} exists with unexpected size {dst_size} B")
        print(f"  plan: {src_key} ({item['size']:,} B) -> {dst_key}"
              + (f"  [overwrites {dst_size:,} B; backup first]" if item["overwrites"] else ""))
        todo.append(item)

    if not args.execute:
        print("\nDry-run only. Re-run with --execute to copy.")
        return

    for item in todo:
        src_key = SRC_PREFIX + item["rel"]
        dst_key = DST_PREFIX + item["rel"]
        if item["backup"]:
            backup_key = BACKUP_PREFIX + item["backup"]
            # A backup key must never be overwritten: on a partial-failure
            # re-run it would be the only surviving copy of the original.
            if head_size(client, backup_key) is not None:
                print(f"  backup exists, keeping: {backup_key}")
            else:
                print(f"  backup: {dst_key} -> {backup_key}")
                client.copy_object(BUCKET, dst_key, BUCKET, backup_key)
        print(f"  copy: {src_key} -> {dst_key} ...")
        copy_object(client, src_key, dst_key, item["size"])
        got = head_size(client, dst_key)
        if got != item["size"]:
            sys.exit(f"ABORT: post-copy size {got} != {item['size']} for {dst_key}")
        print(f"    done ({got:,} B verified)")

    print("\nRe-running full reconciliation against ModelScope ...")
    repo = list_repo()
    missing, total = verify_complete(client, repo)
    if missing:
        sys.exit(f"INCOMPLETE: {len(missing)}/{total} still missing/mismatched: "
                 f"{missing[:5]}")
    print(f"COMPLETE: {DST_PREFIX} matches all {total} repo files (key+size).")


if __name__ == "__main__":
    main()
