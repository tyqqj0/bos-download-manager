"""Measure both ends of a transfer: bytes and top-level children.

These four functions were written inside `scripts/transfer_import.py`. They are
here because the automatic dispatcher needs the same numbers the manual script
verifies against, and a second copy of "how many bytes are under this prefix"
is exactly how the manual script ended up disagreeing with the rest of the
codebase about which bucket a model lives in. The script now imports them, so
there is one definition and one place a bug can hide.

Everything here is READ-ONLY on both ends: `list_objects` on BOS, `list_files`
on 地瓜云. Nothing in this module can create, move or delete a byte anywhere.
That is a load-bearing property, not a coincidence — verification runs against
data we must not disturb, and the "BOS untouched" check would be meaningless if
the checker could write.
"""

import logging
import time

logger = logging.getLogger("dlm.transfer")


def bos_stats(bos, bucket: str, prefix: str):
    """`(total_bytes, object_count)` under `prefix`.

    Walks every page — a 3.4 TB dataset is ~50k objects, so ~50 round trips.
    Far too slow for a request handler, which is why arming never calls this and
    the dispatcher runs it on the scheduler's thread pool.
    """
    total, count, marker = 0, 0, ""
    while True:
        resp = bos.list_objects(bucket, prefix=prefix, marker=marker, max_keys=1000)
        for obj in getattr(resp, "contents", None) or []:
            total += obj.size
            count += 1
        if not getattr(resp, "is_truncated", False):
            break
        marker = resp.next_marker
    return total, count


def bos_top_children(bos, bucket: str, prefix: str) -> set:
    """Top-level child names under `prefix` (dirs without trailing /, plus
    direct files). The source half of the scope check."""
    names, marker = set(), ""
    while True:
        resp = bos.list_objects(bucket, prefix=prefix, delimiter="/",
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


def jfs_folder_size(dcloud, parent: str, name: str, retries: int = 3,
                    sleep_s: float = 30):
    """Recursive size of `parent/name` per the files API, None if absent.

    None and 0 are different answers and callers must keep them apart: None
    means the import created nothing at all, 0 means it created an empty
    folder. Both fail the size check, but they fail for different reasons.
    """
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
            logger.warning(f"jfs list {parent} attempt {attempt + 1} failed: {e}")
            if sleep_s:
                time.sleep(sleep_s)


def jfs_children(dcloud, path: str) -> set:
    """Immediate child names of a 地瓜云 folder. The target half of the scope
    check."""
    names, page = set(), 1
    while True:
        resp = dcloud.list_files(path=path, page=page, page_size=50)
        files = (resp.get("data") or {}).get("files") or []
        names |= {f["name"] for f in files}
        if len(files) < 50:
            return names
        page += 1


__all__ = ["bos_stats", "bos_top_children", "jfs_children", "jfs_folder_size"]
