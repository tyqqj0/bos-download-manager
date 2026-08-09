"""Did the remote import actually deliver? Three read-only checks.

The step this module implements is the hole in the old transfer poller: it wrote
`transfer_status='done'` the moment the far side said 成功, and looked at nothing.
"The remote job reported success" and "the bytes are there" are not the same
claim — the 2026-08-09 DL3DV failure ran for five days and died on two objects,
and a `done` that nobody measured is the same class of untrustworthy record as
the false download `done` that started this whole feature.

The three checks mirror `scripts/transfer_import.py`, deliberately: the manual
script and the dispatcher must agree about what "verified" means, or the two
surfaces will disagree about the same directory.

  1. **size** — the target folder holds at least the bytes the source prefix
     held when we dispatched. Short ⇒ `short`, never `done`.
  2. **scope** — the target's top-level children are a subset of the source's.
     A size check passes happily on oversize, so only this one catches prefix
     bleed (importing `RDT-1B` without the trailing slash would drag in
     `RDT-1B-repair/` and `RDT-1B_extracted/`) and pathological nesting.
  3. **BOS untouched** — the source prefix's bytes and object count are what
     they were at dispatch. We only ever read BOS, so a difference is somebody
     else writing the prefix mid-transfer. Recorded and alerted, never used to
     fail the transfer: it is information about the world, not a fault here.

The denominator for check 1 is the BOS measurement taken at dispatch time, NOT
`SUM(shards.total_bytes)`. Those differ by orders of magnitude on any resumed
task — RoboDojo dispatched 715.7 GB against 6189.9 GB actually sitting on the
prefix — and verifying a full-prefix import against one round's dispatch would
pass a transfer that moved a tenth of the data.

Every call here is a list: re-running a verification is free and changes
nothing, which is what lets the poller re-verify a row whose process died
mid-check instead of having to remember where it was.
"""

import logging

from .measure import bos_stats, bos_top_children, jfs_children, jfs_folder_size

logger = logging.getLogger("dlm.transfer")


def verify_transfer(bos, dcloud, plan, bos_bytes: int, bos_objects: int) -> dict:
    """Run the three checks for one finished import.

    `bos_bytes`/`bos_objects` are the dispatch-time measurement of
    `plan.source`. Returns:

        {"status": "done" | "short",   # what to write to transfer_status
         "jfs_bytes": int | None,      # None = the target does not exist
         "extra_children": [str] | None,   # None = the scope listing failed
         "bos_bytes": int, "bos_objects": int,   # re-measured now
         "bos_changed": bool,
         "detail": str}                # human-readable, goes to transfer_error
    """
    # A row with no dispatch-time measurement cannot be verified at all: every
    # `jfs_bytes >= 0` would pass. Refuse rather than rubber-stamp.
    if bos_bytes <= 0:
        return {
            "status": "short", "jfs_bytes": None, "extra_children": None,
            "bos_bytes": bos_bytes, "bos_objects": bos_objects,
            "bos_changed": False,
            "detail": ("cannot verify: no dispatch-time BOS measurement "
                       "(transfer_bos_bytes is 0)"),
        }

    # ---- Check 1: size.
    jfs_bytes = jfs_folder_size(dcloud, plan.parent, plan.name)

    # ---- Check 2: scope. A failed listing leaves this unknown; it must not
    # turn into a false "extras found", so `None` propagates instead of an
    # empty set.
    extra = None
    try:
        src_children = bos_top_children(bos, plan.bucket, plan.prefix)
        dst_children = jfs_children(dcloud, plan.target)
        extra = sorted(dst_children - src_children)
    except Exception as e:  # noqa: BLE001 — an unknown scope is not a failure
        logger.warning(f"scope check listing failed for {plan.target}: {e}")

    # ---- Check 3: BOS untouched.
    bos_bytes_now, bos_objects_now = bos_stats(bos, plan.bucket, plan.prefix)
    bos_changed = (bos_bytes_now != bos_bytes or bos_objects_now != bos_objects)

    notes = []
    status = "done"

    if jfs_bytes is None:
        status = "short"
        notes.append(f"target {plan.target} does not exist on 地瓜云 "
                     f"({bos_bytes} bytes expected)")
    elif jfs_bytes < bos_bytes:
        status = "short"
        notes.append(f"size check failed: 地瓜云 has {jfs_bytes} bytes, "
                     f"{bos_bytes - jfs_bytes} short of the {bos_bytes} on BOS")
    else:
        notes.append(f"{jfs_bytes} bytes >= {bos_bytes} on BOS")

    if extra:
        status = "short"
        notes.append(f"scope check failed: target holds {len(extra)} "
                     f"child(ren) absent from the source prefix: "
                     f"{extra[:10]}")
    elif extra is None:
        notes.append("scope check skipped (listing failed)")

    if bos_changed:
        # Cannot have been us — this process only lists BOS. Surfaced on the
        # row either way, including on an otherwise clean `done`.
        notes.append(f"BOS prefix changed during the transfer: "
                     f"{bos_bytes}B/{bos_objects} objects at dispatch -> "
                     f"{bos_bytes_now}B/{bos_objects_now} now")

    return {
        "status": status,
        "jfs_bytes": jfs_bytes,
        "extra_children": extra,
        "bos_bytes": bos_bytes_now,
        "bos_objects": bos_objects_now,
        "bos_changed": bos_changed,
        "detail": "; ".join(notes),
    }


__all__ = ["verify_transfer"]
