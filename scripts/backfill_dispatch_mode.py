#!/usr/bin/env python3
"""scripts/backfill_dispatch_mode.py — put the existing backlog on pool.

Flipping DEFAULT_DISPATCH_MODE only governs rows created after the flip.
SQLite's `ALTER TABLE tasks ADD COLUMN dispatch_mode TEXT DEFAULT 'sharded'`
materialised that literal on every row that already existed, so the whole
backlog is physically stamped 'sharded' and no Python-side default can reach
it. R2 wants three classes of row on pool — new tasks, existing `pending`
rows, and re-dispatched `failed`/`paused` rows — and this script is the only
thing that delivers the second and third.

Scope, deliberately narrow:

  * `downloading` rows are NEVER touched. A running task's mode describes the
    coordinator currently driving it; rewriting it under a live
    ShardedDownloadWorkflow would make every later read lie about what is
    actually running. A3 requires proving this, so the script snapshots those
    rows before and after and diffs them.
  * Terminal history (`done`, `revoked`, `skipped`) is left alone. Those rows
    will never dispatch again, so changing them is churn that only makes the
    audit trail wrong about how they ran.

Run on S1 (the only host with the database):

    python3 scripts/backfill_dispatch_mode.py            # dry run, default
    python3 scripts/backfill_dispatch_mode.py --apply
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dlm.queue.snapshot import _conn, init_db

BACKFILL_STATUSES = ("pending", "paused", "failed")

# COALESCE is load-bearing, not decoration. Under SQL's three-valued logic
# `NULL <> 'pool'` evaluates to NULL — not true — so a bare comparison
# SILENTLY SKIPS every row whose dispatch_mode is NULL, which is exactly the
# kind of row most in need of backfilling. The migration does stamp 'sharded'
# on rows that existed when it ran, but /api/storage/register creates task rows
# without passing dispatch_mode at all, so "NULL rows cannot exist" is a bet
# with no upside. COALESCE makes the query correct whether or not it holds.
SELECT_SQL = (
    "SELECT id, name, status, dispatch_mode FROM tasks "
    f"WHERE status IN ({','.join('?' * len(BACKFILL_STATUSES))}) "
    "  AND COALESCE(dispatch_mode, '') <> 'pool' "
    "ORDER BY status, id"
)
UPDATE_SQL = (
    "UPDATE tasks SET dispatch_mode = 'pool' "
    f"WHERE status IN ({','.join('?' * len(BACKFILL_STATUSES))}) "
    "  AND COALESCE(dispatch_mode, '') <> 'pool'"
)


def snapshot_downloading(conn):
    """Mode of every in-flight task, for the before/after diff A3 asks for."""
    rows = conn.execute(
        "SELECT id, dispatch_mode FROM tasks WHERE status = 'downloading' "
        "ORDER BY id"
    ).fetchall()
    return {r["id"]: r["dispatch_mode"] for r in rows}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="actually write; without it the script only reports")
    args = ap.parse_args()

    init_db()
    conn = _conn()

    before = snapshot_downloading(conn)
    print(f"downloading rows (must not change): {len(before)}")
    for task_id, mode in before.items():
        print(f"  {task_id}  {mode}")

    candidates = [dict(r) for r in conn.execute(SELECT_SQL, BACKFILL_STATUSES)]
    print(f"\n{len(candidates)} row(s) to move to pool:")
    for row in candidates:
        print(f"  {row['id']}  {row['status']:<10} "
              f"{row['dispatch_mode']!r} -> 'pool'   {row['name']}")

    if not args.apply:
        print("\nDRY RUN — nothing written. Re-run with --apply to commit.")
        return 0

    if not candidates:
        print("\nNothing to do.")
        return 0

    cur = conn.execute(UPDATE_SQL, BACKFILL_STATUSES)
    conn.commit()
    print(f"\nUPDATE affected {cur.rowcount} row(s).")

    after = snapshot_downloading(conn)
    if after != before:
        # Loud and non-zero: a backfill that moved a running task is an
        # incident, not a warning. Nothing here can undo it, so the only
        # useful action is to stop and show exactly what changed.
        print("\n*** downloading rows CHANGED — this must not happen ***",
              file=sys.stderr)
        for task_id in sorted(set(before) | set(after)):
            if before.get(task_id) != after.get(task_id):
                print(f"  {task_id}: {before.get(task_id)!r} -> "
                      f"{after.get(task_id)!r}", file=sys.stderr)
        return 1
    print(f"downloading rows unchanged ({len(after)} checked).")

    remaining = conn.execute(SELECT_SQL, BACKFILL_STATUSES).fetchall()
    if remaining:
        print(f"\n*** {len(remaining)} row(s) still not on pool ***",
              file=sys.stderr)
        return 1
    print("All pending/paused/failed rows are on pool.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
