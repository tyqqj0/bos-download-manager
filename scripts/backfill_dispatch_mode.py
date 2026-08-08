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
  * `--exclude-task ID` holds a row back. The cutover plan's Phase 3 needs it:
    the two tasks paused for the deploy get their mode from an individual
    reshard, which is the step behind the operator approval gate, and a
    backfill that flipped them first would have moved them before that gate
    (review GAP-2).

Run on S1 (the only host with the database):

    python3 scripts/backfill_dispatch_mode.py            # dry run, default
    python3 scripts/backfill_dispatch_mode.py --apply
    python3 scripts/backfill_dispatch_mode.py --exclude-task t-a --exclude-task t-b --apply
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
_WHERE = (
    f"WHERE status IN ({','.join('?' * len(BACKFILL_STATUSES))}) "
    "  AND COALESCE(dispatch_mode, '') <> 'pool' "
)
SELECT_SQL = (
    "SELECT id, name, status, dispatch_mode FROM tasks " + _WHERE
    + "ORDER BY status, id"
)
UPDATE_SQL = "UPDATE tasks SET dispatch_mode = 'pool' " + _WHERE


def _excluded_clause(exclude: list[str]) -> str:
    """`AND id NOT IN (...)` for the excluded ids, or nothing.

    Built as one string used by BOTH the select and the update, and appended
    to both with the same parameters, so a row can never be reported as held
    back by the preview and then written by the apply.
    """
    if not exclude:
        return ""
    return f"  AND id NOT IN ({','.join('?' * len(exclude))}) "


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
    ap.add_argument("--exclude-task", action="append", default=[],
                    metavar="TASK_ID",
                    help="hold this task id back (repeatable). Use for tasks "
                         "whose mode an individual reshard will set.")
    args = ap.parse_args()

    exclude = list(dict.fromkeys(args.exclude_task))
    select_sql = SELECT_SQL
    update_sql = UPDATE_SQL
    params = list(BACKFILL_STATUSES)
    if exclude:
        clause = _excluded_clause(exclude)
        # The select ends with ORDER BY, so the clause goes before it.
        select_sql = (
            "SELECT id, name, status, dispatch_mode FROM tasks " + _WHERE
            + clause + "ORDER BY status, id"
        )
        update_sql = UPDATE_SQL + clause
        params = list(BACKFILL_STATUSES) + exclude

    init_db()
    conn = _conn()

    before = snapshot_downloading(conn)
    print(f"downloading rows (must not change): {len(before)}")
    for task_id, mode in before.items():
        print(f"  {task_id}  {mode}")

    if exclude:
        print(f"\nheld back by --exclude-task ({len(exclude)}):")
        for task_id in exclude:
            row = conn.execute(
                "SELECT status, dispatch_mode FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            if row is None:
                # Not fatal: excluding an id that does not exist is a safe
                # no-op, and refusing would make a copy-pasted list of ids
                # useless the moment one of them was already cleaned up. Still
                # said out loud, because the usual cause is a typo — and a
                # typo'd exclusion silently protects nothing.
                print(f"  {task_id}  (NO SUCH TASK — exclusion does nothing)")
            else:
                print(f"  {task_id}  {row['status']:<10} "
                      f"stays {row['dispatch_mode']!r}")

    candidates = [dict(r) for r in conn.execute(select_sql, params)]
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

    cur = conn.execute(update_sql, params)
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

    remaining = conn.execute(select_sql, params).fetchall()
    if remaining:
        print(f"\n*** {len(remaining)} row(s) still not on pool ***",
              file=sys.stderr)
        return 1
    print("All pending/paused/failed rows are on pool"
          + (f" (excluding {len(exclude)} held back)." if exclude else "."))
    return 0


if __name__ == "__main__":
    sys.exit(main())
