"""Post the imports and check them afterwards. The scheduler's two transfer stages.

`dispatch_ready_transfers()` takes `ready` rows and turns them into running
remote imports; `poll_transfers()` takes running imports to `done` / `short` /
`failed`. They are separate stages on purpose: dispatching does the expensive
BOS scan and can be starved by a slow far side, while polling must keep running
so an import that finished does not sit in `transferring` forever.

Concurrency: 16 imports in flight, at most 4 newly posted per cycle. 16 is the
user's call (2026-08-10) — the far side's bandwidth is tens of Gb/s, and it does
the copying, so our limit is politeness rather than throughput. The per-cycle
cap exists for a different reason: each new import costs a full BOS prefix scan
(~50 list pages for a 3.4 TB dataset), and a cycle that posted all 16 at once
would hold a thread-pool slot for minutes. Ramping 4 at a time reaches 16 in one
minute anyway.

Everything the far side is told to read comes from `tasks.transfer_prefix`,
recorded when the task was armed — never re-derived here, and never
`tasks.bos_path` (measured 2026-08-10: molmobot-data's column holds a 地瓜云
destination path). If the derived prefix has moved since arming, that is a
rename after the fact and the row is blocked, not transferred to the new place.

Failure handling is deliberately asymmetric:

  - A row whose dispatch attempt fails stays `ready` with the reason on it, so
    the next cycle retries. Nothing was posted, so nothing is inconsistent.
  - Two consecutive dispatch failures stop the cycle. That is the far side (or
    our credentials) refusing us, and 14 more attempts would only make noise —
    same rule, same reason, as `scripts/transfer_import.py`.
  - A remote import that reports failure becomes `failed` and stays there. It
    needs a human to decide whether to retry, and the manual button is how.
  - A row whose remote record we can no longer find (the list is capped at the
    newest 500) is measured instead of guessed: a complete target lands `done`,
    an incomplete one stays `transferring` with the shortfall recorded. Nothing
    here writes `short` on the strength of a missing record.
"""

import logging
import os
import time
from dataclasses import replace

from ..core.bos import create_bos_client
from ..core.config import load_config
from ..queue import snapshot
from . import inflight
from .arm import IN_FLIGHT, band_ratio, transfers_paused
from .dcloud import DCloudClient
from .measure import bos_stats
from .targets import plan_from_mapping
from .verify import verify_transfer

logger = logging.getLogger("dlm.transfer")

MAX_IN_FLIGHT = 16
MAX_PER_CYCLE = 4

# Dispatch failures in a row before the cycle gives up. Mirrors
# CONSECUTIVE_ITEM_FAIL_LIMIT in scripts/transfer_import.py.
CONSECUTIVE_FAIL_LIMIT = 2

# In-flight rows whose remote record we cannot find that get measured in one
# poll pass. `inflight.fetch_tasks` reads the newest 500 records, which as
# measured on 2026-08-10 reach back about six weeks — so an import falls off
# this window only if 500 newer ones are posted while it runs. Uncommon, but the
# cost of assuming it cannot happen is a row sitting in `transferring` forever,
# holding one of the 16 slots and raising nothing. Bounded because each
# measurement is a full walk of both ends, and the whole stage has 600s.
UNKNOWN_VERIFY_PER_CYCLE = 2


def _clients():
    """`(bos, dcloud, cfg)`, or None when the credentials are not present.

    Returning None rather than raising: a web process started without
    DCLOUD_USER/DCLOUD_PASS should serve the dashboard and skip transfers, which
    is exactly what the old poller did.
    """
    user = os.environ.get("DCLOUD_USER")
    password = os.environ.get("DCLOUD_PASS")
    if not user or not password:
        return None
    cfg = load_config()
    bos = create_bos_client(cfg["BAIDU_AK"], cfg["BAIDU_SK"], cfg["BOS_ENDPOINT"])
    dcloud = DCloudClient(user, password)
    dcloud.login()
    return bos, dcloud, cfg


def plan_for_row(row) -> object:
    """The transfer plan for a DB row, with the source taken verbatim from
    `transfer_prefix`.

    The destination is always derived (`plan_from_mapping`); only the source is
    read off the row. That asymmetry is the point: the source is a fact about
    where the bytes were measured and must not drift, while the destination is a
    naming rule with exactly one owner.
    """
    plan = plan_from_mapping(row)
    stored = row.get("transfer_prefix") if hasattr(row, "get") else None
    if stored:
        bucket, _, prefix = str(stored).partition("/")
        if bucket and prefix:
            plan = replace(plan, bucket=bucket, prefix=prefix)
    return plan


def _rows_with_status(*statuses) -> list:
    marks = ",".join("?" for _ in statuses)
    rows = snapshot._conn().execute(
        f"SELECT * FROM tasks WHERE transfer_status IN ({marks}) "
        f"ORDER BY transfer_armed_at ASC, id ASC", statuses).fetchall()
    return [dict(r) for r in rows]


def _update(task_id: str, **cols):
    cols["updated_at"] = time.time()
    assignments = ", ".join(f"{k} = ?" for k in cols)
    conn = snapshot._conn()
    conn.execute(f"UPDATE tasks SET {assignments} WHERE id = ?",
                 (*cols.values(), task_id))
    conn.commit()


def _dispatch_one(bos, dcloud, cfg, remote_tasks, row) -> dict:
    """Measure, gate, post. Returns a report fragment for one row.

    Any exception propagates to the caller, which counts it toward the
    consecutive-failure limit and leaves the row `ready`.
    """
    task_id, name = row["id"], row.get("name") or row["id"]
    plan = plan_for_row(row)
    source = f"{plan.bucket}/{plan.prefix}"

    # Renamed after arming: `name`/`category` now resolve somewhere else. Arm's
    # gate 3 only saw the state at completion time, so this is the same check
    # one step later — cheap, pure, and the alternative is silently importing
    # into a directory nobody asked for.
    derived = plan_from_mapping(row).source
    if row.get("transfer_prefix") and derived != source:
        reason = (f"prefix drift: armed against {source!r} but name/category "
                  f"now resolve to {derived!r} — renamed after arming")
        _update(task_id, transfer_status="blocked", transfer_error=reason)
        logger.error(f"transfer blocked: {name} — {reason}")
        return {"blocked": {"task_id": task_id, "name": name, "reason": reason}}

    bos_bytes, bos_objects = bos_stats(bos, plan.bucket, plan.prefix)
    _update(task_id, transfer_bos_bytes=bos_bytes,
            transfer_bos_objects=bos_objects)

    status, severity, note = band_ratio(bos_bytes, int(row.get("transfer_bytes") or 0))
    if status == "blocked":
        _update(task_id, transfer_status="blocked", transfer_error=note)
        logger.error(f"transfer blocked: {name} {source} — {note}")
        return {"blocked": {"task_id": task_id, "name": name, "reason": note}}
    if severity:
        logger.warning(f"transfer {name} {source}: {note}")

    # Never post over an import that is already running against this
    # (source, target) — see inflight.py for the two days that cost us.
    # `endpoint_source` and `import_from_bos` are both left on their default
    # endpoint so the string we search for is the string the far side recorded.
    running = inflight.find_running(
        remote_tasks,
        source=inflight.endpoint_source(plan.bucket, plan.prefix),
        target=plan.target,
        task_id=row.get("transfer_task_id"),
    )
    if running is not None:
        remote_id = running.get("task_id")
        reattached = True
    else:
        try:
            root, leaf = plan.parent.rsplit("/", 1)
            dcloud.create_folder(root + "/", leaf)
        except Exception:
            pass  # already exists, or the import will create the path itself
        remote_id = dcloud.import_from_bos(
            bos_ak=cfg["BAIDU_AK"], bos_sk=cfg["BAIDU_SK"],
            bos_bucket=plan.bucket, bos_path=plan.prefix,
            target_path=plan.target,
        )
        reattached = False

    # Committed immediately, before anything else happens: if this write is lost
    # the next cycle finds the remote task still running and re-attaches to it
    # rather than posting a second import over the same directory.
    _update(task_id, transfer_status="transferring", transfer_task_id=remote_id,
            transfer_error=None)
    logger.info(f"transfer {'re-attached' if reattached else 'dispatched'}: "
                f"{name} {source} -> {plan.target} (remote {remote_id}, "
                f"{bos_bytes} bytes / {bos_objects} objects, {note})")
    return {"dispatched": {"task_id": task_id, "name": name,
                           "remote_task_id": remote_id,
                           "reattached": reattached,
                           "bos_bytes": bos_bytes, "note": note}}


def dispatch_ready_transfers() -> dict:
    """Post imports for up to `MAX_PER_CYCLE` armed rows. Blocking; runs on the
    scheduler's thread pool."""
    report = {"dispatched": [], "blocked": [], "errors": [],
              "in_flight": 0, "quota": 0}
    snapshot.init_db()

    if transfers_paused():
        report["skipped"] = "transfers are paused"
        return report

    in_flight = len(_rows_with_status(*IN_FLIGHT))
    report["in_flight"] = in_flight
    quota = min(MAX_IN_FLIGHT - in_flight, MAX_PER_CYCLE)
    report["quota"] = max(quota, 0)
    if quota <= 0:
        return report

    ready = _rows_with_status("ready")[:quota]
    if not ready:
        return report

    clients = _clients()
    if clients is None:
        report["errors"].append("DCLOUD_USER/DCLOUD_PASS not set — "
                                "no transfer can be posted")
        return report
    bos, dcloud, cfg = clients

    try:
        remote_tasks = inflight.fetch_tasks(dcloud)
    except Exception as e:
        # Without the remote list we cannot tell a re-post from a first post,
        # and posting blind is the one failure mode this feature was built to
        # avoid. Give up on the cycle; nothing has been written.
        report["errors"].append(f"could not list remote async tasks: {e}")
        logger.error(f"transfer dispatch: remote task list failed: {e}")
        return report

    consecutive = 0
    for index, row in enumerate(ready):
        try:
            fragment = _dispatch_one(bos, dcloud, cfg, remote_tasks, row)
            consecutive = 0
        except Exception as e:  # noqa: BLE001 — one row must not kill the cycle
            consecutive += 1
            name = row.get("name") or row["id"]
            message = f"dispatch failed: {e}"
            # Stays `ready`: nothing was posted, so the next cycle simply tries
            # again. The reason is on the row so the dashboard shows it.
            try:
                _update(row["id"], transfer_error=message)
            except Exception:
                pass
            report["errors"].append(f"{name}: {message}")
            logger.error(f"transfer dispatch failed for {name} "
                         f"({consecutive}/{CONSECUTIVE_FAIL_LIMIT}): {e}")
            if consecutive >= CONSECUTIVE_FAIL_LIMIT:
                report["errors"].append(
                    f"{consecutive} dispatch failures in a row — stopping this "
                    f"cycle with {len(ready) - index - 1} row(s) untried")
                break
            continue
        for key, value in fragment.items():
            report[key].append(value)

    return report


def _verify_and_write(bos, dcloud, row, only_if_clean: bool = False) -> dict:
    """Run the three checks and record the verdict. Idempotent — re-running it
    changes nothing but the timestamp, which is what lets a row that died
    mid-verification simply be verified again.

    `only_if_clean` is for a row whose remote record we can no longer find. A
    complete target is proof enough to land `done`; an incomplete one is NOT
    proof of failure — "still importing, record aged off the list" and "the
    import died" measure identically, and writing `short` on a live import
    would invite a human to post a second importer onto the same directory. So
    an incomplete measurement only records what it saw and leaves the row where
    it was, to be measured again next pass.
    """
    task_id, name = row["id"], row.get("name") or row["id"]
    plan = plan_for_row(row)
    verdict = verify_transfer(
        bos, dcloud, plan,
        int(row.get("transfer_bos_bytes") or 0),
        int(row.get("transfer_bos_objects") or 0),
    )
    if only_if_clean and verdict["status"] != "done":
        _update(task_id, transfer_error=f"remote record not found; {verdict['detail']}")
        logger.warning(f"transfer in limbo: {name} — remote record not found and "
                       f"the target is not complete yet: {verdict['detail']}")
        return verdict
    # A clean `done` carries no error text — but a `done` whose source prefix
    # changed underneath it keeps the note, because that is the one thing about
    # a successful transfer somebody still needs to look at.
    keep_note = verdict["status"] != "done" or verdict["bos_changed"]
    _update(task_id, transfer_status=verdict["status"],
            transfer_verified_bytes=int(verdict["jfs_bytes"] or 0),
            transfer_error=verdict["detail"] if keep_note else None)
    if verdict["status"] == "done":
        logger.info(f"transfer verified: {name} — {verdict['detail']}")
    else:
        logger.error(f"transfer SHORT: {name} — {verdict['detail']}")
    return verdict


def poll_transfers() -> dict:
    """Advance in-flight transfers: remote success → verify → `done` / `short`,
    remote failure → `failed`. Blocking; runs on the scheduler's thread pool."""
    report = {"done": [], "short": [], "failed": [], "errors": [],
              "running": 0, "unknown_remote": 0}
    snapshot.init_db()

    rows = _rows_with_status(*IN_FLIGHT)
    if not rows:
        return report

    clients = _clients()
    if clients is None:
        report["errors"].append("DCLOUD_USER/DCLOUD_PASS not set")
        return report
    bos, dcloud, _cfg = clients

    try:
        remote_tasks = inflight.fetch_tasks(dcloud)
    except Exception as e:
        report["errors"].append(f"could not list remote async tasks: {e}")
        logger.error(f"transfer poll: remote task list failed: {e}")
        return report
    by_id = {t.get("task_id"): t for t in remote_tasks}
    unknown_measured = 0

    for row in rows:
        task_id, name = row["id"], row.get("name") or row["id"]
        try:
            # `verifying` needs no remote lookup at all: the far side already
            # said it finished, and what is owed is a measurement. This is how
            # a process that died mid-verification recovers — including after
            # the remote record has aged off the list.
            if row.get("transfer_status") != "verifying":
                remote_id = row.get("transfer_task_id")
                remote = by_id.get(remote_id) if remote_id else None
                if remote is None:
                    report["unknown_remote"] += 1
                    if unknown_measured >= UNKNOWN_VERIFY_PER_CYCLE:
                        continue
                    unknown_measured += 1
                    verdict = _verify_and_write(bos, dcloud, row, only_if_clean=True)
                    if verdict["status"] == "done":
                        report["done"].append(
                            {"task_id": task_id, "name": name,
                             "jfs_bytes": verdict["jfs_bytes"],
                             "detail": f"remote record not found; {verdict['detail']}"})
                    continue
                state = inflight.classify(remote.get("status"))
                if state == "running":
                    report["running"] += 1
                    continue
                if state == "failed":
                    error = remote.get("error_msg") or remote.get("status") or "failed"
                    _update(task_id, transfer_status="failed",
                            transfer_error=str(error))
                    report["failed"].append({"task_id": task_id, "name": name,
                                             "reason": str(error)})
                    logger.error(f"transfer FAILED on the far side: {name} — {error}")
                    continue
                # Remote success. Recorded before the checks run, so the row
                # never reads as "still copying" while we are measuring.
                _update(task_id, transfer_status="verifying")
                row = {**row, "transfer_status": "verifying"}

            verdict = _verify_and_write(bos, dcloud, row)
            bucket = "done" if verdict["status"] == "done" else "short"
            report[bucket].append({"task_id": task_id, "name": name,
                                   "jfs_bytes": verdict["jfs_bytes"],
                                   "detail": verdict["detail"]})
        except Exception as e:  # noqa: BLE001 — one row must not kill the cycle
            report["errors"].append(f"{name}: {e}")
            logger.error(f"transfer poll failed for {name}: {e}")

    return report


__all__ = ["CONSECUTIVE_FAIL_LIMIT", "MAX_IN_FLIGHT", "MAX_PER_CYCLE",
           "UNKNOWN_VERIFY_PER_CYCLE",
           "dispatch_ready_transfers", "plan_for_row", "poll_transfers"]
