"""Decide whether a finished download may be transferred, and queue it.

Arming is a pure database decision. It runs inside `/api/task-progress`, the
request a coordinator workflow makes to report its own completion, so it must
not issue a single byte of external I/O: a BOS prefix scan of a 3.4 TB dataset
is thousands of `list_objects` pages, and parking a worker's completion report
behind that would stall the workflow that sent it. Everything expensive —
measuring BOS, posting the import, verifying the far side — belongs to the
scheduler's transfer stages, which run on the thread pool under a deadline.

Why the trigger is here and not in `complete_task()`: two paths reach `done`,
and only one of them means a workflow said so. The other is
`reconciler.py`'s inference from shard rows, which is exactly how
`t-20260805-460d45` became a `done` task that had downloaded nothing.
`completed_at` cannot tell them apart, so the distinction has to be structural
— arming is called from the one route a workflow reports through, and the
reconciler's inferred `done` never arms.

Two outcomes are NOT the same thing:

  - **skip** — nothing was written. The task is not `done` yet, was already
    armed, or transfers are paused. Silent by design, and re-callable.
  - **blocked** — the gates say this `done` is not believable. Written to the
    row with the reason, so it shows up in the dashboard's transfer column and
    (phase three) raises a CRITICAL alert. Never silent: a false `done` that
    quietly did nothing is indistinguishable from a task nobody finished yet.

Nothing here ever writes the task's own `status`. That was the old Celery
transfer's central bug (`dlm/transfer/tasks.py` set `status="transferring"`,
whose residue is still the `"transferring": "done"` patch in the frontend
mapper): download state and transfer state are orthogonal, and conflating them
made a transfer failure look like a download failure.
"""

import logging
import os
import time

from ..core.bos import bos_target  # noqa: F401 — re-exported for the dispatcher
from ..queue import snapshot
from .targets import plan_from_mapping

logger = logging.getLogger("dlm.transfer")

PAUSED_KEY = "transfer_paused"

# A transfer that is already moving must not be re-queued by anyone, operator
# included: the far side would then hold two imports writing one directory.
IN_FLIGHT = ("transferring", "verifying")

# Bands for `ratio = bos_bytes / transfer_bytes`, measured by the dispatcher
# (arm has no BOS access — see the module docstring).
#
# Neither threshold is arbitrary. Across all seven shard-era `done` tasks the
# TIGHTEST real ratio is exactly 1.0000, and that one row is the only task that
# never resumed — a resumed task's prefix carries earlier rounds' bytes, so its
# ratio runs 1.02–8.65. Meanwhile every known false `done` is off by ~100×
# (0.0002–0.013). So moving the floor from 0.999 down to 0.50 costs no
# detection at all and removes the risk of rejecting a real completion.
RATIO_READY = float(os.environ.get("DLM_TRANSFER_RATIO_READY", "0.95"))
RATIO_MIN = float(os.environ.get("DLM_TRANSFER_RATIO_MIN", "0.50"))


def auto_transfer_enabled() -> bool:
    return os.environ.get("DLM_AUTO_TRANSFER", "on").strip().lower() not in (
        "0", "off", "false", "no")


def transfers_paused() -> bool:
    """The operator's pause switch, persisted (it used to live in `cache`, so a
    web restart silently resumed everything)."""
    return str(snapshot.get_setting(PAUSED_KEY, "0")).strip().lower() in (
        "1", "true", "yes", "on", "paused")


def set_transfers_paused(paused: bool):
    snapshot.set_setting(PAUSED_KEY, "1" if paused else "0")


def band_ratio(bos_bytes: int, dispatched_bytes: int):
    """`(transfer_status, alert_severity, note)` for a measured BOS prefix.

    Consumed by the dispatcher, which is where the BOS scan happens. The middle
    band still transfers: the far side's import is a read-only pull with
    skip-same semantics, so topping up 97% later is cheap, while stalling a real
    completion needs a human and teaches people to distrust the automation. It
    is never silent, though — the middle band always alerts.

    `dispatched_bytes == 0` is legal, not an error: a fully-resumed task has
    every file filtered out before dispatch (`AgiBotWorld-Alpha` — 0 dispatched,
    9000.8 GB on BOS). The ratio is meaningless there, so a non-empty prefix is
    the whole test.
    """
    if bos_bytes <= 0:
        return "blocked", "critical", "BOS prefix is empty (0 bytes)"
    if dispatched_bytes <= 0:
        return "ready", None, (
            f"dispatched 0 bytes (fully resumed); BOS has {bos_bytes} bytes")
    ratio = bos_bytes / dispatched_bytes
    if ratio >= RATIO_READY:
        return "ready", None, f"ratio {ratio:.4f}"
    if ratio >= RATIO_MIN:
        return "ready", "warning", (
            f"ratio {ratio:.4f} — BOS has {bos_bytes} bytes, "
            f"{dispatched_bytes - bos_bytes} short of the {dispatched_bytes} "
            f"dispatched; transferring anyway")
    return "blocked", "critical", (
        f"ratio {ratio:.4f} — BOS has {bos_bytes} bytes against "
        f"{dispatched_bytes} dispatched; refusing to transfer")


def _shard_verdict(task_id: str):
    """`(ok, detail)` for gate 4. Pool batches and sharded shards share the
    `shards` table, so this needs no per-mode branch — and `release_pool_batches`
    returning non-done rows to `pending` is what makes a cancelled pool task
    fail this gate on its own."""
    shards = snapshot.get_shards_by_task(task_id)
    if not shards:
        return False, "0 shard rows — nothing proves what was downloaded"
    not_done = [s for s in shards if s.get("status") != "done"]
    if not_done:
        return False, (f"{len(not_done)}/{len(shards)} shard rows are not done "
                       f"({sorted({s.get('status') for s in not_done})})")
    return True, f"{len(shards)} shard rows all done"


def dispatched_bytes(task_id: str) -> int:
    return sum(int(s.get("total_bytes") or 0)
               for s in snapshot.get_shards_by_task(task_id))


def _write(task_id: str, status: str, error, prefix=None, nbytes=0):
    now = time.time()
    conn = snapshot._conn()
    conn.execute(
        "UPDATE tasks SET transfer_status = ?, transfer_error = ?, "
        "transfer_prefix = COALESCE(?, transfer_prefix), transfer_bytes = ?, "
        "transfer_armed_at = ?, updated_at = ? WHERE id = ?",
        (status, error, prefix, nbytes, now, now, task_id),
    )
    conn.commit()


def maybe_arm_transfer(task_id: str, manual: bool = False) -> dict:
    """Queue `task_id` for transfer if every gate passes.

    `manual=True` (the dashboard's trigger / retry buttons) skips only the
    "never armed" gate — an operator asking again for a task that failed or was
    blocked is the point of those buttons. The believability gates are not
    skippable by either caller.

    Returns `{"armed": bool, "status": <written or None>, "reason": str}`.
    Callers treat this as advisory: arming must never break the progress report
    it hangs off.
    """
    task = snapshot.get_task(task_id)
    if not task:
        return {"armed": False, "status": None, "reason": "unknown task"}

    if task.get("status") != "done":
        return {"armed": False, "status": None,
                "reason": f"task status is {task.get('status')!r}, not done"}

    if not manual and not auto_transfer_enabled():
        return {"armed": False, "status": None,
                "reason": "DLM_AUTO_TRANSFER is off"}

    # Gate 1 — never armed. `complete_task()` has no idempotence guard, so a
    # repeated `done` report must not queue the same task twice. A manual
    # trigger deliberately re-queues a `failed`/`blocked`/`short` row, but not
    # one whose import is still running on the far side.
    if task.get("transfer_status") in IN_FLIGHT:
        return {"armed": False, "status": None,
                "reason": f"transfer is {task['transfer_status']} — "
                          f"already in flight"}
    if not manual and task.get("transfer_status") is not None:
        return {"armed": False, "status": None,
                "reason": f"already transfer_status={task['transfer_status']!r}"}

    # Gate 2 — not paused.
    if transfers_paused():
        return {"armed": False, "status": None, "reason": "transfers are paused"}

    plan = plan_from_mapping(task)

    # Gate 3 — the prefix has not moved since dispatch. NULL means the row
    # predates the column; the byte-ratio band backstops it. Rejecting on
    # absence would block every pre-existing task for a fact we never recorded.
    dispatched_prefix = task.get("dispatch_prefix")
    if dispatched_prefix and dispatched_prefix != plan.source:
        reason = (f"prefix drift: dispatched to {dispatched_prefix!r} but "
                  f"name/category now resolve to {plan.source!r} — someone "
                  f"renamed the task after it ran")
        _write(task_id, "blocked", reason)
        return {"armed": False, "status": "blocked", "reason": reason}

    # Gate 4 — the shard rows account for the whole task.
    ok, detail = _shard_verdict(task_id)
    if not ok:
        _write(task_id, "blocked", detail)
        return {"armed": False, "status": "blocked", "reason": detail}

    nbytes = dispatched_bytes(task_id)
    _write(task_id, "ready", None, prefix=plan.source, nbytes=nbytes)
    logger.info(f"transfer armed: {task_id} ({task.get('name')}) "
                f"{plan.source} -> {plan.target}, {detail}, {nbytes} bytes")
    return {"armed": True, "status": "ready",
            "reason": f"{detail}; {nbytes} bytes dispatched"}


def arm_quietly(task_id: str) -> dict:
    """`maybe_arm_transfer` that cannot raise. For the progress-report path,
    where a transfer bookkeeping bug must not fail a worker's `done` report."""
    try:
        return maybe_arm_transfer(task_id)
    except Exception as exc:  # noqa: BLE001 — advisory by contract
        logger.error(f"maybe_arm_transfer({task_id}) failed: {exc}")
        return {"armed": False, "status": None, "reason": f"error: {exc}"}


__all__ = [
    "PAUSED_KEY", "RATIO_MIN", "RATIO_READY", "arm_quietly",
    "auto_transfer_enabled", "band_ratio", "bos_target", "dispatched_bytes",
    "maybe_arm_transfer", "set_transfers_paused", "transfers_paused",
]
