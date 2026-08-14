"""Putting a task on hold, and taking it off.

A "hold" is a `paused` row that also carries `hold_reason` + `hold_detail`: it
says not just that the task is stopped but *what a human has to do* before it
can move. Today there is one reason, `needs_approval` — an HF repo whose gate
we are authenticated for but not authorised on.

This lives outside `routes/` because three callers need the same sequence and
none of them is the natural owner:

  * the add path (routes/tasks.py, routes/queue.py) — inserts an already-held
    row, so it needs `set_hold` only, not this module;
  * the worker report path (routes/servers.py `/api/missing-files`) — a batch
    that hit a 403 mid-run;
  * the scheduler's recheck loop — releases a hold when the gate opens.

Holding a *running* task is more than a status write. The coordinator workflow
keeps handing batches to workers until it is cancelled, and each one rediscovers
the same 403 and burns its three Temporal attempts — a pool batch that exhausts
them is permanently `failed` and never re-dispatched. That is the exact loss
this whole feature exists to stop (assembly101 lost 20 of 113 batches in one
approval window), so the cancel is part of the hold, not a follow-up someone
might forget.
"""

from __future__ import annotations

import logging

logger = logging.getLogger("dlm.web")

NEEDS_APPROVAL = "needs_approval"

# Only these can be held. A `done`/`failed`/`revoked` row is history, and a
# report arriving against one is a zombie activity (the same reasoning as
# /api/task-progress's TERMINAL guard); an already-`paused` row is either held
# already or paused by an operator, and neither wants this overwriting it.
HOLDABLE_STATUSES = ("downloading", "pending")


async def hold_for_approval(task_id: str, detail: str) -> bool:
    """Stop `task_id` and mark it as waiting for a human to click Agree.

    Returns True if this call is what held it, False if it was not in a
    holdable status (already held, already finished, or revoked).

    Steps, in this order for a reason: the status flip lands first so that a
    dispatch cycle racing this call sees `paused` and skips the task, then the
    workflow cancel stops what is already in flight, then the in-flight batch
    rows are released so nothing claims a worker that has stopped working on
    it. That is the same sequence /queue/pause uses, and it reuses the same two
    helpers rather than reimplementing either.
    """
    from ..queue import snapshot
    from .routes import run_blocking

    def _flip():
        """-> (held, dispatch_mode). A tuple rather than just the mode: `None`
        is a legitimate dispatch_mode on an old row, so returning it alone
        cannot distinguish "not holdable" from "held, mode unset" — and reading
        that as "not holdable" would skip the cancel on exactly the rows least
        likely to have been tested."""
        snapshot.init_db()
        task = snapshot.get_task(task_id)
        if not task or task.get("status") not in HOLDABLE_STATUSES:
            return False, None
        snapshot.update_task_progress(task_id, status="paused", phase=None,
                                      speed_mbps=0)
        snapshot.set_hold(task_id, NEEDS_APPROVAL, detail)
        return True, task.get("dispatch_mode")

    held, dispatch_mode = await run_blocking(_flip)
    if not held:
        return False

    from .temporal_client import cancel_workflow
    try:
        await cancel_workflow(task_id, dispatch_mode=dispatch_mode)
    except Exception as e:
        # The row is already `paused` + held, which is the part an operator
        # sees and the part that stops the next dispatch cycle. A Temporal
        # hiccup here must not undo it: leaving the task `downloading` because
        # the cancel failed is the worst of the three outcomes.
        logger.error("hold_for_approval: cancel_workflow failed for %s: %s: %s",
                     task_id, type(e).__name__, e)

    if dispatch_mode == "pool":
        from .routes.queue import release_pool_batches
        try:
            await release_pool_batches({"task_id": task_id})
        except Exception as e:
            logger.error("hold_for_approval: batch release failed for %s: "
                         "%s: %s", task_id, type(e).__name__, e)

    logger.warning("Task %s held for approval: %s", task_id, detail)
    return True


def release_to_pending(task_id: str, expect_status: str | None = None) -> bool:
    """The blocking half of taking a task off hold: back to `pending`, hold
    cleared, batch rows dropped. Callers must already be off the event loop.
    Returns True if the row was flipped.

    Shared by /queue/resume (the "已审批，继续" button), the recheck loop below,
    and reconciler.return_preempted_tasks so the three cannot drift — a
    preempted victim coming back needs exactly this sequence, hold or no hold.
    Dropping the batch rows is required, not cosmetic: chunking recomputes batch
    boundaries on the next dispatch and `create_pool_batches_in_db` refuses a
    request whose row set disagrees with what is on file, so stale rows would
    make the very next dispatch error out.

    `expect_status` skips the flip when the row has moved on since the caller
    read it. The two hold callers pass nothing — they read the row in the same
    breath. The reconciler passes 'preempted', because its candidate list is a
    whole cycle old by the time it writes here.
    """
    from ..queue import snapshot

    if expect_status is not None:
        current = (snapshot.get_task(task_id) or {}).get("status")
        if current != expect_status:
            return False

    snapshot.update_task_progress(task_id, status="pending", phase="resuming",
                                  speed_mbps=0, clear_error=True)
    snapshot.delete_shards_by_task(task_id)
    # A hold explains why a row is paused; once it is pending the explanation
    # is stale, and leaving it set would keep the approval banner on a task
    # that is downloading fine.
    snapshot.clear_hold(task_id)
    return True


# Per-cycle cap on rechecks. Each probe costs up to preflight.TOTAL_BUDGET_S
# (10s) in a thread, and they run in sequence — five is what fits inside the
# stage deadline the scheduler gives this. `get_held_tasks` orders by oldest
# check first, so the cap rotates rather than starving the tail.
RECHECK_LIMIT = 5


async def recheck_holds(limit: int = RECHECK_LIMIT) -> dict:
    """Re-probe every needs-approval hold and release the ones that now pass.

    This is what makes the hold self-clearing: an operator clicks Agree on the
    HuggingFace page and the task resumes on its own within one interval, with
    no button press. The manual button stays — a probe can be wrong (UNKNOWN on
    an HF hiccup, 401 on a token problem) and an operator who knows the gate is
    open must not have to wait for a measurement to agree.

    Only OK releases. NEEDS_APPROVAL is the expected steady state (still
    waiting), and NOT_FOUND / UNKNOWN are refusals to conclude — releasing on
    either would hand a task back to the fleet on no evidence. A source this
    module does not probe at all is skipped before that test, because there OK
    means "no opinion" rather than "reachable". Everything that is not released
    gets `touch_hold_check`, which is also what stops the cap above from
    re-probing the same five for ever.
    """
    from ..queue import snapshot
    from . import preflight
    from .routes import run_blocking

    def _fetch():
        snapshot.init_db()
        # The full held set, not `limit + 1`. A limit+1 probe answers "is there
        # more" but makes `truncated` below permanently 1 no matter how many are
        # actually waiting — and a log line reading "1 beyond the cap" when nine
        # are is the same lie as reporting a capped sweep as a complete one. The
        # read is a handful of rows from a local SQLite file; get_held_tasks'
        # own default (50) is the real ceiling.
        return snapshot.get_held_tasks(NEEDS_APPROVAL)

    held = await run_blocking(_fetch)
    report: dict = {"checked": 0, "released": [], "still_held": 0, "truncated": 0}
    if not held:
        return report

    if len(held) > limit:
        # Say so rather than silently covering `limit` of N: a capped sweep
        # that reads as a complete one is how a task sits held for hours with
        # nothing in the log to explain it.
        report["truncated"] = len(held) - limit
        logger.info("Hold recheck: %d held task(s) beyond this cycle's cap of "
                    "%d; they rotate in next cycle", report["truncated"], limit)
        held = held[:limit]

    for task in held:
        task_id = task.get("id")

        def _touch(tid=task_id):
            snapshot.init_db()
            snapshot.touch_hold_check(tid)

        if (task.get("source") or "") != preflight.PROBED_SOURCE:
            # Nothing to re-probe. check_repo_access answers OK for every source
            # it does not probe, and OK is the release condition below — so a
            # ModelScope task held by the 403 path (which is source-blind, and
            # right to be) would be handed back every cycle, rediscover the same
            # refusal, and burn a batch's three attempts each time. These holds
            # are human-only; say so in the report rather than silently counting
            # them as checked.
            await run_blocking(_touch)
            report["still_held"] += 1
            report.setdefault("unprobeable", []).append(task_id)
            continue

        result = await preflight.check_repo_access(
            task.get("repo_id") or "", task.get("source") or "hf",
            task.get("type") or "dataset")
        report["checked"] += 1

        if result.outcome == preflight.OK:
            def _release(tid=task_id):
                snapshot.init_db()
                release_to_pending(tid)
            await run_blocking(_release)
            report["released"].append(task_id)
            logger.warning("Task %s released from approval hold: gate is open "
                           "(%s)", task_id, result.detail)
        else:
            await run_blocking(_touch)
            report["still_held"] += 1

    return report
