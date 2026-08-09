"""Temporal client singleton for the web server.

Every call out to Temporal from here runs on the uvicorn event loop, so
every one of them needs a deadline. An await that never returns is not a
slow request — it stops the loop iteration it belongs to. The scheduler
loop reached through this module drives auto-dispatch, reconcile and
health verification, and a hang there leaves a process that answers HTTP
normally while nothing is dispatched or reconciled ever again.
"""

import asyncio
import logging
import os
import time
from datetime import timedelta
from typing import NamedTuple, Optional

from temporalio.client import Client

logger = logging.getLogger("dlm.web")

_client: Optional[Client] = None
_client_loop: Optional[asyncio.AbstractEventLoop] = None

# The download workflow types, in one place. Six sites used to inline this
# same list, and each was a place a new workflow type could be forgotten.
# `PoolDownloadWorkflow` is listed ahead of its class existing (T5 adds it) —
# tests/test_workflow_registry.py asserts the defn classes in workflows.py
# are a *subset* of this tuple, so listing it early is safe.
WORKFLOW_TYPES = (
    "DownloadDatasetWorkflow",
    "SplitDownloadWorkflow",
    "ShardedDownloadWorkflow",
    "ShardWorkerWorkflow",
    "PoolDownloadWorkflow",
)

# The coordinator/parent types — what fleet-wide cancel operates on. Children
# (ShardWorkerWorkflow) are NOT cancelled directly: they die with their parent
# via ParentClosePolicy=TERMINATE, which skips the child's failure handler.
# Cancelling a child directly runs that handler, the shard reports "failed",
# the coordinator marks the whole task `failed` — a terminal status the
# reconciler never re-dispatches. Parent-only cancel leaves the task in
# `downloading`, where orphan re-dispatch reclaims it losslessly.
PARENT_WORKFLOW_TYPES = tuple(t for t in WORKFLOW_TYPES if t != "ShardWorkerWorkflow")

# Workflow-ID prefix for each type — the second half of the registry. Three
# call sites (fleet.has_live_workflow, cancel_workflow,
# terminate_workflow_and_wait) used to hand-copy these same prefix literals,
# and each copy could drift independently (that history is why this table
# exists instead of a fourth copy). ShardWorkerWorkflow's IDs are built from
# a *shard* id, not a task id (`shard-` + naming.shard_row_id(task_id, idx)
# = f"shard-s-{task_id}-{idx}") — callers that need the task-id-keyed IDs
# should iterate PARENT_WORKFLOW_TYPES, not WORKFLOW_TYPES, against this map.
WORKFLOW_ID_PREFIXES = {
    "DownloadDatasetWorkflow": "dl-",
    "SplitDownloadWorkflow": "split-download-",
    "ShardedDownloadWorkflow": "sharded-",
    "ShardWorkerWorkflow": "shard-",
    "PoolDownloadWorkflow": "pool-",
}

# Named aliases for the two prefixes fleet.has_live_workflow needs outside a
# type-list iteration (the legacy single-node ID and the shard-child prefix).
# Importing these by name — instead of indexing WORKFLOW_ID_PREFIXES with a
# literal type-name string — keeps fleet.py free of workflow-type literals,
# which test_event_loop_safety.py's AST scan treats as a re-inlined copy of
# WORKFLOW_TYPES.
LEGACY_DOWNLOAD_ID_PREFIX = WORKFLOW_ID_PREFIXES["DownloadDatasetWorkflow"]
SHARD_WORKER_ID_PREFIX = WORKFLOW_ID_PREFIXES["ShardWorkerWorkflow"]
# Same reasoning, for the pool patrol's workflow_id construction
# (reconciler.inspect_pool_tasks) — it needs the PoolDownloadWorkflow prefix
# without re-inlining the type name outside this module.
POOL_DOWNLOAD_ID_PREFIX = WORKFLOW_ID_PREFIXES["PoolDownloadWorkflow"]

CONNECT_TIMEOUT = 5  # seconds to reach the frontend; connect() has no deadline
QUERY_TIMEOUT = timedelta(seconds=15)  # per list/describe RPC


async def get_client() -> Client:
    """Get or create the Temporal client connection.

    Creates a new client if the event loop has changed (e.g., called from
    a different context than where the client was first created).

    Prefer `connected_client()` — `Client.connect` takes no deadline of its
    own, so if the frontend completes the TCP handshake and then goes quiet
    this await never returns.
    """
    global _client, _client_loop
    current_loop = asyncio.get_running_loop()

    if _client is None or _client_loop is not current_loop:
        host = os.environ.get("TEMPORAL_HOST", "localhost:7233")
        logger.info(f"Connecting to Temporal at {host}...")
        _client = await Client.connect(host)
        _client_loop = current_loop

    return _client


async def connected_client(timeout: float = CONNECT_TIMEOUT) -> Client:
    """`get_client()` that cannot hang forever."""
    return await asyncio.wait_for(get_client(), timeout=timeout)


async def running_workflows(client: Optional[Client] = None) -> dict:
    """Every RUNNING download workflow, as `{workflow_id: task_queue}`.

    The callers want three different views of this — a set of ids, the set
    of busy task queues, and the id→queue mapping — so it returns the
    mapping and lets them narrow it. Every scan carries an rpc_timeout;
    none of the six hand-rolled copies did.
    """
    client = client or await connected_client()
    found: dict[str, str] = {}
    for wf_type in WORKFLOW_TYPES:
        async for wf in client.list_workflows(
            f'WorkflowType="{wf_type}" AND ExecutionStatus="Running"',
            rpc_timeout=QUERY_TIMEOUT,
        ):
            found[wf.id] = wf.task_queue
    return found


async def start_download(task_dict: dict, task_queue: str = "download-workers"):
    """Start a DownloadDatasetWorkflow for a task."""
    from ..temporal.models import TaskInput
    from ..temporal.workflows import DownloadDatasetWorkflow

    client = await connected_client()
    task_input = TaskInput(
        id=task_dict["id"],
        name=task_dict["name"],
        repo_id=task_dict["repo_id"],
        source=task_dict.get("source", "hf"),
        type=task_dict.get("type", "dataset"),
        category=task_dict.get("category", ""),
        priority=task_dict.get("priority", 5),
        size_gb=task_dict.get("size_gb", 0),
    )

    workflow_id = f"{WORKFLOW_ID_PREFIXES['DownloadDatasetWorkflow']}{task_dict['id']}"
    handle = await client.start_workflow(
        DownloadDatasetWorkflow.run,
        args=[task_input],
        id=workflow_id,
        task_queue=task_queue,
    )
    logger.info(f"Started workflow {workflow_id} on queue {task_queue}")
    return handle


async def start_split_download(task_dict: dict, worker_count: int = 2):
    """Start a SplitDownloadWorkflow for a large dataset (legacy — kept for bj1-4)."""
    from ..temporal.models import TaskInput
    from ..temporal.workflows import SplitDownloadWorkflow

    client = await connected_client()
    task_input = TaskInput(
        id=task_dict["id"],
        name=task_dict["name"],
        repo_id=task_dict["repo_id"],
        source=task_dict.get("source", "hf"),
        type=task_dict.get("type", "dataset"),
        category=task_dict.get("category", ""),
        priority=task_dict.get("priority", 5),
        size_gb=task_dict.get("size_gb", 0),
    )

    handle = await client.start_workflow(
        SplitDownloadWorkflow.run,
        args=[task_input, worker_count],
        id=f"{WORKFLOW_ID_PREFIXES['SplitDownloadWorkflow']}{task_dict['id']}",
        task_queue="download-workers",
    )
    logger.info(f"Started split workflow for {task_dict['name']} ({worker_count} workers)")
    return handle


async def queue_poller_count(client, queue: str) -> int | None:
    """How many workers currently poll `queue` for workflow tasks.

    None means Temporal could not be asked (RPC error, old server) — callers
    must treat that as "unknown", never as zero.
    """
    from temporalio.api.enums.v1 import TaskQueueType
    from temporalio.api.taskqueue.v1 import TaskQueue
    from temporalio.api.workflowservice.v1 import DescribeTaskQueueRequest

    try:
        resp = await client.workflow_service.describe_task_queue(
            DescribeTaskQueueRequest(
                namespace=client.namespace,
                task_queue=TaskQueue(name=queue),
                task_queue_type=TaskQueueType.TASK_QUEUE_TYPE_WORKFLOW,
            )
        )
        return len(resp.pollers)
    except Exception as e:  # pragma: no cover - depends on live server
        logger.warning(f"describe_task_queue({queue}) failed: {e}")
        return None


async def start_sharded_download(task_dict: dict, task_queue: str | None = None):
    """Start a ShardedDownloadWorkflow — auto-sharding coordinator.

    `task_queue` must be one the source's own workers poll; see
    fleet.coordinator_queue. Defaults to the shared HK queue, which is correct
    for every source except ModelScope.

    Refuses to start when the target queue has no pollers. Temporal accepts
    such a start happily and the execution sits RUNNING forever: nothing
    reconciles it (has_live_workflow stays true, so reconcile only records it
    as "stale" and redispatch_orphaned skips it), the task stays `downloading`
    with zero shards, and auto_dispatch's listing guard blocks every other task
    of that source for 15 minutes. The reachable trigger is deploy order —
    restarting dlm-web before deploy-workers.sh has restarted any node of the
    source, so the new coordinator queue exists in code but nobody polls it
    yet. Failing here instead leaves the task `pending` for the next 30s cycle.
    """
    from ..temporal.models import TaskInput
    from ..temporal.workflows import ShardedDownloadWorkflow
    from .fleet import SHARED_COORDINATOR_QUEUE

    client = await connected_client()
    task_input = TaskInput(
        id=task_dict["id"],
        name=task_dict.get("name", ""),
        repo_id=task_dict.get("repo_id", ""),
        source=task_dict.get("source", "hf"),
        type=task_dict.get("type", "dataset"),
        category=task_dict.get("category", ""),
        priority=task_dict.get("priority", 5),
        size_gb=task_dict.get("size_gb", 0),
        shard_count=int(task_dict.get("max_workers") or 0),
    )

    queue = task_queue or SHARED_COORDINATOR_QUEUE
    pollers = await queue_poller_count(client, queue)
    if pollers == 0:
        raise RuntimeError(
            f"no worker polls coordinator queue {queue!r} — refusing to start "
            f"{task_dict['id']}, which would hang RUNNING with nothing to "
            f"reconcile it. Deploy the workers for this source first "
            f"(bash scripts/deploy-workers.sh), then dlm-web."
        )
    # WORKFLOW_ID_PREFIXES, not a literal "sharded-": the cancel/terminate
    # sweep derives the same ID from this map, and the two drifting apart
    # would leave a workflow nothing can stop. The value is identical, so
    # running executions keep their IDs.
    workflow_id = f"{WORKFLOW_ID_PREFIXES['ShardedDownloadWorkflow']}{task_dict['id']}"
    handle = await client.start_workflow(
        ShardedDownloadWorkflow.run,
        task_input,
        id=workflow_id,
        task_queue=queue,
    )
    logger.info(
        f"Started sharded workflow {workflow_id} on queue {queue} "
        f"(pollers={pollers if pollers is not None else 'unknown'})")
    return handle


class PoolPollerGateError(RuntimeError):
    """Raised when a pool task's activity queue has no live consumer.

    Pool workers register activities on the shared pool-hf/pool-ms queue but
    no workflow, so a start that lands on a queue nothing is draining would
    sit its batches there — indistinguishable from "dispatched fine" until
    the schedule_to_close timeout fires, hours later. Refusing here, loudly,
    is the only way this gate has teeth: a silent fallback to sharded would
    defeat it outright.

    "No live consumer" means zero ACTIVITY pollers AND no recent batch report
    from a worker of that source. Both halves are needed because zero pollers
    alone is also what a 100%-busy fleet looks like — see
    pool_queue_is_served.
    """


async def _pool_poller_count(client: Client, queue_name: str) -> int:
    """Live ACTIVITY-type pollers on a pool queue, via raw gRPC.

    Pool workers register activities but no workflow — DescribeTaskQueue on
    the WORKFLOW type (the type `Client` has no convenience method for
    anyway) reads zero pollers even on a healthy fleet. There is no
    `Client` method for this at all, so this goes straight at
    `client.service_client.workflow_service.describe_task_queue` with the
    ACTIVITY task_queue_type.
    """
    from temporalio.api.enums.v1 import TaskQueueType
    from temporalio.api.taskqueue.v1 import TaskQueue
    from temporalio.api.workflowservice.v1 import DescribeTaskQueueRequest

    request = DescribeTaskQueueRequest(
        namespace=client.namespace,
        task_queue=TaskQueue(name=queue_name),
        task_queue_type=TaskQueueType.TASK_QUEUE_TYPE_ACTIVITY,
    )
    response = await client.service_client.workflow_service.describe_task_queue(
        request, timeout=QUERY_TIMEOUT
    )
    return len(response.pollers)


# Maps the raw proto enum to the plain strings the pool patrol (decision A)
# switches on, so reconciler.py never has to import temporalio.api directly.
_PENDING_ACTIVITY_STATE_NAMES = None


def _pending_activity_state_names() -> dict:
    global _PENDING_ACTIVITY_STATE_NAMES
    if _PENDING_ACTIVITY_STATE_NAMES is None:
        from temporalio.api.enums.v1 import PendingActivityState

        _PENDING_ACTIVITY_STATE_NAMES = {
            PendingActivityState.PENDING_ACTIVITY_STATE_SCHEDULED: "SCHEDULED",
            PendingActivityState.PENDING_ACTIVITY_STATE_STARTED: "STARTED",
            PendingActivityState.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED: "CANCEL_REQUESTED",
        }
    return _PENDING_ACTIVITY_STATE_NAMES


def _proto_timestamp_to_epoch(ts) -> float:
    """Seconds since epoch for a protobuf `Timestamp`.

    Caller must have already checked `HasField` — this only converts.
    `Timestamp.ToDatetime()` returns a naive datetime and `.timestamp()`
    would interpret that as local time — wrong for a UTC wall-clock proto.
    `seconds`/`nanos` are already Unix-epoch, so read them directly.
    """
    return ts.seconds + ts.nanos / 1e9


async def pending_activities(workflow_id: str) -> list[dict]:
    """Normalised pending-activity rows for one workflow, or [] if absent.

    Feeds the pool patrol's trigger 2 (SCHEDULED aged past
    POOL_STARVED_SCHEDULED_S) and trigger 3 (attempt climbing) — see decision
    A. `handle.describe()` returns a `WorkflowExecutionDescription`; the
    pending activities live on the raw proto
    (`desc.raw_description.pending_activities`), each a `PendingActivityInfo`
    with `activity_type.name`, `state` (`PendingActivityState`), `attempt`,
    `scheduled_time`, `last_started_time` — verified against the installed
    temporalio (1.31.0) by introspecting the message's own DESCRIPTOR; every
    name in the brief matched exactly, no differences to report.

    An inspection pass must never be able to stop the scheduler loop: any
    failure (workflow not found, RPC error, timeout) returns [] rather than
    raising. `rpc_timeout=QUERY_TIMEOUT` reuses the module's existing RPC
    deadline rather than adding a second one.
    """
    try:
        client = await connected_client()
        handle = client.get_workflow_handle(workflow_id)
        desc = await handle.describe(rpc_timeout=QUERY_TIMEOUT)
    except Exception:
        return []

    state_names = _pending_activity_state_names()
    rows = []
    for pai in desc.raw_description.pending_activities:
        rows.append({
            "activity_type": pai.activity_type.name,
            "state": state_names.get(pai.state, "UNSPECIFIED"),
            "attempt": pai.attempt,
            "scheduled_at": _proto_timestamp_to_epoch(pai.scheduled_time)
            if pai.HasField("scheduled_time") else None,
            "last_started_at": _proto_timestamp_to_epoch(pai.last_started_time)
            if pai.HasField("last_started_time") else None,
        })
    return rows


def live_pool_batch_count(source: str, now: float | None = None) -> int:
    """Worker-reported running batches on this source's pool queue — the busy/dead tiebreak.

    BLOCKING (SQLite): call from a thread, never straight off the event loop.

    Zero ACTIVITY pollers on a pool queue is ambiguous by construction — a pool
    worker holds one batch at a time and a Temporal worker at its concurrency
    limit stops polling, so "every worker is busy" and "no worker ever
    registered this queue" produce the identical reading. This is the
    disambiguator both the dispatch gate and the pool patrol consult before
    they act on a zero: a nonzero count means a live worker wrote to a batch
    row on this queue inside POOL_LIVE_BATCH_WINDOW_S.

    `source` is normalised to its queue's canonical source before the count,
    because one pool queue is one worker pool: a `huggingface` task and an
    `hf` task share pool-hf and must vouch for each other.
    """
    from ..queue.snapshot import count_live_pool_batches, init_db
    from .fleet import POOL_LIVE_BATCH_WINDOW_S, pool_queue_source

    init_db()
    ref = time.time() if now is None else now
    return count_live_pool_batches(
        pool_queue_source(source), ref - POOL_LIVE_BATCH_WINDOW_S
    )


class PoolQueueService(NamedTuple):
    """Why a pool queue counts as served (or not) — see pool_queue_is_served.

    `served` is the answer both callers act on. `pollers` is kept because
    served-with-zero-pollers means something different from served-with-a-free-
    worker: the first is a saturated fleet, where a batch activity sitting
    SCHEDULED is ordinary queueing rather than starvation.
    """
    served: bool
    pollers: int
    live_batches: int


async def pool_queue_is_served(
    client: Client, source: str, queue_name: str
) -> PoolQueueService:
    """Whether this source's pool queue has a live consumer, and on what evidence.

    Raises whatever the describe RPC raises — callers differ on what an
    unknown answer means (the gate refuses fail-closed, the patrol stays
    silent), so neither judgement is made here.
    """
    pollers = await _pool_poller_count(client, queue_name)
    if pollers > 0:
        return PoolQueueService(True, pollers, 0)
    live = await asyncio.get_running_loop().run_in_executor(
        None, live_pool_batch_count, source
    )
    if live > 0:
        logger.info(
            f"{queue_name} has 0 activity pollers but {live} running batch(es) "
            f"reported recently — the fleet is saturated, not absent "
            f"(one batch per pool worker, so a busy worker stops polling)"
        )
    return PoolQueueService(live > 0, 0, live)


async def start_pool_download(task_dict: dict, task_queue: str | None = None):
    """Start a PoolDownloadWorkflow — work-stealing coordinator.

    `task_queue` is the COORDINATOR's queue (list/filter/chunk, then the
    window loop) and must be one the task's own source polls — see
    fleet.coordinator_queue. It defaults to the shared HK queue, which is
    correct for every source except ModelScope. This used to be
    "download-workers" **explicitly**, on the reasoning that it matched
    start_sharded_download; that stopped being true when a ModelScope
    coordinator on the HK-only queue ran its listing on an HF node and died
    with `No module named 'modelscope'` (t-20260806-cbf39e), so both paths
    now take the queue from their one caller, start_task_download.

    Only the BATCHES run on the shared pool-hf/pool-ms queue
    (`dlm.temporal.workflows.pool_task_queue`), which this function reads but
    never rebuilds — that is a different queue from this argument.

    Mode gate (plan change #2): refuses to start — raising
    PoolPollerGateError rather than falling back to sharded — if that pool
    queue has no live consumer at all. "No live consumer" is deliberately
    NOT "fewer pollers than alive workers", which is what this gate asked
    until #91: a pool worker runs one batch at a time, so a fully-loaded
    fleet reports zero pollers and the old comparison refused every dispatch
    precisely when the pool was working (measured 2026-08-09: HK 7/7 busy ->
    pool-hf = 0 pollers, and the rejection count decayed 14 -> 9 -> 8 -> 0 as
    the fleet filled). See pool_queue_is_served.
    """
    from ..temporal.models import TaskInput
    from ..temporal.workflows import PoolDownloadWorkflow, pool_task_queue
    from .fleet import POOL_LIVE_BATCH_WINDOW_S, SHARED_COORDINATOR_QUEUE

    source = task_dict.get("source", "hf")
    coordinator = task_queue or SHARED_COORDINATOR_QUEUE
    client = await connected_client()

    queue_name = pool_task_queue(source)
    # T7 review (routed to T9): the describe RPC uses the SDK default
    # retry=False, so a transient gRPC blip refuses the dispatch — the
    # correct fail-closed direction — but on its own raises the same kind of
    # exception a caller sees from any other failure, so "the fleet is
    # under-polled" and "the network blipped" were distinguishable in the
    # log only by luck of the underlying gRPC error text. Both branches below
    # still refuse (raise PoolPollerGateError, no fallback to sharded); only
    # the log message and the exception's message differ, so a human
    # scanning the log — or a future alert keyed on message content — can
    # tell them apart. The probe now has two failure sources (the RPC and the
    # SQLite read below), so the message names the probe rather than one of
    # them, and carries the exception type to separate a gRPC error from an
    # sqlite3.OperationalError.
    #
    # The batch-row half of pool_queue_is_served takes SQLite's write lock via
    # init_db(), so it runs in an executor there rather than on the event loop:
    # start_pool_download is awaited straight from the /queue/preempt and
    # /doctor/fix handlers, which push every other SQLite touch through
    # run_blocking. Bounded at busy_timeout (5s), but that is 5s of accept()
    # gap for the whole web server. See tests/test_event_loop_safety.py — its
    # AST scan only inspects the handler's own body, so this indirection is
    # invisible to it.
    try:
        service = await pool_queue_is_served(client, source, queue_name)
    except Exception as e:
        msg = (
            f"pool gate: liveness probe for {queue_name} failed: {type(e).__name__}: {e} "
            f"(the probe is describe_task_queue plus a SQLite batch-row read — "
            f"either can raise; refusing dispatch fail-closed, this is NOT "
            f"necessarily under-polling)"
        )
        logger.critical(msg)
        raise PoolPollerGateError(msg) from e
    if not service.served:
        msg = (
            f"pool gate rejected task {task_dict.get('id')}: {queue_name} has "
            f"0 activity pollers and no worker on that queue reported a running "
            f"pool batch in the last {POOL_LIVE_BATCH_WINDOW_S}s — nothing would "
            f"drain this queue (is a pool worker registered on {queue_name}?)"
        )
        logger.critical(msg)
        raise PoolPollerGateError(msg)

    task_input = TaskInput(
        id=task_dict["id"],
        name=task_dict.get("name", ""),
        repo_id=task_dict.get("repo_id", ""),
        source=source,
        type=task_dict.get("type", "dataset"),
        category=task_dict.get("category", ""),
        priority=task_dict.get("priority", 5),
        size_gb=task_dict.get("size_gb", 0),
    )

    workflow_id = f"{WORKFLOW_ID_PREFIXES['PoolDownloadWorkflow']}{task_dict['id']}"
    handle = await client.start_workflow(
        PoolDownloadWorkflow.run,
        task_input,
        id=workflow_id,
        task_queue=coordinator,
    )
    logger.info(
        f"Started pool workflow {workflow_id} on queue {coordinator} "
        f"(batches on {queue_name}, which has a live consumer)"
    )
    return handle


def _record_dispatch_prefix(task: dict):
    """Freeze the upload prefix this dispatch will use, for the transfer gate.

    Here rather than in the two route handlers that create tasks because this
    function is the single funnel every dispatch passes through — sharded, pool,
    /queue/add, /queue/retry, the reconciler and doctor's repair path all arrive
    here, and each of them re-dispatches with whatever name/category the row
    holds NOW. That value is what the uploader will use, so it is the only
    prefix a later transfer may read from; recomputing `bos_target()` at
    transfer time would follow a rename the uploader never saw.

    Best effort by design: a task that cannot record its prefix must still be
    dispatched. The consequence of a miss is a NULL column, which the transfer
    drift gate treats as "unknown, skip me" — the byte-ratio band still applies.
    """
    from ..queue import snapshot
    from ..transfer.targets import plan_from_mapping

    try:
        snapshot.set_dispatch_prefix(task["id"], plan_from_mapping(task).source)
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"could not record dispatch_prefix for "
                       f"{task.get('id')}: {exc}")


async def start_task_download(task: dict):
    """Dispatch entry point: routes a task to its coordinator by dispatch_mode.

    `start_download` (:121 above) is a legacy name already occupied — it's
    still called by dlm/temporal/dispatch.py and scripts/migrate-tasks.py —
    so this is a new function, not an overload. A missing/NULL dispatch_mode
    means 'sharded' (every task row created before this task existed, and
    every caller that still doesn't know about the column). An unrecognised
    mode string is an error: silently falling back to sharded would hide a
    caller bug instead of surfacing it.

    This is also the ONE place the coordinator queue is decided. Both modes
    get `coordinator_queue(source)` rather than the shared HK default: the
    coordinator runs the repo listing, and a ModelScope listing on an HF node
    fails outright on 2 of the 7 HK boxes (`No module named 'modelscope'`,
    t-20260806-cbf39e) and takes 10+ minutes on the rest. Callers pass a task
    row and nothing else, so there is no per-call-site queue argument left to
    get wrong.
    """
    from .fleet import coordinator_queue

    mode = task.get("dispatch_mode") or "sharded"
    queue = coordinator_queue(task.get("source") or "hf")
    _record_dispatch_prefix(task)
    if mode == "sharded":
        return await start_sharded_download(task, task_queue=queue)
    if mode == "pool":
        return await start_pool_download(task, task_queue=queue)
    raise ValueError(f"unknown dispatch_mode={mode!r} for task {task.get('id')}")


async def _find_running_workflow_ids(client, task_id: str) -> list:
    """All RUNNING workflow IDs containing task_id — catches suffixed legacy
    IDs (e.g. dl-{task_id}-bjN-v3) that fixed patterns miss."""
    try:
        return [wf_id for wf_id in await running_workflows(client) if task_id in wf_id]
    except Exception:
        # Best effort: the caller falls back to the fixed ID patterns. Swallowing
        # this is only safe because the scan is now bounded — an untimed one
        # would hang /queue/pause and /tasks/{id}/skip instead of failing.
        return []


async def cancel_workflow(task_id: str, dispatch_mode: Optional[str] = None):
    """Cancel running workflow(s) for a task — handles all ID patterns.

    dispatch_mode='pool' skips the per-shard ShardWorkerWorkflow sweep below:
    pool batches run as activities inside PoolDownloadWorkflow, not as child
    workflows, so there is no shard-row-keyed handle to cancel there — only
    the parent `pool-{task_id}` (already covered by PARENT_WORKFLOW_TYPES,
    which includes PoolDownloadWorkflow) matters. Walking every batch row
    (up to ~1500 for a pool task) to build a handle that can never exist is
    pure wasted RPCs. Any other value (None/'sharded'/anything else) keeps
    this function's sharded-path effect byte-identical to before — that is
    the only branch this function makes on dispatch_mode.
    """
    client = await connected_client()
    wf_ids = {f"{WORKFLOW_ID_PREFIXES[t]}{task_id}" for t in PARENT_WORKFLOW_TYPES}
    wf_ids.update(await _find_running_workflow_ids(client, task_id))

    for wf_id in wf_ids:
        try:
            handle = client.get_workflow_handle(wf_id)
            await handle.cancel(rpc_timeout=QUERY_TIMEOUT)
            logger.info(f"Cancelled workflow {wf_id}")
        except Exception:
            pass

    if dispatch_mode == "pool":
        return

    # Also cancel shard child workflows
    try:
        from ..queue.snapshot import get_shards_by_task, init_db
        init_db()
        shards = get_shards_by_task(task_id)
        shard_prefix = WORKFLOW_ID_PREFIXES["ShardWorkerWorkflow"]
        for shard in shards:
            try:
                handle = client.get_workflow_handle(f"{shard_prefix}{shard['id']}")
                await handle.cancel(rpc_timeout=QUERY_TIMEOUT)
            except Exception:
                pass
    except Exception:
        pass


async def terminate_workflow_and_wait(
    task_id: str, timeout_s: int = 120, dispatch_mode: Optional[str] = None
) -> bool:
    """Terminate all workflows for a task and wait until they are closed.

    Unlike cancel_workflow (async cancel, returns immediately), this blocks
    until Temporal reports every handle closed — required before requeuing a
    task under the same workflow ID (e.g. /queue/reshard). Returns True when
    everything closed within the timeout.

    dispatch_mode='pool' skips building per-shard ShardWorkerWorkflow handles
    for the same reason cancel_workflow does: pool batches are activities,
    not child workflows, so there are no such handles to wait on. Any other
    value keeps the sharded-path effect byte-identical to before.

    Each describe() call below already carries its own QUERY_TIMEOUT (15s)
    RPC budget via rpc_timeout — that is what keeps one unresponsive handle
    from silently consuming the whole `timeout_s` poll window; the overall
    deadline is unchanged.
    """
    import time as _time
    from temporalio.client import WorkflowExecutionStatus

    client = await connected_client()
    wf_ids = {f"{WORKFLOW_ID_PREFIXES[t]}{task_id}" for t in PARENT_WORKFLOW_TYPES}
    wf_ids.update(await _find_running_workflow_ids(client, task_id))
    handles = [client.get_workflow_handle(wf_id) for wf_id in wf_ids]
    if dispatch_mode != "pool":
        try:
            from ..queue.snapshot import get_shards_by_task, init_db
            init_db()
            shard_prefix = WORKFLOW_ID_PREFIXES["ShardWorkerWorkflow"]
            for shard in get_shards_by_task(task_id):
                handles.append(client.get_workflow_handle(f"{shard_prefix}{shard['id']}"))
        except Exception:
            pass

    for handle in handles:
        try:
            await handle.terminate(
                reason=f"reshard/requeue of {task_id}", rpc_timeout=QUERY_TIMEOUT
            )
        except Exception:
            pass  # not found / already closed

    deadline = _time.monotonic() + timeout_s
    open_statuses = {WorkflowExecutionStatus.RUNNING}
    while _time.monotonic() < deadline:
        still_open = 0
        for handle in handles:
            try:
                # Without a deadline a single hung describe() outlives
                # `timeout_s` entirely — the loop only re-checks between
                # iterations, so it never gets to notice the deadline passed.
                desc = await handle.describe(rpc_timeout=QUERY_TIMEOUT)
                if desc.status in open_statuses:
                    still_open += 1
            except Exception:
                pass  # not found = closed
        if still_open == 0:
            return True
        await asyncio.sleep(2)
    return False
