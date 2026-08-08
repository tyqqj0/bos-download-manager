"""Dataclasses for Temporal workflow I/O."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class TaskInput:
    """Input to the download workflow."""
    id: str
    name: str
    repo_id: str
    source: str = "hf"           # "hf" or "modelscope"
    type: str = "dataset"        # "dataset" or "model"
    category: str = ""
    priority: int = 5
    size_gb: float = 0
    assigned_files: list = field(default_factory=list)  # for split tasks
    filelist_path: Optional[str] = None  # pre-computed filelist (skip list_repo_files)
    shard_count: int = 0  # user-requested shard count (0 = auto)


@dataclass
class FileInfo:
    """A single file in a repo."""
    path: str
    size: int  # bytes


@dataclass
class PipelineStats:
    """Real-time pipeline statistics."""
    total_files: int = 0
    downloaded_files: int = 0
    uploaded_files: int = 0
    failed_files: int = 0
    total_bytes: int = 0
    uploaded_bytes: int = 0
    speed_mbps: float = 0
    phase: str = "starting"
    paused: bool = False
    # Identity of every file counted in failed_files: {path, reason, size_bytes}.
    # The count alone told us a batch lost N files but never WHICH — that fact
    # lived in one line of one worker's log, so nothing downstream could record
    # or act on it. `reason` is a short classifier from FAIL_* below, never
    # exception text: those carry KB-scale CDN URLs on this fleet.
    #
    # Invariant: len(failed_details) == failed_files. default_factory keeps old
    # activity-result payloads (which have no such key) deserializable on replay.
    failed_details: list = field(default_factory=list)


# Failure classifiers for PipelineStats.failed_details.reason. Named constants
# rather than inline strings because activities.py filters on a subset of them
# (only source-permanent failures belong in the missing-file archive) and a
# typo in either place would silently drop or admit the wrong class.
FAIL_ACCESS_DENIED = "access_denied"
FAIL_UPSTREAM_EMPTY = "upstream_empty"
FAIL_DOWNLOAD_RETRIES_EXHAUSTED = "download_retries_exhausted"
FAIL_UNHANDLED_DOWNLOAD_ERROR = "unhandled_download_error"
FAIL_UPLOAD_CANCELLED = "upload_cancelled"
FAIL_SIZE_MISMATCH = "size_mismatch"
FAIL_UPLOAD_FAILED = "upload_failed"
FAIL_UPLOAD_RETRIES_EXHAUSTED = "upload_retries_exhausted"
FAIL_STAGED_FILE_MISSING = "staged_file_missing"


# ── Pool batch retry budget and missing-file tolerance (T3) ──────────────
#
# These live here, not in workflows.py or fleet.py, because the workflow and
# the activity must read the SAME values and neither may import the other:
# `activities.py` importing `workflows.py` would be a cycle, and a workflow
# module importing `dlm.web.fleet` breaks determinism.

# The retry ceiling for one pool batch. `POOL_BATCH_RETRY`'s RetryPolicy in
# workflows.py sets `maximum_attempts` from this, and `run_pool_batch` decides
# "is this my last chance" from the same number — the two used to be a literal
# 3 in one place and would have become a second literal in the other. Off-by-one
# between them is silent and asymmetric: raise the retry policy without raising
# this and the batch is forgiven one attempt early, throwing away the retry that
# might have succeeded on another worker; lower it and the tolerance never fires
# at all, which looks exactly like the bug T3 exists to fix.
POOL_BATCH_MAX_ATTEMPTS = 3

# How many permanently-failed files a batch may carry and still be judged
# complete on its last attempt.
#
# An absolute count, not a ratio, because of what the two sides are: we want to
# forgive individual bad objects at the source (ModelScope serving RoboDojo's
# depth files as 0 bytes) and to keep failing on systemic faults (network down,
# disk full, credentials rotated out from under us). A systemic fault in a
# 500-file batch kills far more than 5. A ratio would do the opposite of what we
# want at both ends — 33% of a 3-file batch is still just one bad object, while
# 5% of a 500-file batch is 25 files nobody looked at.
#
# Read at import so a typo (DLM_POOL_BATCH_FAIL_MAX=five) surfaces in the worker
# log at startup instead of turning into a ValueError inside an activity that
# has already spent an hour downloading.
POOL_BATCH_FAIL_MAX = int(os.environ.get("DLM_POOL_BATCH_FAIL_MAX", "5"))
if POOL_BATCH_FAIL_MAX < 0:
    raise ValueError(
        f"DLM_POOL_BATCH_FAIL_MAX={POOL_BATCH_FAIL_MAX} must be >= 0 "
        "(0 disables tolerance; negative is meaningless)"
    )

# Failure reasons that describe the FILE rather than the run — the only ones
# that belong in the `missing_files` archive, and (see run_pool_batch) the only
# ones a batch may be forgiven for.
#
# The exclusions are the point. `PipelineStats.failed_files` counts cancelled
# uploads too (pipeline.py's `_count_upload_task_failures`: cancellation must be
# counted so the batch is not reported clean, but it means an operator paused
# the task, not that the source lost the file). `staged_file_missing` is local
# disk state that the next attempt re-downloads. Archiving either would fill
# "which files is this dataset missing" with entries that are not missing.
ARCHIVABLE_FAIL_REASONS = frozenset({
    FAIL_ACCESS_DENIED,
    FAIL_UPSTREAM_EMPTY,
    FAIL_DOWNLOAD_RETRIES_EXHAUSTED,
    FAIL_UNHANDLED_DOWNLOAD_ERROR,
    FAIL_SIZE_MISMATCH,
    FAIL_UPLOAD_FAILED,
    FAIL_UPLOAD_RETRIES_EXHAUSTED,
})


# ── Task-level missing-file ceiling (T4) ─────────────────────────────────
#
# Per-batch tolerance (above) must not add up to a task-level lie: 5 forgiven
# files × up to 1500 batches is 7500 missing files, which is not "a few". So
# the coordinator applies its own ceiling at finalize time:
#
#     limit = max(TASK_MISSING_ABS, listed_files * TASK_MISSING_RATIO)
#
# at or under it the task reports `done` (with a WARNING alert and the files
# queryable); over it the task reports `failed`.
#
# Two terms because either alone misjudges one end of the fleet's range. The
# ratio keeps a 5-million-file dataset from failing over 120 files nobody would
# call broken; the absolute floor keeps a 200-file task from failing over a
# single file (0.5% of 200 is 1). Same reason POOL_BATCH_FAIL_MAX is absolute:
# the sizes here span four orders of magnitude.
#
# Lives here rather than in workflows.py because a workflow module must not
# read os.environ under the replay sandbox, and models is imported inside
# `workflow.unsafe.imports_passed_through()`.
TASK_MISSING_ABS = int(os.environ.get("DLM_TASK_MISSING_ABS", "10"))
TASK_MISSING_RATIO = float(os.environ.get("DLM_TASK_MISSING_RATIO", "0.005"))
if TASK_MISSING_ABS < 0 or TASK_MISSING_RATIO < 0:
    raise ValueError(
        f"DLM_TASK_MISSING_ABS={TASK_MISSING_ABS} / "
        f"DLM_TASK_MISSING_RATIO={TASK_MISSING_RATIO} must both be >= 0"
    )


def task_missing_limit(listed_files: int) -> int:
    """How many missing files this task may carry and still report `done`.

    Deliberately a plain function of one number so the workflow can call it
    inside `run()` (no I/O, no clock, no env read at call time — replay-safe)
    and the alerting side can reproduce the same value from what was stored.

    `listed_files` must be the LISTING count, not the number of files this run
    actually dispatched. The dispatched count is what the BOS resume filter
    left over, so on a task resuming at 99% it would be a few hundred — and
    0.5% of that rounds to 0, quietly turning the ratio term off exactly where
    a big dataset needs it most.
    """
    return max(TASK_MISSING_ABS, int(max(0, listed_files) * TASK_MISSING_RATIO))


# How many archived rows the finalize re-check will HEAD against BOS before it
# refuses the scan (review GAP-1).
#
# The archive is written optimistically and is NOT bounded by the per-batch
# tolerance: every permanently-failed file upserts a row whether or not its
# batch was forgiven, so under a systemic fault (a key rotated out from under
# the fleet, a CDN outage) a 240k-file task can archive six figures of rows.
# One HEAD each, at 16 threads, does not finish inside any sane
# heartbeat_timeout — and the retry policy then re-runs the identical scan,
# which is how a bounded bookkeeping step turns into a HEAD storm against a
# task that is already doomed.
#
# The default is deliberately the largest archive a task that still has a
# chance at `done` can carry: a batch is forgiven for at most
# POOL_BATCH_FAIL_MAX files, a task chunks into at most 1500 batches
# (fleet.POOL_MAX_BATCHES — restated as a literal here rather than imported,
# because this module is loaded on workers and dlm.web.fleet is S1-side), and
# any batch failing beyond tolerance makes the task `failed` on the
# failed_batches branch without consulting this scan at all. So a scan larger
# than this can only belong to a task whose verdict is already decided.
MISSING_VERIFY_MAX = int(
    os.environ.get("DLM_MISSING_VERIFY_MAX", str(1500 * POOL_BATCH_FAIL_MAX))
)
if MISSING_VERIFY_MAX < 0:
    raise ValueError(
        f"DLM_MISSING_VERIFY_MAX={MISSING_VERIFY_MAX} must be >= 0 "
        "(0 disables the re-check's BOS scan entirely)"
    )

# Rows per heartbeat during that scan. `verify_missing_files` used to emit
# exactly one heartbeat — before the blocking call — so a scan longer than
# heartbeat_timeout was killed and retried forever no matter how well it was
# progressing. Chunking the scan lets the activity prove liveness while it
# works, and makes the progress visible in `temporal workflow describe`.
MISSING_VERIFY_CHUNK = 256



@dataclass
class TaskResult:
    """Output from the download workflow."""
    status: str  # "done", "failed"
    files_uploaded: int = 0
    bytes_uploaded: int = 0
    error: Optional[str] = None


@dataclass
class ShardInput:
    """Input to ShardWorkerWorkflow — describes one shard of a task."""
    shard_id: str = ""
    task_id: str = ""
    task_name: str = ""
    repo_id: str = ""
    source: str = "hf"
    type: str = "dataset"
    category: str = ""
    shard_index: int = 0
    filelist_key: str = ""
    filelist_md5: str = ""  # content hash guarding stale batch-progress markers
    priority: int = 5
    size_bytes: int = 0


@dataclass
class ShardResult:
    """Output from ShardWorkerWorkflow."""
    shard_id: str = ""
    status: str = "done"
    files_uploaded: int = 0
    bytes_uploaded: int = 0
    error: Optional[str] = None
