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
