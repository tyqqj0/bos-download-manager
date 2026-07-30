"""Dataclasses for Temporal workflow I/O."""

from __future__ import annotations

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
