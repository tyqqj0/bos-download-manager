"""Identifier conventions shared by the workflows, activities, and the web API.

Two names travel between S1 and the workers and must be built and parsed the
same way on both ends:

  shard row id   `s-{task_id}-{index}`   — the SQLite `shards.id`
  shard run name `{task_name}/shard-{index}` — the pipeline's task name, which
                 scopes the staging directory and is parsed back to recover
                 both the row id and the FLAT BOS prefix

A mismatch is silent, not loud: `/api/shard-progress` answers "shard not
found" to a fire-and-forget POST, so the shard simply stops reporting and the
dashboard shows it stalled while it downloads fine.
"""

from __future__ import annotations

SHARD_SEP = "/shard-"


def shard_row_id(task_id: str, shard_index: int | str) -> str:
    """The SQLite `shards.id` for one shard of a task."""
    return f"s-{task_id}-{shard_index}"


def shard_task_name(task_name: str, shard_index: int | str) -> str:
    """The pipeline task name a shard worker runs under."""
    return f"{task_name}{SHARD_SEP}{shard_index}"


def split_shard_name(name: str) -> tuple[str, str | None]:
    """Inverse of shard_task_name: `(base_task_name, shard_index or None)`.

    The base name is what determines the BOS prefix — shards of a task all
    upload to the task's flat prefix, never a `shard-N/` subprefix, or the
    resume filter cannot see the files they already uploaded.
    """
    if SHARD_SEP not in name:
        return name, None
    base, _, suffix = name.rpartition(SHARD_SEP)
    if not suffix.isdigit():
        return name, None
    return base, suffix
