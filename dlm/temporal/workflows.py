"""Temporal workflow definitions — orchestrate the download lifecycle."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Optional

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from .models import TaskInput, TaskResult


BATCH_SIZE = 500  # files per pipeline batch (Temporal checkpoint boundary)

NON_RETRYABLE_ERRORS = [
    "NotFoundError",
    "GatedRepoError",
    "AuthError",
]

ACTIVITY_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=30),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=10),
    maximum_attempts=5,
    non_retryable_error_types=NON_RETRYABLE_ERRORS,
)


@workflow.defn
class DownloadDatasetWorkflow:
    """Download a dataset: list files → pipeline download+upload in batches.

    Each batch is a checkpoint. If the worker dies, Temporal restarts
    from the last completed batch. Within a batch, the local .progress.json
    tracks individual files for sub-batch resume.
    """

    @workflow.run
    async def run(self, task_input: TaskInput) -> TaskResult:
        server_key = workflow.info().task_queue.removeprefix("download-")

        # 1. Report starting
        await workflow.execute_activity(
            "report_to_dashboard",
            args=[task_input.id, "downloading", "starting", 0, 0, 0, server_key, None],
            start_to_close_timeout=timedelta(seconds=30),
        )

        # 2. Clean staging (keep only this task)
        await workflow.execute_activity(
            "cleanup_all_staging",
            args=[task_input.name],
            start_to_close_timeout=timedelta(minutes=5),
        )

        # 3. List files
        try:
            file_dicts = await workflow.execute_activity(
                "list_repo_files",
                args=[task_input],
                start_to_close_timeout=timedelta(minutes=10),
                heartbeat_timeout=timedelta(minutes=3),
                retry_policy=ACTIVITY_RETRY,
            )
        except Exception as e:
            error_msg = str(e)
            await workflow.execute_activity(
                "report_to_dashboard",
                args=[task_input.id, "failed", None, None, None, None, server_key, error_msg],
                start_to_close_timeout=timedelta(seconds=30),
            )
            return TaskResult(status="failed", error=error_msg)

        if not file_dicts:
            await workflow.execute_activity(
                "report_to_dashboard",
                args=[task_input.id, "failed", None, None, None, None, server_key, "No files found in repo"],
                start_to_close_timeout=timedelta(seconds=30),
            )
            return TaskResult(status="failed", error="No files found in repo")

        # 4. Filter to assigned files (for split tasks)
        if task_input.assigned_files:
            assigned_set = set(task_input.assigned_files)
            file_dicts = [f for f in file_dicts if f["path"] in assigned_set]

        total_bytes = sum(f["size"] for f in file_dicts)
        total_gb = total_bytes / (1024 ** 3)

        # 5. Load progress (resume support)
        completed_paths = await workflow.execute_activity(
            "load_progress",
            args=[task_input],
            start_to_close_timeout=timedelta(seconds=30),
        )

        if completed_paths:
            completed_set = set(completed_paths)
            file_dicts = [f for f in file_dicts if f["path"] not in completed_set]
            # recalculate uploaded bytes from remaining
            uploaded_bytes = total_bytes - sum(f["size"] for f in file_dicts)
        else:
            uploaded_bytes = 0

        # 6. Process in batches
        batch_num = 0
        all_completed = list(completed_paths) if completed_paths else []

        # Sort: largest files first for better disk utilization
        file_dicts.sort(key=lambda f: f["size"], reverse=True)

        while file_dicts:
            batch_num += 1
            batch = file_dicts[:BATCH_SIZE]
            file_dicts = file_dicts[BATCH_SIZE:]

            pct = (uploaded_bytes / total_bytes * 100) if total_bytes > 0 else 0

            await workflow.execute_activity(
                "report_to_dashboard",
                args=[task_input.id, "downloading", f"batch {batch_num}",
                      round(pct, 1), None, round(uploaded_bytes / 1024**3, 2), server_key, None],
                start_to_close_timeout=timedelta(seconds=30),
            )

            # Run pipeline for this batch
            result = await workflow.execute_activity(
                "run_pipeline_batch",
                args=[task_input, batch],
                start_to_close_timeout=timedelta(hours=24),
                heartbeat_timeout=timedelta(minutes=10),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(minutes=1),
                    backoff_coefficient=2.0,
                    maximum_interval=timedelta(minutes=30),
                    maximum_attempts=3,
                    non_retryable_error_types=NON_RETRYABLE_ERRORS,
                ),
            )

            # Save progress checkpoint
            uploaded_bytes += result["uploaded_bytes"]
            batch_paths = [f["path"] for f in batch]
            all_completed.extend(batch_paths)

            await workflow.execute_activity(
                "save_progress",
                args=[task_input.name, all_completed],
                start_to_close_timeout=timedelta(seconds=30),
            )

        # 7. Done!
        await workflow.execute_activity(
            "clear_progress",
            args=[task_input.name],
            start_to_close_timeout=timedelta(seconds=30),
        )
        await workflow.execute_activity(
            "cleanup_staging",
            args=[task_input.name, False],
            start_to_close_timeout=timedelta(minutes=5),
        )
        await workflow.execute_activity(
            "report_to_dashboard",
            args=[task_input.id, "done", None, 100, 0, round(total_gb, 2), server_key, None],
            start_to_close_timeout=timedelta(seconds=30),
        )

        return TaskResult(
            status="done",
            files_uploaded=len(all_completed),
            bytes_uploaded=uploaded_bytes,
        )


@workflow.defn
class SplitDownloadWorkflow:
    """Split a large dataset across multiple workers.

    Divides files into N chunks (greedy by size) and runs
    DownloadDatasetWorkflow as child workflows — one per chunk.
    Each child runs on a different worker's task queue.
    """

    @workflow.run
    async def run(self, task_input: TaskInput, worker_count: int = 2) -> TaskResult:
        # List all files
        file_dicts = await workflow.execute_activity(
            "list_repo_files",
            args=[task_input],
            start_to_close_timeout=timedelta(minutes=10),
            heartbeat_timeout=timedelta(minutes=3),
            retry_policy=ACTIVITY_RETRY,
        )

        if not file_dicts:
            return TaskResult(status="failed", error="No files found")

        # Greedy partition by size
        file_dicts.sort(key=lambda f: f["size"], reverse=True)
        chunks: list[list] = [[] for _ in range(worker_count)]
        chunk_sizes = [0] * worker_count

        for f in file_dicts:
            min_idx = chunk_sizes.index(min(chunk_sizes))
            chunks[min_idx].append(f["path"])
            chunk_sizes[min_idx] += f["size"]

        # Launch child workflows
        child_handles = []
        for i, chunk in enumerate(chunks):
            child_input = TaskInput(
                id=f"{task_input.id}-part{i+1}",
                name=f"{task_input.name}",  # same name — same BOS prefix
                repo_id=task_input.repo_id,
                source=task_input.source,
                type=task_input.type,
                category=task_input.category,
                priority=task_input.priority,
                size_gb=chunk_sizes[i] / (1024 ** 3),
                assigned_files=chunk,
            )
            handle = await workflow.start_child_workflow(
                DownloadDatasetWorkflow.run,
                args=[child_input],
                id=f"{task_input.id}-part{i+1}",
                task_queue=f"download-workers",  # any available worker
            )
            child_handles.append(handle)

        # Wait for all children
        results = await asyncio.gather(*child_handles)
        total_files = sum(r.files_uploaded for r in results)
        total_bytes = sum(r.bytes_uploaded for r in results)
        failed = [r for r in results if r.status == "failed"]

        if failed:
            return TaskResult(
                status="failed",
                files_uploaded=total_files,
                bytes_uploaded=total_bytes,
                error=f"{len(failed)}/{worker_count} parts failed",
            )

        return TaskResult(
            status="done",
            files_uploaded=total_files,
            bytes_uploaded=total_bytes,
        )
