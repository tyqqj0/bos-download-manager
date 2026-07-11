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

        # 3. List files (saves to disk, returns path)
        if task_input.filelist_path:
            filelist_path = task_input.filelist_path
        else:
            try:
                filelist_path = await workflow.execute_activity(
                    "list_repo_files",
                    args=[task_input],
                    start_to_close_timeout=timedelta(minutes=30),
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

        # 3b. Read file list metadata (count + total size only)
        filelist_meta = await workflow.execute_activity(
            "read_filelist",
            args=[filelist_path],
            start_to_close_timeout=timedelta(minutes=5),
        )

        total_file_count = filelist_meta["count"]
        total_bytes = filelist_meta["total_bytes"]
        total_gb = total_bytes / (1024 ** 3)

        if total_file_count == 0:
            await workflow.execute_activity(
                "report_to_dashboard",
                args=[task_input.id, "failed", None, None, None, None, server_key, "No files found in repo"],
                start_to_close_timeout=timedelta(seconds=30),
            )
            return TaskResult(status="failed", error="No files found in repo")

        # 4. Load progress (resume support — each entry = one completed batch)
        completed_paths = await workflow.execute_activity(
            "load_progress",
            args=[task_input],
            start_to_close_timeout=timedelta(seconds=30),
        )

        completed_batches = len(completed_paths) if completed_paths else 0
        start_idx = completed_batches * BATCH_SIZE
        uploaded_bytes = int(total_bytes * min(start_idx, total_file_count) / total_file_count) if start_idx > 0 else 0

        # 5. Process in batches (by index into the on-disk file list)
        batch_num = completed_batches
        all_batch_markers = list(completed_paths) if completed_paths else []

        while start_idx < total_file_count:
            batch_num += 1
            current_batch_size = min(BATCH_SIZE, total_file_count - start_idx)

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
                args=[task_input, filelist_path, start_idx, current_batch_size],
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

            # Update progress
            uploaded_bytes += result["uploaded_bytes"]
            start_idx += current_batch_size
            all_batch_markers.append(f"batch-{batch_num}-done")

            await workflow.execute_activity(
                "save_progress",
                args=[task_input.name, all_batch_markers],
                start_to_close_timeout=timedelta(seconds=30),
            )

        # 6. Done!
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
            files_uploaded=total_file_count,
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
        # List all files (saves to disk)
        filelist_path = await workflow.execute_activity(
            "list_repo_files",
            args=[task_input],
            start_to_close_timeout=timedelta(minutes=30),
            heartbeat_timeout=timedelta(minutes=3),
            retry_policy=ACTIVITY_RETRY,
        )

        # Partition into chunks on the worker (avoids sending full list via gRPC)
        partitions = await workflow.execute_activity(
            "partition_filelist",
            args=[filelist_path, worker_count],
            start_to_close_timeout=timedelta(minutes=5),
        )

        if not partitions:
            return TaskResult(status="failed", error="No files found")

        # Launch child workflows — each gets its own filelist partition
        child_handles = []
        for i, part in enumerate(partitions):
            child_input = TaskInput(
                id=f"{task_input.id}-part{i+1}",
                name=task_input.name,
                repo_id=task_input.repo_id,
                source=task_input.source,
                type=task_input.type,
                category=task_input.category,
                priority=task_input.priority,
                size_gb=part["total_bytes"] / (1024 ** 3),
                filelist_path=part["path"],
            )
            handle = await workflow.start_child_workflow(
                DownloadDatasetWorkflow.run,
                args=[child_input],
                id=f"{task_input.id}-part{i+1}",
                task_queue="download-workers",
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
