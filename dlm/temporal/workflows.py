"""Temporal workflow definitions — orchestrate the download lifecycle."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Optional

from temporalio import workflow
from temporalio.common import Priority, RetryPolicy
from temporalio.workflow import ActivityCancellationType

with workflow.unsafe.imports_passed_through():
    from ..core.naming import shard_task_name
    from .models import TaskInput, TaskResult, ShardInput, ShardResult


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

        # 2. List files — returns {path, count, total_bytes, worker_queue}
        if task_input.filelist_path:
            # Pre-computed filelist (from SplitDownloadWorkflow, same worker)
            filelist_meta = await workflow.execute_activity(
                "read_filelist",
                args=[task_input.filelist_path],
                start_to_close_timeout=timedelta(minutes=5),
            )
            filelist_path = task_input.filelist_path
            total_file_count = filelist_meta["count"]
            total_bytes = filelist_meta["total_bytes"]
            worker_queue = workflow.info().task_queue
        else:
            try:
                result = await workflow.execute_activity(
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

            filelist_path = result["path"]
            total_file_count = result["count"]
            total_bytes = result["total_bytes"]
            worker_queue = result["worker_queue"]

        total_gb = total_bytes / (1024 ** 3)

        if total_file_count == 0:
            await workflow.execute_activity(
                "report_to_dashboard",
                args=[task_input.id, "failed", None, None, None, None, server_key, "No files found in repo"],
                start_to_close_timeout=timedelta(seconds=30),
            )
            return TaskResult(status="failed", error="No files found in repo")

        # 3. Preflight: verify enough disk space to run (pipeline needs ~25GB working room)
        try:
            disk_ok = await workflow.execute_activity(
                "check_disk_space",
                args=[25],
                start_to_close_timeout=timedelta(seconds=10),
                schedule_to_close_timeout=timedelta(minutes=2),
                retry_policy=RetryPolicy(maximum_attempts=3),
                task_queue=worker_queue,
            )
        except Exception:
            disk_ok = True  # activity unavailable (old worker) — proceed cautiously
        if not disk_ok:
            error_msg = f"Insufficient disk space on {server_key} (need 25GB free)"
            await workflow.execute_activity(
                "report_to_dashboard",
                args=[task_input.id, "failed", None, None, None, None, server_key, error_msg],
                start_to_close_timeout=timedelta(seconds=30),
            )
            return TaskResult(status="failed", error=error_msg)

        # 4. Load progress (resume support — each entry = one completed batch)
        completed_paths = await workflow.execute_activity(
            "load_progress",
            args=[task_input],
            start_to_close_timeout=timedelta(seconds=30),
            task_queue=worker_queue,
        )

        completed_batches = len(completed_paths) if completed_paths else 0
        start_idx = completed_batches * BATCH_SIZE
        uploaded_bytes = int(total_bytes * min(start_idx, total_file_count) / total_file_count) if start_idx > 0 else 0

        # 5. Process in batches — pinned to the worker that has the filelist
        batch_num = completed_batches
        all_batch_markers = list(completed_paths) if completed_paths else []

        try:
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

                # Run pipeline on the same worker that has the filelist
                # 7-day timeout: large batches (500 files × 40GB each = 20TB)
                # can take days at 150-300 GB/hr. Heartbeat (10min) catches stalls.
                result = await workflow.execute_activity(
                    "run_pipeline_batch",
                    args=[task_input, filelist_path, start_idx, current_batch_size,
                          uploaded_bytes, total_bytes],
                    start_to_close_timeout=timedelta(days=7),
                    heartbeat_timeout=timedelta(minutes=10),
                    task_queue=worker_queue,
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
                    task_queue=worker_queue,
                )

        except Exception as e:
            error_msg = str(e)[:500]
            # Report failure to dashboard
            try:
                await workflow.execute_activity(
                    "report_to_dashboard",
                    args=[task_input.id, "failed", None, None, None, None, server_key, error_msg],
                    start_to_close_timeout=timedelta(seconds=30),
                )
            except Exception:
                pass
            # Clean staging — preserve .progress.json + .filelist.json for resume.
            # Use aggressive timeout: worker may be dead.
            try:
                await workflow.execute_activity(
                    "cleanup_staging",
                    args=[task_input.name, True],  # keep_progress=True
                    start_to_close_timeout=timedelta(seconds=30),
                    task_queue=worker_queue,
                    retry_policy=RetryPolicy(
                        maximum_attempts=2,
                        initial_interval=timedelta(seconds=5),
                    ),
                )
            except Exception:
                pass  # best effort — worker may be dead
            return TaskResult(status="failed", error=error_msg)

        # 6. Done — these run only on success (except returns early above).
        # Each wrapped in its own try/except: cleanup/report failures must not
        # turn a successful download into a "failed" task.
        try:
            await workflow.execute_activity(
                "clear_progress",
                args=[task_input.name],
                start_to_close_timeout=timedelta(seconds=30),
                task_queue=worker_queue,
            )
        except Exception:
            pass

        try:
            await workflow.execute_activity(
                "cleanup_staging",
                args=[task_input.name, False],
                start_to_close_timeout=timedelta(minutes=5),
                task_queue=worker_queue,
            )
        except Exception:
            pass

        try:
            await workflow.execute_activity(
                "report_to_dashboard",
                args=[task_input.id, "done", None, 100, 0, round(total_gb, 2), server_key, None],
                start_to_close_timeout=timedelta(seconds=30),
            )
        except Exception:
            pass

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

    worker_queues: list of task queue names (e.g. ["download-bj1", "download-bj2"]).
    If provided, partitions are distributed round-robin across these queues
    and partition files are replicated to each target worker.
    If empty/None, all children run on the listing worker (legacy behavior).
    """

    @workflow.run
    async def run(self, task_input: TaskInput, worker_count: int = 2) -> TaskResult:

        try:
            # List all files — returns {path, count, total_bytes, worker_queue}
            result = await workflow.execute_activity(
                "list_repo_files",
                args=[task_input],
                start_to_close_timeout=timedelta(minutes=30),
                heartbeat_timeout=timedelta(minutes=3),
                retry_policy=ACTIVITY_RETRY,
            )

            filelist_path = result["path"]
            listing_queue = result["worker_queue"]

            if result["count"] == 0:
                return TaskResult(status="failed", error="No files found")

            # Partition into chunks — on the worker that has the filelist
            partitions = await workflow.execute_activity(
                "partition_filelist",
                args=[filelist_path, worker_count],
                start_to_close_timeout=timedelta(minutes=5),
                task_queue=listing_queue,
            )

            if not partitions:
                return TaskResult(status="failed", error="No files found")

        except Exception as e:
            error_msg = str(e)[:500]
            try:
                await workflow.execute_activity(
                    "report_to_dashboard",
                    args=[task_input.id, "failed", None, None, None, None, None, error_msg],
                    start_to_close_timeout=timedelta(seconds=30),
                )
            except Exception:
                pass
            return TaskResult(status="failed", error=error_msg)

        # Launch child workflows — all on the listing worker (partition files are local)
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
                task_queue=listing_queue,
            )
            child_handles.append(handle)

        # Wait for all children
        results = await asyncio.gather(*child_handles)
        total_files = sum(r.files_uploaded for r in results)
        total_bytes = sum(r.bytes_uploaded for r in results)
        failed = [r for r in results if r.status == "failed"]

        if failed:
            error_msg = f"{len(failed)}/{worker_count} parts failed"
            try:
                await workflow.execute_activity(
                    "report_to_dashboard",
                    args=[task_input.id, "failed", None, None, None, None, None, error_msg],
                    start_to_close_timeout=timedelta(seconds=30),
                )
            except Exception:
                pass
            return TaskResult(
                status="failed",
                files_uploaded=total_files,
                bytes_uploaded=total_bytes,
                error=error_msg,
            )

        total_gb = total_bytes / (1024 ** 3)
        try:
            await workflow.execute_activity(
                "report_to_dashboard",
                args=[task_input.id, "done", None, 100, 0, round(total_gb, 2), None, None],
                start_to_close_timeout=timedelta(seconds=30),
            )
        except Exception:
            pass

        return TaskResult(
            status="done",
            files_uploaded=total_files,
            bytes_uploaded=total_bytes,
        )


# ── Shard-based workflows ──────────────────────────────────


SHARD_MIN_BYTES = 5 * 1024 ** 3       # 5 GB minimum per shard
AUTO_SHARD_THRESHOLD = 10 * 1024 ** 3  # auto-shard above 10 GB


@workflow.defn
class ShardWorkerWorkflow:
    """Execute a single shard: download assigned files and upload to BOS.

    Reuses PipelineEngine via run_pipeline_batch — no pipeline changes.
    Staging path uses shard-specific subdir to avoid conflicts.
    """

    @workflow.run
    async def run(self, shard_input: ShardInput) -> ShardResult:
        shard_id = shard_input.shard_id
        server_key = workflow.info().task_queue.removeprefix("download-")
        shard_name = shard_task_name(shard_input.task_name, shard_input.shard_index)

        # Mark shard running + record server
        await workflow.execute_activity(
            "update_shard_status",
            args=[shard_id, "running"],
            start_to_close_timeout=timedelta(seconds=30),
        )
        await workflow.execute_activity(
            "assign_shard_server",
            args=[shard_id, server_key],
            start_to_close_timeout=timedelta(seconds=30),
        )

        # Disk preflight
        try:
            disk_ok = await workflow.execute_activity(
                "check_disk_space",
                args=[25],
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=RetryPolicy(maximum_attempts=3),
            )
        except Exception:
            disk_ok = True
        if not disk_ok:
            await workflow.execute_activity(
                "update_shard_status",
                args=[shard_id, "failed", f"Insufficient disk on {server_key}"],
                start_to_close_timeout=timedelta(seconds=30),
            )
            return ShardResult(shard_id=shard_id, status="failed", error="disk_full")

        # Load progress for resume — md5-guarded: markers from a different
        # filelist (re-partition, resume filter) are discarded, not trusted
        shard_task = TaskInput(id=shard_input.task_id, name=shard_name, repo_id=shard_input.repo_id)
        completed = await workflow.execute_activity(
            "load_progress",
            args=[shard_task, shard_input.filelist_md5],
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=ACTIVITY_RETRY,
        )

        # Download shard filelist from BOS to local disk, then read it
        staging_dir = f"/data/staging/{shard_name}"
        local_filelist = await workflow.execute_activity(
            "download_shard_filelist",
            args=[shard_input.filelist_key, staging_dir],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=ACTIVITY_RETRY,
        )
        filelist_info = await workflow.execute_activity(
            "read_filelist",
            args=[local_filelist],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=ACTIVITY_RETRY,
        )

        total_files = filelist_info["count"]
        total_bytes = filelist_info.get("total_bytes", 0)

        if total_files == 0:
            await workflow.execute_activity(
                "update_shard_status",
                args=[shard_id, "done"],
                start_to_close_timeout=timedelta(seconds=30),
            )
            return ShardResult(shard_id=shard_id, status="done")

        # Pipeline TaskInput with shard-specific name for staging path isolation
        task_for_pipeline = TaskInput(
            id=shard_input.task_id,
            name=shard_name,
            repo_id=shard_input.repo_id,
            source=shard_input.source,
            type=shard_input.type,
            category=shard_input.category,
            priority=shard_input.priority,
            size_gb=shard_input.size_bytes / (1024 ** 3),
        )

        # Process in batches
        completed_batches = len(completed) if completed else 0
        start_idx = completed_batches * BATCH_SIZE
        uploaded_bytes = int(total_bytes * min(start_idx, total_files) / total_files) if start_idx > 0 else 0
        uploaded_files = start_idx  # approximate resume count
        batch_markers = list(completed) if completed else []

        try:
            batch_num = completed_batches
            while start_idx < total_files:
                batch_num += 1
                current_batch_size = min(BATCH_SIZE, total_files - start_idx)

                result = await workflow.execute_activity(
                    "run_pipeline_batch",
                    args=[task_for_pipeline, local_filelist,
                          start_idx, current_batch_size, uploaded_bytes, total_bytes],
                    start_to_close_timeout=timedelta(days=7),
                    heartbeat_timeout=timedelta(minutes=10),
                    retry_policy=RetryPolicy(
                        initial_interval=timedelta(minutes=1),
                        maximum_attempts=3,
                        non_retryable_error_types=NON_RETRYABLE_ERRORS,
                    ),
                )

                uploaded_bytes += result.get("uploaded_bytes", 0)
                uploaded_files += result.get("uploaded_files", 0)
                start_idx += current_batch_size
                batch_markers.append(f"batch-{batch_num}-done")

                await workflow.execute_activity(
                    "save_progress",
                    args=[shard_name, batch_markers, shard_input.filelist_md5],
                    start_to_close_timeout=timedelta(seconds=30),
                )

                # Report shard progress (cumulative counts)
                await workflow.execute_activity(
                    "report_shard_progress",
                    args=[shard_id, uploaded_files,
                          uploaded_bytes, result.get("speed_mbps", 0)],
                    start_to_close_timeout=timedelta(seconds=30),
                )

        except Exception as e:
            error_msg = str(e)[:500]
            try:
                await workflow.execute_activity(
                    "cleanup_staging", args=[shard_name, True],
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=RetryPolicy(maximum_attempts=2),
                )
            except Exception:
                pass
            await workflow.execute_activity(
                "update_shard_status",
                args=[shard_id, "failed", error_msg],
                start_to_close_timeout=timedelta(seconds=30),
            )
            return ShardResult(shard_id=shard_id, status="failed", error=error_msg)

        # Cleanup and mark done
        try:
            await workflow.execute_activity(
                "clear_progress", args=[shard_name],
                start_to_close_timeout=timedelta(seconds=30),
            )
            await workflow.execute_activity(
                "cleanup_staging", args=[shard_name, False],
                start_to_close_timeout=timedelta(minutes=5),
            )
        except Exception:
            pass

        await workflow.execute_activity(
            "update_shard_status",
            args=[shard_id, "done"],
            start_to_close_timeout=timedelta(seconds=30),
        )

        return ShardResult(
            shard_id=shard_id,
            status="done",
            files_uploaded=total_files,
            bytes_uploaded=uploaded_bytes,
        )


@workflow.defn
class ShardedDownloadWorkflow:
    """Coordinator: list files, partition into shards, dispatch to workers.

    Replaces SplitDownloadWorkflow. All DB writes go through activities (R1).
    Uses return_exceptions=True on gather (R3).
    """

    @workflow.run
    async def run(self, task_input: TaskInput) -> TaskResult:
        task_id = task_input.id

        # Report starting
        await workflow.execute_activity(
            "report_to_dashboard",
            args=[task_id, "downloading", "listing files", 0, 0, 0, None, None],
            start_to_close_timeout=timedelta(seconds=30),
        )

        # Step 1: List repo files
        try:
            filelist_result = await workflow.execute_activity(
                "list_repo_files",
                args=[task_input],
                start_to_close_timeout=timedelta(minutes=30),
                heartbeat_timeout=timedelta(minutes=3),
                retry_policy=ACTIVITY_RETRY,
            )
        except Exception as e:
            error_msg = str(e)[:500]
            await workflow.execute_activity(
                "report_to_dashboard",
                args=[task_id, "failed", None, None, None, None, None, error_msg],
                start_to_close_timeout=timedelta(seconds=30),
            )
            return TaskResult(status="failed", error=error_msg)

        total_files = filelist_result.get("count", 0)
        filelist_path = filelist_result["path"]
        # Later activities read filelist_path from local disk — they MUST run
        # on the worker that produced it, pinned via its personal queue.
        listing_queue = filelist_result.get("worker_queue", "download-workers")

        if total_files == 0:
            await workflow.execute_activity(
                "report_to_dashboard",
                args=[task_id, "done", "empty repo", 100, 0, 0, None, None],
                start_to_close_timeout=timedelta(seconds=30),
            )
            return TaskResult(status="done")

        # Step 1b: BOS-aware resume — drop files already uploaded (key + size
        # match under the task's target prefix). One paginated BOS list.
        try:
            filter_result = await workflow.execute_activity(
                "filter_filelist_against_bos",
                args=[filelist_path, task_input],
                task_queue=listing_queue,
                start_to_close_timeout=timedelta(minutes=15),
                heartbeat_timeout=timedelta(minutes=3),
                retry_policy=ACTIVITY_RETRY,
            )
        except Exception as e:
            error_msg = f"BOS resume filter failed: {str(e)[:400]}"
            await workflow.execute_activity(
                "report_to_dashboard",
                args=[task_id, "failed", None, None, None, None, None, error_msg],
                start_to_close_timeout=timedelta(seconds=30),
            )
            return TaskResult(status="failed", error=error_msg)
        skipped_files = filter_result["skipped_files"]
        skipped_gb = filter_result["skipped_bytes"] / (1024 ** 3)
        total_files = filter_result["remaining_files"]
        total_bytes = filter_result["remaining_bytes"]
        filtered_path = filter_result["filtered_path"]

        # Persist the filter result on the task row (phase gets overwritten fast)
        await workflow.execute_activity(
            "report_resume_info",
            args=[task_id, skipped_files, skipped_gb],
            start_to_close_timeout=timedelta(seconds=30),
        )

        if total_files == 0:
            await workflow.execute_activity(
                "report_to_dashboard",
                args=[task_id, "done",
                      f"all {skipped_files} files already on BOS", 100, 0,
                      round(skipped_gb, 2), None, None],
                start_to_close_timeout=timedelta(seconds=30),
            )
            return TaskResult(status="done")

        # Step 2: Find idle workers (exclude this task's own claim/shards)
        idle_workers = await workflow.execute_activity(
            "query_idle_workers",
            args=[task_input.source, task_id],
            start_to_close_timeout=timedelta(seconds=30),
        )

        # Step 3: Determine shard count — user-requested wins, capped by idle
        requested = getattr(task_input, "shard_count", 0) or 0
        if requested > 0:
            num_shards = max(1, min(requested, len(idle_workers) or 1))
        elif total_bytes < AUTO_SHARD_THRESHOLD or len(idle_workers) <= 1:
            num_shards = 1
        else:
            num_shards = min(
                len(idle_workers),
                max(1, total_bytes // SHARD_MIN_BYTES),
            )

        staging_dir = f"/data/staging/{task_input.name}"

        # Step 4: Partition + upload filelists to BOS (always, even for 1 shard).
        # Pinned: reads the filtered filelist from the listing worker's disk.
        try:
            raw_parts = await workflow.execute_activity(
                "partition_files_greedy",
                args=[filtered_path, num_shards, staging_dir],
                task_queue=listing_queue,
                start_to_close_timeout=timedelta(minutes=10),
                retry_policy=ACTIVITY_RETRY,
            )
        except Exception as e:
            error_msg = f"Partition failed: {str(e)[:400]}"
            await workflow.execute_activity(
                "report_to_dashboard",
                args=[task_id, "failed", None, None, None, None, None, error_msg],
                start_to_close_timeout=timedelta(seconds=30),
            )
            return TaskResult(status="failed", error=error_msg)
        partitions = [{**p, "shard_index": i} for i, p in enumerate(raw_parts)]

        # Step 5: Create shard rows in SQLite (via activity — R1)
        shard_ids = await workflow.execute_activity(
            "create_shards_in_db",
            args=[task_id, partitions],
            start_to_close_timeout=timedelta(seconds=60),
        )

        phase_msg = f"dispatching {num_shards} shards"
        if requested > 0:
            phase_msg += f" (requested={requested} got={num_shards})"
        if skipped_files > 0:
            phase_msg += f", skipped {skipped_files} files ({skipped_gb:.1f} GB) already on BOS"
        await workflow.execute_activity(
            "report_to_dashboard",
            args=[task_id, "downloading", phase_msg, 0, 0, 0, None, None],
            start_to_close_timeout=timedelta(seconds=30),
        )

        # Step 6: Start child workflows on worker queues
        workers_to_use = idle_workers[:num_shards] if idle_workers else ["download-workers"]
        child_handles = []

        for i, (shard_id, partition) in enumerate(zip(shard_ids, partitions)):
            worker_key = workers_to_use[i] if i < len(workers_to_use) else workers_to_use[0]
            queue = f"download-{worker_key}"

            shard_in = ShardInput(
                shard_id=shard_id,
                task_id=task_id,
                task_name=task_input.name,
                repo_id=task_input.repo_id,
                source=task_input.source,
                type=task_input.type,
                category=task_input.category,
                shard_index=i,
                filelist_key=partition["filelist_key"],
                filelist_md5=partition.get("filelist_md5", ""),
                priority=task_input.priority,
                size_bytes=partition["total_bytes"],
            )

            # Record assignment
            await workflow.execute_activity(
                "assign_shard_server",
                args=[shard_id, worker_key],
                start_to_close_timeout=timedelta(seconds=30),
            )

            handle = await workflow.start_child_workflow(
                ShardWorkerWorkflow.run,
                shard_in,
                id=f"shard-{shard_id}",
                task_queue=queue,
                retry_policy=RetryPolicy(maximum_attempts=3),
            )
            child_handles.append((shard_id, handle))

        # Step 7: Wait for all children (R3: return_exceptions=True)
        results = await asyncio.gather(
            *(h for _, h in child_handles),
            return_exceptions=True,
        )

        # Step 8: Process results
        total_uploaded = 0
        total_bytes_up = 0
        failed_shards = []

        # A shard counts as successful only if it positively says so. Every
        # other outcome — raised, cancelled, returned status="failed", or a
        # shape this loop doesn't recognise — lands in failed_shards, because
        # the alternative is reporting a task `done` that downloaded nothing.
        for (shard_id, _), result in zip(child_handles, results):
            # BaseException, not Exception: gather(return_exceptions=True)
            # hands back asyncio.CancelledError for a cancelled child, and
            # that is not an Exception — it matched neither branch here and
            # was silently dropped, i.e. counted as a shard that succeeded.
            if isinstance(result, BaseException):
                failed_shards.append(shard_id)
                try:
                    await workflow.execute_activity(
                        "update_shard_status",
                        args=[shard_id, "failed", str(result)[:200]],
                        start_to_close_timeout=timedelta(seconds=30),
                    )
                except Exception:
                    pass
            elif isinstance(result, ShardResult):
                # Bytes a shard moved before giving up are real — count them
                # either way so the aggregate stays honest.
                total_uploaded += result.files_uploaded
                total_bytes_up += result.bytes_uploaded
                # ShardWorkerWorkflow reports insufficient disk and batch
                # failure as a NORMAL RETURN carrying status="failed", not as
                # a raise. Ignoring .status here marked t-20260805-460d45
                # (molmobot-data) `done` at 0 of 9611 GB on 2026-08-06 while
                # its one shard row read `failed`. ShardResult.status defaults
                # to "done", so the type alone never says "this succeeded" —
                # only this check does. The shard already marked its own row
                # failed on that path, so no update_shard_status call here.
                if result.status != "done":
                    failed_shards.append(shard_id)
            else:
                failed_shards.append(shard_id)

        # Final aggregation
        await workflow.execute_activity(
            "aggregate_task_from_shards",
            args=[task_id],
            start_to_close_timeout=timedelta(seconds=60),
        )

        if failed_shards:
            error_msg = f"{len(failed_shards)}/{num_shards} shards failed"
            await workflow.execute_activity(
                "report_to_dashboard",
                args=[task_id, "failed", None, None, None, None, None, error_msg],
                start_to_close_timeout=timedelta(seconds=30),
            )
            return TaskResult(
                status="failed",
                # Carried, not dropped: the counters are already computed, the
                # legacy multi-worker path returns them on failure too, and a
                # failed task should not also lose the record of what it did
                # move. Nothing outside the workflow layer reads these — the
                # dashboard aggregate comes from the shard rows above.
                files_uploaded=total_uploaded,
                bytes_uploaded=total_bytes_up,
                error=error_msg,
            )

        total_gb = total_bytes_up / (1024 ** 3)
        await workflow.execute_activity(
            "report_to_dashboard",
            args=[task_id, "done", None, 100, 0, round(total_gb, 2), None, None],
            start_to_close_timeout=timedelta(seconds=30),
        )

        return TaskResult(
            status="done",
            files_uploaded=total_uploaded,
            bytes_uploaded=total_bytes_up,
        )


# ── Pool dispatch (work-stealing) ───────────────────────────────────

# A pool batch waits in a shared queue behind every other task's batches, so
# its timers are sized for queueing, not just for running:
#   schedule_to_close  the ONLY timer covering queue wait + all retries. An
#                      empty pool (every worker down) would otherwise sit
#                      silent forever instead of failing the task.
#   start_to_close     one attempt's ceiling — a 32 GiB batch on a slow link.
#   heartbeat          run_pool_batch beats through preflight and the engine.
# Deliberately no schedule_to_start: a retry would land back on the same
# empty queue, so timing out on "not started yet" only destroys work.
POOL_BATCH_SCHEDULE_TO_CLOSE = timedelta(hours=48)
POOL_BATCH_START_TO_CLOSE = timedelta(hours=12)
POOL_BATCH_HEARTBEAT = timedelta(minutes=10)


def pool_task_queue(source: str) -> str:
    """The shared pool queue for a source.

    Named here rather than inlined so the deploy side (which must start a
    worker per pool queue, or batches queue forever with no error) can grep
    for the string instead of reproducing it from prose. The workflow cannot
    import `dlm.web.fleet`, so this is the single definition.
    """
    return "pool-ms" if source == "modelscope" else "pool-hf"

# 5 minutes, not 30 seconds: a batch that failed because its worker died
# should not be re-dispatched onto the same still-dying worker three times in
# 90 seconds. Temporal has no anti-affinity, so backoff is the only lever.
POOL_BATCH_RETRY = RetryPolicy(
    initial_interval=timedelta(minutes=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=30),
    maximum_attempts=3,
    non_retryable_error_types=NON_RETRYABLE_ERRORS + [
        "BatchLimitExceededError",
        "PoolBatchMismatch",
    ],
)


@workflow.defn
class PoolDownloadWorkflow:
    """Coordinator for pool (work-stealing) dispatch.

    Where ShardedDownloadWorkflow cuts the repo into one shard per worker up
    front and hands each worker a child workflow, this cuts it into many
    small batches and keeps a sliding window of them in flight on a shared
    queue. Any worker that finishes a batch takes the next one, so a worker
    that joins late, recovers, or simply runs faster contributes without a
    reshard — and two tasks can share a fleet without either one having to
    own whole machines.

    Determinism rules this loop must keep (a replay that violates one fails
    with NonDeterminismError after the next deploy, not now):
      * `workflow.start_activity` WITHOUT await — awaiting serializes the
        whole window into one batch at a time.
      * `workflow.wait(..., FIRST_COMPLETED)`, never `asyncio.wait`: the
        latter returns real sets, whose iteration order varies per process.
      * completed handles processed in batch-index order, and every
        `h.result()` wrapped — an uncaught ActivityError kills the workflow
        and cancels every in-flight batch with it.
    """

    @workflow.run
    async def run(self, task_input: TaskInput) -> TaskResult:
        task_id = task_input.id

        await workflow.execute_activity(
            "report_to_dashboard",
            args=[task_id, "downloading", "listing files", 0, 0, 0, None, None],
            start_to_close_timeout=timedelta(seconds=30),
        )

        # Step 1: list — same activity the sharded path uses, and like it,
        # everything downstream reads the filelist off this worker's disk, so
        # those activities are pinned to its personal queue.
        try:
            filelist_result = await workflow.execute_activity(
                "list_repo_files",
                args=[task_input],
                start_to_close_timeout=timedelta(minutes=30),
                heartbeat_timeout=timedelta(minutes=3),
                retry_policy=ACTIVITY_RETRY,
            )
        except Exception as e:
            return await self._fail(task_id, str(e)[:500])

        listing_queue = filelist_result.get("worker_queue", "download-workers")
        if filelist_result.get("count", 0) == 0:
            await workflow.execute_activity(
                "report_to_dashboard",
                args=[task_id, "done", "empty repo", 100, 0, 0, None, None],
                start_to_close_timeout=timedelta(seconds=30),
            )
            return TaskResult(status="done")

        # Step 2: BOS-aware resume filter (pinned to the listing worker)
        try:
            filter_result = await workflow.execute_activity(
                "filter_filelist_against_bos",
                args=[filelist_result["path"], task_input],
                task_queue=listing_queue,
                start_to_close_timeout=timedelta(minutes=15),
                heartbeat_timeout=timedelta(minutes=3),
                retry_policy=ACTIVITY_RETRY,
            )
        except Exception as e:
            return await self._fail(task_id, f"BOS resume filter failed: {str(e)[:400]}")

        skipped_files = filter_result["skipped_files"]
        skipped_gb = filter_result["skipped_bytes"] / (1024 ** 3)

        await workflow.execute_activity(
            "report_resume_info",
            args=[task_id, skipped_files, skipped_gb],
            start_to_close_timeout=timedelta(seconds=30),
        )

        if filter_result["remaining_files"] == 0:
            await workflow.execute_activity(
                "report_to_dashboard",
                args=[task_id, "done",
                      f"all {skipped_files} files already on BOS", 100, 0,
                      round(skipped_gb, 2), None, None],
                start_to_close_timeout=timedelta(seconds=30),
            )
            return TaskResult(status="done")

        # Step 3: chunk into batches (pinned — reads the filtered filelist)
        try:
            chunks = await workflow.execute_activity(
                "chunk_filelist",
                args=[filter_result["filtered_path"], task_input],
                task_queue=listing_queue,
                start_to_close_timeout=timedelta(minutes=15),
                heartbeat_timeout=timedelta(minutes=3),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=30),
                    maximum_attempts=3,
                    non_retryable_error_types=NON_RETRYABLE_ERRORS + ["BatchLimitExceededError"],
                ),
            )
        except Exception as e:
            return await self._fail(task_id, f"Chunking failed: {str(e)[:400]}")

        batch_keys = chunks["batch_keys"]
        batch_counts = chunks["counts"]
        batch_bytes = chunks["bytes"]
        num_batches = len(batch_keys)
        if num_batches == 0:
            await workflow.execute_activity(
                "report_to_dashboard",
                args=[task_id, "done", "nothing left to download", 100, 0,
                      round(skipped_gb, 2), None, None],
                start_to_close_timeout=timedelta(seconds=30),
            )
            return TaskResult(status="done")

        # Step 4: batch rows. Idempotent — a retry re-sends the identical
        # body and resets any row a dead attempt left running.
        batch_infos = [
            {
                "shard_index": i,
                "filelist_key": batch_keys[i],
                "total_files": batch_counts[i],
                "total_bytes": batch_bytes[i],
            }
            for i in range(num_batches)
        ]
        try:
            create_result = await workflow.execute_activity(
                "create_pool_batches_in_db",
                args=[task_id, batch_infos],
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=30),
                    maximum_attempts=3,
                    non_retryable_error_types=NON_RETRYABLE_ERRORS + ["PoolBatchMismatch"],
                ),
            )
        except Exception as e:
            return await self._fail(task_id, f"Batch row creation failed: {str(e)[:400]}")

        if create_result.get("ignored"):
            # An operator stopped the task while we were listing. Its status is
            # already whatever they set; do not overwrite it.
            return TaskResult(status="paused", error="task stopped before dispatch")

        shard_ids = create_result["shard_ids"]

        phase = f"pool: {num_batches} batches"
        if skipped_files:
            phase += f", skipped {skipped_files} files ({skipped_gb:.1f} GB) on BOS"
        await workflow.execute_activity(
            "report_to_dashboard",
            args=[task_id, "downloading", phase, 0, 0, 0, None, None],
            start_to_close_timeout=timedelta(seconds=30),
        )

        # Step 5: the window loop. Bookkeeping-activity exhaustion (S1 down
        # past ACTIVITY_RETRY's ~15 minutes) would otherwise propagate out of
        # the workflow: in-flight batches cancelled mid-upload, no terminal
        # report, and the task row left saying `downloading` behind a dead
        # workflow — the "looks alive, nothing running" state the orphan
        # reconciler exists to mop up, reached by a routine hiccup.
        try:
            outcome = await self._run_window_loop(
                task_input, shard_ids, batch_keys, list(range(num_batches))
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            return await self._fail(task_id, f"pool dispatch failed: {str(e)[:400]}")
        if outcome["stopped"]:
            # An operator stopped the task mid-window: every remaining batch
            # declined to run. Its status is already whatever they set, and
            # reporting `done` here would both lie and un-stop it.
            return TaskResult(status="paused", error="task stopped during dispatch")

        # Step 6: one re-dispatch round for whatever failed. A single poison
        # worker can eat a batch's three attempts and fail an otherwise
        # healthy task; a second pass usually lands the batch elsewhere.
        retried_failures: list[int] = []
        if outcome["failed"]:
            await workflow.execute_activity(
                "report_to_dashboard",
                args=[task_id, "downloading",
                      f"retrying {len(outcome['failed'])} failed batches",
                      None, None, None, None, None],
                start_to_close_timeout=timedelta(seconds=30),
            )
            try:
                retry_outcome = await self._run_window_loop(
                    task_input, shard_ids, batch_keys, sorted(outcome["failed"])
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                return await self._fail(
                    task_id, f"pool re-dispatch failed: {str(e)[:400]}",
                    files_uploaded=outcome["uploaded_files"],
                    bytes_uploaded=outcome["uploaded_bytes"])
            if retry_outcome["stopped"]:
                return TaskResult(status="paused", error="task stopped during dispatch")
            retried_failures = retry_outcome["failed"]
            outcome["uploaded_files"] += retry_outcome["uploaded_files"]
            outcome["uploaded_bytes"] += retry_outcome["uploaded_bytes"]

        await workflow.execute_activity(
            "aggregate_task_from_shards",
            args=[task_id],
            start_to_close_timeout=timedelta(seconds=60),
        )

        if retried_failures:
            return await self._fail(
                task_id,
                f"{len(retried_failures)}/{num_batches} batches failed after retry",
                files_uploaded=outcome["uploaded_files"],
                bytes_uploaded=outcome["uploaded_bytes"],
            )

        total_gb = outcome["uploaded_bytes"] / (1024 ** 3)
        await workflow.execute_activity(
            "report_to_dashboard",
            args=[task_id, "done", None, 100, 0, round(total_gb, 2), None, None],
            start_to_close_timeout=timedelta(seconds=30),
        )
        return TaskResult(
            status="done",
            files_uploaded=outcome["uploaded_files"],
            bytes_uploaded=outcome["uploaded_bytes"],
        )

    async def _fail(self, task_id: str, error: str, files_uploaded: int = 0,
                    bytes_uploaded: int = 0) -> TaskResult:
        """Report the task failed, carrying whatever did land.

        A partly-successful task's bytes are real and already on BOS; a result
        that reports zero would make the workflow history disagree with the
        aggregate and with the bucket.

        Releases batch rows first: an abnormal exit leaves rows attributing
        workers that stopped, same as the cancel path. Harmless before any row
        exists (the endpoint just reports zero released). Note the dashboard
        report below carries no retry policy — Temporal's default is unbounded,
        so this blocks until S1 answers rather than failing fast. That is
        deliberate and matches the sharded coordinator: a task whose failure
        never got reported is the orphan state we are avoiding.
        """
        try:
            await self._release(task_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            # Cleanup must never be the reason a failure goes unreported.
            pass
        await workflow.execute_activity(
            "report_to_dashboard",
            args=[task_id, "failed", None, None, None, None, None, error],
            start_to_close_timeout=timedelta(seconds=30),
        )
        return TaskResult(status="failed", error=error,
                          files_uploaded=files_uploaded,
                          bytes_uploaded=bytes_uploaded)

    async def _run_window_loop(
        self,
        task_input: TaskInput,
        shard_ids: list,
        batch_keys: list,
        pending: list,
    ) -> dict:
        """Keep up to `window` batches in flight until `pending` is drained.

        `pending` is a list of batch indices, consumed in order. Returns
        `{"stopped": bool, "failed": [idx...], "uploaded_files": int,
        "uploaded_bytes": int}`. `stopped` means a batch reported that the
        parent task is no longer runnable — the caller must not report a
        terminal status of its own.
        """
        task_id = task_input.id
        queue = pool_task_queue(task_input.source)
        # Priority 0-2 is the queue-jump band; Temporal's in-queue priority is
        # a free tie-break on top of the window, never the fairness mechanism.
        priority = Priority(priority_key=1 if (task_input.priority or 0) <= 2 else 3)

        remaining = list(pending)
        in_flight: dict = {}       # batch_index -> activity handle
        window = 1                 # first wake recomputes it from live P
        uploaded_files = 0
        uploaded_bytes = 0
        failed: list = []
        stopped = False

        try:
            while remaining or in_flight:
                # Fill the window. start_activity, NOT execute_activity: an
                # await here would run the window one batch at a time.
                while remaining and len(in_flight) < window:
                    idx = remaining.pop(0)
                    in_flight[idx] = workflow.start_activity(
                        "run_pool_batch",
                        args=[task_input, idx, batch_keys[idx]],
                        task_queue=queue,
                        schedule_to_close_timeout=POOL_BATCH_SCHEDULE_TO_CLOSE,
                        start_to_close_timeout=POOL_BATCH_START_TO_CLOSE,
                        heartbeat_timeout=POOL_BATCH_HEARTBEAT,
                        retry_policy=POOL_BATCH_RETRY,
                        cancellation_type=ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
                        priority=priority,
                    )

                # workflow.wait, never asyncio.wait — the latter's real sets
                # iterate in process-dependent order and replay explodes.
                await workflow.wait(
                    list(in_flight.values()), return_when=asyncio.FIRST_COMPLETED
                )

                # Sorted by batch index so the sequence of activity calls this
                # produces is identical on replay.
                results = []
                for idx in sorted(in_flight):
                    handle = in_flight[idx]
                    if not handle.done():
                        continue
                    try:
                        stats = handle.result()
                    except Exception as e:
                        failed.append(idx)
                        results.append({
                            "batch_index": idx,
                            "shard_id": shard_ids[idx],
                            "status": "failed",
                            "error": str(e)[:500],
                        })
                        continue
                    if stats and stats.get("ignored"):
                        # The batch found the parent task terminal and declined
                        # to run — an operator stopped it. Stop dispatching:
                        # draining the rest one `ignored` at a time would cost
                        # a wake each and end by reporting `done` for a task
                        # that downloaded nothing. Rows stay as they are; the
                        # cleanup below releases whatever still claims a worker.
                        stopped = True
                        continue
                    uploaded_files += (stats or {}).get("uploaded_files", 0)
                    uploaded_bytes += (stats or {}).get("uploaded_bytes", 0)
                    results.append({
                        "batch_index": idx,
                        "shard_id": shard_ids[idx],
                        "status": "done",
                        "error": None,
                    })

                for r in results:
                    in_flight.pop(r["batch_index"], None)
                # Handles that finished with `ignored` are done too, and must
                # not be waited on again.
                for idx in [i for i, h in in_flight.items() if h.done()]:
                    in_flight.pop(idx, None)

                if stopped:
                    remaining = []
                    # Neither cleanup may turn an operator's pause into a
                    # `failed` report: the task is stopped, and claiming a
                    # terminal status on their behalf is the very thing the
                    # stop is protecting against.
                    try:
                        if results:
                            # Batches that really finished before the stop still
                            # deserve their terminal rows; the endpoint's own
                            # TERMINAL guard will drop them if the task is
                            # already stopped, which is correct either way.
                            await self._record(task_id, results)
                        await self._release(task_id)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        pass
                    break

                # One bookkeeping activity per wake: writes the terminal batch
                # rows and returns the recomputed window.
                window_info = await self._record(task_id, results)
                window = max(1, int((window_info or {}).get("window", 1)))
        except asyncio.CancelledError:
            # Shielded: a bare activity call here would itself be cancelled,
            # leaving rows claiming workers that have already stopped. Bounded
            # (see _release) — an unbounded shielded retry would hold a
            # cancelled workflow open indefinitely while S1 is wedged, and
            # pause/reshard both wait for this workflow to close.
            await asyncio.shield(self._release(task_id))
            raise

        return {
            "stopped": stopped,
            "failed": failed,
            "uploaded_files": uploaded_files,
            "uploaded_bytes": uploaded_bytes,
        }

    async def _record(self, task_id: str, results: list) -> dict:
        """Write finished batches' terminal rows and read the next window."""
        return await workflow.execute_activity(
            "record_batches_and_window",
            args=[task_id, results],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=ACTIVITY_RETRY,
        )

    async def _release(self, task_id: str):
        """Release rows that still claim a worker.

        Bounded on purpose: this runs inside `asyncio.shield` on the
        cancellation path, so the workflow cannot close until it resolves.
        `schedule_to_close` caps the whole chain — without it, the default
        unbounded retry policy would keep a paused task in "cancel requested"
        for as long as S1 stays unreachable, and both pause and reshard block
        on the workflow actually closing.
        """
        return await workflow.execute_activity(
            "release_pool_batches",
            args=[task_id],
            schedule_to_close_timeout=timedelta(minutes=10),
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=15),
                maximum_attempts=5,
            ),
        )
