"""Temporal workflow definitions — orchestrate the download lifecycle."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Optional

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
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
        shard_name = f"{shard_input.task_name}/shard-{shard_input.shard_index}"

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

        for (shard_id, _), result in zip(child_handles, results):
            if isinstance(result, Exception):
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
                total_uploaded += result.files_uploaded
                total_bytes_up += result.bytes_uploaded

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
            return TaskResult(status="failed", error=error_msg)

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
