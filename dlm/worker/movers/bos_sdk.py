"""BOS SDK direct upload mover — bypasses FUSE for fast writes."""

import json
import os
import shutil
import tarfile
import logging
from pathlib import Path
from threading import Event
from typing import Callable, List
from concurrent.futures import ThreadPoolExecutor, as_completed

from ...core.config import load_config
from ...core.bos import bos_target, create_bos_client, upload_file, MULTIPART_THRESHOLD
from ..errors import TaskError, ErrorClass
from .base import Mover

logger = logging.getLogger(__name__)

UPLOAD_WORKERS = 32
TAR_WORKERS = 4
EXCLUDE_PATTERNS = (".incomplete", ".huggingface", ".cache", "__pycache__")

# Tar mode triggers when avg file size < 5MB AND file count >= 100
SMALL_FILE_THRESHOLD = 50 * 1024 * 1024
MIN_FILES_FOR_TAR = 50


class BOSSDKMover(Mover):
    def __init__(self):
        config = load_config()
        self.client = create_bos_client(
            config["BAIDU_AK"], config["BAIDU_SK"], config["BOS_ENDPOINT"]
        )

    def move(
        self,
        source_dir: Path,
        task,
        progress_callback: Callable[[int, int], None],
        cancel_event: Event,
    ) -> None:
        bucket, prefix = self._resolve_target(task)

        files = list(self._collect_files(source_dir))
        if not files:
            logger.warning(f"No files to upload in {source_dir}")
            return

        total_bytes = sum(f.stat().st_size for f in files)
        avg_size = total_bytes / len(files) if files else float("inf")

        if avg_size < SMALL_FILE_THRESHOLD and len(files) >= MIN_FILES_FOR_TAR:
            logger.info(
                f"Tar mode: {len(files)} files, avg {avg_size/1024:.0f}KB"
            )
            self._move_tarred(
                source_dir, bucket, prefix, files, total_bytes,
                progress_callback, cancel_event,
            )
        else:
            self._move_parallel(
                source_dir, bucket, prefix, files, total_bytes,
                progress_callback, cancel_event,
            )

    def _move_parallel(
        self,
        source_dir: Path,
        bucket: str,
        prefix: str,
        files: List[Path],
        total_bytes: int,
        progress_callback: Callable[[int, int], None],
        cancel_event: Event,
    ) -> None:
        """Standard mode: upload each file individually with thread pool."""
        uploaded_bytes = 0
        failed = []

        logger.info(
            f"Uploading {len(files)} files ({total_bytes / 1024**3:.1f}GB) "
            f"to {bucket}/{prefix}"
        )

        with ThreadPoolExecutor(max_workers=UPLOAD_WORKERS) as pool:
            futures = {}
            for f in files:
                if cancel_event.is_set():
                    break
                rel = f.relative_to(source_dir)
                key = prefix + rel.as_posix()
                futures[pool.submit(self._upload_one, bucket, key, f)] = f

            for future in as_completed(futures):
                if cancel_event.is_set():
                    for remaining in futures:
                        remaining.cancel()
                    raise TaskError("Upload cancelled", ErrorClass.TRANSIENT)

                f = futures[future]
                try:
                    future.result()
                    size = f.stat().st_size
                    f.unlink()
                    uploaded_bytes += size
                    progress_callback(uploaded_bytes, total_bytes)
                except Exception as e:
                    logger.error(f"Failed to upload {f}: {e}")
                    failed.append((f, str(e)))

        if failed:
            raise TaskError(
                f"Failed to upload {len(failed)}/{len(files)} files. "
                f"First error: {failed[0][1]}",
                ErrorClass.TRANSIENT,
            )

        self._cleanup_empty_dirs(source_dir)
        logger.info(f"Upload complete: {len(files)} files, {uploaded_bytes / 1024**3:.1f}GB")

    def _move_tarred(
        self,
        source_dir: Path,
        bucket: str,
        prefix: str,
        files: List[Path],
        total_bytes: int,
        progress_callback: Callable[[int, int], None],
        cancel_event: Event,
    ) -> None:
        """Tar mode: pack subdirectories into .tar files before uploading."""
        # Group files by top-level subdirectory
        subdirs = set()
        root_files = []
        for f in files:
            rel = f.relative_to(source_dir)
            if len(rel.parts) > 1:
                subdirs.add(rel.parts[0])
            else:
                root_files.append(f)

        uploaded_bytes = 0
        failed = []
        tar_prefix = prefix.rstrip("/") + "/_tars/"

        logger.info(
            f"Tar upload: {len(subdirs)} subdirs + {len(root_files)} root files, "
            f"{total_bytes / 1024**3:.1f}GB total"
        )

        # Tar and upload subdirectories in parallel
        with ThreadPoolExecutor(max_workers=TAR_WORKERS) as pool:
            futures = {}
            for subdir_name in subdirs:
                if cancel_event.is_set():
                    break
                subdir_path = source_dir / subdir_name
                tar_path = source_dir / f"{subdir_name}.tar"
                key = tar_prefix + f"{subdir_name}.tar"
                futures[pool.submit(
                    self._tar_and_upload, subdir_path, tar_path, bucket, key
                )] = subdir_name

            for future in as_completed(futures):
                if cancel_event.is_set():
                    for remaining in futures:
                        remaining.cancel()
                    raise TaskError("Upload cancelled", ErrorClass.TRANSIENT)

                subdir_name = futures[future]
                try:
                    tar_size = future.result()
                    uploaded_bytes += tar_size
                    progress_callback(uploaded_bytes, total_bytes)
                except Exception as e:
                    logger.error(f"Failed to tar+upload {subdir_name}: {e}")
                    failed.append((subdir_name, str(e)))

        # Upload root-level files directly
        for f in root_files:
            if cancel_event.is_set():
                raise TaskError("Upload cancelled", ErrorClass.TRANSIENT)
            rel = f.relative_to(source_dir)
            key = prefix + rel.as_posix()
            try:
                self._upload_one(bucket, key, f)
                size = f.stat().st_size
                f.unlink()
                uploaded_bytes += size
                progress_callback(uploaded_bytes, total_bytes)
            except Exception as e:
                logger.error(f"Failed to upload {f}: {e}")
                failed.append((str(f), str(e)))

        # Upload manifest
        manifest = {
            "format": "tarred",
            "subdirs": sorted(subdirs),
            "root_files": [f.name for f in root_files],
        }
        manifest_key = prefix.rstrip("/") + "/_manifest.json"
        manifest_bytes = json.dumps(manifest, ensure_ascii=False, indent=2).encode()
        try:
            self.client.put_object(bucket, manifest_key, manifest_bytes)
        except Exception as e:
            logger.warning(f"Failed to upload manifest: {e}")

        if failed:
            raise TaskError(
                f"Failed {len(failed)} tar uploads. First: {failed[0][1]}",
                ErrorClass.TRANSIENT,
            )

        self._cleanup_empty_dirs(source_dir)
        logger.info(
            f"Tar upload complete: {len(subdirs)} tars + {len(root_files)} files, "
            f"{uploaded_bytes / 1024**3:.1f}GB"
        )

    def _tar_and_upload(self, subdir_path: Path, tar_path: Path, bucket: str, key: str) -> int:
        """Tar a subdirectory, upload the tar, clean up. Returns tar size in bytes."""
        with tarfile.open(str(tar_path), "w") as tf:
            tf.add(str(subdir_path), arcname=subdir_path.name)

        tar_size = tar_path.stat().st_size
        logger.debug(f"Uploading tar {tar_path.name} ({tar_size / 1024**2:.0f}MB)")

        upload_file(self.client, bucket, key, str(tar_path))

        tar_path.unlink()
        shutil.rmtree(subdir_path, ignore_errors=True)
        return tar_size

    def _upload_one(self, bucket: str, key: str, local_path: Path):
        upload_file(self.client, bucket, key, str(local_path))

    def _resolve_target(self, task) -> tuple:
        """Determine bucket and prefix from task type/category."""
        return bos_target(task)

    def _collect_files(self, source_dir: Path):
        """Walk source_dir, skip excluded patterns."""
        for f in source_dir.rglob("*"):
            if not f.is_file():
                continue
            if any(exc in f.parts for exc in EXCLUDE_PATTERNS):
                continue
            if any(f.name.endswith(exc) for exc in EXCLUDE_PATTERNS):
                continue
            yield f

    def _cleanup_empty_dirs(self, source_dir: Path):
        """Remove empty directories after upload."""
        for d in sorted(source_dir.rglob("*"), key=lambda p: len(p.parts), reverse=True):
            if d.is_dir():
                try:
                    d.rmdir()
                except OSError:
                    pass
