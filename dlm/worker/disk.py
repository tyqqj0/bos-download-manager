"""Disk space management for download workers."""

import shutil
import time
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

STAGING_PATH = Path("/data/staging")
WARNING_RATIO = 0.85
CRITICAL_RATIO = 0.92
CLEANUP_AGE_HOURS = 48
SAFETY_MARGIN_GB = 20


class DiskManager:
    def __init__(self, staging_path: Path = STAGING_PATH):
        self.staging_path = staging_path
        self.staging_path.mkdir(parents=True, exist_ok=True)

    def available_gb(self) -> float:
        stat = shutil.disk_usage(self.staging_path)
        return stat.free / (1024 ** 3)

    def usage_ratio(self) -> float:
        stat = shutil.disk_usage(self.staging_path)
        return stat.used / stat.total

    def preflight_check(self, estimated_gb: float) -> tuple:
        """Pre-flight: can we start this download?
        Returns (ok: bool, reason: str)."""
        avail = self.available_gb()
        ratio = self.usage_ratio()

        if ratio > CRITICAL_RATIO:
            return False, f"Disk critically full ({ratio:.0%}, {avail:.1f}GB free)"
        if estimated_gb > 0 and estimated_gb > avail - SAFETY_MARGIN_GB:
            return False, f"Insufficient space: need ~{estimated_gb:.0f}GB, have {avail:.0f}GB free"
        return True, ""

    def pressure_level(self) -> str:
        """Check current disk pressure: 'ok', 'warning', 'critical'."""
        ratio = self.usage_ratio()
        if ratio > CRITICAL_RATIO:
            return "critical"
        if ratio > WARNING_RATIO:
            return "warning"
        return "ok"

    def cleanup_stale(self, active_task_ids: set = None) -> list:
        """Remove staging directories older than CLEANUP_AGE_HOURS
        that don't belong to active tasks."""
        if active_task_ids is None:
            active_task_ids = set()
        removed = []
        cutoff = time.time() - CLEANUP_AGE_HOURS * 3600

        if not self.staging_path.exists():
            return removed

        for d in self.staging_path.iterdir():
            if not d.is_dir():
                continue
            if d.name in active_task_ids:
                continue
            try:
                mtime = d.stat().st_mtime
                if mtime < cutoff:
                    shutil.rmtree(d)
                    removed.append(d.name)
                    logger.info(f"Cleaned stale staging: {d.name}")
            except Exception as e:
                logger.warning(f"Failed to clean {d.name}: {e}")
        return removed

    def emergency_cleanup(self) -> float:
        """Aggressive cleanup: remove ALL staging dirs + hf_cache. Returns GB freed."""
        before = self.available_gb()

        if self.staging_path.exists():
            for d in self.staging_path.iterdir():
                if not d.is_dir():
                    continue
                try:
                    shutil.rmtree(d, ignore_errors=True)
                except Exception as e:
                    logger.warning(f"Failed to remove {d.name}: {e}")

        hf_cache = Path("/tmp/hf_cache")
        if hf_cache.exists():
            shutil.rmtree(hf_cache, ignore_errors=True)

        freed = self.available_gb() - before
        logger.info(f"Emergency cleanup freed ~{freed:.1f}GB")
        return freed
