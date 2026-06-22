"""Abstract base for download handlers."""

from abc import ABC, abstractmethod
from pathlib import Path
from threading import Event
from typing import Optional, Callable


class DownloadHandler(ABC):
    @abstractmethod
    def validate(self, task) -> None:
        """Pre-flight check. Raises TaskError if download cannot proceed."""
        pass

    @abstractmethod
    def estimate_size(self, task) -> Optional[float]:
        """Return estimated size in GB, or None if unknown."""
        pass

    @abstractmethod
    def download(
        self,
        task,
        staging_dir: Path,
        progress_callback: Callable[[int, int, float], None],
        cancel_event: Event,
    ) -> None:
        """Download to staging_dir.
        progress_callback(downloaded_bytes, total_bytes, speed_bytes_per_sec)
        Checks cancel_event for graceful abort."""
        pass
