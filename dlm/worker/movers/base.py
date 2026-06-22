"""Abstract base for movers."""

from abc import ABC, abstractmethod
from pathlib import Path
from threading import Event
from typing import Callable


class Mover(ABC):
    @abstractmethod
    def move(
        self,
        source_dir: Path,
        task,
        progress_callback: Callable[[int, int], None],
        cancel_event: Event,
    ) -> None:
        """Move source_dir content to permanent storage.
        progress_callback(bytes_uploaded, total_bytes).
        Raises TaskError on failure."""
        pass
