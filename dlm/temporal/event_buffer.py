"""Event Buffer — at-least-once delivery of monitoring events to S1.

Events are stored in a local ring buffer and flushed to S1 in batches.
On POST failure, events are retained and retried with exponential backoff.
This ensures network blips don't cause permanent data loss.

Usage:
    buffer = EventBuffer(server_key="w1")
    await buffer.start()
    buffer.emit("file_downloaded", {"file": "a.parquet", "size_bytes": 1024, "duration_s": 2.3})
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from collections import deque
from typing import Optional

logger = logging.getLogger(__name__)

MAX_BUFFER_SIZE = 5000      # ring buffer capacity (oldest events dropped if full)
FLUSH_INTERVAL = 5          # batch POST every 5 seconds
MAX_BATCH_SIZE = 200        # max events per POST
RETRY_BACKOFF = [1, 2, 5, 10, 30, 60]  # seconds between retries


class EventBuffer:
    """Buffered event delivery with retry."""

    def __init__(self, server_key: str, coordinator: Optional[str] = None):
        self.server_key = server_key
        self.coordinator = coordinator or os.environ.get(
            "DLM_COORDINATOR", "http://154.85.43.52:8080"
        )
        self._buffer: deque = deque(maxlen=MAX_BUFFER_SIZE)
        self._flush_task: Optional[asyncio.Task] = None
        self._retry_count = 0
        self._running = False

    def emit(self, event_type: str, data: dict):
        """Add event to buffer (non-blocking, thread-safe via deque)."""
        event = {
            "type": event_type,
            "server_key": self.server_key,
            "timestamp": time.time(),
            "data": data,
        }
        self._buffer.append(event)

    @property
    def pending_count(self) -> int:
        return len(self._buffer)

    async def start(self):
        """Start the background flush loop."""
        self._running = True
        self._flush_task = asyncio.create_task(self._flush_loop())

    async def stop(self):
        """Stop the flush loop and attempt final flush."""
        self._running = False
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        # Best-effort final flush
        await self._flush_once()

    async def _flush_loop(self):
        """Periodically flush buffered events to S1."""
        while self._running:
            try:
                await asyncio.sleep(FLUSH_INTERVAL)
                await self._flush_once()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"Event flush error: {e}")

    async def _flush_once(self):
        """Attempt to send buffered events to S1."""
        if not self._buffer:
            self._retry_count = 0
            return

        # Take a batch from the front of the buffer
        batch = []
        for _ in range(min(MAX_BATCH_SIZE, len(self._buffer))):
            if self._buffer:
                batch.append(self._buffer[0])
                self._buffer.popleft()

        if not batch:
            return

        success = await self._post_events(batch)
        if success:
            self._retry_count = 0
        else:
            # Put events back at the front for retry
            for event in reversed(batch):
                self._buffer.appendleft(event)
            # Backoff
            backoff_idx = min(self._retry_count, len(RETRY_BACKOFF) - 1)
            self._retry_count += 1
            await asyncio.sleep(RETRY_BACKOFF[backoff_idx])

    async def _post_events(self, events: list) -> bool:
        """POST events batch to S1. Returns True on success."""
        url = f"{self.coordinator}/api/events"
        payload = {
            "server_key": self.server_key,
            "events": events,
        }

        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        return True
                    logger.debug(f"Event POST failed: HTTP {resp.status}")
                    return False
        except ImportError:
            return await self._post_events_sync(events)
        except Exception as e:
            logger.debug(f"Event POST error: {e}")
            return False

    async def _post_events_sync(self, events: list) -> bool:
        """Fallback using synchronous requests library (non-blocking via to_thread)."""
        import asyncio
        import requests as req

        url = f"{self.coordinator}/api/events"
        payload = {
            "server_key": self.server_key,
            "events": events,
        }

        def _do_post():
            resp = req.post(url, json=payload, timeout=10)
            return resp.status_code == 200

        try:
            return await asyncio.to_thread(_do_post)
        except Exception as e:
            logger.debug(f"Event POST (sync) error: {e}")
            return False


# Module-level singleton (initialized in __main__.py)
_global_buffer: Optional[EventBuffer] = None


def get_event_buffer() -> Optional[EventBuffer]:
    """Get the global event buffer instance."""
    return _global_buffer


def init_event_buffer(server_key: str) -> EventBuffer:
    """Initialize the global event buffer."""
    global _global_buffer
    _global_buffer = EventBuffer(server_key)
    return _global_buffer
