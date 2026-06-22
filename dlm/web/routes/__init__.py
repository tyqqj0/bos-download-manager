"""Web route modules — shared utilities."""

import asyncio
from concurrent.futures import ThreadPoolExecutor

_executor = ThreadPoolExecutor(max_workers=4)


async def run_blocking(fn):
    """Run a blocking function in the shared thread pool."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, fn)
