"""Dashboard API — GET /api/dashboard"""

from fastapi import APIRouter

from ..cache import cache
from . import run_blocking

router = APIRouter(tags=["dashboard"])


@router.get("/dashboard")
async def get_dashboard():
    """Dashboard summary: total progress, status counts, worker overview."""
    data = cache.get_dashboard()
    if not data:
        # Cold cache (first ~10s after start, or a wedged scheduler): fall back
        # to a live read, but off the event loop — init_db takes the write lock
        # and get_dashboard_summary is 7+ queries.
        def _live():
            from ...queue.snapshot import init_db, get_dashboard_summary
            init_db()
            return get_dashboard_summary()

        return await run_blocking(_live)
    return data
