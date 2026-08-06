"""Dashboard API — GET /api/dashboard"""

from fastapi import APIRouter

from ..cache import cache
from . import run_blocking

router = APIRouter(tags=["dashboard"])


@router.get("/dashboard")
async def get_dashboard():
    """Dashboard summary: total progress, status counts, worker overview."""
    # Module constant (env-derived at import time), not a DB read — no
    # executor hop needed to read it. Exposed so the add-task form can show
    # the live server default instead of a client-side literal that would
    # silently drift from it the moment DLM_DEFAULT_DISPATCH_MODE flips.
    from ..fleet import DEFAULT_DISPATCH_MODE

    data = cache.get_dashboard()
    if not data:
        # Cold cache (first ~10s after start, or a wedged scheduler): fall back
        # to a live read, but off the event loop — init_db takes the write lock
        # and get_dashboard_summary is 7+ queries.
        def _live():
            from ...queue.snapshot import init_db, get_dashboard_summary
            init_db()
            return get_dashboard_summary()

        data = await run_blocking(_live)
    return {**data, "default_dispatch_mode": DEFAULT_DISPATCH_MODE}
