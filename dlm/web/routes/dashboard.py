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
    from ..fleet import DEFAULT_DISPATCH_MODE, servers_view

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
    # `servers` is derived here, not stored: it is a pure transform of the
    # `workers` rows already in the payload (merge the two hostnames per node,
    # stamp worker_alive), so it costs no extra query and cannot go stale
    # against them. The Workers strip, the server filter dropdown and the whole
    # Servers tab read `dashboard.servers` and had been rendering from `{}`
    # since the Temporal rewrite dropped the scheduler line that used to supply
    # it — see fleet.servers_view.
    return {
        **data,
        "servers": servers_view(data.get("workers") or []),
        "default_dispatch_mode": DEFAULT_DISPATCH_MODE,
    }
