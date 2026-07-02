"""Dashboard API — GET /api/dashboard"""

from fastapi import APIRouter

from ..cache import cache

router = APIRouter(tags=["dashboard"])


@router.get("/dashboard")
async def get_dashboard():
    """Dashboard summary: total progress, status counts, worker overview."""
    data = cache.get_dashboard()
    if not data:
        from ...queue.snapshot import init_db, get_dashboard_summary
        init_db()
        return get_dashboard_summary()
    return data
