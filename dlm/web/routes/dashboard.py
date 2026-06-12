"""Dashboard API — GET /api/dashboard"""

from fastapi import APIRouter

from ..cache import cache

router = APIRouter(tags=["dashboard"])


@router.get("/dashboard")
async def get_dashboard():
    """Dashboard summary: total progress, status counts, server overview, recent activity."""
    data = cache.get_dashboard()
    if not data:
        return {"status": "loading", "message": "Initial sync in progress..."}
    return data
