"""FastAPI application factory — Celery + Redis architecture."""

import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

logger = logging.getLogger("dlm.web")

STATIC_DIR = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    from .scheduler import background_scheduler
    from ..queue.snapshot import init_db
    init_db()
    logger.info("Starting background scheduler...")
    task = asyncio.create_task(background_scheduler())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def create_app() -> FastAPI:
    app = FastAPI(
        title="DLM Dashboard",
        description="Dataset Download Manager — Celery + Redis",
        version="2.0.0",
        lifespan=lifespan,
    )

    from .routes.dashboard import router as dashboard_router
    from .routes.tasks import router as tasks_router
    from .routes.queue import router as queue_router
    from .routes.servers import router as servers_router
    from .routes.doctor import router as doctor_router
    from .routes.transfer import router as transfer_router
    from .routes.storage import router as storage_router

    app.include_router(queue_router, prefix="/api")
    app.include_router(tasks_router, prefix="/api")
    app.include_router(dashboard_router, prefix="/api")
    app.include_router(servers_router, prefix="/api")
    app.include_router(doctor_router, prefix="/api")
    app.include_router(transfer_router, prefix="/api")
    app.include_router(storage_router, prefix="/api")

    STATIC_DIR.mkdir(parents=True, exist_ok=True)
    app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")

    return app
