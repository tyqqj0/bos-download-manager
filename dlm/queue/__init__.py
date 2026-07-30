"""DLM task queue — Celery + Redis based task coordination."""

try:
    from .app import app
    __all__ = ["app"]
except ImportError:
    app = None
    __all__ = []
