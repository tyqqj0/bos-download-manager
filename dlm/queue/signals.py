"""Redis-based cooperative signals for worker tasks.

Workers poll these signals every 5s. Setting a signal causes the worker
to gracefully cancel its current task (preserving staging for resume).
"""

import os

import redis

_redis = None

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
SIGNAL_PREFIX = "dlm:signal:"
SIGNAL_TTL = 86400


def get_redis():
    global _redis
    if _redis is None:
        _redis = redis.from_url(REDIS_URL, decode_responses=True)
    return _redis


def signal_pause(task_id: str, reason: str = "manual"):
    """Set a pause signal for a task. Worker will stop within 5s."""
    get_redis().set(f"{SIGNAL_PREFIX}{task_id}", reason, ex=SIGNAL_TTL)


def signal_clear(task_id: str):
    """Clear any pending signal for a task."""
    get_redis().delete(f"{SIGNAL_PREFIX}{task_id}")


def check_signal(task_id: str) -> str | None:
    """Check if there's a pending signal. Returns reason string or None."""
    return get_redis().get(f"{SIGNAL_PREFIX}{task_id}")
