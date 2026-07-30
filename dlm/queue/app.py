"""Celery application configuration."""

import os
from pathlib import Path

from celery import Celery

# Ensure .env is loaded (web server may not have it in os.environ)
try:
    from dotenv import load_dotenv
    for candidate in [
        Path(__file__).resolve().parents[2] / ".env",
        Path.home() / ".env",
    ]:
        if candidate.exists():
            load_dotenv(candidate)
except ImportError:
    pass

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

app = Celery("dlm", broker=REDIS_URL, backend=REDIS_URL)

app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_track_started=True,
    task_time_limit=86400 * 3,
    task_soft_time_limit=86400 * 2,
    worker_prefetch_multiplier=1,
    worker_concurrency=1,
    worker_max_tasks_per_child=None,
    worker_max_memory_per_child=8_000_000,
    task_acks_late=False,
    task_reject_on_worker_lost=False,
    broker_transport_options={
        "priority_steps": list(range(10)),
        "sep": ":",
        "queue_order_strategy": "priority",
    },
    task_routes={
        "dlm.transfer.tasks.transfer_to_juicefs": {"queue": "transfers"},
    },
)

app.autodiscover_tasks(["dlm.transfer"])
app.conf.include = ["dlm.transfer.tasks"]
