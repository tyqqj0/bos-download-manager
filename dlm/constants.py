"""DLM — 数据集下载管理 CLI"""

from pathlib import Path

STATUSES = ["queued", "dispatched", "downloading", "done", "failed", "skipped", "needs-auth"]
PRIORITIES = ["P0", "P1", "P2", "P3"]
CATEGORIES = [
    "manipulation",
    "whole-body",
    "ego-centric",
    "navigation",
    "driving-vqa",
    "multimodal",
    "other",
]

META_BUCKET = "westlake-autolab-databuilder-meta"
DATA_BUCKET = "auwomo-data"
MODEL_BUCKET = "auwomo-model-open"
STATE_KEY = "download-manager/state.json"

# Local paths on a worker. STAGING_ROOT is where downloads land before upload.
#
# EVENT_BUFFER_STATUS_FILE is the channel between two separate processes on the
# same host: the Temporal worker's EventBuffer writes it, the sidecar reads it
# and forwards the value as `event_buffer_pending`. Defined once, here, because
# the two ends are in different packages and a divergence would not fail
# anything — it would just make the health signal read "unknown" forever, which
# looks exactly like the pre-2026-08-09 state where nothing wrote the file.
STAGING_ROOT = Path("/data/staging")
EVENT_BUFFER_STATUS_FILE = STAGING_ROOT / ".event_buffer_status"
