"""DLM — 数据集下载管理 CLI"""

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
