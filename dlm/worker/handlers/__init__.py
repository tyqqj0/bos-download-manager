"""Download handlers — pluggable download source implementations."""

from .base import DownloadHandler
from .hf import HuggingFaceHandler
from .modelscope_handler import ModelScopeHandler
from .wget import WgetHandler

_HANDLERS = {
    "hf": HuggingFaceHandler,
    "modelscope": ModelScopeHandler,
    "wget": WgetHandler,
}


def get_handler(source: str) -> DownloadHandler:
    cls = _HANDLERS.get(source)
    if cls is None:
        raise ValueError(f"Unknown source type: {source}. Available: {list(_HANDLERS.keys())}")
    return cls()
