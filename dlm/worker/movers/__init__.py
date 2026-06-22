"""Upload movers — pluggable strategies for moving data to BOS."""

from .base import Mover
from .bos_sdk import BOSSDKMover
from .rsync_fuse import RsyncFuseMover

_DEFAULT_MOVER = "bos_sdk"


def get_mover(strategy: str = None) -> Mover:
    """Get the mover implementation. Defaults to BOS SDK, falls back to rsync."""
    strategy = strategy or _DEFAULT_MOVER
    if strategy == "bos_sdk":
        try:
            from baidubce.services.bos.bos_client import BosClient  # noqa: F401
            return BOSSDKMover()
        except ImportError:
            return RsyncFuseMover()
    return RsyncFuseMover()
