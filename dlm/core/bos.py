"""BOS client wrapper for DLM."""

from baidubce.bce_client_configuration import BceClientConfiguration
from baidubce.auth.bce_credentials import BceCredentials
from baidubce.services.bos.bos_client import BosClient


def create_bos_client(ak: str, sk: str, endpoint: str) -> BosClient:
    config = BceClientConfiguration(
        credentials=BceCredentials(ak, sk),
        endpoint=endpoint,
    )
    return BosClient(config)


def get_prefix_size(client: BosClient, bucket: str, prefix: str) -> int:
    """Sum total bytes of all objects under a prefix (paginated)."""
    total = 0
    marker = ""
    while True:
        response = client.list_objects(bucket, prefix=prefix, marker=marker, max_keys=1000)
        if response.contents:
            for obj in response.contents:
                total += obj.size
        if not response.is_truncated:
            break
        marker = response.next_marker
    return total
