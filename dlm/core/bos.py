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
