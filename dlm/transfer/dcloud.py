"""D-Robotics Cloud (cloud.d-robotics.cc) API client.

Handles:
- SSO login (AES-ECB encrypted credentials)
- BOS→JuiceFS import (bucket-to-bucket transfer)
- Preheat/warmup task creation
- Async task monitoring
"""

import base64
import json
import logging
import time
from dataclasses import dataclass
from typing import Optional

import requests
from Crypto.Cipher import AES

logger = logging.getLogger(__name__)

SSO_URL = "https://sso.d-robotics.cc/api"
API_URL = "https://cloud.d-robotics.cc/api"
AES_KEY = b"wJE911ku0VOpUtx0"


def _aes_encrypt(plaintext: str) -> str:
    cipher = AES.new(AES_KEY, AES.MODE_ECB)
    data = plaintext.encode("utf-8")
    padding = 16 - len(data) % 16
    padded = data + bytes([padding] * padding)
    encrypted = cipher.encrypt(padded)
    return base64.b64encode(encrypted).decode()


@dataclass
class DCloudSession:
    token: str
    tenant_id: str
    org_symbol: str
    user_id: str


class DCloudClient:
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
        self.session: Optional[DCloudSession] = None
        self._http = requests.Session()
        self._http.headers.update({
            "Content-Type": "application/json",
            "SourceApp": "dataCopilot",
        })

    def login(self) -> DCloudSession:
        payload = json.dumps({"userName": self.username, "password": self.password})
        encrypted = _aes_encrypt(payload)

        resp = self._http.post(f"{SSO_URL}/login", json={
            "type": "up",
            "data": encrypted,
        }, timeout=15)
        resp.raise_for_status()
        result = resp.json()

        if result.get("status") != 0:
            raise RuntimeError(f"Login failed: {result.get('message')}")

        token = result["data"]
        self._http.headers["Authorization"] = token

        user_info = self._get_user_info()
        self.session = user_info
        self._http.headers["d-tenant-id"] = user_info.tenant_id
        logger.info(f"Logged in as {self.username}, org={user_info.org_symbol}")
        return user_info

    def _get_user_info(self) -> DCloudSession:
        resp = self._http.get(
            f"{API_URL}/dcloudUserApi/user/userAndOrg", timeout=10
        )
        resp.raise_for_status()
        data = resp.json().get("data", {})

        user_id = data.get("userId", "")
        org = data.get("organization", {})
        org_symbol = org.get("symbol", "")

        # Pick the first non-default tenant (matching frontend logic)
        default_tenant = "2daaf00a-58d3-4833-979a-3c084a5b9ac3"
        tenants = data.get("tenants", [])
        tenant_id = ""
        for t in tenants:
            if t.get("tenantId") != default_tenant:
                tenant_id = t["tenantId"]
                break
        if not tenant_id and tenants:
            tenant_id = tenants[0].get("tenantId", "")

        return DCloudSession(
            token=self._http.headers.get("Authorization", ""),
            tenant_id=tenant_id,
            org_symbol=org_symbol,
            user_id=user_id,
        )

    def import_from_bos(
        self,
        bos_ak: str,
        bos_sk: str,
        bos_bucket: str,
        bos_path: str,
        target_path: str,
        bos_endpoint: str = "bj.bcebos.com",
    ) -> str:
        """Import data from BOS bucket to JuiceFS.

        Args:
            bos_ak: Baidu AK
            bos_sk: Baidu SK
            bos_bucket: BOS bucket name
            bos_path: Path within the bucket (e.g. "downloads/datasets/xxx")
            target_path: Target path on JuiceFS (e.g. "/datasets/xxx")
            bos_endpoint: BOS endpoint domain (default: bj.bcebos.com)

        Returns:
            Task ID for the async import job.
        """
        endpoint_str = f"{bos_bucket}.{bos_endpoint}/{bos_path}"

        resp = self._http.post(f"{API_URL}/infrabffApi/files/import", json={
            "type": "bos",
            "access_key": bos_ak,
            "secret_key": bos_sk,
            "endpoint": endpoint_str,
            "path": target_path,
        }, timeout=30)
        resp.raise_for_status()
        result = resp.json()

        if result.get("status") != 0:
            raise RuntimeError(f"Import failed: {result.get('message')} — {result.get('extra')}")

        task_id = result.get("data", "")
        logger.info(f"Import task created: {task_id}")
        logger.info(f"  Source: {endpoint_str}")
        logger.info(f"  Target: {target_path}")
        return task_id

    def list_async_tasks(self, page: int = 1, page_size: int = 50) -> list:
        """List async import/export tasks."""
        resp = self._http.post(f"{API_URL}/infrabffApi/async-tasks/list", json={
            "page": page,
            "page_size": page_size,
        }, timeout=10)
        resp.raise_for_status()
        result = resp.json()
        return result.get("data", {}).get("list", [])

    def wait_for_task(self, task_id: str, timeout_s: int = 7200, poll_s: int = 30) -> dict:
        """Poll until an async task completes or fails."""
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            tasks = self.list_async_tasks(page_size=100)
            for t in tasks:
                if t.get("task_id") == task_id:
                    status = t.get("status", "")
                    if status in ("成功", "success", "done"):
                        logger.info(f"Task {task_id} completed successfully")
                        return t
                    elif status in ("失败", "failed", "error"):
                        logger.error(f"Task {task_id} failed: {t.get('error_msg')}")
                        return t
                    else:
                        logger.debug(f"Task {task_id} status: {status}")
                    break
            time.sleep(poll_s)
        raise TimeoutError(f"Task {task_id} did not complete within {timeout_s}s")

    def create_warmup_task(
        self,
        source_paths: list[str],
        target_storage_id: str,
        schedule_type: str = "once",
        cron_expr: str = "",
    ) -> dict:
        """Create a preheat/warmup task.

        Args:
            source_paths: Paths on JuiceFS to preheat
            target_storage_id: CacheGroup storage ID
            schedule_type: "once" or "cron"
            cron_expr: Cron expression (only if schedule_type=="cron")
        """
        body = {
            "source_paths": source_paths,
            "target_storage_id": target_storage_id,
            "schedule": {"type": schedule_type},
        }
        if schedule_type == "cron" and cron_expr:
            body["schedule"]["cron"] = cron_expr
            body["concurrency_policy"] = "Forbid"

        resp = self._http.post(
            f"{API_URL}/infrabffApi/robotFsApi/api/v2/warmup/tasks",
            json=body, timeout=15,
        )
        resp.raise_for_status()
        result = resp.json()
        if result.get("status") != 0:
            raise RuntimeError(f"Warmup task failed: {result.get('message')}")
        logger.info(f"Warmup task created for {len(source_paths)} paths")
        return result

    def list_warmup_tasks(self, page: int = 1, page_size: int = 20) -> list:
        """List preheat/warmup tasks."""
        resp = self._http.post(
            f"{API_URL}/infrabffApi/robotFsApi/api/v2/warmup/tasks/list",
            json={"page": page, "page_size": page_size}, timeout=10,
        )
        resp.raise_for_status()
        result = resp.json()
        return result.get("data", {}).get("list", [])

    def list_cachegroup_storages(self) -> list:
        """List available CacheGroup storages (targets for preheat)."""
        resp = self._http.get(
            f"{API_URL}/infrabffApi/dcloudResourceApi/storages",
            params={
                "storage_type": "cachegroup",
                "storage_scope": "compute_cluster",
                "page_size": "200",
            },
            timeout=10,
        )
        resp.raise_for_status()
        result = resp.json()
        return result.get("data", {}).get("items", [])

    def create_folder(self, path: str, name: str) -> dict:
        """Create a folder on JuiceFS. May fail if filesystem not provisioned."""
        resp = self._http.post(f"{API_URL}/infrabffApi/files/folder", json={
            "path": path,
            "name": name,
        }, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def list_files(self, path: str = "", page: int = 1, page_size: int = 50) -> dict:
        """List files/folders at path. May fail if filesystem not provisioned."""
        resp = self._http.post(f"{API_URL}/infrabffApi/files", json={
            "path": path,
            "page": page,
            "page_size": page_size,
        }, timeout=10)
        resp.raise_for_status()
        return resp.json()
