"""Storage routes — BOS and JuiceFS file browsing, registration, and management."""

import os
import logging
from concurrent.futures import ThreadPoolExecutor

import asyncio
from fastapi import APIRouter

logger = logging.getLogger("dlm.web")
router = APIRouter(tags=["storage"])

_executor = ThreadPoolExecutor(max_workers=4)


def _run_blocking(fn, *args):
    loop = asyncio.get_event_loop()
    return loop.run_in_executor(_executor, fn, *args)


@router.get("/storage/bos")
async def list_bos(bucket: str = "auwomo-data", prefix: str = ""):
    """List directories and files in a BOS bucket under a prefix."""
    from ...core.bos import create_bos_client, list_prefixes, get_prefix_size
    from ...core.config import load_config
    from ...core.state import StateManager

    def do_list():
        config = load_config()
        client = create_bos_client(config["BAIDU_AK"], config["BAIDU_SK"], config["BOS_ENDPOINT"])

        dirs, files = list_prefixes(client, bucket, prefix=prefix)

        mgr = StateManager.create()
        state = mgr.load(use_cache=True)
        registered_paths = {t.bos_path.strip("/") for t in state.tasks if t.bos_path}

        items = []
        for d in sorted(dirs):
            name = d[len(prefix):].strip("/")
            path_key = d.strip("/")
            task_match = next(
                (t for t in state.tasks if t.bos_path and t.bos_path.strip("/") == path_key),
                None,
            )
            items.append({
                "name": name,
                "type": "dir",
                "prefix": d,
                "size": None,
                "registered": path_key in registered_paths,
                "task_id": task_match.id if task_match else None,
                "task_status": task_match.status if task_match else None,
                "transfer_status": task_match.transfer_status if task_match else None,
            })

        for key, size in sorted(files, key=lambda x: x[0]):
            name = key[len(prefix):]
            items.append({
                "name": name,
                "type": "file",
                "prefix": key,
                "size": size,
                "registered": False,
                "task_id": None,
                "task_status": None,
                "transfer_status": None,
            })

        return items

    items = await _run_blocking(do_list)
    return {"bucket": bucket, "prefix": prefix, "items": items}


@router.get("/storage/bos/size")
async def get_bos_size(bucket: str, prefix: str):
    """Get total size of a BOS prefix (may be slow for large prefixes)."""
    from ...core.bos import create_bos_client, get_prefix_size
    from ...core.config import load_config

    def do_size():
        config = load_config()
        client = create_bos_client(config["BAIDU_AK"], config["BAIDU_SK"], config["BOS_ENDPOINT"])
        return get_prefix_size(client, bucket, prefix)

    size = await _run_blocking(do_size)
    return {"bucket": bucket, "prefix": prefix, "size_bytes": size}


@router.get("/storage/juicefs")
async def list_juicefs(path: str = "/", section: str = "managed"):
    """List files/directories on JuiceFS via D-Robotics API."""
    from ...transfer.dcloud import DCloudClient

    dcloud_user = os.environ.get("DCLOUD_USER")
    dcloud_pass = os.environ.get("DCLOUD_PASS")
    if not dcloud_user or not dcloud_pass:
        return {"error": "DCLOUD_USER/DCLOUD_PASS not configured", "items": []}

    JUICEFS_PREFIX = "/727a2f92-30c"
    MANAGED_ROOTS = [
        {"name": "auwomo-datasets/raw-data", "path": f"{JUICEFS_PREFIX}/auwomo-datasets/raw-data/", "label": "Datasets"},
        {"name": "auwomo-model", "path": f"{JUICEFS_PREFIX}/auwomo-model/", "label": "Models"},
    ]

    def do_list():
        client = DCloudClient(dcloud_user, dcloud_pass)
        client.login()
        http = client._http

        if section == "roots":
            return MANAGED_ROOTS

        if path == "/":
            full_path = f"{JUICEFS_PREFIX}/"
        elif path.startswith("/"):
            full_path = f"{JUICEFS_PREFIX}{path}"
        else:
            full_path = f"{JUICEFS_PREFIX}/{path}"

        if not full_path.endswith("/"):
            full_path += "/"

        resp = http.post("https://cloud.d-robotics.cc/api/infrabffApi/files", json={
            "path": full_path,
            "page": 1,
            "page_size": 200,
        }, timeout=15)

        if resp.status_code != 200:
            return []

        data = resp.json().get("data", {})
        files = data.get("files", [])

        items = []
        for f in files:
            items.append({
                "name": f.get("name", ""),
                "is_dir": f.get("is_dir", False),
                "size": f.get("size", 0),
            })

        return sorted(items, key=lambda x: (not x["is_dir"], x["name"]))

    items = await _run_blocking(do_list)
    return {"path": path, "section": section, "items": items}


@router.post("/storage/register")
async def register_bos_data(body: dict):
    """Register existing BOS data as a completed task, optionally start transfer."""
    from ...core.state import StateManager
    from ...core.models import Task, _now
    import uuid

    bucket = body.get("bucket", "auwomo-data")
    prefix = body.get("prefix", "").strip("/")
    name = body.get("name", "")
    category = body.get("category", "other")
    task_type = body.get("type", "dataset")
    auto_transfer = body.get("auto_transfer", False)

    if not prefix or not name:
        return {"error": "prefix and name are required"}

    def do_register():
        mgr = StateManager.create()
        state = mgr.load(use_cache=False)

        existing = next((t for t in state.tasks if t.bos_path and t.bos_path.strip("/") == prefix), None)
        if existing:
            return {"error": f"Already registered as {existing.id} ({existing.name})", "task_id": existing.id}

        task_id = f"t-reg-{uuid.uuid4().hex[:8]}"
        task = Task(
            id=task_id,
            name=name,
            source="bos",
            repo_id=f"bos://{bucket}/{prefix}",
            status="done",
            category=category,
            type=task_type,
            bos_path=f"{prefix}/",
            size_gb=0,
            downloaded_gb=0,
            created_at=_now(),
            completed_at=_now(),
            transfer_status="queued" if auto_transfer else None,
        )
        state.tasks.append(task)
        mgr.save(state)
        return {"ok": True, "task_id": task_id, "name": name}

    result = await _run_blocking(do_register)
    return result


@router.post("/storage/juicefs/move")
async def move_juicefs(body: dict):
    """Move a directory on JuiceFS (rename operation — metadata only on JuiceFS)."""
    from ...transfer.dcloud import DCloudClient

    source = body.get("source", "")
    target = body.get("target", "")
    if not source or not target:
        return {"error": "source and target are required"}

    if ".." in source or ".." in target:
        return {"error": "Path traversal not allowed"}

    MANAGED_PREFIXES = ["/auwomo-datasets/raw-data/", "/auwomo-model/"]
    if not any(target.startswith(p) for p in MANAGED_PREFIXES):
        return {"error": f"Target must be under a managed directory: {MANAGED_PREFIXES}"}

    if source == target:
        return {"error": "Source and target are the same"}

    dcloud_user = os.environ.get("DCLOUD_USER")
    dcloud_pass = os.environ.get("DCLOUD_PASS")
    if not dcloud_user or not dcloud_pass:
        return {"error": "DCLOUD_USER/DCLOUD_PASS not configured"}

    JUICEFS_PREFIX = "/727a2f92-30c"

    def do_move():
        client = DCloudClient(dcloud_user, dcloud_pass)
        client.login()
        http = client._http

        full_source = f"{JUICEFS_PREFIX}{source}" if source.startswith("/") else f"{JUICEFS_PREFIX}/{source}"
        full_target = f"{JUICEFS_PREFIX}{target}" if target.startswith("/") else f"{JUICEFS_PREFIX}/{target}"

        resp = http.post("https://cloud.d-robotics.cc/api/infrabffApi/files/move", json={
            "source": full_source,
            "target": full_target,
        }, timeout=30)

        if resp.status_code == 404:
            return {"error": "Move API not available on D-Robotics platform", "status_code": 404}

        result = resp.json()
        if result.get("status") == 0:
            return {"ok": True, "source": source, "target": target}
        return {"error": result.get("message", "Move failed"), "detail": result}

    result = await _run_blocking(do_move)
    return result
