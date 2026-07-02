"""Celery task: transfer completed downloads from BOS to JuiceFS via D-Robotics."""

import logging
import os
import time

from ..queue.app import app
from ..queue import snapshot

logger = logging.getLogger(__name__)


@app.task(
    bind=True,
    name="dlm.transfer.tasks.transfer_to_juicefs",
    max_retries=3,
    retry_backoff=True,
    retry_backoff_max=1800,
)
def transfer_to_juicefs(self, prev_result: dict = None, task_meta: dict = None):
    """Transfer data from BOS to JuiceFS via D-Robotics import API.

    Can be called standalone or as part of a chain (receives prev_result from download).

    Args:
        prev_result: Result from previous task in chain (download_dataset output).
        task_meta: Task metadata (used when called directly, not from chain).
    """
    from .dcloud import DCloudClient
    from ..constants import DATA_BUCKET, MODEL_BUCKET

    if prev_result and isinstance(prev_result, dict):
        task_id = prev_result.get("task_id")
        task_info = snapshot.get_task(task_id) if task_id else None
    elif task_meta:
        task_info = task_meta
        task_id = task_meta.get("id")
    else:
        logger.error("transfer_to_juicefs called without task context")
        return {"status": "error", "error": "no task context"}

    if not task_info:
        return {"status": "error", "error": f"task {task_id} not found in snapshot"}

    task_name = task_info.get("name", "unknown")
    task_type = task_info.get("type", "dataset")
    category = task_info.get("category", "")
    bos_path = task_info.get("bos_path", "").lstrip("/")

    if not bos_path:
        return {"status": "error", "error": "no bos_path"}

    dcloud_user = os.environ.get("DCLOUD_USER")
    dcloud_pass = os.environ.get("DCLOUD_PASS")
    bos_ak = os.environ.get("BAIDU_AK")
    bos_sk = os.environ.get("BAIDU_SK")

    if not all([dcloud_user, dcloud_pass, bos_ak, bos_sk]):
        logger.warning("Transfer credentials not configured, skipping")
        return {"status": "skipped", "task_id": task_id, "reason": "no credentials"}

    if task_type == "model":
        bos_bucket = MODEL_BUCKET
        if category:
            target_path = f"/727a2f92-30c/auwomo-model/{category}/{task_name}"
        else:
            target_path = f"/727a2f92-30c/auwomo-model/{task_name}"
    else:
        bos_bucket = DATA_BUCKET
        if category:
            target_path = f"/727a2f92-30c/auwomo-datasets/raw-data/{category}/{task_name}"
        else:
            target_path = f"/727a2f92-30c/auwomo-datasets/raw-data/{task_name}"

    logger.info(f"Starting transfer: {task_name} → {target_path}")
    snapshot.update_task_progress(task_id, status="transferring", phase="importing")

    try:
        client = DCloudClient(dcloud_user, dcloud_pass)
        client.login()

        if category:
            try:
                base = "/727a2f92-30c/auwomo-model/" if task_type == "model" else "/727a2f92-30c/auwomo-datasets/raw-data/"
                client.create_folder(base, category)
            except Exception:
                pass

        import_task_id = client.import_from_bos(
            bos_ak=bos_ak,
            bos_sk=bos_sk,
            bos_bucket=bos_bucket,
            bos_path=bos_path,
            target_path=target_path,
        )

        logger.info(f"Import started for {task_name}: dcloud_task={import_task_id}")

        result = client.wait_for_task(import_task_id, timeout_s=7200, poll_s=30)
        status = result.get("status", "")

        if status in ("成功", "success", "done"):
            logger.info(f"Transfer completed: {task_name}")
            snapshot.update_task_progress(
                task_id,
                status="done",
                phase=None,
            )
            from datetime import datetime, timezone
            now = datetime.now(timezone.utc).isoformat(timespec="seconds")
            conn = snapshot._conn()
            conn.execute(
                "UPDATE tasks SET transfer_status = ?, transfer_task_id = ?, completed_at = ? WHERE id = ?",
                ("done", import_task_id, now, task_id),
            )
            conn.commit()
            return {"status": "done", "task_id": task_id, "transfer_task_id": import_task_id}
        else:
            error_msg = result.get("error_msg", status)
            logger.error(f"Transfer failed: {task_name} — {error_msg}")
            snapshot.update_task_progress(task_id, phase=None)
            conn = snapshot._conn()
            conn.execute(
                "UPDATE tasks SET transfer_status = ?, transfer_error = ? WHERE id = ?",
                ("failed", error_msg, task_id),
            )
            conn.commit()
            raise self.retry(exc=RuntimeError(error_msg), countdown=300)

    except self.MaxRetriesExceededError:
        logger.error(f"Transfer max retries exceeded: {task_name}")
        conn = snapshot._conn()
        conn.execute(
            "UPDATE tasks SET transfer_status = ? WHERE id = ?",
            ("failed", task_id),
        )
        conn.commit()
        return {"status": "failed", "task_id": task_id}

    except Exception as e:
        logger.error(f"Transfer error for {task_name}: {e}")
        snapshot.update_task_progress(task_id, phase=None)
        raise self.retry(exc=e, countdown=120)
