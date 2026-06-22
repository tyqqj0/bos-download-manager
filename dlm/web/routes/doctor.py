"""Doctor API — cluster health check and one-click repair."""

from datetime import datetime, timezone
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ..cache import cache
from . import run_blocking

router = APIRouter(tags=["doctor"])

HEARTBEAT_TIMEOUT = 180  # seconds
INVALID_REPO_PATTERNS = [".org", ".io", ".ai", "github.com", "arxiv"]


def _find_stuck_downloads(state) -> list[dict]:
    """Find tasks marked 'downloading' but not actually being worked on."""
    heartbeats = state.worker_heartbeats
    stuck = []
    now = datetime.now(timezone.utc)

    for t in state.tasks:
        if t.status != "downloading":
            continue

        hb = heartbeats.get(t.server or "", {})
        alive_at = hb.get("alive_at", "")
        current_task = hb.get("current_task", "")

        is_stuck = False
        reason = ""

        # Worker is dead (heartbeat > threshold)
        if not alive_at:
            is_stuck = True
            reason = "worker has no heartbeat"
        else:
            try:
                age = (now - datetime.fromisoformat(alive_at)).total_seconds()
                if age > HEARTBEAT_TIMEOUT:
                    is_stuck = True
                    reason = f"worker offline ({int(age)}s)"
                elif current_task and current_task != t.id:
                    is_stuck = True
                    reason = f"worker doing different task"
                elif not current_task:
                    is_stuck = True
                    reason = "worker idle (no current_task)"
            except (ValueError, TypeError):
                is_stuck = True
                reason = "bad heartbeat timestamp"

        if is_stuck:
            stuck.append({
                "task_id": t.id,
                "name": t.name,
                "server": t.server,
                "reason": reason,
            })

    return stuck


def _find_dead_workers(state) -> list[dict]:
    """Find workers with stale heartbeats."""
    from ...core.servers import load_servers

    server_cfgs = load_servers()
    heartbeats = state.worker_heartbeats
    now = datetime.now(timezone.utc)
    dead = []

    for key, cfg in server_cfgs.items():
        if cfg.local or not cfg.enabled:
            continue
        hb = heartbeats.get(key, {})
        alive_at = hb.get("alive_at", "")
        if not alive_at:
            dead.append({"key": key, "host": cfg.host, "reason": "no heartbeat"})
            continue
        try:
            age = (now - datetime.fromisoformat(alive_at)).total_seconds()
            if age > HEARTBEAT_TIMEOUT:
                dead.append({
                    "key": key,
                    "host": cfg.host,
                    "reason": f"last seen {int(age)}s ago",
                })
        except (ValueError, TypeError):
            dead.append({"key": key, "host": cfg.host, "reason": "bad timestamp"})

    return dead


def _find_zombie_tasks(state) -> list[dict]:
    """Find tasks that should be skipped (permanently invalid)."""
    zombies = []
    for t in state.tasks:
        if t.status in ("done", "skipped", "needs-auth"):
            continue
        repo = t.repo_id or ""
        reason = ""

        if t.retry_count >= 99:
            reason = "retry exhausted (99+)"
        elif t.status == "failed" and t.error_class == "not_found":
            reason = f"repo not found: {repo}"
        elif any(p in repo for p in INVALID_REPO_PATTERNS):
            reason = f"invalid repo (website URL): {repo}"

        if reason:
            zombies.append({
                "task_id": t.id,
                "name": t.name,
                "status": t.status,
                "reason": reason,
            })

    return zombies


def _find_disk_full(state) -> list[dict]:
    """Find workers with critically low disk."""
    heartbeats = state.worker_heartbeats
    full = []
    for key, hb in heartbeats.items():
        disk = hb.get("disk_free_gb", 999)
        if disk < 20:
            full.append({
                "key": key,
                "disk_free_gb": round(disk, 1),
            })
    return full


@router.get("/doctor")
async def diagnose():
    """Run health diagnostics and return findings."""
    def _do():
        from ...core.state import StateManager
        mgr = StateManager.create()
        state = mgr.load(use_cache=False)

        findings = {
            "stuck_downloads": _find_stuck_downloads(state),
            "dead_workers": _find_dead_workers(state),
            "zombie_tasks": _find_zombie_tasks(state),
            "disk_full": _find_disk_full(state),
        }
        total = sum(len(v) for v in findings.values())
        findings["total_issues"] = total
        findings["healthy"] = total == 0
        return findings

    return await run_blocking(_do)


class FixRequest(BaseModel):
    actions: list[str] = []  # ["reset_stuck", "restart_dead", "skip_zombie"]


@router.post("/doctor")
async def fix(req: FixRequest):
    """Apply repair actions."""
    def _do():
        from ...core.state import StateManager
        mgr = StateManager.create()
        state = mgr.load(use_cache=False)
        results = {}

        actions = req.actions
        if not actions:
            actions = ["reset_stuck", "restart_dead", "skip_zombie"]

        if "reset_stuck" in actions:
            stuck = _find_stuck_downloads(state)
            fixed = []
            for item in stuck:
                for t in state.tasks:
                    if t.id == item["task_id"]:
                        t.status = "dispatched"
                        t.speed_mbps = 0
                        t.phase = None
                        fixed.append(item["name"])
                        break
            results["reset_stuck"] = fixed

        if "skip_zombie" in actions:
            zombies = _find_zombie_tasks(state)
            skipped = []
            for item in zombies:
                for t in state.tasks:
                    if t.id == item["task_id"]:
                        t.status = "skipped"
                        t.speed_mbps = 0
                        t.phase = None
                        skipped.append(item["name"])
                        break
            results["skip_zombie"] = skipped

        # Save state changes
        if results.get("reset_stuck") or results.get("skip_zombie"):
            mgr.save(state)

        if "restart_dead" in actions:
            dead = _find_dead_workers(state)
            restarted = []
            from ...core.servers import load_servers
            from ...core.ssh import ssh_exec
            cfgs = load_servers()
            for item in dead:
                key = item["key"]
                cfg = cfgs.get(key)
                if not cfg:
                    continue
                cmd = (
                    "tmux kill-session -t dlm-worker 2>/dev/null; sleep 1; "
                    f"cd {cfg.path} && "
                    "tmux new-session -d -s dlm-worker "
                    f"'set -a && source /root/.env && source .env 2>/dev/null && set +a && "
                    f"python3 -m dlm.worker.daemon --server-key {key}'"
                )
                out, ok = ssh_exec(cfg.host, cfg.user, cmd, timeout=15)
                if ok:
                    restarted.append(key)
            results["restart_dead"] = restarted

        return results

    return await run_blocking(_do)


@router.post("/servers/{key}/cleanup")
async def cleanup_staging(key: str):
    """Clean orphan staging directories on a server."""
    def _do():
        from ...core.state import StateManager
        from ...core.servers import load_servers
        from ...core.ssh import ssh_exec

        cfgs = load_servers()
        if key not in cfgs:
            return {"error": f"Server {key} not found"}
        cfg = cfgs[key]

        mgr = StateManager.create()
        state = mgr.load(use_cache=False)

        # Get staging directories on the server
        out, ok = ssh_exec(cfg.host, cfg.user,
                           "ls -1 /data/staging/ 2>/dev/null", timeout=10)
        if not ok:
            return {"error": f"Cannot list staging on {key}: {out}"}

        staging_dirs = [d.strip() for d in out.splitlines() if d.strip()]
        if not staging_dirs:
            return {"cleaned": [], "message": "No staging directories found"}

        # Find which tasks are done/skipped/failed — their staging can be cleaned
        cleanable_names = set()
        for t in state.tasks:
            if t.status in ("done", "skipped", "failed") and t.server == key:
                cleanable_names.add(t.name)
                if t.repo_id:
                    cleanable_names.add(t.repo_id.split("/")[-1])

        # Match staging dirs to cleanable tasks
        to_clean = []
        for d in staging_dirs:
            if d in cleanable_names:
                to_clean.append(d)
            else:
                # Check partial match (staging dir name might differ from task name)
                for name in cleanable_names:
                    if name.lower().replace("-", "").replace("_", "") == \
                       d.lower().replace("-", "").replace("_", ""):
                        to_clean.append(d)
                        break

        if not to_clean:
            return {"cleaned": [], "message": "No orphan staging found"}

        # Remove them
        paths = " ".join(f"/data/staging/{d}" for d in to_clean)
        rm_out, rm_ok = ssh_exec(cfg.host, cfg.user,
                                 f"rm -rf {paths} && echo OK", timeout=120)
        if rm_ok and "OK" in rm_out:
            # Also clear HF cache
            ssh_exec(cfg.host, cfg.user,
                     "rm -rf /root/.cache/huggingface/* 2>/dev/null", timeout=30)
            return {"cleaned": to_clean, "freed_count": len(to_clean)}
        return {"error": f"Cleanup failed: {rm_out}"}

    result = await run_blocking(_do)
    if "error" in result:
        raise HTTPException(500, result["error"])
    return result
