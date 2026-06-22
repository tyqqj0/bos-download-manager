"""dlm status — Cluster status from heartbeats (no SSH)."""

import click
from datetime import datetime, timezone

from ..core.state import StateManager


@click.command("status")
@click.option("--json", "as_json", is_flag=True, help="JSON 输出")
def status_cmd(as_json):
    """显示所有 Worker 实时状态（从心跳读取，秒级响应）。"""
    mgr = StateManager.create()
    state = mgr.load(use_cache=False)

    heartbeats = state.worker_heartbeats
    downloading = [t for t in state.tasks if t.status == "downloading"]

    if as_json:
        import json
        data = {
            "heartbeats": heartbeats,
            "downloading": [
                {"id": t.id, "server": t.server, "name": t.name,
                 "progress_pct": t.progress_pct, "speed_mbps": t.speed_mbps,
                 "eta_seconds": t.eta_seconds, "phase": t.phase}
                for t in downloading
            ],
        }
        click.echo(json.dumps(data, ensure_ascii=False, indent=2))
        return

    now = datetime.now(timezone.utc)

    click.echo("═" * 58)
    click.echo("  DLM Cluster Status")
    click.echo("═" * 58)

    if not heartbeats:
        click.echo("\n  无 Worker 心跳。daemon 可能未启动。")
        click.echo("  启动: ssh <server> 'python3 -m dlm.worker --server-key <key>'")
        _print_summary(state)
        return

    for key in sorted(heartbeats.keys()):
        hb = heartbeats[key]
        alive_at = hb.get("alive_at", "")
        pid = hb.get("pid", "?")
        disk_gb = hb.get("disk_free_gb", 0)
        current_task = hb.get("current_task")

        # Determine alive status
        icon, status_text = _alive_status(alive_at, now)

        click.echo(f"\n  {icon} {key} — {status_text}  (pid={pid}, disk={disk_gb:.0f}GB free)")

        # Show current task with progress
        task_info = next((t for t in downloading if t.server == key), None)
        if task_info:
            name = task_info.name[:30]
            pct = task_info.progress_pct
            speed = task_info.speed_mbps
            eta = task_info.eta_seconds
            phase = task_info.phase or "downloading"

            progress_bar = _bar(min(pct, 100))
            speed_str = f"{speed:.1f} MB/s" if speed > 0 else ""
            eta_str = _format_eta(eta) if eta else ""

            click.echo(f"    ↓ {name}")
            if phase == "moving":
                click.echo(f"      上传到 BOS 中...  {speed_str}")
            else:
                click.echo(f"      {progress_bar} {pct:.0f}%  {speed_str}  {eta_str}")
                if phase and phase not in ("downloading", "starting"):
                    click.echo(f"      阶段: {phase}")
        elif current_task:
            click.echo(f"    任务: {current_task} (等待状态更新)")
        else:
            click.echo(f"    空闲")

    _print_summary(state)


def _alive_status(alive_at: str, now: datetime) -> tuple:
    """Return (icon, text) based on heartbeat age."""
    if not alive_at:
        return "○", "未知"
    try:
        ts = datetime.fromisoformat(alive_at)
        age_s = (now - ts).total_seconds()
    except (ValueError, TypeError):
        return "○", "未知"

    if age_s < 180:
        return "●", f"活跃 ({int(age_s)}s ago)"
    elif age_s < 300:
        return "◐", f"延迟 ({int(age_s)}s ago)"
    else:
        mins = int(age_s / 60)
        return "○", f"离线 ({mins}min ago)"


def _bar(pct: float, width: int = 20) -> str:
    """Simple progress bar."""
    pct = min(max(pct, 0), 100)
    filled = int(width * pct / 100)
    return f"[{'█' * filled}{'░' * (width - filled)}]"


def _format_eta(seconds: int) -> str:
    if seconds is None or seconds <= 0:
        return ""
    if seconds < 60:
        return f"ETA {seconds}s"
    if seconds < 3600:
        return f"ETA {seconds // 60}min"
    hours = seconds // 3600
    mins = (seconds % 3600) // 60
    return f"ETA {hours}h{mins}m"


def _print_summary(state):
    """Print task count summary."""
    click.echo(f"\n{'─' * 58}")
    by_status = {}
    for t in state.tasks:
        by_status[t.status] = by_status.get(t.status, 0) + 1
    parts = [f"{v} {k}" for k, v in sorted(by_status.items(), key=lambda x: x[1], reverse=True)]
    click.echo(f"  {len(state.tasks)} 个任务: {', '.join(parts)}")

    # Alerts
    now = datetime.now(timezone.utc)
    alerts = _check_alerts(state, now)
    if alerts:
        click.echo(f"\n{'─' * 58}")
        click.echo("  告警:")
        for a in alerts:
            click.echo(a)


def _check_alerts(state, now):
    """Detect conditions that need attention."""
    alerts = []
    heartbeats = state.worker_heartbeats

    for key in sorted(heartbeats.keys()):
        hb = heartbeats[key]
        disk = hb.get("disk_free_gb", 999)
        if disk < 5:
            alerts.append(f"    !! {key}: 磁盘将满 ({disk:.0f}GB free)")
        alive_at = hb.get("alive_at", "")
        if alive_at:
            try:
                age = (now - datetime.fromisoformat(alive_at)).total_seconds()
                if age > 300:
                    alerts.append(f"    !! {key}: 离线 ({age/60:.0f}min 未响应)")
            except (ValueError, TypeError):
                pass

    for t in state.tasks:
        if t.status == "downloading" and t.worker_heartbeat:
            try:
                hb_age = (now - datetime.fromisoformat(t.worker_heartbeat)).total_seconds()
                if hb_age > 3600:
                    alerts.append(
                        f"    !! {t.server}: {t.name[:20]} 可能卡住 ({hb_age/3600:.0f}h 无更新)"
                    )
            except (ValueError, TypeError):
                pass

    failed = [t for t in state.tasks if t.status == "failed"]
    if failed:
        alerts.append(f"    !! {len(failed)} 个任务失败")

    return alerts
