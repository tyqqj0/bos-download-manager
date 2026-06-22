"""dlm watch — Live progress monitor (auto-refresh)."""

import click
import time
from datetime import datetime, timezone

from ..core.state import StateManager


@click.command("watch")
@click.option("--interval", default=10, help="刷新间隔（秒）")
def watch_cmd(interval):
    """实时监控下载进度（每 10s 自动刷新）。"""
    mgr = StateManager.create()
    try:
        while True:
            click.clear()
            _render(mgr)
            click.echo(f"\n[每 {interval}s 刷新, Ctrl+C 退出]")
            time.sleep(interval)
    except KeyboardInterrupt:
        click.echo("\n退出。")


def _render(mgr: StateManager):
    state = mgr.load(use_cache=False)
    now = datetime.now(timezone.utc)
    heartbeats = state.worker_heartbeats
    downloading = [t for t in state.tasks if t.status == "downloading"]
    downloading.sort(key=lambda t: t.server or "")

    ts = now.strftime("%H:%M:%S")
    click.echo(f"═══ DLM Live ({ts}) ═══")
    click.echo("")

    if not downloading and not heartbeats:
        click.echo("  无活跃下载。")
        return

    # Active downloads
    if downloading:
        for t in downloading:
            svr = t.server or "?"
            name = t.name[:24] if len(t.name) > 24 else t.name
            pct = t.progress_pct
            speed = t.speed_mbps
            eta = t.eta_seconds
            phase = t.phase or "downloading"

            bar = _bar(pct)

            if phase == "moving":
                detail = "上传到 BOS..."
            elif speed > 0:
                eta_str = _eta(eta)
                detail = f"{speed:.1f} MB/s  {eta_str}"
            elif phase in ("validating", "starting"):
                detail = phase
            else:
                detail = ""

            click.echo(f"  {svr:<3} ↓ {name:<24} {bar} {pct:>5.1f}%  {detail}")
    else:
        click.echo("  无活跃下载。")

    # Worker health
    click.echo("")
    click.echo("  Workers:")
    for key in sorted(heartbeats.keys()):
        hb = heartbeats[key]
        alive_at = hb.get("alive_at", "")
        disk = hb.get("disk_free_gb", 0)
        icon = _health_icon(alive_at, now)
        task_id = hb.get("current_task", "")
        task_str = task_id if task_id else "idle"
        click.echo(f"    {icon} {key}  disk={disk:.0f}G  {task_str}")

    # Summary
    click.echo("")
    by_status = {}
    for t in state.tasks:
        by_status[t.status] = by_status.get(t.status, 0) + 1
    parts = [f"{v} {k}" for k, v in sorted(by_status.items(), key=lambda x: x[1], reverse=True)]
    click.echo(f"  总计: {', '.join(parts)}")


def _bar(pct: float, width: int = 16) -> str:
    pct = min(max(pct, 0), 100)
    filled = int(width * pct / 100)
    return f"[{'█' * filled}{'░' * (width - filled)}]"


def _eta(seconds):
    if not seconds or seconds <= 0:
        return ""
    if seconds < 60:
        return f"~{seconds}s"
    if seconds < 3600:
        return f"~{seconds // 60}min"
    return f"~{seconds // 3600}h{(seconds % 3600) // 60}m"


def _health_icon(alive_at: str, now: datetime) -> str:
    if not alive_at:
        return "○"
    try:
        ts = datetime.fromisoformat(alive_at)
        age = (now - ts).total_seconds()
    except (ValueError, TypeError):
        return "○"
    if age < 180:
        return "●"
    elif age < 300:
        return "◐"
    return "○"
