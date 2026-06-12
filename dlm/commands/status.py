"""dlm status — Real-time status of all servers (parallel SSH)."""

import click
import time

from ..core.state import StateManager
from ..core.servers import load_servers
from ..core.models import Server
from ..core.ssh import ssh_parallel


@click.command("status")
@click.option("--watch", is_flag=True, help="每 15 秒自动刷新")
@click.option("--server", "server_filter", default=None, help="只看指定服务器")
def status_cmd(watch, server_filter):
    """显示所有服务器实时状态。"""
    if watch:
        try:
            while True:
                click.clear()
                _show_status(server_filter)
                click.echo(f"\n[每 15 秒刷新，Ctrl+C 退出]")
                time.sleep(15)
        except KeyboardInterrupt:
            pass
    else:
        _show_status(server_filter)


def _show_status(server_filter=None):
    # Load server config from yaml
    server_cfgs = load_servers()
    if server_filter:
        server_cfgs = {k: v for k, v in server_cfgs.items() if k == server_filter}

    if not server_cfgs:
        click.echo("无注册服务器。使用 'dlm init' 或 'dlm server add' 配置。")
        return

    # Build Server models
    srv_models = [
        Server(key=k, host=c.host, user=c.user, path=c.path, enabled=c.enabled)
        for k, c in server_cfgs.items() if c.enabled and not c.local
    ]

    # Parallel SSH: get worker status + current task + queue depth in one command
    status_cmd_str = (
        "tmux has-session -t worker 2>/dev/null && echo WORKER_ALIVE || echo WORKER_DEAD; "
        "cat ~/code/auwomo-tools/current.txt 2>/dev/null || echo ''; "
        "echo '---QUEUE---'; "
        "wc -l < ~/code/auwomo-tools/queue.txt 2>/dev/null || echo 0"
    )
    results = ssh_parallel(srv_models, status_cmd_str, timeout=10) if srv_models else {}

    # Load state for task summary
    try:
        mgr = StateManager.create()
        state = mgr.load()
    except Exception:
        state = None

    click.echo("═" * 60)
    click.echo("  DLM 服务器状态")
    click.echo("═" * 60)

    for key, cfg in server_cfgs.items():
        if not cfg.enabled:
            click.echo(f"\n  {key} ({cfg.host}) — DISABLED")
            continue

        if cfg.local:
            click.echo(f"\n  ● {key} ({cfg.host}) — 本机 (master)")
            continue

        out, ok = results.get(key, ("", False))
        if not ok:
            click.echo(f"\n  ✗ {key} ({cfg.host}) — 连接失败")
            continue

        lines = out.split("\n")
        alive = "WORKER_ALIVE" in lines[0] if lines else False
        status_icon = "●" if alive else "○"
        worker_status = "运行中" if alive else "已停止"

        # Parse current task
        current = ""
        queue_depth = 0
        in_queue_section = False
        for line in lines[1:]:
            if "---QUEUE---" in line:
                in_queue_section = True
                continue
            if in_queue_section:
                try:
                    queue_depth = int(line.strip())
                except ValueError:
                    pass
            else:
                if line.strip():
                    current = line.strip()

        click.echo(f"\n  {status_icon} {key} ({cfg.host}) — Worker {worker_status}")

        if current:
            repo = _extract_repo_name(current)
            click.echo(f"    当前: {repo}")
        else:
            click.echo(f"    当前: 空闲")

        click.echo(f"    队列: {queue_depth} 个待执行")

        # Tasks from state
        if state:
            active = state.active_tasks_for_server(key)
            if active:
                names = ', '.join(t.name for t in active[:3])
                suffix = '...' if len(active) > 3 else ''
                click.echo(f"    分配: {len(active)} 个 ({names}{suffix})")

    # Overall summary
    if state:
        click.echo(f"\n{'─' * 60}")
        total = len(state.tasks)
        by_status = {}
        for t in state.tasks:
            by_status[t.status] = by_status.get(t.status, 0) + 1
        parts = [f"{v} {k}" for k, v in sorted(by_status.items())]
        click.echo(f"  总计 {total} 个任务: {', '.join(parts)}")


def _extract_repo_name(cmd_line: str) -> str:
    """Extract a readable repo name from a download command."""
    parts = cmd_line.split()
    for i, p in enumerate(parts):
        if p.endswith("download.sh") or p.endswith("download-modelscope.sh"):
            if i + 1 < len(parts):
                return parts[i + 1]
    return cmd_line[:60]
