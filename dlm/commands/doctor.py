"""dlm doctor — Health check and auto-repair for download workers."""

import click

from ..core.state import StateManager
from ..core.ssh import ssh_worker_alive, ssh_parallel
from ..core.health import restart_worker, check_task_stuck, check_and_restart_workers


@click.command("doctor")
@click.option("--dry", is_flag=True, help="只检查不修复")
def doctor_cmd(dry):
    """检查所有 worker 健康状态，自动修复问题。"""
    mgr = StateManager.create()
    state = mgr.load(use_cache=False)

    servers = {k: v for k, v in state.servers.items() if v.enabled}
    if not servers:
        click.echo("没有已启用的服务器。")
        return

    click.echo(f"{'═' * 45}")
    click.echo("  DLM 健康检查")
    click.echo(f"{'═' * 45}")

    # Phase 1: Check worker alive (parallel)
    alive_results = ssh_parallel(
        list(servers.values()),
        "tmux has-session -t worker 2>/dev/null"
    )

    dead_keys = []
    restarted_keys = []
    stuck_count = 0

    for key, srv in servers.items():
        alive = alive_results.get(key, ("", False))[1]

        if alive:
            click.echo(f"\n  {key}: Worker 正常 ✓")
        else:
            dead_keys.append(key)
            if dry:
                click.echo(f"\n  {key}: Worker 已停止 ✗")
            else:
                _, ok = restart_worker(srv)
                if ok:
                    restarted_keys.append(key)
                    click.echo(f"\n  {key}: Worker 已停止 → 已重启 ✓")
                else:
                    click.echo(f"\n  {key}: Worker 已停止 → 重启失败 ✗")

        # Phase 2: Check stuck tasks
        stuck = check_task_stuck(srv)
        if stuck:
            stuck_count += 1
            click.echo(f"      ⚠ {stuck['task']} 已运行 {stuck['hours']}h 无进展")
            click.echo(f"        建议: dlm retry {stuck['task']}")

    # Summary
    click.echo(f"\n{'─' * 45}")
    parts = []
    if restarted_keys:
        parts.append(f"修复: {len(restarted_keys)} 个 worker 重启")
    if dead_keys and dry:
        parts.append(f"问题: {len(dead_keys)} 个 worker 已停止 (--dry 未修复)")
    if stuck_count:
        parts.append(f"警告: {stuck_count} 个任务疑似卡住")
    if not dead_keys and not stuck_count:
        parts.append("所有服务器健康 ✓")

    for p in parts:
        click.echo(f"  {p}")
