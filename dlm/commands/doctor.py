"""dlm doctor — Health check and auto-repair for download workers."""

import click

from ..core.state import StateManager
from ..core.models import _now
from ..core.parser import build_download_cmd
from ..core.selector import select_server
from ..core.ssh import ssh_parallel, ssh_append_queue, ssh_check_queue_contains
from ..core.health import restart_worker, check_task_stuck


@click.command("doctor")
@click.option("--dry", is_flag=True, help="只检查不修复")
@click.option("--apply", is_flag=True, help="自动重试卡住的任务")
def doctor_cmd(dry, apply):
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
    stuck_tasks = []

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
            stuck['server'] = key
            stuck_tasks.append(stuck)
            click.echo(f"      ⚠ {stuck['task']} 已运行 {stuck['hours']}h 无进展")
            if not apply:
                click.echo(f"        建议: dlm retry {stuck['task']}")

    # Phase 3: Auto-retry stuck tasks if --apply
    retried = 0
    if apply and stuck_tasks and not dry:
        click.echo(f"\n{'─' * 45}")
        click.echo("  自动重试卡住的任务:")
        for stuck in stuck_tasks:
            task_name = stuck['task']
            task = state.find_task_by_name(task_name) or state.find_task(task_name)
            if not task:
                click.echo(f"    ✗ {task_name}: 未在 state 中找到")
                continue

            target_server = select_server(state)
            if not target_server:
                click.echo(f"    ✗ {task_name}: 无可用服务器")
                continue

            srv = state.servers[target_server]
            cmd = build_download_cmd(
                repo_id=task.repo_id,
                source=task.source,
                dtype=task.type,
                category=task.category,
                remote_path=srv.path,
                include=task.include,
            )

            if ssh_check_queue_contains(srv, task.repo_id):
                click.echo(f"    ⚠ {task_name}: 已在 {target_server} 队列中")
                continue

            ok = ssh_append_queue(srv, cmd)
            if ok:
                task.status = "dispatched"
                task.server = target_server
                task.dispatched_at = _now()
                task.error = None
                task.retry_count += 1
                retried += 1
                click.echo(f"    ✓ {task_name} → {target_server} (重试 #{task.retry_count})")
            else:
                click.echo(f"    ✗ {task_name}: SSH 派发失败")

    # Save if state changed
    if (restarted_keys or retried > 0) and not dry:
        if retried > 0:
            mgr.save(state)

    # Summary
    click.echo(f"\n{'─' * 45}")
    parts = []
    if restarted_keys:
        parts.append(f"修复: {len(restarted_keys)} 个 worker 重启")
    if dead_keys and dry:
        parts.append(f"问题: {len(dead_keys)} 个 worker 已停止 (--dry 未修复)")
    if stuck_tasks:
        if apply and not dry:
            parts.append(f"重试: {retried}/{len(stuck_tasks)} 个卡住任务")
        else:
            parts.append(f"警告: {len(stuck_tasks)} 个任务疑似卡住")
    if not dead_keys and not stuck_tasks:
        parts.append("所有服务器健康 ✓")

    for p in parts:
        click.echo(f"  {p}")
