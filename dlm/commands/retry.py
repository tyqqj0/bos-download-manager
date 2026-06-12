"""dlm retry — Retry failed tasks."""

import click

from ..core.state import StateManager
from ..core.models import _now
from ..core.parser import build_download_cmd
from ..core.selector import select_server
from ..core.ssh import ssh_append_queue, ssh_check_queue_contains


@click.command("retry")
@click.argument("name_or_id", required=False)
@click.option("--server", default=None, help="指定新服务器")
@click.option("--force", is_flag=True, help="允许重试非 failed 状态的任务")
@click.option("--all", "retry_all", is_flag=True, help="重试所有失败任务")
def retry_cmd(name_or_id, server, force, retry_all):
    """重试失败的下载任务。"""
    mgr = StateManager.create()
    state = mgr.load(use_cache=False)

    if retry_all:
        targets = [t for t in state.tasks if t.status == "failed"]
    elif name_or_id:
        task = state.find_task_by_id(name_or_id) or state.find_task_by_name(name_or_id)
        if not task:
            # Try matching repo_id
            task = state.find_task(name_or_id)
        if not task:
            click.echo(f"✗ 未找到任务: {name_or_id}")
            raise SystemExit(1)
        targets = [task]
    else:
        click.echo("请指定任务名称/ID，或使用 --all 重试所有失败任务。")
        raise SystemExit(1)

    if not targets:
        click.echo("无需重试的任务。")
        return

    retried = 0
    for task in targets:
        if task.status != "failed" and not force:
            click.echo(f"  ⚠ {task.name}: 状态为 {task.status}，非 failed（使用 --force 强制）")
            continue

        # Choose server
        target_server = server or task.server
        if not target_server:
            target_server = select_server(state)
        if not target_server:
            click.echo(f"  ✗ {task.name}: 无可用服务器")
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

        # Idempotent check
        if ssh_check_queue_contains(srv, task.repo_id):
            click.echo(f"  ⚠ {task.name}: 已在 {target_server} 队列中")
            task.status = "dispatched"
            task.server = target_server
            retried += 1
            continue

        ok = ssh_append_queue(srv, cmd)
        if ok:
            task.status = "dispatched"
            task.server = target_server
            task.dispatched_at = _now()
            task.error = None
            task.retry_count += 1
            click.echo(f"  ✓ {task.name} → {target_server} (重试 #{task.retry_count})")
            retried += 1
        else:
            click.echo(f"  ✗ {task.name}: SSH 派发失败")

    if retried > 0:
        mgr.save(state)
    click.echo(f"\n重试: {retried}/{len(targets)}")
