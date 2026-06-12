"""dlm dispatch — Dispatch queued tasks to servers."""

import click

from ..core.state import StateManager
from ..core.models import _now
from ..core.parser import build_download_cmd
from ..core.selector import select_server
from ..core.ssh import ssh_append_queue, ssh_check_queue_contains
from ..constants import PRIORITIES


@click.command("dispatch")
@click.option("--server", default=None, help="只派发到指定服务器")
@click.option("--priority", default=None, type=click.Choice(PRIORITIES), help="只派发该优先级及以上")
@click.option("--task", "task_name", default=None, help="指定任务名称或ID")
@click.option("--dry-run", is_flag=True, help="预览不执行")
def dispatch_cmd(server, priority, task_name, dry_run):
    """派发排队中的任务到服务器。"""
    mgr = StateManager.create()
    state = mgr.load(use_cache=False)

    # Filter tasks to dispatch
    candidates = [t for t in state.tasks if t.status == "queued"]

    if task_name:
        candidates = [t for t in candidates if t.name == task_name or t.id == task_name]
    if server:
        candidates = [t for t in candidates if t.server == server or t.server is None]
    if priority:
        pri_idx = PRIORITIES.index(priority)
        candidates = [t for t in candidates if PRIORITIES.index(t.priority) <= pri_idx]

    if not candidates:
        click.echo("无待派发任务。")
        return

    dispatched = 0
    for task in candidates:
        # Assign server if not set
        target = task.server or server
        if not target:
            target = select_server(state, exclude=[])
        if not target:
            click.echo(f"  ⚠ {task.name}: 无可用服务器")
            continue

        if target not in state.servers:
            click.echo(f"  ✗ {task.name}: 未知服务器 {target}")
            continue

        srv = state.servers[target]
        cmd = build_download_cmd(
            repo_id=task.repo_id,
            source=task.source,
            dtype=task.type,
            category=task.category,
            remote_path=srv.path,
            include=task.include,
            custom_name=None,
        )

        if dry_run:
            click.echo(f"  [dry-run] {task.name} → {target}: {cmd}")
            dispatched += 1
            continue

        # Idempotent check
        if ssh_check_queue_contains(srv, task.repo_id):
            click.echo(f"  ⚠ {task.name}: 已在 {target} 队列中")
            task.status = "dispatched"
            task.server = target
            task.dispatched_at = _now()
            dispatched += 1
            continue

        ok = ssh_append_queue(srv, cmd)
        if ok:
            task.status = "dispatched"
            task.server = target
            task.dispatched_at = _now()
            click.echo(f"  ✓ {task.name} → {target}")
            dispatched += 1
        else:
            click.echo(f"  ✗ {task.name}: SSH 派发到 {target} 失败")

    if not dry_run and dispatched > 0:
        mgr.save(state)

    click.echo(f"\n派发完成: {dispatched}/{len(candidates)}")
