"""dlm sync — Reconcile remote server state with central state."""

import re
import click

from ..core.state import StateManager
from ..core.models import _now
from ..core.ssh import ssh_server, ssh_recent_log, ssh_check_current


@click.command("sync")
@click.option("--server", "server_filter", default=None, help="只同步指定服务器")
@click.option("--update", is_flag=True, help="实际更新 state（默认只报告差异）")
def sync_cmd(server_filter, update):
    """同步远程服务器状态到中心 state.json。"""
    mgr = StateManager.create()
    state = mgr.load(use_cache=False)

    servers = state.servers
    if server_filter:
        servers = {k: v for k, v in servers.items() if k == server_filter}

    changes = []

    for key, srv in servers.items():
        if not srv.enabled:
            continue

        click.echo(f"\n同步 {key} ({srv.host})...")

        # Get recent log entries
        log_text = ssh_recent_log(srv, lines=100)
        current = ssh_check_current(srv)

        # Parse DONE and FAILED from logs
        done_repos = set()
        failed_repos = {}
        for line in log_text.splitlines():
            m_done = re.search(r"DONE: .+/(download(?:-modelscope)?\.sh)\s+(\S+)", line)
            m_fail = re.search(r"FAILED \(exit (\d+)\): .+/(download(?:-modelscope)?\.sh)\s+(\S+)", line)
            if m_done:
                done_repos.add(m_done.group(2))
            if m_fail:
                failed_repos[m_fail.group(3)] = f"exit {m_fail.group(1)}"

        # Reconcile tasks
        for task in state.tasks:
            if task.server != key:
                continue

            if task.status in ("done", "skipped", "needs-auth"):
                continue

            if task.repo_id in done_repos and task.status != "done":
                changes.append((task, "done", None))
                if update:
                    task.status = "done"
                    task.completed_at = _now()

            elif task.repo_id in failed_repos and task.status not in ("failed",):
                err = failed_repos[task.repo_id]
                changes.append((task, "failed", err))
                if update:
                    task.status = "failed"
                    task.error = err

            elif current and task.repo_id in current and task.status != "downloading":
                changes.append((task, "downloading", None))
                if update:
                    task.status = "downloading"

    # Report
    if not changes:
        click.echo("\n无变更。state 与服务器一致。")
        return

    click.echo(f"\n{'═' * 50}")
    click.echo(f"发现 {len(changes)} 个状态变更:")
    for task, new_status, err in changes:
        err_str = f" ({err})" if err else ""
        click.echo(f"  {task.name} [{task.server}]: {task.status} → {new_status}{err_str}")

    if update:
        mgr.save(state)
        click.echo(f"\n✓ 已更新 state.json")
    else:
        click.echo(f"\n使用 --update 写入变更。")
