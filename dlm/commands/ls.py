"""dlm ls — List tasks."""

import click

from ..core.state import StateManager
from ..constants import STATUSES, CATEGORIES

SORT_KEYS = ["name", "status", "server", "size", "category"]


@click.command("ls")
@click.option("--status", type=click.Choice(STATUSES), default=None, help="按状态筛选")
@click.option("--server", default=None, help="按服务器筛选")
@click.option("--category", type=click.Choice(CATEGORIES), default=None, help="按分类筛选")
@click.option("--priority", default=None, help="按优先级筛选")
@click.option("--sort", "sort_by", type=click.Choice(SORT_KEYS), default="name", help="排序字段")
@click.option("--reverse", "reverse_sort", is_flag=True, help="倒序排列")
@click.option("--all", "show_all", is_flag=True, help="包含 skipped/needs-auth")
@click.option("--format", "fmt", type=click.Choice(["table", "json", "csv"]), default="table")
def ls_cmd(status, server, category, priority, sort_by, reverse_sort, show_all, fmt):
    """列出下载任务。"""
    mgr = StateManager.create()
    state = mgr.load()

    tasks = state.tasks

    # Filters
    if status:
        tasks = [t for t in tasks if t.status == status]
    elif not show_all:
        tasks = [t for t in tasks if t.status not in ("skipped", "needs-auth")]
    if server:
        tasks = [t for t in tasks if t.server == server]
    if category:
        tasks = [t for t in tasks if t.category == category]
    if priority:
        tasks = [t for t in tasks if t.priority == priority]

    if not tasks:
        click.echo("无匹配任务。")
        return

    # Sort
    tasks = _sort_tasks(tasks, sort_by, reverse_sort)

    if fmt == "json":
        import json
        from dataclasses import asdict
        click.echo(json.dumps([asdict(t) for t in tasks], ensure_ascii=False, indent=2))
        return

    if fmt == "csv":
        click.echo("id,name,repo_id,source,category,size_gb,status,server,priority")
        for t in tasks:
            click.echo(f"{t.id},{t.name},{t.repo_id},{t.source},{t.category},{t.size_gb},{t.status},{t.server or '-'},{t.priority}")
        return

    # Table format
    _print_table(tasks)


STATUS_ORDER = {"downloading": 0, "dispatched": 1, "queued": 2, "failed": 3, "done": 4, "skipped": 5, "needs-auth": 6}


def _sort_tasks(tasks, sort_by, reverse):
    """Sort tasks by the given key."""
    if sort_by == "name":
        key = lambda t: t.name.lower()
    elif sort_by == "status":
        key = lambda t: STATUS_ORDER.get(t.status, 9)
    elif sort_by == "server":
        key = lambda t: (t.server or "ZZZ")
    elif sort_by == "size":
        key = lambda t: t.size_gb
        reverse = not reverse  # default large first
    elif sort_by == "category":
        key = lambda t: t.category
    else:
        key = lambda t: t.name.lower()
    return sorted(tasks, key=key, reverse=reverse)


def _print_table(tasks):
    """Print tasks as a formatted table."""
    sym = {
        "done": "✓", "downloading": "↓", "dispatched": "→",
        "queued": "○", "failed": "✗", "skipped": "⏸", "needs-auth": "🔒",
    }

    header = f"{'ID':<16} {'名称':<24} {'状态':<5} {'服务器':<4} {'大小':<8} {'分类':<12} {'来源':<4}"
    click.echo(header)
    click.echo("─" * len(header))

    for t in tasks:
        s = sym.get(t.status, "?")
        size_str = f"{t.size_gb:.1f}G" if t.size_gb else "-"
        name_display = t.name[:22] if len(t.name) > 22 else t.name
        click.echo(
            f"{t.id:<16} {name_display:<24} {s:<5} {t.server or '-':<4} {size_str:<8} {t.category:<12} {t.source:<4}"
        )

    click.echo("")
    # Summary
    total = len(tasks)
    by_status = {}
    for t in tasks:
        by_status[t.status] = by_status.get(t.status, 0) + 1
    parts = [f"{v} {k}" for k, v in sorted(by_status.items())]
    click.echo(f"共 {total} 个任务: {', '.join(parts)}")

    # Legend
    click.echo(f"图例: ✓ done  ↓ downloading  → dispatched  ○ queued  ✗ failed")
