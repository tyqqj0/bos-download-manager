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
@click.option("--size", "check_size", is_flag=True, help="实时查询 BOS 获取真实下载大小")
@click.option("--refresh", "refresh_total", is_flag=True, help="强制刷新所有 HF 总大小（含已有值的）")
@click.option("--all", "show_all", is_flag=True, help="包含 skipped/needs-auth")
@click.option("--format", "fmt", type=click.Choice(["table", "json", "csv"]), default="table")
def ls_cmd(status, server, category, priority, sort_by, reverse_sort, check_size, refresh_total, show_all, fmt):
    """列出下载任务。"""
    mgr = StateManager.create()
    state = mgr.load()

    # Real-time size check via BOS API
    if check_size or refresh_total:
        click.echo("正在查询真实大小...")
        from ..core.size import fetch_sizes, fetch_hf_total_sizes
        from ..core.config import load_config
        from ..core.bos import create_bos_client

        config = load_config()
        bos = create_bos_client(config["BAIDU_AK"], config["BAIDU_SK"], config["BOS_ENDPOINT"])

        # Downloaded sizes from BOS
        sizes = fetch_sizes(bos, state.tasks, verbose=True)
        updated = 0
        for task in state.tasks:
            if task.id in sizes:
                task.downloaded_gb = sizes[task.id]
                updated += 1

        # Total sizes from HuggingFace
        hf_token = config.get("HF_TOKEN", "")
        hf_sizes = fetch_hf_total_sizes(state.tasks, hf_token=hf_token or None, force=refresh_total)
        hf_updated = 0
        for task in state.tasks:
            if task.id in hf_sizes and hf_sizes[task.id] > 0:
                task.size_gb = hf_sizes[task.id]
                hf_updated += 1

        if updated or hf_updated:
            mgr.save(state)
            click.echo(f"已更新: {updated} 个下载大小, {hf_updated} 个总大小\n")

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
        click.echo("id,name,repo_id,source,category,size_gb,downloaded_gb,status,server,priority")
        for t in tasks:
            click.echo(f"{t.id},{t.name},{t.repo_id},{t.source},{t.category},{t.size_gb},{t.downloaded_gb},{t.status},{t.server or '-'},{t.priority}")
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
        reverse = not reverse
    elif sort_by == "category":
        key = lambda t: t.category
    else:
        key = lambda t: t.name.lower()
    return sorted(tasks, key=key, reverse=reverse)


def _format_size(task) -> str:
    """Format size column: always show downloaded/total when possible."""
    dl = _human_size(task.downloaded_gb) if task.downloaded_gb > 0 else "0"
    if task.size_gb > 0:
        total = _human_size(task.size_gb)
        if task.status == "done" and task.downloaded_gb > 0:
            return _human_size(task.downloaded_gb)
        return f"{dl}/{total}"
    if task.downloaded_gb > 0:
        return f"{dl}/?"
    return "-"


def _human_size(gb: float) -> str:
    """Format GB value concisely."""
    if gb >= 1000:
        return f"{gb / 1000:.1f}T"
    if gb >= 10:
        return f"{gb:.0f}G"
    return f"{gb:.1f}G"


def _print_table(tasks):
    """Print tasks as a formatted table."""
    sym = {
        "done": "✓", "downloading": "↓", "dispatched": "→",
        "queued": "○", "failed": "✗", "skipped": "⏸", "needs-auth": "🔒",
    }

    header = f"{'ID':<16} {'名称':<24} {'状态':<5} {'服务器':<4} {'大小':<12} {'分类':<12} {'来源':<4}"
    click.echo(header)
    click.echo("─" * len(header))

    for t in tasks:
        s = sym.get(t.status, "?")
        size_str = _format_size(t)
        name_display = t.name[:22] if len(t.name) > 22 else t.name
        click.echo(
            f"{t.id:<16} {name_display:<24} {s:<5} {t.server or '-':<4} {size_str:<12} {t.category:<12} {t.source:<4}"
        )

    click.echo("")
    # Summary
    total = len(tasks)
    by_status = {}
    for t in tasks:
        by_status[t.status] = by_status.get(t.status, 0) + 1
    parts = [f"{v} {k}" for k, v in sorted(by_status.items())]
    click.echo(f"共 {total} 个任务: {', '.join(parts)}")

    # Total size
    total_dl = sum(t.downloaded_gb for t in tasks)
    total_est = sum(t.size_gb for t in tasks)
    if total_dl > 0:
        click.echo(f"总量: {_human_size(total_dl)} 已下载 / {_human_size(total_est)} 预估")
    else:
        click.echo(f"总量: {_human_size(total_est)} 预估 (用 --size 获取真实大小)")

    # Legend
    click.echo(f"图例: ✓ done  ↓ downloading  → dispatched  ○ queued  ✗ failed")
