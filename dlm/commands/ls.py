"""dlm ls — List tasks via the web API."""

import click


SORT_KEYS = ["name", "status", "server", "size", "category"]


@click.command("ls")
@click.option("--status", default=None, help="按状态筛选 (downloading/queued/done/failed)")
@click.option("--server", default=None, help="按服务器筛选")
@click.option("--category", default=None, help="按分类筛选")
@click.option("--sort", "sort_by", type=click.Choice(SORT_KEYS), default="status", help="排序字段")
@click.option("--reverse", "reverse_sort", is_flag=True, help="倒序排列")
@click.option("--live", "live_mode", is_flag=True, help="只显示活跃任务 + 实时进度/速度")
@click.option("--all", "show_all", is_flag=True, help="包含 skipped")
@click.option("--format", "fmt", type=click.Choice(["table", "json"]), default="table")
def ls_cmd(status, server, category, sort_by, reverse_sort, live_mode, show_all, fmt):
    """列出下载任务。"""
    from ._api import get

    try:
        params = {"sort": sort_by, "reverse": reverse_sort}
        if status:
            params["status"] = status
        if server:
            params["server"] = server
        if category:
            params["category"] = category
        data = get("/api/tasks", **params)
    except Exception as e:
        click.echo(f"✗ API 错误: {e}")
        raise SystemExit(1)

    tasks = data.get("tasks", [])

    if live_mode:
        tasks = [t for t in tasks if t["status"] in ("downloading",)]
    elif not show_all:
        tasks = [t for t in tasks if t["status"] not in ("skipped",)]

    if not tasks:
        click.echo("无匹配任务。")
        return

    if fmt == "json":
        import json
        click.echo(json.dumps(tasks, ensure_ascii=False, indent=2))
        return

    if live_mode:
        _print_live(tasks)
    else:
        _print_table(tasks)


def _print_table(tasks):
    sym = {
        "done": "✓", "downloading": "↓", "dispatched": "→",
        "queued": "○", "failed": "✗", "skipped": "⏸",
        "paused": "⏸", "preempted": "⏪",
    }
    header = f"{'名称':<30} {'状态':<3} {'服务器':<6} {'大小':<14} {'速度':<10} {'分片':<8}"
    click.echo(header)
    click.echo("─" * 75)

    for t in tasks:
        s = sym.get(t["status"], "?")
        name = t["name"][:28] if len(t["name"]) > 28 else t["name"]
        srv = t.get("server") or "-"
        size_str = _format_size(t)
        speed = ""
        if t["status"] == "downloading" and t.get("speed_mbps", 0) > 0:
            speed = f"{t['speed_mbps']:.1f}MB/s"
        shards = ""
        ts = t.get("total_shards", 0)
        ds = t.get("done_shards", 0)
        if ts and ts > 1:
            shards = f"{ds}/{ts}"
        click.echo(f"{name:<30} {s:<3} {srv:<6} {size_str:<14} {speed:<10} {shards:<8}")

    click.echo("")
    total = len(tasks)
    by_status = {}
    for t in tasks:
        by_status[t["status"]] = by_status.get(t["status"], 0) + 1
    parts = [f"{v} {k}" for k, v in sorted(by_status.items())]
    click.echo(f"共 {total} 个任务: {', '.join(parts)}")


def _print_live(tasks):
    if not tasks:
        click.echo("无活跃下载。")
        return

    click.echo(f"{'服务器':<6} {'名称':<30} {'进度':<7} {'速度':<12} {'大小':<14} {'分片':<8}")
    click.echo("─" * 80)

    for t in tasks:
        name = t["name"][:28] if len(t["name"]) > 28 else t["name"]
        srv = t.get("server") or "-"
        pct = f"{t.get('progress_pct', 0):.0f}%" if t.get("progress_pct") else "-"
        speed = f"{t['speed_mbps']:.1f}MB/s" if t.get("speed_mbps", 0) > 0 else "-"
        size_str = _format_size(t)
        ts = t.get("total_shards", 0)
        ds = t.get("done_shards", 0)
        shards = f"{ds}/{ts}" if ts and ts > 1 else ""
        click.echo(f"{srv:<6} {name:<30} {pct:<7} {speed:<12} {size_str:<14} {shards:<8}")

    click.echo("")
    total_speed = sum(t.get("speed_mbps", 0) for t in tasks)
    click.echo(f"{len(tasks)} 个任务下载中, 总速度: {total_speed:.1f} MB/s")


def _format_size(t):
    dl = t.get("downloaded_gb", 0) or 0
    total = t.get("size_gb", 0) or 0
    if t["status"] == "done":
        return _human(dl) if dl > 0 else (_human(total) if total > 0 else "-")
    if total > 0:
        return f"{_human(dl)}/{_human(total)}"
    if dl > 0:
        return f"{_human(dl)}/?"
    return "-"


def _human(gb):
    if gb >= 1000:
        return f"{gb / 1000:.1f}T"
    if gb >= 10:
        return f"{gb:.0f}G"
    return f"{gb:.1f}G"
