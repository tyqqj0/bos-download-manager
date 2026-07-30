"""dlm add — Add a new download task via the web API."""

import click

from ..constants import CATEGORIES, PRIORITIES


@click.command("add")
@click.argument("url_or_repo")
@click.option("-c", "--category", required=True, type=click.Choice(CATEGORIES), help="数据分类")
@click.option("-t", "--type", "dtype", default="dataset", type=click.Choice(["dataset", "model"]), help="类型")
@click.option("-p", "--priority", default="P1", type=click.Choice(PRIORITIES), help="优先级")
@click.option("-n", "--name", default=None, help="自定义目录名")
@click.option("--size", default=0.0, type=float, help="预估大小 (GB)")
@click.option("--no-dispatch", is_flag=True, help="只添加到列表，不自动派发")
@click.option("--source", default=None, type=click.Choice(["hf", "modelscope"]), help="覆盖自动检测的来源")
def add_cmd(url_or_repo, category, dtype, priority, name, size, no_dispatch, source):
    """添加下载任务。

    URL_OR_REPO: HuggingFace/ModelScope URL 或裸 repo_id (org/name)
    """
    from ._api import post

    body = {
        "url_or_repo": url_or_repo,
        "category": category,
        "type": dtype,
        "priority": priority,
        "no_dispatch": no_dispatch,
    }
    if name:
        body["name"] = name
    if size:
        body["size_gb"] = size

    try:
        data = post("/api/tasks", body)
    except Exception as e:
        click.echo(f"✗ API 错误: {e}")
        raise SystemExit(1)

    if "error" in data or "detail" in data:
        click.echo(f"✗ {data.get('error') or data.get('detail')}")
        raise SystemExit(1)

    task = data.get("task", {})
    click.echo(f"✓ 已添加任务")
    click.echo(f"  ID:       {task.get('id', '?')}")
    click.echo(f"  名称:     {task.get('name', '?')}")
    click.echo(f"  分类:     {task.get('category', category)}")
    click.echo(f"  状态:     {task.get('status', 'queued')}")
    click.echo(f"  优先级:   {task.get('priority', priority)}")
    if not no_dispatch:
        click.echo(f"\n  任务已加入队列，auto_dispatch 会自动分配到空闲 worker。")
