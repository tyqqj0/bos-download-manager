"""dlm add — Add a new download task."""

import click

from ..core.models import Task, State
from ..core.state import StateManager
from ..core.parser import parse_repo, build_download_cmd, derive_bos_path
from ..core.selector import select_server
from ..core.ssh import ssh_append_queue, ssh_check_queue_contains
from ..constants import CATEGORIES, PRIORITIES


@click.command("add")
@click.argument("url_or_repo")
@click.option("-c", "--category", required=True, type=click.Choice(CATEGORIES), help="数据分类")
@click.option("-t", "--type", "dtype", default="dataset", type=click.Choice(["dataset", "model"]), help="类型")
@click.option("-s", "--server", default=None, help="指定服务器 (S1-S4)，不填自动分配")
@click.option("-p", "--priority", default="P1", type=click.Choice(PRIORITIES), help="优先级")
@click.option("-n", "--name", default=None, help="自定义目录名")
@click.option("--include", default=None, help="文件匹配模式")
@click.option("--size", default=0.0, type=float, help="预估大小 (GB)")
@click.option("--no-dispatch", is_flag=True, help="只添加到列表，不派发")
@click.option("--source", default=None, type=click.Choice(["hf", "modelscope"]), help="覆盖自动检测的来源")
def add_cmd(url_or_repo, category, dtype, server, priority, name, include, size, no_dispatch, source):
    """添加下载任务。

    URL_OR_REPO: HuggingFace/ModelScope URL 或裸 repo_id (org/name)
    """
    # Parse URL
    parsed = parse_repo(url_or_repo)
    if source:
        parsed["source"] = source
    if dtype:
        parsed["type"] = dtype

    if parsed["source"] == "unknown":
        click.echo(f"无法识别来源: {url_or_repo}")
        click.echo("请使用完整 URL 或指定 --source hf/modelscope")
        raise SystemExit(1)

    task_name = name or parsed["name"]
    repo_id = parsed["repo_id"]

    # Load state
    mgr = StateManager.create()
    state = mgr.load(use_cache=False)

    # Dedup check
    existing = state.find_task(repo_id)
    if existing:
        if existing.status == "done":
            click.echo(f"✗ 已存在且已完成: {existing.name} (id={existing.id})")
            raise SystemExit(1)
        elif existing.status in ("dispatched", "downloading"):
            click.echo(f"✗ 已在下载中: {existing.name} @ {existing.server} (status={existing.status})")
            raise SystemExit(1)
        elif existing.status == "failed":
            click.echo(f"该任务之前失败过 (id={existing.id})。使用 dlm retry {existing.name} 重试。")
            raise SystemExit(1)
        elif existing.status == "queued":
            click.echo(f"✗ 已在队列中: {existing.name} (id={existing.id})")
            raise SystemExit(1)

    # Create task
    bos_path = derive_bos_path(category, repo_id, parsed["type"])
    task = Task(
        id=state.next_task_id(),
        name=task_name,
        repo_id=repo_id,
        source=parsed["source"],
        type=parsed["type"],
        category=category,
        bos_path=bos_path,
        size_gb=size,
        priority=priority,
        include=include,
        status="queued",
    )

    # Server selection
    if server:
        if server not in state.servers:
            click.echo(f"✗ 未知服务器: {server}")
            raise SystemExit(1)
        task.server = server
    elif not no_dispatch:
        chosen = select_server(state)
        if chosen:
            task.server = chosen
        else:
            click.echo("⚠ 无可用服务器，任务添加为 queued 状态")
            no_dispatch = True

    state.tasks.append(task)

    # Dispatch
    if not no_dispatch and task.server:
        srv = state.servers[task.server]
        cmd = build_download_cmd(
            repo_id=repo_id,
            source=parsed["source"],
            dtype=parsed["type"],
            category=category,
            remote_path=srv.path,
            include=include,
            custom_name=name,
        )

        # Idempotent: check if already in remote queue
        if ssh_check_queue_contains(srv, repo_id):
            click.echo(f"⚠ {repo_id} 已在 {task.server} 的队列中，跳过派发")
            task.status = "dispatched"
        else:
            ok = ssh_append_queue(srv, cmd)
            if ok:
                task.status = "dispatched"
                from ..core.models import _now
                task.dispatched_at = _now()
                click.echo(f"✓ 已派发到 {task.server}")
            else:
                click.echo(f"⚠ SSH 派发失败，任务保留为 queued")

    # Save state
    mgr.save(state)

    # Output
    click.echo(f"")
    click.echo(f"  ID:       {task.id}")
    click.echo(f"  名称:     {task.name}")
    click.echo(f"  来源:     {parsed['source']} ({repo_id})")
    click.echo(f"  分类:     {category}")
    click.echo(f"  BOS路径:  {bos_path}")
    click.echo(f"  服务器:   {task.server or '未分配'}")
    click.echo(f"  状态:     {task.status}")
