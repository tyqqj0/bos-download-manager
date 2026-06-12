"""dlm migrate — Import download-tracker.csv into BOS state."""

import csv
import click
from pathlib import Path

from ..core.state import StateManager
from ..core.models import Task, Server, State, _now
from ..constants import CATEGORIES


DEFAULT_CSV = "docs/download-tracker.csv"


@click.command("migrate")
@click.option("--csv", "csv_path", default=DEFAULT_CSV, help="CSV 文件路径")
@click.option("--dry-run", is_flag=True, help="预览不写入")
def migrate_cmd(csv_path, dry_run):
    """从 download-tracker.csv 导入到 BOS state.json（一次性）。"""
    csv_file = Path(csv_path)
    if not csv_file.exists():
        click.echo(f"✗ 文件不存在: {csv_path}")
        raise SystemExit(1)

    mgr = StateManager.create()

    # Read CSV
    tasks = []
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, 1):
            task = _row_to_task(row, i)
            if task:
                tasks.append(task)

    click.echo(f"读取 {len(tasks)} 个任务")

    # Build state
    state = State()
    state.categories = list(CATEGORIES)
    # Load servers from ~/.dlm/servers.yaml
    from ..core.servers import load_servers
    server_cfgs = load_servers()
    for key, cfg in server_cfgs.items():
        state.servers[key] = Server(key=key, host=cfg.host, user=cfg.user, path=cfg.path, enabled=cfg.enabled)
    state.tasks = tasks

    # Summary
    by_status = {}
    for t in tasks:
        by_status[t.status] = by_status.get(t.status, 0) + 1
    click.echo(f"\n状态分布:")
    for s, n in sorted(by_status.items()):
        click.echo(f"  {s}: {n}")

    if dry_run:
        click.echo(f"\n[dry-run] 不写入 BOS")
        return

    mgr.save(state)
    click.echo(f"\n✓ 已写入 BOS state.json ({len(tasks)} tasks)")


def _row_to_task(row: dict, index: int) -> Task:
    """Convert a CSV row to a Task."""
    name = row.get("name", "").strip()
    if not name:
        return None

    repo_id = row.get("repo_id", "").strip()
    source = row.get("source", "hf").strip()
    method = row.get("method", "").strip()

    # Map method to source
    if method in ("hf", "modelscope", "wget", "kaggle"):
        source = method
    elif method == "gui":
        source = "hf"

    category = row.get("category", "other").strip()
    if category not in CATEGORIES:
        category = "other"

    status = row.get("status", "queued").strip()
    status_map = {"done": "done", "downloading": "downloading", "queued": "queued",
                  "failed": "failed", "skipped": "skipped", "needs-auth": "needs-auth"}
    status = status_map.get(status, "queued")

    size_str = row.get("size_gb", "0").strip()
    try:
        size_gb = float(size_str)
    except ValueError:
        size_gb = 0.0

    server = row.get("server", "").strip()
    if server == "-" or not server:
        server = None

    priority = row.get("priority", "P1").strip()
    bos_path = row.get("bos_path", f"{category}/{name}/").strip()

    return Task(
        id=f"t-migrate-{index:03d}",
        name=name,
        repo_id=repo_id,
        source=source,
        type="dataset",
        category=category,
        bos_path=bos_path,
        size_gb=size_gb,
        status=status,
        server=server,
        priority=priority,
        created_at=_now(),
        notes=row.get("notes", "").strip(),
    )
