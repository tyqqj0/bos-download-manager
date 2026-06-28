"""CLI command: transfer completed datasets from BOS to D-Robotics JuiceFS."""

import logging
import os

import click

logger = logging.getLogger(__name__)


@click.group("transfer")
def transfer_group():
    """Transfer datasets to D-Robotics cloud (地瓜云)."""
    pass


@transfer_group.command("push")
@click.argument("task_ids", nargs=-1)
@click.option("--all-done", is_flag=True, help="Push all completed (done) tasks")
@click.option("--category", help="Only push tasks in this category")
@click.option("--username", envvar="DCLOUD_USER", required=True, help="D-Robotics username")
@click.option("--password", envvar="DCLOUD_PASS", required=True, help="D-Robotics password")
@click.option("--wait/--no-wait", default=False, help="Wait for import to complete")
def push_cmd(task_ids, all_done, category, username, password, wait):
    """Push completed datasets from BOS to D-Robotics JuiceFS.

    Provide specific TASK_IDS or use --all-done to push all completed tasks.
    """
    from ..core.state import StateManager
    from ..transfer.dcloud import DCloudClient

    bos_ak = os.environ.get("BAIDU_AK")
    bos_sk = os.environ.get("BAIDU_SK")
    if not bos_ak or not bos_sk:
        raise click.ClickException("BAIDU_AK and BAIDU_SK must be set in environment")

    mgr = StateManager.create()
    state = mgr.load(use_cache=False)

    # Select tasks to push
    if all_done:
        tasks = [t for t in state.tasks if t.status == "done"]
        if category:
            tasks = [t for t in tasks if t.category == category]
    elif task_ids:
        tasks = [t for t in state.tasks if t.id in task_ids]
        missing = set(task_ids) - {t.id for t in tasks}
        if missing:
            raise click.ClickException(f"Tasks not found: {', '.join(missing)}")
    else:
        raise click.ClickException("Provide TASK_IDS or use --all-done")

    if not tasks:
        click.echo("No tasks to push.")
        return

    click.echo(f"Will push {len(tasks)} task(s) to D-Robotics cloud:")
    for t in tasks:
        click.echo(f"  {t.id}: {t.name} ({t.size_gb:.1f}GB) → {t.bos_path}")

    # Login to D-Robotics
    client = DCloudClient(username, password)
    try:
        client.login()
    except Exception as e:
        raise click.ClickException(f"D-Robotics login failed: {e}")

    # Push each task
    results = []
    for t in tasks:
        bos_path = t.bos_path.lstrip("/")
        target_path = f"/727a2f92-30c/auwomo-datasets/raw-data/{t.category}/{t.name}" if t.category else f"/727a2f92-30c/auwomo-datasets/raw-data/{t.name}"

        click.echo(f"\n→ Importing {t.name}...")
        click.echo(f"  BOS: {t.bos_path}")
        click.echo(f"  Target: {target_path}")

        try:
            task_id = client.import_from_bos(
                bos_ak=bos_ak,
                bos_sk=bos_sk,
                bos_bucket="westlake-autolab-databuilder-data",
                bos_path=bos_path,
                target_path=target_path,
            )
            click.echo(f"  ✓ Import task: {task_id}")
            results.append({"task": t, "import_id": task_id, "target": target_path})
        except Exception as e:
            click.echo(f"  ✗ Failed: {e}")

    if not results:
        click.echo("\nNo imports succeeded.")
        return

    click.echo(f"\n{len(results)}/{len(tasks)} import(s) started.")

    if wait:
        click.echo("\nWaiting for imports to complete...")
        for r in results:
            try:
                result = client.wait_for_task(r["import_id"], timeout_s=7200, poll_s=60)
                status = result.get("status", "unknown")
                click.echo(f"  {r['task'].name}: {status}")
            except TimeoutError:
                click.echo(f"  {r['task'].name}: timed out (still running)")


@transfer_group.command("status")
@click.option("--username", envvar="DCLOUD_USER", required=True)
@click.option("--password", envvar="DCLOUD_PASS", required=True)
def status_cmd(username, password):
    """Show recent import/transfer task status on D-Robotics."""
    from ..transfer.dcloud import DCloudClient

    client = DCloudClient(username, password)
    client.login()

    tasks = client.list_async_tasks(page_size=20)
    if not tasks:
        click.echo("No recent transfer tasks.")
        return

    click.echo(f"{'Status':<6} {'Type':<10} {'Source':<55} {'Target':<25} {'Created'}")
    click.echo("-" * 110)
    for t in tasks:
        source = t.get("source", "?")
        if len(source) > 53:
            source = "..." + source[-50:]
        click.echo(
            f"{t.get('status', '?'):<6} "
            f"{t.get('task_type', '?'):<10} "
            f"{source:<55} "
            f"{t.get('target', '?'):<25} "
            f"{t.get('created_at', '?')}"
        )


@transfer_group.command("warmup")
@click.argument("paths", nargs=-1, required=True)
@click.option("--storage-id", required=True, help="Target CacheGroup storage ID")
@click.option("--username", envvar="DCLOUD_USER", required=True)
@click.option("--password", envvar="DCLOUD_PASS", required=True)
def warmup_cmd(paths, storage_id, username, password):
    """Create a preheat/warmup task for paths on JuiceFS."""
    from ..transfer.dcloud import DCloudClient

    client = DCloudClient(username, password)
    client.login()

    click.echo(f"Creating warmup task for {len(paths)} path(s)...")
    try:
        client.create_warmup_task(
            source_paths=list(paths),
            target_storage_id=storage_id,
        )
        click.echo("✓ Warmup task created")
    except Exception as e:
        raise click.ClickException(f"Failed: {e}")


@transfer_group.command("storages")
@click.option("--username", envvar="DCLOUD_USER", required=True)
@click.option("--password", envvar="DCLOUD_PASS", required=True)
def storages_cmd(username, password):
    """List available CacheGroup storages (preheat targets)."""
    from ..transfer.dcloud import DCloudClient

    client = DCloudClient(username, password)
    client.login()

    storages = client.list_cachegroup_storages()
    if not storages:
        click.echo("No CacheGroup storages available.")
        return

    for s in storages:
        click.echo(f"  ID={s.get('id')} Name={s.get('name')} Cluster={s.get('cluster_name')}")
