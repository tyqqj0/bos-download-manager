"""dlm verify — Verify download integrity."""

import click

from ..core.state import StateManager
from ..core.ssh import ssh_server


@click.command("verify")
@click.option("--server", "server_filter", default=None, help="只检查指定服务器")
@click.option("--fix", is_flag=True, help="将异常任务标记为 failed")
@click.option("--status", "check_status", default="done", help="检查哪个状态的任务 (默认 done)")
def verify_cmd(server_filter, fix, check_status):
    """校验已完成下载的数据完整性。"""
    mgr = StateManager.create()
    state = mgr.load(use_cache=False)

    tasks = [t for t in state.tasks if t.status == check_status]
    if server_filter:
        tasks = [t for t in tasks if t.server == server_filter]

    if not tasks:
        click.echo(f"无 status={check_status} 的任务需要校验。")
        return

    results = {"verified": [], "missing": [], "incomplete": [], "error": []}

    for task in tasks:
        if not task.server or task.server not in state.servers:
            results["error"].append((task, "无服务器信息"))
            continue

        srv = state.servers[task.server]

        # Check if directory exists
        if task.type == "model":
            check_path = f"/mnt/auwomo-model/{task.name}"
        else:
            check_path = f"/mnt/auwomo-data/{task.bos_path}"

        out, ok = ssh_server(srv, f"test -d {check_path} && echo EXISTS || echo MISSING", timeout=10)

        if "MISSING" in out or not ok:
            results["missing"].append((task, check_path))
            continue

        # Check if directory has content
        out, ok = ssh_server(srv, f"ls {check_path} | head -3 | wc -l", timeout=15)
        if ok and out.strip() == "0":
            results["incomplete"].append((task, "目录为空"))
            continue

        # Size check (if size_gb is set)
        if task.size_gb > 0:
            out, ok = ssh_server(srv, f"du -s {check_path} 2>/dev/null | cut -f1", timeout=60)
            if ok and out.strip().isdigit():
                actual_kb = int(out.strip())
                actual_gb = actual_kb / 1024 / 1024
                expected_gb = task.size_gb
                ratio = actual_gb / expected_gb if expected_gb > 0 else 0
                if ratio < 0.5:
                    results["incomplete"].append((task, f"大小偏差: {actual_gb:.1f}G / {expected_gb:.1f}G ({ratio:.0%})"))
                    continue

        results["verified"].append((task, None))

    # Report
    click.echo(f"\n{'═' * 50}")
    click.echo(f"校验结果 ({len(tasks)} 个任务):")
    click.echo(f"  ✓ 通过: {len(results['verified'])}")
    click.echo(f"  ✗ 缺失: {len(results['missing'])}")
    click.echo(f"  ⚠ 不完整: {len(results['incomplete'])}")
    click.echo(f"  ? 错误: {len(results['error'])}")

    if results["missing"]:
        click.echo(f"\n缺失:")
        for task, path in results["missing"]:
            click.echo(f"  {task.name} [{task.server}]: {path}")

    if results["incomplete"]:
        click.echo(f"\n不完整:")
        for task, reason in results["incomplete"]:
            click.echo(f"  {task.name} [{task.server}]: {reason}")

    # Fix
    if fix and (results["missing"] or results["incomplete"]):
        changed = 0
        for task, _ in results["missing"] + results["incomplete"]:
            task.status = "failed"
            task.error = "verify: data missing or incomplete"
            changed += 1
        mgr.save(state)
        click.echo(f"\n✓ 已将 {changed} 个任务标记为 failed")
