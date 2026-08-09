"""dlm status — Cluster status from dashboard API."""

import click
import time


@click.command("status")
@click.option("--json", "as_json", is_flag=True, help="JSON 输出")
def status_cmd(as_json):
    """显示集群状态和活跃下载。"""
    from ._api import get

    try:
        dashboard = get("/api/dashboard")
    except Exception as e:
        click.echo(f"✗ API 错误: {e}")
        raise SystemExit(1)

    if as_json:
        import json
        click.echo(json.dumps(dashboard, ensure_ascii=False, indent=2))
        return

    click.echo("═" * 62)
    click.echo("  DLM Cluster Status")
    click.echo("═" * 62)

    # Summary
    by_status = dashboard.get("by_status", {})
    total = dashboard.get("total_tasks", 0)
    dl_speed = dashboard.get("aggregate_download_speed_mbps", 0)
    ul_speed = dashboard.get("aggregate_upload_speed_mbps", 0)
    dl_tb = dashboard.get("total_downloaded_tb", 0)
    est_tb = dashboard.get("total_estimated_tb", 0)

    click.echo(f"\n  任务: {total} 总计 | {by_status.get('downloading', 0)} 下载中 | "
               f"{by_status.get('pending', 0)} 排队 | {by_status.get('done', 0)} 完成 | "
               f"{by_status.get('failed', 0)} 失败")
    click.echo(f"  速度: ↓{dl_speed:.1f} Mbps  ↑{ul_speed:.1f} Mbps")
    click.echo(f"  总量: {dl_tb:.1f}T / {est_tb:.1f}T")

    # Workers
    workers = dashboard.get("workers", [])
    now = time.time()
    click.echo(f"\n{'─' * 62}")
    click.echo("  Workers:")

    seen_keys = set()
    for w in workers:
        key = w.get("server_key", "?")
        if key in seen_keys:
            continue
        seen_keys.add(key)
        last_seen = w.get("last_seen") or 0
        age = now - last_seen
        disk = w.get("disk_free_gb", 0)

        if age < 180:
            icon, status = "●", "活跃"
        elif age < 600:
            icon, status = "◐", f"延迟 ({int(age)}s)"
        else:
            icon, status = "○", f"离线 ({int(age/60)}min)"

        click.echo(f"    {icon} {key:<5} {status:<16} disk={disk:.0f}GB free")

    # Active downloads
    active = dashboard.get("active_downloads", [])
    if active:
        click.echo(f"\n{'─' * 62}")
        click.echo("  Active Downloads:")
        for dl in active:
            name = dl.get("name", "?")[:35]
            srv = dl.get("server", "?")
            speed = dl.get("speed_mbps", 0)
            pct = dl.get("progress_pct", 0)
            phase = dl.get("phase", "")
            shards = dl.get("shard_servers", [])

            speed_str = f"{speed:.1f}Mbps" if speed > 0 else "-"
            bar = _bar(pct)

            if shards and len(shards) > 1:
                shard_str = ", ".join(s["server"] for s in shards)
                click.echo(f"    ↓ {name}")
                click.echo(f"      [{shard_str}] ({len(shards)} shards)")
                click.echo(f"      {bar} {pct:.0f}%  {speed_str}")
                for s in shards:
                    ss = f"{s.get('speed_mbps',0):.1f}Mbps" if s.get("speed_mbps", 0) > 0 else f"{s.get('done_pct',0):.0f}%"
                    click.echo(f"        {s['server']}: {ss}")
            else:
                click.echo(f"    ↓ {name} @ {srv}")
                click.echo(f"      {bar} {pct:.0f}%  {speed_str}  {phase}")

    # Alerts
    alerts = dashboard.get("alerts", [])
    if alerts:
        click.echo(f"\n{'─' * 62}")
        click.echo("  告警:")
        for a in alerts:
            sev = "!!" if a.get("severity") == "critical" else "⚠"
            click.echo(f"    {sev} {a.get('message', '')}")


def _bar(pct, width=20):
    pct = min(max(pct, 0), 100)
    filled = int(width * pct / 100)
    return f"[{'█' * filled}{'░' * (width - filled)}]"
