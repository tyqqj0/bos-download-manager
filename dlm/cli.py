"""DLM CLI — 数据集下载管理器。

Usage:
    dlm add <url_or_repo> -c <category>
    dlm ls [--live] [--size]
    dlm status
    dlm dispatch
    dlm sync --update
    dlm retry <name>
    dlm verify
    dlm server list
    dlm web
    dlm doctor
"""

import click


@click.group()
@click.version_option(version="0.1.0", prog_name="dlm")
def cli():
    """DLM — 数据集下载管理 CLI

    管理多服务器数据集下载，状态集中存储在 BOS。
    """
    pass


# Register commands
from .commands.add import add_cmd
from .commands.ls import ls_cmd
from .commands.dispatch import dispatch_cmd
from .commands.status import status_cmd
from .commands.sync import sync_cmd
from .commands.retry import retry_cmd
from .commands.verify import verify_cmd
from .commands.server import server_cmd
from .commands.migrate import migrate_cmd
from .commands.init_cmd import init_cmd
from .commands.doctor import doctor_cmd
from .commands.web import web_cmd
from .commands.watch import watch_cmd
from .commands.transfer import transfer_group

cli.add_command(add_cmd)
cli.add_command(ls_cmd)
cli.add_command(dispatch_cmd)
cli.add_command(status_cmd)
cli.add_command(sync_cmd)
cli.add_command(retry_cmd)
cli.add_command(verify_cmd)
cli.add_command(server_cmd)
cli.add_command(migrate_cmd)
cli.add_command(init_cmd)
cli.add_command(doctor_cmd)
cli.add_command(web_cmd)
cli.add_command(watch_cmd)
cli.add_command(transfer_group)


@cli.group("worker")
def worker_group():
    """Worker daemon 管理（在下载服务器上运行）。"""
    pass


@worker_group.command("start")
@click.argument("server_key")
@click.option("--log-level", default="INFO", help="Log level (DEBUG/INFO/WARNING/ERROR)")
def worker_start(server_key, log_level):
    """启动 worker daemon。在下载服务器上运行。

    SERVER_KEY: 此服务器的标识 (如 w1, w2, S1)
    """
    import logging
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    from .worker.daemon import WorkerDaemon
    daemon = WorkerDaemon(server_key=server_key)
    daemon.run()


@worker_group.command("install")
@click.argument("server_key")
def worker_install(server_key):
    """生成 systemd service 文件并安装。"""
    service = f"""[Unit]
Description=DLM Worker Daemon ({server_key})
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=root
WorkingDirectory=/root/code/auwomo-tools
EnvironmentFile=/root/code/auwomo-tools/.env
ExecStart=/usr/local/bin/dlm-worker --server-key {server_key}
Restart=always
RestartSec=10
WatchdogSec=180

[Install]
WantedBy=multi-user.target
"""
    path = f"/etc/systemd/system/dlm-worker.service"
    click.echo(f"写入 {path}:")
    click.echo(service)
    click.echo(f"\n安装命令:")
    click.echo(f"  sudo tee {path} << 'EOF'\n{service}EOF")
    click.echo(f"  sudo systemctl daemon-reload")
    click.echo(f"  sudo systemctl enable --now dlm-worker")


if __name__ == "__main__":
    cli()
