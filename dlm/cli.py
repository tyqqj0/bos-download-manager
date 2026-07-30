"""DLM CLI — 数据集下载管理器。

Usage:
    dlm add <url_or_repo> -c <category>
    dlm ls [--live] [--size]
    dlm status
    dlm server list
    dlm web
"""

import click


@click.group()
@click.version_option(version="0.1.0", prog_name="dlm")
def cli():
    """DLM — 数据集下载管理 CLI

    管理多服务器数据集下载，状态存储在 S1 SQLite（经 HTTP API）。
    """
    pass


# Register commands
from .commands.add import add_cmd
from .commands.ls import ls_cmd
from .commands.status import status_cmd
from .commands.server import server_cmd
from .commands.migrate import migrate_cmd
from .commands.init_cmd import init_cmd
from .commands.web import web_cmd

cli.add_command(add_cmd)
cli.add_command(ls_cmd)
cli.add_command(status_cmd)
cli.add_command(server_cmd)
cli.add_command(migrate_cmd)
cli.add_command(init_cmd)
cli.add_command(web_cmd)


if __name__ == "__main__":
    cli()
