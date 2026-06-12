"""DLM CLI — 数据集下载管理器。

Usage:
    python -m cli.dlm add <url_or_repo> -c <category>
    python -m cli.dlm ls
    python -m cli.dlm status
    python -m cli.dlm dispatch
    python -m cli.dlm sync --update
    python -m cli.dlm retry <name>
    python -m cli.dlm verify
    python -m cli.dlm server list
    python -m cli.dlm migrate --csv docs/download-tracker.csv
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


if __name__ == "__main__":
    cli()
