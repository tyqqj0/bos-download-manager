"""dlm web — Start the web dashboard server."""

import click


@click.command("web")
@click.option("--host", default="0.0.0.0", help="监听地址")
@click.option("--port", default=8080, type=int, help="监听端口")
@click.option("--reload", is_flag=True, help="开发模式（自动重载）")
def web_cmd(host, port, reload):
    """启动 DLM Web 仪表盘。"""
    try:
        import uvicorn
    except ImportError:
        click.echo("错误: 需要安装 web 依赖")
        click.echo("运行: pip install -e '.[web]'")
        raise SystemExit(1)

    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    click.echo(f"DLM Dashboard starting on http://{host}:{port}")
    click.echo(f"API docs: http://{host}:{port}/docs")

    uvicorn.run(
        "dlm.web.app:create_app",
        factory=True,
        host=host,
        port=port,
        reload=reload,
        log_level="info",
    )
