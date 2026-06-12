"""dlm server — Server management."""

import click

from ..core.servers import load_servers, add_server, remove_server, ServerConfig, save_servers
from ..core.models import Server
from ..core.ssh import ssh_exec, ssh_server, ssh_worker_alive, ssh_parallel


@click.group("server")
def server_cmd():
    """服务器管理。"""
    pass


@server_cmd.command("list")
def server_list():
    """列出所有注册服务器及其实时状态。"""
    servers = load_servers()

    if not servers:
        click.echo("无注册服务器。使用 'dlm server add' 或 'dlm init' 添加。")
        return

    click.echo(f"{'Key':<5} {'Host':<18} {'User':<6} {'Enabled':<8} {'Worker':<8} {'Notes'}")
    click.echo("─" * 65)

    # Build Server models for SSH check
    srv_models = {
        k: Server(key=k, host=c.host, user=c.user, path=c.path, enabled=c.enabled)
        for k, c in servers.items()
    }

    for key, cfg in servers.items():
        srv = srv_models[key]
        if cfg.enabled and not cfg.local:
            alive = ssh_worker_alive(srv)
        elif cfg.local:
            alive = True
        else:
            alive = False
        worker_str = "● 运行" if alive else "○ 停止"
        enabled_str = "是" if cfg.enabled else "否"
        click.echo(f"{key:<5} {cfg.host:<18} {cfg.user:<6} {enabled_str:<8} {worker_str:<8} {cfg.notes}")


@server_cmd.command("add")
@click.argument("key")
@click.option("--host", required=True, help="IP 或域名")
@click.option("--user", default="root", help="SSH 用户")
@click.option("--path", default="~/code/auwomo-tools", help="远程工具路径")
@click.option("--local", is_flag=True, help="标记为本机（跳过 SSH）")
@click.option("--setup-key", is_flag=True, help="自动配置 SSH 免密")
@click.option("--notes", default="", help="备注")
def server_add(key, host, user, path, local, setup_key, notes):
    """注册新服务器。"""
    # Test connectivity first (unless local)
    if not local:
        click.echo(f"测试 SSH 连接 {user}@{host}...")
        out, ok = ssh_exec(host, user, "echo OK", timeout=10)
        if not ok:
            if setup_key:
                click.echo(f"  连接失败，尝试配置 SSH key...")
                from ..core.ssh import ssh_copy_id
                out, ok = ssh_copy_id(host, user)
                if not ok:
                    click.echo(f"✗ SSH key 配置失败: {out}")
                    click.echo(f"  请手动执行: ssh-copy-id {user}@{host}")
                    raise SystemExit(1)
                # Retry
                out, ok = ssh_exec(host, user, "echo OK", timeout=10)
                if not ok:
                    click.echo(f"✗ 仍然无法连接: {out}")
                    raise SystemExit(1)
            else:
                click.echo(f"✗ SSH 连接失败: {out}")
                click.echo(f"  提示: 添加 --setup-key 自动配置免密")
                raise SystemExit(1)
        click.echo(f"✓ 连接成功")

    try:
        add_server(key, host, user=user, path=path, local=local, notes=notes)
    except ValueError as e:
        click.echo(f"✗ {e}")
        raise SystemExit(1)

    click.echo(f"✓ 已注册服务器 {key} ({host})")


@server_cmd.command("remove")
@click.argument("key")
@click.option("--force", is_flag=True, help="跳过确认")
def server_remove(key, force):
    """移除服务器。"""
    servers = load_servers()
    if key not in servers:
        click.echo(f"✗ 服务器 {key} 不存在。")
        raise SystemExit(1)

    if not force:
        click.confirm(f"确认移除 {key} ({servers[key].host})?", abort=True)

    remove_server(key)
    click.echo(f"✓ 已移除 {key}")


@server_cmd.command("test")
@click.argument("key", required=False)
@click.option("--all", "test_all", is_flag=True, help="测试所有服务器")
def server_test(key, test_all):
    """测试服务器连通性（并行）。"""
    servers = load_servers()

    if not servers:
        click.echo("无注册服务器。")
        return

    targets = {}
    if test_all:
        targets = servers
    elif key:
        if key not in servers:
            click.echo(f"✗ 服务器 {key} 不存在。")
            raise SystemExit(1)
        targets = {key: servers[key]}
    else:
        click.echo("请指定服务器名或 --all")
        raise SystemExit(1)

    # Build Server models for parallel SSH
    srv_models = [
        Server(key=k, host=c.host, user=c.user, path=c.path)
        for k, c in targets.items() if not c.local
    ]

    if srv_models:
        results = ssh_parallel(srv_models, "echo OK && uptime", timeout=10)
        for k, c in targets.items():
            if c.local:
                click.echo(f"  ✓ {k} ({c.host}): 本机")
            elif k in results:
                out, ok = results[k]
                if ok:
                    lines = out.splitlines()
                    click.echo(f"  ✓ {k} ({c.host}): {lines[-1].strip() if lines else 'OK'}")
                else:
                    click.echo(f"  ✗ {k} ({c.host}): {out}")
    else:
        for k, c in targets.items():
            click.echo(f"  ✓ {k} ({c.host}): 本机")


@server_cmd.command("setup")
@click.argument("key")
def server_setup(key):
    """在服务器上部署脚本并启动 worker。"""
    servers = load_servers()
    if key not in servers:
        click.echo(f"✗ 服务器 {key} 不存在。")
        raise SystemExit(1)

    cfg = servers[key]
    srv = Server(key=key, host=cfg.host, user=cfg.user, path=cfg.path)
    click.echo(f"设置 {key} ({cfg.host})...")

    setup_cmds = " && ".join([
        f"mkdir -p {cfg.path}",
        f"chmod +x {cfg.path}/*.sh 2>/dev/null; true",
        f"touch {cfg.path}/queue.txt",
        "tmux kill-session -t worker 2>/dev/null; true",
        f"tmux new-session -d -s worker 'bash {cfg.path}/queue-worker.sh'",
    ])

    out, ok = ssh_server(srv, setup_cmds, timeout=15)
    if ok:
        click.echo(f"✓ Worker 已启动")
    else:
        click.echo(f"✗ 设置失败: {out}")


@server_cmd.command("setup-key")
@click.argument("key")
def server_setup_key(key):
    """为指定服务器配置 SSH 免密登录。"""
    servers = load_servers()
    if key not in servers:
        click.echo(f"✗ 服务器 {key} 不存在。")
        raise SystemExit(1)

    cfg = servers[key]
    click.echo(f"配置 SSH key → {cfg.user}@{cfg.host}...")

    from ..core.ssh import ssh_copy_id
    out, ok = ssh_copy_id(cfg.host, cfg.user)
    if ok:
        click.echo(f"✓ SSH key 已配置")
        # Verify
        out, ok = ssh_exec(cfg.host, cfg.user, "echo OK", timeout=10)
        if ok:
            click.echo(f"✓ 免密验证通过")
        else:
            click.echo(f"⚠ key 已配置但验证失败: {out}")
    else:
        click.echo(f"✗ 失败: {out}")
        click.echo(f"  手动执行: ssh-copy-id {cfg.user}@{cfg.host}")
