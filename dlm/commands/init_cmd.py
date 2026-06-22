"""dlm init — first-time setup and SSH key distribution."""

import os
import subprocess
from pathlib import Path

import click

from ..core.servers import (
    load_servers, save_servers, add_server, is_initialized,
    ServerConfig, DLM_DIR, SERVERS_FILE,
)
from ..core.ssh import ssh_exec


@click.command("init")
@click.option("--host", multiple=True, help="Server host (repeatable: --host 1.2.3.4 --host 5.6.7.8)")
@click.option("--user", default="root", help="SSH user for all servers (default: root)")
@click.option("--key", "setup_key", is_flag=True, help="Distribute SSH key to servers")
@click.option("--force", is_flag=True, help="Overwrite existing configuration")
def init_cmd(host, user, setup_key, force):
    """初始化 DLM 配置。

    \b
    非交互模式（无参数）：
      dlm init                    → 创建空配置，之后用 dlm server add 逐个添加

    \b
    批量模式：
      dlm init --host 1.2.3.4 --host 5.6.7.8   → 直接注册多台服务器

    \b
    含 SSH key 分发：
      dlm init --host 1.2.3.4 --key             → 注册 + 自动配置免密
    """
    # Check existing config
    if is_initialized() and not force:
        click.echo(f"已初始化: {SERVERS_FILE}")
        servers = load_servers()
        if servers:
            click.echo(f"  已注册 {len(servers)} 台服务器: {', '.join(servers.keys())}")
        click.echo(f"\n  添加服务器: dlm server add <key> --host <ip>")
        click.echo(f"  重新初始化: dlm init --force")
        return

    # Ensure ~/.dlm/ exists
    DLM_DIR.mkdir(parents=True, exist_ok=True)

    # Check .env / credentials
    _check_env()

    # Create servers.yaml
    if host:
        # Non-interactive: register provided hosts
        servers = {}
        for i, h in enumerate(host, 1):
            key = f"S{i}"
            servers[key] = ServerConfig(key=key, host=h, user=user)
            click.echo(f"  注册 {key}: {user}@{h}")
        save_servers(servers)
    else:
        # Minimal init: empty config
        save_servers({})
        click.echo(f"✓ 配置目录已创建: {DLM_DIR}")
        click.echo(f"  配置文件: {SERVERS_FILE}")
        click.echo(f"\n  下一步: dlm server add S1 --host <ip>")
        return

    # SSH key distribution
    if setup_key:
        _distribute_keys(load_servers())

    # Verify connectivity
    click.echo(f"\n验证连接...")
    servers = load_servers()
    all_ok = True
    for key, srv in servers.items():
        out, ok = ssh_exec(srv.host, srv.user, "echo OK", timeout=10)
        if ok:
            click.echo(f"  ✓ {key} ({srv.host})")
        else:
            click.echo(f"  ✗ {key} ({srv.host}): {out}")
            all_ok = False

    if all_ok:
        click.echo(f"\n✓ 初始化完成! 试试: dlm server list")
    else:
        click.echo(f"\n⚠ 部分服务器连接失败。请检查 SSH key 或使用:")
        click.echo(f"  dlm server setup-key <key>")


def _check_env():
    """Verify that .env credentials are accessible."""
    try:
        from ..core.config import load_config
        config = load_config()
        if config.get("BAIDU_AK"):
            click.echo(f"✓ BOS 凭证已配置")
        else:
            click.echo(f"⚠ BAIDU_AK 未设置 — dlm ls/status 需要 BOS 访问")
    except Exception as e:
        click.echo(f"⚠ 配置加载失败: {e}")
        click.echo(f"  请确保 .env 文件包含 BAIDU_AK 和 BAIDU_SK")


def _distribute_keys(servers: dict[str, ServerConfig]):
    """Distribute SSH public key to all registered servers."""
    pubkey_path = _find_pubkey()
    if not pubkey_path:
        click.echo("⚠ 未找到 SSH 公钥。生成中...")
        _generate_key()
        pubkey_path = _find_pubkey()
        if not pubkey_path:
            click.echo("✗ 无法生成 SSH key")
            return

    click.echo(f"\n分发 SSH key: {pubkey_path}")
    with open(pubkey_path, "r") as f:
        pubkey = f.read().strip()

    for key, srv in servers.items():
        if srv.local:
            click.echo(f"  跳过 {key} (本机)")
            continue

        click.echo(f"  → {key} ({srv.host})...", nl=False)
        cmd = (
            f"mkdir -p ~/.ssh && chmod 700 ~/.ssh && "
            f"grep -qF '{pubkey[:40]}' ~/.ssh/authorized_keys 2>/dev/null || "
            f"echo '{pubkey}' >> ~/.ssh/authorized_keys && "
            f"chmod 600 ~/.ssh/authorized_keys && echo OK"
        )
        out, ok = ssh_exec(srv.host, srv.user, cmd, timeout=15)
        if ok and "OK" in out:
            click.echo(" ✓")
        else:
            click.echo(f" ✗ ({out})")
            click.echo(f"    手动执行: ssh-copy-id {srv.user}@{srv.host}")


def _find_pubkey() -> str:
    """Find existing SSH public key."""
    candidates = [
        Path.home() / ".ssh" / "id_ed25519.pub",
        Path.home() / ".ssh" / "id_rsa.pub",
    ]
    for p in candidates:
        if p.exists():
            return str(p)
    return ""


def _generate_key():
    """Generate an SSH key pair."""
    key_path = Path.home() / ".ssh" / "id_ed25519"
    if key_path.exists():
        return
    try:
        result = subprocess.run(
            ["ssh-keygen", "-t", "ed25519", "-f", str(key_path), "-N", ""],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode != 0:
            click.echo(f"  ssh-keygen 失败: {result.stderr.strip()}")
    except FileNotFoundError:
        click.echo("  ssh-keygen 未找到，请手动生成 SSH 密钥")
    except Exception as e:
        click.echo(f"  SSH 密钥生成失败: {e}")
