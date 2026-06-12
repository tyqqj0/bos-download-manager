"""SSH execution helper — parallel execution + ControlMaster support."""

import subprocess
import os
import platform
from typing import Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from .models import Server


def _ctrl_path(host: str, user: str) -> str:
    """SSH ControlMaster socket path."""
    if platform.system() == "Windows":
        return ""
    return f"/tmp/dlm-ssh-{user}@{host}"


def _ssh_args(host: str, user: str) -> list[str]:
    """Build base SSH command args with ControlMaster."""
    args = [
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "ConnectTimeout=10",
        "-o", "BatchMode=yes",
    ]
    ctrl = _ctrl_path(host, user)
    if ctrl:
        args += [
            "-o", f"ControlPath={ctrl}",
            "-o", "ControlMaster=auto",
            "-o", "ControlPersist=60",
        ]
    args.append(f"{user}@{host}")
    return args


def ssh_exec(host: str, user: str, command: str, timeout: int = 30) -> Tuple[str, bool]:
    """Execute a command over SSH. Returns (stdout, success)."""
    ssh_cmd = _ssh_args(host, user) + [command]
    try:
        result = subprocess.run(
            ssh_cmd, capture_output=True, text=True, timeout=timeout
        )
        return result.stdout.strip(), result.returncode == 0
    except subprocess.TimeoutExpired:
        return "[TIMEOUT]", False
    except Exception as e:
        return str(e), False


def ssh_server(server: Server, command: str, timeout: int = 30) -> Tuple[str, bool]:
    """Execute a command on a registered server."""
    return ssh_exec(server.host, server.user, command, timeout)


def ssh_parallel(servers: list[Server], command: str, timeout: int = 15,
                 max_workers: int = 10) -> dict[str, Tuple[str, bool]]:
    """Execute the same command on multiple servers in parallel.
    Returns {server_key: (stdout, success)}.
    """
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(ssh_server, srv, command, timeout): srv.key
            for srv in servers
        }
        for future in as_completed(futures):
            key = futures[future]
            try:
                results[key] = future.result()
            except Exception as e:
                results[key] = (str(e), False)
    return results


def ssh_parallel_multi(servers: list[Server], commands: dict[str, str],
                       timeout: int = 15, max_workers: int = 10) -> dict[str, Tuple[str, bool]]:
    """Execute different commands on different servers in parallel.
    commands: {server_key: command_string}
    Returns {server_key: (stdout, success)}.
    """
    key_to_server = {s.key: s for s in servers}
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {}
        for key, cmd in commands.items():
            if key in key_to_server:
                futures[pool.submit(ssh_server, key_to_server[key], cmd, timeout)] = key
        for future in as_completed(futures):
            key = futures[future]
            try:
                results[key] = future.result()
            except Exception as e:
                results[key] = (str(e), False)
    return results


def ssh_append_queue(server: Server, cmd_line: str) -> bool:
    """Append a command to the server's queue.txt (idempotent: checks for duplicates)."""
    queue_path = f"{server.path}/queue.txt"
    repo_marker = _extract_repo_from_cmd(cmd_line)

    if repo_marker:
        check = f"grep -qF '{repo_marker}' {queue_path} 2>/dev/null"
        _, found = ssh_server(server, check)
        if found:
            return False  # already in queue

    escaped = cmd_line.replace("'", "'\\''")
    append_cmd = f"echo '{escaped}' >> {queue_path}"
    _, ok = ssh_server(server, append_cmd)
    return ok


def ssh_check_current(server: Server) -> str:
    """Read the currently running task from current.txt."""
    out, _ = ssh_server(server, f"cat {server.path}/current.txt 2>/dev/null")
    return out.strip()


def ssh_queue_depth(server: Server) -> int:
    """Count lines in queue.txt."""
    out, ok = ssh_server(server, f"wc -l < {server.path}/queue.txt 2>/dev/null")
    if ok and out.strip().isdigit():
        return int(out.strip())
    return 0


def ssh_recent_log(server: Server, lines: int = 30) -> str:
    """Read the last N lines of queue.log."""
    out, _ = ssh_server(server, f"tail -n {lines} {server.path}/queue.log 2>/dev/null")
    return out


def ssh_worker_alive(server: Server) -> bool:
    """Check if the tmux worker session is running."""
    _, ok = ssh_server(server, "tmux has-session -t worker 2>/dev/null")
    return ok


def ssh_disk_usage(server: Server, path: str, timeout: int = 60) -> str:
    """Get du -sh of a path on the server."""
    out, ok = ssh_server(server, f"du -sh {path} 2>/dev/null", timeout=timeout)
    return out.split()[0] if ok and out else "?"


def ssh_check_queue_contains(server: Server, repo_id: str) -> bool:
    """Check if repo_id appears in queue.txt or current.txt."""
    cmd = f"grep -lF '{repo_id}' {server.path}/queue.txt {server.path}/current.txt 2>/dev/null"
    out, _ = ssh_server(server, cmd)
    return bool(out.strip())


def ssh_copy_id(host: str, user: str, pubkey_path: str = None) -> Tuple[str, bool]:
    """Copy SSH public key to a remote server (requires password input)."""
    if not pubkey_path:
        pubkey_path = os.path.expanduser("~/.ssh/id_rsa.pub")
        if not os.path.exists(pubkey_path):
            pubkey_path = os.path.expanduser("~/.ssh/id_ed25519.pub")

    if not os.path.exists(pubkey_path):
        return "No public key found. Run: ssh-keygen", False

    with open(pubkey_path, "r") as f:
        pubkey = f.read().strip()

    cmd = (
        f"mkdir -p ~/.ssh && chmod 700 ~/.ssh && "
        f"echo '{pubkey}' >> ~/.ssh/authorized_keys && "
        f"chmod 600 ~/.ssh/authorized_keys && echo OK"
    )
    # This needs password auth, so we disable BatchMode
    ssh_cmd = [
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "ConnectTimeout=10",
        f"{user}@{host}",
        cmd,
    ]
    try:
        result = subprocess.run(ssh_cmd, capture_output=True, text=True, timeout=30)
        return result.stdout.strip(), "OK" in result.stdout
    except Exception as e:
        return str(e), False


def _extract_repo_from_cmd(cmd_line: str) -> str:
    """Extract repo_id from a download command for dedup checking."""
    parts = cmd_line.split()
    for i, p in enumerate(parts):
        if p.endswith("download.sh") or p.endswith("download-modelscope.sh"):
            if i + 1 < len(parts):
                return parts[i + 1]
    return ""
