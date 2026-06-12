"""Server configuration management — reads/writes ~/.dlm/servers.yaml"""

import os
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Optional

import yaml


DLM_DIR = Path.home() / ".dlm"
SERVERS_FILE = DLM_DIR / "servers.yaml"


@dataclass
class ServerConfig:
    key: str
    host: str
    user: str = "root"
    path: str = "~/code/auwomo-tools"
    enabled: bool = True
    local: bool = False
    notes: str = ""


def _ensure_dir():
    DLM_DIR.mkdir(parents=True, exist_ok=True)


def load_servers() -> dict[str, ServerConfig]:
    """Load servers from ~/.dlm/servers.yaml. Returns empty dict if not exists."""
    if not SERVERS_FILE.exists():
        return {}

    with open(SERVERS_FILE, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    servers = {}
    for key, cfg in data.get("servers", {}).items():
        if isinstance(cfg, dict):
            servers[key] = ServerConfig(
                key=key,
                host=cfg.get("host", ""),
                user=cfg.get("user", "root"),
                path=cfg.get("path", "~/code/auwomo-tools"),
                enabled=cfg.get("enabled", True),
                local=cfg.get("local", False),
                notes=cfg.get("notes", ""),
            )
    return servers


def save_servers(servers: dict[str, ServerConfig]):
    """Write servers to ~/.dlm/servers.yaml."""
    _ensure_dir()
    data = {"servers": {}}
    for key, srv in servers.items():
        entry = {"host": srv.host, "user": srv.user}
        if srv.path != "~/code/auwomo-tools":
            entry["path"] = srv.path
        if not srv.enabled:
            entry["enabled"] = False
        if srv.local:
            entry["local"] = True
        if srv.notes:
            entry["notes"] = srv.notes
        data["servers"][key] = entry

    with open(SERVERS_FILE, "w", encoding="utf-8") as f:
        yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)


def add_server(key: str, host: str, user: str = "root", path: str = "~/code/auwomo-tools",
               local: bool = False, notes: str = "") -> ServerConfig:
    """Add a server to servers.yaml. Raises if key already exists."""
    servers = load_servers()
    if key in servers:
        raise ValueError(f"Server '{key}' already exists. Use 'dlm server remove {key}' first.")
    srv = ServerConfig(key=key, host=host, user=user, path=path, local=local, notes=notes)
    servers[key] = srv
    save_servers(servers)
    return srv


def remove_server(key: str) -> bool:
    """Remove a server from servers.yaml. Returns False if not found."""
    servers = load_servers()
    if key not in servers:
        return False
    del servers[key]
    save_servers(servers)
    return True


def is_initialized() -> bool:
    """Check if DLM has been initialized (servers.yaml exists)."""
    return SERVERS_FILE.exists()
