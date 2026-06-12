from dataclasses import dataclass, field, asdict
from typing import Optional
import json
from datetime import datetime, timezone


@dataclass
class Server:
    key: str
    host: str
    user: str = "root"
    path: str = "~/code/auwomo-tools"
    enabled: bool = True
    added_at: str = ""
    notes: str = ""

    def __post_init__(self):
        if not self.added_at:
            self.added_at = _now()


@dataclass
class Task:
    id: str
    name: str
    repo_id: str
    source: str
    type: str
    category: str
    bos_path: str
    size_gb: float = 0.0
    status: str = "queued"
    server: Optional[str] = None
    priority: str = "P1"
    include: Optional[str] = None
    created_at: str = ""
    dispatched_at: Optional[str] = None
    completed_at: Optional[str] = None
    retry_count: int = 0
    error: Optional[str] = None
    notes: str = ""

    def __post_init__(self):
        if not self.created_at:
            self.created_at = _now()


@dataclass
class State:
    meta: dict = field(default_factory=lambda: {
        "version": 1,
        "updated_at": "",
        "updated_by": "",
    })
    servers: dict = field(default_factory=dict)
    tasks: list = field(default_factory=list)
    categories: list = field(default_factory=list)

    def find_task(self, repo_id: str) -> Optional[Task]:
        for t in self.tasks:
            if t.repo_id == repo_id:
                return t
        return None

    def find_task_by_id(self, task_id: str) -> Optional[Task]:
        for t in self.tasks:
            if t.id == task_id:
                return t
        return None

    def find_task_by_name(self, name: str) -> Optional[Task]:
        for t in self.tasks:
            if t.name == name:
                return t
        return None

    def next_task_id(self) -> str:
        today = datetime.now(timezone.utc).strftime("%Y%m%d")
        prefix = f"t-{today}-"
        existing = [t.id for t in self.tasks if t.id.startswith(prefix)]
        n = len(existing) + 1
        return f"{prefix}{n:03d}"

    def active_tasks_for_server(self, server_key: str) -> list:
        return [
            t for t in self.tasks
            if t.server == server_key and t.status in ("dispatched", "downloading")
        ]

    def to_dict(self) -> dict:
        return {
            "meta": self.meta,
            "servers": {k: asdict(v) if isinstance(v, Server) else v for k, v in self.servers.items()},
            "tasks": [asdict(t) if isinstance(t, Task) else t for t in self.tasks],
            "categories": self.categories,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "State":
        state = cls()
        state.meta = data.get("meta", state.meta)
        state.categories = data.get("categories", [])
        for k, v in data.get("servers", {}).items():
            if isinstance(v, Server):
                state.servers[k] = v
            else:
                v.pop("key", None)
                state.servers[k] = Server(key=k, **v)
        for t in data.get("tasks", []):
            if isinstance(t, Task):
                state.tasks.append(t)
            else:
                state.tasks.append(Task(**t))
        return state


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
