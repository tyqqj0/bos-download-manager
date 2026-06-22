"""State manager: read/write state.json from BOS."""

import io
import json
import os
import platform
import time
from pathlib import Path
from datetime import datetime, timezone

from ..constants import META_BUCKET, STATE_KEY, CATEGORIES
from .models import State, Server, _now

_CACHE_DIR = Path.home() / ".dlm"
_CACHE_FILE = _CACHE_DIR / "cache.json"
_CACHE_TTL = 60  # seconds


class OptimisticLockError(Exception):
    """Raised when state.json was modified by another writer."""
    pass


class StateManager:
    def __init__(self, bos_client=None):
        self._bos = bos_client
        self._cache_dir = _CACHE_DIR
        self._cache_file = _CACHE_FILE

    @classmethod
    def create(cls) -> "StateManager":
        from .config import load_config
        from .bos import create_bos_client

        config = load_config()
        bos = create_bos_client(config["BAIDU_AK"], config["BAIDU_SK"], config["BOS_ENDPOINT"])
        return cls(bos_client=bos)

    def load(self, use_cache=True) -> State:
        if use_cache:
            cached = self._read_cache()
            if cached is not None:
                return cached

        try:
            response = self._bos.get_object(META_BUCKET, STATE_KEY)
            data = json.loads(response.data.read())
            state = State.from_dict(data)
        except Exception as e:
            err_msg = str(e)
            if "NoSuchKey" in err_msg or "404" in err_msg or "does not exist" in err_msg:
                state = self._initial_state()
            else:
                raise RuntimeError(f"Failed to read state from BOS: {e}") from e

        self._write_cache(state)
        return state

    def save(self, state: State):
        state.meta["updated_at"] = _now()
        state.meta["updated_by"] = f"{os.getenv('USER', os.getenv('USERNAME', 'unknown'))}@{platform.node()}"
        state.meta["version"] = state.meta.get("version", 0) + 1

        data = json.dumps(state.to_dict(), ensure_ascii=False, indent=2).encode("utf-8")
        self._bos.put_object(META_BUCKET, STATE_KEY, io.BytesIO(data),
                             content_length=len(data),
                             content_type="application/json")
        self._write_cache(state)

    def save_with_lock(self, state: State, expected_version: int):
        """Save state with optimistic locking. Raises OptimisticLockError if
        the current version in BOS doesn't match expected_version."""
        current = self.load(use_cache=False)
        actual_version = current.meta.get("version", 0)
        if actual_version != expected_version:
            raise OptimisticLockError(
                f"Version conflict: expected {expected_version}, got {actual_version}"
            )
        self.save(state)

    def update_task(self, task_id: str, updates: dict, max_retries: int = 5) -> State:
        """Atomically update a single task's fields with retry on conflict.
        Returns the saved state."""
        for attempt in range(max_retries):
            state = self.load(use_cache=False)
            version = state.meta.get("version", 0)
            task = state.find_task_by_id(task_id)
            if task is None:
                raise ValueError(f"Task {task_id} not found")
            for k, v in updates.items():
                if hasattr(task, k):
                    setattr(task, k, v)
            try:
                self.save_with_lock(state, expected_version=version)
                return state
            except OptimisticLockError:
                if attempt == max_retries - 1:
                    raise
                time.sleep(0.2 * (2 ** attempt))
        return state

    def update_heartbeat(self, server_key: str, heartbeat: dict, max_retries: int = 3) -> State:
        """Atomically update a worker's heartbeat."""
        for attempt in range(max_retries):
            state = self.load(use_cache=False)
            version = state.meta.get("version", 0)
            state.worker_heartbeats[server_key] = heartbeat
            try:
                self.save_with_lock(state, expected_version=version)
                return state
            except OptimisticLockError:
                if attempt == max_retries - 1:
                    raise
                time.sleep(0.2 * (2 ** attempt))
        return state

    def _initial_state(self) -> State:
        from ..constants import CATEGORIES
        from .servers import load_servers
        state = State()
        state.categories = list(CATEGORIES)
        server_cfgs = load_servers()
        for key, cfg in server_cfgs.items():
            state.servers[key] = Server(
                key=key, host=cfg.host, user=cfg.user,
                path=cfg.path, enabled=cfg.enabled,
            )
        return state

    def _read_cache(self):
        try:
            if not self._cache_file.exists():
                return None
            mtime = self._cache_file.stat().st_mtime
            if time.time() - mtime > _CACHE_TTL:
                return None
            data = json.loads(self._cache_file.read_text(encoding="utf-8"))
            return State.from_dict(data)
        except Exception:
            return None

    def _write_cache(self, state: State):
        try:
            self._cache_dir.mkdir(parents=True, exist_ok=True)
            self._cache_file.write_text(
                json.dumps(state.to_dict(), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:
            pass

    def invalidate_cache(self):
        try:
            self._cache_file.unlink(missing_ok=True)
        except Exception:
            pass
