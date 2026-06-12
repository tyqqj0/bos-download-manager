"""State manager: read/write state.json from BOS."""

import json
import os
import platform
import time
from pathlib import Path
from datetime import datetime, timezone

from ..constants import META_BUCKET, STATE_KEY, CATEGORIES
from .models import State, Server

_CACHE_DIR = Path.home() / ".dlm"
_CACHE_FILE = _CACHE_DIR / "cache.json"
_CACHE_TTL = 60  # seconds


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
        state.meta["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        state.meta["updated_by"] = f"{os.getenv('USER', os.getenv('USERNAME', 'unknown'))}@{platform.node()}"
        state.meta["version"] = state.meta.get("version", 0) + 1

        data = json.dumps(state.to_dict(), ensure_ascii=False, indent=2).encode("utf-8")
        self._bos.put_object(META_BUCKET, STATE_KEY, data,
                             content_length=len(data),
                             content_type="application/json")
        self._write_cache(state)

    def _initial_state(self) -> State:
        from ..constants import CATEGORIES
        from .servers import load_servers
        state = State()
        state.categories = list(CATEGORIES)
        # Load servers from ~/.dlm/servers.yaml (not hardcoded)
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
