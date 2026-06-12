"""DLM configuration — loads credentials from .env file."""

import os
from pathlib import Path
from dotenv import load_dotenv


def _find_env_file() -> Path:
    """Search for .env file. Priority: ~/.dlm/.env > upward from CWD."""
    # Primary: ~/.dlm/.env (works from any directory)
    dlm_env = Path.home() / ".dlm" / ".env"
    if dlm_env.exists():
        return dlm_env
    # Secondary: search upward from CWD
    current = Path.cwd()
    for _ in range(6):
        if (current / ".env").exists():
            return current / ".env"
        current = current.parent
    return dlm_env  # default path for error message


def load_config() -> dict:
    """Load config from .env file."""
    env_path = _find_env_file()
    load_dotenv(env_path)

    config = {
        "BAIDU_AK": os.getenv("BAIDU_AK", ""),
        "BAIDU_SK": os.getenv("BAIDU_SK", ""),
        "BOS_ENDPOINT": os.getenv("BOS_ENDPOINT", "https://bj.bcebos.com"),
        "HF_TOKEN": os.getenv("HF_TOKEN", ""),
    }

    if not config["BAIDU_AK"] or not config["BAIDU_SK"]:
        raise RuntimeError(
            f"BAIDU_AK/BAIDU_SK not set. Create .env file or set environment variables.\n"
            f"  Searched: {env_path}"
        )

    return config
