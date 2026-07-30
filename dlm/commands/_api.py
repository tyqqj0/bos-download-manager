"""HTTP API client for CLI commands — talks to the S1 web dashboard."""

import os
import requests

DEFAULT_URL = "http://154.85.43.52:8080"


def api_url() -> str:
    return os.environ.get("DLM_API_URL", DEFAULT_URL)


def get(path: str, **params) -> dict:
    resp = requests.get(f"{api_url()}{path}", params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def post(path: str, json: dict = None) -> dict:
    resp = requests.post(f"{api_url()}{path}", json=json or {}, timeout=30)
    resp.raise_for_status()
    return resp.json()
