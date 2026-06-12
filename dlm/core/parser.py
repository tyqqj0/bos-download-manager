"""URL/repo_id parser and source detection."""

import re
from urllib.parse import urlparse
from typing import Optional


def parse_repo(url_or_repo: str) -> dict:
    """
    Parse a URL or bare repo_id into structured download info.

    Returns:
        {
            "source": "hf" | "modelscope" | "unknown",
            "repo_id": "org/name",
            "type": "dataset" | "model",
            "name": "name"
        }
    """
    url_or_repo = url_or_repo.strip()

    # HuggingFace URLs
    if _is_hf_url(url_or_repo):
        return _parse_hf_url(url_or_repo)

    # ModelScope URLs
    if _is_modelscope_url(url_or_repo):
        return _parse_modelscope_url(url_or_repo)

    # Bare repo_id (org/name format) — default to HuggingFace dataset
    if "/" in url_or_repo and "://" not in url_or_repo:
        name = url_or_repo.split("/")[-1]
        return {
            "source": "hf",
            "repo_id": url_or_repo,
            "type": "dataset",
            "name": name,
        }

    return {
        "source": "unknown",
        "repo_id": url_or_repo,
        "type": "dataset",
        "name": url_or_repo,
    }


def build_download_cmd(
    repo_id: str,
    source: str,
    dtype: str,
    category: str,
    remote_path: str = "~/code/auwomo-tools",
    include: Optional[str] = None,
    custom_name: Optional[str] = None,
) -> str:
    """Build the shell command for remote execution."""
    if source == "modelscope":
        script = f"{remote_path}/download-modelscope.sh"
    else:
        script = f"{remote_path}/download.sh"

    cmd = f"{script} {repo_id} -t {dtype} -c {category}"
    if custom_name:
        cmd += f" -n {custom_name}"
    if include:
        cmd += f" --include '{include}'"
    return cmd


def derive_bos_path(category: str, name: str, dtype: str = "dataset") -> str:
    """Derive the BOS target path from category and name."""
    if dtype == "model":
        return f"{name}/"
    return f"{category}/{name}/"


def _is_hf_url(s: str) -> bool:
    return any(s.startswith(p) for p in [
        "https://huggingface.co/",
        "http://huggingface.co/",
        "https://hf.co/",
    ])


def _is_modelscope_url(s: str) -> bool:
    return any(s.startswith(p) for p in [
        "https://modelscope.cn/",
        "http://modelscope.cn/",
        "https://www.modelscope.cn/",
    ])


def _parse_hf_url(url: str) -> dict:
    parsed = urlparse(url)
    path_parts = [p for p in parsed.path.strip("/").split("/") if p]

    dtype = "model"
    repo_parts = path_parts

    if path_parts and path_parts[0] == "datasets":
        dtype = "dataset"
        repo_parts = path_parts[1:]
    elif path_parts and path_parts[0] == "models":
        dtype = "model"
        repo_parts = path_parts[1:]

    # Take first two segments as org/name
    if len(repo_parts) >= 2:
        repo_id = f"{repo_parts[0]}/{repo_parts[1]}"
        name = repo_parts[1]
    elif len(repo_parts) == 1:
        repo_id = repo_parts[0]
        name = repo_parts[0]
    else:
        repo_id = ""
        name = ""

    return {"source": "hf", "repo_id": repo_id, "type": dtype, "name": name}


def _parse_modelscope_url(url: str) -> dict:
    parsed = urlparse(url)
    path_parts = [p for p in parsed.path.strip("/").split("/") if p]

    dtype = "dataset"
    repo_parts = path_parts

    if path_parts and path_parts[0] == "datasets":
        dtype = "dataset"
        repo_parts = path_parts[1:]
    elif path_parts and path_parts[0] == "models":
        dtype = "model"
        repo_parts = path_parts[1:]

    if len(repo_parts) >= 2:
        repo_id = f"{repo_parts[0]}/{repo_parts[1]}"
        name = repo_parts[1]
    elif len(repo_parts) == 1:
        repo_id = repo_parts[0]
        name = repo_parts[0]
    else:
        repo_id = ""
        name = ""

    return {"source": "modelscope", "repo_id": repo_id, "type": dtype, "name": name}
