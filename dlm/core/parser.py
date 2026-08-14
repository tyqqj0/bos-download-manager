"""URL/repo_id parser and source detection."""

import re
from urllib.parse import urlparse
from typing import Optional


# Path segments that sit where an org name would but never identify a
# downloadable repo. Pasting one of these produced a real task row every time:
# `https://huggingface.co/collections/X-Humanoid/...` became repo_id
# `collections/X-Humanoid` and failed at listing, and a bare `#dataset`
# became a task literally named `#dataset`. Both are still in the live DB.
# Matched against the first segment AFTER any datasets/models prefix is
# stripped, so `datasets/collections/x` is caught too.
NON_REPO_SEGMENTS = {
    "collections", "spaces", "space", "blog", "docs", "doc", "papers",
    "paper", "settings", "organizations", "organization", "login", "join",
    "pricing", "search", "new", "notifications", "studios", "studio",
    "learn", "posts", "changelog", "tasks", "models-json",
}

# Segments that are structurally the last component of a repo path but carry
# no identity. `RoboCOIN/datasets` is the live example: the task was named
# `datasets` and would have uploaded to `manipulation/datasets/` — a prefix
# that says nothing about what is in it and collides with the next such paste.
GENERIC_NAME_SEGMENTS = {
    "datasets", "dataset", "data", "models", "model", "repo", "repos",
    "main", "tree", "resolve", "blob", "files", "raw",
}


def repo_display_name(repo_id: str) -> str:
    """The task name a repo_id implies.

    Normally the last path component. When that component is generic
    (`org/datasets`), it is prefixed with the owner instead of used alone —
    the name reaches the BOS prefix, so `datasets/` would be both
    uninformative and a collision magnet.
    """
    parts = [p for p in repo_id.split("/") if p]
    if not parts:
        return ""
    last = parts[-1]
    if last.lower() in GENERIC_NAME_SEGMENTS and len(parts) >= 2:
        return f"{parts[-2]}-{last}"
    return last


def parse_repo(url_or_repo: str) -> dict:
    """
    Parse a URL or bare repo_id into structured download info.

    Returns:
        {
            "source": "hf" | "modelscope" | "unknown",
            "repo_id": "org/name",
            "type": "dataset" | "model",
            "name": "name",
            "source_guessed": bool,   # True when nothing in the input said hf
            "error": str | None,      # non-None ⇒ not a downloadable repo
        }

    `error` is advisory to this function's callers and load-bearing for the
    two add routes: they reject rather than store. Returning it beats raising
    because both callers already answer with a structured HTTP error, and the
    parsed fields remain useful for the message.
    """
    url_or_repo = url_or_repo.strip()

    # HuggingFace URLs
    if _is_hf_url(url_or_repo):
        return _parse_hf_url(url_or_repo)

    # ModelScope URLs
    if _is_modelscope_url(url_or_repo):
        return _parse_modelscope_url(url_or_repo)

    # Some other host entirely. Never a repo we can download; saying so here
    # keeps `https://example.com/x/y` from parsing as the repo `x/y`.
    if "://" in url_or_repo:
        return _invalid(
            url_or_repo,
            f"unsupported host in {url_or_repo!r} — expected huggingface.co "
            "or modelscope.cn",
        )

    # Bare repo_id (org/name format). Nothing in the input says which hub it
    # lives on; hf is the guess, flagged as one so the add path can verify it
    # against HF and point at source=modelscope when the repo is not there.
    # (Historically this guess was silent, and a bare ModelScope id was filed
    # as hf — hf tasks only dispatch to the HK fleet, which cannot reach it.)
    if "/" in url_or_repo:
        parts = [p for p in url_or_repo.split("/") if p]
        if len(parts) < 2:
            return _invalid(url_or_repo, f"{url_or_repo!r} is not an org/name repo id")
        if parts[0].lower() in NON_REPO_SEGMENTS:
            return _invalid(
                url_or_repo,
                f"{parts[0]!r} is not an organisation — {url_or_repo!r} looks "
                "like a site path, not a repo",
            )
        repo_id = "/".join(parts[:2])
        return {
            "source": "hf",
            "repo_id": repo_id,
            "type": "dataset",
            "name": repo_display_name(repo_id),
            "source_guessed": True,
            "error": None,
        }

    return _invalid(
        url_or_repo,
        f"{url_or_repo!r} is not a repo id or URL — expected org/name or a "
        "huggingface.co/modelscope.cn link",
    )


def _invalid(raw: str, why: str) -> dict:
    """An input that cannot name a downloadable repo. `name`/`repo_id` still
    carry the raw text so an error message can quote what was actually sent."""
    return {
        "source": "unknown",
        "repo_id": raw,
        "type": "dataset",
        "name": raw,
        "source_guessed": False,
        "error": why,
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


def derive_bos_path(category: str, repo_id: str, dtype: str = "dataset") -> str:
    """Derive the BOS target path from category and repo_id.

    Uses repo_id's last component (after /) to match download.sh's DIR_NAME logic.
    """
    dir_name = repo_id.split("/")[-1] if "/" in repo_id else repo_id
    if dtype == "model":
        return f"{dir_name}/"
    return f"{category}/{dir_name}/"


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


def _parse_hub_url(url: str, source: str, default_type: str) -> dict:
    """Shared shape of both hubs' URLs: an optional `datasets/`|`models/`
    prefix, then `org/name`, then any amount of in-repo path we discard.

    The two hubs differ only in which type a prefix-less URL implies —
    huggingface.co/org/name is a model, modelscope.cn/org/name is a dataset —
    so that is the one parameter rather than two near-identical functions.
    """
    parsed = urlparse(url)
    path_parts = [p for p in parsed.path.strip("/").split("/") if p]

    dtype = default_type
    repo_parts = path_parts

    if path_parts and path_parts[0] == "datasets":
        dtype = "dataset"
        repo_parts = path_parts[1:]
    elif path_parts and path_parts[0] == "models":
        dtype = "model"
        repo_parts = path_parts[1:]

    if repo_parts and repo_parts[0].lower() in NON_REPO_SEGMENTS:
        return _invalid(
            url,
            f"{url!r} is a {repo_parts[0]} page, not a repo — open the repo "
            "itself and paste that URL",
        )

    if len(repo_parts) < 2 or not repo_parts[1]:
        return _invalid(
            url, f"{url!r} does not contain an org/name repo path"
        )

    repo_id = f"{repo_parts[0]}/{repo_parts[1]}"
    return {
        "source": source,
        "repo_id": repo_id,
        "type": dtype,
        "name": repo_display_name(repo_id),
        "source_guessed": False,
        "error": None,
    }


def _parse_hf_url(url: str) -> dict:
    return _parse_hub_url(url, "hf", "model")


def _parse_modelscope_url(url: str) -> dict:
    return _parse_hub_url(url, "modelscope", "dataset")
