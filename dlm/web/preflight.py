"""Add-time reachability probe for HuggingFace repos.

Why this exists: nothing checked whether we are *allowed* to download a repo
before accepting it. A gated repo was chunked into hundreds of pool batches,
each of which discovered its own 403 and burned its 3 Temporal attempts — and a
pool batch that exhausts its attempts is permanently `failed` and never
re-dispatched. Measured cost: assembly101 lost 20 of 113 batches permanently
inside a ~6-hour approval window (2026-08-11), and Franka-Dataset had begun the
same slide (1 of 696 in 8 hours) before it was deleted.

Two traps this deliberately avoids, both measured 2026-08-12:

  * `GET /api/datasets/{id}` reports `gated: 'auto'` *whether or not the caller
    is authorised*. It answers "is this repo gated", which is not the question.
    The only discriminator is the resolve endpoint with the token attached:
    authed 200/3xx = we can download, authed 403 = authenticated but not
    authorised (a human must click Agree on the repo page), 404 = not here.

  * `README.md` must never be the probe target. Gated repos serve it
    anonymously with 200, so it reports success for precisely the repos this
    check exists to catch. `.gitattributes` is behind the gate and present in
    every git-backed repo; the tree API supplies a fallback when it is not.

Failure to reach HF is NOT a failure of the repo. Every network error, 5xx, and
timeout resolves to UNKNOWN, and UNKNOWN never blocks an add — a check that
turns an HF hiccup into a refused task is worse than no check.
"""

from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from typing import Optional

# Per-request and whole-probe ceilings. The add route awaits this, so the
# budget is a user-visible latency floor as much as a safety limit; the whole
# probe is capped below the deadline any reasonable HTTP client would apply.
REQUEST_TIMEOUT_S = 4.0
TOTAL_BUDGET_S = 10.0

# Present in every git-backed repo and behind the gate, unlike README.md.
PRIMARY_PROBE_PATH = ".gitattributes"

# Outcomes. Only NEEDS_APPROVAL and NOT_FOUND are actionable; the other two
# mean "carry on".
OK = "ok"
NEEDS_APPROVAL = "needs_approval"
NOT_FOUND = "not_found"
UNKNOWN = "unknown"

BLOCKING_OUTCOMES = (NEEDS_APPROVAL, NOT_FOUND)

# The only source this module actually probes. Anything else gets OK — which
# means "no opinion", NOT "verified reachable", and callers that act on OK must
# check the source first (see hold.recheck_holds, which would otherwise release
# a ModelScope 403 hold every cycle on the strength of a probe never made).
PROBED_SOURCE = "hf"


@dataclass
class PreflightResult:
    outcome: str
    detail: str
    probe_url: str = ""
    status_code: Optional[int] = None

    @property
    def blocks_add(self) -> bool:
        return self.outcome in BLOCKING_OUTCOMES


def hf_repo_url(repo_id: str, dtype: str = "dataset") -> str:
    """The page a human opens to request access. Datasets live under a
    /datasets/ prefix; models sit at the root."""
    if (dtype or "dataset") == "model":
        return f"https://huggingface.co/{repo_id}"
    return f"https://huggingface.co/datasets/{repo_id}"


def _api_base(repo_id: str, dtype: str) -> str:
    if (dtype or "dataset") == "model":
        return f"https://huggingface.co/api/models/{repo_id}"
    return f"https://huggingface.co/api/datasets/{repo_id}"


def _classify(status: int, url: str) -> Optional[PreflightResult]:
    """Map an HTTP status on a resolve/tree URL to an outcome, or None when the
    caller should try a different URL (404 = maybe just this path)."""
    if 200 <= status < 400:
        return PreflightResult(OK, "可访问", url, status)
    if status == 403:
        return PreflightResult(
            NEEDS_APPROVAL,
            "已认证但未获授权：需要在 HuggingFace 仓库页面点击同意后才能下载",
            url,
            status,
        )
    if status == 401:
        # With a token attached this is not a gate, it is a bad token — an
        # operator problem on our side. Blocking the add would blame the repo
        # for our own misconfiguration.
        return PreflightResult(
            UNKNOWN,
            "HuggingFace 拒绝了 HF_TOKEN（401）—— 预检无法判断，已放行；请检查 S1 的 token",
            url,
            status,
        )
    if status == 404:
        return None
    return PreflightResult(
        UNKNOWN, f"预检收到 HTTP {status}，无法判断，已放行", url, status
    )


def probe_hf_repo(repo_id: str, dtype: str = "dataset",
                  token: Optional[str] = None) -> PreflightResult:
    """Blocking probe. Callers inside the event loop must use
    check_repo_access() instead, which moves this to a thread."""
    import requests

    token = token if token is not None else os.environ.get("HF_TOKEN", "")
    if not token:
        return PreflightResult(
            UNKNOWN,
            "S1 未配置 HF_TOKEN，gated 预检已跳过",
        )

    headers = {"Authorization": f"Bearer {token}"}
    deadline = time.monotonic() + TOTAL_BUDGET_S
    page = hf_repo_url(repo_id, dtype)

    def _head(url: str) -> Optional[PreflightResult]:
        if time.monotonic() >= deadline:
            return PreflightResult(UNKNOWN, "预检超出时间预算，已放行", url)
        try:
            # allow_redirects=False on purpose: a 302 to the CDN already proves
            # authorisation, and following it would download bytes we do not
            # want and log a multi-KB signed URL.
            resp = requests.head(url, headers=headers, allow_redirects=False,
                                 timeout=REQUEST_TIMEOUT_S)
        except Exception as exc:
            return PreflightResult(
                UNKNOWN, f"预检请求失败（{type(exc).__name__}），已放行", url
            )
        return _classify(resp.status_code, url)

    resolved = _head(f"{page}/resolve/main/{PRIMARY_PROBE_PATH}")
    if resolved is not None:
        return resolved

    # .gitattributes 404'd. That says nothing about the repo until we know
    # whether the repo itself exists, so ask the tree API and, if it does,
    # re-probe with a file we know is in there.
    tree_url = f"{_api_base(repo_id, dtype)}/tree/main"
    if time.monotonic() >= deadline:
        return PreflightResult(UNKNOWN, "预检超出时间预算，已放行", tree_url)
    try:
        resp = requests.get(tree_url, headers=headers, timeout=REQUEST_TIMEOUT_S)
    except Exception as exc:
        return PreflightResult(
            UNKNOWN, f"预检请求失败（{type(exc).__name__}），已放行", tree_url
        )

    if resp.status_code == 404:
        return PreflightResult(
            NOT_FOUND,
            f"HuggingFace 上找不到 {repo_id}（404）",
            tree_url,
            404,
        )
    classified = _classify(resp.status_code, tree_url)
    if classified is not None and classified.outcome != OK:
        return classified
    if classified is None:
        return PreflightResult(UNKNOWN, "预检无法判断，已放行", tree_url,
                               resp.status_code)

    try:
        entries = [e for e in resp.json() if e.get("type") == "file"]
    except Exception:
        return PreflightResult(UNKNOWN, "预检无法解析仓库文件列表，已放行", tree_url)

    if not entries:
        # A genuinely empty repo (fastumi100k is one) is reachable; there is
        # nothing gated about having no files.
        return PreflightResult(OK, "仓库可访问（根目录无文件）", tree_url,
                               resp.status_code)

    smallest = min(entries, key=lambda e: e.get("size") or 0)
    fallback = _head(f"{page}/resolve/main/{smallest.get('path')}")
    if fallback is not None:
        return fallback
    return PreflightResult(UNKNOWN, "预检无法判断，已放行", tree_url)


async def check_repo_access(repo_id: str, source: str,
                            dtype: str = "dataset") -> PreflightResult:
    """Async entry point. Non-hf sources are not probed.

    ModelScope is out of scope deliberately: its authorisation model differs,
    and no ModelScope task has ever exhibited this failure. Probing it would be
    speculation dressed as a safety check.

    The blocking work runs in a thread — the add route and the recheck loop are
    both on the web process's event loop, which also serves the watchdog's
    /api/dashboard probe every 30 seconds. A 10-second synchronous HTTP call
    there is a self-inflicted wedge.
    """
    if source != PROBED_SOURCE:
        return PreflightResult(OK, f"source={source}，未做 gated 预检")
    if not repo_id or "/" not in repo_id:
        return PreflightResult(UNKNOWN, "repo_id 不完整，预检跳过")
    return await asyncio.to_thread(probe_hf_repo, repo_id, dtype)
