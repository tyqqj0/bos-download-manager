"""Is this import already running on the far side?

Re-issuing an import that is still in flight is what 2026-08-04's DL3DV run
walked into: `transfer_import.py` gave up at its 72h poll cap and exited, but
the remote task kept going and only failed on **2026-08-09 19:26**
(`juicefs <FATAL>: failed to handle 2 objects`). For those two days a re-run
would have posted a second import over the same target while the first was
still writing it — nothing in the old code looked.

So: before issuing an import, list the remote async tasks and look for one
that is still running against the same (source, target). `list_async_tasks`
carries both fields (verified 2026-08-10: items have keys `created_at`,
`error_msg`, `result_detail`, `source`, `status`, `target`, `task_id`,
`task_type`, `updated_at`), so the match is exact rather than name-guessed.

The in-progress status literal is NOT known — only `成功` and `失败` have been
observed in the history. So "running" is defined as the complement: any status
that is neither a known success nor a known failure. That direction is the safe
one; the cost of misreading a finished task as running is one extra poll cycle,
while the cost of misreading a running task as finished is two importers
writing the same directory.
"""

TERMINAL_OK = frozenset({"成功", "完成", "success", "done", "completed"})
TERMINAL_FAIL = frozenset({
    "失败", "错误", "已取消", "取消",
    "failed", "failure", "error", "cancelled", "canceled", "aborted",
})


def classify(status) -> str:
    """`"ok"` / `"failed"` / `"running"` — anything unrecognized is running."""
    s = str(status or "").strip()
    if s in TERMINAL_OK:
        return "ok"
    if s in TERMINAL_FAIL:
        return "failed"
    return "running"


def endpoint_source(bucket: str, prefix: str, bos_endpoint: str = "bj.bcebos.com") -> str:
    """The `source` string the remote records for an import.

    Must stay identical to the `endpoint` field `DCloudClient.import_from_bos`
    posts (`dcloud.py:128`) — that string is what comes back in the task list,
    and it is the only way to tell two imports of different prefixes apart.
    """
    return f"{bucket}.{bos_endpoint}/{prefix}"


def _norm(path) -> str:
    """Compare paths without tripping over a scheme or a trailing slash.

    The remote echoes back what it was given, and the two ends of a comparison
    come from different places (our f-string vs. their record), so `.../X/` and
    `.../X` must compare equal. Case is preserved: BOS keys are case-sensitive
    and `multimodal/WebVid-10M/` is a different prefix from
    `multimodal/webvid-10M/` — both exist.
    """
    s = str(path or "").strip()
    for scheme in ("https://", "http://"):
        if s.startswith(scheme):
            s = s[len(scheme):]
            break
    return s.rstrip("/")


def paths_match(a, b) -> bool:
    return _norm(a) == _norm(b)


def find_by_id(tasks, task_id):
    for t in tasks:
        if t.get("task_id") == task_id:
            return t
    return None


def find_running(tasks, source, target, task_id=None):
    """A still-running remote task for this (source, target), or None.

    A `task_id` we already recorded wins over a path match: it is the task we
    know we started, and reusing it keeps one item's history in one place.
    """
    running = [t for t in tasks if classify(t.get("status")) == "running"]
    if task_id:
        mine = find_by_id(running, task_id)
        if mine is not None:
            return mine
    for t in running:
        if paths_match(t.get("source"), source) and paths_match(t.get("target"), target):
            return t
    return None


def fetch_tasks(client, max_pages: int = 5, page_size: int = 100) -> list:
    """Recent async tasks, newest first, across up to `max_pages` pages.

    One call refreshes every in-flight transfer at once, which is why the
    dispatcher can poll 16 concurrent imports with O(1) HTTP rather than O(N).
    """
    out = []
    for page in range(1, max_pages + 1):
        batch = client.list_async_tasks(page=page, page_size=page_size)
        if not batch:
            break
        out.extend(batch)
        if len(batch) < page_size:
            break
    return out
