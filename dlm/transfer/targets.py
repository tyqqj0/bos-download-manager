"""Where a task's bytes come FROM on BOS and where they GO on 地瓜云.

The BOS side is `bos_target()` (dlm/core/bos.py) and nothing here second-guesses
it: the downloader's uploader and the resume filter already agree on that prefix,
so a transfer that reads from anywhere else copies the wrong thing. Never read
`tasks.bos_path` — measured 2026-08-10, molmobot-data's column holds
`auwomo-datasets/raw-data/molmobot-data/`, which is a 地瓜云 *destination*, not a
BOS source prefix.

The 地瓜云 side used to live in three places that disagreed:

  - `dlm/transfer/tasks.py` (dead Celery task) built `/auwomo-model/{category}/{name}`
  - `scripts/reconcile_transfers.py:30-32` documented the model category segment
    as optional
  - `scripts/transfer_import.py` hardcoded the dataset layout and could not
    address the model bucket at all

Measured 2026-08-10 against the live filesystem and the remote import history,
the with-category layout is the one that actually happened: a successful import
reads `auwomo-model-open.bj.bcebos.com/Qwen3-VL-30B-A3B-Thinking/ ->
/727a2f92-30c/auwomo-model/multimodal/Qwen3-VL-30B-A3B-Thinking/`, and five of
our six `done` models sit under a category directory. The flat
`/auwomo-model/{name}` entries beside them are other teams' uploads;
Wan2.1-I2V-14B-480P exists under BOTH, which is what a second convention costs.
"""

from dataclasses import dataclass, replace

from ..core.bos import bos_target

JFS_ROOT = "/727a2f92-30c"
DATASET_ROOT = f"{JFS_ROOT}/auwomo-datasets/raw-data"
MODEL_ROOT = f"{JFS_ROOT}/auwomo-model"


@dataclass(frozen=True)
class TransferPlan:
    """One transfer, fully addressed on both ends."""

    bucket: str
    prefix: str   # BOS key prefix, always ends with "/"
    parent: str   # 地瓜云 folder that will hold the target
    name: str

    @property
    def target(self) -> str:
        """The 地瓜云 folder the bytes land in."""
        return f"{self.parent}/{self.name}"

    @property
    def source(self) -> str:
        """`{bucket}/{prefix}` — the form stored in `tasks.transfer_prefix`.

        One string rather than two columns because the pair is only ever
        compared as a whole: a prefix that matches under a different bucket is
        not the same source (a category-less dataset and a model share the key
        shape `{name}/` and differ only by bucket).
        """
        return f"{self.bucket}/{self.prefix}"


def plan_transfer(task) -> TransferPlan:
    """Derive both ends from a task. Accepts anything with .type/.name/.category
    (TaskInput, sqlite3.Row via `plan_from_mapping`, a manifest entry)."""
    bucket, prefix = bos_target(task)
    category = getattr(task, "category", None) or ""
    root = MODEL_ROOT if getattr(task, "type", None) == "model" else DATASET_ROOT
    parent = f"{root}/{category}" if category else root
    return TransferPlan(bucket=bucket, prefix=prefix, parent=parent, name=task.name)


class _AttrView:
    """Attribute access over a mapping (dict, sqlite3.Row, manifest entry).

    Missing keys must raise AttributeError, not KeyError/IndexError, so that
    `getattr(task, "type", None)` in `bos_target` sees an absent field as None
    instead of exploding.
    """

    __slots__ = ("_m",)

    def __init__(self, m):
        self._m = m

    def __getattr__(self, key):
        try:
            return self._m[key]
        except (KeyError, IndexError):
            raise AttributeError(key) from None


def plan_from_mapping(entry) -> TransferPlan:
    """Plan for a manifest entry / DB row.

    An explicit `src` in the entry OVERRIDES the derived BOS prefix. That is not
    a safety valve, it is the normal case for first-generation datasets: BOS
    holds DL3DV-ALL-4K under `datasets/DL3DV-ALL-4K/` while its category is
    `multimodal`, so source and derived prefix legitimately differ. The
    destination is always derived — only the source can be overridden.
    """
    plan = plan_transfer(_AttrView(entry))
    src = entry.get("src") if hasattr(entry, "get") else None
    if src and src != plan.prefix:
        plan = replace(plan, prefix=src)
    return plan
