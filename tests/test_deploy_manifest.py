"""The deploy script's version gate is the only thing enforcing "no mixed
code versions", so what it hashes decides what that guarantee is worth.

It hashed a hand-written list of seven files. `dlm/core/bos.py` was not on it —
the whole upload path, including the multipart driver that returns False and
aborts instead of raising. A worker running a stale uploader passed the gate
with an OK, which is the exact failure the gate exists to make impossible.

Run: pytest tests/test_deploy_manifest.py -q
"""

from __future__ import annotations

import pathlib
import re
import subprocess

REPO = pathlib.Path(__file__).resolve().parent.parent
SCRIPT = REPO / "scripts" / "deploy-workers.sh"


def _manifest_cmd() -> str:
    """The one shell command the script uses for both sides of the compare."""
    m = re.search(r"^MANIFEST_CMD='(.+)'$", SCRIPT.read_text(), re.M)
    assert m, "MANIFEST_CMD not found — was the version gate renamed?"
    return m.group(1)


def test_the_gate_hashes_the_same_command_on_both_sides():
    """A reference hash computed differently from the remote one compares
    nothing. One variable, used twice."""
    text = SCRIPT.read_text()
    cmd = _manifest_cmd()
    assert text.count("$MANIFEST_CMD") == 2, (
        "MANIFEST_CMD must be used for the S1 reference AND the remote probe")
    assert "cd $REMOTE_DIR && $MANIFEST_CMD" in text
    assert "md5sum" in cmd


def test_the_manifest_covers_every_python_file_workers_run():
    """Derived from the tree, not enumerated — an enumeration drifts silently
    the moment a new module lands, and silence here reads as agreement."""
    find_expr = _manifest_cmd().split("|")[0].strip()
    assert find_expr.startswith("find dlm "), f"unexpected file selector: {find_expr}"

    listed = subprocess.run(
        ["bash", "-c", f"cd {REPO} && {find_expr.replace('-print0', '')}"],
        capture_output=True, text=True, check=True,
    ).stdout.split()

    on_disk = sorted(
        str(p.relative_to(REPO)) for p in (REPO / "dlm").rglob("*.py")
        if "__pycache__" not in p.parts
    )
    assert sorted(listed) == on_disk, "find expression misses files rsync carries"

    # The specific omission that motivated this, plus the other paths whose
    # staleness is invisible at runtime.
    for required in ("dlm/core/bos.py", "dlm/temporal/pipeline.py",
                     "dlm/temporal/workflows.py", "dlm/temporal/activities.py",
                     "dlm/core/naming.py", "dlm/temporal/models.py"):
        assert required in on_disk, f"{required} vanished — update this test"


def test_the_hash_is_order_stable_across_hosts():
    """Two hosts must agree. `find` returns filesystem order, which does not,
    so the sort is load-bearing — and its collation must be pinned or a
    different locale on one host reorders the stream."""
    cmd = _manifest_cmd()
    assert "sort" in cmd, "unsorted find output hashes differently per host"
    assert "LC_ALL=C" in cmd, "collation not pinned — locale drift changes the hash"

    first = subprocess.run(["bash", "-c", f"cd {REPO} && {cmd}"],
                           capture_output=True, text=True, check=True).stdout
    second = subprocess.run(["bash", "-c", f"cd {REPO} && {cmd}"],
                            capture_output=True, text=True, check=True).stdout
    assert first.strip() and first == second


def test_a_changed_upload_path_changes_the_hash():
    """The regression test for the actual defect: edit bos.py, hash moves."""
    cmd = _manifest_cmd()
    target = REPO / "dlm" / "core" / "bos.py"
    original = target.read_bytes()

    before = subprocess.run(["bash", "-c", f"cd {REPO} && {cmd}"],
                            capture_output=True, text=True, check=True).stdout
    try:
        target.write_bytes(original + b"\n# version marker\n")
        after = subprocess.run(["bash", "-c", f"cd {REPO} && {cmd}"],
                               capture_output=True, text=True, check=True).stdout
    finally:
        target.write_bytes(original)

    assert before != after, (
        "dlm/core/bos.py is outside the version gate — a worker with a stale "
        "uploader would report OK")
