"""Every activity call must pass ALL of the activity's parameters.

This is not style — it is a correctness requirement imposed by temporalio.
The activity worker only applies the activity's argument type hints when the
declared parameter count matches the number of payloads it received
(temporalio/worker/_activity.py):

    elif arg_types is not None and len(arg_types) != len(start.input):
        arg_types = None

With `arg_types = None` every payload is decoded generically, so a parameter
annotated as a dataclass (`task_input: TaskInput`) arrives as a plain dict and
the activity dies on the first attribute access — `'dict' object has no
attribute 'name'`. That is exactly what killed the first production pool
dispatch: `chunk_filelist` declares three parameters and the coordinator was
passing two, relying on the third's default.

The defaults are still useful to direct callers (these tests call activities as
plain functions), so the rule is enforced at the call sites instead of by
banning defaults.

Run: python3 -m pytest tests/test_activity_arity.py -q   (needs temporalio)
"""

import ast
from pathlib import Path

import pytest

TEMPORAL_DIR = Path(__file__).resolve().parent.parent / "dlm" / "temporal"
ACTIVITIES_FILE = TEMPORAL_DIR / "activities.py"
CALL_ATTRS = {"execute_activity", "start_activity", "execute_local_activity"}


def _activity_signatures() -> dict[str, list[str]]:
    """{activity name: [parameter names]} for every @activity.defn."""
    tree = ast.parse(ACTIVITIES_FILE.read_text())
    sigs = {}
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        is_activity = any(
            isinstance(d, ast.Attribute) and d.attr == "defn"
            for d in node.decorator_list
        )
        if is_activity:
            sigs[node.name] = [a.arg for a in node.args.args]
    return sigs


def _activity_calls():
    """(file, line, activity name, arg count) for every activity invocation."""
    calls = []
    for path in sorted(TEMPORAL_DIR.glob("*.py")):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not isinstance(node.func, ast.Attribute) or node.func.attr not in CALL_ATTRS:
                continue
            if not node.args or not isinstance(node.args[0], ast.Constant):
                continue  # dynamic activity name — nothing static to check
            nargs = None
            for kw in node.keywords:
                if kw.arg == "arg":
                    nargs = 1
                elif kw.arg == "args":
                    if not isinstance(kw.value, (ast.List, ast.Tuple)):
                        nargs = "dynamic"
                    else:
                        nargs = len(kw.value.elts)
            if nargs is None:
                nargs = 0  # no arg/args kwarg = zero-argument activity
            calls.append((path.name, node.lineno, node.args[0].value, nargs))
    return calls


def test_found_activities_and_calls():
    """Guard the guard: a broken parser must not pass as "nothing to check"."""
    sigs = _activity_signatures()
    calls = _activity_calls()
    assert len(sigs) > 20, f"only found {len(sigs)} activities — parser broken?"
    assert len(calls) > 20, f"only found {len(calls)} activity calls — parser broken?"


def test_every_activity_call_passes_every_parameter():
    sigs = _activity_signatures()
    problems = []
    for file, line, name, nargs in _activity_calls():
        if name not in sigs:
            continue  # activity defined elsewhere (or a typo other tests catch)
        params = sigs[name]
        if nargs == "dynamic":
            problems.append(
                f"{file}:{line} {name}(...) builds its args list dynamically — "
                f"this test cannot verify it passes all {len(params)} parameters"
            )
        elif nargs != len(params):
            problems.append(
                f"{file}:{line} {name}(...) passes {nargs} args but the activity "
                f"declares {len(params)}: {params}. temporalio will drop the "
                f"argument type hints and deliver dataclasses as raw dicts."
            )
    assert not problems, "\n".join(problems)


def test_missing_type_hint_yields_a_dict_not_a_dataclass():
    """Pin the temporalio behaviour this whole file exists to prevent.

    If a future temporalio version starts converting without hints, this test
    fails and the rule above can be relaxed deliberately rather than by
    accident.
    """
    temporalio = pytest.importorskip("temporalio.converter")
    from dlm.temporal.models import TaskInput

    converter = temporalio.DataConverter.default.payload_converter
    payloads = converter.to_payloads([TaskInput(id="t-1", name="n", repo_id="o/r")])

    with_hint = converter.from_payloads(payloads, [TaskInput])
    assert isinstance(with_hint[0], TaskInput)
    assert with_hint[0].name == "n"

    without_hint = converter.from_payloads(payloads, None)
    assert isinstance(without_hint[0], dict), (
        "temporalio now reconstructs dataclasses without a type hint — the "
        "arity rule enforced in this file may no longer be necessary"
    )
    with pytest.raises(AttributeError):
        without_hint[0].name
