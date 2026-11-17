"""The program check must not depend on the sample data.

These test the one requirement that is easy to get wrong: *accepting* a bad
rule set because the current facts happen to terminate.
"""

import io
import json
import os
import tempfile
from contextlib import redirect_stderr, redirect_stdout

from chase.cli import main
from chase.model import ProgramError, Schema, assert_weakly_acyclic, parse_program

BAD_RULES = [
    {"id": "grow", "body": ["R(x, y)"], "head": "S(y, w)"},
    {"id": "back", "body": ["S(x, y)"], "head": "R(x, y)"},
]


def _write(tmp, facts):
    sc = {"schema": {"R": ["text", "text"], "S": ["text", "text"]},
          "rules": BAD_RULES, "facts": facts}
    path = os.path.join(tmp, "s.json")
    with open(path, "w") as fh:
        json.dump(sc, fh)
    return path


def test_bad_program_rejected_with_empty_input():
    # With no facts the chase would terminate immediately; the check must
    # still reject the program.
    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "pkg")
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            rc = main(["complete", _write(tmp, {}), "-o", out])
        assert rc == 2
        assert not os.path.exists(out)


def test_bad_program_rejected_with_small_input():
    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "pkg")
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            rc = main(["complete", _write(tmp, {"R": [["a", "b"]]}), "-o", out])
        assert rc == 2


def test_check_command_rejects_bad_program_without_facts():
    with tempfile.TemporaryDirectory() as tmp:
        path = _write(tmp, {})
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = main(["check", path])
        assert rc == 2
        report = json.loads(buf.getvalue())
        assert report["ok"] is False
        assert "not weakly acyclic" in report["error"]


def test_check_annotates_cycle_paths():
    sch = Schema({"R": ("text", "text"), "S": ("text", "text")})
    rules = parse_program({"rules": BAD_RULES}, sch)
    bad = [r for r in rules if r.cyclic]
    assert bad and bad[0].cycle_path
    assert bad[0].cyclic_edges


def test_ordinary_recursion_never_rejected_regardless_of_facts():
    # A legal recursive program must stay accepted even when the data happens
    # to produce no new rows.
    sch = Schema({"E": ("text", "text"), "P": ("text", "text")})
    rules = parse_program({"rules": [
        {"id": "c", "body": ["E(x, y)"], "head": "P(x, y)"},
        {"id": "s", "body": ["P(x, y)", "E(y, z)"], "head": "P(x, z)"},
    ]}, sch)
    assert_weakly_acyclic(rules)
