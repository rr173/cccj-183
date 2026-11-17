"""Budget and termination behaviour: over-budget is 'incomplete', never 'complete'."""

import io
import json
import os
import tempfile
from contextlib import redirect_stderr, redirect_stdout

from chase.chase import Budgets, Engine
from chase.model import Schema, parse_program
from chase.store import Store


def _run(schema, rules, facts, budgets=None):
    sch = Schema({r: tuple(c) for r, c in schema.items()})
    rs = parse_program({"rules": rules}, sch)
    store = Store(":memory:")
    eng = Engine(sch, rs, store, budgets or Budgets())
    eng.preload_input({r: [tuple(x) for x in rows] for r, rows in facts.items()})
    return eng.run()


SCHEMA = {"A": ["text", "text"], "B": ["text", "text"]}
RULES = [{"id": "t", "body": ["A(x, y)"], "head": "B(x, u)"}]
FACTS = {"A": [["k", "v"]]}


def test_fact_budget():
    r = _run(SCHEMA, RULES, FACTS, Budgets(max_facts=1))
    assert r.status == "incomplete"
    assert r.reason == "facts"
    assert r.stats["facts_total"] >= 1


def test_trigger_budget():
    r = _run(SCHEMA, RULES, FACTS, Budgets(max_triggers=0))
    assert r.status == "incomplete"
    assert r.reason in ("triggers", "facts")


def test_time_budget():
    r = _run(SCHEMA, RULES, FACTS, Budgets(max_seconds=0.0))
    assert r.status == "incomplete"
    assert r.reason in ("time", "facts")


def test_round_budget():
    r = _run(SCHEMA, RULES, FACTS, Budgets(max_rounds=0))
    assert r.status == "incomplete"
    assert r.reason in ("rounds", "facts")


def test_over_budget_flag_never_reads_complete():
    r = _run(SCHEMA, RULES, FACTS, Budgets(max_facts=0))
    assert r.status != "complete"
    assert r.status == "incomplete"


def test_recursive_program_terminates_within_budget():
    schema = {"E": ["text", "text"], "P": ["text", "text"]}
    rules = [{"id": "copy", "body": ["E(x, y)"], "head": "P(x, y)"},
             {"id": "step", "body": ["P(x, y)", "E(y, z)"], "head": "P(x, z)"}]
    r = _run(schema, rules, {"E": [["a", "b"], ["b", "c"]]},
             Budgets(max_seconds=10))
    assert r.status == "complete"


def test_non_weakly_acyclic_program_is_rejected_before_running():
    from chase.cli import main
    sc = {
        "schema": {"R": ["text", "text"], "S": ["text", "text"]},
        "rules": [{"id": "a", "body": ["R(x, y)"], "head": "S(y, w)"},
                  {"id": "b", "body": ["S(x, y)"], "head": "R(x, y)"}],
        "facts": {"R": [["a", "b"]]},
    }
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "s.json")
        with open(path, "w") as fh:
            json.dump(sc, fh)
        out = os.path.join(tmp, "pkg")
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            rc = main(["complete", path, "-o", out])
        assert rc == 2
        assert not os.path.exists(out), "a rejected program must not produce a package"
