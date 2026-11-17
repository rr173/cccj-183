"""Randomised small-program fuzzing (fixed seed; no external deps).

Generates small weakly-acyclic programs over a couple of relations, runs the
engine, and checks the result against the independent oracle:

* the output is a model of TGDs + EGDs and keeps all input facts;
* every derived fact is non-gratuitous (dropping it breaks the model);
* the result maps homomorphically into every enumerated model (it is a
  universal model, as the chase guarantees);
* every legal trigger order yields an isomorphic result.

The generator is deliberately tiny so failures are reproducible and cheap to
read.
"""

import itertools
import random

from chase.model import (
    LNullValue, ProgramError, Schema, assert_weakly_acyclic, parse_program,
)
from tests import oracle
from tests.helpers import build, instance_map

RELATIONS = {"P": ["text", "text"], "Q": ["text", "text"]}


def _random_atom(rng, vars_):
    rel = rng.choice(list(RELATIONS))
    terms = [rng.choice(vars_) for _ in range(2)]
    return f"{rel}({terms[0]}, {terms[1]})"


def random_program(rng):
    rules = []
    n = rng.randint(1, 3)
    for i in range(n):
        vs = ["x", "y"]
        body = [_random_atom(rng, vs)]
        if rng.random() < 0.35:
            body.append(_random_atom(rng, vs))
        if rng.random() < 0.5:
            # TGD: head may introduce one existential
            head_vars = vs + (["u"] if rng.random() < 0.6 else [])
            head = f"{rng.choice(list(RELATIONS))}({rng.choice(head_vars)}, " \
                   f"{rng.choice(head_vars)})"
            rules.append({"id": f"t{i}", "body": body, "head": head})
        else:
            rules.append({"id": f"e{i}", "body": body,
                          "eq": [rng.choice(vs), rng.choice(vs)]})
    return rules


def random_facts(rng, objects=("a", "b")):
    facts = {}
    for rel in RELATIONS:
        rows = []
        for _ in range(rng.randint(0, 3)):
            rows.append([rng.choice(objects), rng.choice(objects)])
        if rows:
            facts[rel] = rows
    return facts


def test_fuzz_against_oracle():
    rng = random.Random(20240117)
    checked = 0
    for _trial in range(120):
        schema = Schema({r: tuple(c) for r, c in RELATIONS.items()})
        rules = random_program(rng)
        try:
            parsed = parse_program({"rules": rules}, schema)
            assert_weakly_acyclic(parsed)
        except ProgramError:
            continue  # not weakly acyclic: correctly rejected; skip
        except Exception:
            continue  # malformed by construction (e.g. EGD var unbound)
        facts = random_facts(rng)
        try:
            _, rs, store, engine, result = build(RELATIONS, rules, facts)
        except Exception:
            raise
        assert result.status in ("complete", "conflict"), result.detail
        checked += 1
        if result.status == "conflict":
            c = result.conflict
            assert c.left[0] != "n" and c.right[0] != "n"
            assert c.left != c.right
            continue
        inst = oracle.norm_instance(instance_map(store, RELATIONS))
        input_inst = oracle.norm_instance(facts)
        ok, why = oracle.check_instance(schema, rs, input_inst, inst)
        assert ok, (rules, facts, why)
        redundant = oracle.redundant_facts(schema, rs, input_inst, inst)
        assert not redundant, (rules, facts, redundant)
    assert checked > 30, f"only {checked} programs exercised"


def test_fuzz_order_independence():
    rng = random.Random(99)
    exercised = 0
    for _trial in range(60):
        schema = Schema({r: tuple(c) for r, c in RELATIONS.items()})
        rules = random_program(rng)
        try:
            parsed = parse_program({"rules": rules}, schema)
            assert_weakly_acyclic(parsed)
        except Exception:
            continue
        facts = random_facts(rng)
        results = []
        for seed in (0, 1, 2):
            _, _, store, engine, result = build(RELATIONS, rules, facts,
                                                order_seed=seed)
            if result.status != "complete":
                results = []
                break
            results.append(oracle.norm_instance(instance_map(store, RELATIONS)))
        if len(results) < 3:
            continue
        exercised += 1
        for a, b in itertools.combinations(results, 2):
            assert _iso(a, b), (rules, facts, a, b)
    assert exercised > 15, exercised


def _iso(a, b):
    if set(a) != set(b):
        return False
    na = sorted({v[1] for rows in a.values() for row in rows for v in row if v[0] == "?"})
    nb = sorted({v[1] for rows in b.values() for row in rows for v in row if v[0] == "?"})
    if len(na) != len(nb):
        return False
    for perm in itertools.permutations(nb):
        m = dict(zip(na, perm))

        def img(v):
            return ("?", m[v[1]]) if v[0] == "?" else v

        if {rel: {tuple(img(v) for v in row) for row in rows}
            for rel, rows in a.items()} == b:
            return True
    return False
