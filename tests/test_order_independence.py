"""Order-independence and homomorphism tests across legal trigger orders.

The requirement: for every input there is one result up to a bijection of
labeled nulls, regardless of which legal firing order the chase happens to
take.  The helpers below produce genuinely different legal orders by varying a
per-fact rule shuffle seed, and (for a stronger check) by permuting the input
row order, which also changes the queue order.

Constants must correspond exactly (bidirectional homomorphism with constants
fixed); only unknown *names* may differ.
"""

import itertools
import random

from chase.model import Schema, parse_program
from tests.helpers import build, instance_map
from tests import oracle


def _norm(inst):
    return oracle.norm_instance({rel: rows for rel, rows in inst.items()})


def _nulls(inst):
    return sorted({v[1] for rows in inst.values() for row in rows for v in row
                   if v[0] == "?"})


def _iso(a, b):
    if set(a) != set(b):
        return False
    na, nb = _nulls(a), _nulls(b)
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


def _homomorphic(a, b):
    ok, _ = oracle.homomorphism_exists(a, b)
    return ok


SCENARIOS = []

SCENARIOS.append((
    "orders_known",
    {"Order": ["text", "text"], "Customer": ["text", "text"],
     "KnownAddress": ["text", "text"]},
    [{"id": "t1", "body": ["Order(o, c)"], "head": "Customer(c, a)"},
     {"id": "t2", "body": ["KnownAddress(c, a)"], "head": "Customer(c, a)"},
     {"id": "e", "body": ["Customer(c, a1)", "Customer(c, a2)"], "eq": ["a1", "a2"]}],
    {"Order": [["o1", "c"], ["o2", "c"], ["o3", "d"]],
     "KnownAddress": [["c", "城东"]]},
))

SCENARIOS.append((
    "chain_merge",
    {"A": ["text", "text"], "B": ["text", "text"], "C": ["text", "text"],
     "K1": ["text", "text"], "K2": ["text", "text"]},
    [{"id": "mk1", "body": ["K1(c, x)"], "head": "B(c, u)"},
     {"id": "mk2", "body": ["K2(c, x)"], "head": "B(c, v)"},
     {"id": "tob", "body": ["A(x, y)"], "head": "C(x, w)"},
     {"id": "join", "body": ["B(x, a)", "C(x, b)", "A(x, b)"], "head": "D(x, a)"},
     {"id": "e1", "body": ["B(x, a1)", "B(x, a2)"], "eq": ["a1", "a2"]},
     {"id": "e2", "body": ["B(x, a)", "C(x, b)"], "eq": ["a", "b"]},
     ],
    {"A": [["k", "v"], ["j", "w"]], "K1": [["k", "p"]], "K2": [["k", "q"]]},
))
SCENARIOS[-1][1]["D"] = ["text", "text"]


def test_results_are_isomorphic_across_orders():
    for name, schema, rules, facts in SCENARIOS:
        results = []
        for perm in itertools.permutations(list(facts.keys())):
            permuted = {k: facts[k] for k in perm}
            for seed in (0, 1, 2):
                _, _, store, engine, result = build(schema, rules, permuted,
                                                    order_seed=seed)
                assert result.status == "complete", (name, seed, result.detail)
                results.append((_norm(instance_map(store, schema)), seed))
        base = results[0][0]
        for inst, seed in results[1:]:
            assert _iso(base, inst), (name, seed, base, inst)


def test_constants_correspond_bidirectionally():
    # constants must be preserved exactly in both directions
    for name, schema, rules, facts in SCENARIOS:
        insts = []
        for seed in range(4):
            _, _, store, engine, result = build(schema, rules, facts,
                                                order_seed=seed)
            assert result.status == "complete"
            insts.append(_norm(instance_map(store, schema)))
        for a in insts:
            for b in insts:
                assert _homomorphic(a, b), (name, a, b)
                assert _homomorphic(b, a)


def test_row_order_does_not_matter():
    schema = {"Order": ["text", "text"], "Customer": ["text", "text"],
              "KnownAddress": ["text", "text"]}
    rules = [
        {"id": "t1", "body": ["Order(o, c)"], "head": "Customer(c, a)"},
        {"id": "t2", "body": ["KnownAddress(c, a)"], "head": "Customer(c, a)"},
        {"id": "e", "body": ["Customer(c, a1)", "Customer(c, a2)"], "eq": ["a1", "a2"]},
    ]
    rows = [["o1", "c"], ["o2", "c"], ["o3", "d"]]
    insts = []
    for perm in itertools.permutations(rows):
        _, _, store, engine, result = build(
            schema, rules, {"Order": [list(r) for r in perm]})
        assert result.status == "complete"
        insts.append(_norm(instance_map(store, schema)))
    base = insts[0]
    for inst in insts[1:]:
        assert _iso(base, inst)


def test_same_number_of_unknowns_and_classes_in_every_order():
    name, schema, rules, facts = SCENARIOS[0]
    counts = set()
    for seed in range(6):
        _, _, store, engine, result = build(schema, rules, facts, order_seed=seed)
        live = engine.export()["nulls"]["live"]
        inst = _norm(instance_map(store, schema))
        uses = sum(1 for rows in inst.values() for row in rows for v in row if v[0] == "?")
        counts.add((len(live), uses))
    # c's address is known: no live unknown remains; d keeps exactly one,
    # used once in its single Customer row.
    assert counts == {(1, 1)}, counts
