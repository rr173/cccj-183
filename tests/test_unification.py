"""Unification tests: constants, unknowns, duplicates, late conflicts,
new self-joins after unification, and real constraint satisfaction (not just a
rewritten export)."""

from chase.model import LNullValue
from chase.store import Store
from tests.helpers import build, instance_map


def test_duplicate_input_facts_are_a_set():
    schema = {"A": ["text", "text"], "B": ["text", "text"]}
    rules = [{"id": "t", "body": ["A(x, y)"], "head": "B(x, y)"}]
    _, _, store, engine, result = build(
        schema, rules,
        {"A": [["k", "v"], ["k", "v"], ["k", "v"]]})
    assert result.status == "complete"
    inst = instance_map(store, schema)
    assert inst["A"] == {("k", "v")}
    assert inst["B"] == {("k", "v")}
    assert result.stats["trigger_fires"] == 1


def test_derived_duplicate_facts_collapse():
    # two different body bindings producing the same head must not duplicate
    schema = {"A": ["text", "text"], "C": ["text", "text"]}
    rules = [{"id": "t", "body": ["A(x, y)", "C(x, z)"], "head": "B(x, u)"},
             {"id": "t2", "body": ["B(x, u)"], "head": "D(x)"}]
    schema["B"] = ["text", "text"]
    schema["D"] = ["text"]
    _, _, store, engine, result = build(
        schema, rules, {"A": [["k", "v"]], "C": [["k", "z1"], ["k", "z2"]]})
    assert result.status == "complete"
    inst = instance_map(store, schema)
    # one binding produces a head row, the other sees it as a witness
    assert len(inst["B"]) == 1
    assert result.stats["trigger_skips"] >= 1


def test_unification_merges_duplicate_rows_and_keeps_one():
    # Two *distinct* unknowns for the same customer (two body-only relations
    # keep the rules from witnessing each other), the EGD unifies them, and
    # the two rows must collapse to one.
    schema = {"Customer": ["text", "text"], "K1": ["text"], "K2": ["text"],
              "T1": ["text", "text"], "T2": ["text", "text"]}
    rules = [
        {"id": "mk1", "body": ["K1(c)"], "head": "T1(c, u)"},
        {"id": "use1", "body": ["T1(c, u)"], "head": "Customer(c, u)"},
        {"id": "mk2", "body": ["K2(c)"], "head": "T2(c, u)"},
        {"id": "use2", "body": ["T2(c, u)"], "head": "Customer(c, u)"},
        {"id": "e", "body": ["Customer(c, a1)", "Customer(c, a2)"],
         "eq": ["a1", "a2"]},
    ]
    _, _, store, engine, result = build(
        schema, rules, {"K1": [["c"]], "K2": [["c"]]})
    assert result.status == "complete"
    inst = instance_map(store, schema)
    assert len(inst["Customer"]) == 1, inst["Customer"]
    row = next(iter(inst["Customer"]))
    assert isinstance(row[1], LNullValue)
    assert result.stats["unifications"] >= 1
    assert result.stats["duplicates_removed"] >= 1


def test_unification_with_constant_removes_unknown_rows():
    schema = {"A": ["text", "text"], "B": ["text", "text"], "K": ["text", "text"]}
    rules = [
        {"id": "t1", "body": ["A(x, y)"], "head": "B(x, w)"},
        {"id": "t2", "body": ["K(x, a)"], "head": "B(x, a)"},
        {"id": "e", "body": ["B(x, a1)", "B(x, a2)"], "eq": ["a1", "a2"]},
    ]
    _, _, store, engine, result = build(
        schema, rules, {"A": [["k", "v"]], "K": [["k", "城东"]]})
    assert result.status == "complete"
    inst = instance_map(store, schema)
    assert inst["B"] == {("k", "城东")}
    assert engine.export()["nulls"]["live"] == []


def test_new_self_join_appears_after_unification():
    # B gets an unknown w from A; K gives B a constant; the EGD unifies w with
    # the constant; only then does the join B(x,a),B(x,b),A(x,a),K(x,b) exist.
    schema = {"A": ["text", "text"], "B": ["text", "text"],
              "K": ["text", "text"], "Hit": ["text"]}
    rules = [
        {"id": "t1", "body": ["A(x, y)"], "head": "B(x, w)"},
        {"id": "t2", "body": ["K(x, a)"], "head": "B(x, a)"},
        {"id": "e", "body": ["B(x, a1)", "B(x, a2)"], "eq": ["a1", "a2"]},
        {"id": "j", "body": ["B(x, a)", "B(x, b)", "A(x, a)", "K(x, b)"],
         "head": "Hit(x)"},
    ]
    _, _, store, engine, result = build(
        schema, rules, {"A": [["k", "v"]], "K": [["k", "v"]]})
    assert result.status == "complete"
    inst = instance_map(store, schema)
    assert inst["Hit"] == {("k",)}, "join after unification was missed"
    assert engine.export()["nulls"]["live"] == []


def test_late_conflict_after_two_unifications():
    # e1 unifies an unknown with 城东; e2 then compares 城东 with 城西 only
    # after the second row is reachable.  A conflict must be reported; no
    # value may be silently chosen.
    schema = {"A": ["text", "text"], "P": ["text", "text"],
              "M": ["text", "text"], "K": ["text", "text"],
              "N": ["text", "text"], "S": ["text", "text"], "U": ["text", "text"]}
    rules = [
        {"id": "t1", "body": ["A(x, y)"], "head": "P(x, u)"},
        {"id": "e1", "body": ["P(x, u)", "M(x, v)"], "eq": ["u", "v"]},
        {"id": "t2", "body": ["P(x, a)", "K(x, a)"], "head": "S(x, a)"},
        {"id": "t3", "body": ["N(x, b)"], "head": "U(x, b)"},
        {"id": "e2", "body": ["S(x, a)", "U(x, b)"], "eq": ["a", "b"]},
    ]
    _, _, store, engine, result = build(
        schema, rules,
        {"A": [["k", "1"]], "M": [["k", "城东"]], "K": [["k", "城东"]],
         "N": [["k", "城西"]]})
    assert result.status == "conflict", result.detail
    c = result.conflict
    assert c.rule_id == "e2"
    assert {c.left, c.right} == {("s", "城东"), ("s", "城西")}
    assert result.stats["unifications"] == 1


def test_conflict_support_includes_unification_history():
    schema = {"A": ["text", "text"], "P": ["text", "text"],
              "M": ["text", "text"], "K": ["text", "text"],
              "N": ["text", "text"], "S": ["text", "text"], "U": ["text", "text"]}
    rules = [
        {"id": "t1", "body": ["A(x, y)"], "head": "P(x, u)"},
        {"id": "e1", "body": ["P(x, u)", "M(x, v)"], "eq": ["u", "v"]},
        {"id": "t2", "body": ["P(x, a)", "K(x, a)"], "head": "S(x, a)"},
        {"id": "t3", "body": ["N(x, b)"], "head": "U(x, b)"},
        {"id": "e2", "body": ["S(x, a)", "U(x, b)"], "eq": ["a", "b"]},
    ]
    _, _, store, engine, result = build(
        schema, rules,
        {"A": [["k", "1"]], "M": [["k", "城东"]], "K": [["k", "城东"]],
         "N": [["k", "城西"]]})
    assert result.status == "conflict"
    c = result.conflict
    # the left value's evidence includes the merge that produced 城东 here
    assert any("merge" in x for x in c.left_sources)
    assert any("fact" in x for x in c.left_sources)
    # the right value traces straight back to an input row
    assert any(x.get("origin", {}).get("kind") == "input" for x in c.right_sources)


def test_all_old_and_new_rows_follow_the_representative():
    # An unknown used in two relations (shared) must be replaced in both.
    schema = {"A": ["text", "text"], "B": ["text", "text"], "C": ["text", "text"]}
    rules = [
        {"id": "t1", "body": ["A(x, y)"], "head": "B(x, w)"},
        {"id": "t2", "body": ["B(x, a)"], "head": "C(a, x)"},
        {"id": "t3", "body": ["K(x, z)"], "head": "B(x, z)"},
        {"id": "e", "body": ["B(x, a1)", "B(x, a2)"], "eq": ["a1", "a2"]},
    ]
    schema["K"] = ["text", "text"]
    _, _, store, engine, result = build(
        schema, rules, {"A": [["k", "v"]], "K": [["k", "c"]]})
    assert result.status == "complete"
    inst = instance_map(store, schema)
    assert inst["B"] == {("k", "c")}
    assert inst["C"] == {("c", "k")}, "C still references the merged-away unknown"


def test_recursive_program_converges_and_satisfies_egds():
    schema = {"Edge": ["text", "text"], "Path": ["text", "text"],
              "Best": ["text", "text"]}
    rules = [
        {"id": "base", "body": ["Edge(x, y)"], "head": "Path(x, y)"},
        {"id": "step", "body": ["Path(x, y)", "Edge(y, z)"], "head": "Path(x, z)"},
        {"id": "tobest", "body": ["Path(x, y)"], "head": "Best(x, w)"},
        {"id": "e", "body": ["Best(x, a)", "Best(x, b)"], "eq": ["a", "b"]},
    ]
    _, _, store, engine, result = build(
        schema, rules,
        {"Edge": [["a", "b"], ["b", "c"], ["c", "a"]]})
    assert result.status == "complete"
    inst = instance_map(store, schema)
    assert len(inst["Path"]) == 9
    assert len(inst["Best"]) == 3
    # one unknown per source node, all distinct
    nulls = {row[1].id for row in inst["Best"]}
    assert len(nulls) == 3
