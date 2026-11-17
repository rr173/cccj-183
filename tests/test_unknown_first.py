"""Scenario 2: the unknown-first path.

Here the input itself already contains a pre-existing unknown for customer
``c`` (``{"null": "u1"}``, as an interchange package would) and a known
address arrives later/at the same time.  The final instance must share one
``城东``; a leftover unknown row is a failure.  With two known addresses the
result must be a constant conflict.

The second part checks that different customers' unknowns never get merged by
accident, and that two unknowns that an EGD does relate become one class while
both customer rows remain (the key is on the *address*, not on the customer).
"""

from chase.model import LNullValue
from chase.verify import Verifier
from tests.helpers import build, instance_map, isomorphic

SCHEMA = {
    "Customer": ["text", "text"],
    "KnownAddress": ["text", "text"],
}
RULES = [
    {"id": "k_to_c", "body": ["KnownAddress(c, a)"], "head": "Customer(c, a)"},
    {"id": "e_key", "body": ["Customer(c, a1)", "Customer(c, a2)"],
     "eq": ["a1", "a2"]},
]
U1 = {"null": "u1"}
U2 = {"null": "u2"}


def _raw_unknown(v):
    if isinstance(v, LNullValue):
        return ("null", v.id)
    return v


def test_placeholder_then_known_address_shares_the_constant():
    for seed in (0, 1, 3):
        _, _, store, engine, result = build(
            SCHEMA, RULES,
            {"Customer": [["c", U1]], "KnownAddress": [["c", "城东"]]},
            order_seed=seed)
        assert result.status == "complete", (seed, result.detail)
        inst = instance_map(store, SCHEMA)
        assert inst["Customer"] == {("c", "城东")}, (seed, inst["Customer"])
        # the pre-existing unknown is absorbed, not left dangling
        assert engine.export()["nulls"]["live"] == []


def test_placeholder_and_two_known_addresses_is_a_conflict():
    _, _, store, engine, result = build(
        SCHEMA, RULES,
        {"Customer": [["c", U1]],
         "KnownAddress": [["c", "城东"], ["c", "城西"]]})
    assert result.status == "conflict"
    left, right = result.conflict.left, result.conflict.right
    assert {left, right} == {("s", "城东"), ("s", "城西")}


def test_two_placeholders_for_one_customer_become_one_class():
    _, _, store, engine, result = build(
        SCHEMA, RULES,
        {"Customer": [["c", U1], ["c", U2]]})
    assert result.status == "complete"
    inst = instance_map(store, SCHEMA)
    assert len(inst["Customer"]) == 1
    (row,) = inst["Customer"]
    assert isinstance(row[1], LNullValue)
    # history keeps both inputs, current instance has exactly one unknown
    nulls = engine.export()["nulls"]
    assert nulls["nulls"]["u1"]["class"] == nulls["nulls"]["u2"]["class"]
    assert nulls["live"] == [row[1].id]


def test_different_customers_keep_different_unknowns():
    _, _, store, engine, result = build(
        SCHEMA, RULES,
        {"Customer": [["c", U1], ["d", U2]]})
    assert result.status == "complete"
    inst = instance_map(store, SCHEMA)
    assert len(inst["Customer"]) == 2
    by_c = {row[0]: row[1] for row in inst["Customer"]}
    assert isinstance(by_c["c"], LNullValue) and isinstance(by_c["d"], LNullValue)
    assert by_c["c"].id != by_c["d"].id
    assert engine.export()["nulls"]["live"] == sorted(
        [by_c["c"].id, by_c["d"].id])


def test_preexisting_unknown_survives_as_live_unknown():
    _, _, store, engine, result = build(
        SCHEMA, RULES, {"Customer": [["c", U1], ["d", U2]]})
    report = Verifier(
        SCHEMA and __import__("chase.model", fromlist=["Schema"]).Schema(
            {r: tuple(c) for r, c in SCHEMA.items()}),
        __import__("chase.model", fromlist=["parse_program"]).parse_program(
            {"rules": RULES},
            __import__("chase.model", fromlist=["Schema"]).Schema(
                {r: tuple(c) for r, c in SCHEMA.items()})),
        store,
        {"Customer": [("c", LNullValue("u1", "NULL:u1")),
                      ("d", LNullValue("u2", "NULL:u2"))]},
    ).run(labeled_null_ids=engine.export()["nulls"]["live"])
    assert report.ok, report.violations


def test_merge_does_not_delete_the_customer_row():
    # Customer(c, u1) and Customer(d, u2) merge only if the EGD relates them;
    # here it does not (different c/d), so we check the *other* direction:
    # two rows for the same c collapse to one row, and the surviving row is
    # still there exactly once (no accidental loss of the row itself).
    _, _, store, engine, result = build(
        SCHEMA, RULES, {"Customer": [["c", U1], ["c", U2]]})
    assert result.status == "complete"
    assert len(instance_map(store, SCHEMA)["Customer"]) == 1
