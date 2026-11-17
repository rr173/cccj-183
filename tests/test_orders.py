"""Scenario 1: orders with a known address.

Every customer with an order gets a Customer row; the key EGD makes one
address per customer.  Whether the engine first creates an unknown and then
reads ``城东`` from KnownAddress, or first copies ``城东`` and then sees the
unknown, the final instance must contain exactly one Customer row whose
address is the known ``城东`` -- no leftover unknown row.
"""


from chase.verify import Verifier
from tests.helpers import build, constants_only, instance_map, isomorphic

SCHEMA = {
    "Order": ["text", "text"],
    "Customer": ["text", "text"],
    "KnownAddress": ["text", "text"],
}
RULES = [
    {"id": "t_order_customer", "body": ["Order(o, c)"], "head": "Customer(c, a)"},
    {"id": "t_known_customer", "body": ["KnownAddress(c, a)"], "head": "Customer(c, a)"},
    {"id": "e_key", "body": ["Customer(c, a1)", "Customer(c, a2)"], "eq": ["a1", "a2"]},
]


def _customer_rows(inst):
    return inst["Customer"]


def test_two_orders_one_known_address():
    _, _, store, engine, result = build(
        SCHEMA, RULES,
        {"Order": [["o1", "c"], ["o2", "c"]], "KnownAddress": [["c", "城东"]]})
    assert result.status == "complete", result.detail
    inst = instance_map(store, SCHEMA)
    cust = _customer_rows(inst)
    assert cust == {("c", "城东")}
    # no live labeled null remains: the unknown was unified with a constant
    assert engine.export()["nulls"]["live"] == []
    # both orders share the one customer row
    assert inst["Order"] == {("o1", "c"), ("o2", "c")}


def test_known_address_first_still_converges():
    # Order facts are loaded after KnownAddress; the engine fires whichever
    # trigger it likes, but the result must be the same.
    for seed in (0, 1, 2, 7):
        _, _, store, engine, result = build(
            SCHEMA, RULES,
            {"KnownAddress": [["c", "城东"]], "Order": [["o1", "c"], ["o2", "c"]]},
            order_seed=seed)
        assert result.status == "complete", (seed, result.detail)
        inst = instance_map(store, SCHEMA)
        assert inst["Customer"] == {("c", "城东")}, (seed, inst["Customer"])


def test_two_customers_share_unknown_only_within_customer():
    _, _, store, engine, result = build(
        SCHEMA, RULES,
        {"Order": [["o1", "c"], ["o3", "d"]]})
    assert result.status == "complete"
    inst = instance_map(store, SCHEMA)
    rows = sorted(inst["Customer"], key=lambda r: r[0])
    assert [r[0] for r in rows] == ["c", "d"]
    assert rows[0][1] != rows[1][1]
    # each customer's unknown is a labeled null, not a constant and not shared
    from chase.model import LNullValue
    assert isinstance(rows[0][1], LNullValue)
    assert isinstance(rows[1][1], LNullValue)


def test_two_orders_for_unknown_customer_share_one_unknown():
    _, _, store, engine, result = build(
        SCHEMA, RULES,
        {"Order": [["o1", "c"], ["o2", "c"]]})
    assert result.status == "complete"
    inst = instance_map(store, SCHEMA)
    assert len(inst["Customer"]) == 1
    (row,) = inst["Customer"]
    assert row[0] == "c"
    from chase.model import LNullValue
    assert isinstance(row[1], LNullValue)
    # exactly one live unknown shared by the single customer row
    assert engine.export()["nulls"]["live"] == [row[1].id]


def test_known_conflicting_addresses_is_a_conflict():
    _, _, store, engine, result = build(
        SCHEMA, RULES,
        {"Order": [["o1", "c"], ["o2", "c"]],
         "KnownAddress": [["c", "城东"], ["c", "城西"]]})
    assert result.status == "conflict"
    c = result.conflict
    assert c.rule_id == "e_key"
    assert {c.left, c.right} == {("s", "城东"), ("s", "城西")}
    assert c.binding["c"] == ("s", "c")
    # both constants are traced to their input rows
    left_facts = [x["fact"] for x in c.left_sources if "fact" in x]
    right_facts = [x["fact"] for x in c.right_sources if "fact" in x]
    assert left_facts and right_facts
    assert {"KnownAddress(\"c\", \"城东\")"} == set(left_facts)
    assert {"KnownAddress(\"c\", \"城西\")"} == set(right_facts)


def test_conflict_is_trigger_order_independent():
    for seed in (0, 1, 5):
        _, _, store, engine, result = build(
            SCHEMA, RULES,
            {"Order": [["o1", "c"]], "KnownAddress": [["c", "城东"], ["c", "城西"]]},
            order_seed=seed)
        assert result.status == "conflict", seed
        assert {result.conflict.left, result.conflict.right} == {
            ("s", "城东"), ("s", "城西")}


def test_result_is_independent_of_trigger_order_up_to_null_bijection():
    facts = {"Order": [["o1", "c"], ["o2", "c"]],
             "KnownAddress": [["c", "城东"]]}
    results = []
    for seed in (0, 1, 2, 3):
        _, _, store, engine, result = build(SCHEMA, RULES, facts, order_seed=seed)
        assert result.status == "complete"
        results.append(instance_map(store, SCHEMA))
    for other in results[1:]:
        assert isomorphic(results[0], other)


def test_verifier_accepts_the_result():
    sch, rs, store, engine, result = build(
        SCHEMA, RULES,
        {"Order": [["o1", "c"], ["o2", "c"]], "KnownAddress": [["c", "城东"]]})
    v = Verifier(sch, rs, store,
                 {"Order": [("o1", "c"), ("o2", "c")],
                  "KnownAddress": [("c", "城东")]})
    report = v.run(labeled_null_ids=engine.export()["nulls"]["live"])
    assert report.ok, report.violations


def test_empty_input_is_complete_and_empty():
    _, _, store, engine, result = build(SCHEMA, RULES, {})
    assert result.status == "complete"
    inst = instance_map(store, SCHEMA)
    assert all(not rows for rows in inst.values())
