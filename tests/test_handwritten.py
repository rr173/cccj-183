"""Hand-written expected instances for the required drill cases.

Each case states the *exact* final instance by hand (with placeholders written
as ``U1``/``U2`` for distinct unknowns and ``SAME`` for "whatever the engine
chose, but the same value").  This pins the semantics independently of the
engine's internal ids.
"""

from chase.model import LNullValue
from tests.helpers import build, instance_map


def _rows(inst, rel):
    out = set()
    for row in inst[rel]:
        vals = []
        for v in row:
            if isinstance(v, LNullValue):
                vals.append("UNKNOWN")
            else:
                vals.append(v)
        out.add(tuple(vals))
    return out


ORDERS_SCHEMA = {
    "Order": ["text", "text"],
    "Customer": ["text", "text"],
    "KnownAddress": ["text", "text"],
}
ORDERS_RULES = [
    {"id": "t1", "body": ["Order(o, c)"], "head": "Customer(c, a)"},
    {"id": "t2", "body": ["KnownAddress(c, a)"], "head": "Customer(c, a)"},
    {"id": "e", "body": ["Customer(c, a1)", "Customer(c, a2)"], "eq": ["a1", "a2"]},
]


def test_handwritten_known_address_instance():
    _, _, store, engine, result = build(
        ORDERS_SCHEMA, ORDERS_RULES,
        {"Order": [["o1", "c"], ["o2", "c"]], "KnownAddress": [["c", "城东"]]})
    assert result.status == "complete"
    inst = instance_map(store, ORDERS_SCHEMA)
    # hand-written expectation, value for value
    assert inst["Order"] == {("o1", "c"), ("o2", "c")}
    assert inst["KnownAddress"] == {("c", "城东")}
    assert inst["Customer"] == {("c", "城东")}


def test_handwritten_unknown_instance():
    _, _, store, engine, result = build(
        ORDERS_SCHEMA, ORDERS_RULES, {"Order": [["o1", "c"], ["o2", "c"]]})
    assert result.status == "complete"
    inst = instance_map(store, ORDERS_SCHEMA)
    assert inst["Order"] == {("o1", "c"), ("o2", "c")}
    assert len(inst["Customer"]) == 1
    (row,) = inst["Customer"]
    assert row[0] == "c"
    assert isinstance(row[1], LNullValue)
    # hand-written: exactly one unknown, referenced by exactly one row
    assert sum(1 for rows in inst.values() for r in rows for v in r
               if isinstance(v, LNullValue)) == 1


def test_handwritten_two_customers_two_unknowns():
    _, _, store, engine, result = build(
        ORDERS_SCHEMA, ORDERS_RULES,
        {"Order": [["o1", "c"], ["o2", "c"], ["o3", "d"], ["o4", "d"]]})
    assert result.status == "complete"
    inst = instance_map(store, ORDERS_SCHEMA)
    rows = sorted(inst["Customer"], key=lambda r: r[0])
    assert [r[0] for r in rows] == ["c", "d"]
    assert isinstance(rows[0][1], LNullValue) and isinstance(rows[1][1], LNullValue)
    assert rows[0][1].id != rows[1][1].id


def test_handwritten_conflict():
    _, _, store, engine, result = build(
        ORDERS_SCHEMA, ORDERS_RULES,
        {"Order": [["o1", "c"]],
         "KnownAddress": [["c", "城东"], ["c", "城西"]]})
    assert result.status == "conflict"
    c = result.conflict
    assert c.binding == {"c": ("s", "c"), "a1": ("s", "城东"), "a2": ("s", "城西")} or \
           c.binding == {"c": ("s", "c"), "a1": ("s", "城西"), "a2": ("s", "城东")}
    assert {c.left, c.right} == {("s", "城东"), ("s", "城西")}


def test_handwritten_input_placeholder_and_known_address():
    _, _, store, engine, result = build(
        {"Customer": ["text", "text"], "KnownAddress": ["text", "text"]},
        [{"id": "k", "body": ["KnownAddress(c, a)"], "head": "Customer(c, a)"},
         {"id": "e", "body": ["Customer(c, a1)", "Customer(c, a2)"],
          "eq": ["a1", "a2"]}],
        {"Customer": [["c", {"null": "u1"}]],
         "KnownAddress": [["c", "城东"]]})
    assert result.status == "complete"
    inst = instance_map(store, {"Customer", "KnownAddress"})
    assert inst["Customer"] == {("c", "城东")}
    assert inst["KnownAddress"] == {("c", "城东")}
