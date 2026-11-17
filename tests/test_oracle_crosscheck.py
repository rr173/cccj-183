"""Cross-checks against the independent finite evaluator.

For each scenario the engine's output must be a model according to a
completely separate evaluator (plain sets of tuples, no SQL, no union-find),
must contain every input fact, and must not contain facts that can be dropped
while still remaining a model (the chase creates no gratuitous rows).

The exhaustive enumeration is used on the smallest scenario, where there can
be at most one fresh unknown: it shows the engine result is one of the
*models* of the program, not merely a fixpoint of the engine.
"""

from chase.model import LNullValue, Schema, parse_program
from tests import oracle
from tests.helpers import build, instance_map


def _as_instance(inst):
    return {rel: {tuple(v) for v in rows} for rel, rows in inst.items()}


def _oracle(schema, rules, input_rows):
    sch = Schema({r: tuple(c) for r, c in schema.items()})
    rs = parse_program({"rules": rules}, sch)
    return sch, rs, oracle.norm_instance(input_rows)


def _norms(inst):
    return oracle.norm_instance(_as_instance(inst))


CASES = [
    # (name, schema, rules, facts, max_fresh_to_enumerate_or_None)
    ("one_customer_known",
     {"Order": ["text", "text"], "Customer": ["text", "text"],
      "KnownAddress": ["text", "text"]},
     [{"id": "t1", "body": ["Order(o, c)"], "head": "Customer(c, a)"},
      {"id": "t2", "body": ["KnownAddress(c, a)"], "head": "Customer(c, a)"},
      {"id": "e", "body": ["Customer(c, a1)", "Customer(c, a2)"], "eq": ["a1", "a2"]}],
     {"Order": [["o1", "c"]], "KnownAddress": [["c", "东"]]},
     0),
    ("two_orders_no_address",
     {"Order": ["text", "text"], "Customer": ["text", "text"]},
     [{"id": "t1", "body": ["Order(o, c)"], "head": "Customer(c, a)"},
      {"id": "e", "body": ["Customer(c, a1)", "Customer(c, a2)"], "eq": ["a1", "a2"]}],
     {"Order": [["o1", "c"], ["o2", "c"]]},
     1),
    ("int_keys",
     {"Edge": ["int", "int"], "Path": ["int", "int"], "Weight": ["int", "int"],
      "Best": ["int", "int"]},
     [{"id": "t1", "body": ["Edge(x, y)"], "head": "Path(x, y)"},
      {"id": "t2", "body": ["Path(x, y)", "Weight(y, w)"], "head": "Best(x, w)"},
      {"id": "e", "body": ["Best(x, w1)", "Best(x, w2)"], "eq": ["w1", "w2"]}],
     {"Edge": [[1, 2], [2, 3], [3, 1]],
      "Weight": [[2, 7], [3, 9], [1, 4]]},
     None),
]


def test_engine_result_is_a_model_of_the_program():
    for name, schema, rules, facts, _fresh in CASES:
        _, _, store, engine, result = build(schema, rules, facts)
        assert result.status == "complete", (name, result.detail)
        inst = _norms(instance_map(store, schema))
        sch, rs, input_inst = _oracle(schema, rules, facts)
        ok, why = oracle.check_instance(sch, rs, input_inst, inst)
        assert ok, (name, why)


def test_engine_adds_no_gratuitous_facts():
    for name, schema, rules, facts, _fresh in CASES:
        _, _, store, engine, result = build(schema, rules, facts)
        assert result.status == "complete", name
        inst = _norms(instance_map(store, schema))
        sch, rs, input_inst = _oracle(schema, rules, facts)
        redundant = oracle.redundant_facts(sch, rs, input_inst, inst)
        assert not redundant, (name, redundant)


def test_engine_result_is_a_universal_model_of_the_enumerated_models():
    # Smallest scenario only.  The chase result is a canonical universal
    # model: it must be a model itself, and it must map homomorphically into
    # *every* model the oracle enumerates (constants fixed, unknowns free).
    for name, schema, rules, facts, fresh in CASES:
        if fresh is None or fresh > 1:
            continue
        _, _, store, engine, result = build(schema, rules, facts)
        assert result.status == "complete"
        inst = _norms(instance_map(store, schema))
        sch, rs, input_inst = _oracle(schema, rules, facts)
        models = list(oracle.enumerate_models(sch, rs, input_inst, fresh))
        assert models, f"{name}: oracle found no model (domain too small?)"
        for m in models:
            ok, mapping = oracle.homomorphism_exists(inst, m)
            assert ok, f"{name}: no homomorphism into enumerated model {m}"


def _iso(a, b):
    if set(a) != set(b):
        return False
    nulls_a = sorted({v[1] for rows in a.values() for row in rows for v in row
                      if v[0] == "?"})
    nulls_b = sorted({v[1] for rows in b.values() for row in rows for v in row
                      if v[0] == "?"})
    if len(nulls_a) != len(nulls_b):
        return False
    import itertools
    for perm in itertools.permutations(nulls_b):
        m = dict(zip(nulls_a, perm))

        def img(v):
            return m[v[1]] if v[0] == "?" else v

        mapped = {rel: {tuple(img(v) for v in row) for row in rows}
                  for rel, rows in a.items()}
        if mapped == b:
            return True
    return False


def test_result_is_least_fixpoint_on_recursion():
    # transitive closure must be complete (not a partial closure)
    schema = {"Edge": ["int", "int"], "Path": ["int", "int"]}
    rules = [{"id": "copy", "body": ["Edge(x, y)"], "head": "Path(x, y)"},
             {"id": "step", "body": ["Path(x, y)", "Edge(y, z)"],
              "head": "Path(x, z)"}]
    _, _, store, engine, result = build(
        schema, rules, {"Edge": [[1, 2], [2, 3], [3, 4]]})
    assert result.status == "complete"
    inst = instance_map(store, schema)
    paths = {(r[0], r[1]) for r in inst["Path"]}
    assert paths == {(1, 2), (2, 3), (3, 4), (1, 3), (2, 4), (1, 4)}
