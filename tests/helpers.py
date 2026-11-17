"""Test helpers: run the engine in memory and compare instances up to null renaming."""

from __future__ import annotations

import itertools
from typing import Any, Dict, List, Sequence, Set, Tuple

from chase.chase import Budgets, Engine
from chase.model import LNullValue, Rule, Schema, parse_program, parse_value
from chase.store import Store


def build(schema: Dict[str, Sequence[str]], rules: List[Dict[str, Any]],
          facts: Dict[str, List[List[object]]], budgets: Budgets = None,
          order_seed: int = 0):
    sch = Schema({r: tuple(c) for r, c in schema.items()})
    rs = parse_program({"rules": rules}, sch)
    store = Store(":memory:")
    engine = Engine(sch, rs, store, budgets or Budgets(), order_seed=order_seed)
    engine.load_input({r: [tuple(parse_value(x) for x in row) for row in rows]
                       for r, rows in facts.items()})
    result = engine.run()
    return sch, rs, store, engine, result


def instance_map(store: Store, schema) -> Dict[str, Set[Tuple[object, ...]]]:
    """Final instance as {relation: set(rows)} with LNullValue values."""
    if isinstance(schema, Schema):
        relations = schema.relations
    elif isinstance(schema, dict):
        relations = schema.keys()
    else:
        relations = schema
    out: Dict[str, Set[Tuple[object, ...]]] = {}
    for rel in relations:
        rows = set()
        for row, _src in store.iter_rows(rel):
            vals = []
            for k in row:
                if k[0] == "n":
                    vals.append(LNullValue(str(k[1]), f"NULL:{k[1]}"))
                else:
                    vals.append(k[1])
            rows.add(tuple(vals))
        out[rel] = rows
    return out


def constants_only(inst: Dict[str, Set[Tuple[object, ...]]]) -> Dict[str, Set[Tuple[object, ...]]]:
    """Project away unknowns: replace each with a wildcard marker."""
    out: Dict[str, Set[Tuple[object, ...]]] = {}
    for rel, rows in inst.items():
        proj = set()
        for row in rows:
            proj.add(tuple(("NULL",) if isinstance(v, LNullValue) else ("VAL", v)
                           for v in row))
        out[rel] = proj
    return out


def _nulls_in(inst: Dict[str, Set[Tuple[object, ...]]]) -> List[str]:
    ids: Set[str] = set()
    for rows in inst.values():
        for row in rows:
            for v in row:
                if isinstance(v, LNullValue):
                    ids.add(v.id)
    return sorted(ids)


def isomorphic(inst_a: Dict[str, Set[Tuple[object, ...]]],
               inst_b: Dict[str, Set[Tuple[object, ...]]],
               max_nulls: int = 8) -> bool:
    """Are the two instances equal up to a bijection of labeled nulls?

    Source constants must match exactly and in the same positions.  A null in
    one instance must be matched by a null in the other, and the bijection is
    global (shared nulls stay shared, distinct nulls stay distinct).
    """
    if set(inst_a) != set(inst_b):
        return False
    for rel in inst_a:
        if len(inst_a[rel]) != len(inst_b[rel]):
            return False
    nulls_a = _nulls_in(inst_a)
    nulls_b = _nulls_in(inst_b)
    if len(nulls_a) != len(nulls_b):
        return False
    if len(nulls_a) > max_nulls:
        raise ValueError("too many nulls for exhaustive isomorphism check")
    rows_a = _flat(inst_a)
    rows_b = _flat(inst_b)
    for perm in itertools.permutations(nulls_b):
        mapping = dict(zip(nulls_a, perm))
        mapped = set()
        for rel, row in rows_a:
            mapped.add((rel, tuple(("N", mapping[v.id]) if isinstance(v, LNullValue)
                                   else ("C", v) for v in row)))
        target = set()
        for rel, row in rows_b:
            target.add((rel, tuple(("N", v.id) if isinstance(v, LNullValue)
                                   else ("C", v) for v in row)))
        if mapped == target:
            return True
    return False


def _flat(inst: Dict[str, Set[Tuple[object, ...]]]
          ) -> List[Tuple[str, Tuple[object, ...]]]:
    return [(rel, row) for rel, rows in inst.items() for row in rows]
