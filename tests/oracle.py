"""Independent finite evaluator used as an oracle in tests.

Nothing here uses the engine, the store, union-find, or the verifier.  Facts
are plain tuples; nulls are plain ``("?", id)`` markers; predicates are
evaluated by straightforward nested loops.  Two uses:

``satisfies(...)``   is a given finite instance a model of program + input?
                     (used to cross-check the engine's result with a completely
                     different evaluator);
``enumerate_models(..)``  brute-force all interpretations over a bounded
                     domain (tiny schemas only), so a test can assert that the
                     engine's result is one of them.
"""

from __future__ import annotations

import itertools
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple

from chase.model import Atom, Const, LNullValue, Rule, Schema, Var

Row = Tuple[object, ...]
Instance = Dict[str, Set[Row]]
NULL_MARK = ("?",)


def norm(value: object) -> object:
    """Canonical token: ('c', text) / ('d', int) / ('?', id)."""
    if isinstance(value, LNullValue):
        return ("?", value.id)
    if isinstance(value, str):
        return ("c", value)
    if isinstance(value, int) and not isinstance(value, bool):
        return ("d", value)
    if isinstance(value, tuple) and value and value[0] == "?":
        return value
    raise ValueError(f"bad value {value!r}")


def norm_instance(inst: Dict[str, Iterable[Sequence[object]]]) -> Instance:
    return {rel: {tuple(norm(v) for v in row) for row in rows}
            for rel, rows in inst.items()}


def term_value(term: object, binding: Dict[str, object]) -> object:
    if isinstance(term, Const):
        return ("c", term.value) if term.kind == "string" else ("d", term.value)
    return binding[term.name]


def bindings(rule: Rule, inst: Instance) -> Iterator[Tuple[Dict[str, object], List[Row]]]:
    def rec(i: int, b: Dict[str, object], support: List[Row]):
        if i == len(rule.body):
            yield b, support
            return
        atom = rule.body[i]
        for row in sorted(inst.get(atom.rel, ()), key=repr):
            nb = dict(b)
            ok = True
            for t, v in zip(atom.terms, row):
                if isinstance(t, Const):
                    if term_value(t, {}) != v:
                        ok = False
                        break
                else:
                    prev = nb.get(t.name)
                    if prev is not None and prev != v:
                        ok = False
                        break
                    nb[t.name] = v
            if ok:
                yield from rec(i + 1, nb, support + [row])

    yield from rec(0, {}, [])


def satisfies_tgd(rule: Rule, inst: Instance) -> Tuple[bool, Optional[dict]]:
    assert rule.head is not None
    for b, support in bindings(rule, inst):
        found = False
        for row in inst.get(rule.head.rel, ()):
            ok = True
            seen = {}
            for t, v in zip(rule.head.terms, row):
                if isinstance(t, Const):
                    if term_value(t, b) != v:
                        ok = False
                        break
                elif t.name in b:
                    if b[t.name] != v:
                        ok = False
                        break
                else:
                    if t.name in seen and seen[t.name] != v:
                        ok = False
                        break
                    seen[t.name] = v
            if ok:
                found = True
                break
        if not found:
            return False, {"rule": rule.id, "binding": repr(b), "support": support}
    return True, None


def satisfies_egd(rule: Rule, inst: Instance) -> Tuple[bool, Optional[dict]]:
    for b, support in bindings(rule, inst):
        if term_value(rule.eq_left, b) != term_value(rule.eq_right, b):
            return False, {"rule": rule.id, "binding": repr(b), "support": support}
    return True, None


def check_instance(schema: Schema, rules: Sequence[Rule], input_inst: Instance,
                   inst: Instance) -> Tuple[bool, Optional[dict]]:
    for rel, rows in input_inst.items():
        missing = set(rows) - set(inst.get(rel, ()))
        if missing:
            return False, {"input_missing": sorted(missing, key=repr)}
    for rel in inst:
        if rel not in schema.relations:
            return False, {"undeclared_relation": rel}
    for rule in rules:
        if rule.kind == "tgd":
            ok, why = satisfies_tgd(rule, inst)
        else:
            ok, why = satisfies_egd(rule, inst)
        if not ok:
            return False, why
    return True, None


def active_domain(input_inst: Instance) -> List[object]:
    vals = []
    for rows in input_inst.values():
        for row in rows:
            for v in row:
                if v not in vals:
                    vals.append(v)
    return vals


def enumerate_models(schema: Schema, rules: Sequence[Rule], input_inst: Instance,
                     fresh: int) -> Iterator[Instance]:
    """All models over the fixed domain (input values + ``fresh`` placeholders).

    Relations that occur only in rule bodies (and in the input) are fixed to
    exactly the input rows: no rule can add to them, and a model need not.
    Only relations that appear in some rule head are enumerated.  The domain
    is deliberately small in tests: every value the engine could invent must
    be one of the ``fresh`` placeholders, so this only makes sense for
    scenarios where the chase creates at most ``fresh`` unknowns.
    """
    derived = {r.head.rel for r in rules if r.kind == "tgd" and r.head is not None}
    fixed_rels = [rel for rel in schema.relations if rel not in derived]
    enum_rels = [rel for rel in schema.relations if rel in derived]
    domain = active_domain(input_inst) + [("?", f"f{i}") for i in range(fresh)]
    fixed: Instance = {rel: set(input_inst.get(rel, ())) for rel in fixed_rels}
    row_spaces = [[tuple(p) for p in itertools.product(domain, repeat=len(schema.columns(rel)))]
                  for rel in enum_rels]
    sizes = [len(rows) for rows in row_spaces]
    for masks in itertools.product(*[range(1 << s) for s in sizes]):
        inst: Instance = {rel: set(rows) for rel, rows in fixed.items()}
        for rel, rows, mask in zip(enum_rels, row_spaces, masks):
            inst[rel] = {row for i, row in enumerate(rows) if mask >> i & 1}
        ok, _why = check_instance(schema, rules, input_inst, inst)
        if ok:
            yield inst


def homomorphism_exists(source: Instance, target: Instance,
                        max_search: int = 200_000) -> Tuple[bool, Optional[Dict[str, object]]]:
    """Is there a map h with h(constant)=constant and h(source facts) ⊆ target?"""
    nulls: List[str] = []
    for rows in source.values():
        for row in rows:
            for v in row:
                if v[0] == "?" and v[1] not in nulls:
                    nulls.append(v[1])
    target_vals: List[object] = []
    for rows in target.values():
        for row in rows:
            for v in row:
                if v not in target_vals:
                    target_vals.append(v)
    nulls.sort()
    searched = 0
    for combo in itertools.product(target_vals, repeat=len(nulls)):
        searched += 1
        if searched > max_search:
            raise RuntimeError("homomorphism search too large")
        mapping: Dict[str, object] = dict(zip(nulls, combo))

        def img(v: object) -> object:
            return mapping[v[1]] if v[0] == "?" else v

        ok = True
        for rel, rows in source.items():
            trows = target.get(rel, set())
            for row in rows:
                if tuple(img(v) for v in row) not in trows:
                    ok = False
                    break
            if not ok:
                break
        if ok:
            return True, mapping
    return False, None


def redundant_facts(schema: Schema, rules: Sequence[Rule], input_inst: Instance,
                    inst: Instance) -> List[Tuple[str, Row]]:
    """Derived facts whose removal still leaves a model (gratuitous facts)."""
    from collections import defaultdict
    redundant = []
    input_rows = {(rel, row) for rel, rows in input_inst.items() for row in rows}
    for rel, rows in inst.items():
        for row in rows:
            if (rel, row) in input_rows:
                continue
            trial = {r: set(rs) for r, rs in inst.items()}
            trial[rel].discard(row)
            indexed = _index(trial)
            ok = _satisfies_indexed(schema, rules, indexed)
            if ok:
                redundant.append((rel, row))
    return redundant


class _Indexed:
    def __init__(self, inst: Instance, schema: Schema) -> None:
        self.inst = inst
        self.by_pos: Dict[Tuple[str, int, object], Set[Row]] = {}
        for rel, rows in inst.items():
            for row in rows:
                for i, v in enumerate(row):
                    self.by_pos.setdefault((rel, i, v), set()).add(row)

    def rows(self, rel: str) -> Set[Row]:
        return self.inst.get(rel, set())

    def candidates(self, rel: str, fixed: Dict[int, object]) -> Iterable[Row]:
        if not fixed:
            return self.rows(rel)
        best: Optional[Set[Row]] = None
        for i, v in fixed.items():
            cand = self.by_pos.get((rel, i, v), set())
            if best is None or len(cand) < len(best):
                best = cand
        assert best is not None
        return best


def _index(inst: Instance) -> _Indexed:
    return _Indexed(inst, None)  # type: ignore[arg-type]


def _satisfies_indexed(schema: Schema, rules: Sequence[Rule], idx: _Indexed) -> bool:
    from typing import Iterator as _It

    def bindings(rule: Rule):
        def rec(i, b, support):
            if i == len(rule.body):
                yield b
                return
            atom = rule.body[i]
            fixed = {}
            for pos, t in enumerate(atom.terms):
                if isinstance(t, Const):
                    fixed[pos] = term_value(t, {})
                elif t.name in b:
                    fixed[pos] = b[t.name]
            for row in idx.candidates(atom.rel, fixed):
                nb = dict(b)
                ok = True
                for t, v in zip(atom.terms, row):
                    if isinstance(t, Const):
                        if term_value(t, {}) != v:
                            ok = False
                            break
                    else:
                        prev = nb.get(t.name)
                        if prev is not None and prev != v:
                            ok = False
                            break
                        nb[t.name] = v
                if ok:
                    yield from rec(i + 1, nb, support + [row])

        yield from rec(0, {}, [])

    for rule in rules:
        if rule.kind == "tgd":
            assert rule.head is not None
            for b in bindings(rule):
                fixed = {}
                for pos, t in enumerate(rule.head.terms):
                    if isinstance(t, Const):
                        fixed[pos] = term_value(t, b)
                    elif t.name in b:
                        fixed[pos] = b[t.name]
                ok = False
                for row in idx.candidates(rule.head.rel, fixed):
                    if all(
                        (isinstance(t, Const) and term_value(t, b) == v)
                        or (isinstance(t, Var) and t.name in b and b[t.name] == v)
                        or (isinstance(t, Var) and t.name not in b)
                        for t, v in zip(rule.head.terms, row)):
                        ok = True
                        break
                if not ok:
                    return False
        else:
            for b in bindings(rule):
                if term_value(rule.eq_left, b) != term_value(rule.eq_right, b):
                    return False
    return True
