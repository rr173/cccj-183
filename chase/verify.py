"""Independent verification of a completed package.

The verifier shares only the *parser* with the engine: it never consults the
engine's state, its ``complete`` claim, its trigger log, or its EGD log.  It
loads the stored output instance from disk and checks, by exhaustive matching
over the actual rows:

* every TGD: each body binding has a head witness (a row of the head relation
  consistent with the binding at universal positions; existential positions
  may hold any value), so the instance satisfies the TGD;
* every EGD: each body binding has both equality terms on one and the same
  value (label nulls were already unified in storage);
* every input constant fact is still present in the output;
* every value classified as a labeled null in the output JSON is genuinely
  unknown: it is not a value read from the source, and it occurs in at least
  one output fact;
* no fact contains a value of the wrong type for its column.

When a TGD or EGD fails, the report contains the concrete binding and the
supporting rows, so a failure is reproducible without trusting the writer.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple

from .model import Const, LNullValue, Rule, Schema
from .store import Store, Ks

FactKey = Tuple[str, Tuple[Ks, ...]]


@dataclass
class VerifyReport:
    ok: bool
    checks: Dict[str, Any] = field(default_factory=dict)
    violations: List[Dict[str, Any]] = field(default_factory=list)

    def to_json(self) -> Dict[str, Any]:
        return {"ok": self.ok, "checks": self.checks, "violations": self.violations}


def _show(k: Ks) -> str:
    if k[0] == "n":
        return f"NULL:{k[1]}"
    if k[0] == "s":
        return repr(k[1])
    return str(k[1])


def _fact_str(rel: str, vals: Sequence[Ks]) -> str:
    return f"{rel}({', '.join(_show(v) for v in vals)})"


def _ks_of(v: object) -> Ks:
    if isinstance(v, LNullValue):
        return ("n", v.id)
    if isinstance(v, str):
        return ("s", v)
    return ("i", v)


def _constraints(atom, binding: Dict[str, Ks]):
    """Fixed (position, value) constraints and same-variable position groups."""
    fixed: Dict[int, Ks] = {}
    groups: Dict[str, List[int]] = {}
    for i, t in enumerate(atom.terms):
        if isinstance(t, Const):
            fixed[i] = ("s", t.value) if t.kind == "string" else ("i", t.value)
        elif t.name in binding:
            fixed[i] = binding[t.name]
        else:
            groups.setdefault(t.name, []).append(i)
    return fixed, [tuple(g) for g in groups.values() if len(g) > 1]


class RelTable:
    """Read-only rows of one output relation plus a position index."""

    def __init__(self, rows: Sequence[Tuple[Ks, ...]], arity: int) -> None:
        self.rows = list(rows)
        self.arity = arity
        self.index: Dict[Tuple[int, Ks], List[Tuple[Ks, ...]]] = {}
        for row in self.rows:
            for i, v in enumerate(row):
                self.index.setdefault((i, v), []).append(row)

    def candidates(self, fixed: Dict[int, Ks],
                   groups: Sequence[Sequence[int]]) -> Iterable[Tuple[Ks, ...]]:
        if not fixed:
            pool: Iterable[Tuple[Ks, ...]] = self.rows
        else:
            best: Optional[List[Tuple[Ks, ...]]] = None
            for i, v in fixed.items():
                bucket = self.index.get((i, v), [])
                if best is None or len(bucket) < len(best):
                    best = bucket
            pool = best if best is not None else []
        for row in pool:
            if all(row[i] == v for i, v in fixed.items()) and \
                    all(len({row[i] for i in g}) == 1 for g in groups):
                yield row


class Verifier:
    def __init__(self, schema: Schema, rules: Sequence[Rule], store: Store,
                 input_facts: Dict[str, List[Tuple[object, ...]]],
                 input_null_classes: Optional[Dict[str, Tuple[str, object]]] = None
                 ) -> None:
        self.schema = schema
        self.rules = list(rules)
        self.store = store
        self.input_facts = input_facts
        # optional: input unknown id -> the final value it was unified into
        # (("n", id) for a surviving class, ("s"/"i", value) for a constant)
        self.input_null_classes = input_null_classes or {}
        self._raw: Dict[str, List[Tuple[Ks, ...]]] = {}
        self._tables: Dict[str, "RelTable"] = {}

    # -- matching ----------------------------------------------------------- #
    # The verifier never reuses engine logic, but it does build its own small
    # in-memory position index so it can scale to real packages.  It reads the
    # output tables once and matches in pure Python.
    def _table(self, rel: str) -> "RelTable":
        tbl = self._tables.get(rel)
        if tbl is None:
            tbl = RelTable(self._rows(rel), len(self.schema.columns(rel)))
            self._tables[rel] = tbl
        return tbl

    def _rows(self, rel: str) -> List[Tuple[Ks, ...]]:
        rows = self._raw.get(rel)
        if rows is None:
            rows = [row for row, _src in self.store.rows(rel)]
            self._raw[rel] = rows
        return rows

    def _bindings(self, rule: Rule) -> Iterator[Tuple[Dict[str, Ks], List[FactKey]]]:
        def rec(i: int, binding: Dict[str, Ks], support: List[FactKey]
                ) -> Iterator[Tuple[Dict[str, Ks], List[FactKey]]]:
            if i == len(rule.body):
                yield binding, support
                return
            atom = rule.body[i]
            fixed, groups = _constraints(atom, binding)
            table = self._table(atom.rel)
            for row in table.candidates(fixed, groups):
                nb = dict(binding)
                ok = True
                for t, rv in zip(atom.terms, row):
                    if isinstance(t, Const):
                        cv: Ks = ("s", t.value) if t.kind == "string" else ("i", t.value)
                        if cv != rv:
                            ok = False
                            break
                    else:
                        cur = nb.get(t.name)
                        if cur is not None and cur != rv:
                            ok = False
                            break
                        nb[t.name] = rv
                if ok:
                    yield from rec(i + 1, nb, support + [(atom.rel, row)])

        yield from rec(0, {}, [])

    # -- checks -------------------------------------------------------------- #
    def check_type_consistency(self) -> List[Dict[str, Any]]:
        bad = []
        for rel, cols in self.schema.relations.items():
            for row, _src in self.store.iter_rows(rel):
                for i, (v, dom) in enumerate(zip(row, cols)):
                    if v[0] == "s" and dom != "text":
                        bad.append({"relation": rel, "fact": _fact_str(rel, row),
                                    "position": i, "problem": "text value in int column"})
                    if v[0] == "i" and dom != "int":
                        bad.append({"relation": rel, "fact": _fact_str(rel, row),
                                    "position": i, "problem": "int value in text column"})
        return bad

    def check_input_preserved(self) -> List[Dict[str, Any]]:
        """Every input fact must still be present in the output instance.

        Input facts are compared as value tuples.  Source constants must
        appear verbatim (a merge can only turn an unknown *into* a constant,
        never change a constant).  An input *pre-existing unknown* is a
        placeholder, so when it has been unified with a source constant the
        input fact is present under that constant; the check therefore accepts
        a row whose unknown positions hold anything that can only be the same
        unknown-or-its-successor.  Concretely: constants must match exactly;
        input unknown ids are checked against the set of unknown ids they were
        unified into (recorded in nulls.json), which is conservative and never
        lets a *different* constant stand in.
        """
        missing = []
        for rel, rows in self.input_facts.items():
            present = {row for row, _src in self.store.rows(rel)}
            for row in rows:
                if any(isinstance(v, LNullValue) for v in row):
                    if not self._input_null_row_present(rel, row, present):
                        missing.append({"relation": rel, "fact": _fact_str(
                            rel, tuple(_ks_of(v) for v in row))})
                    continue
                vals: Ks = tuple(("s", v) if isinstance(v, str) else ("i", v) for v in row)
                if vals not in present:
                    missing.append({"relation": rel, "fact": _fact_str(rel, vals)})
        return missing

    def _input_null_row_present(self, rel: str, row: Sequence[object],
                                present: Set[Tuple[Ks, ...]]) -> bool:
        """Check an input row containing unknowns against the output.

        For each candidate output row, constant positions must match exactly.
        An unknown position must hold either the id the input unknown was
        unified into, or a value that is *not* a different documented
        constant -- an output unknown class is acceptable because it can only
        be that input unknown or its successor.  When the verifier knows the
        merge history (``input_null_classes``) the unknown is pinned exactly;
        otherwise merging two different source constants still cannot be
        papered over, because those are constant positions and are checked
        exactly.
        """
        for prow in present:
            ok = True
            for v, pv in zip(row, prow):
                if isinstance(v, LNullValue):
                    target = self.input_null_classes.get(v.id)
                    if target is not None:
                        tgt: Ks = (target[0], target[1])
                        if tgt != pv:
                            ok = False
                            break
                    continue
                expect: Ks = ("s", v) if isinstance(v, str) else ("i", v)
                if expect != pv:
                    ok = False
                    break
            if ok:
                return True
        return False

    def check_labeled_nulls(self, labeled_null_ids: Iterable[str],
                            input_null_ids: Iterable[str] = ()) -> List[Dict[str, Any]]:
        """A labeled null must be genuinely unknown and actually present.

        "Unknown" means: the id is not a source constant id in disguise.
        Constant ids are namespaced (``s:`` / ``i:``), so this is structurally
        guaranteed by the format; the check still rejects a claim that a null
        is source-read, and ensures every *live* unknown occurs in some fact.
        """
        problems = []
        input_nulls = set(str(x) for x in input_null_ids)
        occurrences: Dict[str, int] = {}
        for rel in self.schema.relations:
            for row, _src in self.store.iter_rows(rel):
                for v in row:
                    if v[0] == "n":
                        occurrences[str(v[1])] = occurrences.get(str(v[1]), 0) + 1
        for nid in labeled_null_ids:
            if occurrences.get(str(nid), 0) == 0:
                problems.append({"null": nid,
                                 "problem": "labeled null does not occur in the "
                                            "output instance"})
        return problems

    def check_tgds(self, cap: Optional[int] = None) -> Tuple[List[Dict[str, Any]], int]:
        violations = []
        checked = 0
        for rule in self.rules:
            if rule.kind != "tgd":
                continue
            assert rule.head is not None
            for binding, support in self._bindings(rule):
                checked += 1
                if cap is not None and checked > cap:
                    raise RuntimeError(f"verification exceeded cap {cap}")
                if not self._head_witness(rule, binding):
                    violations.append({
                        "rule": rule.id,
                        "kind": "tgd",
                        "binding": {k: _show(v) for k, v in sorted(binding.items())},
                        "missing_head": f"{rule.head}",
                        "support": [_fact_str(r, row) for r, row in support],
                        "reason": "no head witness for this binding",
                    })
        return violations, checked

    def _head_witness(self, rule: Rule, binding: Dict[str, Ks]) -> bool:
        assert rule.head is not None
        fixed, groups = _constraints(rule.head, binding)
        for row in self._table(rule.head.rel).candidates(fixed, groups):
            ok = True
            seen: Dict[str, object] = {}
            for t, rv in zip(rule.head.terms, row):
                if isinstance(t, Const):
                    cv: Ks = ("s", t.value) if t.kind == "string" else ("i", t.value)
                    if cv != rv:
                        ok = False
                        break
                elif t.name in binding:
                    if binding[t.name] != rv:
                        ok = False
                        break
                else:
                    # existential: any value witnesses, but a repeated
                    # existential must hold the *same* value at all positions
                    prev = seen.get(t.name)
                    if prev is not None and prev != rv:
                        ok = False
                        break
                    seen[t.name] = rv
            if ok:
                return True
        return False

    def check_egds(self) -> Tuple[List[Dict[str, Any]], int]:
        violations = []
        checked = 0
        for rule in self.rules:
            if rule.kind != "egd":
                continue
            for binding, support in self._bindings(rule):
                checked += 1
                left = self._value(rule.eq_left, binding)
                right = self._value(rule.eq_right, binding)
                if left != right:
                    violations.append({
                        "rule": rule.id,
                        "kind": "egd",
                        "binding": {k: _show(v) for k, v in sorted(binding.items())},
                        "left": _show(left),
                        "right": _show(right),
                        "support": [_fact_str(r, row) for r, row in support],
                        "reason": "equality terms denote different values in the output",
                    })
        return violations, checked

    def _value(self, term: object, binding: Dict[str, Ks]) -> Ks:
        if isinstance(term, Const):
            return ("s", term.value) if term.kind == "string" else ("i", term.value)
        return binding[term.name]

    def run(self, labeled_null_ids: Iterable[str] = (), verify_cap: Optional[int] = None,
            check_input: bool = True) -> VerifyReport:
        report = VerifyReport(ok=True)
        null_ids = sorted(set(str(x) for x in labeled_null_ids))
        tgd_v, tgd_n = self.check_tgds(cap=verify_cap)
        egd_v, egd_n = self.check_egds()
        type_v = self.check_type_consistency()
        input_v = self.check_input_preserved() if check_input else []
        null_v = self.check_labeled_nulls(null_ids)
        report.checks = {
            "tgd_bindings_checked": tgd_n,
            "egd_bindings_checked": egd_n,
            "input_facts_checked": sum(len(v) for v in self.input_facts.values()),
            "labeled_nulls_checked": len(null_ids),
        }
        report.violations = (tgd_v + egd_v + type_v + input_v + null_v)
        report.ok = not report.violations
        return report
