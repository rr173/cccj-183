"""The completion engine: typed TGD/EGD chase with labeled nulls and unification.

Semantics
---------
* A TGD is applied in the *standard* way: for one body binding, if an
  assignment of the existential head variables already makes the head atom
  present, nothing is created (the trigger is satisfied).  Otherwise one fresh
  labeled null is created per existential variable *of this firing*, and all
  occurrences of that variable in the head share it.
* A labeled null is a constant of the instance.  EGDs unify nulls: two nulls
  join one equivalence class; a null joined with a source constant *becomes*
  that constant.  Two different source constants are never unified -- that is a
  conflict, reported with the actual binding, both constants, and the source
  paths supporting them.
* The engine runs in rounds: TGD fixpoint, then one round of EGD checks.  Every
  round that unifies anything rebuilds the stored relations through the
  canonical value map (this de-duplicates facts and re-points every old and
  newly derived row), then feeds the changed rows back into the TGD queue,
  because merging can enable *new* body joins.  The run stops only when a TGD
  pass adds nothing and an EGD pass unifies nothing -- i.e. at real constraint
  satisfaction, not at a rewritten export.

Why an existing head witness survives unification: relations are only ever
rewritten by a value map, so no tuple disappears -- it is replaced by its
canonical form.  A witness "there is some value filling the existential
position" remains a witness after its value is unified with anything else,
including a source constant.  Hence finished triggers never need to run again;
what *can* change is which body joins exist, and those are re-queued.

Termination
-----------
Weak acyclicity of the TGDs (checked independently in :mod:`chase.model`)
bounds the generation of new unknowns when no merging happens.  Merging can,
in principle, revive body joins; every budget is therefore enforced and an
exhausted run is reported as ``incomplete`` -- never as a complete
completion.  See ``docs/DESIGN.md``.
"""

from __future__ import annotations

import json
import resource
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple

from .model import (
    Atom,
    BudgetExceeded,
    Const,
    LNullValue,
    ModelError,
    Rule,
    Schema,
    Var,
)
from .store import Store, Ks
from .unify import ConstantConflict, Unifier

FactKey = Tuple[str, Tuple[Ks, ...]]


# --------------------------------------------------------------------------- #
# Budgets / results
# --------------------------------------------------------------------------- #
@dataclass
class Budgets:
    max_facts: int = 500_000
    max_triggers: int = 200_000
    max_probes: int = 5_000_000
    max_seconds: float = 120.0
    max_match_rows: int = 200_000
    max_rounds: int = 10_000


@dataclass
class Conflict:
    rule_id: str
    binding: Dict[str, Ks]
    left: Ks
    right: Ks
    support: List[FactKey]
    left_sources: List[Dict[str, Any]] = field(default_factory=list)
    right_sources: List[Dict[str, Any]] = field(default_factory=list)
    explanation: str = ""


@dataclass
class RunResult:
    status: str  # complete | conflict | incomplete | error
    stats: Dict[str, Any] = field(default_factory=dict)
    conflict: Optional[Conflict] = None
    reason: str = ""
    detail: str = ""


# --------------------------------------------------------------------------- #
# Serialization helpers
# --------------------------------------------------------------------------- #
def ks_json(k: Ks) -> List[Any]:
    return [k[0], k[1]]


def ks_from_json(o: Any) -> Ks:
    kind, val = o
    if kind not in ("s", "i", "n"):
        raise ModelError(f"bad serialized value {o!r}")
    return (kind, val)


def fact_str(rel: str, vals: Sequence[Ks]) -> str:
    return f"{rel}({', '.join(_show_ks(v) for v in vals)})"


def _show_ks(k: Ks) -> str:
    if k[0] == "n":
        return f"NULL:{k[1]}"
    if k[0] == "s":
        return json.dumps(k[1], ensure_ascii=False)
    return str(k[1])


def fact_key(rel: str, vals: Sequence[Ks]) -> FactKey:
    return (rel, tuple(vals))


# --------------------------------------------------------------------------- #
# Disk-backed frontier queue (keeps the frontier out of memory)
# --------------------------------------------------------------------------- #
class Queue:
    def __init__(self, store: Store) -> None:
        self.conn = store.conn
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS _queue (seq INTEGER PRIMARY KEY AUTOINCREMENT, "
            "rel TEXT NOT NULL, vals TEXT NOT NULL)")

    def push(self, rel: str, vals: Sequence[Ks]) -> None:
        self.conn.execute("INSERT INTO _queue(rel, vals) VALUES (?, ?)",
                          (rel, json.dumps([ks_json(v) for v in vals])))

    def push_many(self, rows: Iterable[Tuple[str, Sequence[Ks]]]) -> None:
        data = [(rel, json.dumps([ks_json(v) for v in vals])) for rel, vals in rows]
        if data:
            self.conn.executemany("INSERT INTO _queue(rel, vals) VALUES (?, ?)", data)

    def pop(self, n: int) -> List[Tuple[int, str, Tuple[Ks, ...]]]:
        rows = self.conn.execute(
            "SELECT seq, rel, vals FROM _queue ORDER BY seq LIMIT ?", (n,)).fetchall()
        out = []
        for seq, rel, vals in rows:
            out.append((int(seq), rel, tuple(ks_from_json(v) for v in json.loads(vals))))
        return out

    def delete(self, seqs: Sequence[int]) -> None:
        if seqs:
            self.conn.executemany("DELETE FROM _queue WHERE seq=?", [(s,) for s in seqs])

    def __len__(self) -> int:
        return int(self.conn.execute("SELECT COUNT(*) FROM _queue").fetchone()[0])


# --------------------------------------------------------------------------- #
# Engine
# --------------------------------------------------------------------------- #
class Engine:
    def __init__(self, schema: Schema, rules: Sequence[Rule], store: Store,
                 budgets: Optional[Budgets] = None, provenance: bool = True,
                 order_seed: int = 0) -> None:
        self.schema = schema
        self.rules = list(rules)
        self.tgds = [r for r in self.rules if r.kind == "tgd"]
        self.egds = [r for r in self.rules if r.kind == "egd"]
        self.store = store
        self.budgets = budgets or Budgets()
        self.provenance = provenance
        self.order_seed = order_seed
        for rel, cols in schema.relations.items():
            store.declare(rel, len(cols))
        self.queue = Queue(store)
        self.by_rel_body: Dict[str, List[Tuple[Rule, int]]] = {}
        for rule in self.tgds:
            for k, atom in enumerate(rule.body):
                self.by_rel_body.setdefault(atom.rel, []).append((rule, k))

        self.unifier = Unifier()
        self.null_counter = 0
        self.probes = 0
        self.trigger_fires = 0
        self.trigger_skips = 0
        self.egd_checks = 0
        self.merges: List[Dict[str, Any]] = []
        self.null_info: Dict[str, Dict[str, Any]] = {}
        self.fact_origin: Dict[FactKey, Dict[str, Any]] = {}
        self.trigger_log: Dict[str, Dict[str, Any]] = {}
        self.events: List[Dict[str, Any]] = []
        self.merge_history_canonical: List[Dict[str, Any]] = []
        self._applied_merges: Set[str] = set()
        self.t0 = time.monotonic()
        self._last_budget_check = 0
        self._rounds = 0
        self.stats_extra: Dict[str, Any] = {
            "rebuilds": 0, "rows_rewritten": 0, "duplicates_removed": 0,
        }

    # -- budgets ------------------------------------------------------------ #
    def elapsed(self) -> float:
        return time.monotonic() - self.t0

    def _check_budget(self) -> None:
        if self.elapsed() > self.budgets.max_seconds:
            raise BudgetExceeded("time", f"elapsed {self.elapsed():.2f}s > {self.budgets.max_seconds}s")
        if self.trigger_fires > self.budgets.max_triggers:
            raise BudgetExceeded("triggers", f"{self.trigger_fires} > {self.budgets.max_triggers}")
        if self._rounds > self.budgets.max_rounds:
            raise BudgetExceeded("rounds", f"{self._rounds} > {self.budgets.max_rounds}")

    def _check_fact_budget(self) -> None:
        if self.store.total_facts() > self.budgets.max_facts:
            raise BudgetExceeded("facts", f"{self.store.total_facts()} > {self.budgets.max_facts}")

    def _queue_existing(self) -> int:
        """Queue all stored facts once (used before checks on a loaded store)."""
        n = 0
        for rel in self.schema.relations:
            for row, _src in self.store.iter_rows(rel):
                self.queue.push(rel, row)
                n += 1
        return n

    # -- input -------------------------------------------------------------- #
    def preload_input(self, facts: Dict[str, List[Sequence[object]]],
                      source: str = "<memory>") -> int:
        """Insert input facts without checking budgets or queueing them.

        Used so that even an immediately-exhausted run has the input recorded
        in the store (the package writer needs the instance to exist).
        """
        total = 0
        seen: Set[FactKey] = set()
        for rel, rows in facts.items():
            cols = self.schema.columns(rel)
            prepared: List[Tuple[Ks, ...]] = []
            for idx, raw in enumerate(rows):
                if len(raw) != len(cols):
                    raise ModelError(
                        f"fact {rel}{tuple(raw)!r}: expected {len(cols)} values, got {len(raw)}")
                vals: List[Ks] = []
                for i, (v, dom) in enumerate(zip(raw, cols)):
                    if isinstance(v, LNullValue):
                        self.null_info.setdefault(v.id, {
                            "created_by_rule": None, "trigger": None, "input": True,
                        })
                        vals.append(("n", v.id))
                        continue
                    if dom == "text":
                        if not isinstance(v, str):
                            raise ModelError(
                                f"fact {rel}{tuple(raw)!r} position {i}: text column needs a "
                                f"string, got {type(v).__name__}")
                        vals.append(("s", v))
                    else:
                        if not isinstance(v, int) or isinstance(v, bool):
                            raise ModelError(
                                f"fact {rel}{tuple(raw)!r} position {i}: int column needs an int, "
                                f"got {type(v).__name__}")
                        vals.append(("i", v))
                key = (rel, tuple(vals))
                if key in seen:
                    continue
                seen.add(key)
                prepared.append(tuple(vals))
                if self.provenance:
                    self.fact_origin[key] = {
                        "kind": "input_null" if any(x[0] == "n" for x in vals) else "input",
                        "source": source,
                        "relation": rel,
                        "index": idx,
                    }
                total += 1
            self.store.insert_many(rel, prepared, src=1)
            self.queue.push_many((rel, v) for v in prepared)
        return total

    def load_input(self, facts: Dict[str, List[Sequence[object]]],
                   source: str = "<memory>") -> int:
        n = self.preload_input(facts, source)
        self._check_fact_budget()
        return n

    # -- value helpers ------------------------------------------------------- #
    def _ks_of_term(self, term: object, binding: Dict[str, Ks]) -> Ks:
        if isinstance(term, Const):
            if term.kind == "string":
                return ("s", term.value)
            if term.kind == "int":
                return ("i", term.value)
            raise ModelError("labeled nulls cannot appear in rules")
        return binding[term.name]

    def _null(self, nid: str) -> Ks:
        return ("n", nid)

    def _new_null(self, rule: Rule, support: List[FactKey]) -> Ks:
        while True:
            nid = f"n{self.null_counter}"
            self.null_counter += 1
            if nid not in self.null_info:
                break
        self.null_info[nid] = {"created_by": rule.id, "created_by_rule": rule.id,
                               "trigger": None, "t": self.elapsed(), "input": False}
        return ("n", nid)

    # -- body matching ------------------------------------------------------- #
    def _atom_fixed(self, atom: Atom, binding: Dict[str, Ks]
                    ) -> Tuple[List[Optional[Ks]], List[Tuple[int, ...]]]:
        fixed: List[Optional[Ks]] = []
        groups: Dict[str, List[int]] = {}
        for i, t in enumerate(atom.terms):
            if isinstance(t, Const):
                fixed.append(self._ks_of_term(t, binding))
            elif t.name in binding:
                fixed.append(binding[t.name])
            else:
                fixed.append(None)
                groups.setdefault(t.name, []).append(i)
        return fixed, [tuple(g) for g in groups.values() if len(g) > 1]

    def _fetch(self, atom: Atom, binding: Dict[str, Ks]) -> List[Tuple[Ks, ...]]:
        fixed, groups = self._atom_fixed(atom, binding)
        rows = self.store.fetch_matching(atom.rel, fixed, groups,
                                         cap=self.budgets.max_match_rows)
        return rows

    def _extend(self, rule: Rule, binding: Dict[str, Ks], remaining: List[int],
                support: List[FactKey], chosen: List[int]) -> Iterator[Tuple[Dict[str, Ks], List[FactKey]]]:
        if not remaining:
            yield binding, support
            return
        # pick the most constrained atom next
        def score(i: int) -> Tuple[int, int]:
            fixed, _ = self._atom_fixed(rule.body[i], binding)
            return (sum(1 for f in fixed if f is not None), len(rule.body[i].terms))
        order = sorted(remaining, key=score, reverse=True)
        i = order[0]
        rest = [j for j in remaining if j != i]
        atom = rule.body[i]
        for row in self._fetch(atom, binding):
            nb = dict(binding)
            ok = True
            for t, rv in zip(atom.terms, row):
                if isinstance(t, Var):
                    cur = nb.get(t.name)
                    if cur is not None and cur != rv:
                        ok = False
                        break
                    nb[t.name] = rv
            if not ok:
                continue
            self.probes += 1
            if self.probes - self._last_budget_check >= 4096:
                self._last_budget_check = self.probes
                self._check_budget()
                if self.probes > self.budgets.max_probes:
                    raise BudgetExceeded("probes", f"{self.probes} > {self.budgets.max_probes}")
            yield from self._extend(rule, nb, rest, support + [(atom.rel, row)],
                                    chosen + [i])

    def _bindings_from_anchor(self, rule: Rule, k: int, row: Tuple[Ks, ...]
                              ) -> Iterator[Tuple[Dict[str, Ks], List[FactKey]]]:
        atom = rule.body[k]
        binding: Dict[str, Ks] = {}
        for t, rv in zip(atom.terms, row):
            if isinstance(t, Const):
                if self._ks_of_term(t, binding) != rv:
                    return
            else:
                cur = binding.get(t.name)
                if cur is not None and cur != rv:
                    return
                binding[t.name] = rv
        rest = [i for i in range(len(rule.body)) if i != k]
        yield from self._extend(rule, binding, rest, [(atom.rel, row)], [k])

    def _all_bindings(self, rule: Rule) -> Iterator[Tuple[Dict[str, Ks], List[FactKey]]]:
        # Drive from the smallest relation to keep the scan bounded.
        counts = {r: self.store.count(r) for r in {a.rel for a in rule.body}}
        k = min(range(len(rule.body)), key=lambda i: counts[rule.body[i].rel])
        atom = rule.body[k]
        rest = [i for i in range(len(rule.body)) if i != k]
        for row, _src in self.store.iter_rows(atom.rel):
            binding: Dict[str, Ks] = {}
            ok = True
            for t, rv in zip(atom.terms, row):
                if isinstance(t, Const):
                    if self._ks_of_term(t, binding) != rv:
                        ok = False
                        break
                else:
                    cur = binding.get(t.name)
                    if cur is not None and cur != rv:
                        ok = False
                        break
                    binding[t.name] = rv
            if not ok:
                continue
            yield from self._extend(rule, binding, rest, [(atom.rel, row)], [k])

    # -- TGD firing ---------------------------------------------------------- #
    def _head_plan(self, rule: Rule, binding: Dict[str, Ks]
                   ) -> Tuple[List[Optional[Ks]], List[Tuple[int, ...]], List[Optional[str]]]:
        assert rule.head is not None
        fixed: List[Optional[Ks]] = []
        by_exist: Dict[str, List[int]] = {}
        names: List[Optional[str]] = []
        for i, t in enumerate(rule.head.terms):
            if isinstance(t, Const):
                fixed.append(self._ks_of_term(t, binding))
                names.append(None)
            elif t.name in binding:
                fixed.append(binding[t.name])
                names.append(None)
            else:
                fixed.append(None)
                names.append(t.name)
                by_exist.setdefault(t.name, []).append(i)
        groups = [tuple(v) for v in by_exist.values() if len(v) > 1]
        return fixed, groups, names

    def _head_witnessed(self, rule: Rule, binding: Dict[str, Ks]) -> bool:
        fixed, groups, _names = self._head_plan(rule, binding)
        return self.store.exists(rule.head.rel, fixed, groups)  # type: ignore[union-attr]

    def _fire(self, rule: Rule, binding: Dict[str, Ks], support: List[FactKey]) -> bool:
        if self._head_witnessed(rule, binding):
            self.trigger_skips += 1
            return False
        assert rule.head is not None
        exist_values: Dict[str, Ks] = {}
        vals: List[Ks] = []
        for t in rule.head.terms:
            if isinstance(t, Const):
                vals.append(self._ks_of_term(t, binding))
            elif t.name in binding:
                vals.append(binding[t.name])
            else:
                if t.name not in exist_values:
                    exist_values[t.name] = self._new_null(rule, support)
                vals.append(exist_values[t.name])
        tkey = self._trigger_key(rule, binding)
        self.store.insert(rule.head.rel, vals, src=0)
        self.trigger_fires += 1
        if self.provenance:
            rec = {
                "rule": rule.id,
                "binding": {k: ks_json(v) for k, v in sorted(binding.items())},
                "support": [[rel, [ks_json(v) for v in row]] for rel, row in support],
                "head": [rule.head.rel, [ks_json(v) for v in vals]],
                "existentials": {n: ks_json(v) for n, v in exist_values.items()},
                "t": round(self.elapsed(), 6),
            }
            self.trigger_log[tkey] = rec
            key = (rule.head.rel, tuple(vals))
            if key not in self.fact_origin:
                self.fact_origin[key] = {
                    "kind": "derived",
                    "rule": rule.id,
                    "trigger": tkey,
                    "support": [[rel, [ks_json(v) for v in row]] for rel, row in support],
                }
            for nid in exist_values.values():
                self.null_info[str(nid[1])].update({
                    "rule": rule.id, "trigger": tkey,
                    "display": f"NULL:{nid[1]}",
                    "position": [i for i, v in enumerate(vals) if v == nid],
                })
        self.queue.push(rule.head.rel, vals)
        if self.provenance:
            self._emit({"kind": "firing", "rule": rule.id,
                        "binding": {k: ks_json(v) for k, v in sorted(binding.items())},
                        "existentials": {n: ks_json(v) for n, v in exist_values.items()},
                        "support": [[rel, [ks_json(v) for v in row]] for rel, row in support]},
                       rule.head.rel, vals)
        self._check_fact_budget()
        return True

    def _trigger_key(self, rule: Rule, binding: Dict[str, Ks]) -> str:
        parts = [rule.id]
        for name in sorted(binding):
            parts.append(f"{name}={binding[name][0]}:{binding[name][1]}")
        return "|".join(parts)

    def _process_fact(self, rel: str, row: Tuple[Ks, ...]) -> None:
        entries = self.by_rel_body.get(rel, ())
        if self.order_seed:
            import random
            rnd = random.Random(self.order_seed ^ hash((rel, row)))
            entries = sorted(entries, key=lambda e: rnd.random())
            # deterministic per fact, but varies which rule fires first
        for rule, k in entries:
            for binding, support in self._bindings_from_anchor(rule, k, row):
                self._fire(rule, binding, support)

    def _emit(self, event: Dict[str, Any], rel: str, row: Tuple[Ks, ...]) -> None:
        ev = dict(event)
        ev["fact"] = fact_str(rel, row)
        self.events.append(ev)

    def _chase_tgds(self) -> None:
        while True:
            batch = self.queue.pop(200)
            if not batch:
                return
            seqs = []
            for seq, rel, row in batch:
                seqs.append(seq)
                if not self.store.has(rel, row):
                    continue  # stale row: its canonical version was re-queued
                self._process_fact(rel, row)
            self.queue.delete(seqs)
            self._check_budget()
            self._check_fact_budget()

    # -- EGD round ----------------------------------------------------------- #
    def _egd_value(self, term: object, binding: Dict[str, Ks]) -> Ks:
        return self._ks_of_term(term, binding)

    def _egd_round(self) -> Optional[Conflict]:
        violations: List[Tuple[Rule, Dict[str, Ks], Ks, Ks, List[FactKey]]] = []
        for rule in self.egds:
            for binding, support in self._all_bindings(rule):
                self.egd_checks += 1
                if self.egd_checks % 1024 == 0:
                    self._check_budget()
                left = self.unifier.find(self._egd_value(rule.eq_left, binding))
                right = self.unifier.find(self._egd_value(rule.eq_right, binding))
                if left == right:
                    continue
                if left[0] != "n" and right[0] != "n":
                    return self._make_conflict(rule, binding, left, right, support)
                violations.append((rule, binding, left, right, support))
        if not violations:
            return None
        for rule, binding, left, right, support in violations:
            try:
                event = self.unifier.union_tokens(left, right)
            except ConstantConflict as cc:
                return self._make_conflict(rule, binding, cc.left, cc.right, support)
            if event is not None:
                mg = {
                    "rule": rule.id,
                    "binding": {k: ks_json(v) for k, v in sorted(binding.items())},
                    "left": ks_json(event.left),
                    "right": ks_json(event.right),
                    "result": ks_json(self.unifier.find(event.result)),
                    "support": [[rel, [ks_json(v) for v in row]] for rel, row in support],
                    "t": round(self.elapsed(), 6),
                }
                self.merges.append(mg)
                self.stats_extra["unifications"] = self.stats_extra.get("unifications", 0) + 1
                if self.provenance:
                    self.events.append(dict(mg, kind="merge"))
        return None

    # -- conflict ------------------------------------------------------------ #
    def _make_conflict(self, rule: Rule, binding: Dict[str, Ks], left: Ks, right: Ks,
                       support: List[FactKey]) -> Conflict:
        return Conflict(
            rule_id=rule.id,
            binding=dict(binding),
            left=left,
            right=right,
            support=list(support),
            left_sources=self._sources_of(left),
            right_sources=self._sources_of(right),
            explanation=(
                f"EGD {rule.id} matches with {rule.eq_left}={_show_ks(left)} and "
                f"{rule.eq_right}={_show_ks(right)}; both are distinct known constants, "
                f"so no value can satisfy the dependency. The engine does not pick a "
                f"preferred value."),
        )

    def _sources_of(self, value: Ks) -> List[Dict[str, Any]]:
        """Trace the constants/unification history behind a value."""
        out: List[Dict[str, Any]] = []
        # 1. input facts containing the value
        for rel in self.schema.relations:
            for row, _src in self.store.rows_containing(rel, value):
                key = (rel, tuple(row))
                origin = self.fact_origin.get(key)
                if origin is None or origin.get("kind") != "input":
                    continue
                out.append({"fact": fact_str(rel, row), "origin": origin})
        # 2. unification history reaching this value
        for m in self.merge_history_canonical:
            vals = {tuple(m["left"]), tuple(m["right"]), tuple(m["result"])}
            if (value[0], value[1]) in vals:
                out.append({"merge": m, "kind": "unification"})
        return out

    def _apply_merges(self) -> List[Tuple[str, Tuple[Ks, ...]]]:
        # Only classes that *changed since the last rebuild* need re-writing.
        # merged_ids tracks nulls whose root is no longer themselves; once a
        # rebuild has applied them they are recorded in _applied_merges and
        # skipped, so idempotent EGD rounds do not rewrite the tables again.
        merged_ids: Set[str] = set()
        for nid in list(self.null_info):
            if nid in self._applied_merges:
                continue
            root = self.unifier.find(("n", nid))
            if root != ("n", nid):
                merged_ids.add(nid)
        if not merged_ids:
            return []
        self._applied_merges.update(merged_ids)
        tokens = [("n", n) for n in merged_ids]
        affected = [rel for rel in self.schema.relations
                    if self.store.relation_has_any(rel, tokens)]
        changed: List[Tuple[str, Tuple[Ks, ...]]] = []

        def translate(row: Sequence[Ks]) -> Tuple[Ks, ...]:
            return tuple(self.unifier.canon_ks(v) if v[0] == "n" else v for v in row)

        for rel in affected:
            for old, _src in self.store.iter_rows(rel):
                new = translate(old)
                if new != old:
                    changed.append((rel, new))
        stats = self.store.rebuild(affected, translate)
        self.stats_extra["rebuilds"] += 1
        for rel, st in stats.items():
            self.stats_extra["rows_rewritten"] += st["before"]
            self.stats_extra["duplicates_removed"] += st["collapsed"]
        if self.provenance:
            self._repoint_provenance()
        seen: Set[FactKey] = set()
        fresh: List[Tuple[str, Tuple[Ks, ...]]] = []
        for rel, row in changed:
            key = (rel, row)
            if key in seen:
                continue
            seen.add(key)
            fresh.append((rel, row))
        self.queue.push_many(fresh)
        return fresh

    def _repoint_provenance(self) -> None:
        def canon(v: Ks) -> Ks:
            return self.unifier.canon_ks(v) if v[0] == "n" else v

        # merge history is *history*: keep the pre-unification identities
        for m in self.merges:
            if m not in self.merge_history_canonical:
                self.merge_history_canonical.append(m)
        new_map: Dict[FactKey, Dict[str, Any]] = {}
        for (rel, vals), origin in self.fact_origin.items():
            key = (rel, tuple(canon(v) for v in vals))
            if key in new_map:
                prev = new_map[key]
                if prev.get("kind") == "derived" and origin.get("kind") == "input":
                    new_map[key] = origin
                elif prev.get("kind") == "input" and origin.get("kind") != "input":
                    pass
                else:
                    alts = prev.setdefault("also_origin", [])
                    if origin not in alts:
                        alts.append(origin)
            else:
                new_map[key] = origin
        self.fact_origin = new_map

    # -- driver -------------------------------------------------------------- #
    def run(self) -> RunResult:
        try:
            self._check_fact_budget()
            self._chase_tgds()
            while True:
                self._rounds += 1
                self._check_budget()
                conflict = self._egd_round()
                if conflict is not None:
                    return RunResult(
                        status="conflict",
                        conflict=conflict,
                        stats=self._stats(),
                        reason="egd-conflict",
                        detail=conflict.explanation,
                    )
                if not self.merges:
                    break
                changed = self._apply_merges()
                self.merges = []
                if not changed and len(self.queue) == 0:
                    # no rows changed and nothing to re-chase; still loop once more
                    # to be certain the EGD round sees the rebuilt state.
                    pass
                self._chase_tgds()
            return RunResult(status="complete", stats=self._stats())
        except BudgetExceeded as exc:
            return RunResult(status="incomplete", stats=self._stats(),
                             reason=exc.reason, detail=exc.detail or str(exc))
        except Exception as exc:  # defensive: reported, never a "complete" flag
            return RunResult(status="error", stats=self._stats(),
                             reason=type(exc).__name__, detail=str(exc))

    def _stats(self) -> Dict[str, Any]:
        rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return {
            "facts_total": self.store.total_facts(),
            "facts_by_relation": self.store.all_counts(),
            "inputs": sum(1 for o in self.fact_origin.values() if o.get("kind") == "input"),
            "derived": sum(1 for o in self.fact_origin.values() if o.get("kind") == "derived"),
            "trigger_fires": self.trigger_fires,
            "trigger_skips": self.trigger_skips,
            "triggers_recorded": len(self.trigger_log),
            "body_bindings_enumerated": self.probes,
            "egd_checks": self.egd_checks,
            "unifications": self.stats_extra.get("unifications", 0),
            "rebuilds": self.stats_extra["rebuilds"],
            "duplicates_removed": self.stats_extra["duplicates_removed"],
            "labeled_nulls_created": self.null_counter,
            "null_classes": self._null_classes(),
            "null_classes_absorbed_by_constants": self._nulls_absorbed_by_constants(),
            "elapsed_seconds": round(self.elapsed(), 3),
            "rss_kb": rss_kb,
            "rounds": self._rounds,
        }

    def _null_classes(self) -> int:
        roots = {self.unifier.find(("n", nid)) for nid in self.null_info}
        return sum(1 for r in roots if r[0] == "n")

    def _nulls_absorbed_by_constants(self) -> int:
        roots = {self.unifier.find(("n", nid)) for nid in self.null_info}
        return sum(1 for r in roots if r[0] != "n")

    # -- final canonical view ------------------------------------------------ #
    def final_null_map(self) -> Dict[str, Ks]:
        """Map every created null id to its final canonical value token."""
        return {nid: self.unifier.find(("n", nid)) for nid in self.null_info}

    def normalize_null_ids(self) -> Dict[str, str]:
        """Give surviving labeled nulls final ids (sharing preserved).

        A surviving class whose representative is a *pre-existing input*
        unknown keeps that id, so provenance can point at it directly.
        Other classes get compact ids ``n0``, ``n1``... that never collide
        with an input id.
        """
        final = self.final_null_map()
        survivors = [root for root in set(final.values()) if root[0] == "n"]
        input_ids = {nid for nid, info in self.null_info.items() if info.get("input")}
        roots = sorted(str(t[1]) for t in survivors)
        mapping: Dict[str, str] = {}
        taken: Set[str] = set()
        for rid in roots:
            if rid in input_ids:
                mapping[rid] = rid
                taken.add(rid)
        counter = 0
        for rid in roots:
            if rid in mapping:
                continue
            while f"n{counter}" in taken:
                counter += 1
            mapping[rid] = f"n{counter}"
            taken.add(mapping[rid])
            counter += 1
        return mapping

    def export(self) -> Dict[str, Any]:
        """Build the final, canonical package payload.

        Labeled null ids are left exactly as the equivalence classes define
        them (two occurrences share an id iff they are the same unknown).  The
        writer may renumber them; sharing and distinctness are always kept.
        """
        rename = self.normalize_null_ids()
        final = self.final_null_map()

        def ren_js(ks: Sequence[object]) -> List[Any]:
            k = self.unifier.canon_ks((ks[0], ks[1])) if ks[0] == "n" else (ks[0], ks[1])
            if k[0] == "n":
                return ["n", rename.get(str(k[1]), str(k[1]))]
            return [k[0], k[1]]

        def ren(k: Ks) -> object:
            k = self.unifier.canon_ks(k) if k[0] == "n" else k
            if k[0] == "s":
                return k[1]
            if k[0] == "i":
                return k[1]
            nid = rename.get(str(k[1]), str(k[1]))
            return LNullValue(nid, f"NULL:{nid}")

        def raw_js(ks: Sequence[object]) -> List[Any]:
            """Keep the identity of a (possibly absorbed) null for history."""
            if ks[0] == "n":
                nid = str(ks[1])
                return ["n", rename.get(nid, nid)]
            return [ks[0], ks[1]]

        facts: Dict[str, List[Tuple[object, ...]]] = {}
        for rel in self.schema.relations:
            facts[rel] = [tuple(ren(v) for v in row)
                          for row, _src in self.store.iter_rows(rel)]

        # provenance: rename everything, keep sharing/differentness
        triggers = []
        for tkey, rec in self.trigger_log.items():
            triggers.append({
                "trigger": tkey,
                "rule": rec["rule"],
                "binding": {k: [v[0], v[1]] for k, v in rec["binding"].items()},
                "head": [rec["head"][0], [raw_js(v) for v in rec["head"][1]]],
                "existentials": {n: raw_js(v) for n, v in rec["existentials"].items()},
                "support": [{"fact": _fact_from_json(rel, vals)}
                            for rel, vals in rec["support"]],
            })
        merges = []
        seen_merges: Set[str] = set()
        for m in getattr(self, "merge_history_canonical", []):
            side = [raw_js(m["left"]), raw_js(m["right"])]
            key = json.dumps([m["rule"], side], sort_keys=True, ensure_ascii=False)
            if key in seen_merges:
                continue
            seen_merges.add(key)
            merges.append({
                "rule": m["rule"],
                "binding": {k: [v[0], v[1]] for k, v in m["binding"].items()},
                "unified": [raw_js(m["left"]), raw_js(m["right"])],
                "result": raw_js(m["result"]),
                "support": [{"fact": _fact_from_json(rel, vals)}
                            for rel, vals in m["support"]],
            })
        nulls: Dict[str, Any] = {}
        for nid, root in final.items():
            info = self.null_info[nid]
            entry: Dict[str, Any] = {
                "display": f"NULL:{nid}",
                "origin": "input" if info.get("input") else "rule",
                "created_by_rule": info.get("created_by_rule"),
                "trigger": info.get("trigger"),
            }
            if root[0] == "n":
                target = rename.get(str(root[1]), str(root[1]))
                entry["class"] = target
                entry["unified_with"] = None
            else:
                entry["class"] = None
                entry["unified_with"] = [root[0], root[1]]
            nulls[nid] = entry
        current_nulls = set()
        for rel, rows in facts.items():
            for row in rows:
                for v in row:
                    if isinstance(v, LNullValue):
                        current_nulls.add(v.id)
        origins = {}
        for (rel, vals), origin in self.fact_origin.items():
            key = f"{rel}|{json.dumps([ren_js(v) for v in vals], ensure_ascii=False, sort_keys=True)}"
            if key not in origins:
                origins[key] = origin
        return {
            "facts": facts,
            "triggers": triggers,
            "merges": merges,
            "nulls": {"nulls": nulls, "live": sorted(current_nulls)},
            "origins": origins,
            "events": self.events,
        }

    def close(self) -> None:
        """No-op: the store owns the connection (and its lifecycle)."""


def _fact_from_json(rel: str, vals: Sequence[Sequence[object]]) -> str:
    def show(v: Sequence[object]) -> str:
        if v[0] == "n":
            return f"NULL:{v[1]}"
        if v[0] == "s":
            return json.dumps(v[1], ensure_ascii=False)
        return str(v[1])
    return f"{rel}({', '.join(show(v) for v in vals)})"


