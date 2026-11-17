"""Verification of an on-disk package, reading only what the package contains.

This is the entry used by the ``verify`` command and by the writer before it
publishes.  It never trusts ``package.json``'s status: the status marker is
only a *claim*, and this module re-checks the stored instance against the
stored program from scratch.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Tuple

from .model import ModelError, Rule, Schema, parse_program, parse_value
from .package import Package
from .store import Store
from .verify import VerifyReport, Verifier


class LoadedPackage:
    def __init__(self, pkg: Package) -> None:
        self.pkg = pkg
        self.schema: Schema = Schema({r: tuple(c) for r, c in pkg.schema.items()})
        rules_obj = pkg.rules
        if isinstance(rules_obj, dict) and "rules" in rules_obj:
            program = rules_obj
        else:
            raise ModelError("rules.json must contain a 'rules' list")
        self.rules: List[Rule] = parse_program(program, self.schema)
        # Input facts for the "preservation" check come from package.json.
        # For a package that claims "complete" the engine guarantees they are
        # recorded; if they are absent we cannot prove preservation and say so.
        self.input_facts: Dict[str, List[Tuple[object, ...]]] = {
            r: [] for r in self.schema.relations}
        self.has_input_record = "input_facts" in pkg.meta
        srcs = pkg.meta.get("input_facts", {})
        if not isinstance(srcs, dict):
            raise ModelError("package.json input_facts must be an object")
        for rel, rows in srcs.items():
            if rel not in self.schema.relations:
                raise ModelError(f"input fact for undeclared relation {rel!r}")
            for row in rows:
                self.input_facts[rel].append(tuple(parse_value(v) for v in row))

    def to_store(self) -> Store:
        store = Store(":memory:")
        for rel, cols in self.schema.relations.items():
            store.declare(rel, len(cols))
        for rel, rows in self.pkg.facts.items():
            prepared = []
            for row in rows:
                vals = []
                for v, dom in zip(row, self.schema.columns(rel)):
                    from .model import LNullValue
                    if isinstance(v, LNullValue):
                        vals.append(("n", v.id))
                    elif isinstance(v, str):
                        vals.append(("s", v))
                    else:
                        vals.append(("i", v))
                prepared.append(tuple(vals))
            store.insert_many(rel, prepared, src=0)
        return store

    def verify(self, verify_cap: Optional[int] = None) -> VerifyReport:
        store = self.to_store()
        try:
            v = Verifier(self.schema, self.rules, store, self.input_facts,
                         _null_classes_from(self.pkg.nulls))
            if isinstance(self.pkg.nulls, dict) and "live" in self.pkg.nulls:
                null_ids = [str(x) for x in self.pkg.nulls["live"]]
            else:
                null_ids = [str(x) for x in self.pkg.null_ids()]
            report = v.run(labeled_null_ids=null_ids, verify_cap=verify_cap)
            if not self.has_input_record and self.pkg.meta.get("status") == "complete":
                report.violations.append({
                    "problem": "package.json has no input_facts record; reflection "
                               "cannot prove that all input facts are preserved"})
                report.ok = False
            return report
        finally:
            store.close()


def _null_classes_from(nulls_obj) -> Dict[str, Tuple[str, object]]:
    """Derive input-unknown -> final-value map from nulls.json."""
    out: Dict[str, Tuple[str, object]] = {}
    if not isinstance(nulls_obj, dict):
        return out
    for nid, info in nulls_obj.get("nulls", {}).items():
        if not isinstance(info, dict) or info.get("origin") != "input":
            continue
        if info.get("class") is not None:
            out[str(nid)] = ("n", info["class"])
        elif info.get("unified_with") is not None:
            kind, val = info["unified_with"]
            out[str(nid)] = (kind, val)
    return out


def _verify_payload(schema, rules, facts, input_facts, nulls_obj, verify_cap):
    """Verify an in-memory payload (used before publishing a package)."""
    store = Store(":memory:")
    try:
        for rel, cols in schema.relations.items():
            store.declare(rel, len(cols))
        for rel, rows in facts.items():
            prepared = []
            for row in rows:
                from .model import LNullValue
                vals = []
                for v in row:
                    if isinstance(v, LNullValue):
                        vals.append(("n", v.id))
                    elif isinstance(v, str):
                        vals.append(("s", v))
                    else:
                        vals.append(("i", v))
                prepared.append(tuple(vals))
            store.insert_many(rel, prepared, src=0)
        v = Verifier(schema, rules, store, input_facts, _null_classes_from(nulls_obj))
        null_ids = []
        if isinstance(nulls_obj, dict):
            # "live" are the nulls still present in the instance; older ids in
            # "nulls" are history (absorbed classes) and need not occur.
            null_ids = [str(x) for x in nulls_obj.get("live", [])]
            known = set(str(x) for x in nulls_obj.get("nulls", {}).keys())
            extra = {"null_ids_in_instance_not_documented": sorted(
                _instance_null_ids(store, schema) - known)}
        else:
            extra = {}
        report = v.run(labeled_null_ids=null_ids, verify_cap=verify_cap)
        if extra.get("null_ids_in_instance_not_documented"):
            report.violations.append({
                "problem": "labeled nulls occur in the instance but are missing from "
                           "nulls.json",
                "nulls": extra["null_ids_in_instance_not_documented"]})
            report.ok = False
        return report.to_json()
    finally:
        store.close()


def _instance_null_ids(store: Store, schema) -> Set[str]:
    out: Set[str] = set()
    for rel in schema.relations:
        for row, _src in store.iter_rows(rel):
            for v in row:
                if v[0] == "n":
                    out.add(str(v[1]))
    return out


def verify_package_dir(path: str, verify_cap: Optional[int] = None) -> Dict[str, Any]:
    pkg = Package(path)
    if not pkg.load():
        return {"ok": False, "error": f"no package.json in {path!r}"}
    loaded = LoadedPackage(pkg)
    report = loaded.verify(verify_cap=verify_cap)
    claimed = pkg.meta.get("status")
    result = report.to_json()
    result["claimed_status"] = claimed
    result["consistent"] = (claimed == "complete") == report.ok
    # a complete claim is only meaningful for a complete package
    result["requires_complete"] = claimed == "complete"
    if not report.ok:
        result["independent_violation"] = True
    return result
