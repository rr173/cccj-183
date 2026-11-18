"""Independent reference evaluator for data-quality audits.

Shares **no** code with :mod:`chase.dqaudit` or :mod:`chase.dqpolicy`: it
takes hand-written fixtures (or a stored version payload) as plain Python
data, enumerates every check row by nested loops, and returns status, counts
and deterministic samples per rule.  It exists so the tests can compare the
SQLite/policy engine against an obviously-correct evaluator.

Typed values are represented as ``("s", text)``, ``("i", int)`` and
``("n", null_id)`` — the same identity rule as the rest of the system, but
re-derived here.

A fixture is::

    {
      "sources": {                     # raw records of the sources version
        "<source_id>/<record_id>": {"relation": R, "tuple": [...]}
      },
      "completed": {R: [ [...], ... ]},  # facts of the completion version
      "sources_status": "complete"|"conflict"|"incomplete",
      "provenance": {                  # for a derived target fact
        (R, row): {"kind": "derived"|"input", "rule": ..., "support": [...]}
      },
    }

Any value may be a plain string/int or ``{"null": "id"}``.
"""

from __future__ import annotations

from typing import Any, Dict, List, Sequence, Tuple

Token = Tuple[str, Any]


def tok(v: Any) -> Token:
    if isinstance(v, dict):
        if "null" in v:
            return ("n", v["null"])
        if v.get("type") == "text" or v.get("type") == "int":
            return ("s", v["value"]) if v["type"] == "text" else ("i", v["value"])
        raise AssertionError(f"bad fixture value {v!r}")
    if isinstance(v, str):
        return ("s", v)
    if isinstance(v, int) and not isinstance(v, bool):
        return ("i", v)
    raise AssertionError(f"bad fixture value {v!r}")


def source_rows(fixture: Dict[str, Any]) -> List[Dict[str, Any]]:
    out = []
    for key in sorted(fixture["sources"]):
        sid, rid = key.split("/", 1)
        rec = fixture["sources"][key]
        out.append({"source_id": sid, "record_id": rid,
                    "relation": rec["relation"],
                    "values": [tok(v) for v in rec["tuple"]]})
    return out


def completed_rows(fixture: Dict[str, Any], relation: str
                   ) -> List[List[Token]]:
    return [[tok(v) for v in row]
            for row in fixture.get("completed", {}).get(relation, [])]


def _known(t: Token) -> bool:
    return t[0] in ("s", "i")


def _tok_out(t: Token) -> Dict[str, Any]:
    if t[0] == "n":
        return {"kind": "labeled_null", "id": t[1]}
    if t[0] == "s":
        return {"kind": "string", "value": t[1]}
    return {"kind": "int", "value": t[1]}


def _col(rule: Dict[str, Any]) -> List[int]:
    cols = rule.get("columns", rule.get("column"))
    if isinstance(cols, list):
        return cols
    return [cols]


def evaluate(fixture: Dict[str, Any], rules: Sequence[Dict[str, Any]],
             sample_limit: int = 5) -> Dict[str, Any]:
    """Enumerate every row of every rule; return counts, status and samples."""
    sources = source_rows(fixture)
    results: List[Dict[str, Any]] = []
    totals = {"pass": 0, "violation": 0, "indeterminate": 0, "total": 0}
    for rule in rules:
        layer = rule["layer"]
        rel = rule["relation"]
        cols = _col(rule)
        if layer == "sources":
            rows = [r for r in sources if r["relation"] == rel]
            elems = [r["values"] for r in rows]
        else:
            rows = []
            elems = completed_rows(fixture, rel)
        kind = rule["kind"]
        violations: List[Dict[str, Any]] = []
        inds: List[Dict[str, Any]] = []
        passed = 0
        matched: List[Dict[str, Any]] = []
        dup_groups = 0

        if kind == "required":
            for i, vals in enumerate(elems):
                if not _known(vals[cols[0]]):
                    violations.append(_sample(rule, fixture, rows, elems, i,
                                              layer, "violation"))
                else:
                    passed += 1
        elif kind == "enum":
            allowed = set(tok(c) for c in rule["allowed"])
            for i, vals in enumerate(elems):
                v = vals[cols[0]]
                if v[0] == "n":
                    inds.append(_sample(rule, fixture, rows, elems, i, layer,
                                        "indeterminate"))
                elif v in allowed:
                    passed += 1
                else:
                    violations.append(_sample(rule, fixture, rows, elems, i,
                                              layer, "violation"))
        elif kind == "unique":
            groups: Dict[Tuple[Token, ...], List[int]] = {}
            for i, vals in enumerate(elems):
                key = tuple(vals[c] for c in cols)
                if any(t[0] == "n" for t in key):
                    inds.append(_sample(rule, fixture, rows, elems, i, layer,
                                        "indeterminate"))
                    continue
                groups.setdefault(key, []).append(i)
            for key in sorted(groups, key=lambda k: [_ord(t) for t in k]):
                members = groups[key]
                if len(members) > 1:
                    dup_groups += 1
                    for i in members:
                        violations.append(_sample(rule, fixture, rows, elems,
                                                  i, layer, "violation"))
                else:
                    passed += 1
        elif kind == "reference":
            target_rel = rule["target"]["relation"]
            tcols = rule["target"].get("columns", rule["target"].get("column"))
            if isinstance(tcols, int):
                tcols = [tcols]
            elif isinstance(tcols, list):
                pass
            else:
                tcols = [tcols]
            target = []
            for vals in completed_rows(fixture, target_rel):
                target.append(vals)
            target_keys = set(tuple(v[c] for c in tcols) for v in target)
            for i, vals in enumerate(elems):
                key = tuple(vals[c] for c in cols)
                if any(t[0] == "n" for t in key):
                    inds.append(_sample(rule, fixture, rows, elems, i, layer,
                                        "indeterminate"))
                elif key in target_keys:
                    passed += 1
                    hit = next(v for v in target
                               if tuple(v[c] for c in tcols) == key)
                    matched.append(
                        {"key": [_tok_out(t) for t in key],
                         "target": [_tok_out(t) for t in hit],
                         "origin": fixture.get("provenance", {}).get(
                             (target_rel, tuple(hit)), {"kind": "input"})})
                else:
                    violations.append(_sample(rule, fixture, rows, elems, i,
                                              layer, "violation"))
        else:
            raise AssertionError(f"unknown kind {kind}")

        total = len(elems)
        counts = {"pass": passed, "violation": len(violations),
                  "indeterminate": len(inds), "total": total}
        assert counts["pass"] + counts["violation"] + counts["indeterminate"] \
            == counts["total"], counts
        for k in totals:
            totals[k] += counts[k]
        # deterministic, bounded samples: violations first (row order), then
        # indeterminate rows, each capped so both categories can show up
        vcap = min(len(violations), sample_limit)
        icap = min(len(inds), sample_limit - vcap if vcap < sample_limit else 0)
        if len(inds) and vcap == sample_limit:
            vcap = max(0, sample_limit - 1)
            icap = min(len(inds), sample_limit - vcap)
        picked = violations[:vcap] + inds[:icap]
        status = "fail" if violations else ("indeterminate" if inds else "pass")
        out = {
            "id": rule["id"], "kind": kind, "layer": layer,
            "relation": rel, "status": status, "counts": counts,
            "samples": picked,
            "sample_omitted": (len(violations) + len(inds)) - len(picked),
            "samples_truncated": len(violations) + len(inds) > len(picked),
        }
        if kind == "unique":
            out["duplicate_groups"] = dup_groups
        if kind == "reference":
            out["matched"] = matched
        results.append(out)
    if any(r["status"] == "fail" for r in results):
        overall = "fail"
    elif any(r["status"] == "indeterminate" for r in results):
        overall = "indeterminate"
    else:
        overall = "pass"
    return {"status": overall, "totals": totals, "rules": results}


def _ord(t: Token):
    return (t[0], t[1] if t[0] != "n" else str(t[1]))


def _sample(rule, fixture, rows, elems, i, layer, category):
    sample = {"check_id": rule["id"], "category": category}
    if layer == "sources":
        r = rows[i]
        sample.update({"relation": r["relation"],
                       "source_id": r["source_id"],
                       "record_id": r["record_id"],
                       "values": [_tok_out(t) for t in r["values"]]})
    else:
        sample.update({"fact": {"relation": rule["relation"],
                                "values": [_tok_out(t) for t in elems[i]]}})
    return sample
