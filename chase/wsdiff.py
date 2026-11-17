"""Version comparison for the workspace.

The diff never guesses unknown identity from generated ``n0``/``n1`` names.
Every version stores, for each *input* unknown id that still occurs in its
raw sources, the final value the completion mapped it to: either a known
constant or the final unknown equivalence class.  Comparison happens only on
input unknown ids present in **both** versions:

* **merges / splits / regroups** are reported as *class-level* events: the
  bipartite before/after class relation is computed and each connected
  component with more than one edge becomes one event (never a list of
  fake pairwise events);
* **appeared / disappeared** list input unknowns that entered or left the
  source set;
* **value changes** list, per shared input unknown, the transitions
  unknown -> known (``became_known``), known -> unknown (``became_unknown``)
  and known -> other known (``constant_changed``).

Rule-created unknowns (``n0``, ``n1``, ...) exist only inside one version's
run; no cross-version identity is claimed for them.  Consequently only
facts composed **entirely of known constants** are diffed as business facts
(typed values: the string ``"7"`` and the int ``7`` are different values).
Facts containing unknowns are not diffed — an unknown renumbering is not a
business change.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Sequence, Tuple

# token kinds for the class relation
_CONST = "const"
_CLASS = "class"


def _map_token(entry: Dict[str, Any]) -> Tuple[Any, ...]:
    """Hashable token for one input-null-map entry."""
    if entry["kind"] == "labeled_null":
        return (_CLASS, entry["id"])
    return (_CONST, entry["kind"], json.dumps(entry["value"], ensure_ascii=False,
                                             sort_keys=True))


def _token_json(token: Tuple[Any, ...]) -> Dict[str, Any]:
    if token[0] == _CLASS:
        return {"type": "unknown_class", "class": token[1]}
    return {"type": "constant",
            "value": {"kind": token[1], "value": json.loads(token[2])}}


def _classes(null_map: Dict[str, Any]
             ) -> Dict[Tuple[Any, ...], List[str]]:
    """Group input unknown ids by their final token."""
    out: Dict[Tuple[Any, ...], List[str]] = {}
    for nid in sorted(null_map):
        out.setdefault(_map_token(null_map[nid]), []).append(nid)
    return out


def _const_fact_key(rel: str, values: Sequence[Dict[str, Any]]) -> Optional[str]:
    """Canonical key for a fact, or None if it contains an unknown."""
    parts = []
    for v in values:
        if v["kind"] == "labeled_null":
            return None
        # typed: ("s","7") and ("i",7) can never collide
        parts.append(json.dumps([v["kind"], v["value"]], ensure_ascii=False,
                                sort_keys=True))
    return rel + "(" + ",".join(parts) + ")"


def _const_facts(payload: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for rel, rows in payload["facts"].items():
        for row in rows:
            key = _const_fact_key(rel, row)
            if key is not None:
                out[key] = {"relation": rel, "values": row}
    return out


def _fact_text(fact: Dict[str, Any]) -> str:
    parts = []
    for v in fact["values"]:
        if v["kind"] == "labeled_null":
            parts.append(f"NULL:{v['id']}")
        elif v["kind"] == "string":
            parts.append(json.dumps(v["value"], ensure_ascii=False))
        else:
            parts.append(str(v["value"]))
    return f"{fact['relation']}({', '.join(parts)})"


def diff_payloads(payload_a: Dict[str, Any], payload_b: Dict[str, Any],
                  sources_a: Dict[Tuple[str, str], Tuple[str, Tuple]],
                  sources_b: Dict[Tuple[str, str], Tuple[str, Tuple]],
                  version_a: int, version_b: int) -> Dict[str, Any]:
    """Compute the machine-parseable diff between two version payloads."""
    map_a: Dict[str, Any] = payload_a["input_null_map"]
    map_b: Dict[str, Any] = payload_b["input_null_map"]
    ids_a, ids_b = set(map_a), set(map_b)
    common = ids_a & ids_b

    classes_a = _classes(map_a)
    classes_b = _classes(map_b)

    # bipartite graph between A-class tokens and B-class tokens via shared ids
    adj_a: Dict[Tuple[Any, ...], set] = {}
    adj_b: Dict[Tuple[Any, ...], set] = {}
    for nid in sorted(common):
        ta, tb = _map_token(map_a[nid]), _map_token(map_b[nid])
        adj_a.setdefault(ta, set()).add(tb)
        adj_b.setdefault(tb, set()).add(ta)

    # connected components over the bipartite graph
    seen_a: set = set()
    seen_b: set = set()
    merges: List[Dict[str, Any]] = []
    splits: List[Dict[str, Any]] = []
    regroups: List[Dict[str, Any]] = []
    for start in sorted(adj_a, key=repr):
        if start in seen_a:
            continue
        comp_a: set = set()
        comp_b: set = set()
        stack: List[Tuple[str, Tuple[Any, ...]]] = [("a", start)]
        while stack:
            side, tok = stack.pop()
            if side == "a":
                if tok in comp_a:
                    continue
                comp_a.add(tok)
                stack.extend(("b", t) for t in adj_a.get(tok, ()))
            else:
                if tok in comp_b:
                    continue
                comp_b.add(tok)
                stack.extend(("a", t) for t in adj_b.get(tok, ()))
        seen_a |= comp_a
        seen_b |= comp_b
        if len(comp_a) == 1 and len(comp_b) == 1:
            continue  # 1:1 class correspondence; value changes listed per id
        members_a = sorted({nid for t in comp_a for nid in classes_a[t]})
        members_b = sorted({nid for t in comp_b for nid in classes_b[t]})
        event = {
            "before": [{"token": _token_json(t), "members": classes_a[t]}
                       for t in sorted(comp_a, key=repr)],
            "after": [{"token": _token_json(t), "members": classes_b[t]}
                      for t in sorted(comp_b, key=repr)],
            "members": sorted(set(members_a) | set(members_b)),
        }
        if len(comp_a) > 1 and len(comp_b) == 1:
            event["kind"] = "merge"
            merges.append(event)
        elif len(comp_a) == 1 and len(comp_b) > 1:
            event["kind"] = "split"
            splits.append(event)
        else:
            event["kind"] = "regroup"
            regroups.append(event)

    # per-id value transitions on shared input unknowns
    value_changes: List[Dict[str, Any]] = []
    for nid in sorted(common):
        ta, tb = _map_token(map_a[nid]), _map_token(map_b[nid])
        if ta == tb:
            continue
        if ta[0] == _CLASS and tb[0] == _CONST:
            kind = "became_known"
        elif ta[0] == _CONST and tb[0] == _CLASS:
            kind = "became_unknown"
        elif ta[0] == _CONST and tb[0] == _CONST:
            kind = "constant_changed"
        else:
            kind = "class_changed"
        value_changes.append({
            "input_unknown": nid,
            "kind": kind,
            "before": _token_json(ta),
            "after": _token_json(tb),
        })

    # constant-only fact changes (typed values; "7" != 7 by construction)
    facts_a = _const_facts(payload_a)
    facts_b = _const_facts(payload_b)
    facts_added = [facts_b[k] for k in sorted(set(facts_b) - set(facts_a))]
    facts_removed = [facts_a[k] for k in sorted(set(facts_a) - set(facts_b))]

    # raw source record changes
    keys_a, keys_b = set(sources_a), set(sources_b)
    src_added = []
    for key in sorted(keys_b - keys_a):
        rel, tpl = sources_b[key]
        src_added.append({"source_id": key[0], "record_id": key[1],
                          "relation": rel, "tuple": list(tpl)})
    src_removed = []
    for key in sorted(keys_a - keys_b):
        rel, tpl = sources_a[key]
        src_removed.append({"source_id": key[0], "record_id": key[1],
                            "relation": rel, "tuple": list(tpl)})
    src_replaced = []
    for key in sorted(keys_a & keys_b):
        if sources_a[key] != sources_b[key]:
            src_replaced.append({
                "source_id": key[0], "record_id": key[1],
                "before": {"relation": sources_a[key][0],
                           "tuple": list(sources_a[key][1])},
                "after": {"relation": sources_b[key][0],
                          "tuple": list(sources_b[key][1])},
            })

    return {
        "from_version": version_a,
        "to_version": version_b,
        "from_status": payload_a["status"],
        "to_status": payload_b["status"],
        "input_unknowns": {
            "appeared": sorted(ids_b - ids_a),
            "disappeared": sorted(ids_a - ids_b),
            "merges": merges,
            "splits": splits,
            "regroups": regroups,
            "value_changes": value_changes,
        },
        "constant_facts": {
            "added": facts_added,
            "removed": facts_removed,
        },
        "source_records": {
            "added": src_added,
            "removed": src_removed,
            "replaced": src_replaced,
        },
        "boundaries": [
            "rule-created unknowns (n0, n1, ...) are version-local; no "
            "cross-version identity is claimed for them",
            "only facts composed entirely of known constants are diffed; "
            "facts containing unknowns are not compared, so unknown "
            "renumbering is never reported as a business change",
            "values are typed: the string \"7\" and the int 7 are different "
            "values and are never conflated",
            "merge/split events are class-level: members are reported per "
            "equivalence class, not as pairwise events",
        ],
    }


# --------------------------------------------------------------------------- #
# Human-readable rendering
# --------------------------------------------------------------------------- #
def _tok_text(token_json: Dict[str, Any]) -> str:
    if token_json["type"] == "unknown_class":
        return f"unknown class {token_json['class']}"
    val = token_json["value"]
    if val["kind"] == "string":
        return json.dumps(val["value"], ensure_ascii=False)
    return str(val["value"])


def _members_text(members: Sequence[str]) -> str:
    return "{" + ", ".join(members) + "}"


def render_human(diff: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append(
        f"version {diff['from_version']} ({diff['from_status']}) -> "
        f"version {diff['to_version']} ({diff['to_status']})")
    iu = diff["input_unknowns"]
    lines.append("input unknowns:")
    lines.append(f"  appeared:    {', '.join(iu['appeared']) or '(none)'}")
    lines.append(f"  disappeared: {', '.join(iu['disappeared']) or '(none)'}")
    for label, events in (("MERGE", iu["merges"]), ("SPLIT", iu["splits"]),
                          ("REGROUP", iu["regroups"])):
        for ev in events:
            before = " + ".join(
                f"{_members_text(b['members'])}={_tok_text(b['token'])}"
                for b in ev["before"])
            after = " ; ".join(
                f"{_members_text(a['members'])}=> {_tok_text(a['token'])}"
                for a in ev["after"])
            lines.append(f"  {label}: {before}  ->  {after}")
    if not (iu["merges"] or iu["splits"] or iu["regroups"]):
        lines.append("  class events: (none)")
    if iu["value_changes"]:
        lines.append("  value changes:")
        for vc in iu["value_changes"]:
            lines.append(f"    {vc['input_unknown']}: {_tok_text(vc['before'])} -> "
                         f"{_tok_text(vc['after'])}  ({vc['kind']})")
    else:
        lines.append("  value changes: (none)")
    cf = diff["constant_facts"]
    lines.append("constant-only facts:")
    if cf["added"]:
        lines.append("  added:")
        for f in cf["added"]:
            lines.append(f"    + {_fact_text(f)}")
    if cf["removed"]:
        lines.append("  removed:")
        for f in cf["removed"]:
            lines.append(f"    - {_fact_text(f)}")
    if not (cf["added"] or cf["removed"]):
        lines.append("  (no changes)")
    sr = diff["source_records"]
    lines.append("source records:")
    any_src = False
    for rec in sr["added"]:
        any_src = True
        lines.append(f"  + {rec['source_id']}/{rec['record_id']} "
                     f"{rec['relation']}{tuple(_src_val(t) for t in rec['tuple'])}")
    for rec in sr["removed"]:
        any_src = True
        lines.append(f"  - {rec['source_id']}/{rec['record_id']} "
                     f"{rec['relation']}{tuple(_src_val(t) for t in rec['tuple'])}")
    for rec in sr["replaced"]:
        any_src = True
        lines.append(f"  ~ {rec['source_id']}/{rec['record_id']} "
                     f"{rec['before']['relation']}"
                     f"{tuple(_src_val(t) for t in rec['before']['tuple'])} -> "
                     f"{rec['after']['relation']}"
                     f"{tuple(_src_val(t) for t in rec['after']['tuple'])}")
    if not any_src:
        lines.append("  (no changes)")
    lines.append("boundaries:")
    for b in diff["boundaries"]:
        lines.append(f"  - {b}")
    return "\n".join(lines)


def _src_val(tok: Sequence[Any]) -> object:
    if tok[0] == "n":
        return {"null": tok[1]}
    return tok[1]
