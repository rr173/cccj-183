"""Independent verification of an exported workspace version package.

This goes beyond the stock package verification (which re-checks TGD/EGD
satisfaction, input preservation, types and null bookkeeping from disk,
without trusting the status marker).  For a workspace export it additionally
re-checks the *source chain*:

1. ``sources.json`` records are replayed into a deduplicated input fact set
   which must equal ``package.json``'s ``input_facts`` — so the exported raw
   sources really are what the completion ran on;
2. the recorded input-unknown -> final-value map must agree with
   ``nulls.json`` (a constant claim must match ``unified_with``, a class
   claim must match the documented class, and the class must be live);
3. every source record, canonicalized through that map, must land on a fact
   that is actually in ``facts.csv`` and must be listed under that fact in
   ``fact_sources`` — and every ``fact_sources`` entry must canonicalize
   back to its fact (the manifest cannot cite records it does not have, nor
   hide extra ones);
4. the workspace version identity in ``package.json`` and ``sources.json``
   must agree.

Nothing is believed because a status marker says so: a tampered source
manifest, a tampered null map or a tampered fact table fails here even when
the marker still claims ``complete``.
"""

from __future__ import annotations

import csv
import json
import os
from typing import Any, Dict, List, Optional, Tuple

from .model import ModelError
from .pkgverify import verify_package_dir
from .wsexport import SOURCES_FORMAT

Token = Tuple[str, Any]


def _tok_from_encoded(obj: Any, where: str) -> Token:
    if not isinstance(obj, dict) or "kind" not in obj:
        raise ModelError(f"{where}: invalid encoded value {obj!r}")
    kind = obj["kind"]
    if kind == "labeled_null":
        nid = obj.get("id")
        if not isinstance(nid, str) or not nid:
            raise ModelError(f"{where}: labeled null needs a non-empty string id")
        return ("n", nid)
    if kind == "string":
        v = obj.get("value")
        if not isinstance(v, str):
            raise ModelError(f"{where}: string value must hold a str")
        return ("s", v)
    if kind == "int":
        v = obj.get("value")
        if not isinstance(v, int) or isinstance(v, bool):
            raise ModelError(f"{where}: int value must hold a non-bool int")
        return ("i", v)
    raise ModelError(f"{where}: unknown value kind {kind!r}")


def _tok_from_raw(obj: Any, where: str) -> Token:
    """Token from a scenario-style raw value (str/int/{"null": id})."""
    if isinstance(obj, bool):
        raise ModelError(f"{where}: booleans are not valid values")
    if isinstance(obj, str):
        return ("s", obj)
    if isinstance(obj, int):
        return ("i", obj)
    if isinstance(obj, dict):
        nid = obj.get("null")
        if isinstance(nid, str) and nid:
            return ("n", nid)
    raise ModelError(f"{where}: invalid raw value {obj!r}")


def _tok_to_encoded(tok: Token) -> Dict[str, Any]:
    if tok[0] == "n":
        return {"kind": "labeled_null", "id": tok[1]}
    if tok[0] == "s":
        return {"kind": "string", "value": tok[1]}
    return {"kind": "int", "value": tok[1]}


def _fact_key(rel: str, toks: Tuple[Token, ...]) -> Tuple[str, Tuple[Token, ...]]:
    return (rel, toks)


def _load_json(path: str, what: str) -> Any:
    if not os.path.isfile(path):
        raise ModelError(f"missing {what}: {path}")
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def verify_export(directory: str, verify_cap: Optional[int] = None
                  ) -> Dict[str, Any]:
    """Verify an exported workspace version package from disk."""
    directory = os.path.abspath(directory)
    problems: List[Dict[str, Any]] = []
    checks: Dict[str, Any] = {}

    pkg_meta = _load_json(os.path.join(directory, "package.json"), "package.json")
    sources = _load_json(os.path.join(directory, "sources.json"), "sources.json")
    nulls_obj = _load_json(os.path.join(directory, "nulls.json"), "nulls.json")

    # -- 0. identity -------------------------------------------------------- #
    ws_block = pkg_meta.get("workspace")
    if not isinstance(ws_block, dict):
        problems.append({"check": "identity",
                         "problem": "package.json has no 'workspace' block; "
                                    "not a workspace export"})
        ws_block = {}
    if sources.get("format") != SOURCES_FORMAT:
        problems.append({"check": "identity",
                         "problem": f"sources.json format is not {SOURCES_FORMAT}"})
    claimed_status = pkg_meta.get("status")
    for field, a, b in (
            ("version", ws_block.get("version"), sources.get("workspace_version")),
            ("batch_id", ws_block.get("batch_id"), sources.get("batch_id")),
            ("status", claimed_status, sources.get("status"))):
        if a != b:
            problems.append({"check": "identity",
                             "problem": f"{field} disagrees: package.json says "
                                        f"{a!r}, sources.json says {b!r}"})
    checks["version"] = ws_block.get("version")
    checks["claimed_status"] = claimed_status

    # -- 1. records -> input_facts ------------------------------------------ #
    records = sources.get("records")
    if not isinstance(records, list):
        problems.append({"check": "sources", "problem": "sources.json 'records' "
                                                        "must be a list"})
        records = []
    record_keys: set = set()
    recomputed: Dict[str, set] = {}
    unknown_ids_in_records: set = set()
    record_tuples: Dict[Tuple[str, str], Tuple[str, Tuple[Token, ...]]] = {}
    for i, rec in enumerate(records):
        where = f"records[{i}]"
        if not isinstance(rec, dict):
            problems.append({"check": "sources", "problem": f"{where}: not an object"})
            continue
        sid, rid, rel = rec.get("source_id"), rec.get("record_id"), rec.get("relation")
        key = (sid, rid)
        if not all(isinstance(x, str) and x for x in (sid, rid, rel)):
            problems.append({"check": "sources",
                             "problem": f"{where}: bad source_id/record_id/relation"})
            continue
        if key in record_keys:
            problems.append({"check": "sources",
                             "problem": f"duplicate record key {sid}/{rid}"})
        record_keys.add(key)
        tpl_raw = rec.get("tuple")
        if not isinstance(tpl_raw, list):
            problems.append({"check": "sources",
                             "problem": f"{where}: 'tuple' must be a list"})
            continue
        toks = tuple(_tok_from_encoded(v, where) for v in tpl_raw)
        for t in toks:
            if t[0] == "n":
                unknown_ids_in_records.add(t[1])
        record_tuples[key] = (rel, toks)
        recomputed.setdefault(rel, set()).add(toks)

    input_facts_meta = pkg_meta.get("input_facts")
    if not isinstance(input_facts_meta, dict):
        problems.append({"check": "sources",
                         "problem": "package.json has no input_facts object"})
        input_facts_meta = {}
    recorded: Dict[str, set] = {}
    for rel, rows in input_facts_meta.items():
        acc = recorded.setdefault(rel, set())
        for j, raw_row in enumerate(rows):
            acc.add(tuple(_tok_from_raw(v, f"input_facts.{rel}[{j}]")
                          for v in raw_row))
    for rel in sorted(set(recomputed) | set(recorded)):
        missing = recomputed.get(rel, set()) - recorded.get(rel, set())
        extra = recorded.get(rel, set()) - recomputed.get(rel, set())
        if missing:
            problems.append({"check": "sources",
                             "problem": f"input_facts of {rel} miss tuples present "
                                        f"in sources.json records",
                             "tuples": sorted(map(repr, missing))[:5]})
        if extra:
            problems.append({"check": "sources",
                             "problem": f"input_facts of {rel} contain tuples no "
                                        f"source record produces",
                             "tuples": sorted(map(repr, extra))[:5]})
    checks["source_records"] = len(record_keys)
    checks["input_facts"] = sum(len(v) for v in recomputed.values())

    # -- 2. input unknown map vs records and nulls.json ---------------------- #
    inm_pkg = pkg_meta.get("input_null_map")
    inm_src = sources.get("input_null_map")
    if inm_pkg != inm_src:
        problems.append({"check": "null_map",
                         "problem": "input_null_map disagrees between package.json "
                                    "and sources.json"})
    null_map: Dict[str, Any] = inm_src if isinstance(inm_src, dict) else {}
    if set(null_map) != unknown_ids_in_records:
        problems.append({
            "check": "null_map",
            "problem": "input_null_map ids do not match the unknown ids occurring "
                       "in the source records",
            "map_only": sorted(set(null_map) - unknown_ids_in_records),
            "records_only": sorted(unknown_ids_in_records - set(null_map))})
    nulls_doc = nulls_obj.get("nulls", {}) if isinstance(nulls_obj, dict) else {}
    live = set(nulls_obj.get("live", [])) if isinstance(nulls_obj, dict) else set()
    for nid in sorted(null_map):
        entry = null_map[nid]
        doc = nulls_doc.get(nid)
        if not isinstance(entry, dict) or "kind" not in entry:
            problems.append({"check": "null_map",
                             "problem": f"bad input_null_map entry for {nid!r}"})
            continue
        if not isinstance(doc, dict):
            problems.append({"check": "null_map",
                             "problem": f"input unknown {nid!r} is not documented "
                                        f"in nulls.json"})
            continue
        if entry["kind"] == "labeled_null":
            cls = entry.get("id")
            if doc.get("class") != cls or doc.get("unified_with") is not None:
                problems.append({"check": "null_map",
                                 "problem": f"{nid!r}: map says class {cls!r} but "
                                            f"nulls.json says class "
                                            f"{doc.get('class')!r} / unified_with "
                                            f"{doc.get('unified_with')!r}"})
            if cls not in live:
                problems.append({"check": "null_map",
                                 "problem": f"{nid!r}: final class {cls!r} is not a "
                                            f"live null of the instance"})
        else:
            expect = ["s" if entry["kind"] == "string" else "i", entry.get("value")]
            if doc.get("unified_with") != expect or doc.get("class") is not None:
                problems.append({"check": "null_map",
                                 "problem": f"{nid!r}: map says constant "
                                            f"{expect!r} but nulls.json says "
                                            f"unified_with "
                                            f"{doc.get('unified_with')!r} / class "
                                            f"{doc.get('class')!r}"})
    checks["input_unknowns"] = len(null_map)

    # -- 3. fact_sources chain: records -> canonical facts ------------------- #
    facts_csv: Dict[Tuple[str, Tuple[Token, ...]], None] = {}
    facts_path = os.path.join(directory, "facts.csv")
    if not os.path.isfile(facts_path):
        problems.append({"check": "facts", "problem": "missing facts.csv"})
    else:
        with open(facts_path, encoding="utf-8", newline="") as fh:
            reader = csv.reader(fh)
            header = next(reader, None)
            if header != ["relation", "values"]:
                problems.append({"check": "facts",
                                 "problem": "bad facts.csv header"})
            for row in reader:
                if not row:
                    continue
                rel, payload = row[0], row[1]
                toks = tuple(_tok_from_encoded(v, f"facts.csv {rel}")
                             for v in json.loads(payload))
                facts_csv[_fact_key(rel, toks)] = None

    def canon(rel: str, toks: Tuple[Token, ...]) -> Tuple[str, Tuple[Token, ...]]:
        out: List[Token] = []
        for t in toks:
            if t[0] == "n":
                entry = null_map.get(t[1])
                if entry is None:
                    out.append(t)  # will fail the map check above anyway
                    continue
                if entry["kind"] == "labeled_null":
                    out.append(("n", entry["id"]))
                elif entry["kind"] == "string":
                    out.append(("s", entry["value"]))
                else:
                    out.append(("i", entry["value"]))
            else:
                out.append(t)
        return _fact_key(rel, tuple(out))

    fact_sources = sources.get("fact_sources")
    if not isinstance(fact_sources, list):
        problems.append({"check": "fact_sources",
                         "problem": "sources.json 'fact_sources' must be a list"})
        fact_sources = []
    manifest: Dict[Tuple[str, Tuple[Token, ...]], set] = {}
    for i, fs in enumerate(fact_sources):
        where = f"fact_sources[{i}]"
        try:
            fact = fs["fact"]
            frel = fact["relation"]
            ftoks = tuple(_tok_from_encoded(v, where) for v in fact["values"])
            fkey = _fact_key(frel, ftoks)
        except (KeyError, TypeError, ModelError) as exc:
            problems.append({"check": "fact_sources",
                             "problem": f"{where}: malformed fact: {exc}"})
            continue
        recs = manifest.setdefault(fkey, set())
        for pair in fs.get("records", []):
            if (not isinstance(pair, list) or len(pair) != 2
                    or not all(isinstance(x, str) for x in pair)):
                problems.append({"check": "fact_sources",
                                 "problem": f"{where}: bad record key {pair!r}"})
                continue
            recs.add((pair[0], pair[1]))

    # every record canonicalizes to a listed, existing fact
    for key, (rel, toks) in sorted(record_tuples.items()):
        fkey = canon(rel, toks)
        if fkey not in facts_csv:
            problems.append({"check": "fact_sources",
                             "problem": f"record {key[0]}/{key[1]} canonicalizes to "
                                        f"{fkey[0]}{fkey[1]} which is not in "
                                        f"facts.csv"})
        if key not in manifest.get(fkey, set()):
            problems.append({"check": "fact_sources",
                             "problem": f"record {key[0]}/{key[1]} is not listed as "
                                        f"a source of its fact "
                                        f"{fkey[0]}{fkey[1]}"})
    # every manifest entry cites existing records and lands on an existing fact
    for fkey, recs in sorted(manifest.items(), key=lambda kv: repr(kv[0])):
        if fkey not in facts_csv:
            problems.append({"check": "fact_sources",
                             "problem": f"fact_sources lists {fkey[0]}{fkey[1]} "
                                        f"which is not in facts.csv"})
        for key in sorted(recs):
            if key not in record_tuples:
                problems.append({"check": "fact_sources",
                                 "problem": f"fact_sources cites unknown record "
                                            f"{key[0]}/{key[1]}"})
            elif canon(*record_tuples[key]) != fkey:
                problems.append({"check": "fact_sources",
                                 "problem": f"record {key[0]}/{key[1]} canonicalizes "
                                            f"to {canon(*record_tuples[key])}, not "
                                            f"to the cited fact {fkey[0]}{fkey[1]}"})
    checks["facts"] = len(facts_csv)
    checks["fact_source_links"] = sum(len(v) for v in manifest.values())

    # -- 4. stock package verification (instance vs program) ----------------- #
    try:
        pkg_report = verify_package_dir(directory, verify_cap=verify_cap)
    except (ModelError, OSError, json.JSONDecodeError) as exc:
        pkg_report = {"ok": False, "error": str(exc)}
        problems.append({"check": "package",
                         "problem": f"package does not load/verify: {exc}"})
    checks["package_checks"] = pkg_report.get("checks", {})
    consistent = (claimed_status == "complete") == bool(pkg_report.get("ok"))
    if not consistent:
        problems.append({"check": "status",
                         "problem": f"claimed status {claimed_status!r} does not "
                                    f"match the independent instance check "
                                    f"(ok={pkg_report.get('ok')})"})

    ok = not problems
    return {
        "ok": ok,
        "directory": directory,
        "version": ws_block.get("version"),
        "claimed_status": claimed_status,
        "manifest_ok": not any(p["check"] in ("identity", "sources", "null_map",
                                              "fact_sources", "facts")
                               for p in problems),
        "package_ok": bool(pkg_report.get("ok")),
        "status_consistent": consistent,
        "checks": checks,
        "problems": problems,
        "package_verification": pkg_report,
    }
