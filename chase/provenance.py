"""Provenance database shipped with every package.

Three tables answer "where did this come from?":

``fact_origin``  for every final fact: input source+line, or the rule trigger
                 that derived it;
``trigger``      each TGD firing: rule, body binding, support facts, head row,
                 and the existentials it created (so a labeled null's birth is
                 visible);
``merge``        each EGD-driven unification: rule, binding, the two unified
                 values, the result, and the supporting rows (so an equality
                 proof is visible).

The database is also the trace backend of ``chase trace``.  It is written to a
scratch file and copied into the package only if complete.
"""

from __future__ import annotations

import json
import os
import sqlite3
from typing import Any, Dict, List, Sequence, Tuple

from .model import LNullValue


class ProvenanceDB:
    def __init__(self, path: str, readonly: bool = False) -> None:
        self.path = path
        if readonly:
            self.conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
            self.conn.row_factory = sqlite3.Row
        else:
            parent = os.path.dirname(os.path.abspath(path))
            if parent:
                os.makedirs(parent, exist_ok=True)
            if os.path.exists(path):
                os.remove(path)
            self.conn = sqlite3.connect(path)
            self.conn.row_factory = sqlite3.Row
            self.conn.executescript("""
                CREATE TABLE trigger (
                    id INTEGER PRIMARY KEY,
                    trigger_key TEXT UNIQUE NOT NULL,
                    rule TEXT NOT NULL,
                    binding TEXT NOT NULL,
                    existentials TEXT NOT NULL,
                    head_fact TEXT NOT NULL,
                    support TEXT NOT NULL
                );
                CREATE TABLE merge (
                    id INTEGER PRIMARY KEY,
                    rule TEXT NOT NULL,
                    binding TEXT NOT NULL,
                    unified TEXT NOT NULL,
                    result TEXT NOT NULL,
                    support TEXT NOT NULL
                );
                CREATE TABLE fact_origin (
                    id INTEGER PRIMARY KEY,
                    rel TEXT NOT NULL,
                    vals TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    rule TEXT,
                    trigger_key TEXT,
                    source TEXT,
                    origin JSON NOT NULL
                );
                CREATE INDEX ix_fact_rel ON fact_origin(rel);
                CREATE INDEX ix_merge_rule ON merge(rule);
            """)

    def ingest(self, triggers: Sequence[Dict[str, Any]],
               merges: Sequence[Dict[str, Any]],
               origins: Dict[str, Dict[str, Any]],
               facts: Dict[str, List[Tuple[object, ...]]]) -> None:
        for t in triggers:
            head = f"{t['head'][0]}({', '.join(_show(v) for v in t['head'][1])})"
            self.conn.execute(
                "INSERT OR IGNORE INTO trigger(trigger_key, rule, binding, existentials, "
                "head_fact, support) VALUES (?,?,?,?,?,?)",
                (t["trigger"], t["rule"], json.dumps(t["binding"], ensure_ascii=False),
                 json.dumps(t["existentials"], ensure_ascii=False), head,
                 json.dumps(t["support"], ensure_ascii=False)))
        for m in merges:
            self.conn.execute(
                "INSERT INTO merge(rule, binding, unified, result, support) "
                "VALUES (?,?,?,?,?)",
                (m["rule"], json.dumps(m["binding"], ensure_ascii=False),
                 json.dumps([m["unified"][0], m["unified"][1]], ensure_ascii=False),
                 json.dumps(m["result"], ensure_ascii=False),
                 json.dumps(m["support"], ensure_ascii=False)))
        # Canonical text form of vals, as fact_origin.trace_pattern reads it;
        # used only to join the staged fact set to provenance rows.
        self.conn.execute("CREATE INDEX IF NOT EXISTS ix_fact_rel_vals "
                          "ON fact_origin(rel, vals)")
        keyed: Dict[str, Tuple[str, List[List[object]], Dict[str, Any]]] = {}
        for rel, rows in facts.items():
            for row in rows:
                key = _fact_key(rel, row)
                keyed[key] = (rel, _ks_row(row),
                              origins.get(key, {"kind": "derived",
                                                "note": "origin lost"}))
        for key, (rel, ks_row, origin) in keyed.items():
            self.conn.execute(
                "INSERT INTO fact_origin(rel, vals, kind, rule, trigger_key, source, origin) "
                "VALUES (?,?,?,?,?,?,?)",
                (rel, json.dumps(ks_row, ensure_ascii=False),  # NOSONAR
                 origin.get("kind", "unknown"), origin.get("rule"),
                 origin.get("trigger"), origin.get("source"),
                 json.dumps(origin, ensure_ascii=False, default=str)))
        self.conn.commit()

    # -- schema snapshot ------------------------------------------------------ #
    def write_schema(self, schema_obj: Dict[str, Any],
                     rules_obj: Sequence[Dict[str, Any]]) -> None:
        for t in ("relation", "rule"):
            self.conn.execute(f"DROP TABLE IF EXISTS {t}")
        self.conn.execute(
            "CREATE TABLE relation (name TEXT PRIMARY KEY, types TEXT NOT NULL)")
        for rel, cols in schema_obj.items():
            self.conn.execute("INSERT INTO relation(name, types) VALUES (?,?)",
                              (rel, json.dumps(cols)))
        self.conn.execute(
            "CREATE TABLE rule (id TEXT PRIMARY KEY, kind TEXT NOT NULL, "
            "weak_acyclic INTEGER NOT NULL, definition TEXT NOT NULL)")
        for r in rules_obj:
            self.conn.execute(
                "INSERT INTO rule(id, kind, weak_acyclic, definition) VALUES (?,?,?,?)",
                (r["id"], r["kind"], 1 if r.get("weak_acyclic") else 0,
                 json.dumps(r, ensure_ascii=False)))
        self.conn.commit()

    # -- queries ------------------------------------------------------------- #
    def trace_null(self, null_id: str) -> Dict[str, Any]:
        out: Dict[str, Any] = {"null": null_id, "sections": []}
        pc = f'%"n", "{null_id}"%'
        for row in self.conn.execute(
                "SELECT * FROM trigger WHERE existentials LIKE ? OR head_fact LIKE ?",
                (pc, f"%NULL:{null_id}%")):
            out["sections"].append({"kind": "created_by_trigger", "rule": row["rule"],
                                    "trigger": row["trigger_key"],
                                    "binding": json.loads(row["binding"]),
                                    "head": row["head_fact"],
                                    "support": json.loads(row["support"])})
        for row in self.conn.execute(
                "SELECT * FROM merge WHERE unified LIKE ? OR result LIKE ?", (pc, pc)):
            out["sections"].append({"kind": "unified_by_egd", "rule": row["rule"],
                                    "binding": json.loads(row["binding"]),
                                    "unified": json.loads(row["unified"]),
                                    "result": json.loads(row["result"]),
                                    "support": json.loads(row["support"])})
        out["facts"] = self.trace_pattern(f"*NULL:{null_id}*")
        return out

    def trace_fact(self, fact: str) -> List[Dict[str, Any]]:
        rows = []
        needle = fact.strip()
        for row in self.conn.execute("SELECT * FROM fact_origin"):
            rendered = _render(row["rel"], json.loads(row["vals"]))
            if rendered == needle:
                rows.append({"fact": rendered, "kind": row["kind"],
                             "rule": row["rule"], "trigger": row["trigger_key"],
                             "source": row["source"],
                             "origin": json.loads(row["origin"])})
        return rows

    def trace_pattern(self, pattern: str) -> List[Dict[str, Any]]:
        """Pattern like ``Customer("c", *)``; ``*NULL:n0*`` matches containing."""
        if pattern.startswith("*") and pattern.endswith("*"):
            needle = pattern.strip("*")
            out = []
            for row in self.conn.execute("SELECT * FROM fact_origin"):
                rendered = _render(row["rel"], json.loads(row["vals"]))
                if needle in rendered:
                    out.append({"fact": rendered, "kind": row["kind"],
                                "rule": row["rule"], "trigger": row["trigger_key"],
                                "source": row["source"]})
            return out
        from .trace_parse import parse_pattern
        parsed = parse_pattern(pattern)
        out = []
        for row in self.conn.execute("SELECT * FROM fact_origin WHERE rel=?",
                                     (parsed.rel,)):
            vals = json.loads(row["vals"])
            if parsed.matches(vals):
                out.append({"fact": _render(row["rel"], vals), "kind": row["kind"],
                            "rule": row["rule"], "trigger": row["trigger_key"],
                            "source": row["source"],
                            "values": vals})
        return out

    def close(self) -> Dict[str, Any]:
        counts = {}
        try:
            for t in ("trigger", "merge", "fact_origin"):
                counts[t] = self.conn.execute(
                    f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        except sqlite3.Error:
            pass
        self.conn.close()
        return counts


def _fact_list(raw: Sequence[Sequence[object]]) -> List[str]:
    """[relation, [tokens]] -> ["Rel(..)", ...] for readable trace output."""
    out = []
    for entry in raw:
        if isinstance(entry, dict):
            out.append(entry.get("fact", entry))
        elif isinstance(entry, (list, tuple)) and len(entry) == 2 \
                and isinstance(entry[0], str) and isinstance(entry[1], (list, tuple)):
            rel, vals = entry
            if vals and isinstance(vals[0], str):
                out.append(f"{rel}({', '.join(vals)})")
            else:
                out.append(_render(rel, vals))
        else:
            out.append(entry)
    return out


def _show(v: Sequence[object]) -> str:
    if v[0] == "n":
        return f"NULL:{v[1]}"
    if v[0] == "s":
        return json.dumps(v[1], ensure_ascii=False)
    return str(v[1])


def _ks_row(row: Sequence[object]) -> List[List[object]]:
    """Encode final values as ``[kind, payload]`` tokens for provenance rows."""
    out: List[List[object]] = []
    for v in row:
        if isinstance(v, LNullValue):
            out.append(["n", v.id])
        elif isinstance(v, str):
            out.append(["s", v])
        else:
            out.append(["i", v])
    return out


def _render(rel: str, vals: Sequence[Sequence[object]]) -> str:
    return f"{rel}({', '.join(_show(v) for v in vals)})"


def _fact_key(rel: str, row: Sequence[object]) -> str:
    return f"{rel}|{json.dumps(_ks_row(row), ensure_ascii=False, sort_keys=True)}"
