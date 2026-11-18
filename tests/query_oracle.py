"""Independent brute-force evaluator for declarative workspace queries.

This oracle shares no code with :mod:`chase.query`: it reads the *encoded
facts* of a stored completion version as plain Python data, turns them into
typed tokens (``("s", text)`` / ``("i", int)`` / ``("n", null_id)``), and
evaluates the query by nested-loop matching over the fact sets.  It exists so
tests can compare the SQLite-backed executor against a completely different,
obviously-certain evaluator.

Certainty semantics, re-derived independently here:

* equality is token equality — the text ``"7"``, the int ``7`` and an
  unknown id are three different tokens; two facts join on an unknown only
  when they carry the *same* unknown id;
* a column/constant condition fails whenever the column holds any unknown
  (an unknown can never equal a supplied text/int constant);
* a matched binding is emitted only when every projected column evaluates
  to a known constant, and the output set is de-duplicated;
* the deterministic order mirrors the SQLite executor's
  ``(kind, value)`` ordering, so normalized results compare exactly.
"""

from __future__ import annotations

import zlib
from typing import Any, Dict, List, Sequence, Tuple

from chase.model import LNullValue
from chase.package import decode_value
from chase.query import (
    ColumnRef,
    Constant,
    ParsedQuery,
    QueryError,
    RelationUse,
    parse_query,
)
from chase.model import Schema

Token = Tuple[str, Any]  # ("s", str) | ("i", int) | ("n", str)


def token_of(encoded: Dict[str, Any]) -> Token:
    value = decode_value(encoded)
    if isinstance(value, LNullValue):
        return ("n", value.id)
    if isinstance(value, str):
        return ("s", value)
    return ("i", value)


def load_version_facts(ws, version: int) -> Dict[str, List[Tuple[Token, ...]]]:
    """Read one stored version's completed facts as token tuples."""
    import json
    row = ws.conn.execute(
        "SELECT payload FROM versions WHERE version=?", (version,)).fetchone()
    if row is None:
        raise QueryError(f"no version {version}")
    payload = json.loads(zlib.decompress(row["payload"]))
    facts: Dict[str, List[Tuple[Token, ...]]] = {}
    for rel, rows in payload["facts"].items():
        facts[rel] = [tuple(token_of(v) for v in r) for r in rows]
    return facts


def _ref_token(ref: ColumnRef, binding: Dict[str, RelationUse],
               rows: Dict[str, Tuple[Token, ...]]) -> Token:
    use = binding[ref.alias]
    return rows[use.alias][ref.position]


def brute_force(query_obj: object, schema: Schema,
                facts: Dict[str, List[Tuple[Token, ...]]]) -> List[List[Dict[str, Any]]]:
    """Evaluate ``query_obj`` over ``facts`` with plain nested loops."""
    query: ParsedQuery = parse_query(query_obj, schema)
    aliases = {u.alias: u for u in query.relations}

    # one cartesian product of the participating (possibly repeated) relations
    def rec(i: int, rows: Dict[str, Tuple[Token, ...]]):
        if i == len(query.relations):
            yield dict(rows)
            return
        use = query.relations[i]
        for row in facts.get(use.relation, []):
            rows[use.alias] = row
            yield from rec(i + 1, rows)
        rows.pop(use.alias, None)

    def condition_holds(cond) -> bool:
        kind, left, right = cond
        if kind == "colcol":
            return _ref_token(left, aliases, rows) == _ref_token(right, aliases, rows)
        const: Constant = right
        token = _ref_token(left, aliases, rows)
        # unknowns never equal supplied constants
        if token[0] == "n":
            return False
        return token == const.key()

    out: set = set()
    for rows in rec(0, {}):
        if not all(condition_holds(c) for c in query.conditions):
            continue
        projected: List[Token] = []
        certain = True
        for sc in query.select:
            token = _ref_token(sc.ref, aliases, rows)
            if token[0] == "n":
                certain = False
                break
            projected.append(token)
        if not certain:
            continue
        out.add(tuple(projected))

    def sort_key(token: Token):
        # mirror the SQL ORDER BY kind, val: "i" < "s" alphabetically,
        # and the value's own comparison within a kind
        return token[0], token[1]

    ordered = sorted(out, key=lambda t: tuple(sort_key(x) for x in t))
    return [[{"kind": ("string" if t[0] == "s" else "int"), "value": t[1]}
             for t in row] for row in ordered]
