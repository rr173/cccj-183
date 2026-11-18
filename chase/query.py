"""Declarative, version-bound relational queries over a completed workspace.

The query language is a *restricted, declarative JSON* dialect — no SQL, no
Python expressions, no negation, no aggregation, no user code:

    {
      "relations": [{"name": "Order", "alias": "o"},
                    {"name": "Customer", "alias": "c"}],
      "select":    [{"column": "o.order_id", "name": "order_id"},
                    {"column": "c.address",  "name": "address"}],
      "where":     [{"eq": ["o.customer", "c.customer"]},
                    {"eq": ["c.address", {"type": "text", "value": "东区"}]}]
    }

Semantics (the certainty rules)
-------------------------------
* A query runs against the **completed facts of one pinned completion
  version** (rules' results included), never against raw sources alone.
* Equality is on the typed value identity used by the whole system:
  ``("s", text)`` / ``("i", int)`` / ``("n", null_id)``.  The text ``"7"``,
  the int ``7`` and an unknown are three different values and never compare
  equal.  Two occurrences of the *same* unknown id are the same unknown and
  do join; two different unknown ids never compare equal, regardless of any
  display-name similarity.
* Unknowns **may participate in joins** (column/column equality), but a
  column can never equal a supplied constant when it holds an unknown, and a
  result tuple is emitted only when **every projected column is a known
  constant**.  Unknowns therefore never leak into output as if they were
  ordinary text such as ``"n0"``.
* Output is the set of distinct, fully-known projected tuples, in a stable
  order.  An empty result means "no *certain* answer in this version", not
  "no matching row exists in reality".

Execution
---------
The pinned version's fact payload is streamed through a temporary on-disk
:class:`~chase.store.Store` (SQLite), one relation at a time and in bounded
batches — neither the whole payload nor any full join result is accumulated
in Python memory.  The query itself is one parameterized SQLite statement
that uses the store's indexes; result rows are read and written in bounded
batches.  Row/time caps, when reached, are reported as an explicit
``truncated`` status rather than presented as a complete answer.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import time
import zlib
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple

from .model import DOMAINS, ModelError, Schema, schema_from_json
from .package import decode_value
from .store import Store

ALIAS_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
OUT_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
QUERY_FORMAT = "workspace-declarative-query/1"


class QueryError(Exception):
    """The query is rejected (structure, reference or type violation)."""


# --------------------------------------------------------------------------- #
# Parsed query model
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ColumnRef:
    alias: str
    column: str          # symbolic or positional column name
    position: int
    domain: str          # "text" | "int"


@dataclass(frozen=True)
class Constant:
    domain: str          # "text" | "int"
    value: object        # str | int

    def key(self) -> Tuple[str, Any]:
        return ("s", self.value) if self.domain == "text" else ("i", self.value)


@dataclass(frozen=True)
class RelationUse:
    relation: str
    alias: str
    index: int           # position in the relations list / SQL alias q{index}


@dataclass(frozen=True)
class SelectColumn:
    ref: ColumnRef
    name: str


# conditions are ("colcol", ColumnRef, ColumnRef) | ("colconst", ColumnRef, Constant)
Condition = Tuple[str, Any, Any]


@dataclass
class ParsedQuery:
    relations: List[RelationUse]
    select: List[SelectColumn]
    conditions: List[Condition]
    canonical: Dict[str, Any] = field(default_factory=dict)
    digest: str = ""

    def output_columns(self) -> List[Dict[str, str]]:
        return [{"name": s.name, "type": s.ref.domain} for s in self.select]


# --------------------------------------------------------------------------- #
# Validation / parsing
# --------------------------------------------------------------------------- #
_ALLOWED_TOP = {"relations", "select", "where"}
_ALLOWED_REL = {"name", "alias"}
_ALLOWED_SEL = {"column", "name"}
_ALLOWED_COND = {"eq"}
_ALLOWED_CONST = {"type", "value"}


def _reject(detail: str) -> None:
    raise QueryError(detail)


def _column_ref(raw: object, aliases: Dict[str, RelationUse],
                schema: Schema, where: str) -> ColumnRef:
    if not isinstance(raw, str):
        _reject(f"{where}: column reference must be a string 'alias.column', "
                f"got {raw!r}")
    parts = raw.split(".")
    if len(parts) != 2 or not parts[0] or not parts[1]:
        _reject(f"{where}: malformed column reference {raw!r}; expected 'alias.column'")
    alias, column = parts
    use = aliases.get(alias)
    if use is None:
        _reject(f"{where}: unknown relation alias {alias!r} in {raw!r}; declared "
                f"aliases: {sorted(aliases)}")
    try:
        position = schema.column_position(use.relation, column)
    except ModelError as exc:
        _reject(f"{where}: {exc}")
    domain = schema.columns(use.relation)[position]
    return ColumnRef(alias, column, position, domain)


def _constant(raw: object, where: str) -> Constant:
    """A typed query constant.  Labeled nulls are explicitly rejected here."""
    if not isinstance(raw, dict):
        _reject(f"{where}: constant must be an object "
                f"{{'type': 'text'|'int', 'value': ...}}, got {raw!r}")
    extra = set(raw) - _ALLOWED_CONST
    if extra:
        _reject(f"{where}: unsupported constant field(s) {sorted(extra)}; only "
                f"'type' and 'value' are allowed")
    typ = raw.get("type")
    if typ not in DOMAINS:
        _reject(f"{where}: constant 'type' must be one of {list(DOMAINS)}, got {typ!r}")
    if "value" not in raw:
        _reject(f"{where}: constant is missing 'value'")
    value = raw["value"]
    if isinstance(value, bool):
        _reject(f"{where}: booleans are not relational values")
    if typ == "text":
        if not isinstance(value, str):
            _reject(f"{where}: text constant must hold a JSON string, got "
                    f"{type(value).__name__}")
        return Constant("text", value)
    if not isinstance(value, int):
        _reject(f"{where}: int constant must hold a JSON integer, got "
                f"{type(value).__name__}")
    return Constant("int", value)


def parse_query(obj: object, schema: Schema) -> ParsedQuery:
    """Validate the whole query against ``schema``; reject everything else.

    Every name, alias, column reference, constant type and duplicated
    definition is checked.  Unsupported structures (unknown keys, operators
    other than equality, null constants, negation, aggregation, ...) are
    rejected outright — a condition is never silently dropped.
    """
    if not isinstance(obj, dict):
        _reject("query must be a JSON object")
    extra = set(obj) - _ALLOWED_TOP
    if extra:
        _reject(f"unsupported top-level field(s) {sorted(extra)}; allowed fields are "
                f"{sorted(_ALLOWED_TOP)} (no negation, aggregation, arithmetic or "
                f"user-defined expressions)")

    rels_raw = obj.get("relations")
    if not isinstance(rels_raw, list) or not rels_raw:
        _reject("'relations' must be a non-empty list of {name, alias} entries")
    uses: List[RelationUse] = []
    aliases: Dict[str, RelationUse] = {}
    seen_relations: List[str] = []
    for i, entry in enumerate(rels_raw):
        where = f"relations[{i}]"
        if not isinstance(entry, dict):
            _reject(f"{where}: entry must be an object")
        if set(entry) - _ALLOWED_REL:
            _reject(f"{where}: unsupported field(s) {sorted(set(entry) - _ALLOWED_REL)}")
        name, alias = entry.get("name"), entry.get("alias")
        if not isinstance(name, str) or name not in schema.relations:
            _reject(f"{where}: relation {name!r} is not declared in the workspace schema "
                    f"({sorted(schema.relations)})")
        if not isinstance(alias, str) or not ALIAS_RE.match(alias):
            _reject(f"{where}: alias must be a non-empty identifier matching "
                    f"{ALIAS_RE.pattern}, got {alias!r}")
        if alias in aliases:
            _reject(f"{where}: duplicate relation alias {alias!r}; every participating "
                    f"relation needs a unique alias (self-joins use two aliases)")
        use = RelationUse(name, alias, i)
        uses.append(use)
        aliases[alias] = use
        seen_relations.append(name)

    select_raw = obj.get("select")
    if not isinstance(select_raw, list) or not select_raw:
        _reject("'select' must be a non-empty list of {column, name?} entries")
    selects: List[SelectColumn] = []
    out_names: set = set()
    for i, entry in enumerate(select_raw):
        where = f"select[{i}]"
        if not isinstance(entry, dict):
            _reject(f"{where}: entry must be an object")
        if set(entry) - _ALLOWED_SEL:
            _reject(f"{where}: unsupported field(s) {sorted(set(entry) - _ALLOWED_SEL)}")
        ref = _column_ref(entry.get("column"), aliases, schema, where)
        name = entry.get("name", f"{ref.alias}_{ref.column}")
        if not isinstance(name, str) or not OUT_NAME_RE.match(name):
            _reject(f"{where}: output name must match {OUT_NAME_RE.pattern}, got {name!r}")
        if name in out_names:
            _reject(f"{where}: duplicate output column name {name!r}")
        out_names.add(name)
        selects.append(SelectColumn(ref, name))

    conditions: List[Condition] = []
    where_raw = obj.get("where", [])
    if not isinstance(where_raw, list):
        _reject("'where' must be a list of equality conditions")
    for i, entry in enumerate(where_raw):
        where = f"where[{i}]"
        if not isinstance(entry, dict):
            _reject(f"{where}: condition must be an object")
        if set(entry) - _ALLOWED_COND:
            _reject(f"{where}: only equality conditions of the form "
                    f"{{'eq': [term, term]}} are supported; got field(s) "
                    f"{sorted(set(entry))}")
        eq = entry.get("eq")
        if not isinstance(eq, list) or len(eq) != 2:
            _reject(f"{where}: 'eq' must be a two-element list [term, term]")
        left_raw, right_raw = eq
        left_is_ref = isinstance(left_raw, str)
        right_is_ref = isinstance(right_raw, str)
        if left_is_ref and right_is_ref:
            left = _column_ref(left_raw, aliases, schema, where)
            right = _column_ref(right_raw, aliases, schema, where)
            if left.domain != right.domain:
                _reject(f"{where}: equality {left_raw} = {right_raw} compares a "
                        f"{left.domain} column with a {right.domain} column; typed "
                        f"values of different domains can never be equal")
            conditions.append(("colcol", left, right))
        elif left_is_ref or right_is_ref:
            # normalize to (column, constant)
            if left_is_ref:
                ref = _column_ref(left_raw, aliases, schema, where)
                const = _constant(right_raw, where)
            else:
                ref = _column_ref(right_raw, aliases, schema, where)
                const = _constant(left_raw, where)
            if ref.domain != const.domain:
                _reject(f"{where}: {ref.alias}.{ref.column} has type {ref.domain} but the "
                        f"constant is {const.domain} {const.value!r}; a text value and an "
                        f"int value can never be equal")
            conditions.append(("colconst", ref, const))
        else:
            _reject(f"{where}: an equality must involve at least one relation column; "
                    f"constant-to-constant conditions are not accepted")

    canonical = {
        "format": QUERY_FORMAT,
        # relations/where are order-insensitive content: normalize their
        # serialization so the same logical query has one digest; the select
        # order is meaningful (it defines output order) and is kept as given
        "relations": sorted(
            ({"name": u.relation, "alias": u.alias} for u in uses),
            key=lambda x: json.dumps(x, sort_keys=True)),
        "select": [{"column": f"{s.ref.alias}.{s.ref.column}", "name": s.name}
                   for s in selects],
        "where": sorted((_canonical_condition(c) for c in conditions),
                        key=lambda x: json.dumps(x, sort_keys=True, ensure_ascii=False)),
    }
    digest = hashlib.sha256(
        json.dumps(canonical, ensure_ascii=False, sort_keys=True,
                   separators=(",", ":")).encode("utf-8")).hexdigest()
    return ParsedQuery(uses, selects, conditions, canonical, digest)


def _canonical_condition(c: Condition) -> Dict[str, Any]:
    kind, left, right = c
    if kind == "colcol":
        return {"eq": [f"{left.alias}.{left.column}", f"{right.alias}.{right.column}"]}
    return {"eq": [f"{left.alias}.{left.column}",
                   {"type": right.domain, "value": right.value}]}


# --------------------------------------------------------------------------- #
# Streaming payload reader
# --------------------------------------------------------------------------- #
def stream_decompress(blob: bytes, out_path: str, chunk: int = 1 << 20) -> None:
    """Decompress a version payload blob to ``out_path`` in bounded chunks."""
    d = zlib.decompressobj()
    with open(out_path, "wb") as fh:
        for start in range(0, len(blob), chunk):
            fh.write(d.decompress(blob[start:start + chunk]))
        fh.write(d.flush())


def _is_complete_json_array(text: str) -> bool:
    """True if ``text`` is one balanced JSON array ending at depth zero.

    Brackets inside string literals are ignored; depth must finish at exactly
    zero without ever going negative.  Used as a structural guard after the
    fast boundary split; ``json.loads`` performs the actual value validation.
    """
    depth = 0
    in_str = False
    esc = False
    for ch in text:
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
        elif ch == '"':
            in_str = True
        elif ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth < 0:
                return False
    return depth == 0 and not in_str and text[:1] == "["


def iter_payload_facts(path: str, read_chunk: int = 1 << 20
                       ) -> Iterator[Tuple[str, List[Dict[str, Any]]]]:
    """Yield ``(relation, encoded_row)`` pairs from a decompressed payload file.

    The file is streamed through a rolling buffer: at most one read chunk plus
    a single boundary-crossing row fragment is held in Python memory, so a
    very large version is never loaded as one document.

    The workspace writer emits the facts section as compact JSON (no newlines
    inside it).  Adjacent rows of one relation are separated by the fixed
    token ``"], ["`` and the relation array itself closes with ``"]]"``; the
    split runs in C via :mod:`re`, and each row is then validated with one
    ``json.loads``.  Relation keys and structural punctuation use the standard
    decoder.  Malformed or truncated input raises :class:`QueryError`.
    """
    decoder = json.JSONDecoder()
    # optional whitespace between the row-closing and next-opening brackets,
    # since payload JSON spacing is not guaranteed to be exactly one space
    row_split = re.compile(r"\],\s*\[")

    with open(path, encoding="utf-8") as fh:
        buf = ""

        def refill() -> bool:
            nonlocal buf
            piece = fh.read(read_chunk)
            if piece:
                buf += piece
            return bool(piece)

        # locate the "facts" key, retaining only a possibly-split marker
        marker = '"facts"'
        while True:
            pos = buf.find(marker)
            if pos >= 0:
                buf = buf[pos + len(marker):]
                break
            kept = min(len(marker), len(buf))
            buf = buf[-kept:] if kept else ""
            if not refill():
                raise QueryError("version payload has no 'facts' section")

        def expect(ch: str) -> None:
            nonlocal buf
            while True:
                buf = buf.lstrip()
                if buf:
                    break
                if not refill():
                    raise QueryError("unexpected end of version payload")
            if buf[0] != ch:
                raise QueryError(f"malformed payload: expected {ch!r}")
            buf = buf[1:]

        def read_key() -> str:
            nonlocal buf
            while True:
                buf = buf.lstrip()
                try:
                    value, end = decoder.raw_decode(buf)
                    buf = buf[end:]
                    if not isinstance(value, str):
                        raise QueryError(
                            "malformed payload: expected string key")
                    return value
                except json.JSONDecodeError as exc:
                    if not refill():
                        raise QueryError(
                            f"malformed payload key: {exc}") from None

        expect(":")
        expect("{")
        while True:
            buf = buf.lstrip()
            while not buf and refill():
                buf = buf.lstrip()
            if not buf:
                raise QueryError("unexpected end of facts section")
            if buf[0] == "}":
                return
            rel = read_key()
            expect(":")
            expect("[")

            # Invariant: `window` holds bytes from the row array starting at a
            # row's opening '[' (or a carried row fragment) and `buf` only
            # ever holds bytes not yet appended to it.  The relation ends at
            # the first ']]'; everything before that point is rows separated
            # by '], [' and is split with a C-level regex.
            window = ""
            relation_closed = False
            first = True
            while not relation_closed:
                buf = buf.lstrip()
                while not buf and refill():
                    buf = buf.lstrip()
                if not buf:
                    raise QueryError(
                        f"unexpected end of relation {rel!r} rows")
                combined = window + buf
                close_idx = combined.find("]]")
                if close_idx >= 0:
                    # rows occupy combined[:close_idx + 1] (through the last
                    # row's ']'); the second ']' closes the relation array
                    window = combined[:close_idx + 1]
                    leftover = combined[close_idx + 2:]
                    buf = leftover
                    relation_closed = True
                else:
                    # no relation end in sight yet: hold everything as the
                    # rolling window and read more, but first emit any whole
                    # rows already separated inside it
                    window = combined
                    buf = ""
                # split into row bodies; only a piece followed by the
                # boundary token is complete.  Keep the tail as the carry.
                parts = row_split.split(window)
                if relation_closed:
                    bodies, window = parts, ""
                else:
                    bodies, window = parts[:-1], parts[-1]
                for body in bodies:
                    text = body if (first and body[:1] == "[") else "[" + body
                    if not text.endswith("]"):
                        text += "]"
                    if not _is_complete_json_array(text):
                        raise QueryError(
                            f"malformed fact row in relation {rel!r}: "
                            f"{text[:60]!r}")
                    try:
                        row = json.loads(text)
                    except json.JSONDecodeError as exc:
                        raise QueryError(
                            f"bad fact row in {rel!r}: {exc}") from None
                    if not isinstance(row, list):
                        raise QueryError(
                            f"fact row of {rel!r} is not a list")
                    yield rel, row
                    first = False
            if window:
                raise QueryError(
                    f"trailing row fragment in relation {rel!r}")
            # the relation array's closing ']' was already consumed with the
            # ']]' boundary; next is either a ',' (more relations) or '}'
            buf = buf.lstrip()
            while not buf and refill():
                buf = buf.lstrip()
            if buf[:1] == ",":
                buf = buf[1:]
                continue
            if buf[:1] == "}":
                return
            raise QueryError("malformed 'facts' section in version payload")


# --------------------------------------------------------------------------- #
# SQLite-backed executor
# --------------------------------------------------------------------------- #
def _ks_from_encoded(encoded: Dict[str, Any]) -> Tuple[str, Any]:
    value = decode_value(encoded)
    if hasattr(value, "id"):  # LNullValue
        return ("n", value.id)
    if isinstance(value, str):
        return ("s", value)
    return ("i", value)


class QueryExecutor:
    """Loads one pinned version into a scratch SQLite store and runs queries.

    The store lives on disk; rows are inserted in bounded batches and query
    results are fetched in bounded batches.  Nothing accumulates beyond the
    value dictionary SQLite needs for indexed joins.
    """

    def __init__(self, scratch_dir: str, batch_size: int = 1000) -> None:
        self.scratch_dir = scratch_dir
        self.batch_size = batch_size
        self.db_path = os.path.join(scratch_dir, "query.sqlite")
        self.store = Store(self.db_path)
        self.facts_loaded: Dict[str, int] = {}
        self._schema: Optional[Schema] = None
        self.phase_seconds: Dict[str, float] = {}

    def close(self) -> None:
        self.store.close()

    def _intern_batch(self, rows: List[Tuple[Tuple[str, Any], ...]]) -> None:
        """Pre-intern every value of one row batch in bulk.

        The generic :meth:`Store.uid` issues an INSERT and a SELECT per unseen
        value; here the distinct unseen keys of the whole batch are inserted
        with one ``executemany`` and read back in chunked ``IN`` queries, so a
        large fact load costs a bounded number of SQLite round trips while the
        value dictionary stays the only cross-batch memory that grows.
        """
        store = self.store
        unseen = set()
        for row in rows:
            for key in row:
                if key not in store._uid_cache:
                    unseen.add(key)
        if not unseen:
            return
        store.conn.executemany(
            "INSERT OR IGNORE INTO _val(kind, val) VALUES (?, ?)",
            [(k, str(v)) for k, v in unseen])
        unseen_list = list(unseen)
        chunk = 400
        for start in range(0, len(unseen_list), chunk):
            part = unseen_list[start:start + chunk]
            clauses = " OR ".join("(kind=? AND val=?)" for _ in part)
            params: List[Any] = []
            for k, v in part:
                params.extend((k, str(v)))
            for uid, kind, val in store.conn.execute(
                    f"SELECT id, kind, val FROM _val WHERE {clauses}", params):
                key = (kind, int(val)) if kind == "i" else (kind, val)
                store._uid_cache[key] = uid
                store._ks_cache[uid] = key

    def load_payload_file(self, payload_path: str, schema: Schema) -> Dict[str, int]:
        """Stream every relation of the payload into the scratch store."""
        t0 = time.monotonic()
        self._schema = schema
        # one transaction for the whole bulk load: the store defaults to
        # autocommit, which would fsync a WAL/rollback step per statement
        self.store.begin()
        try:
            for rel, cols in schema.relations.items():
                self.store.declare(rel, len(cols))
                self.facts_loaded[rel] = 0
            pending: Dict[str, List[Tuple[Tuple[str, Any], ...]]] = {}
            total = 0
            for rel, encoded_row in iter_payload_facts(payload_path):
                if rel not in schema.relations:
                    raise QueryError(f"payload fact for undeclared relation {rel!r}")
                if len(encoded_row) != len(schema.columns(rel)):
                    raise QueryError(f"arity mismatch in payload relation {rel!r}")
                ks_row = tuple(_ks_from_encoded(v) for v in encoded_row)
                pending.setdefault(rel, []).append(ks_row)
                if len(pending[rel]) >= self.batch_size:
                    self._intern_batch(pending[rel])
                    self.store.insert_many(rel, pending[rel])
                    total += len(pending[rel])
                    self.facts_loaded[rel] += len(pending[rel])
                    pending[rel] = []
            for rel, rows in pending.items():
                if rows:
                    self._intern_batch(rows)
                    self.store.insert_many(rel, rows)
                    total += len(rows)
                    self.facts_loaded[rel] += len(rows)
            self.store.commit()
        except BaseException:
            self.store.rollback()
            raise
        self.facts_loaded["__total__"] = total
        self.phase_seconds["load_seconds"] = round(time.monotonic() - t0, 4)
        return dict(self.facts_loaded)

    # -- SQL compilation ---------------------------------------------------- #
    def _table_alias(self, use: RelationUse) -> str:
        return f"q{use.index}"

    def _col_sql(self, ref: ColumnRef, aliases: Dict[str, RelationUse]) -> str:
        return f'{self._table_alias(aliases[ref.alias])}.c{ref.position}'

    def compile_sql(self, query: ParsedQuery,
                    max_rows: Optional[int]) -> Tuple[str, List[Any]]:
        aliases = {u.alias: u for u in query.relations}
        params: List[Any] = []
        from_parts = []
        for use in query.relations:
            cols = self._schema.columns(use.relation)
            from_parts.append(f'"f_{use.relation}" AS {self._table_alias(use)}')
        where: List[str] = []
        for kind, left, right in query.conditions:
            if kind == "colcol":
                where.append(f"{self._col_sql(left, aliases)} = "
                             f"{self._col_sql(right, aliases)}")
            else:
                const: Constant = right
                uid = self.store.uid(const.key())  # intern in scratch _val
                where.append(f"{self._col_sql(left, aliases)} = ?")
                params.append(uid)

        select_parts: List[str] = []
        order_parts: List[str] = []
        joins: List[str] = []
        for j, sc in enumerate(query.select):
            col = self._col_sql(sc.ref, aliases)
            p = f"p{j}"
            joins.append(f"JOIN _val AS {p} ON {p}.id = {col}")
            # inner join + kind filter: unknowns can never reach the projection
            where.append(f"{p}.kind IN ('s', 'i')")
            select_parts.extend([f"{p}.kind AS pk{j}", f"{p}.val AS pv{j}"])
            order_parts.extend([f"{p}.kind", f"{p}.val"])
        sql = ("SELECT DISTINCT " + ", ".join(select_parts) +
               " FROM " + ", ".join(from_parts) + " " + " ".join(joins))
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY " + ", ".join(order_parts)
        if max_rows is not None:
            sql += " LIMIT ?"
            params.append(max_rows + 1)
        return sql, params

    def explain(self, query: ParsedQuery) -> List[Tuple[str, str, str]]:
        sql, params = self.compile_sql(query, None)
        return list(self.store.conn.execute("EXPLAIN QUERY PLAN " + sql, params))

    # -- execution ---------------------------------------------------------- #
    def run(self, query: ParsedQuery, sink: Optional[Callable[[List[Any]], None]] = None,
            max_rows: Optional[int] = None,
            max_seconds: Optional[float] = None
            ) -> Dict[str, Any]:
        sql, params = self.compile_sql(query, max_rows)
        started = time.monotonic()
        cur = self.store.conn.execute(sql, params)
        rows_out: List[List[Any]] = []
        emitted = 0
        scanned = 0
        truncated = False
        limit_reason = None
        deadline = started + max_seconds if max_seconds is not None else None
        while True:
            batch = cur.fetchmany(self.batch_size)
            if not batch:
                break
            for row in batch:
                scanned += 1
                if max_rows is not None and emitted >= max_rows:
                    # the (max_rows + 1)-th row proves the answer was cut short
                    truncated = True
                    limit_reason = "max_rows"
                    break
                decoded = self._decode_row(row, query)
                emitted += 1
                if sink is not None:
                    sink(decoded)  # streaming: nothing accumulates here
                else:
                    rows_out.append(decoded)
            if truncated:
                break
            if deadline is not None and time.monotonic() > deadline:
                truncated = True
                limit_reason = "max_seconds"
                break
        cur.close()
        elapsed = time.monotonic() - started
        return {
            "rows": rows_out,
            "row_count": emitted,
            "rows_scanned": scanned - (1 if limit_reason == "max_rows" else 0),
            "truncated": truncated,
            "limit_reason": limit_reason,
            "elapsed_seconds": round(elapsed, 6),
            "phase_seconds": dict(self.phase_seconds),
            "batch_size": self.batch_size,
            "facts_loaded": dict(self.facts_loaded),
            "scratch_db_bytes": _file_size(self.db_path),
        }

    @staticmethod
    def _decode_row(row: Sequence[Any], query: ParsedQuery) -> List[Any]:
        out: List[Any] = []
        for j, sc in enumerate(query.select):
            kind, val = row[2 * j], row[2 * j + 1]
            if kind == "s":
                out.append({"kind": "string", "value": val})
            elif kind == "i":
                out.append({"kind": "int", "value": int(val)})
            else:  # filtered out by compile_sql; defensive only
                raise QueryError(f"unknown value reached projection {sc.name!r}")
        return out


def _file_size(path: str) -> int:
    total = 0
    for suffix in ("", "-wal", "-shm"):
        try:
            total += os.path.getsize(path + suffix)
        except OSError:
            pass
    return total


def make_scratch_dir(base: Optional[str] = None) -> Tuple[str, str]:
    """Create a private scratch directory (caller removes it)."""
    d = tempfile.mkdtemp(prefix=".wsquery-", dir=base)
    return d, os.path.join(d, "payload.json")
