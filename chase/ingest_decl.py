"""Snapshot ingestion declarations and strict cell conversion.

A *declaration* describes one reusable supplier-to-workspace connector: which
``source_id`` s it manages, which workspace relation each one maps to (exactly
one relation per source), the columns that compose a record key, the mapping
from declared target fields to CSV headers, how unknown cells are spelled in
the feed, and the CSV shards whose union is one complete snapshot per
``source_id``.

Two rules are deliberately non-negotiable:

* **closed configuration** — every key at every level must be one of the
  documented keys.  A misspelled option (``shards_`` instead of ``shards``,
  ``nul_markers`` ...) is a hard error, never silently ignored.
* **no loose coercion** — text is preserved verbatim (an empty string or a
  whitespace-only string is a real text value, not a missing value), and an
  int cell must be an optional sign followed by ASCII digits only.  Decimals,
  exponents, internal whitespace and thousands separators are rejected;
  ``"07"`` and ``"+7"`` normalize to the same integer 7.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .model import Schema

_INT_RE = re.compile(r"[+-]?[0-9]+\Z")

_DECL_KEYS = {"connector", "version", "unknown_markers", "sources", "comment"}
_UNKNOWN_MARKER_KEYS = {"marker", "name_prefix", "name_separator",
                        "name_pattern"}
_SOURCE_KEYS = {"source_id", "relation", "key_columns", "fields", "shards",
                "empty_snapshot", "comment"}
_FIELD_KEYS = {"name", "header", "comment"}
_SHARD_KEYS = {"path", "comment"}
_DECL_REQUIRED = {"connector", "version", "unknown_markers", "sources"}
_SOURCE_REQUIRED = {"source_id", "relation", "key_columns", "fields",
                    "shards", "empty_snapshot"}
_FIELD_REQUIRED = {"name", "header"}
_SHARD_REQUIRED = {"path"}


class DeclarationError(Exception):
    """The declaration document is malformed or contradicts the schema."""


# --------------------------------------------------------------------------- #
# strict cell conversion
# --------------------------------------------------------------------------- #
def parse_int_cell(text: str) -> int:
    """Parse an int cell under the strict snapshot rules.

    Only an optional ``+``/``-`` sign followed by one or more ASCII digits is
    accepted.  ``"07"`` and ``"+7"`` normalize to the same int; decimals,
    exponents, surrounding or internal whitespace, empty strings and
    non-ASCII digits are all rejected rather than guessed.
    """
    if not _INT_RE.match(text):
        raise ValueError(
            f"not a strict integer: {text!r} (optional sign + ASCII digits only; "
            f"decimals, exponents and whitespace are rejected)")
    return int(text)


@dataclass(frozen=True)
class UnknownMarkers:
    """How one connector spells unknown cells.

    A cell equal to ``marker`` is an anonymous unknown.  A cell starting with
    ``name_prefix`` carries a local unknown name: the part after the prefix
    up to ``name_separator`` is the name, and everything after the separator
    is an ordinary text suffix.  Example with prefix ``"NULL:"`` and
    separator ``"|"``: ``"NULL:u"`` is local unknown ``u``; ``"NULL:u|x"`` is
    unknown ``u`` followed by the text suffix ``"x"`` (only valid in text
    columns).

    When ``name_pattern`` is configured, a prefixed cell is only an unknown
    if its local name fully matches that regex; otherwise the whole cell is
    an ordinary string.  This lets a feed carry literal text that happens to
    share the prefix (e.g. an internal-looking id ``"NULL:n0"``) without it
    being mistaken for an unknown.
    """

    marker: str
    name_prefix: str
    name_separator: str
    name_pattern: Optional[re.Pattern] = None

    def classify(self, cell: str) -> Tuple[str, Optional[str], Optional[str]]:
        """Return ``("unknown", name, suffix)`` or ``("value", None, None)``.

        Only cells matching the explicitly configured spellings are unknowns.
        An ordinary string that merely looks like an internal null id stays a
        string.
        """
        if cell == self.marker:
            return ("unknown", None, None)
        if self.name_prefix and cell.startswith(self.name_prefix):
            rest = cell[len(self.name_prefix):]
            if not rest:
                # prefix with nothing after it is just an ordinary string
                return ("value", None, None)
            if self.name_separator and self.name_separator in rest:
                name, suffix = rest.split(self.name_separator, 1)
            else:
                name, suffix = rest, None
            if name == "":
                return ("value", None, None)
            if self.name_pattern is not None and \
                    self.name_pattern.fullmatch(name) is None:
                return ("value", None, None)
            return ("unknown", name, suffix)
        return ("value", None, None)


# --------------------------------------------------------------------------- #
# declaration dataclasses
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class FieldMap:
    name: str           # target field (schema column name position)
    header: str         # CSV header that supplies it


@dataclass(frozen=True)
class ShardSpec:
    path: str


@dataclass(frozen=True)
class SourceSpec:
    source_id: str
    relation: str
    key_columns: Tuple[str, ...]
    fields: Tuple[FieldMap, ...]
    shards: Tuple[ShardSpec, ...]
    empty_snapshot: bool

    def header_for(self, column: str) -> str:
        for f in self.fields:
            if f.name == column:
                return f.header
        raise DeclarationError(f"no mapped header for column {column!r}")


@dataclass(frozen=True)
class Declaration:
    connector: str
    version: int
    unknown_markers: UnknownMarkers
    sources: Tuple[SourceSpec, ...]
    raw: Dict[str, Any] = field(repr=False, default_factory=dict)
    path: Optional[str] = None

    def source(self, source_id: str) -> SourceSpec:
        for s in self.sources:
            if s.source_id == source_id:
                return s
        raise DeclarationError(f"source_id {source_id!r} is not in this declaration")

    @property
    def content_hash_input(self) -> str:
        """Stable text used for declaration identity and binding checks."""
        return json.dumps(_binding_view(self.raw), ensure_ascii=False,
                          sort_keys=True, separators=(",", ":"))


def _require_keys(obj: Dict[str, Any], allowed: set, where: str,
                  required: Optional[set] = None) -> None:
    extra = sorted(set(obj) - allowed)
    if extra:
        raise DeclarationError(
            f"{where}: undefined configuration key(s) {extra}; only {sorted(allowed)} "
            f"are accepted (misspelled options are never ignored)")
    required = allowed if required is None else required
    missing = sorted(required - set(obj))
    if missing:
        raise DeclarationError(f"{where}: missing required key(s) {missing}")


def _require_nonempty_str(value: Any, where: str) -> str:
    if not isinstance(value, str) or not value:
        raise DeclarationError(f"{where}: must be a non-empty string")
    return value


def parse_declaration(obj: Any, schema: Schema,
                      path: Optional[str] = None) -> Declaration:
    """Validate a declaration document against the workspace's schema.

    The workspace keeps ownership of schema and rules: this only checks that
    the connector's relations, columns and key types are compatible.
    """
    if not isinstance(obj, dict):
        raise DeclarationError("declaration must be a JSON object")
    _require_keys(obj, _DECL_KEYS, "declaration root",
                  required=_DECL_REQUIRED)

    connector = _require_nonempty_str(obj["connector"], "connector")
    version = obj["version"]
    if not isinstance(version, int) or isinstance(version, bool) or version < 1:
        raise DeclarationError("'version' must be a positive integer")

    um_raw = obj["unknown_markers"]
    if not isinstance(um_raw, dict):
        raise DeclarationError("'unknown_markers' must be an object")
    _require_keys(um_raw, _UNKNOWN_MARKER_KEYS, "unknown_markers",
                  required={"marker", "name_prefix", "name_separator"})
    marker = _require_nonempty_str(um_raw["marker"], "unknown_markers.marker")
    name_prefix = um_raw["name_prefix"]
    name_separator = um_raw["name_separator"]
    name_pattern_raw = um_raw.get("name_pattern")
    for key, val in (("name_prefix", name_prefix),
                     ("name_separator", name_separator)):
        if not isinstance(val, str):
            raise DeclarationError(f"unknown_markers.{key} must be a string")
    if name_prefix and not name_separator:
        raise DeclarationError(
            "unknown_markers.name_separator is required when name_prefix is set")
    if name_separator and not name_prefix:
        raise DeclarationError(
            "unknown_markers.name_prefix is required when name_separator is set")
    if name_prefix and name_prefix == marker:
        raise DeclarationError(
            "unknown_markers.marker and name_prefix must be distinguishable")
    name_pattern = None
    if name_pattern_raw is not None:
        if not isinstance(name_pattern_raw, str) or not name_pattern_raw:
            raise DeclarationError(
                "unknown_markers.name_pattern must be a non-empty regex string")
        try:
            name_pattern = re.compile(name_pattern_raw)
        except re.error as exc:
            raise DeclarationError(
                f"unknown_markers.name_pattern is not a valid regex: {exc}")
    markers = UnknownMarkers(marker, name_prefix, name_separator,
                             name_pattern)

    sources_raw = obj["sources"]
    if not isinstance(sources_raw, list) or not sources_raw:
        raise DeclarationError("'sources' must be a non-empty list")
    sources: List[SourceSpec] = []
    seen_sids: set = set()
    for idx, sraw in enumerate(sources_raw):
        where = f"sources[{idx}]"
        if not isinstance(sraw, dict):
            raise DeclarationError(f"{where}: must be an object")
        _require_keys(sraw, _SOURCE_KEYS, where, required=_SOURCE_REQUIRED)
        sid = _require_nonempty_str(sraw["source_id"], f"{where}.source_id")
        if sid in seen_sids:
            raise DeclarationError(f"{where}: source_id {sid!r} is declared twice")
        seen_sids.add(sid)
        relation = _require_nonempty_str(sraw["relation"], f"{where}.relation")
        if relation not in schema.relations:
            raise DeclarationError(
                f"{where}: relation {relation!r} is not declared in the workspace "
                f"schema (the workspace owns the schema)")
        columns = list(schema.names(relation))

        key_columns = sraw["key_columns"]
        if (not isinstance(key_columns, list) or not key_columns
                or not all(isinstance(c, str) and c for c in key_columns)):
            raise DeclarationError(
                f"{where}.key_columns: a non-empty list of column names is required")
        if len(set(key_columns)) != len(key_columns):
            raise DeclarationError(f"{where}.key_columns: duplicate key column")
        for c in key_columns:
            if c not in columns:
                raise DeclarationError(
                    f"{where}.key_columns: {c!r} is not a column of {relation}")

        fields_raw = sraw["fields"]
        if not isinstance(fields_raw, list) or not fields_raw:
            raise DeclarationError(f"{where}.fields: a non-empty list is required")
        field_names: List[str] = []
        headers: List[str] = []
        fmaps: List[FieldMap] = []
        for fidx, fraw in enumerate(fields_raw):
            fwhere = f"{where}.fields[{fidx}]"
            if not isinstance(fraw, dict):
                raise DeclarationError(f"{fwhere}: must be an object")
            _require_keys(fraw, _FIELD_KEYS, fwhere, required=_FIELD_REQUIRED)
            name = _require_nonempty_str(fraw["name"], f"{fwhere}.name")
            header = _require_nonempty_str(fraw["header"], f"{fwhere}.header")
            if name not in columns:
                raise DeclarationError(
                    f"{fwhere}: field {name!r} is not a column of {relation}")
            if name in field_names:
                raise DeclarationError(f"{fwhere}: field {name!r} is mapped twice")
            if header in headers:
                raise DeclarationError(
                    f"{fwhere}: CSV header {header!r} is mapped to two fields")
            field_names.append(name)
            headers.append(header)
            fmaps.append(FieldMap(name, header))
        if set(field_names) != set(columns):
            raise DeclarationError(
                f"{where}.fields: must map every column of {relation} exactly once "
                f"(have {sorted(field_names)}, need {sorted(columns)})")

        shards_raw = sraw["shards"]
        if not isinstance(shards_raw, list) or not shards_raw:
            raise DeclarationError(
                f"{where}.shards: a non-empty list is required (use empty_snapshot "
                f"with header-only shards for an explicit empty snapshot)")
        shards: List[ShardSpec] = []
        seen_paths: set = set()
        for shidx, shraw in enumerate(shards_raw):
            shwhere = f"{where}.shards[{shidx}]"
            if not isinstance(shraw, dict):
                raise DeclarationError(f"{shwhere}: must be an object")
            _require_keys(shraw, _SHARD_KEYS, shwhere, required=_SHARD_REQUIRED)
            p = _require_nonempty_str(shraw["path"], f"{shwhere}.path")
            if p in seen_paths:
                raise DeclarationError(f"{shwhere}: shard path {p!r} is listed twice")
            seen_paths.add(p)
            shards.append(ShardSpec(p))

        empty_snapshot = sraw["empty_snapshot"]
        if not isinstance(empty_snapshot, bool):
            raise DeclarationError(f"{where}.empty_snapshot: must be true or false")

        sources.append(SourceSpec(
            source_id=sid, relation=relation, key_columns=tuple(key_columns),
            fields=tuple(fmaps), shards=tuple(shards),
            empty_snapshot=empty_snapshot))

    return Declaration(connector=connector, version=version,
                       unknown_markers=markers, sources=tuple(sources),
                       raw=obj, path=path)


def load_declaration(path: str, schema: Schema) -> Declaration:
    with open(path, encoding="utf-8") as fh:
        obj = json.load(fh)
    return parse_declaration(obj, schema, path=path)


# --------------------------------------------------------------------------- #
# typed composite key encoding
# --------------------------------------------------------------------------- #
def encode_record_key(key_tokens: Sequence[Tuple[str, Any]]) -> str:
    """Encode a typed composite key as an unambiguous, collision-free string.

    Every component carries its domain tag (``"s"`` text / ``"i"`` int), so
    the text ``"7"`` and the integer 7 can never collide, and no separator
    concatenation is used: the encoding is canonical JSON of the typed token
    list.  Key columns are never unknown.
    """
    return json.dumps(list(key_tokens), ensure_ascii=False,
                      separators=(",", ":"))


def _binding_view(raw: Dict[str, Any]) -> Dict[str, Any]:
    """The parts of a declaration that must never change for one connector."""
    return {
        "connector": raw["connector"],
        "unknown_markers": raw["unknown_markers"],
        "sources": [
            {
                "source_id": s["source_id"],
                "relation": s["relation"],
                "key_columns": s["key_columns"],
                "fields": s["fields"],
            }
            for s in raw["sources"]
        ],
    }
