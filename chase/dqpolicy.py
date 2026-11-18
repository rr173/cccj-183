"""Restricted, declarative data-quality policies.

A policy is a JSON document with a stable identity (``policy_id``), a
revision number, a sample limit, and a list of rules.  It is deliberately
**not** a program: a rule may only name a check layer, a relation, one or
more columns, a stable check id and a severity.  There is no SQL, no
expression language, no regular expression and no user code anywhere in the
format; every value a rule compares against is a typed constant.

Four checks are defined:

``required``
    the named column must hold a known ``text`` or ``int`` value.  A
    ``labeled_null`` means the value is still unknown: it is a **violation**,
    never an ordinary string such as ``"n0"``.

``unique``
    typed uniqueness over the **raw source records** of a source version.
    Two different source records are different inputs even when they
    normalize to the same relation fact, so they cannot disappear.  The text
    ``"7"``, the int ``7`` and a labeled null (by its id) are independent
    key values.  Only ``sources``-layer rules may use this check: completed
    facts are a set and would silently collapse the very duplicates this
    check exists to find.

``enum``
    only an explicitly listed constant of the matching type passes.  A known
    constant that is not listed is a violation; a ``labeled_null`` is
    *indeterminate* and is never counted as a pass.

``reference``
    a key on one relation must have a matching row in another relation.  A
    fully constant key with no match is a violation; a key containing any
    labeled null is indeterminate (it cannot be resolved under the certainty
    rules).  The target side may be rule-completed facts, in which case the
    report cites the target fact and its provenance.

The parser rejects unknown fields, unknown relations/columns, duplicate rule
ids, duplicate allowed values, cross-domain reference keys, booleans/floats,
and a ``unique`` rule on the ``completed`` layer.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from . import model as _model
from .model import Schema

FORMAT = "data-quality-policy/1"
LAYERS = ("sources", "completed")
KINDS = ("required", "unique", "enum", "reference")
SEVERITIES = ("error", "warning", "info")
POLICY_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]+$")
RULE_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]+$")

_ALLOWED_TOP = {"format", "policy_id", "revision", "description", "sample_limit",
                "indeterminate_fails", "rules"}
_ALLOWED_RULE = {"id", "kind", "layer", "relation", "severity", "column",
                 "columns", "allowed", "target"}
_ALLOWED_TARGET = {"relation", "columns"}
_ALLOWED_CONST = {"type", "value"}

Token = Tuple[str, Any]


class PolicyError(Exception):
    """The policy is rejected as a whole (no audit is run)."""


@dataclass(frozen=True)
class DQRule:
    id: str
    kind: str
    layer: str
    relation: str
    severity: str
    columns: Tuple[int, ...]                  # zero-based positions
    column_names: Tuple[str, ...]
    allowed: Tuple[Token, ...] = ()           # enum: ("s"|"i", value)
    target_relation: Optional[str] = None
    target_columns: Tuple[int, ...] = ()
    target_column_names: Tuple[str, ...] = ()

    def canonical(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "id": self.id,
            "kind": self.kind,
            "layer": self.layer,
            "relation": self.relation,
            "severity": self.severity,
        }
        if self.kind in ("required", "enum"):
            out["column"] = self.column_names[0]
        else:
            out["columns"] = list(self.column_names)
        if self.kind == "enum":
            out["allowed"] = [_token_json(t) for t in self.allowed]
        if self.kind == "reference":
            out["target"] = {
                "relation": self.target_relation,
                "columns": list(self.target_column_names),
            }
        return out

    def describe(self) -> Dict[str, Any]:
        d = self.canonical()
        d["column_positions"] = list(self.columns)
        if self.kind == "reference":
            d["target"] = {
                "relation": self.target_relation,
                "columns": list(self.target_column_names),
                "column_positions": list(self.target_columns),
            }
        return d


@dataclass(frozen=True)
class ParsedPolicy:
    policy_id: str
    revision: int
    description: Optional[str]
    sample_limit: int
    indeterminate_fails: bool
    rules: Tuple[DQRule, ...]
    canonical: Dict[str, Any]
    digest: str


def _token_json(token: Token) -> Dict[str, Any]:
    if token[0] == "s":
        return {"type": "text", "value": token[1]}
    return {"type": "int", "value": token[1]}


def _constant(raw: object, where: str) -> Token:
    if not isinstance(raw, dict):
        raise PolicyError(
            f"{where}: a constant must be an object "
            f"{{\"type\": \"text\"|\"int\", \"value\": ...}}")
    extra = set(raw) - _ALLOWED_CONST
    if extra:
        raise PolicyError(
            f"{where}: unsupported constant field(s) {sorted(extra)}; only "
            f"'type' and 'value' are allowed")
    typ = raw.get("type")
    if typ not in ("text", "int"):
        raise PolicyError(f"{where}: constant 'type' must be 'text' or 'int', "
                          f"got {typ!r}")
    if "value" not in raw:
        raise PolicyError(f"{where}: constant is missing 'value'")
    value = raw["value"]
    if isinstance(value, bool):
        raise PolicyError(f"{where}: booleans are not relational values")
    if typ == "text":
        if not isinstance(value, str):
            raise PolicyError(f"{where}: a text constant must hold a JSON string")
        return ("s", value)
    if not isinstance(value, int):
        raise PolicyError(f"{where}: an int constant must hold a JSON integer")
    return ("i", value)


def _column(schema: Schema, relation: str, raw: object, where: str
            ) -> Tuple[int, str]:
    if not isinstance(raw, str) or not raw:
        raise PolicyError(f"{where}: column must be a non-empty string")
    try:
        pos = schema.column_position(relation, raw)
    except _model.ModelError as exc:  # type: ignore[attr-defined]
        raise PolicyError(f"{where}: {exc}") from None
    return pos, schema.names(relation)[pos]


def _columns(schema: Schema, relation: str, raw: object, where: str,
             allow_multiple: bool) -> Tuple[Tuple[int, ...], Tuple[str, ...]]:
    if isinstance(raw, list):
        if not raw:
            raise PolicyError(f"{where}: 'columns' must not be empty")
        if not allow_multiple and len(raw) != 1:
            raise PolicyError(f"{where}: exactly one column is required")
        items = raw
    elif isinstance(raw, str) and not allow_multiple:
        items = [raw]
    else:
        raise PolicyError(
            f"{where}: {'a list of columns' if allow_multiple else 'one column'} "
            f"is required")
    positions: List[int] = []
    names: List[str] = []
    for j, item in enumerate(items):
        pos, name = _column(schema, relation, item, f"{where}[{j}]")
        if pos in positions:
            raise PolicyError(f"{where}: duplicate column {name!r}")
        positions.append(pos)
        names.append(name)
    return tuple(positions), tuple(names)


def _constant_sort_key(t: Token) -> str:
    return t[0] + ":" + json.dumps(t[1], ensure_ascii=False, sort_keys=True)


def _parse_rule(raw: object, idx: int, schema: Schema) -> DQRule:
    where = f"rules[{idx}]"
    if not isinstance(raw, dict):
        raise PolicyError(f"{where}: a rule must be an object")
    extra = set(raw) - _ALLOWED_RULE
    if extra:
        raise PolicyError(
            f"{where}: unsupported rule field(s) {sorted(extra)}; allowed fields "
            f"are {sorted(_ALLOWED_RULE)} (a policy cannot contain SQL, "
            f"expressions, regular expressions or code)")
    rid = raw.get("id")
    if not isinstance(rid, str) or not RULE_ID_RE.match(rid):
        raise PolicyError(
            f"{where}: a stable non-empty 'id' matching {RULE_ID_RE.pattern} "
            f"is required")
    kind = raw.get("kind")
    if kind not in KINDS:
        raise PolicyError(f"rule {rid}: 'kind' must be one of {list(KINDS)}, "
                          f"got {kind!r}")
    layer = raw.get("layer")
    if layer not in LAYERS:
        raise PolicyError(
            f"rule {rid}: every rule must explicitly select a layer, one of "
            f"{list(LAYERS)}; got {layer!r}")
    relation = raw.get("relation")
    if not isinstance(relation, str) or relation not in schema.relations:
        raise PolicyError(
            f"rule {rid}: relation {relation!r} is not declared in the workspace "
            f"schema ({sorted(schema.relations)})")
    severity = raw.get("severity")
    if severity not in SEVERITIES:
        raise PolicyError(
            f"rule {rid}: 'severity' must be one of {list(SEVERITIES)}, "
            f"got {severity!r}")

    allowed_tokens: Tuple[Token, ...] = ()
    target_rel: Optional[str] = None
    target_cols: Tuple[int, ...] = ()
    target_names: Tuple[str, ...] = ()
    if kind == "required":
        if "columns" in raw or "allowed" in raw or "target" in raw:
            raise PolicyError(f"rule {rid}: required uses only 'column'")
        cols, names = _columns(schema, relation, raw.get("column"), rid, False)
    elif kind == "enum":
        if "columns" in raw or "target" in raw:
            raise PolicyError(f"rule {rid}: enum uses 'column' and 'allowed'")
        cols, names = _columns(schema, relation, raw.get("column"), rid, False)
        allowed_raw = raw.get("allowed")
        if not isinstance(allowed_raw, list) or not allowed_raw:
            raise PolicyError(
                f"rule {rid}: enum requires a non-empty 'allowed' list of typed "
                f"constants")
        seen: Dict[Token, bool] = {}
        tokens: List[Token] = []
        for j, item in enumerate(allowed_raw):
            tok = _constant(item, f"rule {rid} allowed[{j}]")
            if tok in seen:
                raise PolicyError(
                    f"rule {rid}: allowed constant {_token_json(tok)!r} is "
                    f"duplicated")
            seen[tok] = True
            tokens.append(tok)
        dom = schema.columns(relation)[cols[0]]
        for tok in tokens:
            if (tok[0] == "s") != (dom == "text"):
                raise PolicyError(
                    f"rule {rid}: allowed {_token_json(tok)!r} has the wrong "
                    f"type for {relation}.{names[0]} ({dom}); a text '7' and "
                    f"an int 7 are different values")
        allowed_tokens = tuple(sorted(tokens, key=_constant_sort_key))
    elif kind == "unique":
        if layer != "sources":
            raise PolicyError(
                f"rule {rid}: typed uniqueness is defined on raw source records; "
                f"a completed-layer fact set has already de-duplicated records, "
                f"so this rule must use layer 'sources'")
        if "column" in raw or "allowed" in raw or "target" in raw:
            raise PolicyError(f"rule {rid}: unique uses only 'columns'")
        cols, names = _columns(schema, relation, raw.get("columns"), rid, True)
    else:  # reference
        if "column" in raw or "allowed" in raw:
            raise PolicyError(
                f"rule {rid}: reference uses 'columns' and 'target'")
        cols, names = _columns(schema, relation, raw.get("columns"), rid, True)
        target = raw.get("target")
        if not isinstance(target, dict):
            raise PolicyError(f"rule {rid}: reference requires a 'target' object")
        extra_t = set(target) - _ALLOWED_TARGET
        if extra_t:
            raise PolicyError(
                f"rule {rid}: unsupported target field(s) {sorted(extra_t)}")
        tr = target.get("relation")
        if not isinstance(tr, str) or tr not in schema.relations:
            raise PolicyError(
                f"rule {rid}: target relation {tr!r} is not declared in the "
                f"workspace schema")
        tcols, tnames = _columns(schema, tr, target.get("columns"), rid, True)
        if len(tcols) != len(cols):
            raise PolicyError(
                f"rule {rid}: reference key has {len(cols)} column(s) but the "
                f"target key has {len(tcols)}")
        left_types = tuple(schema.columns(relation)[i] for i in cols)
        right_types = tuple(schema.columns(tr)[i] for i in tcols)
        if left_types != right_types:
            raise PolicyError(
                f"rule {rid}: reference key types {left_types} do not match "
                f"target key types {right_types}; typed values of different "
                f"domains can never be equal")
        target_rel = tr
        target_cols = tcols
        target_names = tnames

    return DQRule(
        id=rid, kind=kind, layer=layer, relation=relation, severity=severity,
        columns=cols, column_names=names, allowed=allowed_tokens,
        target_relation=target_rel, target_columns=target_cols,
        target_column_names=target_names)


def parse_policy(obj: object, schema: Schema) -> ParsedPolicy:
    """Validate and normalize a policy against ``schema``.

    Validation is complete before any data is read.  Rejection never produces
    a report and never persists a run.

    The stored canonical form (the one embedded in run reports) re-parses to
    the same digest, so a historical run can always be re-verified.
    """
    if not isinstance(obj, dict):
        raise PolicyError("policy must be a JSON object")
    extra = set(obj) - _ALLOWED_TOP
    if extra:
        raise PolicyError(
            f"unsupported top-level field(s) {sorted(extra)}; allowed fields are "
            f"{sorted(_ALLOWED_TOP)}")
    fmt = obj.get("format")
    if fmt != FORMAT:
        raise PolicyError(f"policy 'format' must be {FORMAT!r}, got {fmt!r}")
    pid = obj.get("policy_id")
    if not isinstance(pid, str) or not POLICY_ID_RE.match(pid):
        raise PolicyError(
            f"policy_id must be a stable non-empty identifier matching "
            f"{POLICY_ID_RE.pattern}")
    revision = obj.get("revision")
    if (not isinstance(revision, int) or isinstance(revision, bool)
            or revision < 1):
        raise PolicyError("revision must be a positive integer")
    description = obj.get("description")
    if description is not None and not isinstance(description, str):
        raise PolicyError("description must be a string when present")
    sample_limit = obj.get("sample_limit")
    if (not isinstance(sample_limit, int) or isinstance(sample_limit, bool)
            or sample_limit < 0 or sample_limit > 1000):
        raise PolicyError("sample_limit must be an integer between 0 and 1000")
    indeterminate_fails = obj.get("indeterminate_fails", False)
    if not isinstance(indeterminate_fails, bool):
        raise PolicyError("indeterminate_fails must be a boolean")
    rules_raw = obj.get("rules")
    if not isinstance(rules_raw, list) or not rules_raw:
        raise PolicyError("rules must be a non-empty list")

    rules: List[DQRule] = []
    ids: set = set()
    for idx, raw in enumerate(rules_raw):
        rule = _parse_rule(raw, idx, schema)
        if rule.id in ids:
            raise PolicyError(f"duplicate rule id {rule.id!r}")
        ids.add(rule.id)
        rules.append(rule)
    rules.sort(key=lambda r: r.id)

    canonical: Dict[str, Any] = {
        "format": FORMAT,
        "policy_id": pid,
        "revision": revision,
        "sample_limit": sample_limit,
        "indeterminate_fails": indeterminate_fails,
        "rules": [r.canonical() for r in rules],
    }
    if description is not None:
        canonical["description"] = description
    text = json.dumps(canonical, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return ParsedPolicy(pid, revision, description, sample_limit,
                        indeterminate_fails, tuple(rules), canonical, digest)
