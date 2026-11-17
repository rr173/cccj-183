"""Typed relational model for the dependency completion tool.

Concepts
--------
``Var``      a variable of a rule body/head, finally typed ``text`` or ``int``.
``Const``    a constant.  ``kind`` is ``"string"`` (text), ``"int"`` (int) or
             ``"labeled_null"`` (a *new* unknown value).  A labeled null is a
             placeholder; it is never reported as something read from a source.
``Term``     a constant or a variable (rule syntax).
``Atom``     ``Rel(term, ...)``.
``Rule``     parsed ``TGD``/``EGD`` with a fixed id, plus its weak-acyclicity
             annotation (special edges / rejecting cycle) computed for the
             whole program.

The input facts are a *set*: ``Instance`` de-duplicates exact triples, and
every fact is an immutable value.  Each labeled null has its own stable id
(``n0``, ``n1``, ...) which is exposed separately from its display string, so
that ``"c"`` read from a source and the placeholder shown as ``"c"`` can never
be confused.

Weak acyclicity (as requested)
------------------------------
Nodes of the dependency graph are relation positions ``(relation, column)``.
For every TGD:

* **ordinary edge** ``(B,i) -> (H,j)`` when variable ``x`` occurs at body
  position ``(B,i)`` and at head position ``(H,j)`` -- it only copies an
  already existing value;
* **special edge** ``(B,i) -> (H,j)`` from *every* body variable position to
  every head position holding an *existential* variable -- this is where a
  fresh unknown is born.

A rule may cycle through ordinary edges (ordinary recursion, e.g.
``R(x) -> S(x)`` and ``S(x) -> R(x)``) but a cycle that contains a special
edge raises the "generation" of unknowns without bound and is rejected.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field, replace
from typing import Dict, List, Optional, Sequence, Set, Tuple

DOMAINS = ("text", "int")
REL_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
VAR_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
RULE_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]+$")


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class ModelError(Exception):
    """Malformed input (schema, program, facts)."""


class ProgramError(ModelError):
    """The program is rejected (e.g. not weakly acyclic)."""


class BudgetExceeded(Exception):
    def __init__(self, reason: str, detail: str = "") -> None:
        self.reason = reason
        self.detail = detail
        super().__init__(f"budget exhausted: {reason}" + (f" ({detail})" if detail else ""))


# --------------------------------------------------------------------------- #
# Terms and atoms
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Var:
    name: str

    def __str__(self) -> str:
        return self.name


@dataclass(frozen=True)
class Const:
    kind: str  # "string" | "int" | "labeled_null"
    value: object

    def __post_init__(self) -> None:
        if self.kind not in ("string", "int", "labeled_null"):
            raise ModelError(f"unknown constant kind {self.kind!r}")
        if self.kind == "string" and not isinstance(self.value, str):
            raise ModelError("string constant must hold a str")
        if self.kind == "int" and (not isinstance(self.value, int) or isinstance(self.value, bool)):
            raise ModelError("int constant must hold a non-bool int")
        if self.kind == "labeled_null" and not isinstance(self.value, str):
            raise ModelError("labeled null constant must hold an identifier string")

    @property
    def domain(self) -> str:
        if self.kind == "int":
            return "int"
        if self.kind == "string":
            return "text"
        raise ModelError("a labeled null has no intrinsic domain (its rule var does)")

    def to_json(self) -> Dict[str, object]:
        if self.kind == "labeled_null":
            return {"kind": "labeled_null", "id": self.value, "display": f"NULL:{self.value}"}
        if self.kind == "string":
            return {"kind": "string", "value": self.value}
        return {"kind": "int", "value": self.value}


Term = object  # Var | Const


@dataclass(frozen=True)
class Atom:
    rel: str
    terms: Tuple[object, ...]  # tuple[Var | Const, ...]

    def __str__(self) -> str:
        return f"{self.rel}({', '.join(str(t) for t in self.terms)})"


@dataclass(frozen=True)
class LNullValue:
    """An actual unknown value appearing inside a fact.

    ``id`` is the stable identity of the unknown (shared unknown => same id,
    distinct unknown => different ids).  ``display`` is cosmetic only and is
    deliberately distinguishable from a source string.
    """

    id: str
    display: str

    def __str__(self) -> str:
        return self.display


@dataclass(frozen=True)
class Fact:
    rel: str
    values: Tuple[object, ...]  # tuple[str | int | LNullValue, ...]

    def key(self) -> Tuple[object, ...]:
        return (self.rel,) + self.values

    def __str__(self) -> str:
        return f"{self.rel}({', '.join(_show(v) for v in self.values)})"


def _show(value: object) -> str:
    if isinstance(value, LNullValue):
        return value.display
    if isinstance(value, str):
        return f'"{value}"'
    return str(value)


def value_key(value: object) -> Tuple[object, ...]:
    if isinstance(value, LNullValue):
        return ("n", value.id)
    if isinstance(value, str):
        return ("s", value)
    if isinstance(value, int) and not isinstance(value, bool):
        return ("i", value)
    raise ModelError(f"not a fact value: {value!r}")


def render_values(values: Sequence[object]) -> List[object]:
    out: List[object] = []
    for v in values:
        if isinstance(v, LNullValue):
            out.append(v.to_json())
        elif isinstance(v, str):
            out.append({"kind": "string", "value": v})
        else:
            out.append({"kind": "int", "value": v})
    return out


def parse_value(raw: object) -> object:
    """Parse a *source* value from JSON.

    Strings and ints are source constants.  A JSON object
    ``{"null": "u1"}`` (or ``{"kind": "labeled_null", "id": "u1"}``) declares
    a pre-existing unknown placeholder: every occurrence of ``u1`` in the
    input denotes the same unknown, and different ids denote different
    unknowns (until an EGD unifies them).  Unknowns are never source facts.
    """
    if isinstance(raw, bool):
        raise ModelError("booleans are not valid relational values")
    if isinstance(raw, (str, int)):
        return raw
    if isinstance(raw, dict):
        nid = raw.get("null", raw.get("id") if raw.get("kind") == "labeled_null" else None)
        if isinstance(nid, str) and nid:
            return LNullValue(nid, f"NULL:{nid}")
        raise ModelError(f"invalid labeled-null object {raw!r}; expected {{\"null\": \"id\"}}")
    raise ModelError(f"facts/constants must be strings, ints or null placeholders, "
                     f"got {type(raw).__name__}")


# --------------------------------------------------------------------------- #
# Schema
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Schema:
    relations: Dict[str, Tuple[str, ...]]

    def __post_init__(self) -> None:
        if not self.relations:
            raise ModelError("schema declares no relations")
        for rel, cols in self.relations.items():
            if not REL_NAME_RE.match(rel):
                raise ModelError(f"illegal relation name {rel!r}")
            if not cols:
                raise ModelError(f"relation {rel!r} must have at least one column")
            for c in cols:
                if c not in DOMAINS:
                    raise ModelError(
                        f"relation {rel!r} column type must be text/int, got {c!r}")

    def check_atom(self, atom: Atom, where: str) -> None:
        if atom.rel not in self.relations:
            raise ModelError(f"{where}: relation {atom.rel!r} is not declared in the schema")
        cols = self.relations[atom.rel]
        if len(atom.terms) != len(cols):
            raise ModelError(
                f"{where}: atom {atom} has {len(atom.terms)} terms but {atom.rel} has "
                f"{len(cols)} columns")
        for i, (t, dom) in enumerate(zip(atom.terms, cols)):
            if isinstance(t, Var):
                continue
            if not isinstance(t, Const):
                raise ModelError(f"{where}: atom {atom} position {i} holds {t!r}")
            if t.kind == "labeled_null":
                raise ModelError(f"{where}: labeled nulls may not appear in rules")
            if t.kind == "int" and dom != "int":
                raise ModelError(
                    f"{where}: int constant {t.value} in a {dom} column of {atom.rel}")
            if t.kind == "string" and dom != "text":
                raise ModelError(
                    f"{where}: string constant {t.value!r} in a {dom} column of {atom.rel}")

    def columns(self, rel: str) -> Tuple[str, ...]:
        try:
            return self.relations[rel]
        except KeyError:
            raise ModelError(f"relation {rel!r} is not declared in the schema") from None

    def to_json(self) -> Dict[str, List[str]]:
        return {r: list(c) for r, c in self.relations.items()}


def schema_from_json(obj: object) -> Schema:
    if not isinstance(obj, dict):
        raise ModelError("schema must be an object mapping relation -> list of column types")
    rels = {}
    for rel, cols in obj.items():
        if not isinstance(cols, list) or not all(isinstance(c, str) for c in cols):
            raise ModelError(f"schema.{rel} must be a list of 'text'/'int'")
        rels[rel] = tuple(cols)
    return Schema(rels)


# --------------------------------------------------------------------------- #
# Rules
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Rule:
    id: str
    kind: str  # "tgd" | "egd"
    body: Tuple[Atom, ...]
    head: Optional[Atom] = None          # TGD
    eq_left: Optional[object] = None     # EGD
    eq_right: Optional[object] = None    # EGD
    signature: str = ""
    var_types: Dict[str, str] = field(default_factory=dict)
    existentials: Tuple[Var, ...] = ()
    special_edges: Tuple[Tuple[str, int, str, int], ...] = ()
    cyclic: bool = False
    cyclic_edges: Tuple[Tuple[str, int, str, int], ...] = ()
    cycle_path: Tuple[Tuple[str, int], ...] = ()

    def existential_names(self) -> Set[str]:
        return {v.name for v in self.existentials}

    def body_vars(self) -> Set[str]:
        out: Set[str] = set()
        for a in self.body:
            for t in a.terms:
                if isinstance(t, Var):
                    out.add(t.name)
        return out

    def head_vars(self) -> Set[str]:
        out: Set[str] = set()
        if self.head is not None:
            for t in self.head.terms:
                if isinstance(t, Var):
                    out.add(t.name)
        return out

    def display_eq(self) -> str:
        return f"{self.eq_left} = {self.eq_right}"

    def to_json(self) -> Dict[str, object]:
        base: Dict[str, object] = {
            "id": self.id,
            "kind": self.kind,
            "body": [str(a) for a in self.body],
            "signature": self.signature,
            "var_types": dict(sorted(self.var_types.items())),
            "weak_acyclic": not self.cyclic,
        }
        if self.kind == "tgd":
            base["head"] = str(self.head)
            base["existentials"] = [v.name for v in self.existentials]
        else:
            base["eq"] = [str(self.eq_left), str(self.eq_right)]
        base["special_edges"] = [list(e) for e in self.special_edges]
        base["cyclic_edges"] = [list(e) for e in self.cyclic_edges]
        if self.cycle_path:
            base["cycle_path"] = [list(p) for p in self.cycle_path]
        return base


# -- parsing ---------------------------------------------------------------- #
class ParseError(ModelError):
    pass


_ATOM_RE = re.compile(r"^\s*([A-Za-z][A-Za-z0-9_]*)\s*\((.*)\)\s*$", re.S)


def _split_outside_quotes(text: str) -> Tuple[List[str], str]:
    """Return (chunks outside quotes, text with quoted parts blanked)."""
    outside: List[str] = []
    blanked: List[str] = []
    in_q = False
    for ch in text:
        if ch == '"':
            in_q = not in_q
            outside.append(ch)
            blanked.append('"')
            continue
        if in_q:
            blanked.append(" ")
        else:
            outside.append(ch)
            blanked.append(ch)
    return outside, "".join(blanked)


def _strip_quotes(s: str) -> str:
    s = s.strip()
    if len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        inner = s[1:-1]
        if '"' in inner:
            raise ParseError(f"unsupported quoted string {s!r}")
        return inner
    return s


def parse_term(raw: str, ctx: str) -> object:
    s = raw.strip()
    if not s:
        raise ParseError(f"{ctx}: empty term")
    if s.startswith('"'):
        return Const("string", _strip_quotes(s))
    if re.fullmatch(r"[+-]?\d+", s):
        return Const("int", int(s))
    if VAR_NAME_RE.match(s):
        return Var(s)
    raise ParseError(f"{ctx}: cannot parse term {raw!r}")


def parse_atom(raw: str, ctx: str) -> Atom:
    m = _ATOM_RE.match(raw)
    if not m:
        raise ParseError(f"{ctx}: malformed atom {raw!r}")
    rel, inside = m.group(1), m.group(2).strip()
    if not inside:
        raise ParseError(f"{ctx}: atom {raw!r} has no terms")
    parts: List[str] = []
    cur: List[str] = []
    in_q = False
    for ch in inside:
        if ch == '"':
            in_q = not in_q
            cur.append(ch)
        elif ch == "," and not in_q:
            parts.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    if in_q:
        raise ParseError(f"{ctx}: unterminated quoted string in {raw!r}")
    parts.append("".join(cur))
    if any(p.strip() == "" for p in parts):
        raise ParseError(f"{ctx}: empty term in atom {raw!r}")
    terms = tuple(parse_term(p, ctx) for p in parts)
    return Atom(rel, terms)


_DISALLOWED = ("+", "*", "/", "%", "!")


def _check_supported(rule_obj: dict, rid: str) -> None:
    texts: List[str] = []
    for key in ("body", "head", "eq"):
        val = rule_obj.get(key)
        if isinstance(val, list):
            texts.extend(str(x) for x in val)
        elif val is not None:
            texts.append(str(val))
    for text in texts:
        _, blanked = _split_outside_quotes(text)
        if re.search(r"(?<![A-Za-z0-9_])(not|NOT|Not)(?![A-Za-z0-9_])", blanked):
            raise ParseError(f"rule {rid}: negation is not supported")
        for op in _DISALLOWED:
            if op in blanked:
                raise ParseError(f"rule {rid}: arithmetic/user code ({op!r}) is not supported")
        if re.search(r"[<>]|:=|==|\bif\b|\bwhen\b", blanked):
            raise ParseError(f"rule {rid}: only declarative TGD/EGD syntax is supported")


def _check_eq_types(rid: str, left, right, var_types: Dict[str, str]) -> None:
    doms = []
    for term in (left, right):
        if isinstance(term, Var):
            doms.append(var_types[term.name])
        elif isinstance(term, Const):
            doms.append(term.domain)
        else:
            raise ParseError(f"rule {rid}: bad equality term {term!r}")
    if doms[0] != doms[1]:
        raise ParseError(
            f"rule {rid}: equality {left} = {right} compares {doms[0]} with {doms[1]}; "
            f"a text value and an int value can never be equal")


def _sig_atom(atom: Atom, var_types: Dict[str, str]) -> str:
    parts = []
    for t in atom.terms:
        if isinstance(t, Var):
            parts.append(var_types[t.name] + ":" + t.name)
        elif t.kind == "string":
            parts.append("text:" + repr(t.value))
        elif t.kind == "int":
            parts.append("int:" + str(t.value))
        else:
            raise ParseError(f"labeled null constants may not appear in rules: {atom}")
    return atom.rel + "(" + ",".join(parts) + ")"


def _compute_var_types(rid: str, atoms: Sequence[Atom], schema: Schema) -> Dict[str, str]:
    types: Dict[str, str] = {}
    for atom in atoms:
        cols = schema.columns(atom.rel)
        for t, dom in zip(atom.terms, cols):
            if isinstance(t, Var):
                prev = types.get(t.name)
                if prev is not None and prev != dom:
                    raise ParseError(
                        f"rule {rid}: variable {t.name} is used both as {prev} and as {dom} "
                        f"(relation {atom.rel})")
                types[t.name] = dom
    return types


def parse_rule(obj: object, schema: Schema, idx: int) -> Rule:
    if not isinstance(obj, dict):
        raise ParseError(f"rule #{idx} must be an object")
    rid = obj.get("id")
    if not isinstance(rid, str) or not RULE_ID_RE.match(rid):
        raise ParseError(
            f"rule #{idx}: a fixed non-empty 'id' matching {RULE_ID_RE.pattern} is required")
    if "body" not in obj:
        raise ParseError(f"rule {rid}: missing 'body'")
    body_raw = obj["body"]
    if not isinstance(body_raw, list) or not body_raw:
        raise ParseError(f"rule {rid}: 'body' must be a non-empty list of atoms")
    body = tuple(parse_atom(str(b), f"rule {rid} body") for b in body_raw)
    for atom in body:
        schema.check_atom(atom, f"rule {rid} body")
    _check_supported(obj, rid)

    has_head = "head" in obj
    has_eq = "eq" in obj
    if has_head == has_eq:
        raise ParseError(
            f"rule {rid}: exactly one of 'head' (TGD) or 'eq' (EGD) is required")
    if has_head:
        head = parse_atom(str(obj["head"]), f"rule {rid} head")
        schema.check_atom(head, f"rule {rid} head")
        return _finish_tgd(rid, body, head, schema)
    eq = obj["eq"]
    if not isinstance(eq, list) or len(eq) != 2:
        raise ParseError(f"rule {rid}: 'eq' must be a two-element list [term, term]")
    left = parse_term(str(eq[0]), f"rule {rid} eq")
    right = parse_term(str(eq[1]), f"rule {rid} eq")
    return _finish_egd(rid, body, left, right, schema)


def _finish_tgd(rid: str, body: Tuple[Atom, ...], head: Atom, schema: Schema) -> Rule:
    var_types = _compute_var_types(rid, body + (head,), schema)
    bvars = {t.name for a in body for t in a.terms if isinstance(t, Var)}
    exist: List[Var] = []
    seen: Set[str] = set()
    for t in head.terms:
        if isinstance(t, Var) and t.name not in bvars and t.name not in seen:
            seen.add(t.name)
            exist.append(t)
    sig = "; ".join(_sig_atom(a, var_types) for a in body) + " -> " + _sig_atom(head, var_types)
    return Rule(
        id=rid,
        kind="tgd",
        body=body,
        head=head,
        signature=sig,
        var_types=var_types,
        existentials=tuple(exist),
    )


def _finish_egd(rid: str, body: Tuple[Atom, ...], left, right, schema: Schema) -> Rule:
    var_types = _compute_var_types(rid, body, schema)
    for term in (left, right):
        if isinstance(term, Var) and term.name not in var_types:
            raise ParseError(
                f"rule {rid}: EGD variable {term.name} does not occur in the body "
                f"(EGD variables must be bound)")
        if isinstance(term, Const) and term.kind == "labeled_null":
            raise ParseError(f"rule {rid}: labeled nulls may not appear in rules")
    _check_eq_types(rid, left, right, var_types)
    sig = "; ".join(_sig_atom(a, var_types) for a in body) + " -> " + f"{left} = {right}"
    return Rule(id=rid, kind="egd", body=body, eq_left=left, eq_right=right,
                signature=sig, var_types=var_types)


def _rule_edges(rule: Rule) -> Tuple[List[Tuple[str, int, str, int]], List[Tuple[str, int, str, int]]]:
    """Return (ordinary_edges, special_edges) of one TGD."""
    ordinary: List[Tuple[str, int, str, int]] = []
    special: List[Tuple[str, int, str, int]] = []
    if rule.kind != "tgd":
        return ordinary, special
    assert rule.head is not None
    body_pos = {(a.rel, i): t.name
                for a in rule.body for i, t in enumerate(a.terms) if isinstance(t, Var)}
    for i, t in enumerate(rule.head.terms):
        if not isinstance(t, Var):
            continue
        for (b_rel, b_i), name in body_pos.items():
            if name == t.name:
                ordinary.append((b_rel, b_i, rule.head.rel, i))
    bvar_positions = list(body_pos)
    existential_names = {v.name for v in rule.existentials}
    for i, t in enumerate(rule.head.terms):
        if isinstance(t, Var) and t.name in existential_names:
            for (b_rel, b_i) in bvar_positions:
                special.append((b_rel, b_i, rule.head.rel, i))
    return sorted(set(ordinary)), sorted(set(special))


def analyze_program(rules: Sequence[Rule]) -> List[Rule]:
    """Global weak-acyclicity analysis.

    Builds the relation-position dependency graph of all TGDs together and
    rejects every TGD that owns a special edge lying on a directed cycle.
    Returns rules with ``special_edges``/``cyclic``/``cycle_path`` filled in.
    """
    edges: List[Tuple[Tuple[str, int], Tuple[str, int], str, str]] = []
    for r in rules:
        if r.kind != "tgd":
            continue
        ordinary, special = _rule_edges(r)
        for (a, b, c, d) in ordinary:
            edges.append(((a, b), (c, d), r.id, "ordinary"))
        for (a, b, c, d) in special:
            edges.append(((a, b), (c, d), r.id, "special"))

    adjacency: Dict[Tuple[str, int], List[Tuple[Tuple[str, int], str]]] = {}
    for src, dst, rid, kind in edges:
        adjacency.setdefault(src, []).append((dst, rid))

    def path(start, goal) -> Optional[List[Tuple[str, int]]]:
        stack = [(start, [start])]
        seen = {start}
        while stack:
            node, prefix = stack.pop()
            if node == goal:
                return prefix
            for nxt, _rid in adjacency.get(node, ()):
                if nxt not in seen:
                    seen.add(nxt)
                    stack.append((nxt, prefix + [nxt]))
        return None

    offending: Dict[str, Tuple[Tuple[str, int, str, int], Tuple[Tuple[str, int], ...]]] = {}
    for src, dst, rid, kind in edges:
        if kind != "special":
            continue
        back = path(dst, src)
        if back is not None:
            cycle = tuple([src] + back)
            prev = offending.get(rid)
            if prev is None or len(cycle) < len(prev[1]):
                offending[rid] = ((src[0], src[1], dst[0], dst[1]), cycle)

    out: List[Rule] = []
    for r in rules:
        if r.kind == "tgd":
            _ordinary, special = _rule_edges(r)
            ann = offending.get(r.id)
            r = replace(
                r,
                special_edges=tuple(special),
                cyclic=ann is not None,
                cyclic_edges=(ann[0],) if ann else (),
                cycle_path=ann[1] if ann else (),
            )
        out.append(r)
    return out


def parse_program(obj: object, schema: Schema) -> List[Rule]:
    if not isinstance(obj, dict) or not isinstance(obj.get("rules"), list):
        raise ParseError("program must be an object with a 'rules' list")
    rules: List[Rule] = []
    ids: Set[str] = set()
    sigs: Dict[str, str] = {}
    for idx, r in enumerate(obj["rules"]):
        rule = parse_rule(r, schema, idx)
        if rule.id in ids:
            raise ParseError(f"duplicate rule id {rule.id!r}")
        ids.add(rule.id)
        if rule.signature in sigs:
            raise ParseError(
                f"rules {sigs[rule.signature]!r} and {rule.id!r} are duplicates (same signature)")
        sigs[rule.signature] = rule.id
        rules.append(rule)
    return analyze_program(rules)


def assert_weakly_acyclic(rules: Sequence[Rule]) -> None:
    bad = [r for r in rules if r.cyclic]
    if bad:
        details = []
        for r in bad:
            cyc = " -> ".join(f"{rel}.{pos}" for rel, pos in r.cycle_path)
            details.append(f"{r.id}: special edge "
                           f"{r.cyclic_edges[0][0]}.{r.cyclic_edges[0][1]} -> "
                           f"{r.cyclic_edges[0][2]}.{r.cyclic_edges[0][3]} on cycle {cyc}")
        raise ProgramError("program rejected: not weakly acyclic (" + "; ".join(details) + ")")
