"""Tiny parser for fact patterns used by ``chase trace --row``.

``Customer("c", *)``, ``Order(o1, _)``, ``KnownAddress("c", "城东")``.
Values must be quoted strings or bare integers; ``*`` / ``_`` are wildcards.
"""

from __future__ import annotations

import re
from typing import List, Sequence

_ATOM = re.compile(r"^\s*([A-Za-z][A-Za-z0-9_]*)\s*\((.*)\)\s*$")


class PatternError(Exception):
    pass


def _split(inside: str) -> List[str]:
    parts, cur, in_q = [], [], False
    for ch in inside:
        if ch == '"':
            in_q = not in_q
            cur.append(ch)
        elif ch == "," and not in_q:
            parts.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    parts.append("".join(cur))
    return parts


class Pattern:
    def __init__(self, rel: str, terms: List[object]) -> None:
        self.rel = rel
        self.terms = terms  # None = wildcard, else ("s",..)|("i",..)

    def matches(self, vals: Sequence[Sequence[object]]) -> bool:
        if len(vals) != len(self.terms):
            return False
        for pat, val in zip(self.terms, vals):
            if pat is None:
                continue
            if tuple(pat) != (val[0], val[1]):
                return False
        return True

def parse_pattern(text: str) -> Pattern:
    m = _ATOM.match(text)
    if not m:
        raise PatternError(f"malformed fact pattern {text!r}")
    rel, inside = m.group(1), m.group(2)
    terms: List[object] = []
    for raw in _split(inside):
        s = raw.strip()
        if s in ("*", "_"):
            terms.append(None)
        elif s.startswith('"') and s.endswith('"') and len(s) >= 2:
            terms.append(("s", s[1:-1]))
        elif re.fullmatch(r"[+-]?\d+", s):
            terms.append(("i", int(s)))
        else:
            raise PatternError(
                f"pattern term {s!r}: use \"quoted string\", an integer, or *")
    return Pattern(rel, terms)
