"""Union-find over labeled nulls, with constants as absorbing roots.

A value is identified by a *token*:

* ``("n", null_id)``  -- a labeled null placeholder,
* ``("s", text)``     -- a source string,
* ``("i", integer)``  -- a source integer.

Only null tokens have a parent.  Two distinct constants can never be unioned
(the engine reports a conflict); a null unioned with a constant becomes that
constant, which is exactly the "unknowns and constants are unified" rule.
Null classes pick the smallest null id as representative, which makes ids
compact but trigger-order dependent (the tests compare results up to a
bijection, as required).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Set, Tuple

from .model import value_key

Token = Tuple[str, object]


class ConstantConflict(Exception):
    def __init__(self, left: Token, right: Token) -> None:
        self.left = left
        self.right = right
        super().__init__(f"cannot unify distinct constants {left!r} and {right!r}")


def token_of(value: object) -> Token:
    return value_key(value)  # type: ignore[return-value]


def _null_order(token: Token) -> Tuple[int, str]:
    s = str(token[1])
    digits = "".join(ch for ch in s if ch.isdigit())
    return (int(digits) if digits else 10 ** 18, s)


@dataclass(frozen=True)
class UnionEvent:
    left: Token
    right: Token
    result: Token


class Unifier:
    def __init__(self) -> None:
        self._parent: Dict[Token, Token] = {}

    def find(self, token: Token) -> Token:
        if token[0] != "n":
            return token
        path: List[Token] = []
        cur = token
        while cur[0] == "n":
            nxt = self._parent.get(cur)
            if nxt is None or nxt == cur:
                break
            path.append(cur)
            cur = nxt
        for node in path:
            self._parent[node] = cur
        return cur

    def canon_ks(self, key: Tuple[str, object]) -> Tuple[str, object]:
        root = self.find(key)  # type: ignore[arg-type]
        return (root[0], root[1])

    def canon(self, value: object) -> object:
        token = token_of(value)
        root = self.find(token)
        if root == token:
            return value
        if root[0] == "n":
            from .model import LNullValue
            return LNullValue(str(root[1]), f"NULL:{root[1]}")
        return root[1]

    def token_for(self, value: object) -> Token:
        return self.find(token_of(value))

    def union_tokens(self, left: Token, right: Token) -> UnionEvent:
        lr, rr = self.find(left), self.find(right)
        if lr == rr:
            return None  # type: ignore[return-value]
        if lr[0] != "n" and rr[0] != "n":
            raise ConstantConflict(lr, rr)
        if lr[0] != "n":
            self._parent[rr] = lr
            return UnionEvent(left, right, lr)
        if rr[0] != "n":
            self._parent[lr] = rr
            return UnionEvent(left, right, rr)
        if _null_order(lr) <= _null_order(rr):
            self._parent[rr] = lr
            return UnionEvent(left, right, lr)
        self._parent[lr] = rr
        return UnionEvent(left, right, rr)

    def union(self, left: object, right: object) -> UnionEvent:
        return self.union_tokens(token_of(left), token_of(right))

    def null_ids(self) -> Set[str]:
        out: Set[str] = set()
        for token in self._parent:
            if token[0] == "n":
                out.add(str(token[1]))
        return out

    def roots(self, null_ids: Iterable[str]) -> Set[Token]:
        return {self.find(("n", nid)) for nid in null_ids}
