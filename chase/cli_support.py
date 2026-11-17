"""Scenario loading and JSON helpers shared by the CLI and the library."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Sequence, Tuple

from .model import ModelError, Rule, Schema, parse_program, schema_from_json


def load_scenario(path: str) -> Dict[str, Any]:
    with open(path, encoding="utf-8") as fh:
        try:
            obj = json.load(fh)
        except json.JSONDecodeError as exc:
            raise ModelError(f"{path}: invalid JSON: {exc}") from None
    if not isinstance(obj, dict):
        raise ModelError(f"{path}: scenario must be a JSON object")
    for req in ("schema", "rules"):
        if req not in obj:
            raise ModelError(f"{path}: missing required key {req!r}")
    return obj


def scenario_schema(sc: Dict[str, Any]) -> Tuple[Schema, List[Rule]]:
    schema = schema_from_json(sc["schema"])
    rules = parse_program({"rules": sc["rules"]}, schema)
    return schema, rules


def _ks_to_text(v: Sequence[object]) -> str:
    if v[0] == "n":
        return f"NULL:{v[1]}"
    if v[0] == "s":
        return json.dumps(v[1], ensure_ascii=False)
    return str(v[1])


def conflict_to_json(conflict) -> Dict[str, Any]:
    return {
        "status": "conflict",
        "rule": conflict.rule_id,
        "binding": {k: [v[0], v[1]] for k, v in conflict.binding.items()},
        "left": list(conflict.left),
        "right": list(conflict.right),
        "left_display": _ks_to_text(conflict.left),
        "right_display": _ks_to_text(conflict.right),
        "support": [{"fact": f"{rel}({', '.join(_ks_to_text(v) for v in row)})"}
                    for rel, row in conflict.support],
        "left_sources": conflict.left_sources,
        "right_sources": conflict.right_sources,
        "explanation": conflict.explanation,
        "note": "no value is chosen by priority; the input is inconsistent",
    }
