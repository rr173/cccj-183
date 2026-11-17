"""Export one workspace version as a verifiable package directory.

The export keeps the existing package format (``schema.json``, ``rules.json``,
``facts.csv``, ``nulls.json``, ``package.json``, ``provenance.sqlite``) so the
stock ``chase verify`` works on it, and adds:

* ``sources.json`` — the source manifest: every raw source record of that
  version (keyed by ``source_id``/``record_id``), the map from each final
  fact to **all** source records supporting it (not one merged source), and
  the input-unknown -> final-value map;
* a ``workspace`` block inside ``package.json`` — version, batch id, parent
  version and the version's honest status.

Exports are written from the immutable per-version payload stored at commit
time: later commits or failures never rewrite a historical export, and two
exports of the same version are byte-identical.
"""

from __future__ import annotations

import csv
import json
import os
import tempfile
from typing import Any, Dict, Optional

from .package import FORMAT as PKG_FORMAT
from .workspace import Workspace, WS_FORMAT

SOURCES_FORMAT = "relational-completion-workspace-sources/1"


def _json_dump(obj: Any, path: str) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, ensure_ascii=False, indent=2, sort_keys=True)
        fh.write("\n")


def export_version(ws: Workspace, version: int, out_dir: str,
                   force: bool = False) -> Dict[str, Any]:
    """Materialize ``version`` of ``ws`` into ``out_dir`` (atomically)."""
    info = ws.get_version(version)
    payload = ws.version_payload(version)
    sources = ws.sources_at(version)
    out_dir = os.path.abspath(out_dir)
    if os.path.exists(out_dir) and os.listdir(out_dir):
        if not force:
            raise FileExistsError(
                f"export directory {out_dir!r} exists and is not empty "
                f"(use --force to replace it)")
        import shutil
        shutil.rmtree(out_dir)
    parent_dir = os.path.dirname(out_dir) or "."
    os.makedirs(parent_dir, exist_ok=True)
    tmp = tempfile.mkdtemp(prefix=".export-", dir=parent_dir)
    try:
        _build(ws, info, payload, sources, tmp)
        os.replace(tmp, out_dir)
    except BaseException:
        import shutil
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    return {
        "version": version,
        "status": info["status"],
        "batch_id": info["batch_id"],
        "output": out_dir,
        "facts": sum(len(v) for v in payload["facts"].values()),
        "source_records": len(sources),
    }


def _build(ws: Workspace, info: Dict[str, Any], payload: Dict[str, Any],
           sources: Dict[Any, Any], path: str) -> None:
    status = payload["status"]
    _json_dump(payload["schema"], os.path.join(path, "schema.json"))
    _json_dump({"rules": payload["rules"], "weak_acyclic": True},
               os.path.join(path, "rules.json"))
    _json_dump(payload["nulls"], os.path.join(path, "nulls.json"))
    with open(os.path.join(path, "facts.csv"), "w", encoding="utf-8",
              newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(["relation", "values"])
        for rel, rows in payload["facts"].items():
            for row in rows:
                w.writerow([rel, json.dumps(row, ensure_ascii=False)])
    if payload.get("conflict") is not None:
        _json_dump(payload["conflict"], os.path.join(path, "conflict.json"))
    prov = ws.version_provenance(info["version"])
    if prov is not None:
        with open(os.path.join(path, "provenance.sqlite"), "wb") as fh:
            fh.write(prov)

    records = [
        {"source_id": sid, "record_id": rid, "relation": rel,
         "tuple": [_encode_token(tok) for tok in tpl]}
        for (sid, rid), (rel, tpl) in sorted(sources.items())
    ]
    sources_json = {
        "format": SOURCES_FORMAT,
        "workspace_version": info["version"],
        "batch_id": info["batch_id"],
        "parent_version": info["parent_version"],
        "status": status,
        "records": records,
        "fact_sources": payload["fact_sources"],
        "input_null_map": payload["input_null_map"],
    }
    _json_dump(sources_json, os.path.join(path, "sources.json"))

    meta = {
        "format": PKG_FORMAT,
        "status": status,
        "status_detail": payload["reason"],
        "detail": payload["detail"],
        "workspace": {
            "format": WS_FORMAT,
            "version": info["version"],
            "batch_id": info["batch_id"],
            "parent_version": info["parent_version"],
        },
        "input_facts": payload["input_facts"],
        "input_null_map": payload["input_null_map"],
        "input_source": "workspace-sources",
        "budgets": payload["budgets"],
        "stats": payload["stats"],
        "weak_acyclic": True,
        "written_at": info["created_at"],
        "relation_order": list(payload["facts"].keys()),
        "notes": [
            "this export corresponds to exactly one source version of one "
            "workspace; its status is stated honestly (complete/conflict/"
            "incomplete) and a non-complete export is not a usable "
            "completion package",
            "sources.json holds the raw source records of this version; "
            "derived rows of this package must not be fed back as source "
            "records — corrections go through 'ws commit' on raw sources",
            "labeled nulls are placeholders ('unknown'), never source values",
        ],
    }
    # status marker last, atomically (same convention as Package.write)
    tmp_meta = os.path.join(path, ".package.json.tmp")
    _json_dump(meta, tmp_meta)
    os.replace(tmp_meta, os.path.join(path, "package.json"))


def _encode_token(tok: Any) -> Dict[str, Any]:
    if tok[0] == "n":
        return {"kind": "labeled_null", "id": tok[1],
                "display": f"NULL:{tok[1]}"}
    if tok[0] == "s":
        return {"kind": "string", "value": tok[1]}
    return {"kind": "int", "value": tok[1]}
