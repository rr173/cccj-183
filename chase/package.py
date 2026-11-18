"""Package writer/reader.

A completion package is a directory laid out as::

    <dir>/
      schema.json          relation -> column types
      rules.json           normalized program with weak-acyclicity annotations
      facts.csv            the output instance, one row of JSON values per line
      nulls.json           labeled nulls: id, display, provenance, final class
      conflict.json        (only when status == conflict)
      package.json         status marker and stats (written LAST)
      provenance.sqlite    triggers, merges, source paths

``package.json`` is written last and atomically; a crash at any earlier point
leaves no status file, so the directory is never mistaken for a complete
package.  A complete package is additionally verified independently before it
is moved into place, and publishing never overwrites an existing complete
package.
"""

from __future__ import annotations

import csv
import io
import json
import os
import shutil
import tempfile
import time
from typing import Any, Dict, List, Optional, Tuple

from .model import ModelError, LNullValue

FORMAT = "relational-completion-package/1"
STATUS_COMPLETE = "complete"
STATUS_CONFLICT = "conflict"
STATUS_INCOMPLETE = "incomplete"
STATUS_ERROR = "error"


def _json_dump(obj: Any, path: str) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, ensure_ascii=False, indent=2, sort_keys=True)
        fh.write("\n")


def encode_value(v: object) -> Dict[str, Any]:
    if isinstance(v, LNullValue):
        return {"kind": "labeled_null", "id": v.id, "display": v.display}
    if isinstance(v, str):
        return {"kind": "string", "value": v}
    if isinstance(v, int) and not isinstance(v, bool):
        return {"kind": "int", "value": v}
    raise ModelError(f"cannot encode value {v!r}")


def decode_value(obj: Any) -> object:
    if not isinstance(obj, dict) or "kind" not in obj:
        raise ModelError(f"invalid encoded value {obj!r}")
    kind = obj["kind"]
    if kind == "labeled_null":
        nid = obj.get("id")
        if not isinstance(nid, str) or not nid:
            raise ModelError("labeled null needs a non-empty string id")
        return LNullValue(nid, obj.get("display", f"NULL:{nid}"))
    if kind == "string":
        v = obj.get("value")
        if not isinstance(v, str):
            raise ModelError("string value must hold a str")
        return v
    if kind == "int":
        v = obj.get("value")
        if not isinstance(v, int) or isinstance(v, bool):
            raise ModelError("int value must hold an int")
        return v
    raise ModelError(f"unknown value kind {kind!r}")


# --------------------------------------------------------------------------- #
# Package object
# --------------------------------------------------------------------------- #
class Package:
    def __init__(self, directory: str) -> None:
        self.dir = os.path.abspath(directory)
        self.schema: Dict[str, List[str]] = {}
        self.rules: Dict[str, Any] = {}
        self.facts: Dict[str, List[Tuple[object, ...]]] = {}
        self.nulls: Dict[str, Any] = {}
        self.conflict: Optional[Dict[str, Any]] = None
        self.meta: Dict[str, Any] = {}

    def path(self, name: str) -> str:
        return os.path.join(self.dir, name)

    # -- writing ------------------------------------------------------------ #
    def write(self, path: str, status: str, schema: Dict[str, List[str]],
              rules: Dict[str, Any], facts: Dict[str, List[Tuple[object, ...]]],
              nulls: Dict[str, Any], meta: Dict[str, Any],
              conflict: Optional[Dict[str, Any]] = None) -> None:
        if os.path.exists(path):
            if os.path.isdir(path) and os.listdir(path):
                raise FileExistsError(
                    f"refusing to overwrite existing package directory {path!r}")
        os.makedirs(path, exist_ok=True)
        _json_dump(schema, os.path.join(path, "schema.json"))
        _json_dump(rules, os.path.join(path, "rules.json"))
        _json_dump(nulls, os.path.join(path, "nulls.json"))
        with open(os.path.join(path, "facts.csv"), "w", encoding="utf-8", newline="") as fh:
            w = csv.writer(fh, lineterminator="\n")
            w.writerow(["relation", "values"])
            for rel, rows in facts.items():
                for row in rows:
                    w.writerow([rel, json.dumps([encode_value(v) for v in row],
                                                ensure_ascii=False)])
        if conflict is not None:
            _json_dump(conflict, os.path.join(path, "conflict.json"))
        meta = dict(meta)
        meta["format"] = FORMAT
        meta["status"] = status
        meta["written_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        meta["relation_order"] = list(facts.keys())
        # status marker last, atomically, so an interrupted write is inert
        tmp = os.path.join(path, ".package.json.tmp")
        _json_dump(meta, tmp)
        os.replace(tmp, os.path.join(path, "package.json"))

    # -- reading ------------------------------------------------------------ #
    def load(self, require_status: Optional[str] = None) -> bool:
        meta_path = self.path("package.json")
        if not os.path.isfile(meta_path):
            return False
        with open(meta_path, encoding="utf-8") as fh:
            self.meta = json.load(fh)
        if self.meta.get("format") != FORMAT:
            raise ModelError(f"{self.dir}: not a {FORMAT} package")
        if require_status is not None and self.meta.get("status") != require_status:
            return False
        with open(self.path("schema.json"), encoding="utf-8") as fh:
            self.schema = json.load(fh)
        with open(self.path("rules.json"), encoding="utf-8") as fh:
            self.rules = json.load(fh)
        with open(self.path("nulls.json"), encoding="utf-8") as fh:
            self.nulls = json.load(fh)
        if os.path.isfile(self.path("conflict.json")):
            with open(self.path("conflict.json"), encoding="utf-8") as fh:
                self.conflict = json.load(fh)
        # accept both the legacy {rel: [types]} schema and the named-column
        # {columns, column_names} layout
        if isinstance(self.schema, dict) and "columns" in self.schema:
            type_map = self.schema["columns"]
        else:
            type_map = self.schema
        facts: Dict[str, List[Tuple[object, ...]]] = {r: [] for r in type_map}
        with open(self.path("facts.csv"), encoding="utf-8", newline="") as fh:
            reader = csv.reader(fh)
            header = next(reader, None)
            if header != ["relation", "values"]:
                raise ModelError(f"{self.dir}: bad facts.csv header")
            for row in reader:
                if not row:
                    continue
                rel, payload = row[0], row[1]
                if rel not in facts:
                    raise ModelError(f"{self.dir}: fact for undeclared relation {rel!r}")
                vals = tuple(decode_value(v) for v in json.loads(payload))
                if len(vals) != len(type_map[rel]):
                    raise ModelError(f"{self.dir}: arity mismatch in {rel}: {vals!r}")
                facts[rel].append(vals)
        self.facts = facts
        return True

    def null_ids(self) -> List[str]:
        return list(self.nulls.get("nulls", {}).keys())

    def get_facts(self, rel: str) -> List[Tuple[object, ...]]:
        return self.facts.get(rel, [])


def open_package(path: str, require_status: Optional[str] = None) -> Optional[Package]:
    pkg = Package(path)
    if not pkg.load(require_status=require_status):
        return None
    return pkg


def stage_and_publish(final_path: str, build, force: bool = False) -> str:
    """Build into a temp dir, then move into ``final_path``.

    ``build(tmpdir) -> None`` writes the package under ``tmpdir``.
    Publishing refuses to clobber an existing *complete* package unless
    ``force`` -- errors and interrupted runs therefore cannot overwrite a
    complete package.
    """
    tmp = tempfile.mkdtemp(prefix=".staging-", dir=os.path.dirname(final_path) or ".")
    try:
        build(tmp)
        if os.path.exists(final_path):
            existing = os.path.join(final_path, "package.json")
            if os.path.isfile(existing) and not force:
                with open(existing, encoding="utf-8") as fh:
                    status = json.load(fh).get("status")
                if status == STATUS_COMPLETE:
                    raise FileExistsError(
                        f"a complete package already exists at {final_path!r} "
                        f"(pass force=True to replace it)")
            shutil.rmtree(final_path)
        os.replace(tmp, final_path)
        return final_path
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
