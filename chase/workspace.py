"""Versioned relational-completion workspace.

A workspace pins a typed schema and a TGD/EGD program at creation time and
then accepts a *stream of source corrections*:

* Sources are **records** keyed by ``(source_id, record_id)``.  Each record
  stores a relation name and its **raw typed tuple** — the tuple is never
  rewritten by unification; unification only happens inside one completion
  run.
* Corrections arrive in **batches** with a unique ``batch_id``.  A batch is a
  list of ``put`` (insert or replace one record) and ``delete`` (remove an
  existing record) operations.  The whole batch is validated, applied to a
  *candidate* source set, and only then is the completion run — no business
  conflict is ever reported against a per-operation intermediate state.
* Every first-seen legal batch produces one monotonically numbered **source
  version**.  The version stores the batch identity, the traceable change to
  the raw source set, and the completion outcome: ``complete``, ``conflict``
  or ``incomplete``.  Illegal batches (bad operations, type errors, unknown
  relations) are rejected as a whole: they consume no version and change no
  source.
* A legal batch whose completion conflicts or exhausts its budget is still a
  queryable version; later corrections continue from the *latest source
  version*, never silently from the last successful one.  Two pointers are
  exposed separately: ``latest_source_version`` and
  ``latest_complete_version``.
* Results are always decided from the **raw sources of that version**, never
  from a previous quotient: the completion is re-run from scratch, so unknowns
  that were merged by an EGD can split apart again once the records that
  forced the equality are revoked, and derived facts whose last external
  support disappeared vanish (recursive derivations cannot keep each other
  alive).
* Re-sending a batch with the same ``batch_id`` and the same typed content
  returns the original receipt without recomputing; the same id with
  different content is rejected.  Content equality is decided on a canonical
  typed form: JSON object key order and operation order are irrelevant,
  ``{"null": "u1"}`` is the unknown ``u1`` (never the string ``"u1"``), and
  JSON booleans/floats are rejected rather than confused with integers.
* All state mutations of a commit happen in **one SQLite transaction**: after
  a crash, a reopening process sees either the pre-commit or the fully
  committed state — sources, version result and both version pointers can
  never disagree.  Writers serialize on the database lock; a commit that
  names a parent version that is no longer the latest fails with a
  *stale parent* error and is never silently re-based.

Persistent layout::

    <workspace>/
      workspace.sqlite     all state (meta, batches, versions, sources, history)

The module deliberately re-uses the existing engine (:mod:`chase.chase`),
rule language (:mod:`chase.model`) and unknown semantics.  Exported version
packages keep the existing package format so ``chase verify`` works on them.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
import zlib
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .chase import Budgets, Engine
from .cli_support import conflict_to_json
from .model import (
    LNullValue,
    ModelError,
    ProgramError,
    Rule,
    Schema,
    assert_weakly_acyclic,
    parse_program,
    parse_value,
    schema_from_json,
)
from .package import encode_value
from .provenance import ProvenanceDB
from .store import Store

WS_FORMAT = "relational-completion-workspace/1"

STATUS_COMPLETE = "complete"
STATUS_CONFLICT = "conflict"
STATUS_INCOMPLETE = "incomplete"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class WorkspaceError(Exception):
    """Internal workspace failure (never silently swallowed)."""


class BatchInvalid(Exception):
    """The batch is illegal as a whole: rejected, no version consumed."""

    def __init__(self, reason: str, detail: str = "") -> None:
        self.reason = reason
        self.detail = detail
        super().__init__(reason + (f": {detail}" if detail else ""))


class BatchIdConflict(Exception):
    """Same batch_id re-used with different typed content."""

    def __init__(self, batch_id: str) -> None:
        self.batch_id = batch_id
        super().__init__(
            f"batch id {batch_id!r} was already committed with different content; "
            f"re-sends must be byte-identical in typed content")


class StaleParent(Exception):
    """The parent version the caller based the batch on is no longer latest."""

    def __init__(self, expected: int, latest: int) -> None:
        self.expected = expected
        self.latest = latest
        super().__init__(
            f"stale parent version: batch is based on v{expected} but the latest "
            f"source version is v{latest}; re-read the workspace and decide again "
            f"(the workspace never re-bases a batch silently)")


# --------------------------------------------------------------------------- #
# Crash hook (test-only): CHASE_WS_CRASH_AT=before_commit|after_commit
# --------------------------------------------------------------------------- #
def _crash_point(name: str) -> None:
    if os.environ.get("CHASE_WS_CRASH_AT") == name:
        # hard kill: no cleanup, no finally — exactly what a crash is
        os._exit(97)


# --------------------------------------------------------------------------- #
# Batch parsing and canonical content
# --------------------------------------------------------------------------- #
def _canon_value(raw: object, where: str) -> List[Any]:
    """Typed canonical token for one source value.

    ``["s", text]`` / ``["i", int]`` / ``["n", null_id]``.  Booleans and
    floats are rejected (a JSON ``true`` must never become the int ``1``);
    ``{"null": "u1"}`` is the unknown ``u1`` — a string ``"u1"`` is a
    different, ordinary constant.
    """
    v = parse_value(raw)  # raises ModelError on bool/float/JSON null/bad dict
    if isinstance(v, LNullValue):
        return ["n", v.id]
    if isinstance(v, str):
        return ["s", v]
    return ["i", v]


def _token_to_model(tok: Sequence[Any]) -> object:
    if tok[0] == "n":
        return LNullValue(str(tok[1]), f"NULL:{tok[1]}")
    if tok[0] == "s":
        return tok[1]
    return tok[1]


class ParsedOp:
    def __init__(self, op: str, source_id: str, record_id: str,
                 relation: Optional[str], tuple_tokens: Optional[List[List[Any]]],
                 canonical: Dict[str, Any]) -> None:
        self.op = op
        self.source_id = source_id
        self.record_id = record_id
        self.relation = relation
        self.tuple_tokens = tuple_tokens
        self.canonical = canonical

    @property
    def key(self) -> Tuple[str, str]:
        return (self.source_id, self.record_id)


class ParsedBatch:
    def __init__(self, batch_id: str, parent: Optional[int],
                 budgets: Dict[str, Any], ops: List[ParsedOp],
                 content_hash: str, content_json: str) -> None:
        self.batch_id = batch_id
        self.parent = parent
        self.budgets = budgets
        self.ops = ops
        self.content_hash = content_hash
        self.content_json = content_json


def parse_batch(obj: object, schema: Schema) -> ParsedBatch:
    """Validate a batch object against the fixed schema.

    Pure function of the batch and the (immutable) schema: raises
    :class:`BatchInvalid` on any illegal operation, type error or reference
    to an undeclared relation.  Nothing here touches workspace state.
    """
    if not isinstance(obj, dict):
        raise BatchInvalid("malformed-batch", "batch must be a JSON object")
    batch_id = obj.get("batch_id")
    if not isinstance(batch_id, str) or not batch_id:
        raise BatchInvalid("malformed-batch", "a non-empty string 'batch_id' is required")
    parent = obj.get("parent")
    if parent is not None and (not isinstance(parent, int) or isinstance(parent, bool)
                               or parent < 0):
        raise BatchInvalid("malformed-batch", "'parent' must be a non-negative integer")
    budgets = obj.get("budgets", {})
    if not isinstance(budgets, dict):
        raise BatchInvalid("malformed-batch", "'budgets' must be an object")
    try:
        Budgets(**budgets)
    except TypeError as exc:
        raise BatchInvalid("bad-budgets", f"unknown budget key: {exc}")
    ops_raw = obj.get("ops")
    if not isinstance(ops_raw, list):
        raise BatchInvalid("malformed-batch", "'ops' must be a list (possibly empty)")

    ops: List[ParsedOp] = []
    seen_keys: set = set()
    for idx, raw in enumerate(ops_raw):
        where = f"op #{idx}"
        if not isinstance(raw, dict):
            raise BatchInvalid("malformed-op", f"{where}: operation must be an object")
        op = raw.get("op")
        if op not in ("put", "delete"):
            raise BatchInvalid("malformed-op",
                               f"{where}: 'op' must be 'put' or 'delete', got {op!r}")
        sid, rid = raw.get("source_id"), raw.get("record_id")
        for name, val in (("source_id", sid), ("record_id", rid)):
            if not isinstance(val, str) or not val:
                raise BatchInvalid("malformed-op",
                                   f"{where}: {name} must be a non-empty string")
        key = (sid, rid)
        if key in seen_keys:
            raise BatchInvalid(
                "duplicate-key",
                f"{where}: source record {sid}/{rid} is operated on twice in one "
                f"batch; a batch must contain at most one operation per record key")
        seen_keys.add(key)
        if op == "delete":
            canon = {"op": "delete", "source_id": sid, "record_id": rid}
            ops.append(ParsedOp("delete", sid, rid, None, None, canon))
            continue
        relation = raw.get("relation")
        if not isinstance(relation, str) or relation not in schema.relations:
            raise BatchInvalid("unknown-relation",
                               f"{where}: relation {relation!r} is not declared in the "
                               f"workspace schema")
        cols = schema.relations[relation]
        tpl = raw.get("tuple")
        if not isinstance(tpl, list):
            raise BatchInvalid("malformed-op", f"{where}: 'tuple' must be a list")
        if len(tpl) != len(cols):
            raise BatchInvalid(
                "arity-mismatch",
                f"{where}: relation {relation} has {len(cols)} columns, got "
                f"{len(tpl)} values")
        tokens: List[List[Any]] = []
        for pos, (raw_v, dom) in enumerate(zip(tpl, cols)):
            try:
                tok = _canon_value(raw_v, where)
            except ModelError as exc:
                raise BatchInvalid("type-error", f"{where} position {pos}: {exc}")
            if tok[0] == "s" and dom != "text":
                raise BatchInvalid(
                    "type-error",
                    f"{where} position {pos}: text value {tok[1]!r} in {dom} column "
                    f"of {relation}")
            if tok[0] == "i" and dom != "int":
                raise BatchInvalid(
                    "type-error",
                    f"{where} position {pos}: int value {tok[1]!r} in {dom} column "
                    f"of {relation}")
            tokens.append(tok)
        canon = {"op": "put", "source_id": sid, "record_id": rid,
                 "relation": relation, "tuple": tokens}
        ops.append(ParsedOp("put", sid, rid, relation, tokens, canon))

    # Canonical content: operation order and JSON object key order are
    # irrelevant; the typed tuple tokens are the content.  Unknowns compare
    # by id, never by display string.
    canon_ops = sorted((op.canonical for op in ops),
                       key=lambda c: json.dumps(c, sort_keys=True))
    content = {"batch_id": batch_id, "ops": canon_ops}
    content_json = json.dumps(content, ensure_ascii=False, sort_keys=True,
                              separators=(",", ":"))
    digest = hashlib.sha256(content_json.encode("utf-8")).hexdigest()
    return ParsedBatch(batch_id, parent, budgets, ops, digest, content_json)


# --------------------------------------------------------------------------- #
# Workspace
# --------------------------------------------------------------------------- #
_SCHEMA_SQL = """
CREATE TABLE meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE batches (
    batch_id TEXT PRIMARY KEY,
    version INTEGER NOT NULL,
    parent_version INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    content_json TEXT NOT NULL,
    receipt_json TEXT NOT NULL,
    received_at TEXT NOT NULL
);
CREATE TABLE versions (
    version INTEGER PRIMARY KEY,
    batch_id TEXT NOT NULL UNIQUE,
    parent_version INTEGER NOT NULL,
    status TEXT NOT NULL,
    reason TEXT NOT NULL,
    detail TEXT NOT NULL,
    stats_json TEXT NOT NULL,
    budgets_json TEXT NOT NULL,
    payload BLOB NOT NULL,
    provenance BLOB,
    created_at TEXT NOT NULL
);
CREATE TABLE source_records (
    source_id TEXT NOT NULL,
    record_id TEXT NOT NULL,
    relation TEXT NOT NULL,
    tuple_json TEXT NOT NULL,
    since_version INTEGER NOT NULL,
    PRIMARY KEY (source_id, record_id)
);
CREATE TABLE record_history (
    version INTEGER NOT NULL,
    source_id TEXT NOT NULL,
    record_id TEXT NOT NULL,
    op TEXT NOT NULL,
    relation TEXT,
    tuple_json TEXT,
    prev_relation TEXT,
    prev_tuple_json TEXT,
    PRIMARY KEY (version, source_id, record_id)
);
CREATE TABLE rejected_batches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT,
    reason TEXT NOT NULL,
    detail TEXT NOT NULL,
    content_hash TEXT,
    received_at TEXT NOT NULL
);
"""


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _pack(obj: Any) -> bytes:
    return zlib.compress(json.dumps(obj, ensure_ascii=False).encode("utf-8"))


def _unpack(blob: bytes) -> Any:
    return json.loads(zlib.decompress(blob).decode("utf-8"))


class Workspace:
    """An open workspace directory (close with :meth:`close` or `with`)."""

    # -- construction ------------------------------------------------------ #
    @classmethod
    def init(cls, directory: str, program: Dict[str, Any],
             budgets: Optional[Dict[str, Any]] = None) -> "Workspace":
        """Create a new workspace pinned to ``program`` (schema + rules)."""
        directory = os.path.abspath(directory)
        db_path = os.path.join(directory, "workspace.sqlite")
        if os.path.exists(db_path):
            raise WorkspaceError(
                f"workspace already exists at {directory!r}; refusing to re-initialize "
                f"(history would be destroyed)")
        schema_obj = program.get("schema")
        rules_obj = program.get("rules")
        if schema_obj is None or rules_obj is None:
            raise ModelError("program must contain 'schema' and 'rules'")
        schema = schema_from_json(schema_obj)
        rules = parse_program({"rules": rules_obj}, schema)
        assert_weakly_acyclic(rules)
        eff_budgets = program.get("budgets", {}) or {}
        if not isinstance(eff_budgets, dict):
            raise ModelError("program 'budgets' must be an object")
        eff_budgets = dict(eff_budgets)
        if budgets:
            eff_budgets.update(budgets)
        try:
            Budgets(**eff_budgets)  # validate budget keys now
        except TypeError as exc:
            raise ModelError(f"unknown budget key: {exc}") from None
        os.makedirs(directory, exist_ok=True)
        conn = sqlite3.connect(db_path, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript(_SCHEMA_SQL)
        meta = {
            "format": WS_FORMAT,
            "schema": json.dumps(schema.to_json(), ensure_ascii=False),
            "rules": json.dumps(rules_obj, ensure_ascii=False),
            "budgets": json.dumps(eff_budgets, ensure_ascii=False),
            "latest_source_version": "0",
            "latest_complete_version": "",   # empty = none yet
            "created_at": _now(),
        }
        for k, v in meta.items():
            conn.execute("INSERT INTO meta(key, value) VALUES (?, ?)", (k, v))
        conn.commit()
        conn.close()
        return cls(directory)

    def __init__(self, directory: str) -> None:
        self.dir = os.path.abspath(directory)
        self.db_path = os.path.join(self.dir, "workspace.sqlite")
        if not os.path.isfile(self.db_path):
            raise WorkspaceError(f"no workspace at {self.dir!r} (run ws init first)")
        self.conn = sqlite3.connect(self.db_path, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA busy_timeout=30000")
        meta = {r["key"]: r["value"] for r in
                self.conn.execute("SELECT key, value FROM meta")}
        if meta.get("format") != WS_FORMAT:
            raise WorkspaceError(f"{self.dir}: not a {WS_FORMAT} workspace")
        self._meta = meta
        self.schema: Schema = schema_from_json(json.loads(meta["schema"]))
        self.rules: List[Rule] = parse_program(
            {"rules": json.loads(meta["rules"])}, self.schema)
        assert_weakly_acyclic(self.rules)
        self.default_budgets: Dict[str, Any] = json.loads(meta["budgets"])

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

    def __enter__(self) -> "Workspace":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- meta pointers ------------------------------------------------------ #
    def latest_source_version(self) -> int:
        row = self.conn.execute("SELECT value FROM meta WHERE key='latest_source_version'")
        return int(row.fetchone()[0])

    def latest_complete_version(self) -> Optional[int]:
        row = self.conn.execute(
            "SELECT value FROM meta WHERE key='latest_complete_version'")
        raw = row.fetchone()[0]
        return int(raw) if raw else None

    def _set_pointer(self, key: str, value: str) -> None:
        self.conn.execute("UPDATE meta SET value=? WHERE key=?", (value, key))
        self._meta[key] = value

    # -- batch commit ------------------------------------------------------- #
    def commit(self, batch_obj: object, parent: Optional[int] = None,
               budget_overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Validate and commit one batch; returns the commit receipt.

        The receipt is stable: re-sending the same batch id with the same
        typed content returns the *original* receipt with
        ``idempotent_replay: true`` and performs no computation.
        """
        try:
            parsed = parse_batch(batch_obj, self.schema)
        except BatchInvalid as exc:
            batch_id = batch_obj.get("batch_id") if isinstance(batch_obj, dict) else None
            self._record_rejection(batch_id if isinstance(batch_id, str) else None,
                                   exc.reason, exc.detail, None)
            raise

        if parent is not None and parsed.parent is not None and parent != parsed.parent:
            raise BatchInvalid(
                "parent-mismatch",
                f"batch declares parent v{parsed.parent} but the caller passed "
                f"--parent {parent}; pass at most one")
        effective_parent = parent if parent is not None else parsed.parent

        self.conn.execute("BEGIN IMMEDIATE")
        try:
            # 1. idempotent replay / id-content conflict — checked BEFORE the
            #    parent test so a completed batch's re-send is never rejected
            #    just because the world moved on.
            row = self.conn.execute(
                "SELECT content_hash, receipt_json FROM batches WHERE batch_id=?",
                (parsed.batch_id,)).fetchone()
            if row is not None:
                if row["content_hash"] == parsed.content_hash:
                    receipt = json.loads(row["receipt_json"])
                    receipt["idempotent_replay"] = True
                    self.conn.execute("ROLLBACK")
                    return receipt
                self.conn.execute("ROLLBACK")
                self._record_rejection(parsed.batch_id, "batch-id-conflict",
                                       "same batch id with different typed content",
                                       parsed.content_hash)
                raise BatchIdConflict(parsed.batch_id)

            # 2. optimistic concurrency: the parent the caller based the batch
            #    on must still be the latest source version.
            latest = self.latest_source_version()
            base = effective_parent if effective_parent is not None else latest
            if base != latest:
                self.conn.execute("ROLLBACK")
                self._record_rejection(
                    parsed.batch_id, "stale-parent",
                    f"based on v{base}, latest is v{latest}", parsed.content_hash)
                raise StaleParent(base, latest)

            # 3. whole-batch candidate source set (no per-op business checks)
            current = self._current_sources()
            candidate: Dict[Tuple[str, str], Tuple[str, Tuple[List[Any], ...]]] = \
                dict(current)
            history: List[Tuple[ParsedOp, Optional[Tuple[str, Tuple[List[Any], ...]]]]] = []
            for op in parsed.ops:
                prev = candidate.get(op.key)
                if op.op == "delete":
                    if prev is None:
                        self.conn.execute("ROLLBACK")
                        self._record_rejection(
                            parsed.batch_id, "delete-missing-record",
                            f"delete references unknown source record "
                            f"{op.source_id}/{op.record_id}", parsed.content_hash)
                        raise BatchInvalid(
                            "delete-missing-record",
                            f"delete references unknown source record "
                            f"{op.source_id}/{op.record_id}")
                    del candidate[op.key]
                else:
                    assert op.relation is not None and op.tuple_tokens is not None
                    candidate[op.key] = (op.relation, tuple(op.tuple_tokens))
                history.append((op, prev))

            # 4. run the completion on the candidate set, from raw sources
            budgets = dict(self.default_budgets)
            budgets.update(parsed.budgets)
            if budget_overrides:
                budgets.update({k: v for k, v in budget_overrides.items()
                                if v is not None})
            payload, prov_blob, run = self._run_completion(candidate, budgets)

            # 5. persist everything in ONE transaction: sources, history,
            #    batch identity, version result, pointers.
            version = latest + 1
            now = _now()
            for op, prev in history:
                self.conn.execute(
                    "INSERT INTO record_history(version, source_id, record_id, op, "
                    "relation, tuple_json, prev_relation, prev_tuple_json) "
                    "VALUES (?,?,?,?,?,?,?,?)",
                    (version, op.source_id, op.record_id, op.op,
                     op.relation,
                     json.dumps(op.tuple_tokens, ensure_ascii=False)
                     if op.tuple_tokens is not None else None,
                     prev[0] if prev else None,
                     json.dumps(list(prev[1]), ensure_ascii=False) if prev else None))
                if op.op == "delete":
                    self.conn.execute(
                        "DELETE FROM source_records WHERE source_id=? AND record_id=?",
                        (op.source_id, op.record_id))
                else:
                    self.conn.execute(
                        "INSERT INTO source_records(source_id, record_id, relation, "
                        "tuple_json, since_version) VALUES (?,?,?,?,?) "
                        "ON CONFLICT(source_id, record_id) DO UPDATE SET "
                        "relation=excluded.relation, tuple_json=excluded.tuple_json, "
                        "since_version=excluded.since_version",
                        (op.source_id, op.record_id, op.relation,
                         json.dumps(op.tuple_tokens, ensure_ascii=False), version))

            status = run["status"]
            stats_json = json.dumps(run["stats"], ensure_ascii=False)
            self.conn.execute(
                "INSERT INTO versions(version, batch_id, parent_version, status, "
                "reason, detail, stats_json, budgets_json, payload, provenance, "
                "created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (version, parsed.batch_id, latest, status, run["reason"],
                 run["detail"], stats_json, json.dumps(budgets, ensure_ascii=False),
                 _pack(payload), prov_blob, now))
            receipt = {
                "committed": True,
                "idempotent_replay": False,
                "batch_id": parsed.batch_id,
                "version": version,
                "parent_version": latest,
                "status": status,
                "reason": run["reason"],
                "detail": run["detail"],
                "stats": run["stats"],
                "content_hash": parsed.content_hash,
                "received_at": now,
            }
            self.conn.execute(
                "INSERT INTO batches(batch_id, version, parent_version, content_hash, "
                "content_json, receipt_json, received_at) VALUES (?,?,?,?,?,?,?)",
                (parsed.batch_id, version, latest, parsed.content_hash,
                 parsed.content_json, json.dumps(receipt, ensure_ascii=False), now))
            self._set_pointer("latest_source_version", str(version))
            if status == STATUS_COMPLETE:
                self._set_pointer("latest_complete_version", str(version))
            receipt["latest_source_version"] = version
            receipt["latest_complete_version"] = self.latest_complete_version()
            # test-only crash hook: die inside the uncommitted transaction
            _crash_point("before_commit")
            self.conn.commit()
            # test-only crash hook: die after a fully committed transaction
            _crash_point("after_commit")
            return receipt
        except (BatchIdConflict, StaleParent, BatchInvalid):
            raise
        except sqlite3.Error as exc:
            self.conn.rollback()
            raise WorkspaceError(f"database error during commit: {exc}") from exc
        except BaseException:
            self.conn.rollback()
            raise

    def _record_rejection(self, batch_id: Optional[str], reason: str,
                          detail: str, content_hash: Optional[str]) -> None:
        """Audit trail for rejected batches (never consumes a version)."""
        try:
            self.conn.execute(
                "INSERT INTO rejected_batches(batch_id, reason, detail, content_hash, "
                "received_at) VALUES (?,?,?,?,?)",
                (batch_id, reason, detail, content_hash, _now()))
            self.conn.commit()
        except sqlite3.Error:
            self.conn.rollback()

    # -- completion run ------------------------------------------------------ #
    def _run_completion(
            self,
            candidate: Dict[Tuple[str, str], Tuple[str, Tuple[List[Any], ...]]],
            budgets_obj: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], Optional[bytes], Dict[str, Any]]:
        """Re-run the chase from the raw candidate sources (full recompute).

        Nothing is inherited from earlier versions: no union-find state, no
        derived rows, no quotient.  This is what lets merged unknowns split
        again and unsupported derivations disappear.
        """
        budgets = Budgets(**budgets_obj)
        facts: Dict[str, List[Tuple[object, ...]]] = {}
        for (rel, tpl) in candidate.values():
            facts.setdefault(rel, []).append(
                tuple(_token_to_model(tok) for tok in tpl))

        scratch = os.path.join(self.dir, ".scratch.sqlite")
        prov_path = os.path.join(self.dir, ".prov.sqlite")
        for p in (scratch, prov_path):
            try:
                os.remove(p)
            except OSError:
                pass
        store = Store(scratch)
        try:
            engine = Engine(self.schema, self.rules, store, budgets,
                            provenance=True)
            input_count = engine.preload_input(facts, source="workspace-sources")
            result = engine.run()
            if result.status == "error":
                raise WorkspaceError(
                    f"engine error during completion: {result.reason}: {result.detail}")
            exported = engine.export()

            # input unknown id -> final value (constant or final class id)
            final = engine.final_null_map()
            rename = engine.normalize_null_ids()
            input_null_map: Dict[str, Any] = {}
            for nid, info in engine.null_info.items():
                if not info.get("input"):
                    continue
                root = final[nid]
                if root[0] == "n":
                    cls = rename.get(str(root[1]), str(root[1]))
                    input_null_map[nid] = {"kind": "labeled_null", "id": cls}
                elif root[0] == "s":
                    input_null_map[nid] = {"kind": "string", "value": root[1]}
                else:
                    input_null_map[nid] = {"kind": "int", "value": root[1]}

            # final fact -> every source record whose raw tuple lands on it
            def canon_tokens(tpl: Sequence[Sequence[Any]]) -> List[Any]:
                out: List[Any] = []
                for tok in tpl:
                    if tok[0] == "n":
                        root = final.get(str(tok[1]), ("n", tok[1]))
                        if root[0] == "n":
                            cls = rename.get(str(root[1]), str(root[1]))
                            out.append({"kind": "labeled_null", "id": cls})
                        elif root[0] == "s":
                            out.append({"kind": "string", "value": root[1]})
                        else:
                            out.append({"kind": "int", "value": root[1]})
                    elif tok[0] == "s":
                        out.append({"kind": "string", "value": tok[1]})
                    else:
                        out.append({"kind": "int", "value": tok[1]})
                return out

            fact_sources: Dict[str, Dict[str, Any]] = {}
            for (sid, rid), (rel, tpl) in sorted(candidate.items()):
                values = canon_tokens(tpl)
                fkey = rel + "|" + json.dumps(values, ensure_ascii=False,
                                              sort_keys=True)
                entry = fact_sources.setdefault(
                    fkey, {"fact": {"relation": rel, "values": values},
                           "records": []})
                entry["records"].append([sid, rid])
            fact_sources_list = [fact_sources[k] for k in sorted(fact_sources)]

            stats = dict(result.stats)
            stats["input_facts_loaded"] = input_count
            stats["source_records"] = len(candidate)
            stats["recompute_scope"] = "full"

            prov_blob: Optional[bytes] = None
            prov = ProvenanceDB(prov_path)
            prov.ingest(exported["triggers"], exported["merges"],
                        exported["origins"], exported["facts"])
            prov.write_schema(self.schema.to_json(),
                              [r.to_json() for r in self.rules])
            prov.close()
            with open(prov_path, "rb") as fh:
                prov_blob = zlib.compress(fh.read())

            conflict_json = None
            if result.status == STATUS_CONFLICT and result.conflict is not None:
                conflict_json = conflict_to_json(result.conflict)

            input_facts_json = {
                rel: [[_raw_json(v) for v in row] for row in rows]
                for rel, rows in facts.items()}

            payload = {
                "schema": self.schema.to_json(),
                "rules": json.loads(self._meta["rules"]),
                "facts": {rel: [[encode_value(v) for v in row] for row in rows]
                          for rel, rows in exported["facts"].items()},
                "nulls": exported["nulls"],
                "input_facts": input_facts_json,
                "input_null_map": input_null_map,
                "fact_sources": fact_sources_list,
                "conflict": conflict_json,
                "status": result.status,
                "reason": result.reason,
                "detail": result.detail,
                "stats": stats,
                "budgets": budgets_obj,
            }
            run = {"status": result.status, "reason": result.reason,
                   "detail": result.detail, "stats": stats}
            return payload, prov_blob, run
        finally:
            store.close()
            for p in (scratch, prov_path):
                try:
                    os.remove(p)
                except OSError:
                    pass

    # -- queries ------------------------------------------------------------- #
    def _current_sources(self) -> Dict[Tuple[str, str], Tuple[str, Tuple[List[Any], ...]]]:
        out: Dict[Tuple[str, str], Tuple[str, Tuple[List[Any], ...]]] = {}
        for row in self.conn.execute(
                "SELECT source_id, record_id, relation, tuple_json "
                "FROM source_records"):
            out[(row["source_id"], row["record_id"])] = (
                row["relation"], tuple(json.loads(row["tuple_json"])))
        return out

    def sources_at(self, version: Optional[int] = None
                   ) -> Dict[Tuple[str, str], Tuple[str, Tuple[List[Any], ...]]]:
        """Raw source set of ``version`` (default: latest).

        Reconstructed exactly from the current set plus the per-version
        change history (each history row stores the replaced previous
        record), so every version's sources stay traceable.
        """
        latest = self.latest_source_version()
        if version is None:
            version = latest
        if version < 0 or version > latest:
            raise WorkspaceError(f"no version {version} (latest is {latest})")
        cur = self._current_sources()
        for v in range(latest, version, -1):
            rows = self.conn.execute(
                "SELECT source_id, record_id, prev_relation, prev_tuple_json "
                "FROM record_history WHERE version=?", (v,)).fetchall()
            for row in rows:
                key = (row["source_id"], row["record_id"])
                if row["prev_relation"] is None:
                    cur.pop(key, None)
                else:
                    cur[key] = (row["prev_relation"],
                                tuple(json.loads(row["prev_tuple_json"])))
        return cur

    def get_version(self, version: int) -> Dict[str, Any]:
        row = self.conn.execute(
            "SELECT version, batch_id, parent_version, status, reason, detail, "
            "stats_json, budgets_json, created_at FROM versions WHERE version=?",
            (version,)).fetchone()
        if row is None:
            raise WorkspaceError(f"no version {version}")
        return {
            "version": row["version"],
            "batch_id": row["batch_id"],
            "parent_version": row["parent_version"],
            "status": row["status"],
            "reason": row["reason"],
            "detail": row["detail"],
            "stats": json.loads(row["stats_json"]),
            "budgets": json.loads(row["budgets_json"]),
            "created_at": row["created_at"],
        }

    def versions(self) -> List[Dict[str, Any]]:
        latest_complete = self.latest_complete_version()
        out = []
        for row in self.conn.execute(
                "SELECT version FROM versions ORDER BY version"):
            info = self.get_version(int(row["version"]))
            info["is_latest_source"] = info["version"] == self.latest_source_version()
            info["is_latest_complete"] = info["version"] == latest_complete
            out.append(info)
        return out

    def version_payload(self, version: int) -> Dict[str, Any]:
        row = self.conn.execute(
            "SELECT payload FROM versions WHERE version=?", (version,)).fetchone()
        if row is None:
            raise WorkspaceError(f"no version {version}")
        return _unpack(row["payload"])

    def version_provenance(self, version: int) -> Optional[bytes]:
        row = self.conn.execute(
            "SELECT provenance FROM versions WHERE version=?", (version,)).fetchone()
        if row is None or row["provenance"] is None:
            return None
        return zlib.decompress(row["provenance"])

    def status(self) -> Dict[str, Any]:
        counts = {}
        for table in ("batches", "versions", "source_records", "record_history",
                      "rejected_batches"):
            counts[table] = int(self.conn.execute(
                f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        return {
            "workspace": self.dir,
            "format": WS_FORMAT,
            "latest_source_version": self.latest_source_version(),
            "latest_complete_version": self.latest_complete_version(),
            "relations": sorted(self.schema.relations),
            "rules": len(self.rules),
            "counts": counts,
        }

    def resolve_version(self, spec: str) -> int:
        """Resolve 'latest' / 'latest-complete' / an integer to a version."""
        if spec == "latest":
            v = self.latest_source_version()
            if v == 0:
                raise WorkspaceError("workspace has no versions yet")
            return v
        if spec == "latest-complete":
            v = self.latest_complete_version()
            if v is None:
                raise WorkspaceError("workspace has no complete version yet")
            return v
        try:
            v = int(spec)
        except ValueError:
            raise WorkspaceError(f"cannot resolve version spec {spec!r}") from None
        self.get_version(v)  # raises if missing
        return v


def _raw_json(v: object) -> object:
    """Serialize a model value as scenario-style JSON ({"null": id} markers)."""
    if isinstance(v, LNullValue):
        return {"null": v.id}
    return v
