"""Reusable CSV snapshot ingestion for the completion workspace.

The dangerous failure mode this module is built around is *silent deletion*:
a bad supplier row must never be skipped and then misread as "the supplier
retracted that record".  Consequently ingestion is split into two explicit
phases:

1. :meth:`IngestManager.stage_task` fully parses every shard, strictly
   converts every cell, and reconciles the typed snapshot against the current
   managed-source ledger — all without touching the workspace's source
   versions.  Everything needed to re-check or apply later (the typed rows
   and the full declaration) is persisted in the workspace SQLite database, so
   deleting or replacing the original CSV files after staging changes
   nothing.
2. :meth:`IngestManager.apply_task` turns the recorded candidates into one
   ordinary workspace batch of ``put``/``delete`` ops against the exact parent
   source version recorded at stage time.  A conflict/incomplete completion is
   still a successful ingestion application: it produces exactly one source
   version and one receipt, and re-applying the same task never replays it.

Streaming and memory
--------------------
CSV files are parsed one logical record at a time via :mod:`chase.csvscan`;
rows, diagnostics and candidates live in SQLite tables, not in Python
collections.  The whole CSV text, the whole key set and all row diagnostics
are never accumulated in memory.  The existing chase engine may still solve
the completion in its own way — that is measured and reported separately.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .csvscan import CSVRecord, CSVStructureError, iter_records
from .ingest_decl import (
    Declaration,
    SourceSpec,
    UnknownMarkers,
    encode_record_key,
    parse_int_cell,
)
from .workspace import Workspace, parse_batch

# row outcomes
ROW_OK = "ok"                 # fully converted
ROW_QUARANTINE = "quarantine" # key solved, non-key field invalid
ROW_BLOCKED = "blocked"       # key cannot be trusted; snapshot non-integral

# candidate decisions
CAND_PUT = "put"
CAND_DELETE = "delete"
CAND_KEEP = "keep"
CAND_QUARANTINE = "quarantine"

# task states
ST_STAGED = "staged"
ST_BLOCKED = "blocked"
ST_APPLIED = "applied"


class IngestError(Exception):
    """Staging/usage error that prevents creating or applying the task."""


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _file_digest(path: str) -> Tuple[str, int]:
    h = hashlib.sha256()
    size = 0
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(1 << 20)
            if not chunk:
                break
            h.update(chunk)
            size += len(chunk)
    return h.hexdigest(), size


# --------------------------------------------------------------------------- #
# row conversion
# --------------------------------------------------------------------------- #
def convert_cell(cell: str, domain: str, markers: UnknownMarkers,
                 is_key: bool, where: str
                 ) -> Tuple[Optional[List[Any]], Optional[str]]:
    """Strictly convert one cell.

    Returns ``(token, local_unknown_name)`` where token is ``None`` for an
    unknown (the local name is returned instead) or a typed token
    ``["s", text]`` / ``["i", int]``.  Raises ``ValueError`` with a located
    message on any illegal content.  Text is never trimmed: an empty string
    and a whitespace-only string are ordinary text values.
    """
    kind, local_name, suffix = markers.classify(cell)
    if kind == "unknown":
        if is_key:
            raise ValueError(f"{where}: unknown marker is not allowed in key columns")
        if suffix is not None and domain != "text":
            raise ValueError(
                f"{where}: an unknown-with-text-suffix is only valid in a text "
                f"column, this column is {domain}")
        return None, local_name
    if domain == "int":
        return ["i", parse_int_cell(cell)], None
    return ["s", cell], None


def convert_row(cells: Sequence[str], headers: Sequence[str],
                spec: SourceSpec, schema: Schema,
                markers: UnknownMarkers
                ) -> Tuple[str, Optional[List[List[Any]]],
                           Optional[List[List[Any]]], List[str], List[str]]:
    """Strictly convert one logical record.

    Returns ``(outcome, key_tokens, tuple_tokens, errors, unknown_names)``.
    Key columns are converted first; a key failure yields ``ROW_BLOCKED`` with
    no key (the snapshot then loses integrity).  Non-key failures yield
    ``ROW_QUARANTINE`` with a solved key.
    """
    columns = list(schema.names(spec.relation))
    domains = schema.relations[spec.relation]
    header_to_cell = dict(zip(headers, cells))
    key_set = set(spec.key_columns)
    errors: List[str] = []
    unknown_names: List[str] = []
    tuple_tokens: List[List[Any]] = []
    key_failed = False

    for pos, col in enumerate(columns):
        header = spec.header_for(col)
        cell = header_to_cell[header]
        domain = domains[pos]
        is_key = col in key_set
        where = f"column {col!r} (header {header!r})"
        try:
            tok, local_name = convert_cell(
                cell, domain, markers, is_key, where)
        except ValueError as exc:
            errors.append(str(exc))
            if is_key:
                key_failed = True
            tuple_tokens.append(["s", cell] if domain == "text"
                                else ["i", None])
            continue
        if tok is None:
            unknown_names.append(local_name if local_name is not None else "")
            tuple_tokens.append(["n", local_name])  # resolved post-grouping
        else:
            tuple_tokens.append(tok)

    if key_failed:
        return ROW_BLOCKED, None, None, errors, []
    key_tokens = [tuple_tokens[columns.index(c)] for c in spec.key_columns]
    if errors:
        return ROW_QUARANTINE, key_tokens, None, errors, []
    return ROW_OK, key_tokens, tuple_tokens, [], unknown_names


# --------------------------------------------------------------------------- #
# manager
# --------------------------------------------------------------------------- #
class IngestManager:
    """Staging/apply engine bound to an open :class:`Workspace`."""

    def __init__(self, ws: Workspace) -> None:
        self.ws = ws
        self.conn = ws.conn
        self._ensure_schema()
        self._recover_interrupted_tasks()

    def _recover_interrupted_tasks(self) -> None:
        """Reconcile tasks against workspace batches after a hard exit.

        The apply transaction writes the workspace version *and* the
        connector ledger together, but the task row's ``applied`` marker is
        flipped afterwards.  A process killed in that window leaves a task
        looking un-applied while its batch ``ingest:<task_id>`` is in fact
        committed.  On reopen we adopt the committed receipt rather than ever
        replaying the batch.
        """
        rows = self.conn.execute(
            "SELECT task_id FROM ing_tasks WHERE applied_version IS NULL").fetchall()
        for row in rows:
            task_id = row["task_id"]
            batch = self.conn.execute(
                "SELECT version, receipt_json FROM batches WHERE batch_id=?",
                ("ingest:" + task_id,)).fetchone()
            if batch is None:
                continue  # commit never happened; task stays re-applicable
            self.conn.execute(
                "UPDATE ing_tasks SET status=?, applied_version=?, "
                "receipt_json=?, applied_at=COALESCE(applied_at, ?) "
                "WHERE task_id=?",
                (ST_APPLIED, batch["version"], batch["receipt_json"],
                 _now(), task_id))
        self.conn.commit()

    # -- schema ------------------------------------------------------------- #
    def _ensure_schema(self) -> None:
        self.conn.executescript("""
        CREATE TABLE IF NOT EXISTS ing_connectors (
            connector TEXT PRIMARY KEY,
            binding_hash TEXT NOT NULL,
            binding_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ing_claims (
            source_id TEXT PRIMARY KEY,
            connector TEXT NOT NULL,
            relation TEXT NOT NULL,
            claimed_task TEXT NOT NULL,
            claimed_version INTEGER NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ing_managed_records (
            source_id TEXT NOT NULL,
            record_key TEXT NOT NULL,
            relation TEXT NOT NULL,
            tuple_json TEXT NOT NULL,
            fingerprint TEXT NOT NULL,
            since_version INTEGER NOT NULL,
            last_seen_version INTEGER NOT NULL,
            since_task TEXT NOT NULL,
            PRIMARY KEY (source_id, record_key)
        );
        CREATE TABLE IF NOT EXISTS ing_tasks (
            task_id TEXT PRIMARY KEY,
            connector TEXT NOT NULL,
            declaration_json TEXT NOT NULL,
            declaration_path TEXT,
            declaration_hash TEXT NOT NULL,
            binding_hash TEXT NOT NULL,
            status TEXT NOT NULL,
            applicable INTEGER NOT NULL,
            needs_empty_confirm INTEGER NOT NULL,
            parent_version INTEGER NOT NULL,
            content_hash TEXT NOT NULL,
            baseline_json TEXT NOT NULL,
            stats_json TEXT NOT NULL,
            blocking_json TEXT NOT NULL,
            applied_version INTEGER,
            receipt_json TEXT,
            created_at TEXT NOT NULL,
            applied_at TEXT
        );
        CREATE TABLE IF NOT EXISTS ing_files (
            task_id TEXT NOT NULL,
            source_id TEXT NOT NULL,
            shard_index INTEGER NOT NULL,
            path TEXT NOT NULL,
            sha256 TEXT,
            size_bytes INTEGER,
            header_json TEXT,
            status TEXT NOT NULL,
            logical_records INTEGER NOT NULL,
            data_records INTEGER NOT NULL,
            error_code TEXT,
            error_message TEXT,
            error_physical_line INTEGER,
            PRIMARY KEY (task_id, source_id, shard_index)
        );
        CREATE TABLE IF NOT EXISTS ing_rows (
            task_id TEXT NOT NULL,
            source_id TEXT NOT NULL,
            shard_index INTEGER NOT NULL,
            logical_seq INTEGER NOT NULL,
            physical_start INTEGER NOT NULL,
            physical_end INTEGER NOT NULL,
            record_key TEXT,
            outcome TEXT NOT NULL,
            tuple_json TEXT,
            errors_json TEXT NOT NULL,
            PRIMARY KEY (task_id, source_id, shard_index, logical_seq)
        );
        CREATE TABLE IF NOT EXISTS ing_unknowns (
            task_id TEXT NOT NULL,
            source_id TEXT NOT NULL,
            local_name TEXT,
            global_id TEXT NOT NULL,
            occurrences INTEGER NOT NULL,
            PRIMARY KEY (task_id, source_id, local_name)
        );
        CREATE TABLE IF NOT EXISTS ing_blocking (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id TEXT NOT NULL,
            source_id TEXT NOT NULL,
            code TEXT NOT NULL,
            message TEXT NOT NULL,
            shard_index INTEGER,
            logical_seq INTEGER,
            physical_start INTEGER,
            physical_end INTEGER,
            path TEXT
        );
        CREATE TABLE IF NOT EXISTS ing_baseline (
            task_id TEXT NOT NULL,
            source_id TEXT NOT NULL,
            record_key TEXT NOT NULL,
            tuple_json TEXT NOT NULL,
            since_version INTEGER NOT NULL,
            PRIMARY KEY (task_id, source_id, record_key)
        );
        CREATE TABLE IF NOT EXISTS ing_candidates (
            task_id TEXT NOT NULL,
            source_id TEXT NOT NULL,
            record_key TEXT NOT NULL,
            decision TEXT NOT NULL,
            relation TEXT NOT NULL,
            tuple_json TEXT,
            old_tuple_json TEXT,
            reason TEXT NOT NULL,
            evidence_json TEXT NOT NULL,
            PRIMARY KEY (task_id, source_id, record_key)
        );
        CREATE INDEX IF NOT EXISTS idx_ing_rows_task_key
            ON ing_rows(task_id, source_id, record_key);
        CREATE INDEX IF NOT EXISTS idx_ing_cand_task
            ON ing_candidates(task_id, source_id);
        CREATE INDEX IF NOT EXISTS idx_ing_managed_sid
            ON ing_managed_records(source_id, record_key);
        CREATE INDEX IF NOT EXISTS idx_ing_blocking_task
            ON ing_blocking(task_id);
        """)
        # lightweight migration for databases created before baseline tracking
        cols = {r["name"] for r in self.conn.execute(
            "PRAGMA table_info(ing_tasks)")}
        if "baseline_json" not in cols:
            self.conn.execute(
                "ALTER TABLE ing_tasks ADD COLUMN baseline_json TEXT NOT NULL "
                "DEFAULT '{}'")
        self.conn.commit()

    # -- public: staging ----------------------------------------------------- #
    def stage_task(self, task_id: str, declaration: Declaration
                   ) -> Dict[str, Any]:
        if not isinstance(task_id, str) or not task_id:
            raise IngestError("task_id must be a non-empty string")
        content_hash = hashlib.sha256(
            declaration.content_hash_input.encode("utf-8")).hexdigest()
        existing = self.conn.execute(
            "SELECT content_hash FROM ing_tasks WHERE task_id=?",
            (task_id,)).fetchone()
        if existing is not None:
            if existing["content_hash"] != content_hash:
                raise IngestError(
                    f"task id {task_id!r} already exists with different staged "
                    f"content; re-send the same declaration or use a new task id")
            return self.task_report(task_id)

        parent = self.ws.latest_source_version()
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            self._check_connector_binding(declaration)
            self.conn.execute(
                "INSERT INTO ing_tasks(task_id, connector, declaration_json, "
                "declaration_path, declaration_hash, binding_hash, status, "
                "applicable, needs_empty_confirm, parent_version, content_hash, "
                "baseline_json, stats_json, blocking_json, created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (task_id, declaration.connector,
                 json.dumps(declaration.raw, ensure_ascii=False),
                 declaration.path,
                 hashlib.sha256(json.dumps(
                     declaration.raw, ensure_ascii=False,
                     sort_keys=True).encode("utf-8")).hexdigest(),
                 content_hash, ST_STAGED, 1, 0, parent, content_hash,
                 "{}", "{}", "[]", _now()))
            # blocking diagnostics are streamed into ing_blocking, never
            # accumulated in a Python list (a huge feed can have many rows)
            stats: Dict[str, Any] = {"sources": {}, "totals": {}}
            for spec in declaration.sources:
                stats["sources"][spec.source_id] = self._stage_source(
                    task_id, spec, declaration)
            self._reconcile(task_id, declaration, stats)
            # baseline is streamed into ing_baseline (one row per managed
            # record) rather than materialized as one JSON blob, so its size
            # never scales the process RSS.
            self._capture_baseline(task_id, declaration)
            blocking_count = self.conn.execute(
                "SELECT COUNT(*) FROM ing_blocking WHERE task_id=?",
                (task_id,)).fetchone()[0]
            # An explicit full-clear needs a human confirmation.  The gate is
            # based on *effect*, not the declaration flag alone: any integral
            # snapshot with zero data rows that would retract existing managed
            # records is a clear, so a header-only feed with the flag left off
            # can never silently delete.  An empty snapshot of a not-yet-held
            # source deletes nothing and stays applicable without confirmation.
            needs_confirm = self._needs_empty_confirmation(
                task_id, declaration, stats)
            applicable = blocking_count == 0 and not needs_confirm
            self.conn.execute(
                "UPDATE ing_tasks SET status=?, applicable=?, "
                "needs_empty_confirm=?, stats_json=?, blocking_json=?, "
                "baseline_json=? WHERE task_id=?",
                (ST_BLOCKED if blocking_count else ST_STAGED,
                 1 if applicable else 0, 1 if needs_confirm else 0,
                 json.dumps(stats, ensure_ascii=False),
                 json.dumps({"count": blocking_count}, ensure_ascii=False),
                 "{}", task_id))
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise
        return self.task_report(task_id)

    def _add_blocking(self, task_id: str, source_id: str, code: str,
                      message: str, shard_index: Optional[int] = None,
                      logical_seq: Optional[int] = None,
                      physical_start: Optional[int] = None,
                      physical_end: Optional[int] = None,
                      path: Optional[str] = None) -> None:
        self.conn.execute(
            "INSERT INTO ing_blocking(task_id, source_id, code, message, "
            "shard_index, logical_seq, physical_start, physical_end, path) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (task_id, source_id, code, message, shard_index, logical_seq,
             physical_start, physical_end, path))

    def _blocking_list(self, task_id: str) -> List[Dict[str, Any]]:
        return [dict(r) for r in self.conn.execute(
            "SELECT source_id, code, message, shard_index, logical_seq, "
            "physical_start, physical_end, path FROM ing_blocking "
            "WHERE task_id=? ORDER BY id", (task_id,))]

    def _needs_empty_confirmation(self, task_id: str,
                                  declaration: Declaration,
                                  stats: Dict[str, Any]) -> bool:
        for spec in declaration.sources:
            src = stats["sources"][spec.source_id]
            if src["data_records"] != 0 or not src["integral"]:
                continue
            # a zero-row snapshot that would actually clear managed records
            delete_n = self.conn.execute(
                "SELECT COUNT(*) FROM ing_candidates WHERE task_id=? "
                "AND source_id=? AND decision=?",
                (task_id, spec.source_id, CAND_DELETE)).fetchone()[0]
            if delete_n > 0:
                return True
        return False


    def _capture_baseline(self, task_id: str,
                          declaration: Declaration) -> None:
        """Persist the exact live state of each managed source at check time.

        Rows are inserted in bounded batches from a cursor, so the baseline
        never materializes as one in-memory JSON blob.  Apply later verifies
        the live source set still equals this stored baseline.
        """
        for spec in declaration.sources:
            cur = self.conn.execute(
                "SELECT record_id, tuple_json, since_version "
                "FROM source_records WHERE source_id=?",
                (spec.source_id,))
            batch = cur.fetchmany(1000)
            while batch:
                self.conn.executemany(
                    "INSERT INTO ing_baseline(task_id, source_id, record_key, "
                    "tuple_json, since_version) VALUES (?,?,?,?,?)",
                    [(task_id, spec.source_id, r["record_id"],
                      r["tuple_json"], r["since_version"]) for r in batch])
                batch = cur.fetchmany(1000)

    def _check_connector_binding(self, declaration: Declaration) -> None:
        digest = hashlib.sha256(
            declaration.content_hash_input.encode("utf-8")).hexdigest()
        row = self.conn.execute(
            "SELECT binding_hash FROM ing_connectors WHERE connector=?",
            (declaration.connector,)).fetchone()
        if row is not None and row["binding_hash"] != digest:
            raise IngestError(
                f"connector {declaration.connector!r} is already bound to a "
                f"different relation/key/mapping/unknown interpretation; "
                f"bindings cannot change for one connector identity — use a new "
                f"connector name (no silent configuration migration)")
        if row is None:
            self.conn.execute(
                "INSERT INTO ing_connectors(connector, binding_hash, "
                "binding_json, created_at) VALUES (?,?,?,?)",
                (declaration.connector, digest,
                 declaration.content_hash_input, _now()))

    # -- per-source streaming parse ------------------------------------------ #
    def _stage_source(self, task_id: str, spec: SourceSpec,
                      declaration: Declaration) -> Dict[str, Any]:
        markers = declaration.unknown_markers
        src_stats = {"relation": spec.relation, "files": 0,
                     "logical_records": 0, "data_records": 0,
                     "ok": 0, "quarantine": 0, "blocked": 0,
                     "duplicate_collapsed": 0,
                     "integral": True, "empty_snapshot": spec.empty_snapshot}

        for shard_index, shard in enumerate(spec.shards):
            src_stats["files"] += 1
            path = shard.path
            try:
                digest, size = _file_digest(path)
            except OSError as exc:
                src_stats["integral"] = False
                self._add_blocking(task_id, spec.source_id,
                                   "file-unreadable", str(exc), path=path)
                self._insert_file_row(task_id, spec.source_id, shard_index,
                                      path, None, None, None, ROW_BLOCKED, 0, 0,
                                      "file-unreadable", str(exc), None)
                continue

            file_blocked = False
            try:
                fh = open(path, "rb")
            except OSError as exc:  # pragma: no cover - digest already opened it
                src_stats["integral"] = False
                self._add_blocking(task_id, spec.source_id,
                                   "file-unreadable", str(exc), path=path)
                self._insert_file_row(task_id, spec.source_id, shard_index,
                                      path, digest, size, None, ROW_BLOCKED, 0, 0,
                                      "file-unreadable", str(exc), None)
                continue

            logical = 0
            data_count = 0
            headers: Optional[List[str]] = None
            header_invalid = False
            try:
                for rec in iter_records(fh, path):
                    logical += 1
                    if rec.sequence == 1:
                        headers = rec.fields
                        # validate the header before touching any data row so a
                        # duplicate header can never silently drop a column
                        dup = self._duplicate_headers(headers)
                        missing = self._missing_headers(spec, headers)
                        if dup or missing:
                            header_invalid = True
                            src_stats["integral"] = False
                            code = ("duplicate-header" if dup else
                                    "missing-mapped-column")
                            msg = (f"duplicate headers {dup}" if dup else
                                   f"headers required by the mapping are "
                                   f"absent: {missing}")
                            self._add_blocking(
                                task_id, spec.source_id, code, msg,
                                physical_start=rec.physical_start,
                                physical_end=rec.physical_end, path=path)
                            self._insert_file_row(
                                task_id, spec.source_id, shard_index, path,
                                digest, size, json.dumps(headers),
                                ROW_BLOCKED, logical, 0, code, msg,
                                rec.physical_start)
                            break
                        continue
                    data_count += 1
                    self._ingest_record(task_id, spec, shard_index, rec,
                                        headers, markers, src_stats,
                                        declaration.connector)
            except CSVStructureError as exc:
                file_blocked = True
                src_stats["integral"] = False
                self._add_blocking(
                    task_id, spec.source_id, exc.code, str(exc),
                    shard_index=shard_index, path=path)
                self._insert_file_row(
                    task_id, spec.source_id, shard_index, path, digest, size,
                    json.dumps(headers) if headers is not None else None,
                    ROW_BLOCKED, logical, data_count, exc.code, str(exc),
                    exc.physical_line)
            finally:
                fh.close()
            if file_blocked or header_invalid:
                continue

            if headers is None:
                src_stats["integral"] = False
                self._add_blocking(
                    task_id, spec.source_id, "empty-file",
                    "file contains not even a header record", path=path)
                self._insert_file_row(task_id, spec.source_id, shard_index,
                                      path, digest, size, None, ROW_BLOCKED, 0, 0,
                                      "empty-file", "no header record", None)
                continue

            self._insert_file_row(
                task_id, spec.source_id, shard_index, path, digest, size,
                json.dumps(headers), "ok", logical, data_count, None, None, None)
            src_stats["logical_records"] += logical
            src_stats["data_records"] += data_count

        # unknown counts are aggregated in SQL from the stored rows, never in
        # an in-memory name dictionary
        self._build_unknowns(task_id, spec, declaration.connector)
        return src_stats

    def _build_unknowns(self, task_id: str, spec: SourceSpec,
                        connector: str) -> None:
        # Aggregate local unknown names from stored OK rows.  Rows are read
        # via a streaming cursor; only the distinct-name count is kept in
        # memory, which is bounded by the number of unknown names, not rows.
        counts: Dict[str, int] = {}
        cur = self.conn.execute(
            "SELECT tuple_json FROM ing_rows WHERE task_id=? AND source_id=? "
            "AND outcome=?", (task_id, spec.source_id, ROW_OK))
        while True:
            batch = cur.fetchmany(1000)
            if not batch:
                break
            for r in batch:
                for tok in json.loads(r["tuple_json"]):
                    if tok[0] == "n":
                        name = tok[1] if tok[1] else ""
                        counts[name] = counts.get(name, 0) + 1
        for local_name, occ in counts.items():
            global_id = self._global_unknown_id(
                connector, spec.source_id, local_name)
            self.conn.execute(
                "INSERT INTO ing_unknowns(task_id, source_id, local_name, "
                "global_id, occurrences) VALUES (?,?,?,?,?)",
                (task_id, spec.source_id,
                 None if local_name == "" else local_name, global_id, occ))

    def _insert_file_row(self, task_id, sid, shard_index, path, digest, size,
                         header_json, status, logical, data_count, code, msg,
                         physical_line) -> None:
        self.conn.execute(
            "INSERT INTO ing_files(task_id, source_id, shard_index, path, "
            "sha256, size_bytes, header_json, status, logical_records, "
            "data_records, error_code, error_message, error_physical_line) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (task_id, sid, shard_index, path, digest, size, header_json,
             status, logical, data_count, code, msg, physical_line))

    @staticmethod
    def _duplicate_headers(headers: Sequence[str]) -> List[str]:
        seen: set = set()
        dup: List[str] = []
        for h in headers:
            if h in seen and h not in dup:
                dup.append(h)
            seen.add(h)
        return dup

    @staticmethod
    def _missing_headers(spec: SourceSpec, headers: Sequence[str]) -> List[str]:
        have = set(headers)
        return sorted({f.header for f in spec.fields if f.header not in have})

    def _ingest_record(self, task_id: str, spec: SourceSpec,
                       shard_index: int, rec: CSVRecord,
                       headers: Sequence[str], markers: UnknownMarkers,
                       src_stats: Dict[str, Any], connector: str) -> None:
        expected = len(headers)
        if len(rec.fields) != expected:
            src_stats["integral"] = False
            src_stats["blocked"] += 1
            msg = (f"expected {expected} fields, got {len(rec.fields)}; key "
                   f"positions cannot be trusted")
            self._add_blocking(
                task_id, spec.source_id, "field-count-mismatch", msg,
                shard_index=shard_index, logical_seq=rec.sequence,
                physical_start=rec.physical_start,
                physical_end=rec.physical_end)
            self.conn.execute(
                "INSERT INTO ing_rows(task_id, source_id, shard_index, "
                "logical_seq, physical_start, physical_end, outcome, "
                "errors_json) VALUES (?,?,?,?,?,?,?,?)",
                (task_id, spec.source_id, shard_index, rec.sequence,
                 rec.physical_start, rec.physical_end, ROW_BLOCKED,
                 json.dumps([msg], ensure_ascii=False)))
            return

        outcome, key_tokens, tuple_tokens, errors, _names = convert_row(
            rec.fields, headers, spec, self.ws.schema, markers)
        if outcome == ROW_BLOCKED:
            src_stats["integral"] = False
            src_stats["blocked"] += 1
            for e in errors:
                self._add_blocking(
                    task_id, spec.source_id, "unconvertible-key", e,
                    shard_index=shard_index, logical_seq=rec.sequence,
                    physical_start=rec.physical_start,
                    physical_end=rec.physical_end)
            self.conn.execute(
                "INSERT INTO ing_rows(task_id, source_id, shard_index, "
                "logical_seq, physical_start, physical_end, outcome, "
                "errors_json) VALUES (?,?,?,?,?,?,?,?)",
                (task_id, spec.source_id, shard_index, rec.sequence,
                 rec.physical_start, rec.physical_end, ROW_BLOCKED,
                 json.dumps(errors, ensure_ascii=False)))
            return

        assert key_tokens is not None
        key = encode_record_key(key_tokens)
        if outcome == ROW_QUARANTINE:
            src_stats["quarantine"] += 1
        else:
            src_stats["ok"] += 1
        self.conn.execute(
            "INSERT INTO ing_rows(task_id, source_id, shard_index, "
            "logical_seq, physical_start, physical_end, record_key, outcome, "
            "tuple_json, errors_json) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (task_id, spec.source_id, shard_index, rec.sequence,
             rec.physical_start, rec.physical_end, key, outcome,
             json.dumps(tuple_tokens, ensure_ascii=False)
             if tuple_tokens is not None else None,
             json.dumps(errors, ensure_ascii=False)))

    @staticmethod
    def _global_unknown_id(connector: str, source_id: str,
                           local_name: str) -> str:
        # Same local name within one source_id shares one input unknown;
        # identical names in different source_ids stay isolated.  The empty
        # local name is the anonymous marker.
        raw = f"{connector}::{source_id}::{local_name if local_name else '~anon'}"
        return "u" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]

    # -- reconciliation ------------------------------------------------------ #
    def _reconcile(self, task_id: str, declaration: Declaration,
                   stats: Dict[str, Any]) -> None:
        totals = {"put": 0, "delete": 0, "keep": 0, "quarantine": 0,
                  "rows_ok": 0, "rows_quarantine": 0, "rows_blocked": 0,
                  "duplicate_collapsed": 0}
        for spec in declaration.sources:
            src_stats = stats["sources"][spec.source_id]
            self._resolve_unknowns(task_id, spec)
            decisions = self._reconcile_source(task_id, spec)
            src_stats["candidates"] = decisions
            src_stats["duplicate_collapsed"] = self._count_duplicates(
                task_id, spec.source_id)
            for k, v in decisions.items():
                totals[k] += v
            totals["rows_ok"] += src_stats["ok"]
            totals["rows_quarantine"] += src_stats["quarantine"]
            totals["rows_blocked"] += src_stats["blocked"]
            totals["duplicate_collapsed"] += src_stats["duplicate_collapsed"]
        stats["totals"] = totals

    def _resolve_unknowns(self, task_id: str, spec: SourceSpec) -> None:
        name_map = {
            ("" if r["local_name"] is None else r["local_name"]):
                r["global_id"]
            for r in self.conn.execute(
                "SELECT local_name, global_id FROM ing_unknowns "
                "WHERE task_id=? AND source_id=?",
                (task_id, spec.source_id))}
        # stream rows in bounded batches; only one tuple is held at a time
        cur = self.conn.execute(
            "SELECT rowid, tuple_json FROM ing_rows WHERE task_id=? "
            "AND source_id=? AND outcome=? AND tuple_json IS NOT NULL",
            (task_id, spec.source_id, ROW_OK))
        while True:
            batch = cur.fetchmany(1000)
            if not batch:
                break
            for row in batch:
                tpl = json.loads(row["tuple_json"])
                for tok in tpl:
                    if tok[0] == "n":
                        tok[1] = name_map[tok[1] if tok[1] else ""]
                self.conn.execute(
                    "UPDATE ing_rows SET tuple_json=? WHERE rowid=?",
                    (json.dumps(tpl, ensure_ascii=False), row["rowid"]))

    def _count_duplicates(self, task_id: str, sid: str) -> int:
        row = self.conn.execute(
            "SELECT COALESCE(SUM(c - 1), 0) AS d FROM ("
            "SELECT record_key, COUNT(*) AS c FROM ing_rows "
            "WHERE task_id=? AND source_id=? AND outcome=? GROUP BY record_key)",
            (task_id, sid, ROW_OK)).fetchone()
        return int(row["d"])

    def _reconcile_source(self, task_id: str, spec: SourceSpec
                          ) -> Dict[str, int]:
        """SQL-driven reconciliation with bounded memory.

        One grouped, index-backed statement produces every appeared key with
        its ok/quarantine counts, distinct-tuple count, an evidence JSON array
        (aggregated set-side via ``json_group_array``) and the joined managed
        ledger row.  Python only decides the decision/reason and flushes
        candidates in bounded batches — no per-key SQL round trips and no
        in-memory materialization of the whole key set.
        """
        sid = spec.source_id
        counts = {"put": 0, "delete": 0, "keep": 0, "quarantine": 0}
        if not self._source_integral(task_id, sid):
            self._mark_candidates_unavailable(task_id, spec)
            return counts

        key_cur = self.conn.execute(
            "SELECT r.record_key AS record_key, "
            "SUM(CASE WHEN r.outcome=? THEN 1 ELSE 0 END) AS ok_n, "
            "SUM(CASE WHEN r.outcome=? THEN 1 ELSE 0 END) AS bad_n, "
            "COUNT(DISTINCT CASE WHEN r.outcome=? THEN r.tuple_json END) AS d_tup, "
            "MAX(CASE WHEN r.outcome=? THEN r.tuple_json END) AS any_tuple, "
            "m.tuple_json AS old_tuple, m.relation AS old_relation, "
            "(SELECT json_group_array(json_object("
            "'shard', e.shard_index, 'logical_seq', e.logical_seq, "
            "'physical_start', e.physical_start, "
            "'physical_end', e.physical_end, 'outcome', e.outcome)) "
            "FROM ing_rows e WHERE e.task_id=r.task_id AND e.source_id=r.source_id "
            "AND e.record_key=r.record_key) AS evidence "
            "FROM ing_rows r LEFT JOIN ing_managed_records m "
            "ON m.source_id=r.source_id AND m.record_key=r.record_key "
            "WHERE r.task_id=? AND r.source_id=? "
            "GROUP BY r.record_key ORDER BY r.record_key",
            (ROW_OK, ROW_QUARANTINE, ROW_OK, ROW_OK, task_id, sid))

        cand_batch: List[Any] = []
        block_batch: List[Any] = []

        def flush() -> None:
            if cand_batch:
                self.conn.executemany(
                    "INSERT INTO ing_candidates(task_id, source_id, record_key, "
                    "decision, relation, tuple_json, old_tuple_json, reason, "
                    "evidence_json) VALUES (?,?,?,?,?,?,?,?,?)", cand_batch)
                cand_batch.clear()
            if block_batch:
                self.conn.executemany(
                    "INSERT INTO ing_blocking(task_id, source_id, code, message) "
                    "VALUES (?,?,?,?)", block_batch)
                block_batch.clear()

        while True:
            batch = key_cur.fetchmany(500)
            if not batch:
                break
            for g in batch:
                key = g["record_key"]
                evidence = g["evidence"]
                old_tuple = g["old_tuple"]
                old_relation = g["old_relation"] or spec.relation
                if g["bad_n"] > 0 and g["ok_n"] > 0:
                    block_batch.append((
                        task_id, sid, "key-has-invalid-rows",
                        f"{g['bad_n']} quarantined row(s) share a key with "
                        f"{g['ok_n']} valid row(s)"))
                    cand_batch.append((
                        task_id, sid, key, CAND_QUARANTINE, spec.relation, None,
                        old_tuple,
                        "key appeared but valid and invalid rows coexist; task "
                        "cannot apply", evidence))
                    counts["quarantine"] += 1
                    continue
                if g["d_tup"] > 1:
                    block_batch.append((
                        task_id, sid, "conflicting-duplicate-key",
                        f"{g['d_tup']} distinct legal tuples for one key; no "
                        f"traversal-order winner is allowed"))
                    cand_batch.append((
                        task_id, sid, key, CAND_QUARANTINE, spec.relation, None,
                        old_tuple,
                        "distinct legal tuples for one key; task cannot apply",
                        evidence))
                    counts["quarantine"] += 1
                    continue
                if g["bad_n"] > 0:
                    reason = ("key appeared only in quarantined rows; not "
                              "present before, so nothing is inserted"
                              if old_tuple is None else
                              "key appeared only in quarantined rows; the "
                              "existing managed tuple is retained untouched")
                    cand_batch.append((
                        task_id, sid, key, CAND_QUARANTINE, spec.relation, None,
                        old_tuple, reason, evidence))
                    counts["quarantine"] += 1
                    continue
                new_tuple_blob = g["any_tuple"]
                if old_tuple is None:
                    decision, reason = CAND_PUT, "key absent from managed ledger"
                    counts["put"] += 1
                elif old_tuple == new_tuple_blob:
                    decision, reason = CAND_KEEP, \
                        "snapshot tuple identical to current managed record"
                    counts["keep"] += 1
                else:
                    decision, reason = CAND_PUT, \
                        "snapshot tuple differs from current managed record (replace)"
                    counts["put"] += 1
                cand_batch.append((
                    task_id, sid, key, decision, old_relation, new_tuple_blob,
                    old_tuple, reason, evidence))
            flush()

        # retractions set-side: managed keys with no row in this snapshot
        del_cur = self.conn.execute(
            "SELECT m.record_key, m.tuple_json, m.relation "
            "FROM ing_managed_records m "
            "WHERE m.source_id=? AND NOT EXISTS ("
            "SELECT 1 FROM ing_rows r WHERE r.task_id=? AND r.source_id=? "
            "AND r.record_key=m.record_key) ORDER BY m.record_key",
            (sid, task_id, sid))
        del_batch: List[Any] = []
        while True:
            batch = del_cur.fetchmany(500)
            if not batch:
                break
            for old in batch:
                del_batch.append((
                    task_id, sid, old["record_key"], CAND_DELETE,
                    old["relation"], None, old["tuple_json"],
                    "managed key absent from every shard of this complete "
                    "snapshot", "[]"))
                counts["delete"] += 1
            if len(del_batch) >= 500:
                self.conn.executemany(
                    "INSERT INTO ing_candidates(task_id, source_id, record_key, "
                    "decision, relation, tuple_json, old_tuple_json, reason, "
                    "evidence_json) VALUES (?,?,?,?,?,?,?,?,?)", del_batch)
                del_batch.clear()
        if del_batch:
            self.conn.executemany(
                "INSERT INTO ing_candidates(task_id, source_id, record_key, "
                "decision, relation, tuple_json, old_tuple_json, reason, "
                "evidence_json) VALUES (?,?,?,?,?,?,?,?,?)", del_batch)
        flush()
        return counts

    def _source_integral(self, task_id: str, sid: str) -> bool:
        bad_file = self.conn.execute(
            "SELECT 1 FROM ing_files WHERE task_id=? AND source_id=? "
            "AND status != 'ok' LIMIT 1", (task_id, sid)).fetchone()
        if bad_file is not None:
            return False
        bad_row = self.conn.execute(
            "SELECT 1 FROM ing_rows WHERE task_id=? AND source_id=? "
            "AND outcome=? LIMIT 1",
            (task_id, sid, ROW_BLOCKED)).fetchone()
        return bad_row is None

    def _mark_candidates_unavailable(self, task_id: str,
                                     spec: SourceSpec) -> None:
        # set-side: one quarantine candidate per distinct appeared key, no
        # Python key accumulation and no per-row round trip
        self.conn.execute(
            "INSERT INTO ing_candidates(task_id, source_id, record_key, "
            "decision, relation, tuple_json, old_tuple_json, reason, "
            "evidence_json) "
            "SELECT DISTINCT ?, ?, record_key, ?, ?, NULL, NULL, ?, '[]' "
            "FROM ing_rows WHERE task_id=? AND source_id=? "
            "AND record_key IS NOT NULL",
            (task_id, spec.source_id, CAND_QUARANTINE, spec.relation,
             "snapshot not integral; no apply decision is allowed",
             task_id, spec.source_id))

    def _insert_candidate(self, task_id: str, sid: str, key: str,
                          decision: str, relation: str,
                          new_tuple: Optional[List[Any]],
                          old_row: Optional[sqlite3.Row], reason: str,
                          evidence: List[Dict[str, Any]]) -> None:
        self.conn.execute(
            "INSERT INTO ing_candidates(task_id, source_id, record_key, "
            "decision, relation, tuple_json, old_tuple_json, reason, "
            "evidence_json) VALUES (?,?,?,?,?,?,?,?,?)",
            (task_id, sid, key, decision, relation,
             json.dumps(new_tuple, ensure_ascii=False)
             if new_tuple is not None else None,
             old_row["tuple_json"] if old_row is not None else None,
             reason, json.dumps(evidence, ensure_ascii=False)))

    # -- apply --------------------------------------------------------------- #
    def preapply_checks(self, task_id: str, confirm_empty: bool
                        ) -> Dict[str, Any]:
        task = self._require_task(task_id)
        if task["applied_version"] is not None:
            return {"already_applied": True,
                    "version": task["applied_version"], "problems": []}
        problems: List[str] = []
        declaration = self._task_declaration(task)
        for spec in declaration.sources:
            self._check_claim_and_touch(task, spec, problems)
        if task["status"] == ST_BLOCKED:
            n_blocking = self.conn.execute(
                "SELECT COUNT(*) FROM ing_blocking WHERE task_id=?",
                (task_id,)).fetchone()[0]
            problems.append(
                f"task has {n_blocking} blocking issue(s); inspect "
                f"'ingest report' first")
        if task["needs_empty_confirm"] and not confirm_empty:
            problems.append(
                "explicit empty snapshot requires --confirm-empty to apply")
        latest = self.ws.latest_source_version()
        if latest != task["parent_version"]:
            problems.append(
                f"stale parent: staged against v{task['parent_version']}, "
                f"latest source version is v{latest}; create a new checked "
                f"task — the workspace never re-bases silently")
        return {"already_applied": False, "problems": problems}

    def _check_claim_and_touch(self, task: sqlite3.Row, spec: SourceSpec,
                               problems: List[str]) -> None:
        sid = spec.source_id
        connector = task["connector"]
        claim = self.conn.execute(
            "SELECT connector, relation FROM ing_claims WHERE source_id=?",
            (sid,)).fetchone()
        if claim is None:
            existing = self.conn.execute(
                "SELECT 1 FROM source_records WHERE source_id=? LIMIT 1",
                (sid,)).fetchone()
            if existing is not None:
                problems.append(
                    f"source {sid}: workspace already holds records under this "
                    f"source_id (manual or other feed); the connector may not "
                    f"claim it, overwrite it or clean it up")
            return
        if claim["connector"] != connector:
            problems.append(
                f"source {sid}: claimed by connector {claim['connector']!r}, "
                f"not {connector!r}")
            return
        if claim["relation"] != spec.relation:
            problems.append(
                f"source {sid}: bound to relation {claim['relation']!r}, "
                f"declaration says {spec.relation!r}")
            return
        # external touch: the live source set must still equal the baseline
        # captured when THIS task was checked.  since_version participates, so
        # a same-value replacement is a touch too; integrators re-check via a
        # fresh task rather than having one silently re-based.  Comparison is
        # done set-side in SQL, never by loading both sets into Python.
        baseline_n = self.conn.execute(
            "SELECT COUNT(*) FROM ing_baseline WHERE task_id=? AND source_id=?",
            (task["task_id"], sid)).fetchone()[0]
        current_n = self.conn.execute(
            "SELECT COUNT(*) FROM source_records WHERE source_id=?",
            (sid,)).fetchone()[0]
        if baseline_n != current_n:
            missing = self.conn.execute(
                "SELECT record_key FROM ing_baseline WHERE task_id=? "
                "AND source_id=? AND record_key NOT IN ("
                "SELECT record_id FROM source_records WHERE source_id=?) LIMIT 1",
                (task["task_id"], sid, sid)).fetchone()
            extra = self.conn.execute(
                "SELECT record_id FROM source_records WHERE source_id=? "
                "AND record_id NOT IN (SELECT record_key FROM ing_baseline "
                "WHERE task_id=? AND source_id=?) LIMIT 1",
                (sid, task["task_id"], sid)).fetchone()
            key = (missing or extra)["record_key"] if (missing or extra) else "?"
            problems.append(
                f"source {sid}: externally modified since staging (record set "
                f"changed around key {key!r}); re-stage and re-check before "
                f"applying")
            return
        diff = self.conn.execute(
            "SELECT b.record_key, b.since_version AS bv, c.since_version AS cv "
            "FROM ing_baseline b JOIN source_records c "
            "ON b.record_key = c.record_id AND c.source_id = b.source_id "
            "WHERE b.task_id=? AND b.source_id=? AND ("
            "b.since_version != c.since_version "
            "OR b.tuple_json != c.tuple_json) LIMIT 1",
            (task["task_id"], sid)).fetchone()
        if diff is not None:
            if diff["bv"] != diff["cv"]:
                problems.append(
                    f"source {sid}: externally modified since staging (record "
                    f"{diff['record_key']} was re-written, including a "
                    f"same-value replacement); re-stage and re-check before "
                    f"applying")
            else:
                problems.append(
                    f"source {sid}: externally modified since staging (record "
                    f"{diff['record_key']} tuple changed); re-stage and "
                    f"re-check before applying")
            return

    def apply_task(self, task_id: str, confirm_empty: bool = False
                   ) -> Dict[str, Any]:
        task = self._require_task(task_id)
        if task["applied_version"] is not None:
            receipt = json.loads(task["receipt_json"])
            receipt["idempotent_replay"] = True
            return receipt

        checks = self.preapply_checks(task_id, confirm_empty)
        if checks["problems"]:
            raise IngestError("; ".join(checks["problems"]))

        declaration = self._task_declaration(task)
        ops = self._build_batch_ops(task_id, declaration)
        batch = {"batch_id": "ingest:" + task_id, "ops": ops,
                 "parent": task["parent_version"]}
        parse_batch(batch, self.ws.schema)  # same legal-batch gate as ws commit

        def apply_callback(conn: sqlite3.Connection, version: int) -> None:
            self._persist_apply(conn, task_id, declaration, version)

        receipt = self.ws.commit(batch, parent=task["parent_version"],
                                 external_hook=apply_callback)
        self.conn.execute(
            "UPDATE ing_tasks SET status=?, applied_version=?, "
            "receipt_json=?, applied_at=? WHERE task_id=?",
            (ST_APPLIED, receipt["version"],
             json.dumps(receipt, ensure_ascii=False), _now(), task_id))
        self.conn.commit()
        return receipt

    def _build_batch_ops(self, task_id: str,
                         declaration: Declaration) -> List[Dict[str, Any]]:
        ops: List[Dict[str, Any]] = []
        rows = self.conn.execute(
            "SELECT source_id, record_key, decision, relation, tuple_json "
            "FROM ing_candidates WHERE task_id=? AND decision IN (?,?) "
            "ORDER BY source_id, record_key",
            (task_id, CAND_PUT, CAND_DELETE)).fetchall()
        for r in rows:
            if r["decision"] == CAND_DELETE:
                ops.append({"op": "delete", "source_id": r["source_id"],
                            "record_id": r["record_key"]})
            else:
                tpl = json.loads(r["tuple_json"])
                ops.append({"op": "put", "source_id": r["source_id"],
                            "record_id": r["record_key"],
                            "relation": r["relation"],
                            "tuple": [self._token_to_json(t) for t in tpl]})
        return ops

    @staticmethod
    def _token_to_json(tok: Sequence[Any]) -> Any:
        if tok[0] == "n":
            return {"null": tok[1]}
        return tok[1]

    def _persist_apply(self, conn: sqlite3.Connection, task_id: str,
                       declaration: Declaration, version: int) -> None:
        now = _now()
        for spec in declaration.sources:
            claimed = conn.execute(
                "SELECT 1 FROM ing_claims WHERE source_id=?",
                (spec.source_id,)).fetchone()
            if claimed is None:
                conn.execute(
                    "INSERT INTO ing_claims(source_id, connector, relation, "
                    "claimed_task, claimed_version, created_at) "
                    "VALUES (?,?,?,?,?,?)",
                    (spec.source_id, declaration.connector, spec.relation,
                     task_id, version, now))
            conn.execute("DELETE FROM ing_managed_records WHERE source_id=?",
                         (spec.source_id,))
            # The post-apply managed set is exactly the live workspace source
            # set for this source.  put/keep candidates cover every live row;
            # quarantine candidates whose key already existed carry the
            # retained OLD tuple and must be carried forward too, otherwise
            # the next reconciliation would mistake a retained row for an
            # external change.
            cands = conn.execute(
                "SELECT record_key, relation, tuple_json FROM ing_candidates "
                "WHERE task_id=? AND source_id=? AND decision IN (?,?)",
                (task_id, spec.source_id, CAND_PUT, CAND_KEEP)).fetchall()
            retained = conn.execute(
                "SELECT record_key, relation, old_tuple_json AS tuple_json "
                "FROM ing_candidates WHERE task_id=? AND source_id=? "
                "AND decision=? AND old_tuple_json IS NOT NULL",
                (task_id, spec.source_id, CAND_QUARANTINE)).fetchall()
            for c in list(cands) + list(retained):
                tpl = json.loads(c["tuple_json"])
                fp = json.dumps({"tuple": tpl}, ensure_ascii=False,
                                sort_keys=True)
                # the live workspace row's since_version is the authoritative
                # touch marker (a same-value replacement still bumps it)
                live = conn.execute(
                    "SELECT since_version FROM source_records "
                    "WHERE source_id=? AND record_id=?",
                    (spec.source_id, c["record_key"])).fetchone()
                live_version = int(live["since_version"]) if live else version
                conn.execute(
                    "INSERT INTO ing_managed_records(source_id, record_key, "
                    "relation, tuple_json, fingerprint, since_version, "
                    "last_seen_version, since_task) VALUES (?,?,?,?,?,?,?,?)",
                    (spec.source_id, c["record_key"], c["relation"],
                     c["tuple_json"], fp, live_version, version, task_id))

    # -- queries ------------------------------------------------------------- #
    def _require_task(self, task_id: str) -> sqlite3.Row:
        row = self.conn.execute(
            "SELECT * FROM ing_tasks WHERE task_id=?", (task_id,)).fetchone()
        if row is None:
            raise IngestError(f"no ingestion task {task_id!r}")
        return row

    def _task_declaration(self, task: sqlite3.Row) -> Declaration:
        from .ingest_decl import parse_declaration
        return parse_declaration(json.loads(task["declaration_json"]),
                                 self.ws.schema)

    def task_report(self, task_id: str) -> Dict[str, Any]:
        task = self._require_task(task_id)
        stats = json.loads(task["stats_json"])
        # blocking diagnostics and file rows come from their own tables so a
        # huge report never has to embed every diagnostic in the task row
        blocking = self._blocking_list(task_id)
        files = [dict(r) for r in self.conn.execute(
            "SELECT source_id, shard_index, path, sha256, size_bytes, status, "
            "logical_records, data_records, error_code, error_message, "
            "error_physical_line FROM ing_files WHERE task_id=? "
            "ORDER BY source_id, shard_index", (task_id,))]
        return {
            "task_id": task_id,
            "connector": task["connector"],
            "status": task["status"],
            "applicable": bool(task["applicable"]),
            "needs_empty_confirm": bool(task["needs_empty_confirm"]),
            "parent_version": task["parent_version"],
            "applied_version": task["applied_version"],
            "declaration": {"path": task["declaration_path"],
                            "hash": task["declaration_hash"]},
            "stats": stats,
            "blocking": blocking,
            "files": files,
            "created_at": task["created_at"],
            "applied_at": task["applied_at"],
        }

    def candidate_count(self, task_id: str, source_id: Optional[str] = None,
                        decision: Optional[str] = None) -> int:
        q = "SELECT COUNT(*) FROM ing_candidates WHERE task_id=?"
        params: List[Any] = [task_id]
        if source_id:
            q += " AND source_id=?"
            params.append(source_id)
        if decision:
            q += " AND decision=?"
            params.append(decision)
        return int(self.conn.execute(q, params).fetchone()[0])

    def list_candidates(self, task_id: str, source_id: Optional[str] = None,
                        decision: Optional[str] = None,
                        limit: Optional[int] = None,
                        offset: int = 0) -> Dict[str, Any]:
        where = "WHERE task_id=?"
        params: List[Any] = [task_id]
        if source_id:
            where += " AND source_id=?"
            params.append(source_id)
        if decision:
            where += " AND decision=?"
            params.append(decision)
        total = int(self.conn.execute(
            f"SELECT COUNT(*) FROM ing_candidates {where}", params).fetchone()[0])
        q = ("SELECT source_id, record_key, decision, relation, tuple_json, "
             "old_tuple_json, reason, evidence_json FROM ing_candidates "
             + where + " ORDER BY source_id, record_key")
        if limit is not None:
            q += " LIMIT ? OFFSET ?"
            params.extend([int(limit), int(offset)])
        out = []
        for r in self.conn.execute(q, params):
            d = dict(r)
            d["tuple"] = json.loads(r["tuple_json"]) if r["tuple_json"] else None
            d["old_tuple"] = (json.loads(r["old_tuple_json"])
                              if r["old_tuple_json"] else None)
            d["evidence"] = json.loads(r["evidence_json"])
            del d["tuple_json"], d["old_tuple_json"], d["evidence_json"]
            out.append(d)
        return {"total": total, "returned": len(out), "offset": offset,
                "candidates": out}

    def explain_key(self, task_id: str, source_id: str,
                    record_key: str) -> Dict[str, Any]:
        r = self.conn.execute(
            "SELECT * FROM ing_candidates WHERE task_id=? AND source_id=? "
            "AND record_key=?", (task_id, source_id, record_key)).fetchone()
        if r is None:
            raise IngestError(f"no candidate for {source_id}/{record_key}")
        rows = self.conn.execute(
            "SELECT shard_index, logical_seq, physical_start, physical_end, "
            "outcome, tuple_json, errors_json FROM ing_rows WHERE task_id=? "
            "AND source_id=? AND record_key=? ORDER BY shard_index, logical_seq",
            (task_id, source_id, record_key)).fetchall()
        return {
            "source_id": source_id, "record_key": record_key,
            "decision": r["decision"], "reason": r["reason"],
            "new_tuple": json.loads(r["tuple_json"]) if r["tuple_json"] else None,
            "old_tuple": (json.loads(r["old_tuple_json"])
                          if r["old_tuple_json"] else None),
            "input_rows": [
                {"shard": x["shard_index"], "logical_seq": x["logical_seq"],
                 "physical_start": x["physical_start"],
                 "physical_end": x["physical_end"], "outcome": x["outcome"],
                 "tuple": json.loads(x["tuple_json"])
                          if x["tuple_json"] else None,
                 "errors": json.loads(x["errors_json"])} for x in rows],
        }

    def list_tasks(self) -> List[Dict[str, Any]]:
        return [dict(r) for r in self.conn.execute(
            "SELECT task_id, connector, status, applicable, parent_version, "
            "applied_version, created_at FROM ing_tasks ORDER BY created_at")]

    def managed_ledger(self, source_id: Optional[str] = None,
                       limit: Optional[int] = None, offset: int = 0
                       ) -> Dict[str, Any]:
        where = ""
        params: List[Any] = []
        if source_id:
            where = " WHERE source_id=?"
            params.append(source_id)
        total = int(self.conn.execute(
            f"SELECT COUNT(*) FROM ing_managed_records{where}",
            params).fetchone()[0])
        q = ("SELECT source_id, record_key, relation, tuple_json, since_task, "
             "since_version FROM ing_managed_records" + where
             + " ORDER BY source_id, record_key")
        if limit is not None:
            q += " LIMIT ? OFFSET ?"
            params.extend([int(limit), int(offset)])
        out = []
        for r in self.conn.execute(q, params):
            d = dict(r)
            d["tuple"] = json.loads(r["tuple_json"])
            del d["tuple_json"]
            out.append(d)
        return {"total": total, "returned": len(out), "records": out}

    def list_diagnostics(self, task_id: str, source_id: Optional[str] = None,
                         limit: Optional[int] = None, offset: int = 0
                         ) -> Dict[str, Any]:
        where = "WHERE task_id=? AND outcome != ?"
        params: List[Any] = [task_id, ROW_OK]
        if source_id:
            where += " AND source_id=?"
            params.append(source_id)
        total = int(self.conn.execute(
            f"SELECT COUNT(*) FROM ing_rows {where}", params).fetchone()[0])
        q = ("SELECT source_id, shard_index, logical_seq, physical_start, "
             "physical_end, record_key, outcome, errors_json FROM ing_rows "
             + where + " ORDER BY source_id, shard_index, logical_seq")
        if limit is not None:
            q += " LIMIT ? OFFSET ?"
            params.extend([int(limit), int(offset)])
        out = []
        for r in self.conn.execute(q, params):
            d = dict(r)
            d["errors"] = json.loads(r["errors_json"])
            del d["errors_json"]
            out.append(d)
        return {"total": total, "returned": len(out), "diagnostic_rows": out}

    def unknown_report(self, task_id: str) -> List[Dict[str, Any]]:
        return [dict(r) for r in self.conn.execute(
            "SELECT source_id, local_name, global_id, occurrences "
            "FROM ing_unknowns WHERE task_id=? "
            "ORDER BY source_id, local_name", (task_id,))]
