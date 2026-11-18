"""Persistent, version-bound data-quality audits over a correction workspace.

The audit answers the integrator's question *"which rule, which record, which
data version caused this block?"*  It is deliberately separate from the
completion engine, the correction diff and the query engine:

* a **policy** (:mod:`chase.dqpolicy`) is a restricted JSON document with an
  identity, a revision and declarative rules — no SQL, expressions, regular
  expressions or code;
* a **run** pins exactly one source version and/or one completion version per
  layer, evaluates the same policy on demand against historical data, and
  writes a reproducible report;
* every check is evaluated with bounded SQL aggregates over a private scratch
  SQLite database, staged from a **single read-only snapshot** of
  ``workspace.sqlite``.  Violation rows and the report are never accumulated
  in Python lists: only described counts and a bounded, deterministically
  ordered sample are materialized.

Layers
------
``sources``
    Reviews the raw source records retained by a source version.  It can run
    even when that version completed as ``conflict``/``incomplete`` and the
    report states the completion status of the same version.
``completed``
    Reads only the completed facts of a **complete** version (rule-derived
    facts included).

Versions are resolved once, inside one SQLite read transaction that stays
open until the run ends, so a commit landing mid-audit cannot make the same
report mix two versions.  Each layer has its own ``latest`` selector.

Report identity is deterministic: ``report_id`` is derived from the policy
content digest and the resolved version coordinates (never from wall-clock
time or the workspace path), and repeated runs of the same policy content on
the same versions produce the same counts, samples and status.

Persistence lives in ``<workspace>/dq_audits.sqlite`` — a separate database.
An audit never writes a source batch, never moves a version pointer, and
never modifies a completed fact; workspace state is opened ``mode=ro`` with
``query_only``.  One run record is inserted in one transaction after the
whole audit succeeded; a crash therefore shows either no run or the complete
run.
"""

from __future__ import annotations

import hashlib
import json
import os
import resource
import shutil
import sqlite3
import tempfile
import time
import zlib
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

from .dqpolicy import ParsedPolicy, PolicyError, parse_policy
from .model import Schema, schema_from_json
from .query import iter_payload_facts
from .workspace import WS_FORMAT

REPORT_FORMAT = "data-quality-audit-report/1"
RUNS_FORMAT = "data-quality-audit-runs/1"
TOKEN_PREFIX_NULL = '["n",'


class AuditError(Exception):
    """The audit could not be completed (no report, no run record)."""


class AuditVersionError(AuditError):
    """The requested version does not exist / is not usable for its layer."""


# --------------------------------------------------------------------------- #
# Crash / pause hooks (test-only, same style as the workspace hooks)
# --------------------------------------------------------------------------- #
def _crash_point(name: str) -> None:
    if os.environ.get("CHASE_DQ_CRASH_AT") == name:
        os._exit(97)


def _fail_point(name: str) -> None:
    if os.environ.get("CHASE_DQ_FAIL_AT") == name:
        raise AuditError(f"injected audit failure at {name}")


def _pause_point(name: str) -> None:
    if os.environ.get("CHASE_DQ_PAUSE") == name:
        # test-only: keep the pinned read transaction open long enough for a
        # competing writer to commit a new version
        time.sleep(float(os.environ.get("CHASE_DQ_PAUSE_SECONDS", "1.5")))


# --------------------------------------------------------------------------- #
# Persistent run / policy store
# --------------------------------------------------------------------------- #
_RUNS_SQL = """
CREATE TABLE IF NOT EXISTS policy (
    digest TEXT PRIMARY KEY,
    policy_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    canonical_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS run (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_id TEXT NOT NULL,
    policy_digest TEXT NOT NULL,
    policy_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    sources_version INTEGER,
    completed_version INTEGER,
    sources_status TEXT,
    completed_status TEXT,
    overall_status TEXT NOT NULL,
    report_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_run_report ON run(report_id);
CREATE INDEX IF NOT EXISTS ix_run_policy ON run(policy_id);
"""


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


class DQRuns:
    """Policy and run history of one workspace (separate SQLite file)."""

    def __init__(self, workspace_dir: str) -> None:
        self.dir = os.path.abspath(workspace_dir)
        self.path = os.path.join(self.dir, "dq_audits.sqlite")
        self.conn = sqlite3.connect(self.path, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.executescript(_RUNS_SQL)
        self.conn.commit()

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

    def __enter__(self) -> "DQRuns":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def save_policy(self, policy: ParsedPolicy) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO policy(digest, policy_id, revision, "
            "canonical_json, created_at) VALUES (?,?,?,?,?)",
            (policy.digest, policy.policy_id, policy.revision,
             json.dumps(policy.canonical, ensure_ascii=False, sort_keys=True),
             _now()))
        self.conn.commit()

    def policies(self) -> List[Dict[str, Any]]:
        return [dict(r) for r in self.conn.execute(
            "SELECT digest, policy_id, revision, canonical_json, created_at "
            "FROM policy ORDER BY policy_id, revision")]

    def get_policy(self, digest: str) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            "SELECT digest, policy_id, revision, canonical_json, created_at "
            "FROM policy WHERE digest=?", (digest,)).fetchone()
        return dict(row) if row else None

    def record_run(self, report: Dict[str, Any]) -> int:
        """Insert the completed run in ONE transaction (all or nothing)."""
        meta = report["policy"]
        versions = report["versions"]
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            cur = self.conn.execute(
                "INSERT INTO run(report_id, policy_digest, policy_id, revision, "
                "sources_version, completed_version, sources_status, "
                "completed_status, overall_status, report_json, created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (report["report_id"], meta["digest"], meta["policy_id"],
                 meta["revision"],
                 versions["sources"]["version"] if versions["sources"] else None,
                 versions["completed"]["version"] if versions["completed"] else None,
                 versions["sources"]["completion_status"] if versions["sources"] else None,
                 versions["completed"]["completion_status"] if versions["completed"] else None,
                 report["status"],
                 json.dumps(report, ensure_ascii=False, sort_keys=True),
                 _now()))
            run_id = int(cur.lastrowid)
            _crash_point("before_run_commit")
            self.conn.commit()
            _crash_point("after_run_commit")
            return run_id
        except BaseException:
            try:
                self.conn.rollback()
            except sqlite3.Error:
                pass
            raise

    def runs(self, policy_id: Optional[str] = None) -> List[Dict[str, Any]]:
        sql = ("SELECT run_id, report_id, policy_digest, policy_id, revision, "
               "sources_version, completed_version, sources_status, "
               "completed_status, overall_status, created_at FROM run")
        params: List[Any] = []
        if policy_id is not None:
            sql += " WHERE policy_id=?"
            params.append(policy_id)
        sql += " ORDER BY run_id"
        return [dict(r) for r in self.conn.execute(sql, params)]

    def get_run(self, run_id: int) -> Dict[str, Any]:
        row = self.conn.execute(
            "SELECT * FROM run WHERE run_id=?", (run_id,)).fetchone()
        if row is None:
            raise AuditError(f"no audit run {run_id}")
        out = dict(row)
        out["report"] = json.loads(out.pop("report_json"))
        return out

    def runs_for_report(self, report_id: str) -> List[Dict[str, Any]]:
        return [dict(r) for r in self.conn.execute(
            "SELECT run_id, report_id, overall_status, created_at FROM run "
            "WHERE report_id=? ORDER BY run_id", (report_id,))]


# --------------------------------------------------------------------------- #
# Streaming payload helpers
# --------------------------------------------------------------------------- #
def _decompress_blob(conn: sqlite3.Connection, table: str, column: str,
                     rowid: int, out_path: str, write: bool
                     ) -> Optional[str]:
    """Stream one compressed blob to ``out_path`` (or just hash it).

    The blob is read in bounded chunks and inflated incrementally, and each
    inflated piece is consumed immediately, so neither the compressed blob
    nor the decompressed document is ever held in Python memory (a 1 MB
    compressed chunk of very repetitive JSON can inflate to far more than
    that).  Returns the SHA-256 of the *decompressed* content.
    """
    blob = conn.blobopen(table, column, rowid, readonly=True)
    digest = hashlib.sha256()
    dec = zlib.decompressobj()
    fh = open(out_path, "wb") if write else None

    def drain(piece: bytes) -> None:
        if fh is not None:
            fh.write(piece)
        digest.update(piece)

    try:
        while True:
            chunk = blob.read(1 << 16)
            if not chunk:
                break
            data = dec.decompress(chunk, 1 << 16)
            while data:
                drain(data)
                data = dec.decompress(dec.unconsumed_tail, 1 << 16)
        tail = dec.flush()
        if tail:
            drain(tail)
        if not dec.eof:
            raise AuditError(f"truncated compressed {table}.{column} blob")
    finally:
        blob.close()
        if fh is not None:
            fh.close()
    return digest.hexdigest()


def iter_json_section(path: str, key: str, chunk: int = 1 << 20
                      ) -> Iterator[Any]:
    """Yield the elements of the JSON array stored under ``key`` in ``path``.

    The file is read through a rolling buffer and each element is decoded
    with :meth:`json.JSONDecoder.raw_decode`, so a large section (for example
    a fact-to-source-record manifest) is never loaded as one document.  The
    buffer keeps at most one read chunk plus one complete element: every
    consumed prefix is dropped by offset (``buf[buf_pos:]``) in one copy, so
    the cost is linear in the section size rather than quadratic.
    """
    marker = json.dumps(key)
    decoder = json.JSONDecoder()
    with open(path, encoding="utf-8") as fh:
        buf = ""
        pos = 0

        def refill() -> bool:
            nonlocal buf, pos
            piece = fh.read(chunk)
            if not piece:
                return False
            buf = buf[pos:] + piece
            pos = 0
            return True

        def skip_ws() -> bool:
            nonlocal pos
            while True:
                while pos < len(buf) and buf[pos].isspace():
                    pos += 1
                if pos < len(buf):
                    return True
                if not refill():
                    return False

        while True:
            found = buf.find(marker, pos)
            if found >= 0:
                pos = found + len(marker)
                break
            keep = min(len(marker), len(buf) - pos)
            pos = len(buf) - keep
            if not refill():
                raise AuditError(f"payload has no {key!r} section")
        if not skip_ws():
            raise AuditError(f"payload: unexpected end before {key!r}")
        if buf[pos] != ":":
            raise AuditError(f"payload: malformed {key!r} section")
        pos += 1
        if not skip_ws():
            raise AuditError(f"payload: unexpected end at {key!r}")
        if buf[pos] != "[":
            raise AuditError(f"payload: {key!r} is not an array")
        pos += 1
        while True:
            if not skip_ws():
                raise AuditError(f"payload: unterminated {key!r} array")
            ch = buf[pos]
            if ch == ",":
                pos += 1
                continue
            if ch == "]":
                return
            try:
                value, end = decoder.raw_decode(buf, pos)
            except json.JSONDecodeError:
                if not refill():
                    raise AuditError(f"payload: malformed element in {key!r}")
                continue
            pos = end
            yield value


def _token(value: Any) -> List[Any]:
    """Encoded payload value -> typed token ``["s"|"i"|"n", value]``."""
    kind = value.get("kind")
    if kind == "labeled_null":
        return ["n", value["id"]]
    if kind == "string":
        return ["s", value["value"]]
    if kind == "int":
        return ["i", value["value"]]
    raise AuditError(f"unknown encoded value {value!r}")


def _canon_vals_text(vals: str) -> str:
    """Scratch spelling -> the spelling provenance rows are keyed by."""
    return json.dumps(json.loads(vals), ensure_ascii=False)


def _tok_json(tok: Sequence[Any]) -> str:
    return json.dumps(list(tok), ensure_ascii=False, separators=(",", ":"))


def _value_json(tok: Sequence[Any]) -> Dict[str, Any]:
    if tok[0] == "n":
        return {"kind": "labeled_null", "id": tok[1], "display": f"NULL:{tok[1]}"}
    if tok[0] == "s":
        return {"kind": "string", "value": tok[1]}
    return {"kind": "int", "value": int(tok[1])}


def _row_json(toks: Sequence[Sequence[Any]]) -> List[Dict[str, Any]]:
    return [_value_json(t) for t in toks]


# --------------------------------------------------------------------------- #
# The runner
# --------------------------------------------------------------------------- #
class AuditRunner:
    """Runs one policy against one workspace, in one pinned snapshot."""

    def __init__(self, workspace_dir: str) -> None:
        self.workspace = os.path.abspath(workspace_dir)
        self.ws_path = os.path.join(self.workspace, "workspace.sqlite")
        if not os.path.isfile(self.ws_path):
            raise AuditError(f"no workspace at {self.workspace!r}")
        self.ro = sqlite3.connect(f"file:{self.ws_path}?mode=ro", uri=True,
                                  timeout=30)
        self.ro.row_factory = sqlite3.Row
        self.ro.execute("PRAGMA query_only=ON")
        self.ro.execute("PRAGMA busy_timeout=30000")
        meta = {r["key"]: r["value"] for r in
                self.ro.execute("SELECT key, value FROM meta")}
        if meta.get("format") != WS_FORMAT:
            raise AuditError(f"{self.workspace}: not a {WS_FORMAT} workspace")
        self.schema: Schema = schema_from_json(json.loads(meta["schema"]))
        self._max_arity = max(len(c) for c in self.schema.relations.values())
        self.scratch_dir: Optional[str] = None
        self.sc = Optional[sqlite3.Connection]
        self.pv_attached = False
        self.provenance_digest: Dict[int, str] = {}
        self.payload_sha256: Dict[int, str] = {}
        self.source_reference_version: Optional[int] = None
        self.source_target_version: Optional[int] = None
        self.staged_completed_version: Optional[int] = None
        self._scratch: Optional[sqlite3.Connection] = None
        self.phase_seconds: Dict[str, float] = {}

    # -- lifecycle ---------------------------------------------------------- #
    def close(self) -> None:
        if self._scratch is not None:
            try:
                self._scratch.close()
            except Exception:
                pass
            self._scratch = None
        try:
            if self.ro.in_transaction:
                self.ro.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        try:
            self.ro.close()
        except Exception:
            pass
        if self.scratch_dir is not None:
            shutil.rmtree(self.scratch_dir, ignore_errors=True)
            self.scratch_dir = None

    def __enter__(self) -> "AuditRunner":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- pinning ------------------------------------------------------------ #
    def begin_snapshot(self) -> None:
        """Open the read transaction that pins every later read."""
        self.ro.execute("BEGIN")

    def pin_version(self, spec: str, layer: str) -> Dict[str, Any]:
        """Resolve ``spec`` for ``layer`` inside the pinned snapshot."""
        row = self.ro.execute(
            "SELECT value FROM meta WHERE key='latest_source_version'").fetchone()
        latest_src = int(row[0])
        row = self.ro.execute(
            "SELECT value FROM meta WHERE key='latest_complete_version'").fetchone()
        latest_cmp = int(row[0]) if row[0] else None
        spec = str(spec)
        if spec == "latest":
            if layer == "sources":
                if latest_src == 0:
                    raise AuditVersionError(
                        "the workspace has no source version yet")
                version = latest_src
            else:
                if latest_cmp is None:
                    raise AuditVersionError(
                        "the workspace has no complete version yet; a "
                        "conflict/incomplete version is not a completion")
                version = latest_cmp
        else:
            try:
                version = int(spec)
            except ValueError:
                raise AuditVersionError(
                    f"cannot resolve layer 'latest' spec {spec!r}; use an "
                    f"integer version or 'latest'") from None
            if isinstance(version, bool) or version < 1:
                raise AuditVersionError(
                    f"version must be a positive integer, got {spec!r}")
        row = self.ro.execute(
            "SELECT version, batch_id, parent_version, status, created_at "
            "FROM versions WHERE version=?", (version,)).fetchone()
        if row is None:
            raise AuditVersionError(
                f"version {version} does not exist (latest source is "
                f"v{latest_src})")
        status = row["status"]
        if layer == "completed" and status != "complete":
            raise AuditVersionError(
                f"version {version} has completion status {status!r}; the "
                f"completed layer reads only 'complete' versions")
        return {
            "requested_spec": spec,
            "version": int(version),
            "batch_id": row["batch_id"],
            "parent_version": int(row["parent_version"]),
            "completion_status": status,
            "completion_created_at": row["created_at"],
        }

    # -- scratch database ---------------------------------------------------- #
    def open_scratch(self) -> None:
        self.scratch_dir = tempfile.mkdtemp(prefix=".dq-audit-", dir=self.workspace)
        self._scratch = sqlite3.connect(os.path.join(self.scratch_dir,
                                                     "scratch.sqlite"))
        self._scratch.row_factory = sqlite3.Row
        self._scratch.execute("PRAGMA journal_mode=OFF")
        self._scratch.execute("PRAGMA synchronous=OFF")
        self._scratch.execute("PRAGMA cache_size=-65536")
        self._scratch.execute("PRAGMA temp_store=MEMORY")
        self.sc = self._scratch

    def _cols_ddl(self) -> str:
        return ", ".join(f"p{i} TEXT" for i in range(self._max_arity))

    def stage_sources(self, version: int) -> None:
        """Materialize the raw source records of ``version`` into scratch.

        Current sources are streamed in, then each version between ``latest``
        and ``version`` is undone using the per-version change history — the
        same reconstruction the workspace exposes, performed with bounded
        batches inside SQLite rather than in a Python dictionary.
        """
        assert self._scratch is not None
        t0 = time.monotonic()
        sc = self._scratch
        extra = ", has_null INTEGER NOT NULL"
        sc.execute(
            "CREATE TABLE s_src (source_id TEXT NOT NULL, record_id TEXT NOT NULL, "
            f"rel TEXT NOT NULL, vals TEXT NOT NULL, {self._cols_ddl()}, "
            f"nulls_in_row INTEGER NOT NULL, PRIMARY KEY(source_id, record_id))")
        sc.execute("CREATE INDEX ix_src_rel ON s_src(rel)")
        arity_cache: Dict[str, int] = {}

        def row_tokens(rel: str, raw: Sequence[Sequence[Any]]
                       ) -> Tuple[str, List[Any], bool]:
            arity = arity_cache.setdefault(rel, len(self.schema.columns(rel)))
            if len(raw) != arity:
                raise AuditError(f"source record of {rel} has wrong arity")
            p = [_tok_json(t) for t in raw]
            has_null = any(t[0] == "n" for t in raw)
            vals = json.dumps([list(t) for t in raw], ensure_ascii=False,
                              separators=(",", ":"))
            return vals, p, has_null

        cur = self.ro.execute(
            "SELECT source_id, record_id, relation, tuple_json FROM source_records")
        batch: List[tuple] = []
        while True:
            rows = cur.fetchmany(2000)
            if not rows:
                break
            for row in rows:
                rel = row["relation"]
                raw = json.loads(row["tuple_json"])
                vals, p, has_null = row_tokens(rel, raw)
                p = p + [None] * (self._max_arity - len(p))
                batch.append((row["source_id"], row["record_id"], rel, vals,
                              *p, 1 if has_null else 0))
            if batch:
                sc.executemany(
                    "INSERT OR REPLACE INTO s_src VALUES (?,?,?,?,"
                    + ",".join("?" for _ in range(self._max_arity))
                    + ",?)", batch)
                batch = []
        sc.commit()

        latest_row = self.ro.execute(
            "SELECT value FROM meta WHERE key='latest_source_version'").fetchone()
        latest = int(latest_row[0])
        for v in range(latest, version, -1):
            cur = self.ro.execute(
                "SELECT source_id, record_id, prev_relation, prev_tuple_json "
                "FROM record_history WHERE version=?", (v,))
            while True:
                rows = cur.fetchmany(2000)
                if not rows:
                    break
                deletes: List[tuple] = []
                puts: List[tuple] = []
                for row in rows:
                    key = (row["source_id"], row["record_id"])
                    if row["prev_relation"] is None:
                        deletes.append(key)
                        continue
                    rel = row["prev_relation"]
                    raw = json.loads(row["prev_tuple_json"])
                    vals, p, has_null = row_tokens(rel, raw)
                    p = p + [None] * (self._max_arity - len(p))
                    puts.append((key[0], key[1], rel, vals, *p,
                                 1 if has_null else 0))
                if deletes:
                    sc.executemany(
                        "DELETE FROM s_src WHERE source_id=? AND record_id=?",
                        deletes)
                if puts:
                    sc.executemany(
                        "INSERT OR REPLACE INTO s_src VALUES (?,?,?,?,"
                        + ",".join("?" for _ in range(self._max_arity))
                        + ",?)", puts)
        sc.commit()
        _fail_point("during_staging")
        self.phase_seconds["stage_sources_seconds"] = round(
            time.monotonic() - t0, 4)
        self.phase_seconds["stage_sources_peak_rss_mb"] = self._rss_mb()

    def stage_completed(self, version: int) -> None:
        """Stream the completed facts (and fact→record manifest) into scratch.

        The compressed payload is inflated to a temporary file in bounded
        chunks; facts are then parsed by the same streaming reader the query
        engine uses.  Neither the payload document nor the fact set is held
        in a Python list.
        """
        assert self._scratch is not None
        t0 = time.monotonic()
        sc = self._scratch
        sc.execute(
            "CREATE TABLE s_cf (rel TEXT NOT NULL, vals TEXT NOT NULL, "
            f"{self._cols_ddl()}, nulls_in_row INTEGER NOT NULL, "
            "PRIMARY KEY(rel, vals))")
        sc.execute("CREATE INDEX ix_cf_rel ON s_cf(rel)")
        sc.execute(
            "CREATE TABLE s_fs (rel TEXT NOT NULL, vals TEXT NOT NULL, "
            "source_id TEXT NOT NULL, record_id TEXT NOT NULL)")
        sc.execute("CREATE INDEX ix_fs_fact ON s_fs(rel, vals)")

        self.staged_completed_version = version
        payload_path = os.path.join(self.scratch_dir or "", "payload.json")
        digest = _decompress_blob(self.ro, "versions", "payload", version,
                                  payload_path, write=True)
        _fail_point("after_decompress")
        batch: List[tuple] = []
        for rel, encoded in iter_payload_facts(payload_path):
            arity = len(self.schema.columns(rel))
            if len(encoded) != arity:
                raise AuditError(f"completed fact of {rel} has wrong arity")
            toks = [_token(v) for v in encoded]
            p = [_tok_json(t) for t in toks]
            vals = json.dumps(toks, ensure_ascii=False, separators=(",", ":"))
            p = p + [None] * (self._max_arity - len(p))
            batch.append((rel, vals, *p,
                          1 if any(t[0] == "n" for t in toks) else 0))
            if len(batch) >= 2000:
                sc.executemany(
                    "INSERT OR IGNORE INTO s_cf VALUES (?,?,"
                    + ",".join("?" for _ in range(self._max_arity))
                    + ",?)", batch)
                batch = []
        if batch:
            sc.executemany(
                "INSERT OR IGNORE INTO s_cf VALUES (?,?,"
                + ",".join("?" for _ in range(self._max_arity))
                + ",?)", batch)
        sc.commit()

        # fact -> source records, streamed from the payload's manifest
        batch = []
        for entry in iter_json_section(payload_path, "fact_sources"):
            fact = entry.get("fact", {})
            rel = fact.get("relation")
            toks = [_token(v) for v in fact.get("values", [])]
            vals = json.dumps(toks, ensure_ascii=False, separators=(",", ":"))
            for rec in entry.get("records", []):
                batch.append((rel, vals, rec[0], rec[1]))
            if len(batch) >= 4000:
                sc.executemany("INSERT INTO s_fs VALUES (?,?,?,?)", batch)
                batch = []
        if batch:
            sc.executemany("INSERT INTO s_fs VALUES (?,?,?,?)", batch)
        sc.commit()
        os.remove(payload_path)

        # provenance: decompressed once to a temp SQLite file, attached for
        # bounded sample lookups only.
        row = self.ro.execute(
            "SELECT provenance FROM versions WHERE version=?", (version,)).fetchone()
        if row is not None and row["provenance"] is not None:
            prov_path = os.path.join(self.scratch_dir or "", "provenance.sqlite")
            prov_digest = _decompress_blob(self.ro, "versions", "provenance",
                                           version, prov_path, write=True)
            # whichever completion was staged -- the completed layer's
            # version, or a sources-layer reference's same-version target
            self.provenance_digest[version] = prov_digest
            sc.execute("ATTACH DATABASE ? AS pv",
                       (f"file:{prov_path}?mode=ro",))
            self.pv_attached = True
        self.phase_seconds["stage_completed_seconds"] = round(
            time.monotonic() - t0, 4)
        self.payload_sha256[version] = digest
        self.phase_seconds["stage_completed_peak_rss_mb"] = self._rss_mb()

    # -- SQL helpers --------------------------------------------------------- #
    def _plan(self, sql: str, params: Sequence[Any]) -> List[List[Any]]:
        return [[int(r[0]), int(r[1]), str(r[2]), str(r[3])]
                for r in self.sc.execute("EXPLAIN QUERY PLAN " + sql, list(params))]

    @staticmethod
    def _rss_mb() -> float:
        """This process's own peak RSS (VmHWM).

        ``resource.ru_maxrss`` is inherited across fork/exec, so in a child
        launched from a large parent it would report the parent's peak;
        ``/proc/self/status`` VmHWM is this process only.  Falls back to
        ru_maxrss where /proc is unavailable.
        """
        try:
            with open("/proc/self/status", encoding="ascii") as fh:
                for line in fh:
                    if line.startswith("VmHWM:"):
                        return round(int(line.split()[1]) / 1024.0, 1)
        except OSError:
            pass
        return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0, 1)

    def _count(self, sql: str, params: Sequence[Any]) -> int:
        row = self.sc.execute(sql, list(params)).fetchone()
        return int(row[0])

    def _key_indexes(self, table: str, prefix: str, rel: str,
                     cols: Sequence[int]) -> Optional[str]:
        if not cols:
            return None
        name = f"{prefix}_{rel}_{'_'.join(str(c) for c in cols)}"
        name = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in name)
        self.sc.execute(
            f"CREATE INDEX IF NOT EXISTS \"{name}\" ON \"{table}\" "
            f"(rel, {', '.join(f'p{c}' for c in cols)})")
        return name

    def _null_cond(self, cols: Sequence[int]) -> str:
        return " OR ".join(f"instr(p{c}, '{TOKEN_PREFIX_NULL}') = 1"
                           for c in cols)

    # -- per-rule checks ----------------------------------------------------- #
    def _evidence_source(self, row: Any, cols: Sequence[int]) -> Dict[str, Any]:
        toks = [json.loads(row[f"p{i}"]) for i in range(self._max_arity)
                if row[f"p{i}"] is not None]
        return {
            "relation": row["rel"],
            "source_id": row["source_id"],
            "record_id": row["record_id"],
            "values": _row_json(toks),
        }

    def _origin(self, rel: str, vals: str) -> Dict[str, Any]:
        origin: Dict[str, Any] = {"kind": "unknown"}
        if self.pv_attached:
            # provenance stores the canonical json.dumps spelling (", " and
            # ": " separators); compare on that exact text form
            canon = _canon_vals_text(vals)
            row = self.sc.execute(
                "SELECT kind, rule, trigger_key, source FROM pv.fact_origin "
                "WHERE rel=? AND vals=? LIMIT 1", (rel, canon)).fetchone()
            if row is not None:
                origin = {"kind": row["kind"]}
                if row["rule"]:
                    origin["rule"] = row["rule"]
                if row["trigger_key"]:
                    origin["trigger_key"] = row["trigger_key"]
                    trig = self.sc.execute(
                        "SELECT binding, existentials, head_fact, support "
                        "FROM pv.trigger WHERE trigger_key=?",
                        (row["trigger_key"],)).fetchone()
                    if trig is not None:
                        origin["binding"] = json.loads(trig["binding"])
                        origin["head_fact"] = trig["head_fact"]
                        origin["support"] = [s.get("fact") for s in
                                             json.loads(trig["support"])]
                if row["source"]:
                    origin["source"] = row["source"]
        return origin

    def _evidence_completed(self, row: Any, version: int) -> Dict[str, Any]:
        toks = [json.loads(row[f"p{i}"]) for i in range(self._max_arity)
                if row[f"p{i}"] is not None]
        out = {"fact": {"relation": row["rel"], "values": _row_json(toks)}}
        origins: List[Dict[str, Any]] = []
        recs = self.sc.execute(
            "SELECT source_id, record_id FROM s_fs WHERE rel=? AND vals=? "
            "ORDER BY source_id, record_id LIMIT 20", (row["rel"], row["vals"])
        ).fetchall()
        origin = self._origin(row["rel"], row["vals"])
        origin["source_records"] = [{"source_id": r["source_id"],
                                     "record_id": r["record_id"]} for r in recs]
        origin["provenance_ref"] = {
            "version": version,
            "provenance_sha256": self.provenance_digest.get(version),
        }
        origins.append(origin)
        out["provenance"] = origins[0]
        return out

    def _completed_target(self, row: Any, version: int) -> Dict[str, Any]:
        """Evidence for a target fact: source input or rule-derived (with provenance)."""
        assert self.sc is not None
        rel, vals = row["rel"], row["vals"]
        origin = self._origin(rel, vals)
        if origin.get("kind") not in ("input", "input_null", "derived"):
            raise AuditError(
                f"evidence incomplete: the completion's provenance has no "
                f"origin entry for matched target fact {rel}/{vals}; refusing "
                f"to emit a pass with an unverifiable reference")
        recs = self.sc.execute(
            "SELECT source_id, record_id FROM s_fs WHERE rel=? AND vals=? "
            "ORDER BY source_id, record_id LIMIT 20", (rel, vals)).fetchall()
        is_source = origin.get("kind") in ("input", "input_null")
        origin["origin_class"] = "source_input" if is_source else "rule_derived"
        origin["source_records"] = [{"source_id": r["source_id"],
                                     "record_id": r["record_id"]} for r in recs]
        if is_source and not recs:
            raise AuditError(
                f"evidence incomplete: fact {rel}/{vals} is marked as source "
                f"input but the manifest cites no source record")
        origin["provenance_ref"] = {
            "version": version,
            "provenance_sha256": self.provenance_digest.get(version),
        }
        toks = [json.loads(row[f"p{i}"]) for i in range(self._max_arity)
                if row[f"p{i}"] is not None]
        return {"fact": {"relation": rel, "values": _row_json(toks)},
                "provenance": origin}

    def _target_table(self, relation: str) -> str:
        """Where the target relation of a reference is staged.

        ``s_cf`` holds the completion of the version I/O bind; ``s_src`` it is
        the raw records.  A sources-layer rule reads its target from the
        completion of the same source version when that completion was staged.
        """
        assert self.sc is not None
        if self.staged_completed_version is not None:
            return "s_cf"
        return "s_src"

    def _sample_query(self, sql: str, params: Sequence[Any], n: int
                      ) -> List[Any]:
        if n <= 0:
            return []
        return self.sc.execute(sql + " LIMIT ?", list(params) + [n]).fetchall()

    def _rule_counts_and_samples(self, rule, layer: str, version: int,
                                 sample_limit: int) -> Dict[str, Any]:
        assert self.sc is not None
        table = "s_src" if layer == "sources" else "s_cf"
        key_cols = rule.columns
        key_p = [f"p{c}" for c in key_cols]
        null_cond = self._null_cond(key_cols)
        columns_meta = {
            "columns": list(rule.column_names),
            "column_positions": list(rule.columns),
        }
        scan: Dict[str, Any] = {}
        plan_sql: Tuple[str, Sequence[Any]] = (
            f"SELECT COUNT(*) FROM {table} WHERE rel=?", (rule.relation,))
        total = self._count(f"SELECT COUNT(*) FROM {table} WHERE rel=?",
                            (rule.relation,))
        scan["relation_rows"] = total

        if rule.kind == "required":
            col = key_cols[0]
            viol = self._count(
                f"SELECT COUNT(*) FROM {table} WHERE rel=? AND "
                f"instr(p{col}, '{TOKEN_PREFIX_NULL}') = 1", (rule.relation,))
            ind = 0
        elif rule.kind == "enum":
            col = key_cols[0]
            ind = self._count(
                f"SELECT COUNT(*) FROM {table} WHERE rel=? AND "
                f"instr(p{col}, '{TOKEN_PREFIX_NULL}') = 1", (rule.relation,))
            if rule.allowed:
                marks = ", ".join("?" for _ in rule.allowed)
                allowed_p = [_tok_json(t) for t in rule.allowed]
                try:
                    self._key_indexes(table, "ix_dq", rule.relation, (col,))
                except sqlite3.Error:
                    pass
                passed = self._count(
                    f"SELECT COUNT(*) FROM {table} WHERE rel=? AND "
                    f"p{col} IN ({marks})",
                    [rule.relation] + allowed_p)
            else:
                passed = 0
            viol = total - ind - passed
            if rule.allowed:
                marks = ", ".join("?" for _ in rule.allowed)
                plan_sql = (
                    f"SELECT COUNT(*) FROM {table} WHERE rel=? AND "
                    f"p{key_cols[0]} IN ({marks})",
                    [rule.relation] + [_tok_json(t) for t in rule.allowed])
        elif rule.kind == "unique":
            ind = self._count(
                f"SELECT COUNT(*) FROM {table} WHERE rel=? AND ({null_cond})",
                (rule.relation,))
            dup_sql = (
                f"SELECT COALESCE(SUM(c), 0) FROM (SELECT COUNT(*) c FROM "
                f"{table} WHERE rel=? AND NOT ({null_cond}) GROUP BY "
                f"{', '.join(key_p)} HAVING COUNT(*) > 1)")
            viol = self._count(dup_sql, (rule.relation,))
            plan_sql = (dup_sql, (rule.relation,))
        else:  # reference
            ind = self._count(
                f"SELECT COUNT(*) FROM {table} WHERE rel=? AND ({null_cond})",
                (rule.relation,))
            ttable = self._target_table(rule.target_relation)
            self._key_indexes(ttable, "ix_dqt", rule.target_relation,
                              rule.target_columns)
            exists = " AND ".join(
                f"b.p{tc}=a.p{sc}" for sc, tc in
                zip(rule.columns, rule.target_columns))
            viol_sql = (
                f"SELECT COUNT(*) FROM {table} a WHERE a.rel=? AND "
                f"NOT ({null_cond.replace('p', 'a.p')}) AND NOT EXISTS ("
                f"SELECT 1 FROM {ttable} b WHERE b.rel=? AND {exists})")
            viol = self._count(viol_sql, (rule.relation, rule.target_relation))
            plan_sql = (viol_sql, (rule.relation, rule.target_relation))
            scan["index"] = f"(rel, {', '.join('p' + str(c) for c in rule.target_columns)})"

        passed = total - viol - ind
        plan = self._plan(plan_sql[0], plan_sql[1])
        # -- bounded, deterministically ordered samples -------------------- #
        # The budget is split so a rule that has both violations and
        # indeterminate rows always shows at least one of each (up to the
        # limit); when the two categories fit, every row is shown.
        category_total = viol + ind
        if sample_limit <= 0:
            samples: List[Dict[str, Any]] = []
        elif category_total <= sample_limit:
            v_budget, i_budget = viol, ind
            samples = self._fetch_samples(rule, layer, version, null_cond,
                                          v_budget, i_budget)
        else:
            half = max(1, sample_limit // 2)
            v_budget = min(viol, half) if ind else min(viol, sample_limit)
            i_budget = sample_limit - v_budget
            if ind and v_budget >= sample_limit:
                v_budget = sample_limit - 1
                i_budget = 1
            v_budget = min(viol, v_budget)
            i_budget = min(ind, sample_limit - v_budget)
            samples = self._fetch_samples(rule, layer, version, null_cond,
                                          v_budget, i_budget)
        truncated = category_total > len(samples)

        out: Dict[str, Any] = {
            "id": rule.id,
            "kind": rule.kind,
            "layer": layer,
            "severity": rule.severity,
            "relation": rule.relation,
            **columns_meta,
            "counts": {"pass": passed, "violation": viol,
                       "indeterminate": ind, "total": total},
            "sample_omitted": max(0, category_total - len(samples)),
            "samples_truncated": truncated,
            "scan": scan,
            "query_plan": plan,
            "samples": samples,
        }
        if rule.kind == "enum":
            out["allowed"] = [_value_json(t) for t in rule.allowed]
        if rule.kind == "reference":
            out["target"] = {
                "relation": rule.target_relation,
                "columns": list(rule.target_column_names),
                "column_positions": list(rule.target_columns),
            }
            out["matched"] = self._matched_samples(rule, layer, version,
                                                  sample_limit)
        return out

    def _fetch_samples(self, rule, layer: str, version: int,
                       null_cond: str, v_budget: int, i_budget: int
                       ) -> List[Dict[str, Any]]:
        table = "s_src" if layer == "sources" else "s_cf"
        key_p = [f"p{c}" for c in rule.columns]
        order = ("source_id, record_id" if layer == "sources"
                 else "rel, vals")
        samples: List[Dict[str, Any]] = []
        if v_budget > 0:
            if rule.kind == "required":
                cond, params = (f"rel=? AND instr(p{rule.columns[0]}, "
                                f"'{TOKEN_PREFIX_NULL}') = 1", [rule.relation])
                sql = f"SELECT * FROM {table} WHERE {cond} ORDER BY {order}"
            elif rule.kind == "enum":
                marks = ", ".join("?" for _ in rule.allowed)
                cond = (f"rel=? AND instr(p{rule.columns[0]}, "
                        f"'{TOKEN_PREFIX_NULL}') != 1 AND "
                        f"p{rule.columns[0]} NOT IN ({marks})")
                params = ([rule.relation]
                          + [_tok_json(t) for t in rule.allowed])
                sql = f"SELECT * FROM {table} WHERE {cond} ORDER BY {order}"
            elif rule.kind == "unique":
                cols = ", ".join(key_p)
                sql = (
                    f"SELECT s.*, g.c AS group_size FROM {table} s JOIN ("
                    f"SELECT {cols}, COUNT(*) c FROM {table} WHERE rel=? AND "
                    f"NOT ({null_cond}) GROUP BY {cols} HAVING COUNT(*)>1) g "
                    f"ON " + " AND ".join(f"s.{c}=g.{c}" for c in key_p)
                    + f" WHERE s.rel=? ORDER BY " +
                    ", ".join(f"s.{c}" for c in key_p) +
                    ", s.source_id, s.record_id")
                params = [rule.relation, rule.relation]
            else:
                ttable = self._target_table(rule.target_relation)
                self._key_indexes(ttable, "ix_dqt", rule.target_relation,
                                  rule.target_columns)
                exists = " AND ".join(
                    f"b.p{tc}=a.p{sc}" for sc, tc in
                    zip(rule.columns, rule.target_columns))
                sql = (
                    f"SELECT a.* FROM {table} a WHERE a.rel=? AND "
                    f"NOT ({null_cond.replace('p', 'a.p')}) AND NOT EXISTS ("
                    f"SELECT 1 FROM {ttable} b WHERE b.rel=? AND {exists}) "
                    f"ORDER BY a.source_id, a.record_id"
                    if layer == "sources" else
                    f"SELECT a.* FROM {table} a WHERE a.rel=? AND "
                    f"NOT ({null_cond.replace('p', 'a.p')}) AND NOT EXISTS ("
                    f"SELECT 1 FROM {ttable} b WHERE b.rel=? AND {exists}) "
                    f"ORDER BY a.rel, a.vals")
                params = [rule.relation, rule.target_relation]
            for row in self._sample_query(sql, params, v_budget):
                samples.append(self._make_sample(rule, layer, version, row,
                                                 "violation"))
        if i_budget > 0:
            sql = (f"SELECT * FROM {table} WHERE rel=? AND ({null_cond}) "
                   f"ORDER BY {order}")
            for row in self._sample_query(sql, [rule.relation], i_budget):
                samples.append(self._make_sample(rule, layer, version, row,
                                                 "indeterminate"))
        return samples

    def _make_sample(self, rule, layer: str, version: int, row: Any,
                     category: str) -> Dict[str, Any]:
        if layer == "sources":
            ev = self._evidence_source(row, rule.columns)
        else:
            ev = self._evidence_completed(row, version)
        sample = {"check_id": rule.id, "category": category, **ev}
        sample["key"] = [ev["values"][i] if "values" in ev
                         else ev["fact"]["values"][i] for i in rule.columns]
        return sample

    def _matched_samples(self, rule, layer: str, version: int, limit: int
                         ) -> Dict[str, Any]:
        """Satisfied reference keys: which fact matched, and where it came from."""
        table = "s_src" if layer == "sources" else "s_cf"
        ttable = self._target_table(rule.target_relation)
        null_cond = self._null_cond(rule.columns)
        self._key_indexes(ttable, "ix_dqt", rule.target_relation,
                          rule.target_columns)
        exists = " AND ".join(
            f"b.p{tc}=a.p{sc}" for sc, tc in
            zip(rule.columns, rule.target_columns))
        from_clause = (
            f"{table} a JOIN {ttable} b ON b.rel=? AND {exists}")
        where = f"a.rel=? AND NOT ({null_cond.replace('p', 'a.p')})"
        total = self._count(
            f"SELECT COUNT(*) FROM {from_clause} WHERE {where}",
            (rule.target_relation, rule.relation))
        if limit <= 0:
            return {"total": total, "samples": [], "truncated": total > 0}
        if layer == "sources":
            sql = (f"SELECT a.source_id AS a_sid, a.record_id AS a_rid, "
                   f"a.rel AS a_rel, a.vals AS a_vals, b.rel AS b_rel, "
                   f"b.vals AS b_vals, " +
                   ", ".join(f"a.p{c} AS a_p{c}" for c in range(self._max_arity)) +
                   ", " + ", ".join(f"b.p{c} AS b_p{c}"
                                    for c in range(self._max_arity)) +
                   f" FROM {from_clause} WHERE {where} "
                   f"ORDER BY a.source_id, a.record_id, b.rel, b.vals")
        else:
            sql = (f"SELECT a.vals AS a_vals, a.rel AS a_rel, "
                   f"b.vals AS b_vals, b.rel AS b_rel, " +
                   ", ".join(f"a.p{c} AS a_p{c}" for c in range(self._max_arity)) +
                   ", " + ", ".join(f"b.p{c} AS b_p{c}"
                                    for c in range(self._max_arity)) +
                   f" FROM {from_clause} WHERE {where} "
                   f"ORDER BY a.vals, b.vals")
        rows = self.sc.execute(sql + " LIMIT ?",
                               (rule.target_relation, rule.relation, limit)
                               ).fetchall()
        samples = []
        for row in rows:
            if layer == "sources":
                a_toks = [json.loads(row[f"a_p{i}"])
                          for i in range(self._max_arity)
                          if row[f"a_p{i}"] is not None]
                b_toks = [json.loads(row[f"b_p{i}"])
                          for i in range(self._max_arity)
                          if row[f"b_p{i}"] is not None]
                src = {"relation": row["a_rel"],
                       "source_id": row["a_sid"], "record_id": row["a_rid"],
                       "values": _row_json(a_toks)}
                target_rel = row["b_rel"]
                b_row = {"rel": target_rel, "vals": row["b_vals"],
                         **{f"p{i}": row[f"b_p{i}"]
                            for i in range(self._max_arity)}}
                if target_rel != rule.target_relation:
                    raise AuditError(
                        f"internal: matched target relation {target_rel!r} is "
                        f"not a source relation")
                tgt = self._completed_target(b_row, version)
                samples.append({"check_id": rule.id, "category": "matched",
                                "key": [src["values"][i]
                                        for i in rule.columns],
                                "input": src, "matched_target": tgt})
            else:
                b_row = {"rel": row["b_rel"], "vals": row["b_vals"],
                         **{f"p{i}": row[f"b_p{i}"]
                            for i in range(self._max_arity)}}
                a_toks = [json.loads(row[f"a_p{i}"])
                          for i in range(self._max_arity)
                          if row[f"a_p{i}"] is not None]
                src = {"fact": {"relation": row["a_rel"],
                                "values": _row_json(a_toks)}}
                src["provenance"] = self._origin(row["a_rel"], row["a_vals"])
                tgt = self._evidence_completed(b_row, version)
                samples.append({"check_id": rule.id, "category": "matched",
                                "key": [src["fact"]["values"][i]
                                        for i in rule.columns],
                                "input": src, "matched_target": tgt})
        return {"total": total, "samples": samples,
                "truncated": total > len(samples)}


# --------------------------------------------------------------------------- #
# Report assembly
# --------------------------------------------------------------------------- #
def _report_identity(policy: ParsedPolicy, versions: Dict[str, Any],
                     max_samples: int) -> str:
    body = {
        "format": REPORT_FORMAT,
        "policy_digest": policy.digest,
        "versions": {
            k: (v["version"] if v else None) for k, v in versions.items()
        },
        "sample_limit": max_samples,
        "indeterminate_fails": policy.indeterminate_fails,
    }
    text = json.dumps(body, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


_SEMANTICS = (
    "A quality audit answers 'does this data satisfy the named policy in this "
    "pinned version, and which record proves it'. It is NOT the completion "
    "constraint status (which says whether TGDs/EGDs admit a model), NOT a "
    "source-correction diff (which says what changed between versions), and "
    "NOT a query answer (which returns certain tuples). `sources` checks the "
    "raw records retained by the version and runs even when that version "
    "completed as conflict/incomplete; a source-layer reference resolves its "
    "target in the completion of the very same source version and the report "
    "cites whether the target fact is a source input or rule-derived; "
    "`completed` checks the fact set of a complete version. Labeled nulls are "
    "unknown values: `required` reports them as violations, `enum`/`reference` "
    "report them as indeterminate, and no unknown is ever counted as a pass. "
    "A report is identified by the policy digest plus the resolved version "
    "coordinates and sample limit, so re-running the same policy content on "
    "the same versions yields the same report_id, counts and samples.")


def run_audit(workspace: str, policy_obj: object, *,
              sources_version: Optional[str] = None,
              completed_version: Optional[str] = None,
              sample_limit: Optional[int] = None,
              output: Optional[str] = None, force: bool = False,
              persist: bool = True) -> Dict[str, Any]:
    """Validate ``policy_obj``, audit the pinned versions, return the report.

    Any error raises (:class:`PolicyError`, :class:`AuditVersionError`,
    :class:`AuditError`); no run record and no output file is produced.  The
    output file is published with an atomic rename only after the entire
    report exists.
    """
    if sample_limit is not None and (not isinstance(sample_limit, int)
                                     or isinstance(sample_limit, bool)
                                     or sample_limit < 0):
        raise PolicyError("sample_limit must be a non-negative integer")
    if output is not None and os.path.exists(output) and not force:
        raise AuditError(
            f"output file {output!r} already exists; refusing to overwrite a "
            f"previous report (pass --force to replace)")

    runner = AuditRunner(workspace)
    try:
        # resolve + validate the policy before touching any data
        policy = parse_policy(policy_obj, runner.schema)
        eff_limit = policy.sample_limit if sample_limit is None else sample_limit
        # resolve each layer once; a sources-layer reference also resolves its
        # target side, so that completion's facts are staged as well (from the
        # same pinned snapshot, never from an older complete version).
        layers = sorted({r.layer for r in policy.rules})
        runner.begin_snapshot()
        versions: Dict[str, Any] = {"sources": None, "completed": None}
        if "sources" in layers:
            spec = sources_version if sources_version is not None else "latest"
            versions["sources"] = runner.pin_version(spec, "sources")
        if "completed" in layers:
            spec = (completed_version if completed_version is not None
                    else "latest")
            versions["completed"] = runner.pin_version(spec, "completed")
        needs_completion_of_sources = any(
            r.layer == "sources" and r.kind == "reference" for r in policy.rules)
        runner.source_reference_version = None
        if needs_completion_of_sources:
            sv = versions["sources"]
            if sv["completion_status"] != "complete":
                raise AuditVersionError(
                    f"version {sv['version']} has completion status "
                    f"{sv['completion_status']!r}; a source-layer reference "
                    f"rule needs the completion of its target relation from "
                    f"the same source version")
            runner.source_reference_version = sv["version"]
        _pause_point("after_pinned")

        runner.open_scratch()
        t_stage = time.monotonic()
        if versions["sources"] is not None:
            runner.stage_sources(versions["sources"]["version"])
        completed_to_stage = None
        if versions["completed"] is not None:
            completed_to_stage = versions["completed"]["version"]
        elif runner.source_reference_version is not None:
            completed_to_stage = runner.source_reference_version
        if completed_to_stage is not None:
            runner.stage_completed(completed_to_stage)
            if versions["completed"] is None:
                runner.source_target_version = completed_to_stage
        runner.phase_seconds["stage_seconds"] = round(
            time.monotonic() - t_stage, 4)
        _fail_point("after_staging")

        report_id = _report_identity(policy, versions, eff_limit)
        rules_out: List[Dict[str, Any]] = []
        totals = {"pass": 0, "violation": 0, "indeterminate": 0, "total": 0}
        for rule in policy.rules:
            layer = rule.layer
            version = versions[layer]["version"]
            t0 = time.monotonic()
            result = runner._rule_counts_and_samples(rule, layer, version,
                                                     eff_limit)
            result["elapsed_seconds"] = round(time.monotonic() - t0, 4)
            counts = result["counts"]
            for k in totals:
                totals[k] += counts[k]
            if counts["violation"] > 0 or (
                    policy.indeterminate_fails and counts["indeterminate"] > 0):
                result["status"] = "fail"
            elif counts["indeterminate"] > 0:
                result["status"] = "indeterminate"
            else:
                result["status"] = "pass"
            rules_out.append(result)
        if any(r["status"] == "fail" for r in rules_out):
            overall = "fail"
        elif any(r["status"] == "indeterminate" for r in rules_out):
            overall = "indeterminate"
        else:
            overall = "pass"
        _fail_point("before_report")

        report: Dict[str, Any] = {
            "format": REPORT_FORMAT,
            "report_id": report_id,
            "policy": {
                "policy_id": policy.policy_id,
                "revision": policy.revision,
                "digest": policy.digest,
                "indeterminate_fails": policy.indeterminate_fails,
                "sample_limit": eff_limit,
                "description": policy.description,
                "canonical": policy.canonical,
            },
            "versions": {
                layer: (None if v is None else {
                    "requested_spec": v["requested_spec"],
                    "version": v["version"],
                    "batch_id": v["batch_id"],
                    "parent_version": v["parent_version"],
                    "completion_status": v["completion_status"],
                    "completion_created_at": v["completion_created_at"],
                    "layer": layer,
                })
                for layer, v in versions.items()
            },
            "status": overall,
            "totals": totals,
            "rules": rules_out,
            "phase_seconds": dict(runner.phase_seconds),
            "semantics_note": _SEMANTICS,
        }
        if versions["completed"] is not None:
            report["provenance_sha256"] = runner.provenance_digest.get(
                versions["completed"]["version"])
            report["payload_sha256"] = runner.payload_sha256.get(
                versions["completed"]["version"])
        elif runner.source_target_version is not None:
            report["provenance_sha256"] = runner.provenance_digest.get(
                runner.source_target_version)
            report["payload_sha256"] = runner.payload_sha256.get(
                runner.source_target_version)
        report["scan_summary"] = {
            "sources_records": (
                runner._count("SELECT COUNT(*) FROM s_src", [])
                if versions["sources"] is not None else 0),
            "completed_facts": (
                runner._count("SELECT COUNT(*) FROM s_cf", [])
                if versions["completed"] is not None else 0),
            "batch_rows_per_insert": 2000,
            "sample_rows_materialized_limit": eff_limit * len(policy.rules),
            "peak_rss_mb": runner._rss_mb(),  # measured, not part of report identity
        }
        # report identity is content-derived; a second run of the same policy
        # on the same versions must be byte-identical
        canonical_report = json.dumps(report, ensure_ascii=False, sort_keys=True)
        report["report_sha256"] = hashlib.sha256(
            canonical_report.encode("utf-8")).hexdigest()

        if persist:
            with DQRuns(workspace) as runs:
                runs.save_policy(policy)
                run_id = runs.record_run(report)
                report["run_id"] = run_id
                prior = [r for r in runs.runs_for_report(report_id)
                         if r["run_id"] != run_id]
                report["duplicate_of_run_id"] = (
                    prior[0]["run_id"] if prior else None)
        if output is not None:
            _publish_report(output, report)
        return report
    finally:
        runner.close()


def _publish_report(path: str, report: Dict[str, Any]) -> None:
    """Atomically replace ``path`` with the finished report (no partial file)."""
    directory = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".dq-report-", suffix=".json", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(report, fh, ensure_ascii=False, indent=2, sort_keys=True)
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise
