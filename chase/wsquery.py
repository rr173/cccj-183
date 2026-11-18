"""Version-bound query orchestration for the correction workspace.

One query is bound to exactly one completion version:

* the version spec (an integer or the one-shot token ``latest-complete``) is
  resolved and its status is checked **inside a deferred read transaction**,
  and that transaction stays open until the query finishes, so a concurrent
  writer committing a new version during execution can never make one
  multi-relation query read facts from two versions — SQLite's snapshot
  pins the whole statement sequence to the version visible when it began;
* only ``complete`` versions are queryable.  When the newest source version
  is ``conflict``/``incomplete`` it is neither mixed into the data nor
  disguised as the latest successful version: ``latest-complete`` resolves
  to the last complete one and the envelope names both pointers;
* the workspace, its version pointers and batch receipts are never modified
  (the connection executes no write statement against ``workspace.sqlite``);
* all heavy lifting happens in a private scratch SQLite database under the
  system temp directory, streamed in bounded batches.

The returned envelope is deterministic for a fixed (version, query): it
carries no timestamps and results are stably ordered, so repeat runs —
including to a file — produce the same normalized output.
"""

from __future__ import annotations

import json
import os
import shutil
import time
from typing import Any, Dict, List, Optional, TextIO

from .model import schema_from_json
from .query import (
    QueryError,
    QueryExecutor,
    make_scratch_dir,
    parse_query,
    stream_decompress,
)
from .workspace import STATUS_COMPLETE, Workspace, WorkspaceError

# test-only hook: pause while the read transaction is open and the scratch
# store is being built, so concurrency tests can commit a competing version
# mid-query.  Values: "" / "after_version_pinned" / "after_store_loaded".
_HOOK_ENV = "CHASE_WSQUERY_PAUSE"


def _pause(point: str) -> None:
    import time
    if os.environ.get(_HOOK_ENV) == point:  # pragma: no cover - exercised in tests
        time.sleep(1.5)


def _resolve_pinned(ws: Workspace, spec: str) -> Dict[str, Any]:
    """Resolve + validate the target version inside one read transaction."""
    conn = ws.conn
    conn.execute("BEGIN")
    try:
        if spec == "latest-complete":
            version = ws.latest_complete_version()
            if version is None:
                raise QueryError(
                    "latest-complete: the workspace has no complete version yet; "
                    "conflict/incomplete source versions cannot be queried")
        else:
            try:
                version = int(spec)
            except ValueError:
                raise QueryError(
                    f"cannot resolve version spec {spec!r}; use an integer version "
                    f"or 'latest-complete'") from None
            if isinstance(version, bool):
                raise QueryError("version must be an integer or 'latest-complete'")
        info = ws.get_version(version)
        if info["status"] != STATUS_COMPLETE:
            raise QueryError(
                f"version {version} has status {info['status']!r}; only 'complete' "
                f"versions answer relational queries (the version is kept, but it is "
                f"not a usable completion)")
        row = conn.execute(
            "SELECT payload FROM versions WHERE version=?", (version,)).fetchone()
        if row is None:  # defensive
            raise WorkspaceError(f"no payload stored for version {version}")
        payload_blob = row["payload"]
        pointers = {
            "requested_spec": spec,
            "version": version,
            "batch_id": info["batch_id"],
            "status": info["status"],
            "parent_version": info["parent_version"],
            "created_at": info["created_at"],
            "latest_source_version_at_bind": ws.latest_source_version(),
            "latest_complete_version_at_bind": ws.latest_complete_version(),
        }
        # keep the transaction (and its snapshot) open for the whole query;
        # the blob itself is now private to this process
        return {"pointers": pointers, "payload_blob": payload_blob}
    except Exception:
        conn.execute("ROLLBACK")
        raise


def run_query(workspace: str, query_obj: object, version_spec: str,
              *, max_rows: Optional[int] = None,
              max_seconds: Optional[float] = None,
              batch_size: int = 1000,
              output: Optional[str] = None,
              force: bool = False,
              stdout: Optional[TextIO] = None) -> Dict[str, Any]:
    """Run one declarative query against one pinned complete version.

    ``output`` writes the full envelope to a file atomically (a failure or
    interruption never replaces an existing successful output unless
    ``force`` is given).  With no ``output`` the envelope is returned and,
    when ``stdout`` is provided, streamed there.
    """
    if max_rows is not None and max_rows <= 0:
        raise QueryError("max_rows must be a positive integer")
    if max_seconds is not None and max_seconds <= 0:
        raise QueryError("max_seconds must be a positive number")

    ws = Workspace(workspace)
    scratch_dir: Optional[str] = None
    writer: Optional["EnvelopeWriter"] = None
    try:
        pinned = _resolve_pinned(ws, str(version_spec))
        pointers = pinned["pointers"]
        version = pointers["version"]
        try:
            schema = schema_from_json(json.loads(ws._meta["schema"]))
            query = parse_query(query_obj, schema)
        except QueryError:
            ws.conn.execute("ROLLBACK")
            raise

        scratch_dir, payload_path = make_scratch_dir()
        t_decompress = time.monotonic()
        stream_decompress(pinned["payload_blob"], payload_path)
        decompress_seconds = round(time.monotonic() - t_decompress, 4)
        # the compressed blob is the only large payload-derived object held;
        # drop its reference before building the store
        del pinned
        _pause("after_version_pinned")

        executor = QueryExecutor(scratch_dir, batch_size=batch_size)
        try:
            facts_loaded = executor.load_payload_file(payload_path, schema)
            _pause("after_store_loaded")
            plan = [list(r) for r in executor.explain(query)]

            writer = EnvelopeWriter(output, force=force, stdout=stdout) if output \
                else (EnvelopeWriter(None, force=False, stdout=stdout) if stdout
                      else None)
            try:
                if writer is not None:
                    writer.begin(_base_envelope(ws, pointers, query, facts_loaded,
                                                max_rows, max_seconds, plan))
                stats = executor.run(
                    query,
                    sink=(writer.write_row if writer is not None else None),
                    max_rows=max_rows, max_seconds=max_seconds)
            except BaseException:
                if writer is not None:
                    writer.abort()
                raise
        finally:
            executor.close()
        ws.conn.execute("COMMIT")

        envelope = _finalize(pointers, query, facts_loaded, plan, stats,
                             max_rows, max_seconds, decompress_seconds)
        if writer is not None:
            writer.finish(envelope)
        return envelope
    finally:
        if ws.conn.in_transaction:
            ws.conn.execute("ROLLBACK")
        ws.close()
        if scratch_dir is not None:
            shutil.rmtree(scratch_dir, ignore_errors=True)


def _base_envelope(ws: Workspace, pointers: Dict[str, Any], query,
                   facts_loaded: Dict[str, int],
                   max_rows: Optional[int], max_seconds: Optional[float],
                   plan: List[List[Any]]) -> Dict[str, Any]:
    return {
        "format": "workspace-query-result/1",
        "bound_version": {
            "requested_spec": pointers["requested_spec"],
            "version": pointers["version"],
            "batch_id": pointers["batch_id"],
            "completion_status": pointers["status"],
            "parent_version": pointers["parent_version"],
            "latest_source_version_at_bind":
                pointers["latest_source_version_at_bind"],
            "latest_complete_version_at_bind":
                pointers["latest_complete_version_at_bind"],
        },
        "workspace": os.path.abspath(ws.dir),
        "query": query.canonical,
        "query_digest": query.digest,
        "output_columns": query.output_columns(),
        "limits": {"max_rows": max_rows, "max_seconds": max_seconds},
        "result_status": "running",
        "semantics_note": (
            "Rows are certain answers over the facts of the pinned complete "
            "completion version (rule-derived facts included). Labeled nulls "
            "may join on shared identity but never equal a supplied constant "
            "and never appear in output; only distinct tuples whose projected "
            "columns are all known constants are returned. Text '7', int 7 "
            "and an unknown value stay distinct. An empty or truncated result "
            "means 'no fully certain answer under these bounds', not 'no "
            "matching row exists in reality'."),
        "plan": plan,
        "facts_loaded": {k: v for k, v in facts_loaded.items()
                         if k != "__total__"},
        "total_facts_loaded": facts_loaded.get("__total__", 0),
        "results": [],
    }


def _finalize(pointers: Dict[str, Any], query, facts_loaded: Dict[str, int],
              plan: List[List[Any]], stats: Dict[str, Any],
              max_rows: Optional[int], max_seconds: Optional[float],
              decompress_seconds: float
              ) -> Dict[str, Any]:
    status = "truncated" if stats["truncated"] else "complete"
    phase = {"decompress_seconds": decompress_seconds}
    phase.update(stats.get("phase_seconds", {}))
    phase["fetch_seconds"] = stats["elapsed_seconds"]
    envelope = {
        "format": "workspace-query-result/1",
        "bound_version": {
            "requested_spec": pointers["requested_spec"],
            "version": pointers["version"],
            "batch_id": pointers["batch_id"],
            "completion_status": pointers["status"],
            "parent_version": pointers["parent_version"],
            "latest_source_version_at_bind":
                pointers["latest_source_version_at_bind"],
            "latest_complete_version_at_bind":
                pointers["latest_complete_version_at_bind"],
        },
        "query": query.canonical,
        "query_digest": query.digest,
        "output_columns": query.output_columns(),
        "limits": {"max_rows": max_rows, "max_seconds": max_seconds},
        "result_status": status,
        "limit_reason": stats["limit_reason"],
        "semantics_note": (
            "Rows are certain answers over the facts of the pinned complete "
            "completion version (rule-derived facts included). Labeled nulls "
            "may join on shared identity but never equal a supplied constant "
            "and never appear in output; only distinct tuples whose projected "
            "columns are all known constants are returned. Text '7', int 7 "
            "and an unknown value stay distinct. An empty or truncated result "
            "means 'no fully certain answer under these bounds', not 'no "
            "matching row exists in reality'."),
        "plan": plan,
        "facts_loaded": {k: v for k, v in facts_loaded.items()
                         if k != "__total__"},
        "total_facts_loaded": facts_loaded.get("__total__", 0),
        "row_count": stats["row_count"],
        "rows_scanned": stats["rows_scanned"],
        "elapsed_seconds": stats["elapsed_seconds"],
        "phase_seconds": phase,
        "batch_size": stats["batch_size"],
        "scratch_db_bytes": stats["scratch_db_bytes"],
        "results": stats["rows"],
    }
    return envelope


class EnvelopeWriter:
    """Stream the envelope as pretty JSON, or publish it atomically to a file.

    File mode writes a hidden temporary file in the target directory and only
    ``os.replace``s it after the whole query succeeds: an error or Keyboard
    interrupt leaves any previous successful output untouched.
    """

    def __init__(self, path: Optional[str], *, force: bool,
                 stdout: Optional[TextIO]) -> None:
        self.path = path
        self.force = force
        self.stdout = stdout
        self._fh: Optional[TextIO] = None
        self._tmp_path: Optional[str] = None
        self._count = 0

    def begin(self, base: Dict[str, Any]) -> None:
        if self.path is not None:
            if os.path.exists(self.path) and not self.force:
                raise QueryError(
                    f"output file {self.path!r} already exists; refusing to "
                    f"overwrite a previous result (pass force=True / --force to "
                    f"replace)")
            directory = os.path.dirname(os.path.abspath(self.path)) or "."
            os.makedirs(directory, exist_ok=True)
            self._tmp_path = os.path.join(
                directory, f".wsquery-tmp-{os.getpid()}-{id(self)}.json")
            self._fh = open(self._tmp_path, "w", encoding="utf-8")
        else:
            self._fh = self.stdout
        head = {k: base[k] for k in (
            "format", "bound_version", "workspace", "query", "query_digest",
            "output_columns", "limits", "semantics_note",
            "plan", "facts_loaded", "total_facts_loaded")}
        text = json.dumps(head, ensure_ascii=False, indent=2, sort_keys=False)
        assert text.rstrip().endswith("}")
        # drop the head's closing brace; the streamed results array and the
        # tail properties are inserted before one final closing brace
        self._fh.write(text.rstrip()[:-1].rstrip())
        self._fh.write(',\n  "results": [')

    def write_row(self, row: List[Dict[str, Any]]) -> None:
        fh = self._fh
        if self._count:
            fh.write(",")
        fh.write("\n    " + json.dumps(row, ensure_ascii=False))
        self._count += 1

    def finish(self, envelope: Dict[str, Any]) -> None:
        fh = self._fh
        if self._count:
            fh.write("\n  ")
        fh.write("],\n")
        tail = {k: envelope[k] for k in (
            "result_status", "limit_reason", "row_count", "rows_scanned",
            "elapsed_seconds", "phase_seconds", "batch_size",
            "scratch_db_bytes")}
        tail_text = json.dumps(tail, ensure_ascii=False, indent=2)
        # take the tail object's inner property lines at the envelope's
        # two-space indentation level, dropping its wrapping braces
        inner_lines = tail_text.splitlines()[1:-1]
        for line in inner_lines:
            fh.write("  " + line.strip() + "\n")
        fh.write("}\n")
        if self.path is not None:
            fh.close()
            self._fh = None
            if os.path.exists(self.path) and not self.force:
                # defended again at publish time (begin already checked)
                os.remove(self._tmp_path)
                raise QueryError(
                    f"output file {self.path!r} appeared during the run; "
                    f"refusing to overwrite")
            os.replace(self._tmp_path, self.path)
            self._tmp_path = None

    def abort(self) -> None:
        if self._fh is not None and self.path is not None:
            try:
                self._fh.close()
            except OSError:
                pass
        if self._tmp_path is not None:
            try:
                os.remove(self._tmp_path)
            except OSError:
                pass
