"""Workspace CLI: ``python -m chase ws <command> ...``.

Commands
--------
    ws init     WS --program program.json
    ws commit   WS --batch batch.json [--parent N] [--max-seconds S ...]
    ws status   WS
    ws versions WS
    ws sources  WS [--version N]
    ws export   WS --version N|latest|latest-complete -o OUT [--force]
    ws diff     WS --from A --to B [--human]
    ws verify   EXPORT_DIR
    ws query    WS --query Q.json --version N|latest-complete [-o OUT]
                [--max-rows R] [--max-seconds S]

Exit codes (workspace commands)
-------------------------------
0  ok / committed a complete version / verified / complete query result
2  committed a conflict version (the commit itself succeeded)
3  committed an incomplete version, or an internal error occurred
4  verification failed
5  usage error / batch rejected as illegal / query rejected
6  stale parent version (someone else committed first; re-read and decide)
7  batch id re-used with different content
8  query succeeded but its result is truncated (row/time cap reached)
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict

from .model import ModelError, ProgramError
from .query import QueryError
from .wsexport import export_version
from .workspace import (
    BatchIdConflict,
    BatchInvalid,
    StaleParent,
    Workspace,
    WorkspaceError,
)
from .wsdiff import diff_payloads, render_human
from .wsquery import run_query
from .wsverify import verify_export

EXIT_OK = 0
EXIT_CONFLICT = 2
EXIT_INCOMPLETE = 3
EXIT_VERIFY_FAILED = 4
EXIT_USAGE = 5
EXIT_STALE = 6
EXIT_BATCH_ID_CONFLICT = 7
EXIT_TRUNCATED = 8

_STATUS_EXIT = {"complete": EXIT_OK, "conflict": EXIT_CONFLICT,
                "incomplete": EXIT_INCOMPLETE}


def _print(obj: Any) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2, default=str))


def _load_json_file(path: str) -> Any:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


# --------------------------------------------------------------------------- #
# commands
# --------------------------------------------------------------------------- #
def cmd_init(args: argparse.Namespace) -> int:
    try:
        program = _load_json_file(args.program)
        budgets = _load_json_file(args.budgets) if args.budgets else None
        ws = Workspace.init(args.workspace, program, budgets=budgets)
        ws.close()
    except ProgramError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_CONFLICT
    except (ModelError, OSError, json.JSONDecodeError, WorkspaceError,
            TypeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    _print({"ok": True, "workspace": args.workspace,
            "latest_source_version": 0, "latest_complete_version": None})
    return EXIT_OK


def cmd_commit(args: argparse.Namespace) -> int:
    try:
        batch = _load_json_file(args.batch)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"error: cannot read batch file: {exc}", file=sys.stderr)
        return EXIT_USAGE
    overrides: Dict[str, Any] = {}
    for flag, key in ((args.max_seconds, "max_seconds"),
                      (args.max_facts, "max_facts"),
                      (args.max_triggers, "max_triggers")):
        if flag is not None:
            overrides[key] = flag
    try:
        with Workspace(args.workspace) as ws:
            receipt = ws.commit(batch, parent=args.parent,
                                budget_overrides=overrides or None)
    except BatchInvalid as exc:
        _print({"committed": False, "error": "batch-rejected",
                    "reason": exc.reason, "detail": exc.detail})
        return EXIT_USAGE
    except BatchIdConflict as exc:
        _print({"committed": False, "error": "batch-id-conflict",
                    "batch_id": exc.batch_id})
        return EXIT_BATCH_ID_CONFLICT
    except StaleParent as exc:
        _print({"committed": False, "error": "stale-parent",
                    "expected_parent": exc.expected, "latest": exc.latest})
        return EXIT_STALE
    except (WorkspaceError, ModelError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_INCOMPLETE
    except Exception as exc:  # defensive: report, never a half-written state
        print(f"error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return EXIT_INCOMPLETE
    _print(receipt)
    return _STATUS_EXIT.get(receipt["status"], EXIT_INCOMPLETE)


def cmd_status(args: argparse.Namespace) -> int:
    try:
        with Workspace(args.workspace) as ws:
            _print(ws.status())
    except WorkspaceError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    return EXIT_OK


def cmd_versions(args: argparse.Namespace) -> int:
    try:
        with Workspace(args.workspace) as ws:
            _print({"latest_source_version": ws.latest_source_version(),
                    "latest_complete_version": ws.latest_complete_version(),
                    "versions": ws.versions()})
    except WorkspaceError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    return EXIT_OK


def cmd_sources(args: argparse.Namespace) -> int:
    try:
        with Workspace(args.workspace) as ws:
            version = ws.resolve_version(args.version) if args.version else None
            sources = ws.sources_at(version)
            records = [
                {"source_id": sid, "record_id": rid, "relation": rel,
                 "tuple": [_tok_json(t) for t in tpl]}
                for (sid, rid), (rel, tpl) in sorted(sources.items())
            ]
            _print({"version": version if version is not None
                    else ws.latest_source_version(),
                    "records": records, "count": len(records)})
    except WorkspaceError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    return EXIT_OK


def _tok_json(tok: Any) -> Any:
    if tok[0] == "n":
        return {"null": tok[1]}
    return tok[1]


def cmd_export(args: argparse.Namespace) -> int:
    try:
        with Workspace(args.workspace) as ws:
            version = ws.resolve_version(args.version)
            summary = export_version(ws, version, args.output, force=args.force)
    except FileExistsError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    except WorkspaceError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    summary["note"] = (
        "status is exported honestly; a conflict/incomplete export is not a "
        "usable completion package")
    _print(summary)
    return _STATUS_EXIT.get(summary["status"], EXIT_INCOMPLETE)


def cmd_diff(args: argparse.Namespace) -> int:
    try:
        with Workspace(args.workspace) as ws:
            va = ws.resolve_version(str(args.from_version))
            vb = ws.resolve_version(str(args.to_version))
            diff = diff_payloads(ws.version_payload(va), ws.version_payload(vb),
                                 ws.sources_at(va), ws.sources_at(vb), va, vb)
    except WorkspaceError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    if args.human:
        print(render_human(diff))
    else:
        _print(diff)
    return EXIT_OK


def cmd_verify(args: argparse.Namespace) -> int:
    try:
        report = verify_export(args.export, verify_cap=args.verify_cap)
    except (ModelError, OSError, json.JSONDecodeError) as exc:
        _print({"ok": False, "error": str(exc)})
        return EXIT_USAGE
    _print(report)
    return EXIT_OK if report.get("ok") else EXIT_VERIFY_FAILED


def cmd_query(args: argparse.Namespace) -> int:
    """Run a declarative JSON query against one pinned complete version."""
    try:
        query_obj = _load_json_file(args.query)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"error: cannot read query file: {exc}", file=sys.stderr)
        return EXIT_USAGE
    try:
        envelope = run_query(
            args.workspace, query_obj, args.version,
            max_rows=args.max_rows, max_seconds=args.max_seconds,
            batch_size=args.batch_size,
            output=args.output, force=args.force,
            stdout=(None if args.output else sys.stdout))
    except QueryError as exc:
        print(f"error: query rejected: {exc}", file=sys.stderr)
        return EXIT_USAGE
    except WorkspaceError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    if args.output:
        # full envelope went to the file; print a short summary to stdout
        summary = {k: envelope[k] for k in (
            "format", "bound_version", "query_digest", "result_status",
            "limit_reason", "row_count", "rows_scanned", "elapsed_seconds",
            "output", "total_facts_loaded") if k in envelope}
        summary["output"] = args.output
        _print(summary)
    return EXIT_TRUNCATED if envelope["result_status"] == "truncated" else EXIT_OK


# --------------------------------------------------------------------------- #
# parser wiring
# --------------------------------------------------------------------------- #
def register(sub: argparse._SubParsersAction) -> None:
    w = sub.add_parser(
        "ws", help="versioned workspace: source corrections over time")
    wsub = w.add_subparsers(dest="ws_command", required=True)

    p = wsub.add_parser("init", help="create a workspace pinned to a program")
    p.add_argument("workspace")
    p.add_argument("--program", required=True,
                   help="JSON file with 'schema' and 'rules' (optional 'budgets')")
    p.add_argument("--budgets", default=None,
                   help="optional JSON file with default budgets")
    p.set_defaults(func=cmd_init)

    p = wsub.add_parser("commit", help="validate and commit one source batch")
    p.add_argument("workspace")
    p.add_argument("--batch", required=True, help="batch JSON file")
    p.add_argument("--parent", type=int, default=None,
                   help="source version this batch is based on (optimistic "
                        "concurrency; defaults to the current latest)")
    p.add_argument("--max-seconds", type=float, default=None)
    p.add_argument("--max-facts", type=int, default=None)
    p.add_argument("--max-triggers", type=int, default=None)
    p.set_defaults(func=cmd_commit)

    p = wsub.add_parser("status", help="show pointers and counters")
    p.add_argument("workspace")
    p.set_defaults(func=cmd_status)

    p = wsub.add_parser("versions", help="list all source versions")
    p.add_argument("workspace")
    p.set_defaults(func=cmd_versions)

    p = wsub.add_parser("sources", help="dump the raw source records of a version")
    p.add_argument("workspace")
    p.add_argument("--version", default=None,
                   help="version number, 'latest' or 'latest-complete' "
                        "(default: latest)")
    p.set_defaults(func=cmd_sources)

    p = wsub.add_parser("export", help="export one version as a package")
    p.add_argument("workspace")
    p.add_argument("--version", required=True,
                   help="version number, 'latest' or 'latest-complete'")
    p.add_argument("-o", "--output", required=True)
    p.add_argument("--force", action="store_true")
    p.set_defaults(func=cmd_export)

    p = wsub.add_parser("diff", help="compare two versions")
    p.add_argument("workspace")
    p.add_argument("--from", dest="from_version", required=True)
    p.add_argument("--to", dest="to_version", required=True)
    p.add_argument("--human", action="store_true",
                   help="render a human-readable summary instead of JSON")
    p.set_defaults(func=cmd_diff)

    p = wsub.add_parser("verify", help="independently verify an exported version")
    p.add_argument("export")
    p.add_argument("--verify-cap", type=int, default=None)
    p.set_defaults(func=cmd_verify)

    from . import dqcli
    dqcli.register(wsub)

    p = wsub.add_parser(
        "query", help="run a declarative JSON query on one complete version")
    p.add_argument("workspace")
    p.add_argument("--query", required=True,
                   help="declarative query JSON file (relations/select/where)")
    p.add_argument("--version", required=True,
                   help="integer version or 'latest-complete' (resolved once)")
    p.add_argument("-o", "--output", default=None,
                   help="write the full result envelope to this file; an "
                        "existing file is never overwritten without --force, "
                        "and a failed/interrupted run leaves it untouched")
    p.add_argument("--force", action="store_true",
                   help="replace an existing output file")
    p.add_argument("--max-rows", type=int, default=None,
                   help="cap the number of result rows; hitting the cap marks "
                        "the result truncated (exit code 8)")
    p.add_argument("--max-seconds", type=float, default=None,
                   help="wall-clock cap for result fetching; hitting it marks "
                        "the result truncated (exit code 8)")
    p.add_argument("--batch-size", type=int, default=1000,
                   help="rows per SQLite fetch / insert batch (bounded memory)")
    p.set_defaults(func=cmd_query)
