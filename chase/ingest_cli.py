"""Snapshot-ingestion CLI: ``python -m chase ingest <command> ...``.

Commands
--------
    ingest stage   WS --declaration D.json --task TASK_ID
    ingest report  WS --task TASK_ID
    ingest changes WS --task TASK_ID [--source SID] [--decision put|delete|...]
    ingest explain WS --task TASK_ID --source SID --record-key KEY
    ingest rows    WS --task TASK_ID [--source SID]        (per-row diagnostics)
    ingest unknowns WS --task TASK_ID
    ingest ledger  WS [--source SID]                       (managed-source ledger)
    ingest tasks   WS
    ingest apply   WS --task TASK_ID [--confirm-empty]

The existing ``ws`` commands are untouched; ingestion is a separate command
group that feeds the same workspace through ordinary source batches.

Exit codes
----------
0  ok / applied (any completion status) / verified
5  usage error, declaration rejected, task not applicable, stale parent,
   external touch, claim refused
7  task id re-used with different content / connector binding conflict
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from .ingest import IngestError, IngestManager
from .ingest_decl import DeclarationError, load_declaration
from .workspace import Workspace, WorkspaceError

EXIT_OK = 0
EXIT_USAGE = 5
EXIT_ID_CONFLICT = 7


def _print(obj: Any) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2, default=str))


def _open_ws(path: str) -> Workspace:
    return Workspace(path)


# --------------------------------------------------------------------------- #
# commands
# --------------------------------------------------------------------------- #
def cmd_stage(args: argparse.Namespace) -> int:
    try:
        with _open_ws(args.workspace) as ws:
            declaration = load_declaration(args.declaration, ws.schema)
            manager = IngestManager(ws)
            report = manager.stage_task(args.task, declaration)
    except (DeclarationError, IngestError, WorkspaceError, OSError,
            json.JSONDecodeError) as exc:
        print(f"error: {type(exc).__name__}: {exc}", file=sys.stderr)
        code = EXIT_ID_CONFLICT if isinstance(exc, IngestError) and (
            "already exists with different" in str(exc)
            or "bound to a different" in str(exc)) else EXIT_USAGE
        return code
    _print(report)
    return EXIT_OK


def cmd_report(args: argparse.Namespace) -> int:
    try:
        with _open_ws(args.workspace) as ws:
            report = IngestManager(ws).task_report(args.task)
    except (IngestError, WorkspaceError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    _print(report)
    return EXIT_OK


def cmd_changes(args: argparse.Namespace) -> int:
    try:
        with _open_ws(args.workspace) as ws:
            manager = IngestManager(ws)
            result = manager.list_candidates(
                args.task, source_id=args.source, decision=args.decision,
                limit=args.limit, offset=args.offset)
            report = manager.task_report(args.task)
    except (IngestError, WorkspaceError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    _print({"task_id": args.task, "status": report["status"],
            "applicable": report["applicable"],
            "stats": report["stats"]["totals"],
            "total_candidates": result["total"],
            "returned": result["returned"], "offset": result["offset"],
            "candidates": result["candidates"]})
    return EXIT_OK


def cmd_explain(args: argparse.Namespace) -> int:
    try:
        with _open_ws(args.workspace) as ws:
            result = IngestManager(ws).explain_key(
                args.task, args.source, args.record_key)
    except (IngestError, WorkspaceError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    _print(result)
    return EXIT_OK


def cmd_rows(args: argparse.Namespace) -> int:
    try:
        with _open_ws(args.workspace) as ws:
            result = IngestManager(ws).list_diagnostics(
                args.task, args.source, limit=args.limit, offset=args.offset)
    except (IngestError, WorkspaceError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    _print({"task_id": args.task, "total": result["total"],
            "returned": result["returned"],
            "diagnostic_rows": result["diagnostic_rows"]})
    return EXIT_OK


def cmd_unknowns(args: argparse.Namespace) -> int:
    try:
        with _open_ws(args.workspace) as ws:
            rows = IngestManager(ws).unknown_report(args.task)
    except (IngestError, WorkspaceError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    _print({"task_id": args.task, "unknowns": rows})
    return EXIT_OK


def cmd_ledger(args: argparse.Namespace) -> int:
    try:
        with _open_ws(args.workspace) as ws:
            result = IngestManager(ws).managed_ledger(
                args.source, limit=args.limit, offset=args.offset)
    except (IngestError, WorkspaceError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    _print({"total": result["total"], "returned": result["returned"],
            "records": result["records"]})
    return EXIT_OK


def cmd_tasks(args: argparse.Namespace) -> int:
    try:
        with _open_ws(args.workspace) as ws:
            rows = IngestManager(ws).list_tasks()
    except (IngestError, WorkspaceError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    _print({"tasks": rows})
    return EXIT_OK


def cmd_apply(args: argparse.Namespace) -> int:
    try:
        with _open_ws(args.workspace) as ws:
            manager = IngestManager(ws)
            receipt = manager.apply_task(args.task,
                                         confirm_empty=args.confirm_empty)
    except IngestError as exc:
        print(json.dumps({"applied": False, "error": str(exc)},
                         ensure_ascii=False, indent=2), file=sys.stderr)
        return EXIT_USAGE
    except WorkspaceError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    _print(receipt)
    # conflict/incomplete completions are still successful applications
    return EXIT_OK


# --------------------------------------------------------------------------- #
# parser wiring
# --------------------------------------------------------------------------- #
def register(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser(
        "ingest", help="CSV snapshot ingestion: stage, reconcile and apply "
                       "supplier full-snapshot feeds")
    psub = p.add_subparsers(dest="ingest_command", required=True)

    s = psub.add_parser("stage", help="parse and reconcile a snapshot task")
    s.add_argument("workspace")
    s.add_argument("--declaration", required=True)
    s.add_argument("--task", required=True, help="durable ingestion task id")
    s.set_defaults(func=cmd_stage)

    r = psub.add_parser("report", help="full stage report with statistics")
    r.add_argument("workspace")
    r.add_argument("--task", required=True)
    r.set_defaults(func=cmd_report)

    c = psub.add_parser("changes", help="list candidate put/delete/keep rows")
    c.add_argument("workspace")
    c.add_argument("--task", required=True)
    c.add_argument("--source", default=None)
    c.add_argument("--decision", default=None,
                   choices=["put", "delete", "keep", "quarantine"])
    c.add_argument("--limit", type=int, default=None,
                   help="max candidates to print (default: all for the task)")
    c.add_argument("--offset", type=int, default=0)
    c.set_defaults(func=cmd_changes)

    e = psub.add_parser("explain", help="explain one key and trace input rows")
    e.add_argument("workspace")
    e.add_argument("--task", required=True)
    e.add_argument("--source", required=True)
    e.add_argument("--record-key", required=True,
                   help="the typed composite key encoding shown by 'changes'")
    e.set_defaults(func=cmd_explain)

    w = psub.add_parser("rows", help="per-row diagnostics with physical lines")
    w.add_argument("workspace")
    w.add_argument("--task", required=True)
    w.add_argument("--source", default=None)
    w.add_argument("--limit", type=int, default=None)
    w.add_argument("--offset", type=int, default=0)
    w.set_defaults(func=cmd_rows)

    u = psub.add_parser("unknowns", help="local-unknown -> global-id mapping")
    u.add_argument("workspace")
    u.add_argument("--task", required=True)
    u.set_defaults(func=cmd_unknowns)

    l = psub.add_parser("ledger", help="managed-source ledger")
    l.add_argument("workspace")
    l.add_argument("--source", default=None)
    l.add_argument("--limit", type=int, default=None)
    l.add_argument("--offset", type=int, default=0)
    l.set_defaults(func=cmd_ledger)

    t = psub.add_parser("tasks", help="list ingestion tasks")
    t.add_argument("workspace")
    t.set_defaults(func=cmd_tasks)

    a = psub.add_parser("apply", help="apply a staged task as one source batch")
    a.add_argument("workspace")
    a.add_argument("--task", required=True)
    a.add_argument("--confirm-empty", action="store_true",
                   help="required when a declared empty snapshot is staged")
    a.set_defaults(func=cmd_apply)
