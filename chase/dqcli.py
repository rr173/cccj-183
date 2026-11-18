"""CLI for the persistent data-quality audit (``chase ws dq ...``).

Commands
--------
    ws dq check   WS --policy P.json
    ws dq run     WS --policy P.json [--sources-version V] [--completed-version V]
                  [--sample-limit N] [-o OUT] [--force] [--no-persist]
    ws dq runs    WS [--policy-id ID]
    ws dq show    WS --run N

Exit codes
----------
0   overall status ``pass``
2   overall status ``fail``
3   overall status ``indeterminate`` (unknowns, no defaulted pass)
5   usage error / policy rejected / invalid version / run failure

An audit never replaces an existing report file without ``--force`` and never
writes anything to the workspace's source, version or completion state.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from .dqaudit import (
    AuditError,
    AuditVersionError,
    DQRuns,
    run_audit,
)
from .dqpolicy import PolicyError, parse_policy
from .model import schema_from_json
from .workspace import Workspace, WorkspaceError

EXIT_OK = 0
EXIT_FAIL = 2
EXIT_INDETERMINATE = 3
EXIT_USAGE = 5


def _print(obj: Any) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2, default=str))


def _load(path: str) -> Any:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def _workspace_schema(workspace: str):
    try:
        with Workspace(workspace) as ws:
            return ws.schema
    except WorkspaceError as exc:
        raise AuditError(str(exc)) from None


def cmd_dq_check(args: argparse.Namespace) -> int:
    try:
        schema = _workspace_schema(args.workspace)
        policy = parse_policy(_load(args.policy), schema)
    except (PolicyError, AuditError, OSError, json.JSONDecodeError) as exc:
        print(f"error: policy rejected: {exc}", file=sys.stderr)
        return EXIT_USAGE
    _print({"ok": True, "policy_id": policy.policy_id,
            "revision": policy.revision, "digest": policy.digest,
            "sample_limit": policy.sample_limit,
            "indeterminate_fails": policy.indeterminate_fails,
            "rules": [r.describe() for r in policy.rules]})
    return EXIT_OK


def cmd_dq_run(args: argparse.Namespace) -> int:
    try:
        policy_obj = _load(args.policy)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"error: cannot read policy file: {exc}", file=sys.stderr)
        return EXIT_USAGE
    try:
        report = run_audit(
            args.workspace, policy_obj,
            sources_version=args.sources_version,
            completed_version=args.completed_version,
            sample_limit=args.sample_limit,
            output=args.output, force=args.force,
            persist=not args.no_persist)
    except PolicyError as exc:
        print(f"error: policy rejected: {exc}", file=sys.stderr)
        return EXIT_USAGE
    except AuditVersionError as exc:
        print(f"error: invalid version: {exc}", file=sys.stderr)
        return EXIT_USAGE
    except (AuditError, WorkspaceError, OSError) as exc:
        print(f"error: audit failed: {exc}", file=sys.stderr)
        return EXIT_USAGE
    except Exception as exc:  # defensive: never publish a pass on a crash
        print(f"error: audit failed: {type(exc).__name__}: {exc}",
              file=sys.stderr)
        return EXIT_USAGE
    summary_keys = ("format", "report_id", "status", "policy_id", "revision",
                    "policy_digest", "versions", "totals", "run_id",
                    "duplicate_of_run_id", "report_sha256", "scan_summary",
                    "phase_seconds", "provenance_sha256", "payload_sha256")
    summary = {k: report[k] for k in summary_keys if k in report}
    summary["policy_id"] = report["policy"]["policy_id"]
    summary["revision"] = report["policy"]["revision"]
    summary["policy_digest"] = report["policy"]["digest"]
    summary["rules"] = [
        {"id": r["id"], "kind": r["kind"], "layer": r["layer"],
         "severity": r["severity"], "status": r["status"],
         "counts": r["counts"], "samples_truncated": r["samples_truncated"],
         "sample_omitted": r["sample_omitted"],
         "elapsed_seconds": r["elapsed_seconds"],
         "scan": r["scan"], "query_plan": r["query_plan"]}
        for r in report["rules"]]
    if args.output:
        summary["output"] = args.output
    _print(summary)
    return {"pass": EXIT_OK, "fail": EXIT_FAIL,
            "indeterminate": EXIT_INDETERMINATE}.get(report["status"],
                                                      EXIT_USAGE)


def cmd_dq_runs(args: argparse.Namespace) -> int:
    try:
        with DQRuns(args.workspace) as runs:
            _print({"format": "data-quality-audit-runs/1",
                    "policies": [
                        {"digest": p["digest"], "policy_id": p["policy_id"],
                         "revision": p["revision"]}
                        for p in runs.policies()],
                    "runs": runs.runs(args.policy_id)})
    except (AuditError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    return EXIT_OK


def cmd_dq_show(args: argparse.Namespace) -> int:
    try:
        with DQRuns(args.workspace) as runs:
            record = runs.get_run(args.run)
    except (AuditError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    _print(record)
    return EXIT_OK


def register(sub: argparse._SubParsersAction) -> None:
    w = sub.add_parser(
        "dq", help="data-quality policy audits over pinned workspace versions")
    wsub = w.add_subparsers(dest="dq_command", required=True)

    p = wsub.add_parser("check", help="validate a policy against the workspace schema")
    p.add_argument("workspace")
    p.add_argument("--policy", required=True)
    p.set_defaults(func=cmd_dq_check)

    p = wsub.add_parser("run", help="audit one pinned version and persist the report")
    p.add_argument("workspace")
    p.add_argument("--policy", required=True)
    p.add_argument("--sources-version", default=None,
                   help="integer source version or 'latest' (default: latest)")
    p.add_argument("--completed-version", default=None,
                   help="integer complete version or 'latest' (default: latest)")
    p.add_argument("--sample-limit", type=int, default=None,
                   help="override the policy's sample_limit")
    p.add_argument("-o", "--output", default=None,
                   help="write the full report here; published atomically and "
                        "never overwriting an existing file without --force")
    p.add_argument("--force", action="store_true")
    p.add_argument("--no-persist", action="store_true",
                   help="do not record the run in dq_audits.sqlite")
    p.set_defaults(func=cmd_dq_run)

    p = wsub.add_parser("runs", help="list stored policies and audit runs")
    p.add_argument("workspace")
    p.add_argument("--policy-id", default=None)
    p.set_defaults(func=cmd_dq_runs)

    p = wsub.add_parser("show", help="show one stored run report")
    p.add_argument("workspace")
    p.add_argument("--run", type=int, required=True)
    p.set_defaults(func=cmd_dq_show)
