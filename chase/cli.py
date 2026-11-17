"""Command line interface: check, complete, verify, trace.

    python -m chase check    scenario.json
    python -m chase complete scenario.json -o out/package
    python -m chase verify   out/package
    python -m chase trace    out/package 'Customer("c", *)' [--null n0] [--fact '...']

Exit codes: 0 success/complete/verified, 2 conflict, 3 incomplete/error,
4 verification failure, 5 usage or input error.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .chase import Budgets, Engine
from .cli_support import conflict_to_json, load_scenario, scenario_schema
from .model import (
    LNullValue, ModelError, ProgramError, assert_weakly_acyclic, parse_value,
)
from .package import STATUS_COMPLETE, STATUS_INCOMPLETE, Package, stage_and_publish
from .pkgverify import verify_package_dir
from .provenance import ProvenanceDB
from .store import Store

EXIT_OK = 0
EXIT_CONFLICT = 2
EXIT_INCOMPLETE = 3
EXIT_VERIFY_FAILED = 4
EXIT_USAGE = 5


# --------------------------------------------------------------------------- #
# check
# --------------------------------------------------------------------------- #
def cmd_check(args: argparse.Namespace) -> int:
    try:
        sc = load_scenario(args.scenario)
        schema, rules = scenario_schema(sc)
        assert_weakly_acyclic(rules)
    except ProgramError as exc:
        print(json.dumps({"ok": False, "status": "rejected", "error": str(exc)},
                         ensure_ascii=False, indent=2))
        return EXIT_CONFLICT
    except (ModelError, OSError, json.JSONDecodeError) as exc:
        print(json.dumps({"ok": False, "status": "invalid", "error": str(exc)},
                         ensure_ascii=False, indent=2))
        return EXIT_USAGE
    report = {
        "ok": True,
        "status": "accepted",
        "weakly_acyclic": True,
        "schema": schema.to_json(),
        "rules": [r.to_json() for r in rules],
        "counts": {
            "rules": len(rules),
            "tgds": sum(1 for r in rules if r.kind == "tgd"),
            "egds": sum(1 for r in rules if r.kind == "egd"),
        },
    }
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return EXIT_OK


# --------------------------------------------------------------------------- #
# complete
# --------------------------------------------------------------------------- #
def cmd_complete(args: argparse.Namespace) -> int:
    try:
        sc = load_scenario(args.scenario)
        schema, rules = scenario_schema(sc)
        facts = sc.get("facts", {})
        if not isinstance(facts, dict):
            raise ModelError("'facts' must be an object relation -> list of tuples")
        for rel in facts:
            if rel not in schema.relations:
                raise ModelError(f"input facts mention undeclared relation {rel!r}")
        budget_obj = dict(sc.get("budgets", {}) or {})
        if args.max_seconds is not None:
            budget_obj["max_seconds"] = args.max_seconds
        if args.max_facts is not None:
            budget_obj["max_facts"] = args.max_facts
        if args.max_triggers is not None:
            budget_obj["max_triggers"] = args.max_triggers
        budgets = Budgets(**budget_obj)
        assert_weakly_acyclic(rules)
    except ProgramError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_CONFLICT
    except (ModelError, OSError, json.JSONDecodeError, TypeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE

    out = os.path.abspath(args.output)
    # workdir must exist before the store is created; the package itself is
    # staged in a temp dir and moved, so scratch files never pollute it
    workdir = os.path.abspath(args.workdir) if args.workdir else out
    created_output = not os.path.exists(out)
    if os.path.exists(out) and not args.force:
        pj = os.path.join(out, "package.json")
        if os.path.isfile(pj):
            with open(pj, encoding="utf-8") as fh:
                if json.load(fh).get("status") == STATUS_COMPLETE:
                    print(f"error: {out} already holds a complete package; refusing to "
                          f"overwrite (use --force)", file=sys.stderr)
                    return EXIT_USAGE
        elif os.listdir(out):
            print(f"error: {out} exists and is not empty; refusing to overwrite",
                  file=sys.stderr)
            return EXIT_USAGE

    os.makedirs(workdir, exist_ok=True)
    tmp_store_path = os.path.join(workdir, ".work.sqlite")
    if os.path.exists(tmp_store_path):
        os.remove(tmp_store_path)
    engine: Optional[Engine] = None
    store: Optional[Store] = None
    try:
        store = Store(tmp_store_path)
        engine = Engine(schema, rules, store, budgets, provenance=True,
                        order_seed=args.order_seed)
        input_count = engine.preload_input(_normalize_facts(facts),
                                           source=os.path.basename(args.scenario))
        result = engine.run()
        exported = engine.export()
        stats = dict(result.stats)
        stats["input_facts_loaded"] = input_count
        stats["order_seed"] = args.order_seed
        prov = ProvenanceDB(os.path.join(workdir, ".prov.sqlite"))
        prov.ingest(exported["triggers"], exported["merges"], exported["origins"],
                    exported["facts"])
        prov.write_schema(schema.to_json(), [r.to_json() for r in rules])
        provenance_status = prov.close()
        input_values = _normalize_facts(facts)
        meta = {
            "status_detail": result.reason,
            "detail": result.detail,
            "budgets": {k: getattr(budgets, k) for k in (
                "max_facts", "max_triggers", "max_probes", "max_seconds",
                "max_match_rows", "max_rounds")},
            "input_facts": {rel: [list(_encode_raw(v) for v in r) for r in rows]
                            for rel, rows in input_values.items()},
            "input_source": os.path.basename(args.scenario),
            "stats": stats,
            "provenance": provenance_status,
            "weak_acyclic": True,
            "notes": [
                "labeled nulls are placeholders ('unknown'), never source values",
                "input facts are a set; duplicates were dropped",
                "unification rewrote every stored row; completeness is checked "
                "by an independent verifier, not by this status marker",
            ],
        }
        status: str = result.status
        conflict_json = None
        if status == "conflict":
            assert result.conflict is not None
            conflict_json = conflict_to_json(result.conflict)
            meta["detail"] = result.detail

        def build(tmpdir: str) -> None:
            pkg = Package(tmpdir)
            pkg.write(
                tmpdir, status, schema.to_json(),
                {"rules": [r.to_json() for r in rules],
                 "weak_acyclic": True},
                exported["facts"], exported["nulls"], meta,
                conflict=conflict_json)
            # provenance database travels with the package
            if os.path.exists(prov.path):
                import shutil
                shutil.copy2(prov.path, os.path.join(tmpdir, "provenance.sqlite"))

        verify_summary: Optional[Dict[str, Any]] = None
        if status == "complete" and not args.skip_verify:
            verify_summary = _self_verify(schema, rules, exported["facts"],
                                          input_values, exported["nulls"],
                                          args.verify_cap)
            meta["verification"] = verify_summary
            if not verify_summary["ok"]:
                status = "error"
                meta["status"] = "error"
                meta["detail"] = "self-verification failed before publishing"
        meta["status"] = status
        # Build in a staging dir, write package.json last, then move it into
        # place.  Publishing refuses to replace an existing complete package
        # unless --force; an error/incomplete run therefore cannot clobber one.
        stage_and_publish(out, build, force=args.force or status != "complete")

        summary = {
            "status": status,
            "reason": result.reason,
            "detail": result.detail,
            "stats": stats,
            "output": out,
            "verification": verify_summary,
        }
        if conflict_json is not None:
            summary["conflict"] = conflict_json
        print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
        if status == "complete":
            return EXIT_OK
        if status == "conflict":
            return EXIT_CONFLICT
        return EXIT_INCOMPLETE
    except (ModelError, OSError, json.JSONDecodeError) as exc:
        # malformed facts (wrong type, JSON null, bad arity, ...): usage error,
        # and nothing has been published
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    except Exception as exc:
        # any other failure: report, never leave a half-written status marker
        print(f"error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return EXIT_INCOMPLETE
    finally:
        if engine is not None:
            engine.close()
        if store is not None:
            store.close()
        try:
            if os.path.exists(tmp_store_path):
                os.remove(tmp_store_path)
        except OSError:
            pass
        if args.workdir:
            try:
                if os.path.exists(os.path.join(workdir, ".prov.sqlite")):
                    os.remove(os.path.join(workdir, ".prov.sqlite"))
            except OSError:
                pass
        if created_output and os.path.isdir(out) and not os.listdir(out):
            try:
                os.rmdir(out)
            except OSError:
                pass


def _encode_raw(v: object) -> object:
    """Serialize an input value for package.json (labeled nulls as markers)."""
    if isinstance(v, LNullValue):
        return {"null": v.id}
    return v


def _normalize_facts(facts: Dict[str, Sequence[Sequence[object]]]
                     ) -> Dict[str, List[Tuple[object, ...]]]:
    """Turn JSON source values into model values.

    Strings/ints are source constants; ``{"null": "u1"}`` is a pre-existing
    unknown placeholder shared across all its occurrences.
    """
    out: Dict[str, List[Tuple[object, ...]]] = {}
    for rel, rows in facts.items():
        out[rel] = [tuple(parse_value(v) for v in row) for row in rows]
    return out


def _self_verify(schema, rules, facts, input_facts, nulls, verify_cap) -> Dict[str, Any]:
    from .pkgverify import _verify_payload
    return _verify_payload(schema, rules, facts, input_facts, nulls, verify_cap)


# --------------------------------------------------------------------------- #
# verify
# --------------------------------------------------------------------------- #
def cmd_verify(args: argparse.Namespace) -> int:
    try:
        report = verify_package_dir(args.package, verify_cap=args.verify_cap)
    except (ModelError, OSError, json.JSONDecodeError) as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False, indent=2))
        return EXIT_USAGE
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    if not report.get("ok"):
        return EXIT_VERIFY_FAILED
    if report.get("claimed_status") not in (None, STATUS_COMPLETE):
        return EXIT_INCOMPLETE
    return EXIT_OK


# --------------------------------------------------------------------------- #
# trace
# --------------------------------------------------------------------------- #
def cmd_trace(args: argparse.Namespace) -> int:
    pkg = Package(args.package)
    if not pkg.load():
        print(f"error: no package at {args.package!r}", file=sys.stderr)
        return EXIT_USAGE
    prov_path = pkg.path("provenance.sqlite")
    if not os.path.isfile(prov_path):
        print("error: package has no provenance.sqlite", file=sys.stderr)
        return EXIT_USAGE
    prov = ProvenanceDB(prov_path, readonly=True)
    try:
        if args.null:
            rows = prov.trace_null(args.null)
        elif args.fact:
            rows = prov.trace_fact(args.fact)
        elif args.row:
            rows = prov.trace_pattern(args.row)
        else:
            print("error: give one of --null ID, --fact 'Rel(..)', --row 'Rel(..)'",
                  file=sys.stderr)
            return EXIT_USAGE
    finally:
        prov.close()
    print(json.dumps(rows, ensure_ascii=False, indent=2, default=str))
    return EXIT_OK if rows else EXIT_VERIFY_FAILED


# --------------------------------------------------------------------------- #
# arg parsing
# --------------------------------------------------------------------------- #
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m chase",
        description="Typed relational completion: TGD/EGD chase with labeled nulls")
    sub = p.add_subparsers(dest="command", required=True)

    c = sub.add_parser("check", help="parse and check the program (weak acyclicity)")
    c.add_argument("scenario")
    c.set_defaults(func=cmd_check)

    co = sub.add_parser("complete", help="run the chase and write a package")
    co.add_argument("scenario")
    co.add_argument("-o", "--output", required=True)
    co.add_argument("--workdir", default=None,
                    help="where to keep the scratch SQLite store (default: output dir)")
    co.add_argument("--max-seconds", type=float, default=None)
    co.add_argument("--max-facts", type=int, default=None)
    co.add_argument("--max-triggers", type=int, default=None)
    co.add_argument("--order-seed", type=int, default=0,
                    help="vary trigger order (results must stay homomorphic)")
    co.add_argument("--skip-verify", action="store_true")
    co.add_argument("--verify-cap", type=int, default=None)
    co.add_argument("--force", action="store_true")
    co.set_defaults(func=cmd_complete)

    v = sub.add_parser("verify", help="independently verify a package")
    v.add_argument("package")
    v.add_argument("--verify-cap", type=int, default=None)
    v.set_defaults(func=cmd_verify)

    t = sub.add_parser("trace", help="explain where one row / unknown came from")
    t.add_argument("package")
    t.add_argument("--null", default=None, help="labeled null id")
    t.add_argument("--fact", default=None, help="exact fact, e.g. 'Customer(\"c\", *)'")
    t.add_argument("--row", default=None, help="pattern fact")
    t.set_defaults(func=cmd_trace)

    from . import wscli
    wscli.register(sub)
    return p


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)
