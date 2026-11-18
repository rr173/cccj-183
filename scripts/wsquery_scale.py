#!/usr/bin/env python3
"""Runtime scale check for the version-bound query entry point.

Generates a workspace with **at least 100 000 completed facts** at runtime
(nothing large is written into the repository), runs a selective multi-
relation join query through the ordinary :func:`chase.wsquery.run_query`
entry point, independently recomputes expected answer groups with a plain
Python evaluator over stored facts, and reports the honest resource profile:

* rows actually read from each participating relation (the SQLite plan uses
  indexes — full relations are scanned into the scratch store once, but the
  join itself only touches index-selected rows);
* wall-clock seconds for load, join and total;
* peak RSS of this process.

Run::

    python3 -I scripts/wsquery_scale.py --orders 60000 --customers 12000
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import shutil
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from chase.model import LNullValue  # noqa: E402
from chase.package import decode_value  # noqa: E402
from chase.workspace import Workspace  # noqa: E402
from chase.wsquery import run_query  # noqa: E402

SCHEMA = {
    "Order": ["text", "text"],
    "Customer": ["text", "text"],
    "KnownRegion": ["text", "text"],
    "Item": ["text", "text", "int"],
}
RULES = [
    {"id": "t_order_customer", "body": ["Order(o, c)"], "head": "Customer(c, r)"},
    {"id": "t_known_region", "body": ["KnownRegion(c, r)"], "head": "Customer(c, r)"},
    {"id": "e_customer_region",
     "body": ["Customer(c, r1)", "Customer(c, r2)"], "eq": ["r1", "r2"]},
]


def rss_mb() -> float:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def build(workspace: str, orders: int, customers: int, regions: int,
          unknown_every: int) -> dict:
    shutil.rmtree(workspace, ignore_errors=True)
    ws = Workspace.init(workspace, {"schema": SCHEMA, "rules": RULES})
    ops = []
    # known regions for most customers; a regular sprinkle of input unknowns.
    # One unknown id is deliberately shared by two different customers: the
    # partner is a customer that would otherwise also be unknown, so the EGD
    # only ever sees unknown=unknown (never two distinct known constants).
    unknown_customers = {i for i in range(customers)
                         if i > 0 and i % unknown_every == 0}
    for i in range(customers):
        cid = f"c{i}"
        if i in unknown_customers:
            shared = ("u-shared"
                      if i % (unknown_every * 2) == 0 else f"u{i}")
            ops.append({"op": "put", "source_id": "crm",
                        "record_id": f"kr{i}", "relation": "KnownRegion",
                        "tuple": [cid, {"null": shared}]})
            partner = i + 1
            if (i % (unknown_every * 2) == 0
                    and partner < customers
                    and partner not in unknown_customers):
                # the partner's only region record carries the SAME unknown
                unknown_customers.add(partner)
                ops.append({"op": "put", "source_id": "crm",
                            "record_id": f"kr{partner}u", "relation": "KnownRegion",
                            "tuple": [f"c{partner}", {"null": shared}]})
        else:
            ops.append({"op": "put", "source_id": "crm",
                        "record_id": f"kr{i}", "relation": "KnownRegion",
                        "tuple": [cid, f"region{i % regions}"]})
    for j in range(orders):
        ops.append({"op": "put", "source_id": "shop",
                    "record_id": f"o{j}", "relation": "Order",
                    "tuple": [f"o{j}", f"c{j % customers}"]})
    # one Item per order -> the multi-relation join has real selectivity work
    for j in range(orders):
        ops.append({"op": "put", "source_id": "wms",
                    "record_id": f"it{j}", "relation": "Item",
                    "tuple": [f"o{j}", f"sku{j % 97}", (j % 500) + 1]})
    t0 = time.perf_counter()
    receipt = ws.commit({"batch_id": "base", "ops": ops})
    commit_seconds = time.perf_counter() - t0
    if receipt["status"] != "complete":
        raise SystemExit(
            f"scale fixture must complete, got {receipt['status']}: "
            f"{receipt.get('reason')} {receipt.get('detail')}")
    facts_total = receipt["stats"]["facts_total"]
    ws.close()
    return {"commit_seconds": round(commit_seconds, 3),
            "facts_total": facts_total,
            "source_records": receipt["stats"]["source_records"]}


# A selective 3-relation join: orders of customers in one small region (about
# 1/regions of the customers), joined to their item rows.  Two joins plus one
# typed constant filter; projection stays on known columns only.
SELECTIVE_QUERY = {
    "relations": [{"name": "Order", "alias": "o"},
                  {"name": "Customer", "alias": "c"},
                  {"name": "Item", "alias": "it"}],
    "select": [{"column": "o.c0", "name": "order_id"},
               {"column": "c.c1", "name": "region"},
               {"column": "it.c2", "name": "qty"}],
    "where": [{"eq": ["o.c1", "c.c0"]},
              {"eq": ["o.c0", "it.c0"]},
              {"eq": ["c.c1", {"type": "text", "value": "region7"}]}],
}


def independent_expected(workspace: str, region: str) -> dict:
    """Plain-Python re-derivation of the certain answer from stored facts.

    Reads each participating relation fully (as the executor does for its
    scratch load) but performs the nested-loop join itself, applying the same
    certainty rule: unknowns fail constant conditions and never project.
    """
    import zlib
    ws = Workspace(workspace)
    try:
        version = ws.latest_complete_version()
        row = ws.conn.execute(
            "SELECT payload FROM versions WHERE version=?", (version,)).fetchone()
        payload = json.loads(zlib.decompress(row["payload"]))

        def tokens(rel):
            out = []
            for encoded_row in payload["facts"][rel]:
                vals = []
                for cell in encoded_row:
                    v = decode_value(cell)
                    if isinstance(v, LNullValue):
                        vals.append(("n", v.id))
                    elif isinstance(v, str):
                        vals.append(("s", v))
                    else:
                        vals.append(("i", v))
                out.append(tuple(vals))
            return out

        orders, customers, items = (tokens("Order"), tokens("Customer"),
                                    tokens("Item"))
        cust_by_id = {r[0]: r[1] for r in customers}
        item_by_oid = {r[0]: r for r in items}
        answer = set()
        matched_orders = 0
        for oid, cid in orders:
            region_tok = cust_by_id.get(cid)
            item = item_by_oid.get(oid)
            if region_tok is None or item is None:
                continue
            if region_tok == ("s", region):
                matched_orders += 1
                answer.add((oid, ("s", region), item[2]))
        return {
            "version": version,
            "row_count": len(answer),
            "sample": sorted(answer)[:5],
            "matched_orders": matched_orders,
            "relation_sizes": {"Order": len(orders), "Customer": len(customers),
                               "Item": len(items)},
        }
    finally:
        ws.close()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -I scripts/wsquery_scale.py")
    ap.add_argument("--workspace", default=os.path.join(
        tempfile.gettempdir(), "chase-wsquery-scale"))
    ap.add_argument("--orders", type=int, default=60000)
    ap.add_argument("--customers", type=int, default=12000)
    ap.add_argument("--regions", type=int, default=64)
    ap.add_argument("--unknown-every", type=int, default=997)
    ap.add_argument("--max-rows", type=int, default=None)
    ap.add_argument("--reuse", action="store_true",
                    help="skip the build and query an existing workspace")
    args = ap.parse_args(argv)

    report = {"workspace": args.workspace, "orders": args.orders,
              "customers": args.customers, "regions": args.regions}
    t_start = time.perf_counter()
    if args.reuse:
        ws = Workspace(args.workspace)
        v = ws.latest_complete_version()
        info = ws.get_version(v)
        build_info = {"commit_seconds": 0.0,
                      "facts_total": info["stats"]["facts_total"],
                      "source_records": info["stats"]["source_records"]}
        ws.close()
    else:
        build_info = build(args.workspace, args.orders, args.customers,
                           args.regions, args.unknown_every)
    report["build"] = build_info
    report["meets_100k_fact_target"] = build_info["facts_total"] >= 100_000

    # The chase commit above (full in-process engine) sets a high-water mark;
    # record the RSS now so the query's *own* additional memory is separable
    # from the build's.  A CLI subprocess measurement (reported separately)
    # gives the query-only absolute peak.
    rss_after_build = rss_mb()
    t0 = time.perf_counter()
    envelope = run_query(args.workspace, SELECTIVE_QUERY, "latest-complete",
                         max_rows=args.max_rows)
    query_seconds = time.perf_counter() - t0
    rss_after_query = rss_mb()
    expected = independent_expected(args.workspace, "region7")

    # Query-only process: run the same query through the actual CLI entry
    # point in a fresh process, so its absolute peak RSS excludes the chase
    # build entirely and reflects only the bounded streaming query path.
    import subprocess
    q_path = os.path.join(tempfile.gettempdir(), "wsquery-scale-query.json")
    with open(q_path, "w", encoding="utf-8") as fh:
        json.dump(SELECTIVE_QUERY, fh, ensure_ascii=False)
    # measure THIS child's own peak via a small resource-collecting parent;
    # RUSAGE_CHILDREN in this process would be a cumulative high-water mark
    # polluted by the earlier chase build subprocesses
    watchdog = (
        "import json,resource,subprocess,sys,time;"
        "t=time.perf_counter();"
        "p=subprocess.run(sys.argv[1:],capture_output=True,text=True);"
        "out=json.loads(p.stdout);"
        "print(json.dumps({"
        "'rc':p.returncode,'elapsed_seconds':round(time.perf_counter()-t,3),"
        "'peak_rss_mb':round(resource.getrusage(resource.RUSAGE_CHILDREN)"
        ".ru_maxrss/1024.0,1),'row_count':out['row_count'],"
        "'bound_version':out['bound_version']['version'],"
        "'stderr':p.stderr[:200]}))")
    cli_start = time.perf_counter()
    proc = subprocess.run(
        [sys.executable, "-I", "-c", watchdog, sys.executable, "-I",
         os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                      "run_cli.py"),
         "ws", "query", args.workspace, "--query", q_path,
         "--version", "latest-complete"],
        capture_output=True, text=True, timeout=300)
    cli_seconds = round(time.perf_counter() - cli_start, 3)
    if proc.returncode != 0:
        raise SystemExit(f"query-only CLI run failed: {proc.stderr}")
    cli_measured = json.loads(proc.stdout)
    if cli_measured["rc"] != 0:
        raise SystemExit(f"query watchdog child failed: {cli_measured}")
    cli_env = {"row_count": cli_measured["row_count"],
               "bound_version": {"version": cli_measured["bound_version"]}}
    child_peak_mb = cli_measured["peak_rss_mb"]
    assert cli_env["row_count"] == expected["row_count"]
    os.remove(q_path)

    report["query"] = {
        "bound_version": envelope["bound_version"]["version"],
        "result_status": envelope["result_status"],
        "row_count": envelope["row_count"],
        "rows_scanned": envelope["rows_scanned"],
        "elapsed_seconds_reported": envelope["elapsed_seconds"],
        "elapsed_seconds_wall": round(query_seconds, 4),
        "facts_loaded": envelope["facts_loaded"],
        "total_facts_loaded": envelope["total_facts_loaded"],
        "scratch_db_bytes": envelope["scratch_db_bytes"],
        "batch_size": envelope["batch_size"],
        "phase_seconds": envelope["phase_seconds"],
        "plan": envelope["plan"],
        "peak_rss_mb_process_high_water": round(rss_after_query, 1),
        "rss_mb_after_build": round(rss_after_build, 1),
        "rss_mb_added_by_query_in_process":
            round(rss_after_query - rss_after_build, 1),
        "query_only_subprocess": {
            "peak_rss_mb": child_peak_mb,
            "elapsed_seconds": cli_seconds,
            "row_count": cli_env["row_count"],
            "bound_version": cli_env["bound_version"]["version"],
        },
        "total_wall_seconds": round(time.perf_counter() - t_start, 3),
    }
    report["independent_check"] = {
        "expected_row_count": expected["row_count"],
        "matched_orders_before_projection": expected["matched_orders"],
        "expected_sample": [[list(tok) for tok in t] for t in expected["sample"]],
        "actual_sample": envelope["results"][:5],
        "relation_sizes_read": expected["relation_sizes"],
        "matches": envelope["row_count"] == expected["row_count"],
    }
    # independent full-row equality of the sampled prefix, not just a count
    actual_sample = {
        (r[0]["value"], r[1]["value"], r[2]["value"])
        for r in envelope["results"][:5]}
    exp_sample = {(t[0][1], t[1][1], t[2][1]) for t in expected["sample"]}
    report["independent_check"]["sample_rows_equal"] = \
        exp_sample == actual_sample
    report["independent_check"]["ok"] = (
        report["independent_check"]["matches"]
        and report["independent_check"]["sample_rows_equal"]
        and envelope["bound_version"]["version"] == expected["version"]
        and report["meets_100k_fact_target"])
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["independent_check"]["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
