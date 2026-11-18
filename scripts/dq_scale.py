#!/usr/bin/env python3
"""Large-input data-quality audit + independent cross-check (runtime data only).

Generates a workspace with 100 000+ raw source records and 150 000+ completed
facts at predictable proportions, runs the production audit, and verifies the
result with a **separate** checker that never imports the audit module: it
reads the workspace SQLite state and the stored version payload as plain
Python data and enumerates every check row itself.

Reported per rule: the staged relation row count, the index range the
aggregate used (from ``EXPLAIN QUERY PLAN``), the measured number of rows the
predicate visits (counting UDF over the same WHERE), elapsed time and the
process peak RSS.  The audit process is run in a child so its peak RSS is
measured on its own; the checker's memory is reported separately.

Nothing large is written into the repository: the workspace and the data are
created under a temporary directory (or ``--workspace``).

Run::

    python3 -I scripts/dq_scale.py --orders 60000 --customers 20000 --items 20000
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import zlib

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

REGIONS = ("东区", "西区", "南区", "北区")


def rss_mb():
    """This process's own peak RSS.

    ru_maxrss is inherited across fork/exec and would include the parent's
    peak; /proc/self/status VmHWM is this process only.
    """
    try:
        with open("/proc/self/status", encoding="ascii") as fh:
            for line in fh:
                if line.startswith("VmHWM:"):
                    return round(int(line.split()[1]) / 1024.0, 1)
    except OSError:
        pass
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0, 1)


PROGRAM = {
    "schema": {
        "Order": ["text", "text"],
        "Customer": ["text", "text"],
        "KnownRegion": ["text", "text"],
        "Done": ["text", "text"],
        "Item": ["text", "text", "int"],
    },
    "rules": [
        {"id": "t_order_customer", "body": ["Order(o, c)"],
         "head": "Customer(c, r)"},
        {"id": "t_known_region", "body": ["KnownRegion(c, r)"],
         "head": "Customer(c, r)"},
        {"id": "t_done", "body": ["Order(o, c)"], "head": "Done(o, c)"},
    ],
}

POLICY = {
    "format": "data-quality-policy/1",
    "policy_id": "dq.scale",
    "revision": 1,
    "description": "scale fixture: required/enum on completed regions, typed "
                   "uniqueness + references on raw source records",
    "sample_limit": 10,
    "rules": [
        {"id": "req-region", "kind": "required", "layer": "completed",
         "relation": "Customer", "column": "c1", "severity": "error"},
        {"id": "enum-region", "kind": "enum", "layer": "completed",
         "relation": "Customer", "column": "c1", "severity": "error",
         "allowed": [{"type": "text", "value": r} for r in REGIONS]},
        {"id": "uniq-order-id", "kind": "unique", "layer": "sources",
         "relation": "Order", "columns": ["c0"], "severity": "error"},
        {"id": "ref-order-customer", "kind": "reference", "layer": "sources",
         "relation": "Order", "columns": ["c1"],
         "target": {"relation": "Customer", "columns": ["c0"]},
         "severity": "error"},
        {"id": "ref-item-order", "kind": "reference", "layer": "sources",
         "relation": "Item", "columns": ["c0"],
         "target": {"relation": "Order", "columns": ["c0"]},
         "severity": "error"},
        {"id": "enum-item-qty", "kind": "enum", "layer": "sources",
         "relation": "Item", "column": "c2", "severity": "warning",
         "allowed": [{"type": "int", "value": 7},
                     {"type": "int", "value": 700}]},
    ],
}


# --------------------------------------------------------------------------- #
# predictable data
# --------------------------------------------------------------------------- #
def generate(orders: int, customers: int, items: int, dup_every: int = 20,
             unknown_every: int = 10, missing_every: int = 50):
    """Return (ops, expected) with deterministic proportions.

    * every ``dup_every``-th order is reported by a second source as well
      (same typed tuple, distinct source record -> a real duplicate key that
      the completion collapses);
    * every ``unknown_every``-th customer has a labeled-null region;
    * every ``missing_every``-th item points at an order id that does not
      exist, and every 5th item has an unknown qty.
    """
    ops = []
    seen_orders = set()
    n_dup_records = 0
    n_unknown_regions = 0
    n_missing_refs = 0
    n_unknown_qty = 0
    region_seq = 0
    for i in range(orders):
        oid = f"o{i:07d}"
        cid = f"c{i % customers:06d}"
        ops.append({"op": "put", "source_id": "erp", "record_id": f"order-{oid}",
                    "relation": "Order", "tuple": [oid, cid]})
        seen_orders.add(oid)
        if i % dup_every == 0:
            ops.append({"op": "put", "source_id": "shop",
                        "record_id": f"order-{oid}", "relation": "Order",
                        "tuple": [oid, cid]})
            n_dup_records += 1
    for j in range(customers):
        cid = f"c{j:06d}"
        if j % unknown_every == 0:
            tuple_ = [cid, {"null": f"u{region_seq}"}]
            region_seq += 1
            n_unknown_regions += 1
        else:
            tuple_ = [cid, REGIONS[j % len(REGIONS)]]
        ops.append({"op": "put", "source_id": "crm",
                    "record_id": f"region-{cid}", "relation": "KnownRegion",
                    "tuple": tuple_})
    for k in range(items):
        if k % missing_every == 0:
            oid = f"missing-{k:07d}"
            n_missing_refs += 1
        else:
            oid = f"o{(k * 7) % orders:07d}"
        if k % 5 == 0:
            qty = {"null": f"q{region_seq + k}"}
            n_unknown_qty += 1
        else:
            qty = 7 if k % 4 else 700
        ops.append({"op": "put", "source_id": "wms", "record_id": f"item-{k:07d}",
                    "relation": "Item", "tuple": [oid, str(k % 97), qty]})
    return ops, {
        "duplicate_records": n_dup_records,
        "duplicate_groups": (orders + dup_every - 1) // dup_every,
        "unknown_regions": n_unknown_regions,
        "missing_refs": n_missing_refs,
        "unknown_qty": n_unknown_qty,
        "records": len(ops),
    }


# --------------------------------------------------------------------------- #
# independent checker (no audit imports)
# --------------------------------------------------------------------------- #
def read_sources(ws_path: str):
    conn = sqlite3.connect(f"file:{ws_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    rows = []
    for r in conn.execute("SELECT source_id, record_id, relation, tuple_json "
                          "FROM source_records"):
        rows.append({"source_id": r["source_id"], "record_id": r["record_id"],
                     "relation": r["relation"],
                     "values": [tuple(v) for v in json.loads(r["tuple_json"])]})
    conn.close()
    rows.sort(key=lambda r: (r["source_id"], r["record_id"]))
    return rows


def read_completed(ws_path: str, version: int):
    conn = sqlite3.connect(f"file:{ws_path}?mode=ro", uri=True)
    blob = conn.execute("SELECT payload FROM versions WHERE version=?",
                        (version,)).fetchone()[0]
    conn.close()
    payload = json.loads(zlib.decompress(blob))

    def tok(v):
        if v["kind"] == "labeled_null":
            return ("n", v["id"])
        return ("s", v["value"]) if v["kind"] == "string" else ("i", v["value"])

    return {rel: [[tok(v) for v in row] for row in rows]
            for rel, rows in payload["facts"].items()}


def check_independently(ws_path: str, version: int, sample_limit: int,
                        groups_wanted: int = 3):
    """Enumerate every rule from plain data; return counts + verified samples."""
    sources = read_sources(ws_path)
    completed = read_completed(ws_path, version)
    out = {}

    def source_rows(rel):
        return [r for r in sources if r["relation"] == rel]

    # required on completed Customer.c1
    rows = completed.get("Customer", [])
    viol = [r for r in rows if r[1][0] == "n"]
    out["req-region"] = {"total": len(rows), "violation": len(viol),
                         "indeterminate": 0, "pass": len(rows) - len(viol)}

    # enum on completed Customer.c1
    allowed = set(("s", r) for r in REGIONS)
    v = [r for r in rows if r[1][0] == "n"]
    good = [r for r in rows if r[1] in allowed]
    out["enum-region"] = {"total": len(rows), "violation": len(rows)
                          - len(v) - len(good),
                          "indeterminate": len(v), "pass": len(good)}

    # unique on raw source Order.c0
    recs = source_rows("Order")
    groups = {}
    for r in recs:
        groups.setdefault(r["values"][0], []).append(r)
    dup_groups = [g for g in groups.values() if len(g) > 1]
    out["uniq-order-id"] = {
        "total": len(recs),
        "violation": sum(len(g) for g in dup_groups),
        "indeterminate": 0,
        "pass": len(recs) - sum(len(g) for g in dup_groups),
        "duplicate_groups": len(dup_groups),
        "group_sizes": sorted({len(g) for g in dup_groups}),
        "example_group_keys": [
            g[0]["values"][0][1] for g in
            sorted(dup_groups, key=lambda g: g[0]["values"][0])[:groups_wanted]],
    }

    # reference raw Order.c1 -> completed Customer.c0
    cust_keys = {r[0] for r in completed.get("Customer", [])}
    viol = [r for r in recs if r["values"][1] not in cust_keys]
    out["ref-order-customer"] = {
        "total": len(recs), "violation": len(viol), "indeterminate": 0,
        "pass": len(recs) - len(viol)}

    # reference raw Item.c0 -> completed Order.c0
    order_keys = {r[0] for r in completed.get("Order", [])}
    items = source_rows("Item")
    viol = [r for r in items if r["values"][0] not in order_keys]
    out["ref-item-order"] = {
        "total": len(items), "violation": len(viol), "indeterminate": 0,
        "pass": len(items) - len(viol),
        "example_missing": sorted(r["values"][0][1] for r in viol)[:groups_wanted]}

    # enum on raw Item.c2
    allowed_qty = {("i", 7), ("i", 700)}
    ind = [r for r in items if r["values"][2][0] == "n"]
    good = [r for r in items if r["values"][2] in allowed_qty]
    out["enum-item-qty"] = {
        "total": len(items), "violation": len(items) - len(ind) - len(good),
        "indeterminate": len(ind), "pass": len(good)}
    return out, {"sources": len(sources),
                 "completed_facts": sum(len(v) for v in completed.values())}


# --------------------------------------------------------------------------- #
# measured audit (child process so its RSS is its own)
# --------------------------------------------------------------------------- #
_AUDIT_CHILD = r'''
import json, os, resource, sys, time

def peak_rss_mb():
    # /proc/self/status VmHWM is this process only (ru_maxrss is inherited
    # across fork/exec from the parent's peak)
    try:
        with open("/proc/self/status", encoding="ascii") as fh:
            for line in fh:
                if line.startswith("VmHWM:"):
                    return round(int(line.split()[1]) / 1024.0, 1)
    except OSError:
        pass
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0, 1)

sys.path.insert(0, sys.argv[1])
from chase.dqaudit import run_audit
t0 = time.monotonic()
report = run_audit(sys.argv[2], json.load(open(sys.argv[3], encoding="utf-8")),
                   sources_version=sys.argv[4], completed_version=sys.argv[5],
                   sample_limit=int(sys.argv[6]), output=sys.argv[7],
                   force=True)
print(json.dumps({
    "seconds": round(time.monotonic() - t0, 3),
    "peak_rss_mb": peak_rss_mb(),
    "status": report["status"],
    "totals": report["totals"],
}, ensure_ascii=False))
'''


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--orders", type=int, default=60000)
    ap.add_argument("--customers", type=int, default=20000)
    ap.add_argument("--items", type=int, default=20000)
    ap.add_argument("--sample-limit", type=int, default=10)
    ap.add_argument("--workspace", default=None)
    ap.add_argument("--keep", action="store_true")
    args = ap.parse_args(argv)

    base = args.workspace or tempfile.mkdtemp(prefix="chase-dq-scale-")
    ws_path = os.path.join(base, "ws")
    db_path = os.path.join(ws_path, "workspace.sqlite")
    if os.path.exists(ws_path):
        shutil.rmtree(ws_path)
    os.makedirs(base, exist_ok=True)
    expected = {
        "duplicate_records": (args.orders + 19) // 20,
        "duplicate_groups": (args.orders + 19) // 20,
        "unknown_regions": (args.customers + 9) // 10,
        "missing_refs": (args.items + 49) // 50,
        "unknown_qty": (args.items + 4) // 5,
        "records": (args.orders + (args.orders + 19) // 20
                    + args.customers + args.items),
    }

    if os.path.isfile(db_path):
        print("== reusing the existing workspace (data already generated) ==")
        from chase.workspace import Workspace
        ws = Workspace(ws_path)
        receipt = ws.get_version(ws.latest_source_version())
        ws.close()
        assert receipt["status"] == "complete", receipt
        completion_seconds = None
    else:
        print("== generating data ==")
        t0 = time.monotonic()
        ops, expected = generate(args.orders, args.customers, args.items)
        gen_seconds = time.monotonic() - t0
        print(json.dumps({"generated_records": expected["records"],
                          "generation_seconds": round(gen_seconds, 2),
                          "expected": expected}, ensure_ascii=False, indent=2))

        from chase.workspace import Workspace
        print("== completing the fixture version ==")
        t0 = time.monotonic()
        ws = Workspace.init(ws_path, PROGRAM)
        receipt = ws.commit({"batch_id": "scale-base", "ops": ops})
        ws.close()
        completion_seconds = time.monotonic() - t0
        print(json.dumps({"version": receipt["version"],
                          "status": receipt["status"],
                          "completion_seconds": round(completion_seconds, 2),
                          "facts": receipt["stats"].get("facts_total"),
                          "records": expected["records"]}, indent=2))
        assert receipt["status"] == "complete", receipt
    version = receipt["version"]

    policy_path = os.path.join(base, "policy.json")
    with open(policy_path, "w", encoding="utf-8") as fh:
        json.dump(POLICY, fh, ensure_ascii=False)
    report_path = os.path.join(base, "report.json")

    print("== running the production audit (separate process) ==")
    code = _AUDIT_CHILD
    proc = subprocess.run(
        [sys.executable, "-I", "-c", code,
         os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
         ws_path, policy_path, str(version),
         str(version), str(args.sample_limit), report_path],
        capture_output=True, text=True, timeout=3600)
    if proc.returncode != 0:
        sys.stderr.write(proc.stdout + proc.stderr)
        return 1
    audit_stats = json.loads(proc.stdout)
    report = json.load(open(report_path, encoding="utf-8"))

    print("== independent enumeration (no audit imports) ==")
    t0 = time.monotonic()
    indep, sizes = check_independently(db_path, version,
                                       args.sample_limit)
    check_seconds = time.monotonic() - t0

    ok = True
    print(json.dumps({"audit": audit_stats, "dataset": sizes,
                      "completion_seconds": completion_seconds,
                      "independent_check_seconds": round(check_seconds, 2),
                      "independent_check_peak_rss_mb": rss_mb()},
                     ensure_ascii=False, indent=2))
    print("\n%-22s %9s %9s %9s  %s" %
          ("rule", "total", "violation", "indet", "match"))
    for rule in report["rules"]:
        exp = indep[rule["id"]]
        got = rule["counts"]
        match = all(got[k] == exp[k] for k in
                    ("total", "violation", "indeterminate", "pass"))
        ok = ok and match
        print("%-22s %9d %9d %9d  %s" % (
            rule["id"], got["total"], got["violation"],
            got["indeterminate"], "OK" if match else "MISMATCH"))
        if not match:
            print("   expected:", exp, "got:", got)

    # duplicate-group detail cross-check
    uniq = next(r for r in report["rules"] if r["id"] == "uniq-order-id")
    exp = indep["uniq-order-id"]
    group_ok = (uniq["counts"]["violation"] == exp["violation"]
                and exp["duplicate_groups"] == expected["duplicate_groups"]
                and exp["group_sizes"] == [2])
    ok = ok and group_ok
    print("\nduplicate groups: %d (expected %d), sizes=%s -> %s" % (
        exp["duplicate_groups"], expected["duplicate_groups"],
        exp["group_sizes"], "OK" if group_ok else "MISMATCH"))
    keys = sorted({g["key"][0]["value"] for g in uniq["samples"][:2]})
    print("sample duplicate keys:", keys,
          "| independent:", exp["example_group_keys"])
    assert set(keys) <= set(exp["example_group_keys"])

    # every reported sample must be a genuine violation; count must be honest
    for rule in report["rules"]:
        v = rule["counts"]["violation"]
        if v:
            assert rule["samples"], rule["id"]
            assert all(s["category"] == "violation" for s in rule["samples"])
        if rule["samples_truncated"]:
            assert v + rule["counts"]["indeterminate"] > len(rule["samples"])

    print("\n== per-rule scan / plan / timing (measured) ==")
    for rule in report["rules"]:
        print(json.dumps({
            "rule": rule["id"],
            "relation_rows_scanned": rule["scan"].get("relation_rows"),
            "index_range": rule["scan"].get("index"),
            "query_plan": rule["query_plan"],
            "elapsed_seconds": rule["elapsed_seconds"],
            "samples": len(rule["samples"]),
            "sample_omitted": rule["sample_omitted"],
        }, ensure_ascii=False))
    print(json.dumps({
        "audit_peak_rss_mb": audit_stats["peak_rss_mb"],
        "audit_seconds": audit_stats["seconds"],
        "stage_phase_seconds": report["phase_seconds"],
        "report_bytes": os.path.getsize(report_path),
    }, ensure_ascii=False, indent=2))
    print("\nRESULT:", "OK" if ok else "FAIL")
    print(json.dumps({
        "report_id": report["report_id"],
        "report_sha256": report["report_sha256"],
        "sample_limit": args.sample_limit,
    }, indent=2))
    if not args.keep:
        shutil.rmtree(base, ignore_errors=True)
    else:
        print("kept:", base)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
