"""Scale generator for the correction workspace: a base load plus a series of
local corrections, reporting per-version facts, timings and peak RSS.

    python -m chase.wsbench --workspace /tmp/wsb --orders 30000 --customers 6000 \
        --corrections 12

The report is honest about the recompute scope: every commit re-runs the
chase over the *entire* current raw source set (no incremental caching, no
union-find inversion).  Numbers are printed as JSON so they can be pasted
into docs.
"""

from __future__ import annotations

import argparse
import json
import random
import resource
import shutil
import sys
import time

from .workspace import Workspace

SCHEMA = {
    "Order": ["text", "text"],
    "Customer": ["text", "text"],
    "KnownAddress": ["text", "text"],
}

RULES = [
    {"id": "t_order_customer", "body": ["Order(o, c)"], "head": "Customer(c, a)"},
    {"id": "t_known_customer", "body": ["KnownAddress(c, a)"], "head": "Customer(c, a)"},
    {"id": "e_customer_key", "body": ["Customer(c, a1)", "Customer(c, a2)"],
     "eq": ["a1", "a2"]},
]


def rss_kb() -> int:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


def base_ops(orders: int, customers: int, known_fraction: float, seed: int):
    rng = random.Random(seed)
    ops = []
    for i in range(customers):
        if rng.random() < known_fraction:
            ops.append({"op": "put", "source_id": "addr", "record_id": f"a{i}",
                        "relation": "KnownAddress", "tuple": [f"c{i}", f"addr{i % 97}"]})
    for j in range(orders):
        ops.append({"op": "put", "source_id": "shop", "record_id": f"o{j}",
                    "relation": "Order",
                    "tuple": [f"o{j}", f"c{rng.randrange(customers)}"]})
    return ops


def correction_ops(rng: random.Random, step: int, orders: int, customers: int,
                   width: int):
    """One local correction: replace some orders, move some addresses.

    Keys are de-duplicated (last decision wins) because a batch may touch
    each record key at most once.
    """
    decided: dict = {}
    for k in range(width):
        j = rng.randrange(orders)
        decided[("shop", f"o{j}")] = {
            "op": "put", "source_id": "shop", "record_id": f"o{j}",
            "relation": "Order",
            "tuple": [f"o{j}", f"c{rng.randrange(customers)}"]}
    for k in range(width // 4 + 1):
        i = rng.randrange(customers)
        decided[("addr", f"a{i}")] = {
            "op": "put", "source_id": "addr", "record_id": f"a{i}",
            "relation": "KnownAddress",
            "tuple": [f"c{i}", f"addr{(i + step) % 97}"]}
    for k in range(width // 4 + 1):
        j = rng.randrange(orders)
        decided[("shop", f"o{j}")] = {
            "op": "delete", "source_id": "shop", "record_id": f"o{j}"}
    return list(decided.values())


def run(workspace: str, orders: int, customers: int, known_fraction: float,
        corrections: int, width: int, seed: int) -> dict:
    shutil.rmtree(workspace, ignore_errors=True)
    ws = Workspace.init(workspace, {"schema": SCHEMA, "rules": RULES})
    report = {"workspace": workspace, "orders": orders, "customers": customers,
              "known_fraction": known_fraction, "corrections": corrections,
              "correction_width": width,
              "recompute_scope": "full (every commit re-chases the entire "
                                 "current raw source set; no incremental "
                                 "caching, no union-find inversion)",
              "versions": []}
    t_all = time.monotonic()

    t0 = time.monotonic()
    r = ws.commit({"batch_id": "base", "ops":
                   base_ops(orders, customers, known_fraction, seed)})
    report["versions"].append({
        "version": r["version"], "kind": "base", "status": r["status"],
        "facts": r["stats"]["facts_total"], "triggers": r["stats"]["trigger_fires"],
        "seconds": round(time.monotonic() - t0, 3)})

    rng = random.Random(seed + 1)
    for step in range(1, corrections + 1):
        t0 = time.monotonic()
        r = ws.commit({"batch_id": f"corr-{step}",
                       "ops": correction_ops(rng, step, orders, customers, width)})
        report["versions"].append({
            "version": r["version"], "kind": "correction", "status": r["status"],
            "facts": r["stats"]["facts_total"],
            "triggers": r["stats"]["trigger_fires"],
            "seconds": round(time.monotonic() - t0, 3)})

    report["total_seconds"] = round(time.monotonic() - t_all, 3)
    report["peak_rss_kb"] = rss_kb()
    report["final"] = ws.status()
    ws.close()
    return report


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m chase.wsbench")
    ap.add_argument("--workspace", required=True)
    ap.add_argument("--orders", type=int, default=30000)
    ap.add_argument("--customers", type=int, default=6000)
    ap.add_argument("--known-fraction", type=float, default=0.6)
    ap.add_argument("--corrections", type=int, default=12)
    ap.add_argument("--width", type=int, default=50,
                    help="records touched per correction batch")
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args(argv)
    report = run(args.workspace, args.orders, args.customers,
                 args.known_fraction, args.corrections, args.width, args.seed)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
