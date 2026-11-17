"""Scale generator: run the completion on a generated workload and report
relation sizes, trigger counts and peak RSS.

    python -m chase.bench --orders 20000 --customers 5000

The workload is deliberately simple so it can run in a few seconds while still
exercising the disk store, the join matcher, TGD firing and null creation:

* ``Order(o, c)`` for ``orders`` rows over ``customers`` customers,
* ``KnownAddr(c, a)`` for a fraction of customers (so some unknowns unify
  with a constant and some stay),
* TGD: every order's customer needs a Customer row with an address,
* TGD: the known address is copied in,
* EGD: one address per customer.

Numbers are printed as JSON so they can be pasted into a report.  When the run
is complete it also verifies the result with the independent verifier (use
``--no-verify`` to skip that on very large runs).
"""

from __future__ import annotations

import argparse
import json
import random
import resource
import sys
import time

from .chase import Budgets, Engine
from .model import Schema, parse_program
from .pkgverify import _verify_payload
from .store import Store


def generate(orders: int, customers: int, known_fraction: float, seed: int = 7):
    rng = random.Random(seed)
    facts = {"Order": [], "KnownAddress": []}
    for i in range(customers):
        c = f"c{i}"
        if rng.random() < known_fraction:
            facts["KnownAddress"].append([c, f"addr{i % 97}"])
    for j in range(orders):
        facts["Order"].append([f"o{j}", f"c{rng.randrange(customers)}"])
    return facts


SCHEMA = {
    "Order": ["text", "text"],
    "Customer": ["text", "text"],
    "KnownAddress": ["text", "text"],
}
RULES = [
    {"id": "order_customer", "body": ["Order(o, c)"], "head": "Customer(c, a)"},
    {"id": "known_customer", "body": ["KnownAddress(c, a)"], "head": "Customer(c, a)"},
    {"id": "customer_key", "body": ["Customer(c, a1)", "Customer(c, a2)"],
     "eq": ["a1", "a2"]},
]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m chase.bench")
    ap.add_argument("--orders", type=int, default=2000)
    ap.add_argument("--customers", type=int, default=500)
    ap.add_argument("--known-fraction", type=float, default=0.5)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--max-seconds", type=float, default=120.0)
    ap.add_argument("--max-facts", type=int, default=2_000_000)
    ap.add_argument("--store", default=":memory:")
    ap.add_argument("--no-verify", action="store_true")
    ap.add_argument("--order-seed", type=int, default=0)
    args = ap.parse_args(argv)

    facts = generate(args.orders, args.customers, args.known_fraction, args.seed)
    schema = Schema({r: tuple(c) for r, c in SCHEMA.items()})
    rules = parse_program({"rules": RULES}, schema)
    store = Store(args.store)
    try:
        engine = Engine(schema, rules, store,
                        Budgets(max_seconds=args.max_seconds, max_facts=args.max_facts),
                        provenance=False, order_seed=args.order_seed)
        t0 = time.monotonic()
        engine.load_input({r: [tuple(x) for x in rows] for r, rows in facts.items()})
        result = engine.run()
        elapsed = time.monotonic() - t0
        stats = dict(result.stats)
        report = {
            "status": result.status,
            "reason": result.reason,
            "input": {"orders": len(facts["Order"]),
                      "known_addresses": len(facts["KnownAddress"]),
                      "customers": args.customers,
                      "known_fraction": args.known_fraction},
            "relations": stats.get("facts_by_relation"),
            "facts_total": stats.get("facts_total"),
            "trigger_fires": stats.get("trigger_fires"),
            "trigger_skips": stats.get("trigger_skips"),
            "body_bindings_enumerated": stats.get("body_bindings_enumerated"),
            "egd_checks": stats.get("egd_checks"),
            "unifications": stats.get("unifications"),
            "rebuilds": stats.get("rebuilds"),
            "duplicates_removed": stats.get("duplicates_removed"),
            "labeled_nulls_created": stats.get("labeled_nulls_created"),
            "live_null_classes": stats.get("null_classes"),
            "null_classes_absorbed_by_constants": stats.get("null_classes_absorbed_by_constants"),
            "rounds": stats.get("rounds"),
            "elapsed_seconds": round(elapsed, 3),
            "rss_peak_kb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        }
        if result.status == "complete" and not args.no_verify:
            exported = engine.export()
            v = _verify_payload(
                schema, rules, exported["facts"],
                {r: [tuple(x) for x in rows] for r, rows in facts.items()},
                exported["nulls"], None)
            report["verification"] = {"ok": v["ok"],
                                      "violations": len(v["violations"]),
                                      "tgd_bindings_checked": v["checks"]["tgd_bindings_checked"],
                                      "egd_bindings_checked": v["checks"]["egd_bindings_checked"]}
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0 if report["status"] == "complete" else 1
    finally:
        store.close()


if __name__ == "__main__":
    sys.exit(main())
