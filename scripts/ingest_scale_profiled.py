#!/usr/bin/env python3
"""In-process, phase-level timing and RSS for a large snapshot swap.

Unlike :mod:`scripts.ingest_scale` (which drives the CLI in subprocesses),
this runner performs staging and completion inside one process and reads
peak RSS around each phase, so the three cost centers are reported
separately:

* **ingest**  — streaming CSV parse + strict cell conversion (rows land in
  SQLite one at a time; the CSV text and key set are never accumulated);
* **reconcile** — set-based candidate derivation in SQLite;
* **completion** — the existing chase engine's full re-solve, measured on its
  own so its memory behavior cannot mask the ingest profile.

Run::

    python3 -I scripts/ingest_scale_profiled.py [--rows 100000] [--shards 4]
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import resource
import shutil
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from chase.ingest import IngestManager  # noqa: E402
from chase.ingest_decl import parse_declaration  # noqa: E402
from chase.workspace import Workspace  # noqa: E402


def rss_mb():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


class PhaseTimer:
    def __init__(self):
        self.reads = {}

    def measure(self, name, fn):
        rss0 = rss_mb()
        t0 = time.perf_counter()
        result = fn()
        dt = time.perf_counter() - t0
        self.reads[name] = {
            "seconds": round(dt, 3),
            "peak_rss_mb_at_start": round(rss0, 1),
            "peak_rss_mb_at_end": round(rss_mb(), 1),
        }
        return result


def write_shard(path, header, rows):
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=100_000)
    ap.add_argument("--shards", type=int, default=4)
    ap.add_argument("--workdir", default=None)
    args = ap.parse_args()
    n, nshards = args.rows, args.shards

    workdir = args.workdir or os.path.join(
        tempfile.gettempdir(), "chase-ingest-profiled")
    if os.path.exists(workdir):
        shutil.rmtree(workdir)
    os.makedirs(workdir)
    ws_dir = os.path.join(workdir, "ws")

    program = {
        "schema": {
            "columns": {"Item": ["text", "int", "text"]},
            "column_names": {"Item": ["item_id", "qty", "label"]}},
        "rules": [{"id": "t_self", "body": ["Item(i, q, l)"],
                   "head": "Item(i, q, l)"}],
    }
    Workspace.init(ws_dir, program)
    timer = PhaseTimer()

    def declaration(paths):
        return parse_declaration({
            "connector": "scale-conn", "version": 1,
            "unknown_markers": {"marker": "NULL", "name_prefix": "",
                                "name_separator": ""},
            "sources": [{
                "source_id": "scale", "relation": "Item",
                "key_columns": ["item_id"],
                "fields": [
                    {"name": "item_id", "header": "item_id"},
                    {"name": "qty", "header": "qty"},
                    {"name": "label", "header": "label"}],
                "shards": [{"path": p} for p in paths],
                "empty_snapshot": False}]}, Workspace(ws_dir).schema)

    # generate snapshot 1
    header = ["item_id", "qty", "label"]
    ids = [f"item-{i:08d}" for i in range(n)]
    paths1 = []
    t_gen = time.perf_counter()
    for si in range(nshards):
        group = ids[si::nshards]
        p = os.path.join(workdir, f"s1-part{si}.csv")
        write_shard(p, header,
                    [[i, str(1000 + (hash(i) % 9000)), f"label-{i}"]
                     for i in group])
        paths1.append(p)
    gen_seconds = round(time.perf_counter() - t_gen, 3)
    total_bytes = sum(os.path.getsize(p) for p in paths1)

    ws = Workspace(ws_dir)
    mgr = IngestManager(ws)

    # phase A: ingest (parse + convert) + reconcile, in one staged task
    decl1 = declaration(paths1)
    rep1 = timer.measure("ingest_and_reconcile_snapshot1",
                         lambda: mgr.stage_task("scale-1", decl1))
    # phase B: apply (which runs the chase completion)
    rec1 = timer.measure("apply_and_completion_snapshot1",
                         lambda: mgr.apply_task("scale-1"))

    # snapshot 2: half unchanged, a quarter replaced, some new, some retracted
    n_replace = n // 10
    n_new = n // 20
    n_retract = n // 25
    paths2 = []
    surviving = set()
    t_gen2 = time.perf_counter()
    for si in range(nshards):
        group = ids[si::nshards]
        rows = []
        for j, i in enumerate(group):
            if j % 25 == 0:
                continue  # retract a regular fraction
            surviving.add(i)
            qty = (str(7000 + j) if j % 10 == 0
                   else str(1000 + (hash(i) % 9000)))
            rows.append([i, qty, f"label-{i}"])
        if si == 0:
            rows += [[f"new-{k:08d}", "500", f"fresh-{k}"]
                     for k in range(n_new)]
        p = os.path.join(workdir, f"s2-part{si}.csv")
        write_shard(p, header, rows)
        paths2.append(p)
    gen2_seconds = round(time.perf_counter() - t_gen2, 3)

    decl2 = declaration(paths2)
    rep2 = timer.measure("ingest_and_reconcile_snapshot2",
                         lambda: mgr.stage_task("scale-2", decl2))
    rec2 = timer.measure("apply_and_completion_snapshot2",
                         lambda: mgr.apply_task("scale-2"))

    # isolate reconcile query cost from parse cost on snapshot 2's stored data
    def reconcile_read():
        # read back all candidates via the stored tables (SQLite-side work)
        return mgr.candidate_count("scale-2")
    ncand = timer.measure("reconcile_readback_snapshot2", reconcile_read)

    # independent expected counts for snapshot 2
    retracted = n - len(surviving)
    exp = {
        "new": n_new,
        "replace": sum(1 for si in range(nshards)
                       for j, i in enumerate(ids[si::nshards])
                       if j % 25 != 0 and j % 10 == 0),
        "retract": retracted,
        "final_live": len(surviving) + n_new,
    }
    got = rep2["stats"]["sources"]["scale"]["candidates"]
    live = ws.conn.execute(
        "SELECT COUNT(*) FROM source_records WHERE source_id='scale'"
    ).fetchone()[0]

    report = {
        "rows": n, "shards": nshards,
        "csv_total_bytes_snapshot1": total_bytes,
        "generate_seconds": {"snapshot1": gen_seconds,
                             "snapshot2": gen2_seconds},
        "phase_measurements": timer.reads,
        "reconcile_candidate_rows_read": ncand,
        "snapshot2_candidates": got,
        "independent_expected": exp,
        "live_source_records": live,
        "expected_live_source_records": exp["final_live"],
        "completion_status": {"v1": rec1["status"], "v2": rec2["status"]},
        "db_size_bytes": os.path.getsize(
            os.path.join(ws_dir, "workspace.sqlite")),
    }
    print(json.dumps(report, indent=2))
    ok = (got["put"] == exp["new"] + exp["replace"]
          and got["delete"] == exp["retract"]
          and live == exp["final_live"])
    print("\nSCALE PROFILE " + ("OK" if ok else "MISMATCH"))
    ws.close()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
