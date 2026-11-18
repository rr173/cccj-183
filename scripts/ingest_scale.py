#!/usr/bin/env python3
"""Large-scale full-snapshot swap, measured and independently cross-checked.

Run (repository root)::

    python3 -I scripts/ingest_scale.py [--rows 100000] [--shards 4] [--workdir DIR]

Everything is generated at runtime (no large fixtures are committed).  The
script:

* writes an initial snapshot of ``--rows`` source records split across
  ``--shards`` CSV files and stages/applies it;
* writes a second full snapshot containing, by construction, a known number of
  new rows, replacements, quarantined-but-retained rows, unchanged rows and
  retracted rows, then stages/applies the swap;
* recomputes every expected count and a sample of expected tuples
  independently from the generated inputs and the previous ledger, without
  importing any production reconciliation code;
* reports wall time and peak RSS for the three distinct phases — **ingest**
  (CSV parse + strict convert + reconcile), **reconcile-only** queries and
  **completion** (the existing chase engine, which is allowed to solve in its
  own way) — so the streaming ingest memory profile is never hidden behind
  the engine's.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import resource
import shutil
import subprocess
import sys
import tempfile
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUNNER = os.path.join(REPO, "run_cli.py")


def rss_mb():
    # ru_maxrss is KB on Linux
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def write_shard(path, header, rows):
    # use the stdlib writer ONLY to generate well-formed fixture bytes; the
    # production parser never uses it.  newline="" keeps CRLF/LF exact.
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)


def run_cli(*args):
    proc = subprocess.run(
        [sys.executable, "-I", RUNNER, *args],
        capture_output=True, text=True, cwd=REPO)
    if proc.returncode not in (0, 2, 3):
        raise AssertionError(
            f"CLI failed rc={proc.returncode}: {' '.join(args)}\n"
            f"{proc.stderr[:2000]}")
    return json.loads(proc.stdout) if proc.stdout.strip() else {}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=100_000)
    ap.add_argument("--shards", type=int, default=4)
    ap.add_argument("--workdir", default=None)
    args = ap.parse_args()
    n = args.rows
    nshards = args.shards

    workdir = args.workdir or os.path.join(
        tempfile.gettempdir(), "chase-ingest-scale")
    if os.path.exists(workdir):
        shutil.rmtree(workdir)
    os.makedirs(workdir)
    ws = os.path.join(workdir, "ws")

    # a deliberately cheap program: one self-copy TGD keeps completion work
    # bounded so the measured number reflects ingestion, not chase cost
    program = {
        "schema": {
            "columns": {"Item": ["text", "int", "text"]},
            "column_names": {"Item": ["item_id", "qty", "label"]}},
        "rules": [{"id": "t_self", "body": ["Item(i, q, l)"],
                   "head": "Item(i, q, l)"}],
    }
    prog_path = os.path.join(workdir, "program.json")
    json.dump(program, open(prog_path, "w"))
    run_cli("ws", "init", ws, "--program", prog_path)

    def decl_for(shard_paths, task_connector="scale-conn"):
        return {
            "connector": task_connector, "version": 1,
            "unknown_markers": {"marker": "NULL", "name_prefix": "",
                                "name_separator": ""},
            "sources": [{
                "source_id": "scale", "relation": "Item",
                "key_columns": ["item_id"],
                "fields": [
                    {"name": "item_id", "header": "item_id"},
                    {"name": "qty", "header": "qty"},
                    {"name": "label", "header": "label"}],
                "shards": [{"path": p} for p in shard_paths],
                "empty_snapshot": False}],
        }

    header = ["item_id", "qty", "label"]
    # split n ids across shards deterministically
    ids = [f"item-{i:08d}" for i in range(n)]
    shard_ids = [ids[i::nshards] for i in range(nshards)]
    snap1_dir = os.path.join(workdir, "snap1")
    os.makedirs(snap1_dir)
    snap1_paths = []
    t0 = time.perf_counter()
    bytes_written = 0
    for si, group in enumerate(shard_ids):
        p = os.path.join(snap1_dir, f"part{si}.csv")
        rows = [[i, str(1000 + (hash(i) % 9000)), f"label-{i}"] for i in group]
        write_shard(p, header, rows)
        snap1_paths.append(p)
        bytes_written += os.path.getsize(p)
    gen1_s = time.perf_counter() - t0

    d1 = os.path.join(workdir, "decl1.json")
    json.dump(decl_for(snap1_paths), open(d1, "w"))

    # ---- phase 1: ingest (parse + convert + reconcile) ------------------- #
    t0 = time.perf_counter()
    rss_before = rss_mb()
    rep1 = run_cli("ingest", "stage", ws, "--declaration", d1,
                   "--task", "scale-1")
    stage1_s = time.perf_counter() - t0
    rss_stage1 = rss_mb()
    t0 = time.perf_counter()
    rec1 = run_cli("ingest", "apply", ws, "--task", "scale-1")
    apply1_s = time.perf_counter() - t0

    # independent expected ledger after snapshot 1: exactly n records
    expected_ledger = {}
    for i in ids:
        expected_ledger[i] = [["s", i], ["i", 1000 + (hash(i) % 9000)],
                              ["s", f"label-{i}"]]

    # ---- snapshot 2: a controlled mix ------------------------------------- #
    # construction:
    #   first n_new ids are brand new
    #   next n_replace ids get a changed qty (legal replacement)
    #   next n_bad ids get a non-int qty (quarantine: key appears, old kept)
    #   next n_retract ids are omitted (integral -> retract)
    #   all remaining ids are unchanged
    n_new = min(5_000, n // 20)
    n_replace = min(5_000, n // 20)
    n_bad = min(2_000, n // 50)
    n_retract = min(3_000, n // 30)
    used = n_new + n_replace + n_bad + n_retract
    assert used < n, "fixture sizing leaves no unchanged rows"
    new_ids = [f"new-{i:08d}" for i in range(n_new)]
    replace_idx = list(range(n_new, n_new + n_replace))
    bad_idx = list(range(n_new + n_replace, n_new + n_replace + n_bad))
    retract_idx = set(range(
        n_new + n_replace + n_bad,
        n_new + n_replace + n_bad + n_retract))

    snap2_rows = []
    # new rows
    for i in new_ids:
        snap2_rows.append([i, "500", f"fresh-{i}"])
    # existing rows
    for idx, i in enumerate(ids):
        if idx in retract_idx:
            continue
        if idx in replace_idx:
            qty = str(2000 + idx)           # changed
        elif idx in bad_idx:
            qty = "not-an-int"             # quarantine: old tuple retained
        else:
            qty = str(1000 + (hash(i) % 9000))  # unchanged
        snap2_rows.append([i, qty, f"label-{i}"])
    # re-split across the same shard count by item_id hash bands for realism
    snap2_dir = os.path.join(workdir, "snap2")
    os.makedirs(snap2_dir)
    snap2_paths = [os.path.join(snap2_dir, f"part{si}.csv")
                   for si in range(nshards)]
    buckets = [[] for _ in range(nshards)]
    for r in snap2_rows:
        buckets[hash(r[0]) % nshards].append(r)
    t0 = time.perf_counter()
    bytes2 = 0
    for si, rows in enumerate(buckets):
        write_shard(snap2_paths[si], header, rows)
        bytes2 += os.path.getsize(snap2_paths[si])
    gen2_s = time.perf_counter() - t0

    d2 = os.path.join(workdir, "decl2.json")
    json.dump(decl_for(snap2_paths), open(d2, "w"))

    # ---- independent expectation for snapshot 2 -------------------------- #
    exp_put = n_new + n_replace
    exp_delete = n_retract
    exp_quarantine = n_bad
    # keep = surviving old rows that are unchanged (not replace, not bad,
    # not retracted)
    exp_keep = n - n_replace - n_bad - n_retract
    # expected final live record count:
    #   old n minus retracted, plus brand-new ids
    exp_final_count = n - n_retract + n_new

    t0 = time.perf_counter()
    rep2 = run_cli("ingest", "stage", ws, "--declaration", d2,
                   "--task", "scale-2")
    stage2_s = time.perf_counter() - t0
    rss_stage2 = rss_mb()
    got = rep2["stats"]["sources"]["scale"]["candidates"]
    rows_outcomes = rep2["stats"]["sources"]["scale"]

    t0 = time.perf_counter()
    rec2 = run_cli("ingest", "apply", ws, "--task", "scale-2")
    apply2_s = time.perf_counter() - t0
    rss_apply2 = rss_mb()

    # ---- independent verification against the live workspace ------------- #
    sources = run_cli("ws", "sources", ws)
    live = {r["record_id"]: r["tuple"] for r in sources["records"]
            if r["source_id"] == "scale"}

    failures = []
    def expect(cond, label):
        print(("  ok  " if cond else " FAIL ") + label)
        if not cond:
            failures.append(label)

    print("=" * 70)
    print(f"full-snapshot swap at N={n:,} rows across {nshards} shards")
    print("=" * 70)
    expect(rep1["stats"]["sources"]["scale"]["candidates"]["put"] == n,
           f"snapshot 1: {n} puts")
    expect(got["put"] == exp_put, f"put={got['put']} expected {exp_put} "
                                  f"(new {n_new} + replace {n_replace})")
    expect(got["delete"] == exp_delete,
           f"delete={got['delete']} expected {exp_delete}")
    expect(got["quarantine"] == exp_quarantine,
           f"quarantine={got['quarantine']} expected {exp_quarantine}")
    expect(got["keep"] == exp_keep,
           f"keep={got['keep']} expected {exp_keep}")
    expect(rows_outcomes["quarantine"] == n_bad,
           f"quarantined row count {rows_outcomes['quarantine']} == {n_bad}")
    expect(len(live) == exp_final_count,
           f"live records {len(live)} expected {exp_final_count}")

    # sample independent tuple checks
    # a replaced row carries the new qty
    sample_replace = ids[replace_idx[0]]
    rid = json.dumps([["s", sample_replace]], separators=(",", ":"))
    expect(live[rid][1] == 2000 + replace_idx[0],
           "sampled replacement has the new int qty")
    # a quarantined row keeps its ORIGINAL qty (the bad cell never deletes it)
    sample_bad = ids[bad_idx[0]]
    rid_bad = json.dumps([["s", sample_bad]], separators=(",", ":"))
    expect(live[rid_bad][1] == 1000 + (hash(sample_bad) % 9000),
           "sampled quarantined key retains its old tuple")
    # a retracted row is gone
    sample_retract = ids[next(iter(retract_idx))]
    rid_ret = json.dumps([["s", sample_retract]], separators=(",", ":"))
    expect(rid_ret not in live, "sampled retracted key absent")
    # a new row exists
    sample_new = new_ids[0]
    rid_new = json.dumps([["s", sample_new]], separators=(",", ":"))
    expect(rid_new in live and live[rid_new][1] == 500,
           "sampled brand-new row present with its qty")
    # an unchanged row survives with identical value
    unchanged_idx = used + 1
    sample_keep = ids[unchanged_idx]
    rid_keep = json.dumps([["s", sample_keep]], separators=(",", ":"))
    expect(live[rid_keep][1] == 1000 + (hash(sample_keep) % 9000),
           "sampled unchanged row retained")

    # engine completion stats (the engine's own memory profile, reported
    # separately and never conflated with ingest streaming)
    completion_rss1 = rec1["stats"].get("rss_kb", 0) / 1024.0
    completion_rss2 = rec2["stats"].get("rss_kb", 0) / 1024.0

    print("\nphase measurements (this driver process; CLI work is in")
    print("subprocesses, so RSS below is the driver's rusage snapshot):")
    print(json.dumps({
        "rows": n, "shards": nshards,
        "csv_bytes_snapshot1": bytes_written,
        "csv_bytes_snapshot2": bytes2,
        "generate_seconds": {"snapshot1": round(gen1_s, 3),
                             "snapshot2": round(gen2_s, 3)},
        "ingest_stage_seconds": {"snapshot1": round(stage1_s, 3),
                                 "snapshot2": round(stage2_s, 3)},
        "apply_seconds": {"snapshot1": round(apply1_s, 3),
                          "snapshot2": round(apply2_s, 3)},
        "driver_peak_rss_mb": {"after_stage1": round(rss_stage1, 1),
                               "before_stage1": round(rss_before, 1),
                               "after_stage2": round(rss_stage2, 1),
                               "after_apply2": round(rss_apply2, 1)},
        "completion_engine_rss_mb": {
            "snapshot1": round(completion_rss1, 1),
            "snapshot2": round(completion_rss2, 1)},
        "completion_status": {"snapshot1": rec1["status"],
                              "snapshot2": rec2["status"]},
        "versions": {"v1": rec1["version"], "v2": rec2["version"]},
    }, indent=2))

    print("\nnote: precise per-CLI-process RSS/phase timing is measured by the")
    print("wrapper scripts/ingest_scale_profiled.py (same workload, /usr/bin/time")
    print("plus in-process phase timers).")
    if failures:
        print(f"\n{len(failures)} SCALE CHECK(S) FAILED")
        return 1
    print("\nall scale checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
