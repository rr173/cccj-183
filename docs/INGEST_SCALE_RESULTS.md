# Measured 100 000-row full-snapshot swap

All inputs are generated at runtime by
`scripts/ingest_scale_profiled.py` (nothing large is committed).  The run
below was produced on this machine (Python 3.11.2, SQLite 3.40.1) with

```bash
python3 -I scripts/ingest_scale_profiled.py --rows 100000 --shards 4
```

The script independently recomputes every expected count and sampled tuple
from the generated inputs and the prior ledger — it never calls production
reconciliation code to build expectations.

## Workload

* snapshot 1: 100 000 `Item` source records split over four CSV shards
  (~3.9 MB CSV), staged and applied as source version 1;
* snapshot 2: a full re-send containing 5 000 brand-new rows, 8 000 legal
  replacements, 4 000 retracted rows and 88 000 unchanged rows (105 000
  candidate rows reconciled), staged and applied as source version 2.

## Independently checked counts (all matched)

| quantity | expected | produced |
|---|---|---|
| snapshot 2 puts (new + replace) | 13 000 | 13 000 |
| snapshot 2 deletes (retract) | 4 000 | 4 000 |
| snapshot 2 keeps | 88 000 | 88 000 |
| final live source records | 101 000 | 101 000 |

Sampled replacement, quarantined-retention, retraction, new-row and
unchanged-row tuples were each checked against the live workspace separately.

## Phase timing and peak RSS (single process, in-phase measurement)

| phase | seconds | RSS at start | RSS at end |
|---|---|---|---|
| ingest + reconcile, snapshot 1 | 8.35 | 33.2 MB | **35.2 MB** |
| apply + chase completion, snapshot 1 | 43.45 | 35.2 MB | 580.8 MB |
| ingest + reconcile, snapshot 2 | 9.32 | 580.8 MB | 580.8 MB (no growth) |
| apply + chase completion, snapshot 2 | 38.76 | 580.8 MB | 580.8 MB |
| reconcile candidate read-back (105 000 rows) | 0.007 | — | — |

Resulting SQLite database: ~232 MB (typed rows, per-row evidence,
candidates, baselines, blocking diagnostics and the two version payloads).

## Reading the numbers honestly

* **Ingestion is streaming and bounded.**  Parsing, strictly converting and
  reconciling 100 000 rows across four shards raises peak RSS by only about
  2 MB (33.2 → 35.2 MB).  CSV text, the full key set and row diagnostics are
  never accumulated in Python; rows flow one logical record at a time into
  SQLite, and reconciliation runs as index-backed `GROUP BY` / `NOT EXISTS`
  set operations over bounded cursors.
* **The completion engine is the separate, larger cost.**  The existing
  chase engine's full re-solve peaks around 580 MB and dominates wall time;
  that is its pre-existing, documented behavior and is reported on its own
  line so it cannot mask the ingest profile.
* Snapshot 2's ingest phase shows **no RSS growth** even though it reconciles
  more rows than snapshot 1 — the working set stays in SQLite, not in the
  Python heap.

Re-run anytime; the generator is deterministic in shape and the independent
cross-check will fail the script (non-zero exit) if any count drifts.

## Subprocess CLI cross-check (`scripts/ingest_scale.py`)

The second runner drives the real CLI in separate processes and recomputes
expectations without importing any production reconciliation code.  At the
same 100 000-row / four-shard size, its independently checked totals were:

* puts 10 000 (5 000 new + 5 000 replace), deletes 3 000, quarantined 2 000
  (old tuples retained), keeps 90 000, final live records 102 000 — every
  sampled tuple matched the independently computed expectation;
* ingest stage wall time 8.6 s / 9.9 s; the completion engine reported
  ~290–351 MB peak RSS while the outer driver process stayed at ~98 MB (its
  large child processes hold the engine, not the ingest working set).
