# CSV snapshot ingestion into the completion workspace

Suppliers send **full table snapshots** as a set of CSV shards, not hand-edited
`put`/`delete` batches.  The `ingest` command group turns such a feed into the
same source records the existing `ws commit` flow already understands, using a
two-phase **stage then explicitly apply** workflow.  The completion algorithm
is unchanged; ingestion only handles table-to-source-record conversion and
reconciliation.

The danger this is built against is **silent deletion**: a single bad supplier
row must never be skipped and then misread as "the supplier retracted that
record".  Deletions are therefore derived only from a *complete, unblocked*
snapshot in which a managed key genuinely never appears.

## Declaration

One JSON document describes a reusable connector.  Configuration is closed:
every key must be one of the documented keys, so a misspelled option is a hard
error rather than a silently ignored one.

```json
{
  "connector": "supplier-alpha",
  "version": 1,
  "unknown_markers": {
    "marker": "NULL",
    "name_prefix": "NULL:",
    "name_separator": "|",
    "name_pattern": "[a-zA-Z][a-zA-Z0-9_]*"
  },
  "sources": [
    {
      "source_id": "erp",
      "relation": "Order",
      "key_columns": ["order_id"],
      "fields": [
        {"name": "order_id", "header": "order_id"},
        {"name": "qty", "header": "qty"},
        {"name": "customer_id", "header": "customer_id"}
      ],
      "shards": [
        {"path": "/data/erp_snapshot_part1.csv"},
        {"path": "/data/erp_snapshot_part2.csv"}
      ],
      "empty_snapshot": false
    }
  ]
}
```

* **connector** — a stable identity.  Its bound relations, key columns, field
  mappings and unknown interpretation can never change silently; a changed
  binding is rejected and asks for a new connector name (no config migration).
* **sources[].source_id** — the managed workspace source.  One source maps to
  exactly one relation; one declaration may manage several sources/relations.
  A source never listed in the declaration is never touched.
* **key_columns** — named schema columns composing the record key.  Key
  columns cannot be unknown.
* **fields** — every schema column mapped to exactly one CSV header.
* **shards** — the CSV files whose **union** is one complete snapshot.  A
  snapshot cannot be judged for missing records until every shard is read.
* **empty_snapshot** — documents a deliberate full clear (header-only
  shards).  The apply-time confirmation gate is also enforced from the actual
  deletion effect, so the flag cannot be relied on as the only safety net.
* `comment` is allowed at every level and is never treated as content.

## CSV format

Fixed: UTF-8 (one optional leading BOM), comma delimiters, double-quote
escaping (`""` for a literal quote), CRLF or LF, and no requirement for a
trailing newline.  Quoted fields may contain commas and newlines, so records
are parsed by a character-level state machine — **never a physical-line
split**.  Every logical record keeps its sequence number and physical line
span for locating bad rows.  Duplicate headers, missing mapped columns, field
count mismatches and broken quote structure all produce explicit, located
diagnostics.

## Strict conversion (no loose coercion)

* **text** is preserved verbatim.  An empty string `""` and a whitespace-only
  string are ordinary text values, never "missing".
* **int** accepts only an optional sign followed by ASCII digits
  (`[+-]?[0-9]+`).  Decimals, exponents, internal/outer whitespace and
  non-ASCII digits are rejected.  `"07"` and `"+7"` normalize to the same
  integer 7.
* Only the explicitly configured marker spells an unknown.  An anonymous cell
  equals `marker`; a prefixed cell `NULL:u` carries local unknown name `u`;
  `NULL:u|x` is unknown `u` plus a text suffix (text columns only).  An
  optional `name_pattern` regex lets internal-looking strings such as
  `NULL:n0` stay ordinary text.
* The same local unknown name maps to one input unknown **within a source_id**;
  identical names in different source_ids stay isolated.
* Composite keys use a typed, unambiguous JSON encoding — no separator
  concatenation — so the text `"7"` and the integer 7 can never collide.

## Stage, inspect, apply

```bash
python3 -I run_cli.py ingest stage   WS --declaration decl.json --task T1
python3 -I run_cli.py ingest report  WS --task T1
python3 -I run_cli.py ingest changes WS --task T1                 # all candidates
python3 -I run_cli.py ingest changes WS --task T1 --decision put
python3 -I run_cli.py ingest explain WS --task T1 --source erp \
        --record-key '[["s","o1"]]'
python3 -I run_cli.py ingest rows     WS --task T1                 # per-row diagnostics
python3 -I run_cli.py ingest unknowns WS --task T1
python3 -I run_cli.py ingest apply    WS --task T1 [--confirm-empty]
```

`stage` fully parses and converts every shard, reconciles against the current
managed-source ledger, and persists the task, typed rows, per-row diagnostics,
candidate operations, statistics, input file hashes and the declaration
identity **in the workspace SQLite database**.  It never advances a source
version and never reads a completed/normalized fact back as a source.  After
staging, deleting or replacing the original CSV files changes nothing: apply
uses the persisted typed snapshot.

Each candidate carries a reason (`put` — new or replaced, `delete` — absent
from a complete snapshot, `keep` — identical, `quarantine` — bad non-key
fields) and the full list of input rows (shard, logical sequence, physical
lines) supporting the key.

## Reconciliation and quarantine rules

* A row whose **key solves** but whose other fields are illegal is
  **quarantined**: the key counts as having appeared.  If the key existed, its
  old tuple is retained untouched; if not, nothing is inserted.  It never
  produces a put or a delete.
* A row whose **key cannot be trusted** (unconvertible key, field-count
  mismatch, unreadable or structurally broken CSV) makes that source's
  snapshot **non-integral**.  The whole task cannot apply, no deletions are
  derived, but every other file is still checked so all problems can be fixed
  in one pass.
* Identical duplicates across shards collapse to one record, but every row
  stays as evidence.  Distinct legal tuples for one key, or valid and invalid
  rows sharing a key, block the whole task — no traversal-order winner.
* Only a managed key completely absent from an integral snapshot becomes a
  retraction (`delete`).
* A header-only file is a legal empty shard.  A full clear needs explicit
  confirmation.  The gate is **effect-based**, not just flag-based: any
  integral zero-data-row snapshot that would actually delete existing managed
  records is held as `needs_empty_confirm` and only applies with
  `--confirm-empty` — even if `empty_snapshot` was left off — so a header-only
  feed can never silently retract a whole source.  (An empty snapshot of a
  source that holds nothing yet deletes nothing and stays applicable.)

## Claims, external touches and staleness

* A connector's first application may only claim a source that has no records
  and no other connector's claim.  A source already holding manual or
  other-connector records cannot be claimed, overwritten or cleaned up.
* On every later apply, the live workspace state is compared against the
  **baseline captured at stage time**, including the workspace's
  `since_version` marker.  A same-value replacement is still an external touch
  and blocks apply; the integrator re-checks with a fresh task rather than
  having one silently re-based.
* Apply targets the exact parent source version recorded at stage time.  A new
  version committed meanwhile makes apply report a stale parent; the task
  stays inspectable and is never re-reconciled in place.
* Re-sending a task id with the same content is idempotent; the same id with
  different content is rejected.  Re-applying an applied task returns the same
  version/receipt without creating another version — even an empty candidate
  set produces exactly one traceable batch per explicit apply.
* A completion `conflict` or `incomplete` result is still a successful
  application: one source version and one receipt are kept and never replayed.

## Crash safety

The connector ledger, claims and task markers live in the same SQLite database
as the workspace.  Ledger/claim writes run inside the workspace commit
transaction, so a hard process exit leaves either the pre-commit or the fully
committed state.  A task whose marker flip was interrupted is reconciled to the
committed `ingest:<task_id>` batch on the next open.

## Demo and measured scale run

```bash
python3 -I scripts/ingest_demo.py            # full independently-checked story
python3 -I scripts/ingest_scale_profiled.py --rows 100000 --shards 4
```

The scale run generates all inputs at runtime, independently recomputes
new/replace/quarantine/retract counts and sampled tuples, and reports the
ingest, reconcile and completion phases separately.  Measured on this machine
at 100 000 rows over four shards (see
`docs/INGEST_SCALE_RESULTS.md` for the full table):

| phase | seconds | peak RSS |
|---|---|---|
| ingest + reconcile (snapshot 1) | 8.35 | 33.2 → 35.2 MB |
| apply + chase completion | 43.4 | up to 580.8 MB |
| ingest + reconcile (snapshot 2, 105k rows) | 9.32 | no RSS growth |

The streaming ingest layer raises RSS by ~2 MB while handling 100 000 rows:
CSV text, the full key set and row diagnostics never accumulate in the Python
heap — rows flow one logical record at a time into SQLite, evidence is
aggregated set-side with `json_group_array`, and reconciliation uses
index-backed `GROUP BY` / `NOT EXISTS` over bounded cursors.  The existing
chase engine's full re-solve is the separate, larger cost and is reported on
its own line so it can never mask the ingest profile.
