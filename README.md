# Typed relational completion

Reads typed facts and a declarative set of TGDs/EGDs, and produces a package
that is a **constraint-satisfying completion** of the input: derived rows where
the dependencies demand them, explicit `labeled_null` placeholders where a value
is unknown, and a hard failure (`conflict`) when two distinct known constants
would have to be equal.

No default values are invented; unknowns are never dressed up as source facts.

## Quick start

```bash
python3 -m chase check    examples/orders_known.json        # validate the program
python3 -m chase complete examples/orders_known.json -o /tmp/pkg
python3 -m chase verify   /tmp/pkg                          # independent check
python3 -m chase trace    /tmp/pkg --null n0                # where did n0 come from?
```

Exit codes: `0` ok / complete / verified, `2` conflict (or program rejected),
`3` incomplete or error, `4` verification failed, `5` usage/input error.

## Input format

```json
{
  "schema": {
    "Order":         ["text", "text"],
    "Customer":      ["text", "text"],
    "KnownAddress":  ["text", "text"]
  },
  "rules": [
    {"id": "t_order_customer", "body": ["Order(o, c)"], "head": "Customer(c, a)"},
    {"id": "t_known_customer", "body": ["KnownAddress(c, a)"], "head": "Customer(c, a)"},
    {"id": "e_customer_key",   "body": ["Customer(c, a1)", "Customer(c, a2)"],
     "eq": ["a1", "a2"]}
  ],
  "facts": {
    "Order":        [["o1", "c"], ["o2", "c"]],
    "KnownAddress": [["c", "城东"]]
  },
  "budgets": {"max_facts": 500000, "max_triggers": 200000,
              "max_probes": 5000000, "max_seconds": 120,
              "max_match_rows": 200000, "max_rounds": 10000}
}
```

* **Relations** are typed `text`/`int`.  Facts are a **set** (duplicates drop).
* **Unknowns already present in the input** are written `{"null": "u1"}`.
  All occurrences of `u1` are one unknown; different ids are different.
* **TGD**: `head` is one atom; head variables not bound by the body are
  existential.  Constants in heads are allowed; variables must be consistently
  typed.
* **EGD**: `eq` names two terms bound by the body.
* Bodies may self-join, repeat variables and use constants.
* Negation, arithmetic and user code are rejected at parse time; rules must
  have fixed unique ids and may not mention undeclared relations.

## What is in a package

```
pkg/
  package.json      status marker ("complete"/"conflict"/"incomplete"/"error"),
                    budgets, input facts, stats — written LAST, atomically
  schema.json       relation -> column types
  rules.json        normalized rules + weak-acyclicity annotations
  facts.csv         relation,values  where values are
                    {"kind":"string"|"int"|"labeled_null", ...}
  nulls.json        every unknown: input or rule-created, final class,
                    unified_with if absorbed by a constant
  conflict.json     only for conflicts: binding, both constants, support rows,
                    source paths and merge history
  provenance.sqlite triggers, unifications, per-fact origins (trace backend)
```

`facts.csv` distinguishes:

| value | JSON |
|---|---|
| source string | `{"kind": "string", "value": "城东"}` |
| source int | `{"kind": "int", "value": 42}` |
| **unknown placeholder** | `{"kind": "labeled_null", "id": "n0", "display": "NULL:n0"}` |
| missing field | row absent (there is no "null column"; relations are typed) |

JSON `null` is never a fact value: the loader rejects it.  An unknown is not a
value "read from the source" — it carries its own id and provenance.

## Semantics in one page

* Standard chase: a TGD trigger whose head already has a witness is satisfied
  and creates nothing; otherwise one fresh unknown per existential **of that
  firing** is created, shared across its head occurrences.
* EGDs merge unknowns into classes; an unknown merged with a source constant
  *becomes* that constant; two distinct source constants are a **conflict**,
  reported with binding, constants and evidence.
* After each unification round every stored row is rewritten through the
  canonical map, duplicates collapse, and changed rows are re-chased, so joins
  that appear only after unification are still explored.
* Completion means: a TGD pass adds no row and an EGD pass merges nothing.
* TGDs must be weakly acyclic (see `docs/DESIGN.md`); this is checked on the
  rules alone, never inferred from the sample terminating.  Budgets make an
  over-long run `incomplete`, not `complete`.

## Verification

`verify` reloads the package from disk and checks, from scratch and without
trusting `package.json`: each TGD has a head witness for every body binding,
each EGD holds, every input constant fact is preserved, values are type-correct,
and unknown bookkeeping is consistent.  It shares only the parser with the
engine.

## Correction workspace (sources that change over time)

`complete` handles one input.  When a supplier retracts misreported records
and re-sends corrected ones, use the versioned workspace instead — it keeps
the raw source records of every version and re-decides each version's result
from those raw sources, so unknowns merged by an EGD can split apart again
and derivations that lost their last support disappear:

```bash
python3 -m chase ws init     /tmp/ws --program examples/corrections/program.json
python3 -m chase ws commit   /tmp/ws --batch examples/corrections/batch1.json
python3 -m chase ws commit   /tmp/ws --batch examples/corrections/batch2.json
python3 -m chase ws versions /tmp/ws
python3 -m chase ws diff     /tmp/ws --from 1 --to 2 --human
python3 -m chase ws export   /tmp/ws --version 2 -o /tmp/ws-v2
python3 -m chase ws verify   /tmp/ws-v2
```

Source records are keyed `(source_id, record_id)`; batches (`put`/`delete`
ops under a unique `batch_id`) are validated and applied as a whole, every
legal batch becomes a numbered source version (`complete`/`conflict`/
`incomplete` results are all kept), re-sending a batch replays its original
receipt, and `latest source version` vs `latest complete version` are
exposed separately.  State lives in one SQLite file; commits are single
transactions; concurrent writers get a clear stale-parent error.  See
`docs/WORKSPACE.md` and `bash examples/corrections/demo.sh`.


## CSV snapshot ingestion (supplier full-table feeds)

When a supplier sends a **full table snapshot** as CSV shards instead of
hand-written `put`/`delete` ops, use the two-phase ingestion group.  `ingest
stage` fully parses and strictly converts every shard, reconciles new /
replaced / kept / quarantined / retracted source records without advancing a
source version, and persists the typed snapshot so deleting the original CSV
later changes nothing; `ingest apply` then writes one ordinary source batch
against the exact parent version checked at stage time.  A bad row is
quarantined (its key still counts as present) and can never be mistaken for a
retraction; only a key absent from a complete, unblocked snapshot is deleted.

```bash
python3 -I run_cli.py ingest stage   WS --declaration decl.json --task T1
python3 -I run_cli.py ingest changes WS --task T1      # per-key why-put/delete/keep
python3 -I run_cli.py ingest explain WS --task T1 --source erp --record-key '...'
python3 -I run_cli.py ingest apply   WS --task T1
```

Connectors own only the sources they claim, cannot change their binding
silently, detect external `ws commit` touches (including same-value
replacements) and stale parents, and survive hard process exits.  See
`docs/INGEST.md`, `examples/ingest/`, `scripts/ingest_demo.py` and the
100 000-row measured run `scripts/ingest_scale_profiled.py`.

## Declarative queries over one completed version (`ws query`)

`ws query` runs a restricted, declarative **JSON** query (multi-relation
joins, column/column and column/typed-constant equality, projection and set
de-duplication — no SQL, no Python, no negation, aggregation or user code)
against exactly one `complete` version, bound by an integer or the one-shot
`latest-complete` token.  A conflict/incomplete newest source version is never
mixed in or disguised as the latest success.  The version is pinned inside one
read transaction, so a commit landing during a multi-relation query cannot make
it read two versions.

`labeled_null` certainty rules are enforced end to end: unknowns can join on
shared identity (same id across relations) but never equal a supplied constant
and never reach an output column; only distinct tuples whose projections are
all known constants are returned.  The text `"7"`, the int `7` and an unknown
stay distinct.  An empty/truncated result means "no fully certain answer under
these bounds", not "nothing exists in reality".

```bash
python3 -I run_cli.py ws query WS --query Q.json --version latest-complete
python3 -I run_cli.py ws query WS --query Q.json --version 2 -o result.json
```

Execution uses a temporary on-disk SQLite database and indexes, streaming the
pinned version's facts in bounded batches (no whole relation or full join
result in Python memory).  Output files are published atomically, so a failed
or interrupted run never overwrites a previous success.  See `docs/QUERY.md`,
`bash examples/query/demo.sh` and the 144 000-fact measured run
`python3 -I scripts/wsquery_scale.py` (data generated at runtime, not
committed).

## Development

```bash
python3 run_tests.py            # test suite, no external deps required
python3 -m chase.bench --orders 50000 --customers 10000 --no-verify
python3 -m chase.wsbench --workspace /tmp/wsb --orders 10000 --customers 2000 --corrections 8
```

Recorded on this machine (Python 3.11, SQLite 3.40):

| workload | facts in | facts out | triggers | unifications | seconds | peak RSS |
|---|---|---|---|---|---|---|
| 5 000 orders / 1 000 customers | 5 537 | 6 534 | 1 531 fired / 4 006 skipped | 534 | 0.43 | 23 MB |
| 20 000 orders / 4 000 customers | 22 031 | 26 018 | 6 003 fired / 16 028 skipped | 2 016 | 1.9 | 40 MB |
| 50 000 orders / 10 000 customers | 55 030 | 64 985 | 14 952 fired / 40 078 skipped | 4 997 | 4.9 | 72 MB |

The 50 000-row run plus its independent package verification takes 7.9 s
end-to-end (`python3 -m chase.bench --orders 50000 --customers 10000`).

The whole instance lives on disk in SQLite; only one body atom's matches are
materialized at a time (bounded by `max_match_rows`), and the pending trigger
queue is a disk table.

See `docs/DESIGN.md` for termination, the unification/rebuild argument, and the
correctness tests.
