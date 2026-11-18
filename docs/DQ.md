# Data-quality audits (`ws dq`)

`ws dq` is the persistent quality-audit capability of the correction
workspace.  It answers the integrator's question *before delivery* — "does
this version of the order/customer data satisfy our agreed business policy?"
— and *afterwards* — "which rule, which record and which data version caused
this block?".

It is deliberately separate from the three other kinds of verdict the
workspace already produces:

| capability | question answered | scope |
|---|---|---|
| completion status (`ws commit`/`ws versions`) | do the TGDs/EGDs admit a model of this version? | constraints, all rows |
| source diff (`ws diff`) | what changed between two versions? | raw record delta |
| declarative query (`ws query`) | which certain tuples satisfy a query? | one complete version |
| **quality audit (`ws dq`)** | **does the data satisfy the named business policy, and which record proves the finding?** | a business policy, per rule |

The three first are engine/version facts; the fourth is a *policy* verdict.
A version can be `conflict` (completion) and still pass a source-layer audit,
and a `complete` version can fail the policy.  Neither implies the other.

## The policy (restricted JSON, no code)

```json
{
  "format": "data-quality-policy/1",
  "policy_id": "dq.orders_customers",
  "revision": 1,
  "description": "delivery gate for order/customer data",
  "sample_limit": 5,
  "indeterminate_fails": false,
  "rules": [
    {"id": "req-customer-region", "kind": "required", "layer": "completed",
     "relation": "Customer", "column": "c1", "severity": "error"},
    {"id": "enum-customer-region", "kind": "enum", "layer": "completed",
     "relation": "Customer", "column": "c1", "severity": "error",
     "allowed": [{"type": "text", "value": "东区"},
                 {"type": "text", "value": "西区"}]},
    {"id": "uniq-order-source-key", "kind": "unique", "layer": "sources",
     "relation": "Order", "columns": ["c0"], "severity": "error"},
    {"id": "ref-order-customer", "kind": "reference", "layer": "sources",
     "relation": "Order", "columns": ["c1"],
     "target": {"relation": "Customer", "columns": ["c0"]},
     "severity": "error"}
  ]
}
```

* `policy_id` + `revision` identify the policy; content equality is decided
  on a canonical form (rule order, column spelling, allowed-constant order
  are irrelevant) and its SHA-256 is the **policy digest**.
* Every rule **must** state a `layer` (`sources` or `completed`), a `kind`, a
  `relation`, a stable `id` and a `severity` (`error`/`warning`/`info`).
* Columns are the schema's symbolic names or the positional `c0`, `c1`, ...
  form; both are normalized to the symbolic name in the report.
* `sample_limit` (0–1000) bounds samples per rule; the CLI `--sample-limit`
  overrides it.
* `indeterminate_fails` decides whether "cannot be judged" rows make the
  overall status `fail` instead of `indeterminate`.  It can never turn an
  unknown into a pass.

The parser rejects unknown fields, unknown relations/columns, duplicate rule
ids, duplicate allowed values, cross-domain reference keys, enum constants of
the wrong column type, and a `unique` rule on `completed`.  There is no
place for SQL, expressions, regular expressions or user code.  A rejected
policy never produces a report and never persists a run.

## Checks

**`required`** — the column must hold a known `text` or `int` value.
A `labeled_null` is an unknown value: it is a **violation** and is never
written as, or compared with, a string such as `"n0"`.

**`unique`** — typed uniqueness over the **raw source records**.  It is a
source-layer-only check because completed facts are a set: a completion
collapses two identical records into one fact, which would hide exactly the
duplicate key this check exists to find.  Two different source records are
two different inputs.  The text `"7"`, the int `7` and a labeled null (by
its id) are three different key values.  Every member of a duplicated group
counts as a violation, so a single de-duplicated sample cannot hide parallel
offending rows.

**`enum`** — only an explicitly listed constant of the matching type passes.
A known constant that is not listed is a violation; a `labeled_null` is
**indeterminate** — not a pass.  The constant type is checked against the
column type at policy validation time.

**`reference`** — a key on the left relation must match a row in the target
relation.  A fully constant key with no match is a violation; a key
containing any labeled null is indeterminate.  The target side is read from
the completion of the pinned version (rule-derived facts included).  For a
source-layer rule the target comes from the completion of the *same* source
version, so the report states whether the matched fact is a source input or
rule-derived, and for a derived fact it carries the rule id, trigger key,
binding, head fact and supporting facts — plus a `provenance_ref` (version
and SHA-256 of the decompressed provenance database) that can be re-checked
from the workspace.

## Layers and versions

* `sources` reviews the raw records retained by a **source version**.  It
  runs even when that version's completion is `conflict` or `incomplete`,
  and the report always states that completion status — it never substitutes
  an older `complete` version.
* `completed` reads only the facts of a **complete** version (derived facts
  included); a conflict/incomplete version is rejected.
* A layer spec is an integer or `latest`; each layer resolves its own
  `latest` (`latest source version` / `latest complete version`).
* Both versions are resolved **once**, inside one SQLite read transaction
  that stays open until the whole audit ends.  Version rows, raw sources,
  completed payload, provenance and the fact→record manifest therefore come
  from one snapshot: a commit landing mid-audit cannot make a report mix two
  versions.  `latest` means "latest as pinned when the audit started".

## Report and identity

The report records:

* `report_id` — SHA-256 of the policy digest + the resolved version
  coordinates + the effective sample limit + `indeterminate_fails`.  It is
  stable and independent of wall-clock time, run number and workspace path.
  Operational measurements (`phase_seconds`, `scan_summary.peak_rss_mb`,
  per-rule `elapsed_seconds`, `run_id`) are **not** part of it.  A separate
  `report_sha256` covers the whole emitted document, so any operational
  measurement will change it while `report_id`, counts and samples stay the
  same — that is the field to compare for "same verdict, same evidence".
* the `policy` block (id, revision, digest, canonical form) and the
  `versions` block (per layer: requested spec, version, batch id, parent,
  completion status) — the "which data version" answer;
* per rule: `counts` (`pass`, `violation`, `indeterminate`, `total`), status,
  severity, columns, `sample_omitted`, `samples_truncated`, the scan range
  and the SQLite `query_plan`, and the bounded samples;
* per reference rule: `matched` evidence (which target fact satisfied each
  sampled key and where it came from);
* overall `status`: `fail` if any rule fails (or has indeterminate rows when
  `indeterminate_fails` is set), else `indeterminate` if any rule has
  indeterminate rows, else `pass`.  A validation error, invalid version or
  interrupted run produces **no** report at all.

Samples are bounded by the policy, deterministically ordered (source id /
record id, or relation value order) and both categories get a slot when a
rule has violations *and* indeterminate rows.  Truncation never changes the
reported `counts`, and `sample_omitted` / `samples_truncated` say so.

Source samples point at `source_id` + `record_id` + typed values; completed
samples point at the fact + provenance.  A reference target that is source
input must cite at least one source record, and a target fact whose origin is
missing from the provenance database is an *evidence error* (the audit fails
and writes no report) rather than a pass with unverifiable evidence.

`sample_limit: 0` means "counts only": no sample rows are materialized for
any rule, `sample_omitted` equals the number of offending rows and
`samples_truncated` is true whenever there were any.

## Persistence and failure safety

Policies and runs live in `<workspace>/dq_audits.sqlite`, a separate
database.  The workspace state is opened read-only (`mode=ro`,
`query_only`); an audit never writes a source batch, never moves a version
pointer and never modifies a completed fact.

* The complete report is inserted in **one** transaction, only after the
  audit succeeded.  A hard exit before it leaves the previous state; after
  it, the new complete run.  There is no half-written run.
* Report files are written to a temporary file in the target directory and
  `os.replace`d only after the whole success.  A failure or interruption
  leaves the previous file untouched; without `--force` an existing file is
  never replaced.
* Re-opening the workspace gives `ws dq runs` (policies + run history) and
  `ws dq show --run N` (the stored report).  Re-running the same policy
  content on the same versions yields the same `report_id`, counts and
  samples; `duplicate_of_run_id` points at the earlier run.

## CLI

```bash
python3 -I run_cli.py ws dq check WS --policy policy.json
python3 -I run_cli.py ws dq run   WS --policy policy.json \
        [--sources-version N|latest] [--completed-version N|latest] \
        [--sample-limit N] [-o report.json] [--force] [--no-persist]
python3 -I run_cli.py ws dq runs  WS [--policy-id ID]
python3 -I run_cli.py ws dq show  WS --run N
```

`ws dq run` exit codes: `0` overall `pass`, `2` overall `fail`,
`3` overall `indeterminate`, `5` usage error / policy rejected / invalid
version / failed run (no report written).

A runnable example lives in `examples/dq/`: `bash examples/dq/demo.sh`.
Scale (100 000+ records, runtime-generated data, independent cross-check and
per-rule scan/plan/timing/RSS) is `python3 -I scripts/dq_scale.py`; use
`--workspace DIR` to keep the generated data and re-run the audit against the
same fixture.

## Measured scale run

`python3 -I scripts/dq_scale.py` (60 000 orders = 63 000 raw order records
because every 20th is reported by a second source, 20 000 customers, 20 000
items) — 103 000 raw source records, 200 000 completed facts, generated at
runtime and never committed:

| rule | layer | total | violation | indeterminate | plan |
|---|---|---|---|---|---|
| enum-item-qty | sources | 20 000 | 0 | 4 000 | covering index `(rel, p2)` |
| enum-region | completed | 40 000 | 0 | 22 000 | covering index `(rel, p1)` |
| ref-item-order | sources | 20 000 | 400 | 0 | `(rel)` + covering target `(rel, p0)` |
| ref-order-customer | sources | 63 000 | 0 | 0 | `(rel)` + covering target `(rel, p0)` |
| req-region | completed | 40 000 | 22 000 | 0 | covering index `(rel)` |
| uniq-order-id | sources | 63 000 | 6 000 | 0 | index `(rel)` + temp B-tree group by |

Totals 246 000 checks: 191 600 pass, 28 400 violation, 26 000 indeterminate.
Duplicate groups: 3 000 groups of size 2, exactly as generated (the completed
Order fact is one row per key).  The independent enumeration in the same
script agrees on every count.  Audit wall time 10.0 s, peak RSS 121 MB
(staging 8.8 s, per-rule checks 0.01–0.4 s each); the 95 KB report holds 10
samples per rule with the true totals and `sample_omitted`.

Implementation notes for that number: the pinned payload and provenance are
inflated from the SQLite blob in bounded chunks straight to disk (never held
as a Python object), the fact→record manifest is read as a stream (atomic
blocks of 2 000–4 000 rows are inserted into the scratch database), and each
check is a single bounded SQL aggregate whose result is only counts plus a
`samples` list capped by the policy.  No relation, violation set or full
report is ever built in a Python list.

## How this differs from the other three capabilities

* **Completion constraints** say whether the TGD/EGD program is satisfied.
  They are about the *engine's* model, not about a business agreement: a
  conflict blocks every query, but a complete package can still violate
  "region must be an allowed value".
* **Source corrections/diffs** say what changed between two versions.  A diff
  is not a verdict; the audit can run the same policy on both versions and
  show that a finding disappeared because a record was corrected.
* **Queries** return certain tuples.  A query answers "which orders are in
  the east region"; an audit answers "which records violate policy X in
  version Y, and why".  Neither replaces the other: an empty query result
  says nothing about missing references, and a passing audit does not prove
  every query answer is complete.

For the integrator: the audit is the gate (pass/fail/indeterminate + record
evidence + version), the completion status is the engine health check, the
diff explains history, and the query serves downstream needs.
