# Declarative version-bound queries (`ws query`)

The workspace already keeps every source correction as a numbered completion
version with full provenance.  `ws query` answers everyday questions against
**one completed version** — for example "which orders are certainly for an
east-region customer?" — using a restricted, declarative JSON query language.
There is no SQL, no Python expression, no negation, no aggregation and no user
code: only joins, equality, typed constants, projection and set de-duplication.

## Query language

A query is a JSON object with exactly three fields:

```json
{
  "relations": [
    {"name": "Order", "alias": "o"},
    {"name": "Customer", "alias": "c"}
  ],
  "select": [
    {"column": "o.c0", "name": "order_id"},
    {"column": "c.c1", "name": "region"}
  ],
  "where": [
    {"eq": ["o.c1", "c.c0"]},
    {"eq": ["c.c1", {"type": "text", "value": "东区"}]}
  ]
}
```

* **`relations`** — the participating relations, each with a unique `alias`
  (the same relation may appear twice for a self-join).
* **`select`** — projected `alias.column` references with an output `name`.
  Columns are the schema's symbolic names when declared, else the positional
  names `c0`, `c1`, ... .
* **`where`** — equality conditions only.  Each `eq` is either two column
  references (`["o.c1", "c.c0"]`) or a column and a typed constant
  (`["c.c1", {"type": "text", "value": "东区"}]`).  Constants support
  `"text"` (JSON string) and `"int"` (JSON integer).

Everything else is rejected at parse time — unknown fields, unknown relations
or aliases or columns, duplicate aliases/output names, cross-domain equality,
booleans/floats/JSON-null constants, operators other than `eq`, constant-to-
constant conditions, and empty relation/select lists.  A rejected query never
runs and never writes an output.

## Certainty semantics for `labeled_null`

A `labeled_null` means the source has not supplied a value.  It is **not** a
customer literally named `n0`, and it is never treated as an arbitrary known
value.  The query semantics make this precise:

* Equality is on typed identity: `("s", text)`, `("i", int)`, `("n", id)`.
  The text `"7"`, the int `7` and an unknown are three different values and
  never compare equal.
* **Unknowns may participate in joins.**  Two columns compare equal on an
  unknown only when they hold the *same* unknown id; two different unknown ids
  never compare equal, however similar their display names.  The same unknown
  recurring in different relations therefore joins correctly.
* **An unknown never equals a supplied text/int constant.**  A column/constant
  condition simply fails for any row whose column is unknown.
* A matched tuple is emitted **only when every projected column is a known
  constant**.  Unknowns therefore never leak into output as ordinary text.
* Output is the set of **distinct** fully-known tuples in a stable order.

An empty result means *no fully certain answer in this version under these
bounds* — it does **not** mean "no matching row exists in reality".  Every
result envelope repeats this boundary in its `semantics_note` so callers do
not misread "currently undetermined" as "known absent".

Queries run over the version's **completed** relation facts (rule-derived rows
included), never over raw sources alone — that is what lets a question about a
derived `Customer` region be answered from the workspace's rule completion.

## Version binding

Every query is bound to exactly one version, named by `--version`:

* an integer version (`--version 2`), or
* the one-shot token `--version latest-complete`.

Only versions whose completion status is `complete` are queryable.  When the
newest source version is `conflict` or `incomplete`, it is neither mixed into
the data nor disguised as the latest successful version: `latest-complete`
resolves to the last complete version, and the envelope names both
`latest_source_version_at_bind` and `latest_complete_version_at_bind`.
Querying a non-complete version is refused with an error.

The version is resolved **inside a deferred SQLite read transaction** that
stays open for the whole query, pinning one snapshot.  If another process
commits a new version while a multi-relation query is running, that query
still reads every relation from the single version it began with; the new
version is only visible to a later query.  A query never changes the
workspace, its version pointers or any batch receipt.

The envelope reports the bound source version, the completion status, a
canonical query summary (including a `query_digest`) and the results, so every
answer can be traced back to the exact version it came from.

## CLI

```bash
python3 -I run_cli.py ws query WS --query Q.json --version latest-complete
python3 -I run_cli.py ws query WS --query Q.json --version 2 -o result.json
python3 -I run_cli.py ws query WS --query Q.json --version 2 \
    --max-rows 100            # cap output; marks the result truncated
```

Options:

| flag | meaning |
|---|---|
| `--version N\|latest-complete` | required; the one version to bind |
| `-o, --output FILE` | write the full envelope to a file (atomic) |
| `--force` | replace an existing output file |
| `--max-rows R` | cap result rows; reaching it marks `truncated` |
| `--max-seconds S` | wall-clock cap for result fetching |
| `--batch-size N` | rows per SQLite fetch/insert batch (default 1000) |

Exit codes: `0` complete result, `5` query/usage rejected, `8` the query
succeeded but the result is `truncated` by a row/time cap.  A truncated result
is never labeled complete.

### Output safety and determinism

With `-o`, the envelope is built in a hidden temporary file in the target
directory and only `os.replace`d into place after the whole query succeeds.
A failure, rejection or interruption therefore never overwrites an existing
successful output, and an existing file is never replaced without `--force`.
For a fixed (workspace version, query), the `results`, bound version and
query digest are byte-stable; only the timing fields (`elapsed_seconds`,
`phase_seconds`) vary between runs.

## Storage model and bounded resources

The pinned version's compressed fact payload is decompressed to a private
scratch directory and streamed through a rolling-buffer parser into a
temporary on-disk SQLite database:

* neither the whole payload nor any full join result is held in Python
  memory — rows are inserted and fetched in bounded batches;
* the query compiles to one parameterized SQLite statement that uses the
  store's per-column indexes for the joins;
* projection joins to the value dictionary and filters to known kinds, so an
  unknown can never reach an output column;
* the scratch database and temp directory are removed when the query ends.

The envelope's `plan` field carries the SQLite query plan (each index used),
`facts_loaded`/`total_facts_loaded` the real read scope, and
`phase_seconds`/`scratch_db_bytes` the cost profile.

## Runnable example and measured scale

* `bash examples/query/demo.sh` builds a hand-crafted workspace with known
  regions, repeated input unknowns across relations and text `"7"` / int `7`
  homographs, produces complete/conflict/incomplete versions, and exercises
  every semantic case including the rejection paths.
* `python3 -I scripts/wsquery_scale.py --orders 60000 --customers 12000`
  generates a **144 000-fact** version at runtime (nothing large is
  committed to the repo), runs a selective three-relation join, independently
  recomputes the expected answer with a separate plain-Python evaluator, and
  reports read scope, timings and peak RSS.  Recorded on this machine
  (Python 3.11, SQLite 3.40): 144 000 facts streamed and indexed, the join
  fetch itself ~0.05 s, query-only peak RSS ≈ 63 MB, scratch DB ≈ 12 MB,
  with the independent row count and sampled rows matching exactly.
