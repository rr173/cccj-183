# Correction workspace — versioned relational completion over changing sources

The one-shot `chase complete` turns **one** input into **one** package.  Real
feeds are not one-shot: a supplier completes missing exchange packages, then
*retracts* the misreported records and *replaces* them with corrected ones.
If an integrator takes the last completion result and deletes rows from it,
unknowns that were merged by an EGD can no longer be separated, and
derivations that lost their source support survive as orphans.

The workspace (`python -m chase ws ...`) is the durable answer: it keeps the
**raw source records** of every version, re-decides every version's result
**from those raw sources** (never from a previous quotient), and hands out
exports that an independent verifier can re-check without the workspace.

* fixed schema + TGD/EGD program, same rule language and unknown semantics
  as the one-shot tool;
* persistent state in one SQLite file — close the process, reopen any time;
* every first-seen legal batch becomes a monotonically numbered **source
  version** with a `complete` / `conflict` / `incomplete` result;
* two pointers are exposed separately: **latest source version** and
  **latest complete version** — a conflict or budget-exhausted version moves
  the first, never the second, and never blocks later corrections;
* every commit is one SQLite transaction: after a crash you see the state
  before *or* after the commit, never sources without the matching result.

## Quick start (runnable)

```bash
python3 -m chase ws init     /tmp/ws --program examples/corrections/program.json
python3 -m chase ws commit   /tmp/ws --batch examples/corrections/batch1.json
python3 -m chase ws commit   /tmp/ws --batch examples/corrections/batch2.json
python3 -m chase ws versions /tmp/ws
python3 -m chase ws diff     /tmp/ws --from 1 --to 2 --human
python3 -m chase ws export   /tmp/ws --version 2 -o /tmp/ws-v2
python3 -m chase ws verify   /tmp/ws-v2          # source-chain + instance checks
python3 -m chase verify      /tmp/ws-v2          # stock package check still works
```

`bash examples/corrections/demo.sh` runs the whole five-batch story below.
Exit codes of `ws` commands: `0` ok / committed a complete version /
verified, `2` committed a **conflict** version (the commit succeeded!),
`3` committed an **incomplete** version or internal error, `4` verification
failed, `5` usage error / batch rejected as illegal, `6` stale parent
version, `7` batch id re-used with different content.

## Sources, records, batches

* A **source record** is keyed by `(source_id, record_id)` and stores a
  relation name plus its **raw typed tuple**.  The tuple is never rewritten
  by unification — unification happens inside one version's completion run
  only.  Two records may carry the same tuple; that is *record duplication*,
  and it is different from the relation-level fact set, which de-duplicates.
  Deleting one of two records supporting a fact never deletes the fact.
* An explicit input unknown `{"null": "u1"}` names **one original unknown
  across the whole workspace**: two records from different sources carrying
  `u1` refer to the same unknown.  The string `"u1"` is an ordinary constant
  and never the same thing as the unknown `u1`.
* A **batch** is `{"batch_id": ..., "ops": [...]}` with optional `parent`
  and `budgets`.  An operation is
  `{"op": "put", "source_id", "record_id", "relation", "tuple"}` (insert or
  replace — `put` on an existing key replaces its tuple) or
  `{"op": "delete", "source_id", "record_id"}` (the record must exist).
  One batch may touch each record key at most once.
* The whole batch is validated, applied to a **candidate source set**, and
  only then is the completion run.  No business conflict is ever reported
  against a per-operation intermediate state: a batch that deletes the old
  address and puts a conflicting-looking new one in the same batch is legal
  exactly because the net source set is consistent.
* Illegal batches — unknown relation, wrong arity, type errors (a JSON
  boolean, a float, a JSON `null`, text in an `int` column), duplicate keys
  in one batch, deletes of missing records — are **rejected as a whole**:
  they consume no version, change no source, and are written to the
  `rejected_batches` audit table.  A rejected batch id may be re-used with
  corrected content.

### Batch identity and re-sends (supplier reconnects)

Content equality is decided on a **canonical typed form** of the batch:

* JSON object key order and the order of operations are irrelevant;
* values compare as typed tokens: `["s", text]`, `["i", int]`, `["n", id]`.
  `{"null": "u1"}` and `{"kind": "labeled_null", "id": "u1"}` are the same
  token; the string `"u1"` is a different token; a JSON `true` is rejected,
  never read as the int `1`;
* a free-form `comment` field is not content.

Re-sending a batch whose `batch_id` and typed content match a committed
batch returns the **original receipt** (`"idempotent_replay": true`)
without recomputing and without a new version — even if the workspace has
moved on since (the replay check runs before any parent check).  The same
id with different content is rejected with exit code `7`.

## Versions and pointers

Every first-seen legal batch creates the next version `N = latest + 1` and
stores, in one transaction:

* the batch identity (`batch_id`, content hash, receipt);
* the **traceable change** to the raw source set: `record_history` keeps
  each operation together with the *previous* tuple of the key, so every
  version's full source set is reconstructible (`ws sources --version N`);
* the completion result: `status`, stats, the final fact set, the null
  bookkeeping, the **input-unknown → final-value map**, the
  **fact → all supporting source records** map, and the provenance
  database.

A legal batch whose completion hits a constant conflict or a budget limit
is still a queryable version.  Later corrections always continue from the
**latest source version** — the workspace never silently falls back to the
last successful version.  `latest_complete_version` only advances on a
`complete` version.  Exporting a non-complete version is allowed but the
export states its status honestly; it is not a usable completion package.

A batch with **no source changes** (empty `ops`) and a fresh `batch_id` is
legal and re-runs the completion with the (possibly new) budgets — the way
to retry an `incomplete` version with a bigger budget.  The old version
stays queryable.

## Why recompute from raw sources

Each version's completion is a **full re-chase of that version's raw source
set**.  Nothing is inherited: no union-find state, no derived rows, no
quotient.  That is what makes the hard cases right:

* revoke the records that made an EGD hold, and two input unknowns that
  were merged into one class become **two classes again**;
* revoke the only known address, and an unknown does **not** stay glued to
  that constant;
* delete the last external support of a derivation, and mutually recursive
  derived facts (`P -> Q`, `Q -> P`, or a transitive `Reach`) **cannot keep
  each other alive**;
* if another record or another derivation still supports a fact, it stays.

(An implementation may equivalently maintain dependencies and recompute
locally; what it may not do is assume union-find merges can be inverted.
This implementation re-solves, and says so in every version's stats:
`"recompute_scope": "full"`.)

## Comparing versions (`ws diff`)

The diff never guesses unknown identity from generated `n0`/`n1` names.
Each version stores, for every **input** unknown id still occurring in its
sources, the final value: a known constant or the final unknown equivalence
class.  `ws diff A B` reports, as parseable JSON (`--human` for text):

* `input_unknowns.merges / splits / regroups` — **class-level** events over
  the before/after class relation (connected components of the bipartite
  graph), with full member lists and before/after constants — never a pile
  of fake pairwise events;
* `input_unknowns.appeared / disappeared` — input unknowns entering or
  leaving the source set;
* `input_unknowns.value_changes` — per shared input unknown:
  `became_known`, `became_unknown`, `constant_changed`;
* `constant_facts.added / removed` — facts composed **entirely of known
  constants**; values are typed, so the string `"7"` and the int `7` are
  never conflated;
* `source_records.added / removed / replaced` — the raw record changes.

Stated boundaries (also printed in every diff): rule-created unknowns are
**version-local** — no cross-version identity is claimed for them; facts
containing unknowns are not diffed, so an unknown renumbering is never
reported as a business change.

## Exports and independent verification

`ws export --version N -o DIR` writes the stock package layout
(`schema.json`, `rules.json`, `facts.csv`, `nulls.json`, `package.json`,
`conflict.json` when applicable, `provenance.sqlite`) plus:

* `sources.json` — the source manifest: every raw record of the version,
  the map from each final fact to **all** supporting source records (not
  one merged source), and the input-unknown → final-value map;
* a `workspace` block in `package.json` (version, batch id, parent).

Exports are written from the immutable per-version payload stored at commit
time: later commits or failures never rewrite history, and two exports of
the same version are byte-identical.  Derived rows of an export are **not**
a legitimate next input — corrections go through `ws commit` on raw
sources.

`ws verify DIR` re-checks, from disk and without trusting any status
marker:

1. the stock instance checks (TGD/EGD satisfaction, input preservation,
   types, null bookkeeping — the same engine-independent verifier);
2. that `sources.json` records replay to exactly `package.json`'s
   `input_facts`;
3. that the input-unknown map agrees with `nulls.json` (constants match
   `unified_with`, classes match the documented live class);
4. that every record, canonicalized through that map, lands on a real
   `facts.csv` fact listed under it in `fact_sources`, and every
   `fact_sources` entry canonicalizes back — the manifest can neither cite
   records it does not have nor hide extra ones;
5. that the claimed status is consistent with the independent instance
   check.

A tampered source manifest, null map or fact table fails verification.

## Concurrency and crash safety

* All state changes of a commit happen in **one SQLite transaction**
  (WAL journal).  A crash before the commit leaves the pre-commit state;
  a crash after leaves the fully committed state — sources, version result
  and both pointers can never disagree.  (`CHASE_WS_CRASH_AT` is a
  test-only hook that hard-kills the process at the commit boundary; see
  `tests/test_workspace_concurrency.py`.)
* Writers serialize on the database lock.  A commit naming a `--parent`
  that is no longer the latest source version fails with exit code `6`
  (stale parent); the workspace never re-bases a batch silently — the
  caller re-reads and decides.  Two writers racing from the same parent:
  exactly one commits.
* A completed batch's re-send returns the original receipt even when the
  current parent has moved on.

## The demo story (examples/corrections)

Fixed program: orders need customers, known addresses are copied in, one
address per customer (EGD), `SameAs` copies addresses across the bridge,
and `Link`/`Reach` is an ordinary recursive derivation.

| batch | version | result | what happens |
|---|---|---|---|
| b1 initial load | v1 | complete | `erp`+`shop` records support the same `Order(o1,c1)`; unknowns `u1`,`u2` are bridged to `城东`; `Reach` has 3 rows |
| b2 retract bridge + replace address | v2 | complete | one batch removes `SameAs(c1,c2)` and rewrites `城东→城西`: `u1→城西`, `u2` splits off and is unknown again |
| b3 crm sends `浦东` for c1 | v3 | **conflict** | legal batch, kept as a version; `latest_complete` stays v2 |
| b4 retract `浦东` + correct to `城北` | v4 | complete | continues from v3 (the latest source version), not from v2 |
| b5 drop one Order support + both Links | v5 | complete | `Order(o1,c1)` survives on `erp`'s record; `Reach`/`Link` disappear entirely |

Actual `ws diff --from 1 --to 2 --human`:

```
version 1 (complete) -> version 2 (complete)
input unknowns:
  appeared:    (none)
  disappeared: (none)
  SPLIT: {u1, u2}="城东"  ->  {u2}=> unknown class u2 ; {u1}=> "城西"
  value changes:
    u1: "城东" -> "城西"  (constant_changed)
    u2: "城东" -> unknown class u2  (became_unknown)
constant-only facts:
  added:
    + Customer("c1", "城西")
    + KnownAddress("c1", "城西")
  removed:
    - Customer("c1", "城东")
    - Customer("c2", "城东")
    - KnownAddress("c1", "城东")
    - KnownAddress("c2", "城东")
    - SameAs("c1", "c2")
```

Every step of this story is checked in
`tests/test_workspace_corrections.py` by rebuilding the expected small
instance **independently** from the current raw sources (handwritten
expectations + the brute-force oracle evaluator), never by reusing the
production diff logic as its own expectation.

## Scale and honest recompute cost

`python -m chase.wsbench --workspace /tmp/wsb --orders 10000 --customers
2000 --corrections 8` generates its data at runtime (nothing large is
committed to the repo).  Recorded on this machine (Python 3.11, SQLite
3.40):

| version | kind | status | facts | triggers | seconds |
|---|---|---|---|---|---|
| v1 | base (11 150 records) | complete | 13 209 | 1 995 | 3.8 |
| v2–v9 | 8 corrections (≤ 84 ops each) | complete | 13 145–13 202 | ~1 995 | 3.4–3.5 each |

Total 31.4 s for 9 versions, peak RSS 85 MB.  A 30 000-order / 6 000-customer
run with 12 corrections takes 143 s at 202 MB RSS.  **Every commit re-chases
the entire current source set** — that is the stated, honest recompute
scope; no incremental numbers are faked.  Each version's payload (facts,
nulls, provenance) is stored compressed in `workspace.sqlite`, so history
stays fully exportable.
