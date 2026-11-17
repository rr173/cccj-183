# Relational completion package — design notes

## What this is

A typed relational completion tool: it reads *typed facts* and a *declarative
program of dependencies* (TGDs + EGDs), runs a chase, and writes a package
containing

* the completed instance (source facts + derived facts + labeled nulls),
* an explicit statement of which values are **unknown** (`labeled_null`),
* provenance (sources, triggers, unifications),
* an independent verification report.

It is not an ETL defaulting tool.  A value that the system does not know is a
*placeholder*: it is written as `{"kind": "labeled_null", "id": "n0", ...}`,
never as an empty string, a default address, or a source-looking string.

## Typed facts

Relations are declared as `{relation: ["text"|"int", ...]}`.  Facts are a
**set**: duplicate input rows collapse; duplicate derived rows collapse when
inserted.  Each initial unknown placeholder in the input is written
`{"null": "u1"}`; every occurrence of `u1` is the *same* unknown, different ids
are different unknowns (until an EGD unifies them).  Unknowns are recorded in
provenance as `origin: "input"`, never as something "read" from a source.

## Rules

Declarative JSON, no code, no negation, no arithmetic.  A rule has a fixed
`id`, a non-empty conjunctive body of atoms, and either

* a TGD head: one atom; variables not bound in the body are **existential**;
* an EGD equality: `"eq": [term, term]` with variables bound in the body.

Body atoms may self-join, repeat variables and carry constants.  Constants in
heads are allowed (typed); all variables must be typed consistently across
body and head.  Duplicate ids and duplicate rule signatures are rejected, as
are undeclared relations.  The parser rejects negation (`not`, `!`),
arithmetic (`+ - * / %`) and comparison/code (`<`, `>`, `:=`, ...).

## Semantics of the run

**TGD application (standard chase).**  For a body binding, if some head row
exists whose universal-variable positions match the binding (existential
positions may hold anything), the trigger is *satisfied* and nothing is
created.  Otherwise one fresh labeled null is created per existential variable
**of this firing**; repeated occurrences of that variable in the head share it.

> Why we did not use the "conservative" variant: firing on every trigger even
> when a witness exists would create gratuitous facts and could keep an
> unnecessary unknown alive after unification.  The standard variant is
> terminating on our class (see below) and produces a canonical universal
> model, which is what the order-independence tests check (all legal results
> are homomorphically equivalent).

**EGD application.**  Unknowns are *constants* of the instance.  An EGD match
that equates two different known constants is a **conflict** — no priority
choice is made.  Otherwise two unknowns are merged into one equivalence class,
and an unknown merged with a constant *becomes* that constant.  After any
unification round the affected relation tables are rebuilt through the
canonical value map, so **all** old and newly derived rows are re-pointed,
duplicate rows are dropped, and the changed rows are fed back into the TGD
queue.  The run stops only when a TGD pass adds nothing and an EGD pass merges
nothing; the result is a genuine model of the constraints, not merely a
rewritten export.

**Why "finished trigger" witnesses stay valid.**  Rows are only ever rewritten
by a value map, never deleted except as exact duplicates.  A witness that
"some value fills the existential position" remains a witness after that value
is unified with anything (including a source constant).  So satisfied triggers
need not fire again; what **can** change is which *body joins* exist, and those
are exactly the rows we re-queue after each rebuild.  This is why the engine
terminates at constraint satisfaction while still catching joins that only
appear after unification (`test_new_self_join_appears_after_unification`) and
conflicts that only become visible later
(`test_late_conflict_after_two_unifications`).

## Termination and budgets

Weak acyclicity is checked **globally and independently of the sample data**:

* nodes are relation positions `(relation, column)`;
* for each TGD, an **ordinary edge** `(B,i) -> (H,j)` is added when variable
  `x` occurs at body position `(B,i)` and head position `(H,j)`;
* a **special edge** is added from *every* body variable position to every
  head position holding an existential variable;
* a cycle containing a special edge is rejected.  Ordinary recursive cycles
  are allowed (e.g. transitive closure).

The check is done on all TGDs together and reported per rule with a concrete
cycle path (`chase check`).  A program is never accepted just because the
current sample terminates: `assert_weakly_acyclic` runs before any data is
loaded.

Even in the weakly-acyclic class, merging can change cycles in the *data*
graph, so budgets bound the work: facts, trigger firings, body-probe count,
matching rows per body atom, rounds and wall-clock time.  Exceeding any budget
yields `status: "incomplete"` with the exhausted budget named — never
`complete`.

## Verification (independent)

`chase verify <package>` loads the stored instance from disk and re-checks,
from scratch:

* every TGD body binding has a head witness (exhaustive nested-loop matching,
  no indexes, no engine state);
* every EGD body binding has equal values;
* every input constant fact is still present (an input *unknown* is accepted
  in its unified form, but a different constant never is);
* every value is type-correct for its column;
* every live labeled null is documented in `nulls.json` and occurs in the
  instance; an undocumented null fails.

The verifier shares only the rule parser with the engine.  It does not read
the engine's `complete` marker to decide anything; the marker is reported as
`claimed_status` and any disagreement makes the report fail
(`test_tampered_*`).

## Packages and failure safety

A package is a directory: `schema.json`, `rules.json`, `facts.csv`,
`nulls.json`, optional `conflict.json`, `provenance.sqlite`, and
`package.json` — the status marker, **written last and atomically**, after an
independent self-check.  Publishing stages into a temp dir and moves it into
place; it refuses to replace an existing complete package.  An error or
interrupted run therefore cannot overwrite a complete package
(`test_failed_run_does_not_overwrite_complete_package`,
`test_staging_leaves_no_partial_package_on_build_error`).

Conflicts carry the actual binding, the two distinct constants, the supporting
rows, and, for values produced by unification, the merge history that led to
them.

## Storage and scale

All relations, value interning, the work queue and indexes live in SQLite
(`chase/store.py`); the frontier queue is a disk table so the pending work is
not held in RAM.  Body matching is index-assisted per bound position and
bounded by `max_match_rows` per atom; the result tables are streamed
(`fetchmany`) rather than loaded whole.  `python -m chase.bench` reports
relation sizes, trigger counts and peak RSS; e.g. 50k orders / 10k customers
run in ~5 s at ~70 MB RSS (see README for the recorded numbers).

## Entry points

* `python -m chase check scenario.json` — parse and weak-acyclicity check.
* `python -m chase complete scenario.json -o pkg/` — run and write a package.
* `python -m chase verify pkg/` — independent verification.
* `python -m chase trace pkg/ --null n0` / `--fact 'Rel(..)'` / `--row 'Rel(*, *)'`
  — where a row or an unknown came from.

Exit codes: 0 ok, 2 conflict, 3 incomplete/error, 4 verification failed,
5 usage.
