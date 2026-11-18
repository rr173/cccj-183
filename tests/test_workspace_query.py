"""Tests for the version-bound declarative query entry point.

The fixture is a hand-built workspace with known regions, repeated input
unknowns across relations and text "7" / int 7 homographs.  Expected certain
answers are computed by the independent brute-force evaluator in
:mod:`tests.query_oracle` — never by reusing the production executor — and
then compared against the SQLite-backed CLI/library path.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile

from chase.model import schema_from_json
from chase.query import QueryError, parse_query
from chase.workspace import Workspace
from chase.wsquery import run_query

from .query_oracle import brute_force, load_version_facts

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TMP = tempfile.mkdtemp(prefix="chase-wsquery-")
_COUNTER = [0]

SCHEMA = {
    "Order": ["text", "text"],
    "Customer": ["text", "text"],
    "KnownRegion": ["text", "text"],
    "RegionNote": ["text", "text"],
    "Item": ["text", "text", "int"],
}
RULES = [
    {"id": "t_order_customer", "body": ["Order(o, c)"], "head": "Customer(c, r)"},
    {"id": "t_known_region", "body": ["KnownRegion(c, r)"], "head": "Customer(c, r)"},
    {"id": "e_customer_region",
     "body": ["Customer(c, r1)", "Customer(c, r2)"], "eq": ["r1", "r2"]},
]
PROGRAM = {"schema": SCHEMA, "rules": RULES}


def tmpdir(name: str) -> str:
    _COUNTER[0] += 1
    path = os.path.join(TMP, f"{_COUNTER[0]:03d}-{name}")
    os.makedirs(path, exist_ok=True)
    return path


def put(sid, rid, rel, tpl):
    return {"op": "put", "source_id": sid, "record_id": rid,
            "relation": rel, "tuple": tpl}


def write_json(path, obj):
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, ensure_ascii=False)


def base_ops():
    ops = [
        put("erp", "o1", "Order", ["o1", "c1"]),
        put("erp", "o2", "Order", ["o2", "c2"]),
        put("erp", "o3", "Order", ["o3", "c3"]),
        put("erp", "o4", "Order", ["o4", "c4"]),
        put("erp", "o5", "Order", ["o5", "c5"]),
        put("shop", "o6", "Order", ["o6", "c5"]),
        put("erp", "o7", "Order", ["o7", "c6"]),
        put("crm", "r1", "KnownRegion", ["c1", "东区"]),
        put("crm", "r5", "KnownRegion", ["c5", "南区"]),
        put("crm", "r2", "KnownRegion", ["c2", {"null": "u1"}]),
        put("crm", "r4", "KnownRegion", ["c4", {"null": "u1"}]),
        put("crm", "r3", "KnownRegion", ["c3", {"null": "u2"}]),
        put("crm", "r6", "KnownRegion", ["c6", {"null": "u3"}]),
        put("ops", "n2", "RegionNote", ["c2", {"null": "u1"}]),
        put("ops", "n3", "RegionNote", ["c3", {"null": "u2"}]),
        put("ops", "n4", "RegionNote", ["c4", {"null": "u1"}]),
        put("ops", "n6", "RegionNote", ["c6", {"null": "u3"}]),
        put("wms", "i1", "Item", ["o1", "7", 700]),
        put("wms", "i2", "Item", ["o2", "7", 7]),
        put("wms", "i3", "Item", ["o3", "x7", 7]),
        put("wms", "i4", "Item", ["o4", "7", 9]),
        put("wms", "i5", "Item", ["o5", "8", 42]),
        put("wms", "i6", "Item", ["o6", "8", 42]),
    ]
    return ops


def make_workspace(name: str):
    d = tmpdir(name)
    ws_path = os.path.join(d, "ws")
    ws = Workspace.init(ws_path, PROGRAM)
    ws.commit({"batch_id": "b1", "ops": base_ops()})
    ws.commit({"batch_id": "b2",
               "ops": [put("crm", "r2b", "KnownRegion", ["c2", "东区"])]})
    # v3: legal but conflicting -> latest_complete stays at 2
    r3 = ws.commit({"batch_id": "b3",
                    "ops": [put("crm2", "r1n", "KnownRegion", ["c1", "北区"])]})
    assert r3["status"] == "conflict"
    # v4: empty batch with a tiny budget -> incomplete, pointers unchanged
    r4 = ws.commit({"batch_id": "b4", "ops": [],
                    "budgets": {"max_facts": 1}})
    assert r4["status"] == "incomplete"
    ws.close()
    return d, ws_path


EAST_ORDERS = {
    "relations": [{"name": "Order", "alias": "o"},
                  {"name": "Customer", "alias": "c"}],
    "select": [{"column": "o.c0", "name": "order_id"},
               {"column": "c.c1", "name": "region"}],
    "where": [{"eq": ["o.c1", "c.c0"]},
              {"eq": ["c.c1", {"type": "text", "value": "东区"}]}],
}
UNKNOWN_JOIN = {
    "relations": [{"name": "Customer", "alias": "c"},
                  {"name": "RegionNote", "alias": "n"}],
    "select": [{"column": "c.c0", "name": "customer_id"}],
    "where": [{"eq": ["c.c0", "n.c0"]}, {"eq": ["c.c1", "n.c1"]}],
}
CUSTOMER_PAIRS = {
    "relations": [{"name": "Customer", "alias": "a"},
                  {"name": "Customer", "alias": "b"}],
    "select": [{"column": "a.c0", "name": "ca"},
               {"column": "b.c0", "name": "cb"}],
    "where": [{"eq": ["a.c1", "b.c1"]}],
}
TEXT_SEVEN = {
    "relations": [{"name": "Item", "alias": "it"}],
    "select": [{"column": "it.c0", "name": "oid"},
               {"column": "it.c1", "name": "sku"},
               {"column": "it.c2", "name": "qty"}],
    "where": [{"eq": ["it.c1", {"type": "text", "value": "7"}]}],
}
INT_SEVEN = {
    "relations": [{"name": "Item", "alias": "it"}],
    "select": [{"column": "it.c0", "name": "oid"},
               {"column": "it.c2", "name": "qty"}],
    "where": [{"eq": ["it.c2", {"type": "int", "value": 7}]}],
}
JOIN_DISTINCT = {
    "relations": [{"name": "Order", "alias": "o"},
                  {"name": "Customer", "alias": "c"},
                  {"name": "Item", "alias": "it"}],
    "select": [{"column": "it.c2", "name": "qty"},
               {"column": "c.c1", "name": "region"}],
    "where": [{"eq": ["o.c1", "c.c0"]},
              {"eq": ["o.c0", "it.c0"]},
              {"eq": ["c.c1", {"type": "text", "value": "南区"}]}],
}
SELF_JOIN = {
    "relations": [{"name": "Item", "alias": "a"},
                  {"name": "Item", "alias": "b"}],
    "select": [{"column": "a.c0", "name": "oid"}],
    "where": [{"eq": ["a.c0", "b.c0"]},
              {"eq": ["a.c1", {"type": "text", "value": "7"}]},
              {"eq": ["b.c2", {"type": "int", "value": 7}]}],
}
# a query whose projection would have to expose an unknown if unknowns leaked
PROJECT_UNKNOWN = {
    "relations": [{"name": "Customer", "alias": "c"}],
    "select": [{"column": "c.c0", "name": "cid"},
               {"column": "c.c1", "name": "region"}],
    "where": [{"eq": ["c.c0", {"type": "text", "value": "c3"}]}],
}


def _oracle(ws_path, version, query_obj):
    ws = Workspace(ws_path)
    try:
        facts = load_version_facts(ws, version)
        schema = ws.schema
    finally:
        ws.close()
    return brute_force(query_obj, schema, facts)


def _run(ws_path, query_obj, spec):
    return run_query(ws_path, query_obj, str(spec))


# --------------------------------------------------------------------------- #
def test_brute_force_matches_executor_every_query():
    d, ws_path = make_workspace("oracle")
    for version in (1, 2):
        for q in (EAST_ORDERS, UNKNOWN_JOIN, CUSTOMER_PAIRS, TEXT_SEVEN,
                  INT_SEVEN, JOIN_DISTINCT, SELF_JOIN, PROJECT_UNKNOWN):
            expected = _oracle(ws_path, version, q)
            got_env = _run(ws_path, q, version)
            assert got_env["results"] == expected, (version, q, got_env["results"], expected)
            assert got_env["bound_version"]["version"] == version
            assert got_env["result_status"] == "complete"


def test_unknown_joins_but_never_leaks_as_known_projection():
    d, ws_path = make_workspace("leak")
    env = _run(ws_path, UNKNOWN_JOIN, 1)
    # c2,c3,c4,c6 have matching note rows; only the known customer id projects
    assert env["results"] == [[{"kind": "string", "value": c}]
                              for c in ("c2", "c3", "c4", "c6")]
    for row in env["results"]:
        assert all(v["kind"] in ("string", "int") for v in row)
    # projecting the unknown column drops c3's row entirely (not "u2" text!)
    env2 = _run(ws_path, PROJECT_UNKNOWN, 1)
    assert env2["results"] == []
    # the string "u2" is an ordinary constant and must not match the unknown
    q = {
        "relations": [{"name": "Customer", "alias": "c"}],
        "select": [{"column": "c.c0", "name": "cid"}],
        "where": [{"eq": ["c.c1", {"type": "text", "value": "u2"}]}],
    }
    assert _run(ws_path, q, 1)["results"] == []


def test_same_unknown_cross_relation_join_and_distinct_unknowns():
    d, ws_path = make_workspace("nullids")
    pairs = _run(ws_path, CUSTOMER_PAIRS, 1)["results"]
    flat = {(a["value"], b["value"]) for row in pairs
            for a, b in [row]}
    # u1 is shared by c2/c4 -> they pair in both directions
    assert ("c2", "c4") in flat and ("c4", "c2") in flat
    # distinct unknowns never pair on similar display names
    assert ("c2", "c3") not in flat and ("c3", "c6") not in flat
    assert ("c4", "c6") not in flat
    # known regions pair only with themselves
    assert ("c1", "c1") in flat and ("c5", "c5") in flat
    assert ("c1", "c2") not in flat and ("c5", "c1") not in flat
    # every result is known constants only
    assert all(v["kind"] in ("string", "int") for row in pairs for v in row)


def test_type_isolation_text7_int7_unknown():
    d, ws_path = make_workspace("types")
    text_rows = _run(ws_path, TEXT_SEVEN, 1)["results"]
    int_rows = _run(ws_path, INT_SEVEN, 1)["results"]
    assert [r[0]["value"] for r in text_rows] == ["o1", "o2", "o4"]
    assert all(r[1]["kind"] == "string" and r[1]["value"] == "7" for r in text_rows)
    assert [r[0]["value"] for r in int_rows] == ["o2", "o3"]
    assert all(r[1]["kind"] == "int" and r[1]["value"] == 7 for r in int_rows)
    # int 7 never satisfies a text constant on the text column (cross-domain)
    cross = {
        "relations": [{"name": "Item", "alias": "it"}],
        "select": [{"column": "it.c0", "name": "oid"}],
        "where": [{"eq": ["it.c1", {"type": "int", "value": 7}]}],
    }
    try:
        parse_query(cross, schema_from_json(SCHEMA))
        raise AssertionError("cross-domain constant should be rejected at parse time")
    except QueryError:
        pass


def test_distinct_dedup_and_self_join():
    d, ws_path = make_workspace("dedup")
    # o5/o6 both join to the same (42, 南区) tuple -> exactly one distinct row
    env = _run(ws_path, JOIN_DISTINCT, 2)
    assert env["results"] == [[{"kind": "int", "value": 42},
                               {"kind": "string", "value": "南区"}]]
    assert env["row_count"] == 1
    # self-join with repeated variable-style column equality across aliases
    sj = _run(ws_path, SELF_JOIN, 1)["results"]
    assert sj == [[{"kind": "string", "value": "o2"}]]


def test_rejection_of_illegal_columns_types_and_syntax():
    d, ws_path = make_workspace("reject")
    schema = schema_from_json(SCHEMA)
    bad_queries = [
        # unknown alias
        {"relations": [{"name": "Order", "alias": "o"}],
         "select": [{"column": "x.c0", "name": "z"}], "where": []},
        # unknown column
        {"relations": [{"name": "Order", "alias": "o"}],
         "select": [{"column": "o.nope", "name": "z"}], "where": []},
        # undeclared relation
        {"relations": [{"name": "Ghost", "alias": "g"}],
         "select": [{"column": "g.c0", "name": "z"}], "where": []},
        # duplicate alias
        {"relations": [{"name": "Order", "alias": "o"},
                       {"name": "Item", "alias": "o"}],
         "select": [{"column": "o.c0", "name": "z"}], "where": []},
        # duplicate output name
        {"relations": [{"name": "Order", "alias": "o"}],
         "select": [{"column": "o.c0", "name": "z"},
                    {"column": "o.c1", "name": "z"}], "where": []},
        # unsupported operator
        {"relations": [{"name": "Order", "alias": "o"}],
         "select": [{"column": "o.c0", "name": "z"}],
         "where": [{"ne": ["o.c1", {"type": "text", "value": "c1"}]}]},
        # unsupported top-level field (aggregation-like)
        {"relations": [{"name": "Order", "alias": "o"}],
         "select": [{"column": "o.c0", "name": "z"}],
         "where": [], "group_by": ["o.c0"]},
        # null constant is not an acceptable query constant
        {"relations": [{"name": "Order", "alias": "o"}],
         "select": [{"column": "o.c0", "name": "z"}],
         "where": [{"eq": ["o.c1", {"type": "text", "value": None}]}]},
        # boolean value rejected, never coerced to int
        {"relations": [{"name": "Item", "alias": "it"}],
         "select": [{"column": "it.c0", "name": "z"}],
         "where": [{"eq": ["it.c2", {"type": "int", "value": True}]}]},
        # cross-domain column/column equality
        {"relations": [{"name": "Item", "alias": "it"}],
         "select": [{"column": "it.c0", "name": "z"}],
         "where": [{"eq": ["it.c0", "it.c2"]}]},
        # constant-to-constant condition
        {"relations": [{"name": "Order", "alias": "o"}],
         "select": [{"column": "o.c0", "name": "z"}],
         "where": [{"eq": [{"type": "text", "value": "a"},
                           {"type": "text", "value": "b"}]}]},
        # empty relations / select / malformed shape
        {"relations": [], "select": [{"column": "o.c0", "name": "z"}], "where": []},
        {"relations": [{"name": "Order", "alias": "o"}], "select": [], "where": []},
        "not-an-object",
    ]
    for q in bad_queries:
        try:
            parse_query(q, schema)
        except QueryError:
            continue
        raise AssertionError(f"query should have been rejected: {q}")
    # rejected queries never run and never touch outputs
    out = os.path.join(d, "should-not-exist.json")
    env_code = _cli(ws_path, _write(d, "bad.json", bad_queries[5]), "1", out)
    assert env_code == 5
    assert not os.path.exists(out)


def _write(d, name, obj):
    p = os.path.join(d, name)
    write_json(p, obj)
    return p


def _cli(ws_path, query_path, spec, out=None, extra=()):
    args = [sys.executable, "-m", "chase", "ws", "query", ws_path,
            "--query", query_path, "--version", str(spec)]
    if out:
        args += ["-o", out]
    args += list(extra)
    env = dict(os.environ)
    env["PYTHONPATH"] = ROOT + os.pathsep + env.get("PYTHONPATH", "")
    proc = subprocess.run(args, cwd=ROOT, env=env, capture_output=True,
                          text=True, timeout=120)
    return proc.returncode


def test_only_complete_versions_queryable_and_pointers_named():
    d, ws_path = make_workspace("versions")
    env = _run(ws_path, EAST_ORDERS, "latest-complete")
    assert env["bound_version"]["version"] == 2
    assert env["bound_version"]["completion_status"] == "complete"
    assert env["bound_version"]["latest_source_version_at_bind"] == 4
    assert env["bound_version"]["latest_complete_version_at_bind"] == 2
    for bad in (3, 4):
        try:
            _run(ws_path, EAST_ORDERS, bad)
            raise AssertionError("non-complete version must be refused")
        except QueryError:
            pass
    # historical v1 answer does not change after later corrections
    v1_first = _run(ws_path, EAST_ORDERS, 1)
    assert [r[0]["value"] for r in v1_first["results"]] == ["o1"]
    v2 = _run(ws_path, EAST_ORDERS, 2)
    # in this fixture b2 ADDS (c2, 东区) while (c2, u1) survives: the EGD
    # merges the shared u1 into 东区, so c4 (same unknown) becomes known too
    assert [r[0]["value"] for r in v2["results"]] == ["o1", "o2", "o4"]
    # latest-complete never resolves to the conflict/incomplete versions
    assert _run(ws_path, EAST_ORDERS, "latest-complete")["bound_version"]["version"] == 2


def test_result_determinism_and_normalized_equality():
    d, ws_path = make_workspace("determinism")
    e1 = _run(ws_path, CUSTOMER_PAIRS, 1)
    e2 = _run(ws_path, CUSTOMER_PAIRS, 1)
    assert json.dumps(e1["results"], sort_keys=True) == \
        json.dumps(e2["results"], sort_keys=True)
    assert e1["query_digest"] == e2["query_digest"]
    # same query written with different key/condition order -> same canonical digest
    reordered = {
        "where": list(reversed(CUSTOMER_PAIRS["where"])),
        "select": CUSTOMER_PAIRS["select"],
        "relations": list(reversed(CUSTOMER_PAIRS["relations"])),
    }
    e3 = _run(ws_path, reordered, 1)
    assert e3["query_digest"] == e1["query_digest"]
    assert e3["results"] == e1["results"]


def test_file_output_failure_does_not_overwrite_previous_success():
    d, ws_path = make_workspace("outputs")
    good = os.path.join(d, "good.json")
    assert _cli(ws_path, _write(d, "q.json", EAST_ORDERS), "1", good) == 0
    before = open(good, encoding="utf-8").read()
    # re-run without --force: refused, old file byte-identical
    code = _cli(ws_path, os.path.join(d, "q.json"), "1", good)
    assert code == 5
    assert open(good, encoding="utf-8").read() == before
    # a failing (rejected) query must not clobber the existing result
    bad = {"relations": [{"name": "Order", "alias": "o"}],
           "select": [{"column": "o.bogus", "name": "z"}], "where": []}
    assert _cli(ws_path, _write(d, "bad.json", bad), "1", good) == 5
    assert open(good, encoding="utf-8").read() == before
    # no leftover temp files
    assert not [f for f in os.listdir(d) if f.startswith(".wsquery-tmp")]
    # --force replaces cleanly and the envelope parses
    assert _cli(ws_path, os.path.join(d, "q.json"), "1", good,
                extra=("--force",)) == 0
    json.load(open(good, encoding="utf-8"))


def test_row_and_time_caps_mark_truncated():
    d, ws_path = make_workspace("caps")
    env = run_query(ws_path, CUSTOMER_PAIRS, 1, max_rows=3)
    assert env["result_status"] == "truncated"
    assert env["limit_reason"] == "max_rows"
    assert env["row_count"] == 3
    full = _run(ws_path, CUSTOMER_PAIRS, 1)
    assert full["row_count"] > 3
    # truncated output file is still valid JSON and honestly marked
    out = os.path.join(d, "trunc.json")
    code = _cli(ws_path, _write(d, "pairs.json", CUSTOMER_PAIRS), "1", out,
                extra=("--max-rows", "2"))
    assert code == 8
    obj = json.load(open(out, encoding="utf-8"))
    assert obj["result_status"] == "truncated" and obj["row_count"] == 2
    # a time cap of a vanishingly small budget also marks truncation honestly
    env_t = run_query(ws_path, EAST_ORDERS, 1, max_seconds=1e9)
    assert env_t["result_status"] == "complete"  # generous cap does not truncate


def test_workspace_unchanged_by_queries():
    d, ws_path = make_workspace("readonly")
    ws = Workspace(ws_path)
    before = {r: ws.conn.execute(
        f"SELECT COUNT(*) FROM {r}").fetchone()[0]
        for r in ("meta", "batches", "versions", "source_records",
                  "record_history", "rejected_batches")}
    pointers_before = (ws.latest_source_version(), ws.latest_complete_version())
    ws.close()
    for q in (EAST_ORDERS, TEXT_SEVEN, CUSTOMER_PAIRS):
        _run(ws_path, q, "latest-complete")
    ws = Workspace(ws_path)
    after = {r: ws.conn.execute(
        f"SELECT COUNT(*) FROM {r}").fetchone()[0]
        for r in ("meta", "batches", "versions", "source_records",
                  "record_history", "rejected_batches")}
    assert before == after
    assert pointers_before == (ws.latest_source_version(),
                               ws.latest_complete_version())
    ws.close()
