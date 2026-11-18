"""Persistent data-quality audits, checked against an independent oracle.

The fixture is hand-written below (never produced by the audit code) and
enumerated row by row by :mod:`tests.dq_oracle`, which shares no code with
:mod:`chase.dqaudit`.  The production audit reads the same data through a
SQLite workspace, a read-only snapshot and bounded aggregates; every count,
status and sample is then compared.

Fixture coverage (per the acceptance list)
------------------------------------------
* duplicate source keys whose canonical relation fact has been de-duplicated
  (``shop/o1`` and ``erp/o1`` on Order(o1,c1));
* text ``"7"`` / int ``7`` homographs in an int column and a text column;
* one input unknown shared across relations and one that is not;
* a labelled null in a required, an enum and a reference key;
* a reference target that only exists as a rule-derived fact (Done);
* a known reference whose target is missing;
* a source version whose completion is ``conflict``.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile

from chase.dqaudit import run_audit
from chase.dqpolicy import PolicyError, parse_policy
from chase.workspace import Workspace
from tests.dq_oracle import evaluate

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TMP = tempfile.mkdtemp(prefix="chase-dq-")
_COUNTER = [0]

PROGRAM = {
    "schema": {
        "Order": ["text", "text"],          # oid, customer
        "Customer": ["text", "text"],       # cid, region
        "KnownRegion": ["text", "text"],    # cid, region
        "Done": ["text", "text"],           # derived bridge target
        "Item": ["text", "text", "int"],    # order, sku, qty
    },
    "rules": [
        {"id": "t_order_customer", "body": ["Order(o, c)"], "head": "Customer(c, r)"},
        {"id": "t_known_region", "body": ["KnownRegion(c, r)"], "head": "Customer(c, r)"},
        {"id": "t_done", "body": ["Order(o, c)"], "head": "Done(o, c)"},
        {"id": "e_customer_region",
         "body": ["Customer(c, r1)", "Customer(c, r2)"], "eq": ["r1", "r2"]},
    ],
}

# Raw source records of the hand-written fixture.  Keys are source/record.
SOURCES = {
    "erp/o1": {"relation": "Order", "tuple": ["o1", "c1"]},
    "shop/o1": {"relation": "Order", "tuple": ["o1", "c1"]},   # duplicate key
    "erp/o2": {"relation": "Order", "tuple": ["o2", "c2"]},
    "erp/o3": {"relation": "Order", "tuple": ["o3", "c3"]},
    "crm/r1": {"relation": "KnownRegion", "tuple": ["c1", "东区"]},
    "crm/r2": {"relation": "KnownRegion", "tuple": ["c2", {"null": "u1"}]},
    "crm/r3": {"relation": "KnownRegion", "tuple": ["c3", {"null": "u2"}]},
    "wms/i1": {"relation": "Item", "tuple": ["o1", "7", 700]},
    "wms/i2": {"relation": "Item", "tuple": ["o2", "7", 7]},
    "wms/i3": {"relation": "Item", "tuple": ["o3", "7", 7]},
    "wms/i4": {"relation": "Item", "tuple": ["o2", "8", {"null": "u1"}]},
}

# The completion this fixture version would reach, written down by hand:
# - duplicates collapse: one Order(o1,c1) fact for two source records;
# - u1 is shared between KnownRegion(c2, u1) and Item(o2, 8, u1), u2 is alone;
# - Done is only produced by the rule t_done (no Done source record);
# - Customer(c1, 东区), Customer(c2, u1), Customer(c3, u2) exist, and the EGD
#   keeps them (u1, u2 are distinct unknowns).
COMPLETED = {
    "Order": [["o1", "c1"], ["o2", "c2"], ["o3", "c3"]],
    "KnownRegion": [["c1", "东区"], ["c2", {"null": "u1"}],
                    ["c3", {"null": "u2"}]],
    "Customer": [["c1", "东区"], ["c2", {"null": "u1"}],
                 ["c3", {"null": "u2"}]],
    "Done": [["o1", "c1"], ["o2", "c2"], ["o3", "c3"]],
    "Item": [["o1", "7", 700], ["o2", "7", 7], ["o3", "7", 7],
             ["o2", "8", {"null": "u1"}]],
}

def _tok(v):
    if isinstance(v, dict):
        return ("n", v["null"])
    return ("s", v) if isinstance(v, str) else ("i", v)


FIXTURE = {
    "sources": SOURCES,
    "completed": COMPLETED,
    # the sources version's completion status is reported by the audit itself;
    # here it is complete, and a separate test covers a conflict version
    "sources_status": "complete",
    "provenance": {
        ("Done", tuple(_tok(v) for v in row)):
            {"kind": "derived", "rule": "t_done",
             "support": ["Order(o, c)"]}
        for row in COMPLETED["Done"]
    },
}


def tmpdir(name: str) -> str:
    _COUNTER[0] += 1
    path = os.path.join(TMP, f"{_COUNTER[0]:03d}-{name}")
    os.makedirs(path, exist_ok=True)
    return path


def put(sid, rid, rel, tpl):
    return {"op": "put", "source_id": sid, "record_id": rid,
            "relation": rel, "tuple": tpl}


def build_workspace(name="base", extra_batches=()):
    d = tmpdir(name)
    ws_path = os.path.join(d, "ws")
    ws = Workspace.init(ws_path, PROGRAM)
    ws.commit({"batch_id": "b1",
               "ops": [put(k.split("/")[0], k.split("/")[1],
                           v["relation"], v["tuple"])
                       for k, v in sorted(SOURCES.items())]})
    for i, ops in enumerate(extra_batches, start=2):
        r = ws.commit({"batch_id": f"b{i}", "ops": ops})
        assert r["version"] == i
    ws.close()
    return d, ws_path


# Policies expressed both as production JSON and as oracle rules.
def policy_doc(sample_limit=5, indeterminate_fails=False):
    return {
        "format": "data-quality-policy/1",
        "policy_id": "dq.reference",
        "revision": 3,
        "description": "hand-written acceptance policy",
        "sample_limit": sample_limit,
        "indeterminate_fails": indeterminate_fails,
        "rules": [
            {"id": "req-region", "kind": "required", "layer": "completed",
             "relation": "Customer", "column": "c1", "severity": "error"},
            {"id": "enum-region", "kind": "enum", "layer": "completed",
             "relation": "Customer", "column": "c1", "severity": "error",
             "allowed": [{"type": "text", "value": "东区"},
                         {"type": "text", "value": "西区"}]},
            {"id": "uniq-order-key", "kind": "unique", "layer": "sources",
             "relation": "Order", "columns": ["c0"], "severity": "error"},
            {"id": "uniq-item-qty", "kind": "unique", "layer": "sources",
             "relation": "Item", "columns": ["c0", "c2"],
             "severity": "warning"},
            {"id": "ref-order-customer", "kind": "reference",
             "layer": "sources", "relation": "Order", "columns": ["c1"],
             "target": {"relation": "Customer", "columns": ["c0"]},
             "severity": "error"},
            {"id": "ref-done-derived", "kind": "reference",
             "layer": "sources", "relation": "Order", "columns": ["c0"],
             "target": {"relation": "Done", "columns": ["c0"]},
             "severity": "info"},
            {"id": "ref-item-order", "kind": "reference",
             "layer": "sources", "relation": "Item", "columns": ["c0"],
             "target": {"relation": "Order", "columns": ["c0"]},
             "severity": "error"},
            {"id": "enum-qty", "kind": "enum", "layer": "sources",
             "relation": "Item", "column": "c2", "severity": "error",
             "allowed": [{"type": "int", "value": 7},
                         {"type": "int", "value": 700}]},
            {"id": "req-done", "kind": "required", "layer": "completed",
             "relation": "Done", "column": "c1", "severity": "info"},
        ],
    }


def oracle_rules(doc):
    """Translate the policy JSON into the oracle's independent rule form."""
    out = []
    for r in doc["rules"]:
        item = {"id": r["id"], "kind": r["kind"], "layer": r["layer"],
                "relation": r["relation"],
                "severity": r["severity"]}
        # policy columns are the positional c0.. form; the fixture schema has
        # no symbolic names
        def pos(rel, c):
            return int(str(c)[1:])

        if r["kind"] in ("required", "enum"):
            item["column"] = pos(r["relation"], r["column"])
        if r["kind"] == "unique":
            item["columns"] = [pos(r["relation"], c) for c in r["columns"]]
        if r["kind"] == "reference":
            item["columns"] = [pos(r["relation"], c) for c in r["columns"]]
            item["target"] = {
                "relation": r["target"]["relation"],
                "columns": [pos(r["target"]["relation"], c)
                            for c in r["target"]["columns"]]}
        if r["kind"] == "enum":
            item["allowed"] = r["allowed"]
        out.append(item)
    return out


def _counts(prod, oracle):
    assert prod["counts"] == oracle["counts"], (prod["id"], prod["counts"],
                                                oracle["counts"])
    assert prod["status"] == oracle["status"], (prod["id"], prod["status"],
                                                oracle["status"])


def test_counts_and_status_match_independent_oracle():
    doc = policy_doc(sample_limit=5)
    _, ws_path = build_workspace()
    report = run_audit(ws_path, doc, persist=False)
    oracle = evaluate(FIXTURE, oracle_rules(doc), sample_limit=5)
    assert report["status"] == oracle["status"]
    assert report["totals"] == oracle["totals"]
    by_id = {r["id"]: r for r in oracle["rules"]}
    prod_by_id = {r["id"]: r for r in report["rules"]}
    for rid, rule in prod_by_id.items():
        _counts(rule, by_id[rid])
        assert len(rule["samples"]) == len(by_id[rule["id"]]["samples"])
        assert rule["samples_truncated"] == \
            by_id[rule["id"]]["samples_truncated"]


def test_specific_semantics():
    doc = policy_doc()
    _, ws_path = build_workspace()
    report = run_audit(ws_path, doc, persist=False)
    by_id = {r["id"]: r for r in report["rules"]}

    # duplicate source keys survive even though the completed fact is one row
    uniq = by_id["uniq-order-key"]
    assert uniq["counts"] == {"pass": 2, "violation": 2, "indeterminate": 0,
                              "total": 4}
    sources = [s["record_id"] for s in uniq["samples"]]
    assert sorted(sources) == ["o1", "o1"]
    assert {s["source_id"] for s in uniq["samples"]} == {"erp", "shop"}

    # typed uniqueness on (order, qty): o1/700, o2/7, o3/7 are distinct;
    # o2/unknown is indeterminate; the text "7" in the sku column is a
    # different column and never merges
    assert by_id["uniq-item-qty"]["counts"] == {
        "pass": 3, "violation": 0, "indeterminate": 1, "total": 4}

    # required: every labeled_null region is a violation, never a pass
    assert by_id["req-region"]["counts"] == {
        "pass": 1, "violation": 2, "indeterminate": 0, "total": 3}
    assert {s["fact"]["values"][1]["kind"] for s in by_id["req-region"]["samples"]}         == {"labeled_null"}

    # enum: unknown is indeterminate, never a pass; the one listed constant
    # passes and no known-but-unlisted value occurs in this fixture
    enum = by_id["enum-region"]
    assert enum["counts"] == {"pass": 1, "violation": 0, "indeterminate": 2,
                              "total": 3}
    assert enum["status"] == "indeterminate"
    assert {s["category"] for s in enum["samples"]} == {"indeterminate"}

    # reference with an unknown key is indeterminate, known-missing is a ticket
    ref_item = by_id["ref-item-order"]
    assert ref_item["counts"]["indeterminate"] == 0
    assert ref_item["counts"]["violation"] == 0
    # Order's raw records against completed Customer: all four resolve
    assert by_id["ref-order-customer"]["counts"] == {
        "pass": 4, "violation": 0, "indeterminate": 0, "total": 4}

    # derived target: matched fact is rule-derived, with its trigger support
    derived = by_id["ref-done-derived"]
    assert derived["counts"]["pass"] == 4
    sample = derived["matched"]["samples"][0]
    prov = sample["matched_target"]["provenance"]
    assert prov["origin_class"] == "rule_derived"
    assert prov["rule"] == "t_done"
    assert prov["trigger_key"]
    assert prov["support"] in (['Order("o1", "c1")'],
                               ['Order("o2", "c2")'],
                               ['Order("o3", "c3")'])


def test_unknown_reference_is_indeterminate():
    doc = {"format": "data-quality-policy/1", "policy_id": "dq.u",
           "revision": 1, "sample_limit": 3,
           "rules": [{"id": "ref-known", "kind": "reference",
                      "layer": "sources", "relation": "KnownRegion",
                      "columns": ["c1"],
                      "target": {"relation": "Customer", "columns": ["c1"]},
                      "severity": "error"}]}
    _, ws_path = build_workspace()
    report = run_audit(ws_path, doc, persist=False)
    rule = report["rules"][0]
    assert rule["counts"] == {"pass": 1, "violation": 0, "indeterminate": 2,
                              "total": 3}
    assert rule["status"] == "indeterminate"
    assert report["status"] == "indeterminate"
    # never a pass: cannot be counted as satisfied
    assert rule["counts"]["pass"] == 1


def test_indeterminate_fails_option_is_respected():
    doc = {"format": "data-quality-policy/1", "policy_id": "dq.u",
           "revision": 1, "sample_limit": 3, "indeterminate_fails": True,
           "rules": [{"id": "ref-known", "kind": "reference",
                      "layer": "sources", "relation": "KnownRegion",
                      "columns": ["c1"],
                      "target": {"relation": "Customer", "columns": ["c1"]},
                      "severity": "error"}]}
    _, ws_path = build_workspace()
    report = run_audit(ws_path, doc, persist=False)
    assert report["status"] == "fail"
    assert report["rules"][0]["counts"]["indeterminate"] == 2


def test_full_key_duplicate_and_enum_type_mismatch_detected():
    d, ws_path = build_workspace()
    doc = {"format": "data-quality-policy/1", "policy_id": "dq.k",
           "revision": 1, "sample_limit": 10,
           "rules": [
               {"id": "uniq-item-full", "kind": "unique", "layer": "sources",
                "relation": "Item", "columns": ["c0", "c1", "c2"],
                "severity": "error"},
               {"id": "enum-qty", "kind": "enum", "layer": "sources",
                "relation": "Item", "column": "c2", "severity": "error",
                "allowed": [{"type": "int", "value": 7}]},
           ]}
    report = run_audit(ws_path, doc, persist=False)
    by_id = {r["id"]: r for r in report["rules"]}
    # full keys (o1,"7",700), (o2,"7",7), (o3,"7",7), (o2,"8",u1): all
    # distinct -> no duplicated full key, but the unknown key is indeterminate
    assert by_id["uniq-item-full"]["counts"] == {
        "pass": 3, "violation": 0, "indeterminate": 1, "total": 4}
    # the 700 qty and the unknown qty are violations/indeterminate for qty=7
    assert by_id["enum-qty"]["counts"] == {
        "pass": 2, "violation": 1, "indeterminate": 1, "total": 4}


def test_sample_limit_does_not_change_totals():
    d, ws_path = build_workspace()
    doc = policy_doc(sample_limit=2)
    small = run_audit(ws_path, doc, persist=False)
    doc_big = policy_doc(sample_limit=100)
    big = run_audit(ws_path, doc_big, persist=False)
    for a, b in zip(small["rules"], big["rules"]):
        assert a["counts"] == b["counts"]
        assert a["counts"]["violation"] + a["counts"]["indeterminate"] >= \
            len(a["samples"])
        assert a["samples_truncated"] or not a["samples_truncated"]
    # the duplicate-key rule has more violation rows than the 2-sample cap
    uniq = {r["id"]: r for r in small["rules"]}["uniq-order-key"]
    assert uniq["counts"]["violation"] == 2 and len(uniq["samples"]) == 2
    assert uniq["samples_truncated"] is False
    req = {r["id"]: r for r in small["rules"]}["enum-region"]
    assert req["counts"]["total"] == 3
    # two indeterminate rows, limit 2 -> both shown, nothing omitted
    assert len(req["samples"]) == 2
    assert req["samples_truncated"] is False and req["sample_omitted"] == 0
    # a sample limit never changes the true total
    assert req["counts"] == {"pass": 1, "violation": 0, "indeterminate": 2,
                             "total": 3}
    # a rule whose violation rows exceed the limit reports the real total
    big = run_audit(ws_path, policy_doc(sample_limit=1), persist=False)
    uniq1 = {r["id"]: r for r in big["rules"]}["uniq-order-key"]
    assert uniq1["counts"]["violation"] == 2
    assert len(uniq1["samples"]) == 1 and uniq1["samples_truncated"] is True
    assert uniq1["sample_omitted"] == 1


def test_policy_rejections_are_total():
    _, ws_path = build_workspace()
    base = {"format": "data-quality-policy/1", "policy_id": "p", "revision": 1,
            "sample_limit": 3, "rules": []}
    bad_rule = {"id": "x", "kind": "required", "layer": "sources",
                "relation": "Order", "column": "c0", "severity": "error"}
    cases = [
        ("no rules", dict(base, rules=[])),
        ("missing layer", dict(base, rules=[{k: v for k, v in bad_rule.items()
                                             if k != "layer"}])),
        ("bad layer", dict(base, rules=[dict(bad_rule, layer="both")])),
        ("bad severity", dict(base, rules=[dict(bad_rule, severity="fatal")])),
        ("unknown relation", dict(base, rules=[dict(bad_rule,
                                                    relation="Nope")])),
        ("unknown column", dict(base, rules=[dict(bad_rule, column="nope")])),
        ("sql injection field", dict(base, rules=[dict(bad_rule, sql="DROP")])),
        ("unique on completed", dict(base, rules=[
            {"id": "u", "kind": "unique", "layer": "completed",
             "relation": "Order", "columns": ["c0"], "severity": "error"}])),
        ("duplicate ids", dict(base, rules=[bad_rule, dict(bad_rule)])),
        ("bad revision", dict(base, revision=0, rules=[bad_rule])),
        ("bad sample limit", dict(base, sample_limit=100000, rules=[bad_rule])),
        ("constant type mismatch", dict(base, rules=[
            {"id": "e", "kind": "enum", "layer": "sources",
             "relation": "Item", "column": "c2", "severity": "error",
             "allowed": [{"type": "text", "value": "7"}]}])),
        ("duplicate allowed", dict(base, rules=[
            {"id": "e", "kind": "enum", "layer": "sources",
             "relation": "Item", "column": "c2", "severity": "error",
             "allowed": [{"type": "int", "value": 7},
                         {"type": "int", "value": 7}]}])),
        ("reference key arity", dict(base, rules=[
            {"id": "r", "kind": "reference", "layer": "sources",
             "relation": "Order", "columns": ["c0"],
             "target": {"relation": "Customer", "columns": ["c0", "c1"]},
             "severity": "error"}])),
    ]
    with Workspace(ws_path) as ws:
        schema = ws.schema
    for name, doc in cases:
        try:
            parse_policy(doc, schema)
        except PolicyError:
            continue
        raise AssertionError(f"policy case {name!r} was not rejected")
    # the same bad policies never write a report or a run record
    before = sorted(os.listdir(os.path.dirname(ws_path)))
    for name, doc in cases:
        try:
            run_audit(ws_path, doc, output=os.path.join(tmpdir("out"), "r.json"))
        except PolicyError:
            continue
        raise AssertionError(f"bad policy {name!r} produced a report")
    assert os.path.exists(os.path.join(ws_path, "dq_audits.sqlite")) is False
    assert sorted(os.listdir(os.path.dirname(ws_path))) == before
