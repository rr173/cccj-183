"""The handwritten correction sequence, checked step by step.

Uses the fixtures in examples/corrections/.  After every commit the test
rebuilds the expected small instance *independently* from the current raw
sources (handwritten expectations below + the brute-force oracle evaluator
in tests/oracle.py) — the production diff/dependency logic is never used as
its own expectation.

Story line:
  v1  two sources support Order(o1,c1); unknowns u1,u2 bridged to 城东;
      Link chain feeds recursive Reach
  v2  bridge retracted + address replaced in one batch -> u1,u2 split,
      u2 loses its known value
  v3  crm sends a conflicting address -> conflict version (kept)
  v4  bad record retracted + address corrected -> complete again
  v5  one Order support and both Link records deleted -> Order survives,
      Reach disappears completely
"""

from __future__ import annotations

import json
import os
import tempfile

from chase.model import LNullValue, Schema, parse_program
from chase.workspace import Workspace
from chase.wsdiff import diff_payloads
from chase.wsexport import export_version
from chase.wsverify import verify_export
from tests import oracle

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
FIXTURES = os.path.join(ROOT, "examples", "corrections")
TMP = tempfile.mkdtemp(prefix="chase-ws-corr-")


def _load(name):
    with open(os.path.join(FIXTURES, name), encoding="utf-8") as fh:
        return json.load(fh)


def _const_facts(payload):
    out = set()
    for rel, rows in payload["facts"].items():
        for row in rows:
            if any(v["kind"] == "labeled_null" for v in row):
                continue
            out.add((rel, tuple((v["kind"], v.get("value")) for v in row)))
    return out


def _oracle_instance(payload):
    inst = {}
    for rel, rows in payload["facts"].items():
        acc = inst.setdefault(rel, set())
        for row in rows:
            acc.add(tuple(oracle.norm(_to_model_value(v)) for v in row))
    return inst


def _to_model_value(enc):
    if enc["kind"] == "labeled_null":
        return LNullValue(enc["id"], f"NULL:{enc['id']}")
    return enc["value"]


# expected outcomes, handwritten from the raw sources of each version
S = "string"
EXPECTED = {
    1: {
        "status": "complete",
        "null_map": {"u1": "城东", "u2": "城东"},
        "const_facts": {
            ("Order", ((S, "o1"), (S, "c1"))),
            ("KnownAddress", ((S, "c1"), (S, "城东"))),
            ("KnownAddress", ((S, "c2"), (S, "城东"))),
            ("Customer", ((S, "c1"), (S, "城东"))),
            ("Customer", ((S, "c2"), (S, "城东"))),
            ("SameAs", ((S, "c1"), (S, "c2"))),
            ("Link", ((S, "d1"), (S, "d2"))),
            ("Link", ((S, "d2"), (S, "d3"))),
            ("Reach", ((S, "d1"), (S, "d2"))),
            ("Reach", ((S, "d2"), (S, "d3"))),
            ("Reach", ((S, "d1"), (S, "d3"))),
        },
        "live_unknowns": set(),
    },
    2: {
        "status": "complete",
        "null_map": {"u1": "城西", "u2": None},   # u2 unknown again
        "const_facts": {
            ("Order", ((S, "o1"), (S, "c1"))),
            ("KnownAddress", ((S, "c1"), (S, "城西"))),
            ("Customer", ((S, "c1"), (S, "城西"))),
            ("Link", ((S, "d1"), (S, "d2"))),
            ("Link", ((S, "d2"), (S, "d3"))),
            ("Reach", ((S, "d1"), (S, "d2"))),
            ("Reach", ((S, "d2"), (S, "d3"))),
            ("Reach", ((S, "d1"), (S, "d3"))),
        },
        "live_unknowns": {"u2"},
    },
    3: {"status": "conflict"},
    4: {
        "status": "complete",
        "null_map": {"u1": "城北", "u2": None},
        "const_facts": {
            ("Order", ((S, "o1"), (S, "c1"))),
            ("KnownAddress", ((S, "c1"), (S, "城北"))),
            ("Customer", ((S, "c1"), (S, "城北"))),
            ("Link", ((S, "d1"), (S, "d2"))),
            ("Link", ((S, "d2"), (S, "d3"))),
            ("Reach", ((S, "d1"), (S, "d2"))),
            ("Reach", ((S, "d2"), (S, "d3"))),
            ("Reach", ((S, "d1"), (S, "d3"))),
        },
        "live_unknowns": {"u2"},
    },
    5: {
        "status": "complete",
        "null_map": {"u1": "城北", "u2": None},
        "const_facts": {
            ("Order", ((S, "o1"), (S, "c1"))),   # one support left, fact stays
            ("KnownAddress", ((S, "c1"), (S, "城北"))),
            ("Customer", ((S, "c1"), (S, "城北"))),
        },
        "live_unknowns": {"u2"},
    },
}


def _check_version(ws, program, schema, rules, v):
    exp = EXPECTED[v]
    payload = ws.version_payload(v)
    assert payload["status"] == exp["status"], (v, payload["status"])
    if exp["status"] != "complete":
        return
    # 1. constant-only facts match the handwritten expectation exactly
    got = _const_facts(payload)
    want = set(exp["const_facts"])
    assert got == want, (v, "const facts", got ^ want)
    # 2. input unknown finals match the handwritten expectation
    nm = payload["input_null_map"]
    assert sorted(nm) == sorted(exp["null_map"]), (v, nm)
    for nid, want_final in exp["null_map"].items():
        if want_final is None:
            assert nm[nid]["kind"] == "labeled_null", (v, nid, nm[nid])
        else:
            assert nm[nid] == {"kind": "string", "value": want_final}, (v, nid, nm)
    # 3. live unknown classes are exactly the expected ones
    live = set(payload["nulls"]["live"])
    assert live == exp["live_unknowns"], (v, live)
    # 4. independent model check: canonicalize the raw sources through the
    #    EXPECTED finals (handwritten above, not the product's map) and let
    #    the brute-force oracle confirm the exported instance is a model
    canon_map = {nid: (LNullValue(nid, f"NULL:{nid}") if want is None else want)
                 for nid, want in exp["null_map"].items()}
    input_inst = {}
    for (rel, tpl) in ws.sources_at(v).values():
        row = [canon_map[tok[1]] if tok[0] == "n" else tok[1] for tok in tpl]
        input_inst.setdefault(rel, set()).add(tuple(oracle.norm(x) for x in row))
    inst = _oracle_instance(payload)
    ok, why = oracle.check_instance(schema, rules, input_inst, inst)
    assert ok, (v, why)


def test_correction_sequence():
    program = _load("program.json")
    schema = Schema({r: tuple(c) for r, c in program["schema"].items()})
    rules = parse_program({"rules": program["rules"]}, schema)
    ws = Workspace.init(os.path.join(TMP, "ws"), program)

    receipts = []
    for i in range(1, 6):
        receipts.append(ws.commit(_load(f"batch{i}.json")))
    assert [r["status"] for r in receipts] == [
        "complete", "complete", "conflict", "complete", "complete"]
    assert [r["version"] for r in receipts] == [1, 2, 3, 4, 5]
    # the conflict version is kept; corrections continued from it
    assert ws.get_version(4)["parent_version"] == 3
    assert ws.latest_source_version() == 5
    assert ws.latest_complete_version() == 5

    for v in range(1, 6):
        _check_version(ws, program, schema, rules, v)

    # the conflict version honestly conflicts: canonicalize the raw sources
    # through the version's own null map and let the independent evaluator
    # confirm the instance is not a model and the address EGD is violated
    payload3 = ws.version_payload(3)
    inst3 = _oracle_instance(payload3)
    nm3 = payload3["input_null_map"]
    input3 = {}
    for (rel, tpl) in ws.sources_at(3).values():
        row = []
        for tok in tpl:
            if tok[0] == "n":
                final = nm3[tok[1]]
                if final["kind"] == "labeled_null":
                    row.append(oracle.norm(LNullValue(final["id"],
                                                      f"NULL:{final['id']}")))
                else:
                    row.append(oracle.norm(final["value"]))
            else:
                row.append(oracle.norm(tok[1]))
        input3.setdefault(rel, set()).add(tuple(row))
    ok3, _why3 = oracle.check_instance(schema, rules, input3, inst3)
    assert not ok3
    # the recorded conflict involves the two known addresses (either the
    # customer-key or the address-key EGD is detected first; both are
    # violated by the same two constants)
    conflict3 = payload3["conflict"]
    assert conflict3["rule"] in ("e_customer_key", "e_address_key")
    consts = sorted([conflict3["left_display"], conflict3["right_display"]])
    assert consts == ['"城西"', '"浦东"'], consts
    egd_addr = [r for r in rules if r.id == "e_address_key"][0]
    sat, _ = oracle.satisfies_egd(egd_addr, inst3)
    assert not sat

    # diffs tell the correction story
    d12 = diff_payloads(ws.version_payload(1), ws.version_payload(2),
                        ws.sources_at(1), ws.sources_at(2), 1, 2)
    splits = d12["input_unknowns"]["splits"]
    assert len(splits) == 1 and sorted(splits[0]["members"]) == ["u1", "u2"]
    kinds = {vc["input_unknown"]: vc["kind"]
             for vc in d12["input_unknowns"]["value_changes"]}
    assert kinds == {"u1": "constant_changed", "u2": "became_unknown"}

    d23 = diff_payloads(ws.version_payload(2), ws.version_payload(3),
                        ws.sources_at(2), ws.sources_at(3), 2, 3)
    assert d23["to_status"] == "conflict"
    added = {(f["relation"], tuple(v["value"] for v in f["values"]))
             for f in d23["constant_facts"]["added"]}
    assert ("KnownAddress", ("c1", "浦东")) in added

    d34 = diff_payloads(ws.version_payload(3), ws.version_payload(4),
                        ws.sources_at(3), ws.sources_at(4), 3, 4)
    removed = {(f["relation"], tuple(v["value"] for v in f["values"]))
               for f in d34["constant_facts"]["removed"]}
    assert ("KnownAddress", ("c1", "浦东")) in removed
    # in the aborted conflict version u1 never reached a constant, so the
    # recovery reads as "became known again", honestly reflecting v3's state
    kinds = {vc["input_unknown"]: vc["kind"]
             for vc in d34["input_unknowns"]["value_changes"]}
    assert kinds.get("u1") == "became_known"

    d45 = diff_payloads(ws.version_payload(4), ws.version_payload(5),
                        ws.sources_at(4), ws.sources_at(5), 4, 5)
    removed = {(f["relation"], tuple(v["value"] for v in f["values"]))
               for f in d45["constant_facts"]["removed"]}
    assert ("Reach", ("d1", "d3")) in removed and ("Link", ("d1", "d2")) in removed
    added = {(f["relation"], tuple(v["value"] for v in f["values"]))
             for f in d45["constant_facts"]["added"]}
    assert not any(rel == "Order" for rel, _ in added | removed)

    # every complete version exports and verifies independently
    for v in (1, 2, 4, 5):
        out = os.path.join(TMP, f"exp{v}")
        export_version(ws, v, out)
        report = verify_export(out)
        assert report["ok"], (v, report["problems"])
    out3 = os.path.join(TMP, "exp3")
    export_version(ws, 3, out3)
    report3 = verify_export(out3)
    assert report3["ok"] and report3["claimed_status"] == "conflict"
    ws.close()
