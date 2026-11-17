"""Workspace tests: batches, versions, idempotency, persistence, exports, diffs."""

from __future__ import annotations

import json
import os
import tempfile

from chase.workspace import (
    BatchIdConflict, BatchInvalid, StaleParent, Workspace,
)
from chase.wsexport import export_version
from chase.wsverify import verify_export
from chase.wsdiff import diff_payloads

TMP = tempfile.mkdtemp(prefix="chase-ws-test-")
_COUNTER = [0]


def tmpdir(name: str) -> str:
    _COUNTER[0] += 1
    path = os.path.join(TMP, f"{_COUNTER[0]:03d}-{name}")
    os.makedirs(path, exist_ok=True)
    return path


PROGRAM = {
    "schema": {
        "Order": ["text", "text"],
        "Customer": ["text", "text"],
        "KnownAddress": ["text", "text"],
        "SameAs": ["text", "text"],
    },
    "rules": [
        {"id": "t_order_customer", "body": ["Order(o, c)"], "head": "Customer(c, a)"},
        {"id": "t_known_customer", "body": ["KnownAddress(c, a)"], "head": "Customer(c, a)"},
        {"id": "t_sameas_copy", "body": ["SameAs(x, y)", "KnownAddress(x, a)"],
         "head": "KnownAddress(y, a)"},
        {"id": "e_customer_key", "body": ["Customer(c, a1)", "Customer(c, a2)"],
         "eq": ["a1", "a2"]},
        {"id": "e_address_key", "body": ["KnownAddress(c, a1)", "KnownAddress(c, a2)"],
         "eq": ["a1", "a2"]},
    ],
}

INT_PROGRAM = {
    "schema": {"S": ["text"], "I": ["int"]},
    "rules": [{"id": "t_s", "body": ["S(x)"], "head": "I(0)"}],
}


def make_ws(name, program=None):
    return Workspace.init(tmpdir(name), program or PROGRAM)


def put(sid, rid, rel, tpl):
    return {"op": "put", "source_id": sid, "record_id": rid,
            "relation": rel, "tuple": tpl}


def delete(sid, rid):
    return {"op": "delete", "source_id": sid, "record_id": rid}


def batch(bid, ops, **extra):
    b = {"batch_id": bid, "ops": ops}
    b.update(extra)
    return b


def const_facts(ws, version):
    payload = ws.version_payload(version)
    out = set()
    for rel, rows in payload["facts"].items():
        for row in rows:
            if any(v["kind"] == "labeled_null" for v in row):
                continue
            out.add((rel, tuple((v["kind"], v.get("value")) for v in row)))
    return out


def null_map(ws, version):
    return ws.version_payload(version)["input_null_map"]


def expect_raises(exc_type, fn, *args, **kwargs):
    try:
        fn(*args, **kwargs)
    except exc_type as exc:
        return exc
    raise AssertionError(f"expected {exc_type.__name__} from {fn.__name__}")


# --------------------------------------------------------------------------- #
# batch validation
# --------------------------------------------------------------------------- #
def test_init_and_first_commit():
    ws = make_ws("init")
    r = ws.commit(batch("b1", [put("erp", "o1", "Order", ["o1", "c1"])]))
    assert r["committed"] and r["version"] == 1 and r["status"] == "complete"
    assert ws.latest_source_version() == 1
    assert ws.latest_complete_version() == 1
    ws.close()


def test_reject_delete_missing_record():
    ws = make_ws("del-missing")
    expect_raises(BatchInvalid, ws.commit, batch("b1", [delete("erp", "ghost")]))
    assert ws.latest_source_version() == 0
    assert ws.versions() == []
    ws.close()


def test_reject_duplicate_key_in_batch():
    ws = make_ws("dup-key")
    expect_raises(BatchInvalid, ws.commit,
                  batch("b1", [put("erp", "r1", "Order", ["o1", "c1"]),
                               delete("erp", "r1")]))
    assert ws.latest_source_version() == 0
    ws.close()


def test_reject_type_errors():
    ws = make_ws("types")
    for i, bad in enumerate([True, 1.5, None, {"null": ""}, ["x"], {"a": 1}]):
        expect_raises(BatchInvalid, ws.commit,
                      batch(f"b{i}", [put("erp", "r1", "Order", ["o1", bad])]),
                      )
    expect_raises(BatchInvalid, ws.commit,
                  batch("c3", [put("erp", "r1", "Order", ["only-one"])]))
    expect_raises(BatchInvalid, ws.commit,
                  batch("c4", [put("erp", "r1", "Nope", ["a", "b"])]))
    assert ws.latest_source_version() == 0
    ws.close()
    ws2 = make_ws("types2", INT_PROGRAM)
    expect_raises(BatchInvalid, ws2.commit,
                  batch("c1", [put("s", "r", "I", ["not-an-int"])]))
    expect_raises(BatchInvalid, ws2.commit, batch("c2", [put("s", "r", "S", [7])]))
    assert ws2.latest_source_version() == 0
    ws2.close()


def test_rejected_batch_id_can_be_retried():
    ws = make_ws("retry")
    expect_raises(BatchInvalid, ws.commit,
                  batch("b1", [put("erp", "r1", "Order", ["o1", None])]))
    r = ws.commit(batch("b1", [put("erp", "r1", "Order", ["o1", "c1"])]))
    assert r["version"] == 1
    ws.close()


def test_rejections_are_audited():
    ws = make_ws("audit")
    expect_raises(BatchInvalid, ws.commit,
                  batch("bx", [put("erp", "r", "Order", ["o1", None])]))
    expect_raises(BatchInvalid, ws.commit, batch("by", [delete("erp", "ghost")]))
    rows = ws.conn.execute("SELECT reason FROM rejected_batches").fetchall()
    assert len(rows) == 2
    ws.close()


# --------------------------------------------------------------------------- #
# batch semantics
# --------------------------------------------------------------------------- #
def test_batch_atomic_no_intermediate_conflict():
    """A put that would conflict + the delete of the old value in ONE batch."""
    ws = make_ws("atomic")
    ws.commit(batch("b1", [put("erp", "a1", "KnownAddress", ["c1", "城西"])]))
    r = ws.commit(batch("b2", [
        put("crm", "a2", "KnownAddress", ["c1", "浦东"]),  # would conflict alone
        delete("erp", "a1"),                              # but the old one leaves too
    ]))
    assert r["status"] == "complete"
    facts = const_facts(ws, 2)
    assert ("KnownAddress", (("string", "c1"), ("string", "浦东"))) in facts
    assert ("KnownAddress", (("string", "c1"), ("string", "城西"))) not in facts
    ws.close()


def test_legal_conflict_version_and_pointers():
    ws = make_ws("conflict")
    ws.commit(batch("b1", [put("erp", "a1", "KnownAddress", ["c1", "城西"])]))
    r2 = ws.commit(batch("b2", [put("crm", "a2", "KnownAddress", ["c1", "浦东"])]))
    assert r2["status"] == "conflict"
    assert ws.latest_source_version() == 2    # conflict is kept as a version
    assert ws.latest_complete_version() == 1  # complete pointer does not move
    # the next correction continues from the conflict version
    r3 = ws.commit(batch("b3", [delete("crm", "a2")]))
    assert r3["status"] == "complete"
    assert r3["parent_version"] == 2
    assert ws.get_version(3)["parent_version"] == 2
    assert ws.latest_complete_version() == 3
    ws.close()


def test_incomplete_version_and_recompute_batch():
    ws = make_ws("incomplete")
    r1 = ws.commit(batch("b1", [put("erp", "o1", "Order", ["o1", "c1"])],
                         budgets={"max_triggers": 0}))
    assert r1["status"] == "incomplete"
    assert ws.latest_source_version() == 1
    assert ws.latest_complete_version() is None
    r2 = ws.commit(batch("b2-recompute", []))   # no source change, fresh budget
    assert r2["status"] == "complete"
    assert ws.get_version(1)["status"] == "incomplete"  # old version queryable
    assert ws.sources_at(1) == ws.sources_at(2)         # same raw sources
    ws.close()


def test_two_sources_one_fact_delete_one_support():
    ws = make_ws("twosrc")
    ws.commit(batch("b1", [put("erp", "o1", "Order", ["o1", "c1"]),
                           put("shop", "o1", "Order", ["o1", "c1"])]))
    payload = ws.version_payload(1)
    supporters = set()
    for fs in payload["fact_sources"]:
        if fs["fact"]["relation"] == "Order":
            supporters = {tuple(r) for r in fs["records"]}
    assert supporters == {("erp", "o1"), ("shop", "o1")}, supporters
    ws.commit(batch("b2", [delete("shop", "o1")]))
    facts = const_facts(ws, 2)
    assert ("Order", (("string", "o1"), ("string", "c1"))) in facts
    ws.close()


def test_recursive_derivation_loses_last_support():
    prog = {
        "schema": {"Link": ["text", "text"], "Reach": ["text", "text"]},
        "rules": [
            {"id": "t_link", "body": ["Link(x, y)"], "head": "Reach(x, y)"},
            {"id": "t_reach", "body": ["Link(x, y)", "Reach(y, z)"],
             "head": "Reach(x, z)"},
        ],
    }
    ws = make_ws("reach", prog)
    ws.commit(batch("b1", [put("ops", "l1", "Link", ["d1", "d2"]),
                           put("ops", "l2", "Link", ["d2", "d3"])]))
    facts1 = const_facts(ws, 1)
    assert ("Reach", (("string", "d1"), ("string", "d3"))) in facts1
    ws.commit(batch("b2", [delete("ops", "l1")]))
    facts2 = const_facts(ws, 2)
    assert ("Reach", (("string", "d1"), ("string", "d3"))) not in facts2
    assert ("Reach", (("string", "d1"), ("string", "d2"))) not in facts2
    assert ("Reach", (("string", "d2"), ("string", "d3"))) in facts2  # still supported
    ws.commit(batch("b3", [delete("ops", "l2")]))
    assert const_facts(ws, 3) == set()
    ws.close()


def test_mutually_recursive_derivations_cannot_self_support():
    prog = {
        "schema": {"P": ["int"], "Q": ["int"]},
        "rules": [
            {"id": "t_pq", "body": ["P(x)"], "head": "Q(x)"},
            {"id": "t_qp", "body": ["Q(x)"], "head": "P(x)"},
        ],
    }
    ws = make_ws("mutual", prog)
    ws.commit(batch("b1", [put("s", "p1", "P", [1])]))
    assert const_facts(ws, 1) == {("P", (("int", 1),)), ("Q", (("int", 1),))}
    ws.commit(batch("b2", [delete("s", "p1")]))
    assert const_facts(ws, 2) == set()
    ws.close()


def test_unknown_ids_are_workspace_global():
    ws = make_ws("global-null")
    ws.commit(batch("b1", [
        put("erp", "a1", "KnownAddress", ["c1", {"null": "u1"}]),
        put("crm", "a2", "KnownAddress", ["c2", {"null": "u1"}]),  # same unknown
        put("crm", "a3", "KnownAddress", ["c3", "u1"]),            # just a string
    ]))
    assert sorted(null_map(ws, 1)) == ["u1"]
    assert ("KnownAddress", (("string", "c3"), ("string", "u1"))) in const_facts(ws, 1)
    ws.close()


def test_revoking_only_known_address_frees_unknown():
    ws = make_ws("unstick")
    ws.commit(batch("b1", [put("erp", "a1", "KnownAddress", ["c1", {"null": "u1"}]),
                           put("erp", "a2", "KnownAddress", ["c1", "城东"])]))
    assert null_map(ws, 1)["u1"] == {"kind": "string", "value": "城东"}
    ws.commit(batch("b2", [delete("erp", "a2")]))
    # the unknown must not stay glued to 城东
    assert null_map(ws, 2)["u1"]["kind"] == "labeled_null"
    ws.close()


# --------------------------------------------------------------------------- #
# idempotency
# --------------------------------------------------------------------------- #
def test_resend_same_content_replays():
    ws = make_ws("replay")
    ops = [put("erp", "o1", "Order", ["o1", "c1"]),
           put("shop", "o1", "Order", ["o1", "c1"]),
           put("erp", "a1", "KnownAddress", ["c1", {"null": "u1"}])]
    r1 = ws.commit(batch("b1", ops))
    # same typed content, different op order and JSON key order
    ops2 = [dict(reversed(list(op.items()))) for op in reversed(ops)]
    r2 = ws.commit({"ops": ops2, "batch_id": "b1"})
    assert r2["idempotent_replay"] is True
    assert r2["version"] == r1["version"]
    assert len(ws.versions()) == 1
    ws.close()


def test_same_id_different_content_rejected():
    ws = make_ws("idconflict")
    ws.commit(batch("b1", [put("erp", "o1", "Order", ["o1", "c1"])]))
    expect_raises(BatchIdConflict, ws.commit,
                  batch("b1", [put("erp", "o1", "Order", ["o1", "c2"])]))
    assert len(ws.versions()) == 1
    ws.close()


def test_unknown_marker_forms_equal_string_different():
    ws = make_ws("nullforms")
    ws.commit(batch("b1", [put("erp", "a1", "KnownAddress", ["c1", {"null": "u1"}])]))
    r = ws.commit(batch("b1", [put("erp", "a1", "KnownAddress",
                                   ["c1", {"kind": "labeled_null", "id": "u1"}])]))
    assert r["idempotent_replay"] is True
    expect_raises(BatchIdConflict, ws.commit,
                  batch("b1", [put("erp", "a1", "KnownAddress", ["c1", "u1"])]))
    ws.close()


def test_resend_wins_over_moved_parent():
    ws = make_ws("resend-parent")
    ws.commit(batch("b1", [put("erp", "o1", "Order", ["o1", "c1"])]))
    ws.commit(batch("b2", [put("erp", "o2", "Order", ["o2", "c1"])]))
    r = ws.commit(batch("b1", [put("erp", "o1", "Order", ["o1", "c1"])]))
    assert r["idempotent_replay"] is True
    assert r["version"] == 1
    assert len(ws.versions()) == 2
    ws.close()


# --------------------------------------------------------------------------- #
# versions and sources
# --------------------------------------------------------------------------- #
def test_close_reopen_persistence():
    ws = make_ws("reopen")
    ws.commit(batch("b1", [put("erp", "o1", "Order", ["o1", "c1"])]))
    path = ws.dir
    ws.close()
    ws = Workspace(path)
    assert ws.latest_source_version() == 1
    r = ws.commit(batch("b2", [put("erp", "o2", "Order", ["o2", "c1"])]))
    assert r["version"] == 2
    ws.close()
    ws = Workspace(path)
    assert len(ws.versions()) == 2
    assert len(ws.sources_at(1)) == 1
    assert len(ws.sources_at(2)) == 2
    ws.close()


def test_sources_at_reconstructs_history():
    ws = make_ws("history")
    ws.commit(batch("b1", [put("erp", "a1", "KnownAddress", ["c1", "城东"])]))
    ws.commit(batch("b2", [put("erp", "a1", "KnownAddress", ["c1", "城西"]),
                           put("crm", "a2", "KnownAddress", ["c2", "浦东"])]))
    ws.commit(batch("b3", [delete("crm", "a2")]))
    assert ws.sources_at(1)[("erp", "a1")][1] == (["s", "c1"], ["s", "城东"])
    s2 = ws.sources_at(2)
    assert s2[("erp", "a1")][1] == (["s", "c1"], ["s", "城西"])
    assert ("crm", "a2") in s2
    s3 = ws.sources_at(3)
    assert ("crm", "a2") not in s3
    assert s3[("erp", "a1")][1] == (["s", "c1"], ["s", "城西"])
    ws.close()


def test_stale_parent_rejected():
    ws = make_ws("stale")
    ws.commit(batch("b1", [put("erp", "o1", "Order", ["o1", "c1"])]))
    ws.commit(batch("b2", [put("erp", "o2", "Order", ["o2", "c1"])]))
    expect_raises(StaleParent, ws.commit,
                  batch("b3", [put("erp", "o3", "Order", ["o3", "c1"])]), 1)
    expect_raises(StaleParent, ws.commit,
                  batch("b4", [put("erp", "o3", "Order", ["o3", "c1"])], parent=1))
    assert len(ws.versions()) == 2
    ws.close()


# --------------------------------------------------------------------------- #
# export and verification
# --------------------------------------------------------------------------- #
def _ws_with_versions(name):
    ws = make_ws(name)
    ws.commit(batch("b1", [put("erp", "o1", "Order", ["o1", "c1"]),
                           put("erp", "a1", "KnownAddress", ["c1", {"null": "u1"}]),
                           put("erp", "a2", "KnownAddress", ["c1", "城东"])]))
    ws.commit(batch("b2", [put("crm", "a3", "KnownAddress", ["c1", "浦东"])]))
    return ws


def test_export_complete_and_verify():
    ws = _ws_with_versions("exp-ok")
    out = tmpdir("exp-ok-out")
    export_version(ws, 1, out)
    report = verify_export(out)
    assert report["ok"], json.dumps(report["problems"], indent=2, ensure_ascii=False)
    assert report["claimed_status"] == "complete"
    ws.close()


def test_export_conflict_version_honest_status():
    ws = _ws_with_versions("exp-conflict")
    out = tmpdir("exp-conflict-out")
    export_version(ws, 2, out)
    with open(os.path.join(out, "package.json"), encoding="utf-8") as fh:
        meta = json.load(fh)
    assert meta["status"] == "conflict"
    report = verify_export(out)
    assert report["ok"], json.dumps(report["problems"], indent=2, ensure_ascii=False)
    assert report["claimed_status"] == "conflict"
    assert report["package_ok"] is False  # the instance really violates the EGD
    ws.close()


def test_tampered_sources_manifest_rejected():
    ws = _ws_with_versions("tamper-src")
    out = tmpdir("tamper-src-out")
    export_version(ws, 1, out)
    src_path = os.path.join(out, "sources.json")
    with open(src_path, encoding="utf-8") as fh:
        src = json.load(fh)
    src["records"] = src["records"][1:]  # drop a source record
    with open(src_path, "w", encoding="utf-8") as fh:
        json.dump(src, fh, ensure_ascii=False)
    report = verify_export(out)
    assert not report["ok"]
    assert not report["manifest_ok"]
    ws.close()


def test_tampered_fact_sources_rejected():
    ws = _ws_with_versions("tamper-fs")
    out = tmpdir("tamper-fs-out")
    export_version(ws, 1, out)
    src_path = os.path.join(out, "sources.json")
    with open(src_path, encoding="utf-8") as fh:
        src = json.load(fh)
    src["fact_sources"] = src["fact_sources"][1:]
    with open(src_path, "w", encoding="utf-8") as fh:
        json.dump(src, fh, ensure_ascii=False)
    assert not verify_export(out)["ok"]
    ws.close()


def test_tampered_null_map_rejected():
    ws = _ws_with_versions("tamper-null")
    out = tmpdir("tamper-null-out")
    export_version(ws, 1, out)
    pkg_path = os.path.join(out, "package.json")
    with open(pkg_path, encoding="utf-8") as fh:
        meta = json.load(fh)
    meta["input_null_map"]["u1"] = {"kind": "string", "value": "城西"}
    with open(pkg_path, "w", encoding="utf-8") as fh:
        json.dump(meta, fh, ensure_ascii=False)
    assert not verify_export(out)["ok"]
    ws.close()


def test_historical_export_immutable():
    ws = _ws_with_versions("immutable")
    out_a = tmpdir("immutable-a")
    export_version(ws, 1, out_a)
    expect_raises(BatchInvalid, ws.commit, batch("bx", [delete("erp", "ghost")]))
    ws.commit(batch("b3", [delete("crm", "a3")]))
    out_b = tmpdir("immutable-b")
    export_version(ws, 1, out_b)
    for name in ("schema.json", "rules.json", "facts.csv", "nulls.json",
                 "package.json", "sources.json", "provenance.sqlite"):
        with open(os.path.join(out_a, name), "rb") as fh:
            a = fh.read()
        with open(os.path.join(out_b, name), "rb") as fh:
            b = fh.read()
        assert a == b, f"{name} changed between exports"
    ws.close()


# --------------------------------------------------------------------------- #
# diff
# --------------------------------------------------------------------------- #
def test_merge_and_split_events():
    ws = make_ws("merge-split")
    ws.commit(batch("b1", [
        put("erp", "a1", "KnownAddress", ["c1", {"null": "u1"}]),
        put("crm", "a2", "KnownAddress", ["c2", {"null": "u2"}]),
    ]))
    ws.commit(batch("b2", [
        put("bridge", "s1", "SameAs", ["c1", "c2"]),
        put("erp", "a3", "KnownAddress", ["c1", "城东"]),
    ]))
    ws.commit(batch("b3", [delete("bridge", "s1"), delete("erp", "a3")]))

    d12 = diff_payloads(ws.version_payload(1), ws.version_payload(2),
                        ws.sources_at(1), ws.sources_at(2), 1, 2)
    merges = d12["input_unknowns"]["merges"]
    assert len(merges) == 1
    assert sorted(merges[0]["members"]) == ["u1", "u2"]
    after = merges[0]["after"][0]
    assert after["token"]["type"] == "constant"
    assert after["token"]["value"]["value"] == "城东"
    kinds = {vc["input_unknown"]: vc["kind"]
             for vc in d12["input_unknowns"]["value_changes"]}
    assert kinds == {"u1": "became_known", "u2": "became_known"}

    d23 = diff_payloads(ws.version_payload(2), ws.version_payload(3),
                        ws.sources_at(2), ws.sources_at(3), 2, 3)
    splits = d23["input_unknowns"]["splits"]
    assert len(splits) == 1
    assert sorted(splits[0]["members"]) == ["u1", "u2"]
    kinds = {vc["input_unknown"]: vc["kind"]
             for vc in d23["input_unknowns"]["value_changes"]}
    assert kinds == {"u1": "became_unknown", "u2": "became_unknown"}
    ws.close()


def test_appeared_disappeared_and_constant_changed():
    ws = make_ws("appear")
    ws.commit(batch("b1", [put("erp", "a1", "KnownAddress", ["c1", {"null": "u1"}])]))
    # u1 stays in the sources and is unified with the new known address
    ws.commit(batch("b2", [put("erp", "a2", "KnownAddress", ["c1", "城东"]),
                           put("crm", "a3", "KnownAddress", ["c2", {"null": "u2"}])]))
    ws.commit(batch("b3", [delete("crm", "a3"),
                           put("erp", "a2", "KnownAddress", ["c1", "城西"])]))
    d12 = diff_payloads(ws.version_payload(1), ws.version_payload(2),
                        ws.sources_at(1), ws.sources_at(2), 1, 2)
    assert d12["input_unknowns"]["appeared"] == ["u2"]
    kinds = {vc["input_unknown"]: vc["kind"]
             for vc in d12["input_unknowns"]["value_changes"]}
    assert kinds == {"u1": "became_known"}
    d23 = diff_payloads(ws.version_payload(2), ws.version_payload(3),
                        ws.sources_at(2), ws.sources_at(3), 2, 3)
    assert d23["input_unknowns"]["disappeared"] == ["u2"]
    kinds = {vc["input_unknown"]: vc["kind"]
             for vc in d23["input_unknowns"]["value_changes"]}
    assert kinds == {"u1": "constant_changed"}
    ws.close()


def test_typed_constant_facts_and_renumbering():
    prog = {
        "schema": {"T": ["text"], "I": ["int"], "Order": ["text", "text"],
                   "Customer": ["text", "text"]},
        "rules": [
            {"id": "t_oc", "body": ["Order(o, c)"], "head": "Customer(c, a)"},
        ],
    }
    ws = make_ws("typed-diff", prog)
    ws.commit(batch("b1", [put("s", "t7", "T", ["7"]),
                           put("s", "o1", "Order", ["o1", "c1"]),
                           put("s", "o2", "Order", ["o2", "c2"])]))
    ws.commit(batch("b2", [delete("s", "t7"),
                           put("s", "i7", "I", [7]),
                           delete("s", "o2")]))
    d = diff_payloads(ws.version_payload(1), ws.version_payload(2),
                      ws.sources_at(1), ws.sources_at(2), 1, 2)
    added = {(f["relation"], tuple((v["kind"], v.get("value")) for v in f["values"]))
             for f in d["constant_facts"]["added"]}
    removed = {(f["relation"], tuple((v["kind"], v.get("value")) for v in f["values"]))
               for f in d["constant_facts"]["removed"]}
    assert ("I", (("int", 7),)) in added           # int 7 ...
    assert ("T", (("string", "7"),)) in removed    # ... is not the string "7"
    assert ("Order", (("string", "o2"), ("string", "c2"))) in removed
    assert ("Order", (("string", "o1"), ("string", "c1"))) not in removed
    # Customer rows hold (renumbered) unknowns: never reported as fact changes
    assert not any(rel == "Customer" for rel, _ in added)
    assert not any(rel == "Customer" for rel, _ in removed)
    ws.close()
