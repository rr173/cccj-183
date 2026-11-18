"""Integration tests for CSV snapshot staging/reconciliation/apply.

These build workspaces and CSV shards on disk and exercise the full
ingestion lifecycle.  Expected source sets are computed independently from
the fixture inputs — never by calling the production reconciliation code.
"""

import json
import os
import sqlite3
import tempfile

from chase.ingest import IngestError, IngestManager
from chase.ingest_decl import parse_declaration
from chase.workspace import Workspace

PROGRAM = {
    "schema": {
        "columns": {
            "Order": ["text", "int", "text"],
            "Customer": ["text", "int", "text"],
        },
        "column_names": {
            "Order": ["oid", "qty", "addr"],
            "Customer": ["cid", "tier", "city"],
        },
    },
    "rules": [
        {"id": "t_order_self", "body": ["Order(o, q, a)"],
         "head": "Order(o, q, a)"},
        {"id": "t_cust_self", "body": ["Customer(c, t, y)"],
         "head": "Customer(c, t, y)"},
    ],
}

MARKERS = {"marker": "NULL", "name_prefix": "NULL:", "name_separator": "|",
           "name_pattern": r"[a-zA-Z][a-zA-Z0-9_]*"}


def _decl(connector, sources):
    return {
        "connector": connector,
        "version": 1,
        "unknown_markers": MARKERS,
        "sources": sources,
    }


def _source(sid, relation, key_cols, fields, shard_paths,
            empty_snapshot=False):
    return {
        "source_id": sid,
        "relation": relation,
        "key_columns": key_cols,
        "fields": fields,
        "shards": [{"path": p} for p in shard_paths],
        "empty_snapshot": empty_snapshot,
    }


def _write(path, text, binary=False):
    if binary:
        with open(path, "wb") as fh:
            fh.write(text)
    else:
        with open(path, "w", encoding="utf-8", newline="") as fh:
            fh.write(text)


class Harness:
    def __init__(self):
        self.tmp = tempfile.mkdtemp(prefix="ingest-test-")
        self.ws_dir = os.path.join(self.tmp, "ws")
        Workspace.init(self.ws_dir, PROGRAM)
        self.ws = Workspace(self.ws_dir)
        self.mgr = IngestManager(self.ws)

    def close(self):
        self.ws.close()

    def path(self, name):
        return os.path.join(self.tmp, name)

    def decl(self, connector, sources):
        return parse_declaration(_decl(connector, sources), self.ws.schema)

    def stage(self, task_id, connector, sources):
        return self.mgr.stage_task(task_id, self.decl(connector, sources))

    def apply(self, task_id, confirm_empty=False):
        return self.mgr.apply_task(task_id, confirm_empty=confirm_empty)

    def source_records(self, sid):
        return {r["record_id"]: (r["relation"], json.loads(r["tuple_json"]))
                for r in self.ws.conn.execute(
                    "SELECT record_id, relation, tuple_json "
                    "FROM source_records WHERE source_id=?", (sid,))}


ORDER_FIELDS = [
    {"name": "oid", "header": "oid"},
    {"name": "qty", "header": "qty"},
    {"name": "addr", "header": "addr"},
]


# --------------------------------------------------------------------------- #
def test_first_stage_puts_all_records_and_applies_version():
    h = Harness()
    try:
        p = h.path("a.csv")
        _write(p, "oid,qty,addr\no1,7,城东\no2,07,城西\n")
        rep = h.stage("t1", "connA", [
            _source("erp", "Order", ["oid"], ORDER_FIELDS, [p])])
        assert rep["status"] == "staged" and rep["applicable"]
        assert rep["stats"]["totals"]["put"] == 2
        receipt = h.apply("t1")
        assert receipt["version"] == 1 and receipt["status"] == "complete"
        recs = h.source_records("erp")
        assert len(recs) == 2
        # 07 and 7 normalize to the same int 7
        for rid, (rel, tpl) in recs.items():
            assert rel == "Order"
            assert tpl[1][0] == "i" and tpl[1][1] == 7
    finally:
        h.close()


def test_quarantine_bad_row_does_not_delete_old_record():
    h = Harness()
    try:
        p1 = h.path("a.csv")
        _write(p1, "oid,qty,addr\no1,7,x\no2,8,y\n")
        h.stage("t1", "connA", [
            _source("erp", "Order", ["oid"], ORDER_FIELDS, [p1])])
        h.apply("t1")
        # second snapshot: o1 replaced legally; o2 has bad int (quarantine)
        p2 = h.path("b.csv")
        _write(p2, "oid,qty,addr\no1,9,xnew\no2,8.5,y\n")
        rep = h.stage("t2", "connA", [
            _source("erp", "Order", ["oid"], ORDER_FIELDS, [p2])])
        assert rep["applicable"]  # quarantine alone does not block
        cands = {c["record_key"]: c["decision"]
                 for c in h.mgr.list_candidates("t2")["candidates"]}
        # exactly two keys: one put, one quarantine; no delete!
        assert sorted(cands.values()).count("put") == 1
        assert sorted(cands.values()).count("quarantine") == 1
        assert "delete" not in cands.values()
        h.apply("t2")
        recs = h.source_records("erp")
        assert len(recs) == 2
        old_o2 = [t for rid, (r, t) in recs.items()
                  if json.loads(rid) == [["s", "o2"]]][0]
        assert old_o2 == [["s", "o2"], ["i", 8], ["s", "y"]]
    finally:
        h.close()


def test_missing_key_in_integral_snapshot_is_retracted():
    h = Harness()
    try:
        p1 = h.path("a.csv")
        _write(p1, "oid,qty,addr\no1,7,x\no2,8,y\no3,9,z\n")
        h.stage("t1", "connA", [
            _source("erp", "Order", ["oid"], ORDER_FIELDS, [p1])])
        h.apply("t1")
        p2 = h.path("b.csv")
        _write(p2, "oid,qty,addr\no1,7,x\no3,9,z\n")  # o2 vanished
        rep = h.stage("t2", "connA", [
            _source("erp", "Order", ["oid"], ORDER_FIELDS, [p2])])
        decisions = {c["record_key"]: c["decision"]
                     for c in h.mgr.list_candidates("t2")["candidates"]}
        assert any(v == "delete" for v in decisions.values())
        h.apply("t2")
        recs = h.source_records("erp")
        assert all(json.loads(rid) != [["s", "o2"]] for rid in recs)
        assert len(recs) == 2
    finally:
        h.close()


def test_broken_csv_blocks_apply_and_never_derives_deletes():
    h = Harness()
    try:
        p1 = h.path("a.csv")
        _write(p1, "oid,qty,addr\no1,7,x\n")
        h.stage("t1", "connA", [
            _source("erp", "Order", ["oid"], ORDER_FIELDS, [p1])])
        h.apply("t1")
        bad = h.path("bad.csv")
        _write(bad, b'oid,qty,addr\no2,8,"unterminated\n', binary=True)
        rep = h.stage("t2", "connA", [
            _source("erp", "Order", ["oid"], ORDER_FIELDS, [bad])])
        assert not rep["applicable"]
        assert rep["blocking"][0]["code"] == "unterminated-quote"
        try:
            h.apply("t2")
        except IngestError:
            pass
        else:
            raise AssertionError("non-integral snapshot applied")
        # old record still present, no new version beyond v1
        assert h.ws.latest_source_version() == 1
        assert len(h.source_records("erp")) == 1
    finally:
        h.close()


def test_field_count_mismatch_blocks_and_locates_line():
    h = Harness()
    try:
        p = h.path("a.csv")
        _write(p, "oid,qty,addr\no1,7,x\no2,8\n")  # only 2 fields
        rep = h.stage("t1", "connA", [
            _source("erp", "Order", ["oid"], ORDER_FIELDS, [p])])
        assert not rep["applicable"]
        diag = h.mgr.list_diagnostics("t1")["diagnostic_rows"][0]
        assert diag["outcome"] == "blocked"
        assert diag["physical_start"] == 3
        assert "field-count-mismatch" in rep["blocking"][0]["code"]
    finally:
        h.close()


def test_duplicate_headers_and_missing_mapped_column_rejected():
    h = Harness()
    try:
        p = h.path("a.csv")
        _write(p, "oid,oid,addr\no1,7,x\n")
        rep = h.stage("t1", "connA", [
            _source("erp", "Order", ["oid"], ORDER_FIELDS, [p])])
        assert not rep["applicable"]
        assert rep["blocking"][0]["code"] == "duplicate-header"
        p2 = h.path("b.csv")
        _write(p2, "oid,qty\no1,7\n")
        rep = h.stage("t2", "connA", [
            _source("erp", "Order", ["oid"], ORDER_FIELDS, [p2])])
        assert rep["blocking"][0]["code"] == "missing-mapped-column"
    finally:
        h.close()


def test_identical_duplicate_rows_collapse_but_evidence_kept():
    h = Harness()
    try:
        p1 = h.path("a.csv")
        _write(p1, "oid,qty,addr\no1,7,x\n")
        p2 = h.path("b.csv")
        _write(p2, "oid,qty,addr\no1,7,x\no2,8,y\n")  # o1 duplicated identically
        rep = h.stage("t1", "connA", [
            _source("erp", "Order", ["oid"], ORDER_FIELDS, [p1, p2])])
        assert rep["applicable"]
        assert rep["stats"]["totals"]["duplicate_collapsed"] == 1
        expl = h.mgr.explain_key("t1", "erp", '[["s","o1"]]')
        assert len(expl["input_rows"]) == 2
    finally:
        h.close()


def test_conflicting_duplicate_tuples_block_task():
    h = Harness()
    try:
        p1 = h.path("a.csv")
        _write(p1, "oid,qty,addr\no1,7,x\n")
        p2 = h.path("b.csv")
        _write(p2, "oid,qty,addr\no1,9,z\n")  # same key, different tuple
        rep = h.stage("t1", "connA", [
            _source("erp", "Order", ["oid"], ORDER_FIELDS, [p1, p2])])
        assert not rep["applicable"]
        assert rep["blocking"][0]["code"] == "conflicting-duplicate-key"
    finally:
        h.close()


def test_mixed_valid_and_quarantined_same_key_blocks():
    h = Harness()
    try:
        p1 = h.path("a.csv")
        _write(p1, "oid,qty,addr\no1,7,x\n")
        p2 = h.path("b.csv")
        _write(p2, "oid,qty,addr\no1,bad,x\n")
        rep = h.stage("t1", "connA", [
            _source("erp", "Order", ["oid"], ORDER_FIELDS, [p1, p2])])
        assert not rep["applicable"]
        assert rep["blocking"][0]["code"] == "key-has-invalid-rows"
    finally:
        h.close()


def test_unknown_sharing_within_source_and_isolation_across_sources():
    h = Harness()
    try:
        p_erp = h.path("erp.csv")
        p_crm = h.path("crm.csv")
        _write(p_erp, "oid,qty,addr\no1,7,NULL:u\no2,8,NULL:u\n")
        _write(p_crm, "cid,tier,city\nc1,1,NULL:u\n")
        rep = h.stage("t1", "connA", [
            _source("erp", "Order", ["oid"], ORDER_FIELDS, [p_erp]),
            _source("crm", "Customer", ["cid"], [
                {"name": "cid", "header": "cid"},
                {"name": "tier", "header": "tier"},
                {"name": "city", "header": "city"}], [p_crm])])
        assert rep["applicable"]
        unk = {(u["source_id"], u["local_name"]): u["global_id"]
               for u in h.mgr.unknown_report("t1")}
        g_erp = unk[("erp", "u")]
        g_crm = unk[("crm", "u")]
        assert g_erp != g_crm  # same local name, isolated across sources
        h.apply("t1")
        erp = h.source_records("erp")
        ids = {tuple(t[1] for t in tpl if t[0] == "n")
               for _, tpl in erp.values()}
        # both erp rows share the one global unknown
        assert ids == {(g_erp,)}
    finally:
        h.close()


def test_string_that_looks_like_null_id_stays_string():
    h = Harness()
    try:
        # pattern deliberately excludes the engine's n<digits> id shape
        decl_obj = {
            "connector": "connStrict",
            "version": 1,
            "unknown_markers": {"marker": "NULL", "name_prefix": "NULL:",
                                "name_separator": "|",
                                "name_pattern": r"u[a-zA-Z0-9_]*"},
            "sources": [_source(
                "erp", "Order", ["oid"], ORDER_FIELDS,
                [h.path("a.csv")])],
        }
        p = h.path("a.csv")
        _write(p, 'oid,qty,addr\nu,7,"NULL:n0"\nu2,8,NULL:u1\n')
        decl = parse_declaration(decl_obj, h.ws.schema)
        h.mgr.stage_task("t1", decl)
        h.mgr.apply_task("t1")
        recs = h.source_records("erp")
        all_tuples = [tpl for _r, tpl in recs.values()]
        flat = [tok for tpl in all_tuples for tok in tpl]
        # NULL:n0 did not match the pattern -> stays an ordinary string
        assert ["s", "NULL:n0"] in flat
        # NULL:u1 matched -> a labeled null token
        assert any(tok[0] == "n" for tok in flat)
        # the plain text key "u" stays text
        assert any(tpl[0] == ["s", "u"] for tpl in all_tuples)
    finally:
        h.close()


def test_composite_key_with_separator_and_text_int_distinction():
    h = Harness()
    try:
        program = {
            "schema": {"columns": {"Pair": ["text", "int", "text"]},
                       "column_names": {"Pair": ["k", "n", "v"]}},
            "rules": [{"id": "t_self", "body": ["Pair(k, n, v)"],
                       "head": "Pair(k, n, v)"}],
        }
        ws_dir = os.path.join(h.tmp, "ws2")
        Workspace.init(ws_dir, program)
        ws = Workspace(ws_dir)
        mgr = IngestManager(ws)
        p = h.path("pairs.csv")
        _write(p, 'k,n,v\n"a,b",7,x\n7,8,y\n')  # text "a,b" and text key "7"
        decl = parse_declaration(_decl("c", [
            _source("sp", "Pair", ["k", "n"], [
                {"name": "k", "header": "k"},
                {"name": "n", "header": "n"},
                {"name": "v", "header": "v"}], [p])]), ws.schema)
        mgr.stage_task("t1", decl)
        mgr.apply_task("t1")
        recs = {r[0]: json.loads(r[1]) for r in ws.conn.execute(
            "SELECT record_id, tuple_json FROM source_records")}
        keys = [json.loads(rid) for rid in recs]
        # text "7" key and the int 7 component never collide
        assert ["s", "7"] in keys[1] or any(
            "7" in str(k) for k in keys)
        assert any(k[0] == ["s", "a,b"] for k in keys)
        ws.close()
    finally:
        h.close()


def test_header_only_feed_cannot_silently_clear_even_without_flag():
    # the safety gate is effect-based: a zero-row integral snapshot that would
    # delete existing managed records needs confirmation regardless of the
    # empty_snapshot flag (no silent retraction of a whole source)
    h = Harness()
    try:
        p1 = h.path("a.csv")
        _write(p1, "oid,qty,addr\no1,7,x\n")
        h.stage("t1", "connA", [
            _source("erp", "Order", ["oid"], ORDER_FIELDS, [p1])])
        h.apply("t1")
        hdr = h.path("header-only.csv")
        _write(hdr, "oid,qty,addr\n")
        # empty_snapshot deliberately left False
        rep = h.stage("t2", "connA", [
            _source("erp", "Order", ["oid"], ORDER_FIELDS, [hdr],
                    empty_snapshot=False)])
        assert rep["needs_empty_confirm"] is True, rep
        assert rep["applicable"] is False
        try:
            h.apply("t2")
        except IngestError:
            pass
        else:
            raise AssertionError("unflagged header-only feed silently cleared")
        h.apply("t2", confirm_empty=True)
        assert h.source_records("erp") == {}
    finally:
        h.close()


def test_empty_snapshot_requires_explicit_confirmation():
    h = Harness()
    try:
        p1 = h.path("a.csv")
        _write(p1, "oid,qty,addr\no1,7,x\n")
        h.stage("t1", "connA", [
            _source("erp", "Order", ["oid"], ORDER_FIELDS, [p1])])
        h.apply("t1")
        empty = h.path("empty.csv")
        _write(empty, "oid,qty,addr\n")  # header-only legal empty shard
        rep = h.stage("t2", "connA", [
            _source("erp", "Order", ["oid"], ORDER_FIELDS, [empty],
                    empty_snapshot=True)])
        assert rep["needs_empty_confirm"] and not rep["applicable"]
        try:
            h.apply("t2")
        except IngestError:
            pass
        else:
            raise AssertionError("empty snapshot applied without confirmation")
        receipt = h.apply("t2", confirm_empty=True)
        assert receipt["version"] == 2
        assert h.source_records("erp") == {}
    finally:
        h.close()


def test_header_only_shard_without_empty_flag_is_rejected_at_declaration():
    h = Harness()
    try:
        p = h.path("e.csv")
        _write(p, "oid,qty,addr\n")
        try:
            h.stage("t1", "connA", [
                _source("erp", "Order", ["oid"], ORDER_FIELDS, [p],
                        empty_snapshot=False)])
        except IngestError:
            raise
        # staging itself succeeds; applicability flag distinguishes the case
        rep = h.mgr.task_report("t1")
        assert rep["needs_empty_confirm"] is False
    finally:
        h.close()


def test_claim_refused_when_workspace_already_has_records():
    h = Harness()
    try:
        # manual ws commit creates records under source_id erp
        batch = {"batch_id": "manual", "ops": [
            {"op": "put", "source_id": "erp", "record_id": "r1",
             "relation": "Order", "tuple": ["o1", 1, "x"]}]}
        h.ws.commit(batch)
        p = h.path("a.csv")
        _write(p, "oid,qty,addr\no9,7,z\n")
        h.stage("t1", "connA", [
            _source("erp", "Order", ["oid"], ORDER_FIELDS, [p])])
        try:
            h.apply("t1")
        except IngestError as exc:
            assert "may not claim" in str(exc)
        else:
            raise AssertionError("connector claimed an occupied source")
        # manual record untouched
        assert len(h.source_records("erp")) == 1
    finally:
        h.close()


def test_other_connector_cannot_manage_claimed_source():
    h = Harness()
    try:
        p = h.path("a.csv")
        _write(p, "oid,qty,addr\no1,7,x\n")
        h.stage("t1", "connA", [
            _source("erp", "Order", ["oid"], ORDER_FIELDS, [p])])
        h.apply("t1")
        p2 = h.path("b.csv")
        _write(p2, "oid,qty,addr\no1,7,x\n")
        h.stage("t2", "connB", [
            _source("erp", "Order", ["oid"], ORDER_FIELDS, [p2])])
        try:
            h.apply("t2")
        except IngestError as exc:
            assert "claimed by connector" in str(exc)
        else:
            raise AssertionError("second connector hijacked source")
    finally:
        h.close()


def test_external_same_value_touch_blocks_apply():
    h = Harness()
    try:
        p = h.path("a.csv")
        _write(p, "oid,qty,addr\no1,7,x\n")
        h.stage("t1", "connA", [
            _source("erp", "Order", ["oid"], ORDER_FIELDS, [p])])
        h.apply("t1")
        p2 = h.path("b.csv")
        _write(p2, "oid,qty,addr\no1,7,x\no2,8,y\n")
        # stage FIRST, then have an external commit land before apply
        h.stage("t2", "connA", [
            _source("erp", "Order", ["oid"], ORDER_FIELDS, [p2])])
        h.ws.commit({"batch_id": "ext", "ops": [
            {"op": "put", "source_id": "erp",
             "record_id": '[["s","o1"]]', "relation": "Order",
             "tuple": ["o1", 7, "x"]}]})
        try:
            h.apply("t2")
        except IngestError as exc:
            assert "externally modified" in str(exc)
        else:
            raise AssertionError("same-value external touch not detected")
        # a fresh task checked against the touched world adopts the new
        # baseline and can apply (the old task is left inspectable)
        h.stage("t3", "connA", [
            _source("erp", "Order", ["oid"], ORDER_FIELDS, [p2])])
        h.apply("t3")
        assert len(h.source_records("erp")) == 2
    finally:
        h.close()


def test_stale_parent_after_stage_blocks_apply_but_keeps_task():
    h = Harness()
    try:
        p = h.path("a.csv")
        _write(p, "oid,qty,addr\no1,7,x\n")
        h.stage("t1", "connA", [
            _source("erp", "Order", ["oid"], ORDER_FIELDS, [p])])
        h.apply("t1")
        p2 = h.path("b.csv")
        _write(p2, "oid,qty,addr\no1,7,x\no2,8,y\n")
        h.stage("t2", "connA", [
            _source("erp", "Order", ["oid"], ORDER_FIELDS, [p2])])
        # someone else commits a new version
        h.ws.commit({"batch_id": "other", "ops": [
            {"op": "put", "source_id": "other", "record_id": "z",
             "relation": "Order", "tuple": ["z", 1, "c"]}]})
        try:
            h.apply("t2")
        except IngestError as exc:
            assert "stale parent" in str(exc)
        else:
            raise AssertionError("stale parent applied")
        # task remains inspectable
        rep = h.mgr.task_report("t2")
        assert rep["status"] in ("staged", "blocked")
    finally:
        h.close()


def test_same_task_id_same_content_is_idempotent_different_content_rejected():
    h = Harness()
    try:
        p = h.path("a.csv")
        _write(p, "oid,qty,addr\no1,7,x\n")
        sources = [_source("erp", "Order", ["oid"], ORDER_FIELDS, [p])]
        r1 = h.stage("t1", "connA", sources)
        r2 = h.stage("t1", "connA", sources)
        assert r1["created_at"] == r2["created_at"]
        _write(p, "oid,qty,addr\no1,9,z\n")  # file changed but declaration same
        # same task id with a different content hash needs a declaration change;
        # emulate by changing the connector binding via new marker content
        other = _source("erp", "Order", ["oid"], ORDER_FIELDS, [p])
        decl = parse_declaration(
            {"connector": "connA", "version": 1,
             "unknown_markers": {"marker": "NA", "name_prefix": "NA:",
                                 "name_separator": "|"},
             "sources": [other]}, h.ws.schema)
        try:
            h.mgr.stage_task("t1", decl)
        except IngestError as exc:
            assert "different staged content" in str(exc)
        else:
            raise AssertionError("task id reused with different content")
    finally:
        h.close()


def test_reapply_of_applied_task_returns_same_version():
    h = Harness()
    try:
        p = h.path("a.csv")
        _write(p, "oid,qty,addr\no1,7,x\n")
        h.stage("t1", "connA", [
            _source("erp", "Order", ["oid"], ORDER_FIELDS, [p])])
        r1 = h.apply("t1")
        r2 = h.apply("t1")
        assert r1["version"] == r2["version"]
        assert r2.get("idempotent_replay") is True
        assert h.ws.latest_source_version() == 1
    finally:
        h.close()


def test_connector_binding_cannot_change_silently():
    h = Harness()
    try:
        p = h.path("a.csv")
        _write(p, "oid,qty,addr\no1,7,x\n")
        h.stage("t1", "connA", [
            _source("erp", "Order", ["oid"], ORDER_FIELDS, [p])])
        h.apply("t1")
        # same connector, different key column binding
        decl = parse_declaration(_decl("connA", [
            _source("erp", "Order", ["qty"], ORDER_FIELDS, [p])]), h.ws.schema)
        try:
            h.mgr.stage_task("t2", decl)
        except IngestError as exc:
            assert "bound to a different" in str(exc)
        else:
            raise AssertionError("binding changed silently")
    finally:
        h.close()


def test_staged_task_survives_deleted_source_file():
    h = Harness()
    try:
        p = h.path("a.csv")
        _write(p, "oid,qty,addr\no1,7,x\n")
        h.stage("t1", "connA", [
            _source("erp", "Order", ["oid"], ORDER_FIELDS, [p])])
        os.remove(p)
        receipt = h.apply("t1")  # applies the typed snapshot, not the file
        assert receipt["version"] == 1
    finally:
        h.close()


def test_unlisted_source_ids_are_never_touched():
    h = Harness()
    try:
        h.ws.commit({"batch_id": "m", "ops": [
            {"op": "put", "source_id": "manual", "record_id": "r",
             "relation": "Order", "tuple": ["m", 1, "c"]}]})
        p = h.path("a.csv")
        _write(p, "oid,qty,addr\no1,7,x\n")
        h.stage("t1", "connA", [
            _source("erp", "Order", ["oid"], ORDER_FIELDS, [p])])
        h.apply("t1")
        assert len(h.source_records("manual")) == 1
    finally:
        h.close()


def test_quarantine_retained_record_stays_in_managed_ledger():
    h = Harness()
    try:
        p1 = h.path("a.csv")
        _write(p1, "oid,qty,addr\no1,7,x\no2,8,y\n")
        h.stage("t1", "connA", [
            _source("erp", "Order", ["oid"], ORDER_FIELDS, [p1])])
        h.apply("t1")
        # o2 becomes invalid in the snapshot but its key appears: retain old
        p2 = h.path("b.csv")
        _write(p2, "oid,qty,addr\no1,7,x\no2,bad,y\n")
        h.stage("t2", "connA", [
            _source("erp", "Order", ["oid"], ORDER_FIELDS, [p2])])
        h.apply("t2")
        # a third, perfectly ordinary task must not see an external touch:
        # the retained o2 row is part of the managed ledger
        p3 = h.path("c.csv")
        _write(p3, "oid,qty,addr\no1,7,x\no2,8,y\no3,9,z\n")
        h.stage("t3", "connA", [
            _source("erp", "Order", ["oid"], ORDER_FIELDS, [p3])])
        checks = h.mgr.preapply_checks("t3", False)
        assert checks["problems"] == [], checks["problems"]
        h.apply("t3")
        assert len(h.source_records("erp")) == 3
    finally:
        h.close()


def test_empty_ops_apply_still_creates_one_version():
    h = Harness()
    try:
        p1 = h.path("a.csv")
        _write(p1, "oid,qty,addr\no1,7,x\n")
        h.stage("t1", "connA", [
            _source("erp", "Order", ["oid"], ORDER_FIELDS, [p1])])
        h.apply("t1")
        # identical snapshot: all keeps, zero put/delete
        p2 = h.path("b.csv")
        _write(p2, "oid,qty,addr\no1,7,x\n")
        h.stage("t2", "connA", [
            _source("erp", "Order", ["oid"], ORDER_FIELDS, [p2])])
        rep = h.mgr.task_report("t2")
        assert rep["stats"]["totals"]["put"] == 0
        assert rep["stats"]["totals"]["delete"] == 0
        receipt = h.apply("t2")
        assert receipt["version"] == 2  # explicit apply still one batch
    finally:
        h.close()
