"""Crash-recovery and completion-status tests for snapshot ingestion.

The apply path runs its connector-ledger writes inside the workspace commit
transaction, so a hard process exit (``os._exit``) at the commit boundary can
never leave claims/ledger/receipts inconsistent with the source version.
"""

import json
import os
import subprocess
import sys
import tempfile

from chase.ingest import IngestManager
from chase.ingest_decl import parse_declaration
from chase.workspace import Workspace

from tests.test_ingest_integration import (
    MARKERS,
    ORDER_FIELDS,
    _source,
    _write,
)


def _make_ws(tmp, with_conflict_rule=False):
    program = {
        "schema": {
            "columns": {"Order": ["text", "int", "text"]},
            "column_names": {"Order": ["oid", "qty", "addr"]}},
        "rules": [
            {"id": "t_self", "body": ["Order(o, q, a)"],
             "head": "Order(o, q, a)"}],
    }
    ws_dir = os.path.join(tmp, "ws")
    Workspace.init(ws_dir, program)
    return ws_dir


_CRASH_CHILD = r'''
import os, sys
sys.path.insert(0, "/workspace")
from chase.ingest import IngestManager
from chase.ingest_decl import parse_declaration
from chase.workspace import Workspace
ws_dir, decl_path, task_id, crash_at = sys.argv[1:5]
os.environ["CHASE_WS_CRASH_AT"] = crash_at
ws = Workspace(ws_dir)
mgr = IngestManager(ws)
with open(decl_path, encoding="utf-8") as fh:
    decl = parse_declaration(json.load(fh), ws.schema) if False else None
import json
with open(decl_path, encoding="utf-8") as fh:
    decl = parse_declaration(json.load(fh), ws.schema)
mgr.stage_task(task_id, decl)
try:
    mgr.apply_task(task_id)
except BaseException:
    pass
os._exit(0)
'''


def _run_child(ws_dir, decl_path, task_id, crash_at):
    code = (
        "import os,sys,json;"
        "sys.path.insert(0,'/workspace');"
        "os.environ['CHASE_WS_CRASH_AT']=%r;"
        "from chase.ingest import IngestManager;"
        "from chase.ingest_decl import parse_declaration;"
        "from chase.workspace import Workspace;"
        "ws=Workspace(%r);mgr=IngestManager(ws);"
        "decl=parse_declaration(json.load(open(%r,encoding='utf-8')),ws.schema);"
        "mgr.stage_task(%r,decl);"
        "mgr.apply_task(%r)"
        % (crash_at, ws_dir, decl_path, task_id, task_id))
    return subprocess.run([sys.executable, "-I", "-c", code],
                          capture_output=True, text=True)


def _decl_doc(path):
    return {
        "connector": "connA", "version": 1, "unknown_markers": MARKERS,
        "sources": [_source("erp", "Order", ["oid"], ORDER_FIELDS, [path])],
    }


def test_hard_exit_before_commit_leaves_no_version_and_task_reapplicable():
    tmp = tempfile.mkdtemp(prefix="ingest-crash-")
    ws_dir = _make_ws(tmp)
    csv_path = os.path.join(tmp, "a.csv")
    _write(csv_path, "oid,qty,addr\no1,7,x\n")
    decl_path = os.path.join(tmp, "decl.json")
    with open(decl_path, "w", encoding="utf-8") as fh:
        json.dump(_decl_doc(csv_path), fh)
    proc = _run_child(ws_dir, decl_path, "t1", "before_commit")
    # the crash hook hard-exits with code 97 by design
    assert proc.returncode == 97
    # reopen in a fresh process state: nothing committed
    ws = Workspace(ws_dir)
    assert ws.latest_source_version() == 0
    # no claim, no managed ledger rows
    assert ws.conn.execute("SELECT COUNT(*) FROM ing_claims").fetchone()[0] == 0
    assert ws.conn.execute(
        "SELECT COUNT(*) FROM ing_managed_records").fetchone()[0] == 0
    task = IngestManager(ws).task_report("t1")
    assert task["applied_version"] is None
    # now apply for real: works and produces exactly v1
    mgr = IngestManager(ws)
    receipt = mgr.apply_task("t1")
    assert receipt["version"] == 1
    assert ws.latest_source_version() == 1
    ws.close()


def test_hard_exit_after_commit_is_fully_durable_and_idempotent():
    tmp = tempfile.mkdtemp(prefix="ingest-crash-")
    ws_dir = _make_ws(tmp)
    csv_path = os.path.join(tmp, "a.csv")
    _write(csv_path, "oid,qty,addr\no1,7,x\n")
    decl_path = os.path.join(tmp, "decl.json")
    with open(decl_path, "w", encoding="utf-8") as fh:
        json.dump(_decl_doc(csv_path), fh)
    _run_child(ws_dir, decl_path, "t1", "after_commit")
    ws = Workspace(ws_dir)
    assert ws.latest_source_version() == 1
    mgr = IngestManager(ws)
    # ledger and claim durable across the hard exit
    assert ws.conn.execute("SELECT COUNT(*) FROM ing_claims").fetchone()[0] == 1
    assert ws.conn.execute(
        "SELECT COUNT(*) FROM ing_managed_records").fetchone()[0] == 1
    # re-apply returns the same version, never a second one
    receipt = mgr.apply_task("t1")
    assert receipt["version"] == 1 and receipt.get("idempotent_replay")
    assert ws.latest_source_version() == 1
    ws.close()


def test_completion_conflict_is_still_one_successful_application():
    tmp = tempfile.mkdtemp(prefix="ingest-conflict-")
    program = {
        "schema": {
            "columns": {"KnownAddress": ["text", "text"]},
            "column_names": {"KnownAddress": ["cid", "addr"]}},
        "rules": [
            {"id": "e_addr",
             "body": ["KnownAddress(c, a1)", "KnownAddress(c, a2)"],
             "eq": ["a1", "a2"]}],
    }
    ws_dir = os.path.join(tmp, "ws")
    Workspace.init(ws_dir, program)
    csv_path = os.path.join(tmp, "a.csv")
    # same customer c1 with two distinct addresses -> EGD conflict
    _write(csv_path, "cid,addr\nc1,城东\nc1,城西\n")
    decl = {
        "connector": "ka", "version": 1, "unknown_markers": MARKERS,
        "sources": [{
            "source_id": "ka-src", "relation": "KnownAddress",
            "key_columns": ["cid", "addr"],
            "fields": [
                {"name": "cid", "header": "cid"},
                {"name": "addr", "header": "addr"}],
            "shards": [{"path": csv_path}], "empty_snapshot": False}],
    }
    ws = Workspace(ws_dir)
    mgr = IngestManager(ws)
    # both rows are distinct keys, so staging is applicable
    mgr.stage_task("t1", parse_declaration(decl, ws.schema))
    receipt = mgr.apply_task("t1")
    assert receipt["status"] == "conflict"          # completion conflict
    assert receipt["version"] == 1                  # but a real version exists
    assert ws.latest_source_version() == 1
    assert ws.latest_complete_version() is None     # not complete
    # re-apply does not replay the conflict as a new version
    again = mgr.apply_task("t1")
    assert again["version"] == 1
    assert again.get("idempotent_replay") is True
    ws.close()


def test_task_row_evidence_traceable_to_physical_lines():
    tmp = tempfile.mkdtemp(prefix="ingest-evidence-")
    ws_dir = _make_ws(tmp)
    csv_path = os.path.join(tmp, "a.csv")
    _write(csv_path, 'oid,qty,addr\no1,7,"a\nb"\no2,8,y\n')
    ws = Workspace(ws_dir)
    mgr = IngestManager(ws)
    decl = parse_declaration(_decl_doc(csv_path), ws.schema)
    mgr.stage_task("t1", decl)
    expl = mgr.explain_key("t1", "erp", '[["s","o1"]]')
    row = expl["input_rows"][0]
    assert row["physical_start"] == 2 and row["physical_end"] == 3
    assert row["logical_seq"] == 2
    ws.close()
