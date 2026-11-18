"""Version pinning, historical re-runs, concurrency and crash recovery.

Covers the acceptance items that are about *when* and *from where* an audit
reads:

* the same policy re-checks an older version and reports that version's data;
* the two layers can disagree about the same business data, each correctly
  (source records keep a duplicate key that the completion collapsed);
* an explicit source version whose completion is ``conflict`` still runs on
  the sources layer and the report states that status;
* an invalid version yields no report and no run record;
* ``latest`` is resolved once inside one read transaction: a commit landing
  mid-audit cannot make the report mix two versions;
* re-running the same policy content on the same versions returns the same
  ``report_id``, counts and samples, and the earlier run stays readable;
* re-opening the workspace exposes policy and run history;
* a hard exit at either side of the run-record commit leaves either the old
  or the new complete state, never a half-written report.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time

from chase.dqaudit import AuditError, AuditVersionError, DQRuns, run_audit
from chase.workspace import Workspace

from tests.test_dq_audit import (
    PROGRAM,
    build_workspace,
    policy_doc,
    put,
    tmpdir,
)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _two_versions(name="hist"):
    """v1 base, v2 adds a second KnownRegion; v3 conflicts (crm sends 北区)."""
    d = tmpdir(name)
    ws_path = os.path.join(d, "ws")
    ws = Workspace.init(ws_path, PROGRAM)
    from tests.test_dq_audit import SOURCES
    ws.commit({"batch_id": "b1",
               "ops": [put(k.split("/")[0], k.split("/")[1], v["relation"],
                           v["tuple"]) for k, v in sorted(SOURCES.items())]})
    ws.commit({"batch_id": "b2",
               "ops": [put("crm", "r4", "KnownRegion", ["c4", "西区"])]})
    r3 = ws.commit({"batch_id": "b3",
                    "ops": [put("crm", "r5", "KnownRegion", ["c1", "南市"])]})
    assert r3["status"] == "conflict"
    ws.close()
    return d, ws_path


def test_historical_version_can_be_rechecked():
    d, ws_path = _two_versions()
    doc = {"format": "data-quality-policy/1", "policy_id": "dq.hist",
           "revision": 1, "sample_limit": 10,
           "rules": [{"id": "req-known", "kind": "required",
                      "layer": "sources", "relation": "KnownRegion",
                      "column": "c1", "severity": "error"}]}
    v1 = run_audit(ws_path, doc, sources_version="1", persist=False)
    v2 = run_audit(ws_path, doc, sources_version="2", persist=False)
    assert v1["versions"]["sources"]["version"] == 1
    assert v2["versions"]["sources"]["version"] == 2
    assert v1["rules"][0]["counts"]["total"] == 3
    assert v2["rules"][0]["counts"]["total"] == 4
    assert v1["report_id"] != v2["report_id"]


def test_sources_layer_runs_on_conflict_version_and_states_it():
    d, ws_path = _two_versions()
    doc = {"format": "data-quality-policy/1", "policy_id": "dq.conf",
           "revision": 1, "sample_limit": 10,
           "rules": [{"id": "req-known", "kind": "required",
                      "layer": "sources", "relation": "KnownRegion",
                      "column": "c1", "severity": "error"}]}
    report = run_audit(ws_path, doc, sources_version="3", persist=False)
    assert report["versions"]["sources"]["completion_status"] == "conflict"
    assert report["rules"][0]["counts"]["total"] == 5
    # the completed layer refuses that same version: it is not a completion
    doc_done = {"format": "data-quality-policy/1", "policy_id": "dq.conf2",
                "revision": 1, "sample_limit": 10,
                "rules": [{"id": "req-customer", "kind": "required",
                           "layer": "completed", "relation": "Customer",
                           "column": "c1", "severity": "error"}]}
    try:
        run_audit(ws_path, doc_done, completed_version="3")
    except AuditVersionError as exc:
        assert "conflict" in str(exc)
    else:
        raise AssertionError("completed layer accepted a conflict version")


def test_invalid_version_produces_no_report_and_no_run():
    d, ws_path = _two_versions()
    runs_path = os.path.join(ws_path, "dq_audits.sqlite")
    assert not os.path.exists(runs_path)
    out = os.path.join(d, "report.json")
    for spec in ("0", "99", "latest-complete", "abc"):
        try:
            run_audit(ws_path, policy_doc(5), completed_version=spec,
                      output=out)
        except AuditVersionError:
            continue
        raise AssertionError(f"version spec {spec!r} was accepted")
    assert not os.path.exists(out)
    assert not os.path.exists(runs_path)


def test_layers_give_different_but_both_correct_evidence():
    """Same business data: sources sees two records, completion one fact."""
    d, ws_path = build_workspace()
    doc = {"format": "data-quality-policy/1", "policy_id": "dq.layers",
           "revision": 1, "sample_limit": 10,
           "rules": [
               {"id": "uniq-src-order", "kind": "unique",
                "layer": "sources", "relation": "Order", "columns": ["c0"],
                "severity": "error"},
               {"id": "ref-cmp-order", "kind": "reference",
                "layer": "completed", "relation": "Order", "columns": ["c1"],
                "target": {"relation": "Customer", "columns": ["c0"]},
                "severity": "error"},
           ]}
    report = run_audit(ws_path, doc, persist=False)
    by_id = {r["id"]: r for r in report["rules"]}
    # two distinct source records for o1 => the duplicate is a real finding
    assert by_id["uniq-src-order"]["counts"]["violation"] == 2
    assert {s["source_id"] for s in by_id["uniq-src-order"]["samples"]} == \
        {"erp", "shop"}
    # the completion has one Order(o1,c1) fact and it resolves
    assert by_id["ref-cmp-order"]["counts"] == {
        "pass": 3, "violation": 0, "indeterminate": 0, "total": 3}
    assert report["versions"]["sources"]["version"] == 1
    assert report["versions"]["completed"]["version"] == 1


def test_concurrent_commit_does_not_split_a_latest_audit():
    """A commit landing mid-run must not make one report read two versions."""
    d, ws_path = _two_versions("conc")
    doc = {"format": "data-quality-policy/1", "policy_id": "dq.conc",
           "revision": 1, "sample_limit": 10,
           "rules": [{"id": "req-known", "kind": "required",
                      "layer": "sources", "relation": "KnownRegion",
                      "column": "c1", "severity": "error"}]}
    batch_path = os.path.join(d, "late.json")
    with open(batch_path, "w", encoding="utf-8") as fh:
        json.dump({"batch_id": "late", "ops": [
            put("crm", "r9", "KnownRegion", ["c9", "西区"])]}, fh)
    child = (
        "import json,sys,time; sys.path.insert(0,'/workspace');\n"
        "time.sleep(0.6);\n"
        "from chase.workspace import Workspace;\n"
        "ws=Workspace(%r); r=ws.commit(json.load(open(%r,encoding='utf-8')));"
        "print(r['version']); ws.close()\n" % (ws_path, batch_path))
    env = dict(os.environ)
    env["PYTHONPATH"] = ROOT + os.pathsep + env.get("PYTHONPATH", "")
    proc = subprocess.Popen([sys.executable, "-I", "-c", child], env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            text=True)
    old_pause = os.environ.get("CHASE_DQ_PAUSE")
    os.environ["CHASE_DQ_PAUSE"] = "after_pinned"
    try:
        report = run_audit(ws_path, doc, sources_version="latest",
                           persist=False)
    finally:
        if old_pause is None:
            os.environ.pop("CHASE_DQ_PAUSE", None)
        else:
            os.environ["CHASE_DQ_PAUSE"] = old_pause
    out, err = proc.communicate(timeout=120)
    assert proc.returncode == 0, err
    assert out.strip() == "4"          # the competing commit did land
    # the audit pinned the pre-commit latest (v3, itself a conflict version --
    # the sources layer still audits it) and read only that version's data
    assert report["versions"]["sources"]["version"] == 3
    assert report["versions"]["sources"]["completion_status"] == "conflict"
    assert report["rules"][0]["counts"]["total"] == 5
    with Workspace(ws_path) as ws:
        assert ws.latest_source_version() == 4
        assert ws.get_version(3)["status"] == "conflict"


def test_repeat_run_is_verifiable_and_history_survives_reopen():
    d, ws_path = build_workspace("repeat")
    doc = policy_doc(3)
    out1 = os.path.join(d, "one.json")
    out2 = os.path.join(d, "two.json")
    first = run_audit(ws_path, doc, output=out1)
    second = run_audit(ws_path, doc, output=out2)
    assert first["report_id"] == second["report_id"]
    assert second["duplicate_of_run_id"] == first["run_id"]
    assert first["status"] == second["status"]
    for a, b in zip(first["rules"], second["rules"]):
        assert a["counts"] == b["counts"]
        assert a["samples"] == b["samples"]

    # reopen (new process-independent connection) and read the history back
    with DQRuns(ws_path) as runs:
        policies = runs.policies()
        assert [(p["policy_id"], p["revision"]) for p in policies] == \
            [("dq.reference", 3)]
        records = runs.runs("dq.reference")
        assert [r["report_id"] for r in records] == [
            first["report_id"], second["report_id"]]
        stored = runs.get_run(first["run_id"])["report"]
        assert stored["report_id"] == first["report_id"]
        assert stored["rules"] == first["rules"]
        assert stored["versions"]["sources"]["version"] == 1


def test_revision_change_is_a_different_policy_identity():
    d, ws_path = build_workspace("revs")
    doc = policy_doc(3)
    rev3 = run_audit(ws_path, doc)
    doc4 = dict(doc, revision=4)
    rev4 = run_audit(ws_path, doc4)
    assert rev3["policy"]["digest"] != rev4["policy"]["digest"]
    assert rev3["report_id"] != rev4["report_id"]
    with DQRuns(ws_path) as runs:
        assert {p["revision"] for p in runs.policies()} == {3, 4}


def _run_crash_child(ws_path, policy_path, crash_at):
    code = (
        "import os,sys;"
        "sys.path.insert(0,'/workspace');"
        "os.environ['CHASE_DQ_CRASH_AT']=%r;"
        "from chase.dqaudit import run_audit;"
        "import json;"
        "run_audit(%r, json.load(open(%r,encoding='utf-8')), persist=True)"
        % (crash_at, ws_path, policy_path))
    env = dict(os.environ)
    env["PYTHONPATH"] = ROOT + os.pathsep + env.get("PYTHONPATH", "")
    return subprocess.run([sys.executable, "-I", "-c", code], env=env,
                          capture_output=True, text=True, timeout=120)


def test_hard_exit_around_run_persistence_leaves_old_or_new_state():
    d, ws_path = build_workspace("crash")
    doc = policy_doc(3)
    policy_path = os.path.join(d, "policy.json")
    with open(policy_path, "w", encoding="utf-8") as fh:
        json.dump(doc, fh)
    baseline = run_audit(ws_path, doc, persist=True)
    with DQRuns(ws_path) as runs:
        before = runs.runs("dq.reference")
    assert len(before) == 1

    # crash inside the run-insert transaction: nothing of this run is visible
    proc = _run_crash_child(ws_path, policy_path, "before_run_commit")
    assert proc.returncode == 97, (proc.returncode, proc.stderr)
    with DQRuns(ws_path) as runs:
        after = runs.runs("dq.reference")
    assert len(after) == 1 and after[0]["run_id"] == baseline["run_id"]

    # crash right after the commit: exactly one new, complete run is visible
    proc = _run_crash_child(ws_path, policy_path, "after_run_commit")
    assert proc.returncode == 97, (proc.returncode, proc.stderr)
    with DQRuns(ws_path) as runs:
        after = runs.runs("dq.reference")
        assert len(after) == 2
        stored = runs.get_run(after[1]["run_id"])["report"]
    assert stored["status"] == baseline["status"]
    assert [r["counts"] for r in stored["rules"]] == \
        [r["counts"] for r in baseline["rules"]]
    # the workspace itself was not touched: no version, no pointer change
    with Workspace(ws_path) as ws:
        assert ws.latest_source_version() == 1
        assert ws.latest_complete_version() == 1
        assert ws.conn.execute(
            "SELECT COUNT(*) FROM source_records").fetchone()[0] == 11


def test_audit_does_not_write_workspace_state():
    d, ws_path = build_workspace("readonly")
    before_stat = os.stat(os.path.join(ws_path, "workspace.sqlite"))
    with Workspace(ws_path) as ws:
        before_versions = ws.conn.execute(
            "SELECT COUNT(*) FROM versions").fetchone()[0]
        before_sources = ws.conn.execute(
            "SELECT COUNT(*) FROM source_records").fetchone()[0]
    run_audit(ws_path, policy_doc(2))
    with Workspace(ws_path) as ws:
        assert ws.conn.execute(
            "SELECT COUNT(*) FROM versions").fetchone()[0] == before_versions
        assert ws.conn.execute(
            "SELECT COUNT(*) FROM source_records").fetchone()[0] == before_sources
        assert ws.latest_source_version() == 1
        assert ws.latest_complete_version() == 1
    after_stat = os.stat(os.path.join(ws_path, "workspace.sqlite"))
    assert after_stat.st_mtime == before_stat.st_mtime
