"""CLI-level tests for ``ws dq`` (exit codes, atomic report files, history)."""

from __future__ import annotations

import json
import os
import subprocess
import sys

from tests.test_dq_audit import build_workspace, policy_doc, put, tmpdir
from tests.test_dq_hygiene import _two_versions
from chase.workspace import Workspace

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def cli(*args, env_extra=None):
    env = dict(os.environ)
    env["PYTHONPATH"] = ROOT + os.pathsep + env.get("PYTHONPATH", "")
    if env_extra:
        env.update(env_extra)
    return subprocess.run(
        [sys.executable, "-I", os.path.join(ROOT, "run_cli.py"), "ws", "dq",
         *args],
        cwd=ROOT, env=env, capture_output=True, text=True, timeout=180)


def write_json(path, obj):
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, ensure_ascii=False)


def _policy_file(d, doc=None):
    path = os.path.join(d, "policy.json")
    write_json(path, doc or policy_doc(3))
    return path


def test_dq_check_and_run_exit_codes():
    d, ws_path = build_workspace("cli-ok")
    pol = _policy_file(d)
    p = cli("check", ws_path, "--policy", pol)
    assert p.returncode == 0, p.stderr
    info = json.loads(p.stdout)
    assert info["ok"] and info["revision"] == 3 and info["digest"]

    out = os.path.join(d, "report.json")
    p = cli("run", ws_path, "--policy", pol, "-o", out)
    assert p.returncode == 2, (p.returncode, p.stderr)   # overall fail
    summary = json.loads(p.stdout)
    assert summary["status"] == "fail"
    assert os.path.isfile(out)
    report = json.load(open(out, encoding="utf-8"))
    assert report["report_id"] == summary["report_id"]
    assert report["rules"] and "samples" in report["rules"][0]
    assert report["scan_summary"]["sources_records"] == 11

    # indeterminate-only overall status -> exit 3, never a silent pass
    doc = {"format": "data-quality-policy/1", "policy_id": "dq.only-unknown",
           "revision": 1, "sample_limit": 3,
           "rules": [{"id": "ref-known", "kind": "reference",
                      "layer": "sources", "relation": "KnownRegion",
                      "columns": ["c1"],
                      "target": {"relation": "Customer", "columns": ["c1"]},
                      "severity": "error"}]}
    pol2 = _policy_file(d, doc)
    p = cli("run", ws_path, "--policy", pol2)
    assert p.returncode == 3, (p.returncode, p.stdout, p.stderr)
    assert json.loads(p.stdout)["status"] == "indeterminate"


def test_bad_policy_and_bad_version_leave_old_report_intact():
    d, ws_path = build_workspace("cli-safe")
    pol = _policy_file(d)
    out = os.path.join(d, "report.json")
    p = cli("run", ws_path, "--policy", pol, "-o", out)
    assert p.returncode == 2
    original = open(out, "rb").read()
    mtime = os.stat(out).st_mtime_ns

    bad = os.path.join(d, "bad.json")
    doc = policy_doc(3)
    doc["rules"][0]["sql"] = "SELECT * FROM secrets"
    write_json(bad, doc)
    p = cli("run", ws_path, "--policy", bad, "-o", out, "--force")
    assert p.returncode == 5
    assert "policy rejected" in p.stderr
    assert open(out, "rb").read() == original
    assert os.stat(out).st_mtime_ns == mtime

    # unknown version: same guarantee
    p = cli("run", ws_path, "--policy", pol, "--sources-version", "99",
            "-o", out, "--force")
    assert p.returncode == 5
    assert open(out, "rb").read() == original

    # no file overwrite without --force
    p = cli("run", ws_path, "--policy", pol, "-o", out)
    assert p.returncode == 5
    assert "already exists" in p.stderr
    assert open(out, "rb").read() == original


def test_failed_run_does_not_destroy_report_and_writes_no_run_record():
    d, ws_path = build_workspace("cli-fail")
    pol = _policy_file(d)
    out = os.path.join(d, "report.json")
    assert cli("run", ws_path, "--policy", pol, "-o", out).returncode == 2
    good = open(out, "rb").read()
    runs_before = json.loads(cli("runs", ws_path).stdout)["runs"]

    p = cli("run", ws_path, "--policy", pol, "-o", out, "--force",
            env_extra={"CHASE_DQ_FAIL_AT": "before_report"})
    assert p.returncode == 5, (p.returncode, p.stdout, p.stderr)
    assert open(out, "rb").read() == good
    runs_after = json.loads(cli("runs", ws_path).stdout)["runs"]
    assert runs_after == runs_before

    # a failure during staging (after decompress) is equally inert
    p = cli("run", ws_path, "--policy", pol, "-o", out, "--force",
            env_extra={"CHASE_DQ_FAIL_AT": "after_decompress"})
    assert p.returncode == 5
    assert open(out, "rb").read() == good


def test_cli_runs_and_show_reopen_history():
    d, ws_path = build_workspace("cli-hist")
    pol = _policy_file(d)
    out = os.path.join(d, "r.json")
    p = cli("run", ws_path, "--policy", pol, "-o", out)
    assert p.returncode == 2
    first = json.loads(p.stdout)
    p = cli("runs", ws_path)
    assert p.returncode == 0
    listing = json.loads(p.stdout)
    assert listing["runs"][0]["report_id"] == first["report_id"]
    assert listing["policies"][0]["revision"] == 3
    p = cli("show", ws_path, "--run", str(first["run_id"]))
    assert p.returncode == 0
    stored = json.loads(p.stdout)
    assert stored["report"]["report_id"] == first["report_id"]
    assert stored["report"]["totals"] == first["totals"]
    p = cli("show", ws_path, "--run", "999")
    assert p.returncode == 5


def test_cli_sources_layer_on_conflict_version():
    d, ws_path = _two_versions("cli-conf")
    doc = {"format": "data-quality-policy/1", "policy_id": "dq.conf",
           "revision": 1, "sample_limit": 3,
           "rules": [{"id": "req-known", "kind": "required",
                      "layer": "sources", "relation": "KnownRegion",
                      "column": "c1", "severity": "error"}]}
    pol = _policy_file(d, doc)
    out = os.path.join(d, "conf.json")
    p = cli("run", ws_path, "--policy", pol, "--sources-version", "3",
            "-o", out)
    # the completion conflicts, so the two duplicate-region rows are found;
    # the exit code is the audit verdict, not the completion's status
    assert p.returncode == 2, (p.returncode, p.stderr)
    report = json.load(open(out, encoding="utf-8"))
    assert report["versions"]["sources"]["completion_status"] == "conflict"
    # the completion conflicts (two regions for c1), but the raw records
    # themselves carry known values -> the source-layer audit is honestly run
    assert report["status"] in ("pass", "fail")
    assert report["rules"][0]["counts"]["total"] == 5
