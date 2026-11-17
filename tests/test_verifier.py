"""Verifier tests: it must catch a bad package even if the status says complete.

Each test builds a real package on disk, then *tampers* with the stored
instance or metadata and checks that ``verify`` notices.  The verifier never
trusts ``package.json``'s status marker.
"""

import csv
import io
import json
import os
import shutil
import tempfile
from contextlib import redirect_stderr, redirect_stdout

from chase.package import Package
from chase.pkgverify import verify_package_dir

SCENARIO = {
    "schema": {
        "Order": ["text", "text"],
        "Customer": ["text", "text"],
        "KnownAddress": ["text", "text"],
    },
    "rules": [
        {"id": "t1", "body": ["Order(o, c)"], "head": "Customer(c, a)"},
        {"id": "t2", "body": ["KnownAddress(c, a)"], "head": "Customer(c, a)"},
        {"id": "e", "body": ["Customer(c, a1)", "Customer(c, a2)"], "eq": ["a1", "a2"]},
    ],
    "facts": {"Order": [["o1", "c"], ["o2", "c"]]},
}


def _make_package(tmpdir, scenario=None):
    from chase.cli import main
    scen = os.path.join(tmpdir, "scenario.json")
    with open(scen, "w", encoding="utf-8") as fh:
        json.dump(scenario or SCENARIO, fh, ensure_ascii=False)
    out = os.path.join(tmpdir, "pkg")
    with redirect_stdout(io.StringIO()):
        rc = main(["complete", scen, "-o", out])
    assert rc == 0, rc
    return out


def _rewrite_facts(pkgdir, rows):
    with open(os.path.join(pkgdir, "facts.csv"), "w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(["relation", "values"])
        for rel, vals in rows:
            w.writerow([rel, json.dumps(vals, ensure_ascii=False)])


def _read_facts(pkgdir):
    pkg = Package(pkgdir)
    pkg.load()
    from chase.package import encode_value
    return [(rel, [encode_value(v) for v in row])
            for rel, rows in pkg.facts.items() for row in rows]


def test_good_package_verifies():
    with tempfile.TemporaryDirectory() as tmp:
        out = _make_package(tmp)
        report = verify_package_dir(out)
        assert report["ok"], report["violations"]
        assert report["claimed_status"] == "complete"
        assert report["consistent"] is True


def test_tampered_missing_head_is_caught():
    with tempfile.TemporaryDirectory() as tmp:
        out = _make_package(tmp)
        rows = [r for r in _read_facts(out) if r[0] != "Customer"]
        _rewrite_facts(out, rows)
        report = verify_package_dir(out)
        assert not report["ok"]
        kinds = {v.get("kind") for v in report["violations"]}
        assert "tgd" in kinds


def test_tampered_egd_violation_is_caught():
    with tempfile.TemporaryDirectory() as tmp:
        out = _make_package(tmp)
        rows = _read_facts(out)
        rows += [["Customer", [{"kind": "string", "value": "c"},
                               {"kind": "string", "value": "另一处"}]]]
        _rewrite_facts(out, rows)
        report = verify_package_dir(out)
        assert not report["ok"]
        assert any(v.get("kind") == "egd" for v in report["violations"])


def test_tampered_missing_input_fact_is_caught():
    with tempfile.TemporaryDirectory() as tmp:
        out = _make_package(tmp)
        rows = [r for r in _read_facts(out)
                if not (r[0] == "Order" and r[1][0].get("value") == "o2")]
        _rewrite_facts(out, rows)
        report = verify_package_dir(out)
        assert not report["ok"]
        assert any("relation" in v for v in report["violations"])


def test_tampered_null_marked_as_string_is_caught():
    # Replace the labeled null by an ordinary string: this creates a fake
    # "source" value and must be reported both as an EGD/input issue if it
    # becomes visible; at minimum the null bookkeeping becomes inconsistent.
    with tempfile.TemporaryDirectory() as tmp:
        out = _make_package(tmp)
        # scenario has a live null: add a customer with no known address
        scen = json.loads(json.dumps(SCENARIO))
        scen["facts"] = {"Order": [["o1", "c"]]}
        out2 = _make_package(os.path.join(tmp, "second") if os.makedirs(
            os.path.join(tmp, "second")) is None else None, scen)
        rows = _read_facts(out2)
        mutated = []
        for rel, vals in rows:
            if rel == "Customer" and vals[1].get("kind") == "labeled_null":
                vals = [vals[0], {"kind": "string", "value": "FAKE"}]
            mutated.append([rel, vals])
        _rewrite_facts(out2, mutated)
        report = verify_package_dir(out2)
        assert not report["ok"], "a null replaced by a fake string must not verify"
        problems = " ".join(str(v) for v in report["violations"])
        assert "null" in problems.lower() or "does not occur" in problems


def test_missing_status_is_not_a_package():
    with tempfile.TemporaryDirectory() as tmp:
        out = _make_package(tmp)
        os.remove(os.path.join(out, "package.json"))
        report = verify_package_dir(out)
        assert not report["ok"]
        assert "no package.json" in report["error"]


def test_failed_run_does_not_overwrite_complete_package():
    with tempfile.TemporaryDirectory() as tmp:
        out = _make_package(tmp)
        before = _read_facts(out)
        # now run a *conflicting* scenario into the same directory
        bad = json.loads(json.dumps(SCENARIO))
        bad["facts"] = {"Order": [["o1", "c"]],
                        "KnownAddress": [["c", "东"], ["c", "西"]]}
        scen_path = os.path.join(tmp, "bad.json")
        with open(scen_path, "w", encoding="utf-8") as fh:
            json.dump(bad, fh, ensure_ascii=False)
        from chase.cli import main
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            rc = main(["complete", scen_path, "-o", out])
        assert rc != 0
        # the complete package is untouched
        assert _read_facts(out) == before
        assert verify_package_dir(out)["ok"]
