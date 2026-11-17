"""Package integrity: atomic publish, interruption safety, provenance contents."""

import io
import json
import os
import sqlite3
import tempfile
from contextlib import redirect_stderr, redirect_stdout

from chase.cli import main
from chase.package import Package, stage_and_publish
from chase.pkgverify import verify_package_dir

SCENARIO = {
    "schema": {"A": ["text"], "B": ["text", "text"]},
    "rules": [{"id": "t", "body": ["A(x)"], "head": "B(x, u)"}],
    "facts": {"A": [["k1"], ["k2"]]},
}


def _scenario(tmp, obj=None):
    path = os.path.join(tmp, "s.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj or SCENARIO, fh, ensure_ascii=False)
    return path


def test_package_layout_and_provenance():
    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "pkg")
        with redirect_stdout(io.StringIO()):
            rc = main(["complete", _scenario(tmp), "-o", out])
        assert rc == 0
        for name in ("schema.json", "rules.json", "facts.csv", "nulls.json",
                     "package.json", "provenance.sqlite"):
            assert os.path.isfile(os.path.join(out, name)), name
        conn = sqlite3.connect(os.path.join(out, "provenance.sqlite"))
        try:
            trig = conn.execute("SELECT COUNT(*) FROM trigger").fetchone()[0]
            merges = conn.execute("SELECT COUNT(*) FROM merge").fetchone()[0]
            origins = conn.execute("SELECT COUNT(*) FROM fact_origin").fetchone()[0]
            rels = conn.execute("SELECT COUNT(*) FROM relation").fetchone()[0]
            rules = conn.execute("SELECT COUNT(*) FROM rule").fetchone()[0]
        finally:
            conn.close()
        assert trig == 2 and merges == 0 and origins == 4
        assert rels == 2 and rules == 1


def test_staging_leaves_no_partial_package_on_build_error():
    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "pkg")

        def broken_build(tmpdir):
            with open(os.path.join(tmpdir, "facts.csv"), "w") as fh:
                fh.write("partial\n")
            raise RuntimeError("boom")

        try:
            stage_and_publish(out, broken_build)
        except RuntimeError:
            pass
        else:
            raise AssertionError("expected the build error")
        assert not os.path.exists(out)
        # no stray staging dirs either
        assert not [d for d in os.listdir(tmp) if d.startswith(".staging-")]


def test_status_marker_is_written_last():
    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "pkg")

        def build_without_status(tmpdir):
            # emulate a crash after facts but before the status marker
            with open(os.path.join(tmpdir, "facts.csv"), "w") as fh:
                fh.write("relation,values\n")
            raise KeyboardInterrupt()

        try:
            stage_and_publish(out, build_without_status)
        except KeyboardInterrupt:
            pass
        assert not os.path.exists(out)


def test_incomplete_run_writes_no_complete_status():
    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "pkg")
        sc = dict(SCENARIO)
        sc["budgets"] = {"max_facts": 1}
        with redirect_stdout(io.StringIO()):
            rc = main(["complete", _scenario(tmp, sc), "-o", out])
        assert rc == 3  # incomplete
        pkg = Package(out)
        assert pkg.load()
        assert pkg.meta["status"] == "incomplete"
        assert pkg.meta["status"] != "complete"


def test_conflict_package_records_support_and_verify_does_not_claim_complete():
    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "pkg")
        sc = {
            "schema": {"C": ["text", "text"], "K": ["text", "text"]},
            "rules": [
                {"id": "k", "body": ["K(c, a)"], "head": "C(c, a)"},
                {"id": "e", "body": ["C(c, a1)", "C(c, a2)"], "eq": ["a1", "a2"]},
            ],
            "facts": {"K": [["c", "东"], ["c", "西"]]},
        }
        with redirect_stdout(io.StringIO()):
            rc = main(["complete", _scenario(tmp, sc), "-o", out])
        assert rc == 2
        pkg = Package(out)
        assert pkg.load()
        assert pkg.meta["status"] == "conflict"
        assert os.path.isfile(os.path.join(out, "conflict.json"))
        report = verify_package_dir(out)
        assert report["claimed_status"] == "conflict"
        # the instance is genuinely inconsistent, so the EGD check fails
        assert any(v.get("kind") == "egd" for v in report["violations"])


def test_force_replaces_existing_package():
    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "pkg")
        scen = _scenario(tmp)
        with redirect_stdout(io.StringIO()):
            assert main(["complete", scen, "-o", out]) == 0
        with open(os.path.join(out, "facts.csv")) as fh:
            before = fh.read()
        sc2 = json.loads(json.dumps(SCENARIO))
        sc2["facts"] = {"A": [["k1"], ["k2"], ["k3"]]}
        path2 = os.path.join(tmp, "s2.json")
        with open(path2, "w") as fh:
            json.dump(sc2, fh)
        with redirect_stdout(io.StringIO()):
            assert main(["complete", path2, "-o", out, "--force"]) == 0
        with open(os.path.join(out, "facts.csv")) as fh:
            after = fh.read()
        assert before != after
        assert verify_package_dir(out)["ok"]


def test_trace_explains_one_row_and_one_unknown():
    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "pkg")
        with redirect_stdout(io.StringIO()):
            assert main(["complete", _scenario(tmp), "-o", out]) == 0
        pkg = Package(out)
        pkg.load()
        live = pkg.nulls["live"]
        assert len(live) == 2
        buf = io.StringIO()
        with redirect_stdout(buf):
            assert main(["trace", out, "--null", live[0]]) == 0
        trace = json.loads(buf.getvalue())
        kinds = {s["kind"] for s in trace["sections"]}
        assert "created_by_trigger" in kinds
        assert trace["facts"], "live null must be reachable in the instance"

        buf = io.StringIO()
        with redirect_stdout(buf):
            assert main(["trace", out, "--fact", f'B("k1", NULL:{live[0]})']) == 0
        rows = json.loads(buf.getvalue())
        assert rows and rows[0]["rule"] == "t"
        assert rows[0]["kind"] == "derived"
