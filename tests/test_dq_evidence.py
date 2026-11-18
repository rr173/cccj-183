"""Evidence completeness and bounded-memory streaming details.

* a reference rule with an unknown key never guesses a pass;
* a target fact whose provenance is absent makes the audit fail instead of
  emitting a pass with unverifiable evidence;
* the payload/manifest streaming readers stay bounded (small memory, linear
  time) and handle chunk boundaries;
* the decompression of a highly compressible blob is bounded (a 1 MB
  compressed chunk is not inflated in one piece).
"""

from __future__ import annotations

import json
import os
import resource
import sqlite3
import tempfile
import time
import zlib

from chase.dqaudit import (
    AuditError,
    _decompress_blob,
    iter_json_section,
    run_audit,
)
from chase.workspace import Workspace

from tests.test_dq_audit import PROGRAM, build_workspace, put, tmpdir


def _peak_mb():
    try:
        with open("/proc/self/status", encoding="ascii") as fh:
            for line in fh:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) / 1024.0
    except OSError:
        pass
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def test_streaming_manifest_reader_is_linear_and_bounded():
    d = tempfile.mkdtemp(prefix="dq-stream-")
    path = os.path.join(d, "payload.json")
    # ~20 MB manifest written as compact JSON with tricky strings inside rows
    n = 40000
    with open(path, "w", encoding="utf-8") as fh:
        fh.write('{"before": 1, "fact_sources": [')
        for i in range(n):
            if i:
                fh.write(",")
            fh.write(json.dumps({"fact": {"relation": "R",
                                          "values": [{"kind": "string",
                                                      "value": f"v{i},]\\\"x"}]},
                                 "records": [["s", f"r{i}"]]}))
        fh.write('], "after": [1,2,3]}')
    before = _peak_mb()
    t0 = time.monotonic()
    seen = list(iter_json_section(path, "fact_sources", chunk=4096))
    elapsed = time.monotonic() - t0
    assert len(seen) == n
    assert seen[0]["records"] == [["s", "r0"]]
    assert seen[-1]["fact"]["values"][0]["value"] == f"v{n - 1},]\\\"x"
    # linear time (a quadratic rolling buffer would take minutes here)
    assert elapsed < 12, elapsed
    # and the whole 20 MB document is never resident
    assert _peak_mb() - before < 200, (_peak_mb(), before)


def test_decompression_is_bounded_for_highly_compressible_payloads():
    d = tempfile.mkdtemp(prefix="dq-inflate-")
    db = os.path.join(d, "t.db")
    data = b'{"pad": "' + b"a" * 40_000_000 + b'"}'   # compresses to ~40 KB
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE v(id INTEGER PRIMARY KEY, b BLOB)")
    conn.execute("INSERT INTO v VALUES (1, ?)", (zlib.compress(data),))
    conn.commit()
    conn.close()
    ro = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    out = os.path.join(d, "out.json")
    before = _peak_mb()
    digest = _decompress_blob(ro, "v", "b", 1, out, write=True)
    assert os.path.getsize(out) == len(data)
    # 40 MB of output must not mean 40 MB of Python strings
    assert _peak_mb() - before < 20, (_peak_mb(), before)
    assert digest


def test_missing_provenance_fails_instead_of_passing():
    """A completion whose completed facts carry no provenance cannot pass."""
    d = tmpdir("noprov")
    ws_path = os.path.join(d, "ws")
    prog = {"schema": {"Src": ["text", "text"], "Done": ["text", "text"]},
            "rules": [{"id": "t_copy", "body": ["Src(a, b)"],
                       "head": "Done(a, b)"}]}
    ws = Workspace.init(ws_path, prog)
    ws.commit({"batch_id": "b1", "ops": [
        put("s", "r1", "Src", ["k", "v"]),
        put("s", "r2", "Done", ["k", "v"])]})
    ws.close()
    # remove the provenance blob from the stored payload, simulating a
    # version whose evidence is incomplete
    conn = sqlite3.connect(os.path.join(ws_path, "workspace.sqlite"))
    conn.execute("UPDATE versions SET provenance = NULL")
    conn.commit()
    conn.close()
    doc = {"format": "data-quality-policy/1", "policy_id": "dq.noprov",
           "revision": 1, "sample_limit": 3,
           "rules": [{"id": "ref-done", "kind": "reference",
                      "layer": "sources", "relation": "Src",
                      "columns": ["c0"],
                      "target": {"relation": "Done", "columns": ["c0"]},
                      "severity": "error"}]}
    out = os.path.join(d, "report.json")
    try:
        run_audit(ws_path, doc, output=out)
    except AuditError as exc:
        assert "provenance" in str(exc)
    else:
        raise AssertionError("audit emitted a report without evidence")
    assert not os.path.exists(out)
    assert not os.path.exists(os.path.join(ws_path, "dq_audits.sqlite"))


def test_unknown_reference_key_is_never_a_pass_in_any_layer():
    d, ws_path = build_workspace("unk-ref")
    for layer, rel, col in (("sources", "KnownRegion", "c1"),
                            ("completed", "Customer", "c1")):
        doc = {"format": "data-quality-policy/1", "policy_id": f"dq.u.{layer}",
               "revision": 1, "sample_limit": 5,
               "rules": [{"id": "ref", "kind": "reference", "layer": layer,
                          "relation": rel, "columns": [col],
                          "target": {"relation": "KnownRegion",
                                     "columns": ["c1"]},
                          "severity": "error"}]}
        report = run_audit(ws_path, doc, persist=False)
        rule = report["rules"][0]
        assert rule["counts"]["indeterminate"] >= 1, (layer, rule)
        assert rule["status"] == "indeterminate"
        for sample in rule["samples"]:
            assert sample["category"] == "indeterminate"
