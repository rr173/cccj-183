"""Concurrency tests for version-bound queries.

A commit landing *while* a query is running must not let one multi-relation
query read facts from two versions.  The query process holds a read
transaction (SQLite snapshot) from version resolution until it finishes;
the test-only ``CHASE_WSQUERY_PAUSE`` hook parks the process at two points
inside that transaction while another process commits a competing version.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time

from chase.workspace import Workspace
from chase.wsquery import run_query

from .test_workspace_query import (
    CUSTOMER_PAIRS,
    EAST_ORDERS,
    PROGRAM,
    ROOT,
    base_ops,
    put,
)

TMP = tempfile.mkdtemp(prefix="chase-wsquery-conc-")
_COUNTER = [0]


def write_json(path, obj):
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, ensure_ascii=False)


def tmpdir(name: str) -> str:
    _COUNTER[0] += 1
    path = os.path.join(TMP, f"{_COUNTER[0]:03d}-{name}")
    os.makedirs(path, exist_ok=True)
    return path


def build_ws(name):
    d = tmpdir(name)
    ws_path = os.path.join(d, "ws")
    ws = Workspace.init(ws_path, PROGRAM)
    ws.commit({"batch_id": "b1", "ops": base_ops()})
    ws.commit({"batch_id": "b2",
               "ops": [put("crm", "r2b", "KnownRegion", ["c2", "东区"])]})
    ws.close()
    return d, ws_path


def _env(extra=None):
    env = dict(os.environ)
    env["PYTHONPATH"] = ROOT + os.pathsep + env.get("PYTHONPATH", "")
    if extra:
        env.update(extra)
    return env


def _cli(args, env, timeout=120):
    return subprocess.run([sys.executable, "-m", "chase"] + args,
                          cwd=ROOT, env=env, capture_output=True,
                          text=True, timeout=timeout)


def _concurrent_commit(ws_path, batch_path, delay=0.4):
    """Subprocess that waits briefly, then commits a new complete version."""
    script = (
        "import sys, time; sys.path.insert(0, %r);\n"
        "time.sleep(%r);\n"
        "from chase.workspace import Workspace;\n"
        "ws = Workspace(%r);\n"
        "r = ws.commit(json.load(open(%r, encoding='utf-8')));\n"
        "print(r['version'], r['status']); ws.close()\n"
        % (ROOT, delay, ws_path, batch_path))
    env = _env()
    return subprocess.Popen([sys.executable, "-I", "-c",
                             "import json\n" + script],
                            cwd=ROOT, env=env, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True)


def test_commit_during_query_keeps_pinned_snapshot():
    for hook in ("after_version_pinned", "after_store_loaded"):
        d, ws_path = build_ws("race-" + hook)
        q_path = os.path.join(d, "q.json")
        write_json(q_path, EAST_ORDERS)
        batch_path = os.path.join(d, "b5.json")
        write_json(batch_path, {
            "batch_id": "b5-during-query-" + hook,
            "ops": [put("crm", "r9", "KnownRegion", ["c3", "东区"])]})

        writer = _concurrent_commit(ws_path, batch_path)
        # query starts first, parks inside its open read transaction while the
        # competing commit lands and moves latest-complete to v3
        proc = _cli(
            ["ws", "query", ws_path, "--query", q_path,
             "--version", "latest-complete"],
            _env({"CHASE_WSQUERY_PAUSE": hook}))
        out, err = writer.communicate(timeout=120)
        assert proc.returncode == 0, (proc.stdout, proc.stderr, err)
        env = json.loads(proc.stdout)
        # the whole answer comes from the version pinned at start (v2)...
        assert env["bound_version"]["version"] == 2
        assert env["bound_version"]["latest_complete_version_at_bind"] == 2
        # ...even though the writer's commit did land
        wv, wstatus = out.strip().split()
        assert wv == "3" and wstatus == "complete"
        # a re-query now binds the new version and sees the new fact
        again = _cli(["ws", "query", ws_path, "--query", q_path,
                      "--version", "latest-complete"], _env())
        env2 = json.loads(again.stdout)
        assert env2["bound_version"]["version"] == 3
        ids = {r[0]["value"] for r in env2["results"]}
        assert "o3" in ids and "o3" not in {
            r[0]["value"] for r in env["results"]}


def test_explicit_historical_version_immune_to_concurrent_commit():
    d, ws_path = build_ws("hist")
    q_path = os.path.join(d, "q.json")
    write_json(q_path, CUSTOMER_PAIRS)
    batch_path = os.path.join(d, "b.json")
    write_json(batch_path, {"batch_id": "b-hist",
                            "ops": [put("crm", "r9",
                                        "KnownRegion", ["c3", "东区"])]})
    writer = _concurrent_commit(ws_path, batch_path)
    proc = _cli(
        ["ws", "query", ws_path, "--query", q_path, "--version", "1"],
        _env({"CHASE_WSQUERY_PAUSE": "after_version_pinned"}))
    writer.communicate(timeout=120)
    assert proc.returncode == 0, proc.stderr
    env = json.loads(proc.stdout)
    assert env["bound_version"]["version"] == 1
    # the historical answer is exactly the pre-commit v1 answer
    expected = run_query(ws_path, CUSTOMER_PAIRS, "1")
    assert env["results"] == expected["results"]
    # one pinned version named end to end; no mixed-version reporting
    assert env["bound_version"]["requested_spec"] == "1"
    assert env["bound_version"]["completion_status"] == "complete"


def test_snapshot_used_by_every_relation_of_one_query():
    # A three-relation query whose scratch store is fully built before the
    # competing commit: all joins must still resolve against one version.
    d, ws_path = build_ws("snap")
    q = {
        "relations": [{"name": "Order", "alias": "o"},
                      {"name": "Customer", "alias": "c"},
                      {"name": "Item", "alias": "it"}],
        "select": [{"column": "o.c0", "name": "oid"},
                   {"column": "c.c1", "name": "region"}],
        "where": [{"eq": ["o.c1", "c.c0"]},
                  {"eq": ["o.c0", "it.c0"]},
                  {"eq": ["it.c2", {"type": "int", "value": 42}]}],
    }
    q_path = os.path.join(d, "q.json")
    write_json(q_path, q)
    batch_path = os.path.join(d, "b.json")
    write_json(batch_path, {"batch_id": "b-snap",
                            "ops": [put("erp", "o99",
                                        "Order", ["o99", "c1"])]})
    writer = _concurrent_commit(ws_path, batch_path)
    proc = _cli(
        ["ws", "query", ws_path, "--query", q_path,
         "--version", "latest-complete"],
        _env({"CHASE_WSQUERY_PAUSE": "after_store_loaded"}))
    writer.communicate(timeout=120)
    assert proc.returncode == 0, proc.stderr
    env = json.loads(proc.stdout)
    ids = {r[0]["value"] for r in env["results"]}
    # o99 exists only in the concurrently committed v3; the pinned v2 snapshot
    # must not surface it through any participating relation
    assert "o99" not in ids
    assert env["bound_version"]["version"] == 2
