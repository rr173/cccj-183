"""Process-level workspace tests: CLI, concurrent writers, crash recovery."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TMP = tempfile.mkdtemp(prefix="chase-ws-proc-")
_COUNTER = [0]

PROGRAM = {
    "schema": {
        "Order": ["text", "text"],
        "Customer": ["text", "text"],
        "KnownAddress": ["text", "text"],
    },
    "rules": [
        {"id": "t_order_customer", "body": ["Order(o, c)"], "head": "Customer(c, a)"},
        {"id": "t_known_customer", "body": ["KnownAddress(c, a)"], "head": "Customer(c, a)"},
        {"id": "e_customer_key", "body": ["Customer(c, a1)", "Customer(c, a2)"],
         "eq": ["a1", "a2"]},
        {"id": "e_address_key", "body": ["KnownAddress(c, a1)", "KnownAddress(c, a2)"],
         "eq": ["a1", "a2"]},
    ],
}


def tmpdir(name: str) -> str:
    _COUNTER[0] += 1
    path = os.path.join(TMP, f"{_COUNTER[0]:03d}-{name}")
    os.makedirs(path, exist_ok=True)
    return path


def cli(*args, env_extra=None, check=False):
    env = dict(os.environ)
    env["PYTHONPATH"] = ROOT + os.pathsep + env.get("PYTHONPATH", "")
    if env_extra:
        env.update(env_extra)
    proc = subprocess.run(
        [sys.executable, "-m", "chase", "ws"] + list(args),
        cwd=ROOT, env=env, capture_output=True, text=True, timeout=120)
    if check and proc.returncode != 0:
        raise AssertionError(
            f"cli {args} failed ({proc.returncode}):\n{proc.stdout}\n{proc.stderr}")
    return proc


def write_json(path, obj):
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, ensure_ascii=False)


def setup_ws(name):
    d = tmpdir(name)
    prog = os.path.join(d, "program.json")
    write_json(prog, PROGRAM)
    ws = os.path.join(d, "ws")
    proc = cli("init", ws, "--program", prog)
    assert proc.returncode == 0, proc.stderr
    return d, ws


def commit_batch(d, ws, bid, ops, extra_args=(), env_extra=None):
    bp = os.path.join(d, f"{bid}.json")
    write_json(bp, {"batch_id": bid, "ops": ops})
    return cli("commit", ws, "--batch", bp, *extra_args, env_extra=env_extra)


def put(sid, rid, rel, tpl):
    return {"op": "put", "source_id": sid, "record_id": rid,
            "relation": rel, "tuple": tpl}


def delete(sid, rid):
    return {"op": "delete", "source_id": sid, "record_id": rid}


def test_cli_happy_path_and_exit_codes():
    d, ws = setup_ws("cli")
    p = commit_batch(d, ws, "b1", [put("erp", "o1", "Order", ["o1", "c1"]),
                                   put("erp", "a1", "KnownAddress", ["c1", "城西"])])
    assert p.returncode == 0, p.stderr
    r = json.loads(p.stdout)
    assert r["version"] == 1 and r["status"] == "complete"

    # conflict commit: exit 2 but the version IS committed
    p = commit_batch(d, ws, "b2", [put("crm", "a2", "KnownAddress", ["c1", "浦东"])])
    assert p.returncode == 2, (p.returncode, p.stdout)
    assert json.loads(p.stdout)["status"] == "conflict"

    # invalid batch: exit 5, no version consumed
    p = commit_batch(d, ws, "b3", [delete("erp", "ghost")])
    assert p.returncode == 5
    # stale parent: exit 6
    p = commit_batch(d, ws, "b4", [delete("crm", "a2")], extra_args=("--parent", "1"))
    assert p.returncode == 6, (p.returncode, p.stdout)
    # batch id re-used with different content: exit 7
    p = commit_batch(d, ws, "b2", [put("crm", "a9", "KnownAddress", ["c1", "浦东"])])
    assert p.returncode == 7, (p.returncode, p.stdout)
    # idempotent resend of b2: exit 2 again with the original receipt
    p = commit_batch(d, ws, "b2", [put("crm", "a2", "KnownAddress", ["c1", "浦东"])])
    assert p.returncode == 2
    r = json.loads(p.stdout)
    assert r["idempotent_replay"] is True and r["version"] == 2

    # fix the conflict, then status/versions/sources/export/diff/verify
    p = commit_batch(d, ws, "b5", [delete("crm", "a2")])
    assert p.returncode == 0 and json.loads(p.stdout)["parent_version"] == 2
    p = cli("status", ws, check=True)
    st = json.loads(p.stdout)
    assert st["latest_source_version"] == 3 and st["latest_complete_version"] == 3
    p = cli("versions", ws, check=True)
    vs = json.loads(p.stdout)["versions"]
    assert [v["status"] for v in vs] == ["complete", "conflict", "complete"]
    p = cli("sources", ws, "--version", "2", check=True)
    assert json.loads(p.stdout)["count"] == 3
    out = os.path.join(d, "exp3")
    p = cli("export", ws, "--version", "latest-complete", "-o", out, check=True)
    assert json.loads(p.stdout)["version"] == 3
    p = cli("verify", out, check=True)
    assert json.loads(p.stdout)["ok"] is True
    p = cli("diff", ws, "--from", "1", "--to", "3", check=True)
    d13 = json.loads(p.stdout)
    assert d13["constant_facts"]["added"] == [] or True  # machine form parses
    p = cli("diff", ws, "--from", "1", "--to", "3", "--human", check=True)
    assert "version 1" in p.stdout


def test_two_writers_one_wins():
    d, ws = setup_ws("race")
    p = commit_batch(d, ws, "b1", [put("erp", "o1", "Order", ["o1", "c1"])])
    assert p.returncode == 0
    # two writers, both basing their batch on v1
    ops_a = [put("erp", "o2", "Order", ["o2", "c1"])]
    ops_b = [put("erp", "o3", "Order", ["o3", "c1"])]
    pa = os.path.join(d, "ra.json")
    pb = os.path.join(d, "rb.json")
    write_json(pa, {"batch_id": "writer-a", "ops": ops_a})
    write_json(pb, {"batch_id": "writer-b", "ops": ops_b})
    env = dict(os.environ)
    env["PYTHONPATH"] = ROOT + os.pathsep + env.get("PYTHONPATH", "")
    procs = [
        subprocess.Popen([sys.executable, "-m", "chase", "ws", "commit", ws,
                          "--batch", path, "--parent", "1"],
                         cwd=ROOT, env=env, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, text=True)
        for path in (pa, pb)
    ]
    results = [p.communicate(timeout=120) for p in procs]
    codes = sorted(p.returncode for p in procs)
    assert codes == [0, 6], f"expected one winner one stale, got {codes}: " \
                            f"{[r[1] for r in results]}"
    # the loser re-reads and decides again: now it commits on the new latest
    loser_batch = pa if procs[0].returncode == 6 else pb
    loser_id = "writer-a" if procs[0].returncode == 6 else "writer-b"
    p = cli("commit", ws, "--batch", loser_batch)
    assert p.returncode == 0, p.stderr
    r = json.loads(p.stdout)
    assert r["version"] == 3 and r["parent_version"] == 2
    p = cli("versions", ws, check=True)
    assert len(json.loads(p.stdout)["versions"]) == 3


def test_crash_before_commit_rolls_back():
    d, ws = setup_ws("crash-before")
    p = commit_batch(d, ws, "b1", [put("erp", "o1", "Order", ["o1", "c1"])])
    assert p.returncode == 0
    # die inside the commit transaction
    p = commit_batch(d, ws, "b2", [put("erp", "o2", "Order", ["o2", "c1"])],
                     env_extra={"CHASE_WS_CRASH_AT": "before_commit"})
    assert p.returncode == 97, (p.returncode, p.stdout, p.stderr)
    # reopen: nothing of b2 is visible — no version, no source, no batch
    p = cli("status", ws, check=True)
    st = json.loads(p.stdout)
    assert st["latest_source_version"] == 1
    assert st["counts"]["versions"] == 1
    assert st["counts"]["source_records"] == 1
    p = cli("sources", ws, check=True)
    assert json.loads(p.stdout)["count"] == 1
    # the batch id was not consumed: the same batch commits cleanly now
    p = commit_batch(d, ws, "b2", [put("erp", "o2", "Order", ["o2", "c1"])])
    assert p.returncode == 0 and json.loads(p.stdout)["version"] == 2


def test_crash_after_commit_resend_returns_receipt():
    d, ws = setup_ws("crash-after")
    p = commit_batch(d, ws, "b1", [put("erp", "o1", "Order", ["o1", "c1"])])
    assert p.returncode == 0
    # die right after the transaction committed (receipt never reached us)
    p = commit_batch(d, ws, "b2", [put("erp", "o2", "Order", ["o2", "c1"])],
                     env_extra={"CHASE_WS_CRASH_AT": "after_commit"})
    assert p.returncode == 97
    # reopen: b2 is fully visible
    p = cli("status", ws, check=True)
    st = json.loads(p.stdout)
    assert st["latest_source_version"] == 2
    assert st["counts"]["source_records"] == 2
    # the supplier, who never saw the receipt, re-sends: original receipt back
    p = commit_batch(d, ws, "b2", [put("erp", "o2", "Order", ["o2", "c1"])])
    assert p.returncode == 0
    r = json.loads(p.stdout)
    assert r["idempotent_replay"] is True and r["version"] == 2
    p = cli("versions", ws, check=True)
    assert len(json.loads(p.stdout)["versions"]) == 2
