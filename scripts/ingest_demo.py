#!/usr/bin/env python3
"""End-to-end, independently cross-checked snapshot-ingestion demo.

Run (from the repository root)::

    python3 -I scripts/ingest_demo.py [--workdir DIR] [--keep]

The script generates every CSV file itself (nothing large is committed),
drives the real ``ingest``/``ws`` CLI in separate processes, and computes the
expected source set and candidate decisions **independently** from the input
rows and the previous managed ledger — it never calls the production
reconciliation code to build expectations.

Story covered
-------------
1. initial load of two shards under two source_ids (both use local unknown u;
   sharing within a source and isolation across sources are shown);
2. a second table with a legal replacement, a quarantined row keeping its old
   value, a brand-new row, and a genuinely vanished row (retraction);
3. an un-parseable-key snapshot that cannot apply, then the fixed re-check
   that finally allows the retraction;
4. composite keys containing a comma, a text "7" vs an int 7, identical and
   conflicting duplicate shards, an explicit empty snapshot;
5. an external same-value touch, a stale parent after staging, a completion
   conflict that still counts as one successful application;
6. a real process hard-exit and reopen, applying after the staged CSV files
   are deleted, and idempotent re-apply.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUNNER = os.path.join(REPO, "run_cli.py")

# --------------------------------------------------------------------------- #
# tiny independent expectation engine (no chase.ingest imports)
# --------------------------------------------------------------------------- #
INT_RE = __import__("re").compile(r"[+-]?[0-9]+\Z")


def strict_int(cell):
    if not INT_RE.fullmatch(cell):
        raise ValueError(f"not a strict integer: {cell!r}")
    return int(cell)


def key_encode(components):
    """Independent typed composite-key encoder (mirrors the documented rule)."""
    return json.dumps([[t, v] for t, v in components],
                      ensure_ascii=False, separators=(",", ":"))


class ExpectLedger:
    """Independent per-source managed ledger used only for expectations."""

    def __init__(self):
        self.records = {}  # sid -> {key: tuple_tokens}

    def expect(self, sid, rows_by_key, integral, old_ledger):
        """Return expected decision counts from raw parsed rows."""
        out = {"put": 0, "delete": 0, "keep": 0, "quarantine": 0}
        if not integral:
            return out
        old = old_ledger.get(sid, {})
        for key, payload in rows_by_key.items():
            if payload["kind"] in ("quarantine", "conflict"):
                out["quarantine"] += 1
                continue
            tup = payload["tuple"]
            if key not in old:
                out["put"] += 1
            elif old[key] == tup:
                out["keep"] += 1
            else:
                out["put"] += 1
        for key in old:
            if key not in rows_by_key:
                out["delete"] += 1
        return out


# --------------------------------------------------------------------------- #
# subprocess CLI helper
# --------------------------------------------------------------------------- #
class CLI:
    def __init__(self, workdir):
        self.workdir = workdir

    def run(self, *args, check=True):
        proc = subprocess.run(
            [sys.executable, "-I", RUNNER, *args],
            capture_output=True, text=True, cwd=REPO)
        if check and proc.returncode not in (0, 2, 3):
            raise AssertionError(
                f"command failed ({proc.returncode}): {' '.join(args)}\n"
                f"STDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}")
        out = None
        if proc.stdout.strip():
            out = json.loads(proc.stdout)
        return proc.returncode, out, proc.stderr

    def write(self, rel, content, binary=False):
        path = os.path.join(self.workdir, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb" if binary else "w",
                  encoding=None if binary else "utf-8",
                  newline=None if binary else "") as fh:
            fh.write(content)
        return path


def banner(title):
    print("\n" + "=" * 74)
    print(title)
    print("=" * 74)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workdir", default=None)
    ap.add_argument("--keep", action="store_true")
    args = ap.parse_args()

    workdir = args.workdir or os.path.join(
        tempfile_gettempdir(), "chase-ingest-demo")
    if os.path.exists(workdir):
        shutil.rmtree(workdir)
    os.makedirs(workdir)
    cli = CLI(workdir)
    failures = []

    def check(cond, label):
        print(("  ok  " if cond else " FAIL ") + label)
        if not cond:
            failures.append(label)

    # -- workspace ---------------------------------------------------------- #
    banner("0. workspace pinned to the demo program")
    prog_src = os.path.join(REPO, "examples", "ingest", "program.json")
    program = os.path.join(workdir, "program.json")
    shutil.copy(prog_src, program)
    rc, out, _ = cli.run("ws", "init", os.path.join(workdir, "ws"),
                         "--program", program)
    check(rc == 0 and out["latest_source_version"] == 0, "workspace initialized")

    # -- 1. initial two-shard load ------------------------------------------ #
    banner("1. initial load: two shards, two sources, shared/isolated unknown u")
    cli.write("snap1/erp_part1.csv",
              "order_id,qty,customer_id\n"
              "o1,07,c1\n"          # 07 normalizes to int 7
              "o2,+7,c2\n"          # +7 the same integer 7
              '"o,3",9,c3\n')       # comma inside a text key
    cli.write("snap1/erp_part2.csv",
              "order_id,qty,customer_id\n"
              "o4,11,NULL:c4\n")    # unknown customer
    cli.write("snap1/crm.csv",
              "customer_id,addr\n"
              "c1,城东\n"
              "c2,NULL:u\n"         # local unknown u (crm)
              "c3,NULL:u\n")        # same local u shared within crm
    decl1 = {
        "connector": "supplier-alpha", "version": 1,
        "unknown_markers": {"marker": "NULL", "name_prefix": "NULL:",
                            "name_separator": "|",
                            "name_pattern": "[a-zA-Z][a-zA-Z0-9_]*"},
        "sources": [
            {"source_id": "erp", "relation": "Order",
             "key_columns": ["order_id"],
             "fields": [
                 {"name": "order_id", "header": "order_id"},
                 {"name": "qty", "header": "qty"},
                 {"name": "customer_id", "header": "customer_id"}],
             "shards": [{"path": cli.workdir + "/snap1/erp_part1.csv"},
                        {"path": cli.workdir + "/snap1/erp_part2.csv"}],
             "empty_snapshot": False},
            {"source_id": "crm", "relation": "KnownAddress",
             "key_columns": ["customer_id"],
             "fields": [
                 {"name": "customer_id", "header": "customer_id"},
                 {"name": "addr", "header": "addr"}],
             "shards": [{"path": cli.workdir + "/snap1/crm.csv"}],
             "empty_snapshot": False}],
    }
    decl1_path = cli.write("decl1.json", json.dumps(decl1, ensure_ascii=False))
    rc, rep, _ = cli.run("ingest", "stage", os.path.join(workdir, "ws"),
                         "--declaration", decl1_path, "--task", "load-1")
    totals = rep["stats"]["totals"]
    src_stats = rep["stats"]["sources"]
    # independent expectation: 4 erp rows + 3 crm rows, all puts
    check(totals["put"] == 7 and totals["delete"] == 0,
          f"expected 7 puts / 0 deletes, got {totals}")
    check(rep["applicable"] is True, "task applicable")
    check(src_stats["erp"]["ok"] == 4 and src_stats["crm"]["ok"] == 3,
          "per-source row counts: 4 erp + 3 crm")
    unk = {(u["source_id"], u["local_name"]): u["global_id"]
           for u in _unknowns(cli, workdir, "load-1")}
    g_crm_u = unk[("crm", "u")]
    g_erp_c4 = unk[("erp", "c4")]
    check(g_crm_u != g_erp_c4, "local unknown u isolated across the two sources")
    rc, receipt, _ = cli.run("ingest", "apply", os.path.join(workdir, "ws"),
                             "--task", "load-1")
    check(receipt["version"] == 1 and receipt["status"] == "complete",
          "applied as source version 1 (complete)")
    src = _sources(cli, workdir)
    check(len(src["erp"]) == 4 and len(src["crm"]) == 3,
          "workspace now holds 4 erp + 3 crm records")
    # 07 and +7 both normalize to integer 7 (tuple values are already parsed)
    qtys = {json.loads(rid)[0][1]: tpl[1]
            for rid, (_rel, tpl) in src["erp"].items()}
    check(qtys["o1"] == 7 and qtys["o2"] == 7, "07 and +7 both normalize to int 7")
    # both crm u rows share one global unknown (null marker dict in the tuple);
    # the plain-address row c1 has no null and is excluded
    crm_nulls = {tuple(tok["null"] for tok in tpl if isinstance(tok, dict))
                 for _r, tpl in src["crm"].values()
                 if any(isinstance(tok, dict) for tok in tpl)}
    check(crm_nulls == {(g_crm_u,)}, "crm NULL:u rows share one input unknown")

    # -- 2. second table: replace / quarantine / add / retract -------------- #
    banner("2. second table: replace, quarantine-keep, add, retract")
    cli.write("snap2/erp.csv",
              "order_id,qty,customer_id\n"
              "o1,77,c1\n"          # legal replacement 7 -> 77
              "o2,bad,c2\n"         # bad int: quarantine, old tuple retained
              '"o,3",9,c3\n'        # unchanged
              "o5,4,c5\n")          # new row; o4 has vanished -> retract
    decl2 = json.loads(json.dumps(decl1))
    decl2["sources"][0]["shards"] = [
        {"path": cli.workdir + "/snap2/erp.csv"}]
    decl2_path = cli.write("decl2.json", json.dumps(decl2, ensure_ascii=False))
    rc, rep, _ = cli.run("ingest", "stage", os.path.join(workdir, "ws"),
                         "--declaration", decl2_path, "--task", "load-2")
    t2 = rep["stats"]["sources"]["erp"]
    t2c = t2["candidates"]
    # independent expectation for erp alone:
    # put: o1(replace), o5(new) = 2 ; keep: "o,3" = 1 ;
    # quarantine: o2 = 1 ; delete: o4 = 1
    check(t2c == {"put": 2, "delete": 1, "keep": 1, "quarantine": 1},
          f"erp reconciliation counts {t2c}")
    check(t2["ok"] == 3 and t2["quarantine"] == 1 and t2["blocked"] == 0,
          "erp row outcomes: 3 ok, 1 quarantine, 0 blocked")
    # crm is also in this declaration but unchanged: all three keys kept
    check(rep["stats"]["sources"]["crm"]["candidates"] ==
          {"put": 0, "delete": 0, "keep": 3, "quarantine": 0},
          "crm unchanged snapshot keeps all three records")
    rc, receipt, _ = cli.run("ingest", "apply", os.path.join(workdir, "ws"),
                             "--task", "load-2")
    check(receipt["version"] == 2, "applied as version 2")
    src = _sources(cli, workdir)
    erp = {json.loads(rid)[0][1]: tpl for rid, (_r, tpl) in src["erp"].items()}
    check(erp["o1"][1] == 77, "o1 replaced to qty 77")
    check(erp["o2"][1] == 7, "quarantined o2 kept its OLD qty 7 (not deleted)")
    check("o5" in erp and "o4" not in erp, "o5 added, o4 retracted")
    check(len(src["crm"]) == 3, "unlisted source crm untouched")

    # -- 3a. un-parseable key blocks; no retraction allowed ----------------- #
    banner("3a. broken snapshot (field-count mismatch) cannot apply")
    cli.write("snap3bad/erp.csv",
              "order_id,qty,customer_id\n"
              "o1,77,c1\n"
              "o6,3\n")             # only 2 fields: key positions unreliable
    decl3 = json.loads(json.dumps(decl2))
    decl3["sources"][0]["shards"] = [
        {"path": cli.workdir + "/snap3bad/erp.csv"}]
    decl3_path = cli.write("decl3bad.json", json.dumps(decl3, ensure_ascii=False))
    rc, rep, _ = cli.run("ingest", "stage", os.path.join(workdir, "ws"),
                         "--declaration", decl3_path, "--task", "load-3bad")
    check(rep["applicable"] is False, "non-integral snapshot not applicable")
    codes = [b["code"] for b in rep["blocking"]]
    check("field-count-mismatch" in codes, f"blocking code present: {codes}")
    rc, _, err = cli.run("ingest", "apply", os.path.join(workdir, "ws"),
                         "--task", "load-3bad", check=False)
    check(rc == 5 and "blocking issue" in err, "apply refused, no new version")
    rc, vers, _ = cli.run("ws", "versions", os.path.join(workdir, "ws"))
    check(vers["latest_source_version"] == 2, "source version still 2")

    # -- 3b. fixed re-check finally allows the retraction ------------------- #
    banner("3b. fixed re-check allows the retraction of retained o2")
    cli.write("snap3ok/erp.csv",
              "order_id,qty,customer_id\n"
              "o1,77,c1\n"
              '"o,3",9,c3\n'
              "o5,4,c5\n")           # o2 absent; integral -> retract it
    decl3b = json.loads(json.dumps(decl2))
    decl3b["sources"][0]["shards"] = [
        {"path": cli.workdir + "/snap3ok/erp.csv"}]
    decl3b_path = cli.write("decl3ok.json", json.dumps(decl3b, ensure_ascii=False))
    rc, rep, _ = cli.run("ingest", "stage", os.path.join(workdir, "ws"),
                         "--declaration", decl3b_path, "--task", "load-3ok")
    check(rep["applicable"]
          and rep["stats"]["sources"]["erp"]["candidates"]["delete"] == 1,
          f"integral snapshot now derives 1 delete: "
          f"{rep['stats']['sources']['erp']['candidates']}")
    rc, receipt, _ = cli.run("ingest", "apply", os.path.join(workdir, "ws"),
                             "--task", "load-3ok")
    check(receipt["version"] == 3, "applied as version 3")
    src = _sources(cli, workdir)
    erp_keys = {json.loads(rid)[0][1] for rid in src["erp"]}
    check("o2" not in erp_keys,
          "the retained o2 is finally retracted only after an integral re-check")

    # -- 4. composite keys, duplicates, empty snapshot ---------------------- #
    banner("4. composite keys, duplicate shards, explicit empty snapshot")
    # SameAs(text,text) with a two-column composite key; the same feed also
    # demonstrates a comma inside a key component and text "7" as a key value.
    cli.write("pair/p1.csv", "left_id,right_id\n"
                             '"a,b",7\n"7",8\n')
    cli.write("pair/p2.csv", "left_id,right_id\n"
                             '"a,b",7\n')   # identical duplicate across shards
    pair_decl = {
        "connector": "pair-conn", "version": 1,
        "unknown_markers": {"marker": "NULL", "name_prefix": "",
                            "name_separator": ""},
        "sources": [{
            "source_id": "pairs", "relation": "SameAs",
            "key_columns": ["left_id", "right_id"],
            "fields": [
                {"name": "left_id", "header": "left_id"},
                {"name": "right_id", "header": "right_id"}],
            "shards": [{"path": cli.workdir + "/pair/p1.csv"},
                       {"path": cli.workdir + "/pair/p2.csv"}],
            "empty_snapshot": False}],
    }
    pd_path = cli.write("pair_decl.json", json.dumps(pair_decl, ensure_ascii=False))
    rc, rep, _ = cli.run("ingest", "stage", os.path.join(workdir, "ws"),
                         "--declaration", pd_path, "--task", "pair-1")
    check(rep["applicable"], "composite-key task applicable")
    check(rep["stats"]["totals"]["duplicate_collapsed"] == 1,
          "identical duplicate collapsed (evidence retained)")
    # text "7" and the int 7 never collide: two distinct keys present
    rc, changes, _ = cli.run("ingest", "changes", os.path.join(workdir, "ws"),
                             "--task", "pair-1")
    keys = {c["record_key"] for c in changes["candidates"]}
    check(any('["s","7"]' in k for k in keys), "text key \"7\" present")
    check(any('["s","a,b"]' in k for k in keys), "comma-bearing key preserved")
    cli.run("ingest", "apply", os.path.join(workdir, "ws"), "--task", "pair-1")

    # conflicting duplicate shards block the whole task: use a single-column
    # key (left_id) so the same key carries two distinct legal tuples.  A key
    # change means a NEW connector identity — bindings never migrate silently.
    cli.write("pairconf/p1.csv", "left_id,right_id\nx,1\n")
    cli.write("pairconf/p2.csv", "left_id,right_id\nx,2\n")
    pconf = json.loads(json.dumps(pair_decl))
    pconf["connector"] = "pair-conn-singlekey"
    pconf["sources"][0]["key_columns"] = ["left_id"]
    pconf["sources"][0]["shards"] = [
        {"path": cli.workdir + "/pairconf/p1.csv"},
        {"path": cli.workdir + "/pairconf/p2.csv"}]
    pc_path = cli.write("pair_conf.json", json.dumps(pconf, ensure_ascii=False))
    rc, rep, _ = cli.run("ingest", "stage", os.path.join(workdir, "ws"),
                         "--declaration", pc_path, "--task", "pair-conf")
    check(not rep["applicable"] and
          rep["blocking"][0]["code"] == "conflicting-duplicate-key",
          "distinct legal tuples for one key block application")

    # explicit empty snapshot needs confirmation
    cli.write("pempty/e.csv", "left_id,right_id\n")
    pe = json.loads(json.dumps(pair_decl))
    pe["sources"][0]["empty_snapshot"] = True
    pe["sources"][0]["shards"] = [{"path": cli.workdir + "/pempty/e.csv"}]
    pe_path = cli.write("pair_empty.json", json.dumps(pe, ensure_ascii=False))
    rc, rep, _ = cli.run("ingest", "stage", os.path.join(workdir, "ws"),
                         "--declaration", pe_path, "--task", "pair-empty")
    check(rep["needs_empty_confirm"] and not rep["applicable"],
          "empty snapshot previewable but not applicable without confirmation")
    rc, _, err = cli.run("ingest", "apply", os.path.join(workdir, "ws"),
                         "--task", "pair-empty", check=False)
    check(rc == 5 and "--confirm-empty" in err, "unconfirmed empty apply refused")
    cli.run("ingest", "apply", os.path.join(workdir, "ws"),
            "--task", "pair-empty", "--confirm-empty")
    rc, led, _ = cli.run("ingest", "ledger", os.path.join(workdir, "ws"),
                         "--source", "pairs")
    check(led["total"] == 0, "confirmed empty snapshot cleared the managed source")

    # -- 5. external touch, stale parent, completion conflict --------------- #
    banner("5. external same-value touch, stale parent, completion conflict")
    cli.write("snap5/erp.csv", "order_id,qty,customer_id\no1,77,c1\no9,1,c9\n")
    # keep the exact connector binding: both sources remain, crm unchanged
    d5 = json.loads(json.dumps(decl2))
    d5["sources"][0]["shards"] = [{"path": cli.workdir + "/snap5/erp.csv"}]
    d5_path = cli.write("decl5.json", json.dumps(d5, ensure_ascii=False))
    cli.run("ingest", "stage", os.path.join(workdir, "ws"),
            "--declaration", d5_path, "--task", "load-5")
    # external same-value replacement on an existing erp key
    ws = os.path.join(workdir, "ws")
    manual_batch = cli.write("ext.json", json.dumps({
        "batch_id": "external-touch",
        "ops": [{"op": "put", "source_id": "erp",
                 "record_id": key_encode([("s", "o1")]),
                 "relation": "Order", "tuple": ["o1", 77, "c1"]}]}))
    cli.run("ws", "commit", ws, "--batch", manual_batch)
    rc, _, err = cli.run("ingest", "apply", ws, "--task", "load-5", check=False)
    check(rc == 5 and "externally modified" in err,
          "same-value external touch detected and apply blocked")
    # documented recovery: an explicit corrective ws commit restores the
    # managed baseline, then a freshly staged task re-checks cleanly.  The old
    # task stays inspectable and is never silently re-based.
    corrective = cli.write("corrective.json", json.dumps({
        "batch_id": "corrective-restore",
        "ops": [{"op": "put", "source_id": "erp",
                 "record_id": key_encode([("s", "o1")]),
                 "relation": "Order", "tuple": ["o1", 77, "c1"]}]}))
    cli.run("ws", "commit", ws, "--batch", corrective)
    cli.write("snap5b/erp.csv",
              "order_id,qty,customer_id\n"
              "o1,77,c1\n"
              '"o,3",9,c3\n'
              "o5,4,c5\no9,1,c9\n")
    d5b = json.loads(json.dumps(d5))
    d5b["sources"][0]["shards"] = [{"path": cli.workdir + "/snap5b/erp.csv"}]
    d5b_path = cli.write("decl5b.json", json.dumps(d5b, ensure_ascii=False))
    cli.run("ingest", "stage", ws, "--declaration", d5b_path, "--task", "load-5b")
    rc, rep5b, _ = cli.run("ingest", "apply", ws, "--task", "load-5b")
    check(rc == 0 and rep5b["version"] is not None,
          "after an explicit corrective commit and re-check, apply succeeds")

    # stale parent: stage then let another commit land first
    cli.write("snap6/erp.csv", "order_id,qty,customer_id\no1,77,c1\n")
    cli.write("snap6/crm.csv", "customer_id,addr\nc1,城东\nc2,NULL:u\nc3,NULL:u\n")
    d6 = json.loads(json.dumps(d5))
    d6["sources"][0]["shards"] = [{"path": cli.workdir + "/snap6/erp.csv"}]
    d6["sources"][1]["shards"] = [{"path": cli.workdir + "/snap6/crm.csv"}]
    d6_path = cli.write("decl6.json", json.dumps(d6, ensure_ascii=False))
    cli.run("ingest", "stage", ws, "--declaration", d6_path, "--task", "load-6")
    other = cli.write("other.json", json.dumps({
        "batch_id": "other-writer",
        "ops": [{"op": "put", "source_id": "other", "record_id": "z",
                 "relation": "Order", "tuple": ["z", 1, "cz"]}]}))
    cli.run("ws", "commit", ws, "--batch", other)
    rc, _, err = cli.run("ingest", "apply", ws, "--task", "load-6", check=False)
    check(rc == 5 and "stale parent" in err,
          "stale parent refused; task remains inspectable, no silent re-base")
    rc, rep, _ = cli.run("ingest", "report", ws, "--task", "load-6")
    check(rep["task_id"] == "load-6", "stale task still inspectable")

    # completion conflict still counts as one successful application
    cli.write("conf/ka.csv", "customer_id,addr\ncc,城东\ncc,城西\n")
    cdecl = cli.write("cdecl.json", json.dumps({
        "connector": "ka-conn", "version": 1,
        "unknown_markers": {"marker": "NULL", "name_prefix": "",
                            "name_separator": ""},
        "sources": [{
            "source_id": "ka", "relation": "KnownAddress",
            "key_columns": ["customer_id", "addr"],
            "fields": [
                {"name": "customer_id", "header": "customer_id"},
                {"name": "addr", "header": "addr"}],
            "shards": [{"path": cli.workdir + "/conf/ka.csv"}],
            "empty_snapshot": False}]}, ensure_ascii=False))
    cli.run("ingest", "stage", ws, "--declaration", cdecl, "--task", "conf-1")
    rc, receipt, _ = cli.run("ingest", "apply", ws, "--task", "conf-1")
    check(receipt["status"] == "conflict" and receipt["version"] is not None,
          f"completion conflict still committed once: {receipt['status']} "
          f"v{receipt['version']}")
    before = receipt["version"]
    rc, again, _ = cli.run("ingest", "apply", ws, "--task", "conf-1")
    check(again["version"] == before and again.get("idempotent_replay"),
          "conflict task re-apply returns the same version, no replay")

    # -- 6. hard exit, deleted CSV, idempotent re-apply --------------------- #
    banner("6. process hard-exit, reopen, apply after deleting CSV files")
    cli.write("snap7/erp.csv", "order_id,qty,customer_id\no1,77,c1\no10,2,c10\n")
    cli.write("snap7/crm.csv", "customer_id,addr\nc1,城东\nc2,NULL:u\nc3,NULL:u\n")
    d7 = json.loads(json.dumps(d5))
    d7["sources"][0]["shards"] = [{"path": cli.workdir + "/snap7/erp.csv"}]
    d7["sources"][1]["shards"] = [{"path": cli.workdir + "/snap7/crm.csv"}]
    d7_path = cli.write("decl7.json", json.dumps(d7, ensure_ascii=False))
    # stage in one process, then hard-exit a separate process during apply
    cli.run("ingest", "stage", ws, "--declaration", d7_path, "--task", "load-7")
    crash_code = (
        "import os,sys; sys.path.insert(0,%r);"
        "os.environ['CHASE_WS_CRASH_AT']='after_commit';"
        "from chase.workspace import Workspace; from chase.ingest import IngestManager;"
        "w=Workspace(%r); m=IngestManager(w); m.apply_task('load-7'); os._exit(0)"
        % (REPO, ws))
    proc = subprocess.run([sys.executable, "-I", "-c", crash_code],
                          capture_output=True, text=True)
    # the workspace crash hook hard-kills with 97 right after the commit txn
    check(proc.returncode == 97,
          f"child process hard-exited at the commit boundary (rc={proc.returncode})")
    # reopen in yet another process and confirm the durable state
    rc, rep, _ = cli.run("ingest", "report", ws, "--task", "load-7")
    check(rep["applied_version"] is not None,
          "task recovered as applied after hard process exit")
    # delete the staged source file and re-apply: typed snapshot is what runs
    os.remove(os.path.join(workdir, "snap7/erp.csv"))
    rc, again, _ = cli.run("ingest", "apply", ws, "--task", "load-7")
    check(again.get("idempotent_replay") and again["version"] == rep["applied_version"],
          "apply after CSV deletion returns the same version (typed data persisted)")
    rc, vers, _ = cli.run("ws", "versions", ws)
    check(vers["latest_source_version"] == again["version"],
          "re-apply never creates an extra version")

    # -- summary ------------------------------------------------------------- #
    banner("summary")
    if failures:
        print(f"{len(failures)} DEMO CHECK(S) FAILED:")
        for f in failures:
            print("  - " + f)
        return 1
    print("all demo checks passed")
    if not args.keep:
        print(f"(workdir {workdir} left in place for inspection)")
    return 0


def _unknowns(cli, workdir, task):
    _, out, _ = cli.run("ingest", "unknowns",
                        os.path.join(workdir, "ws"), "--task", task)
    return out["unknowns"]


def _sources(cli, workdir):
    _, out, _ = cli.run("ws", "sources", os.path.join(workdir, "ws"))
    grouped = {}
    for r in out["records"]:
        grouped.setdefault(r["source_id"], {})[r["record_id"]] = (
            r["relation"], r["tuple"])
    return grouped


def tempfile_gettempdir():
    import tempfile
    return tempfile.gettempdir()


if __name__ == "__main__":
    sys.exit(main())
