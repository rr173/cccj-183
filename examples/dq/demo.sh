#!/usr/bin/env bash
# Runnable data-quality audit demo.
# Usage:  bash examples/dq/demo.sh [workspace-dir]
set -euo pipefail
cd "$(dirname "$0")/../.."
HERE=examples/dq
ROOT="$(pwd)"
WS="${1:-/tmp/chase-dq-demo}"
rm -rf "$WS" "$WS"-report*.json

chase() { python3 -I "$ROOT/run_cli.py" "$@"; }

step() { printf '\n=== %s ===\n' "$1"; }

step "init workspace (orders, customers, known regions, derived Done bridge)"
chase ws init "$WS" --program "$HERE/program.json"

step "b1: initial load -> v1 (duplicate raw order key; unknown region u1)"
chase ws commit "$WS" --batch "$HERE/batch1.json" >/dev/null

step "validate the policy against the workspace schema (no data read)"
chase ws dq check "$WS" --policy "$HERE/policy.json" | head -12

step "audit v1: source layer + completed layer, pinned together"
set +e
chase ws dq run "$WS" --policy "$HERE/policy.json" \
      --sources-version 1 --completed-version 1 -o "$WS-report-v1.json" \
  | python3 -c 'import json,sys; r=json.load(sys.stdin); print(json.dumps({"status":r["status"],"report_id":r["report_id"][:16],"totals":r["totals"],"rules":[{ "id":x["id"],"status":x["status"],"counts":x["counts"]} for x in r["rules"]]}, ensure_ascii=False, indent=2))'
echo "exit code (2 = fail): $?"
set -e

step "b2: supplier fills the region and reports an item for an unknown order -> v2"
chase ws commit "$WS" --batch "$HERE/batch2.json" >/dev/null

step "audit v2: the unknown-region finding is resolved, the missing reference is found"
set +e
chase ws dq run "$WS" --policy "$HERE/policy.json" \
      --sources-version 2 --completed-version 2 -o "$WS-report-v2.json" \
  | python3 -c 'import json,sys; r=json.load(sys.stdin); print(json.dumps({"status":r["status"],"totals":r["totals"],"rules":[{ "id":x["id"],"status":x["status"],"counts":x["counts"]} for x in r["rules"]]}, ensure_ascii=False, indent=2))'
set -e

step "re-audit history: v1 is unchanged and still reproducible"
set +e
chase ws dq run "$WS" --policy "$HERE/policy.json" \
      --sources-version 1 --completed-version 1 --force -o "$WS-report-v1b.json" \
  >/dev/null
set -e
python3 - "$WS-report-v1.json" "$WS-report-v1b.json" <<'PY'
import json, sys
a = json.load(open(sys.argv[1], encoding="utf-8"))
b = json.load(open(sys.argv[2], encoding="utf-8"))
for d in (a, b):
    # operational measurements (time, RSS, run ids) are not part of the
    # reproducible report identity
    d.pop("phase_seconds", None); d.pop("run_id", None)
    d.pop("duplicate_of_run_id", None); d.pop("report_sha256", None)
    d.get("scan_summary", {}).pop("peak_rss_mb", None)
    for r in d["rules"]:
        r.pop("elapsed_seconds", None)
assert json.dumps(a, sort_keys=True) == json.dumps(b, sort_keys=True)
print("same report_id:", a["report_id"] == b["report_id"],
      "| identical counts/samples: yes")
PY

step "stored run history (survives reopening the workspace)"
chase ws dq runs "$WS" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(json.dumps([{ "run":r["run_id"],"status":r["overall_status"],"sources_v":r["sources_version"],"completed_v":r["completed_version"],"report":r["report_id"][:16]} for r in d["runs"]], ensure_ascii=False, indent=2))'

step "sample evidence from the v2 report (which record caused the finding)"
python3 - "$WS-report-v2.json" <<'PY'
import json, sys
rep = json.load(open(sys.argv[1], encoding="utf-8"))
for r in rep["rules"]:
    if r["samples"] or r.get("matched", {}).get("samples"):
        print(r["id"], "->", r["status"], r["counts"])
        for s in r["samples"][:2]:
            print("  sample:", json.dumps(s, ensure_ascii=False)[:220])
        for s in r.get("matched", {}).get("samples", [])[:1]:
            print("  provenance:", json.dumps(
                s.get("matched_target", {}).get("provenance"), ensure_ascii=False)[:220])
PY
