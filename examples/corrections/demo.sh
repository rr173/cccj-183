#!/usr/bin/env bash
# Runnable demo of the source-correction workspace.
# Usage:  bash examples/corrections/demo.sh [workspace-dir]
set -euo pipefail
cd "$(dirname "$0")/../.."
HERE=examples/corrections
WS="${1:-/tmp/chase-ws-demo}"
rm -rf "$WS" "$WS"-exp-*

step() { printf '\n=== %s ===\n' "$1"; }

step "init workspace (fixed schema + TGD/EGD program)"
python3 -m chase ws init "$WS" --program "$HERE/program.json"

step "b1: initial load -> v1 (u1,u2 bridged to 城东; two records support Order(o1,c1))"
python3 -m chase ws commit "$WS" --batch "$HERE/batch1.json" | python3 -c 'import json,sys; r=json.load(sys.stdin); print(json.dumps({k:r[k] for k in ("committed","version","status","latest_source_version","latest_complete_version")}, ensure_ascii=False))'

step "b2: retract bridge + replace address atomically -> v2 (u1,u2 split; u2 unknown again)"
python3 -m chase ws commit "$WS" --batch "$HERE/batch2.json" | python3 -c 'import json,sys; r=json.load(sys.stdin); print(json.dumps({k:r[k] for k in ("committed","version","status","latest_source_version","latest_complete_version")}, ensure_ascii=False))'

step "diff v1 -> v2 (split of the bridged class, u2 loses its known value)"
python3 -m chase ws diff "$WS" --from 1 --to 2 --human

step "b3: crm sends a conflicting address -> v3 (conflict, kept as a version)"
python3 -m chase ws commit "$WS" --batch "$HERE/batch3.json" | python3 -c 'import json,sys; r=json.load(sys.stdin); print(json.dumps({k:r[k] for k in ("committed","version","status","latest_source_version","latest_complete_version")}, ensure_ascii=False))' || true

step "b4: retract the bad record + correct the address -> v4 (continues from v3)"
python3 -m chase ws commit "$WS" --batch "$HERE/batch4.json" | python3 -c 'import json,sys; r=json.load(sys.stdin); print(json.dumps({k:r[k] for k in ("committed","version","status","latest_source_version","latest_complete_version")}, ensure_ascii=False))'

step "b5: drop one Order support + both Link records -> v5 (Reach disappears)"
python3 -m chase ws commit "$WS" --batch "$HERE/batch5.json" | python3 -c 'import json,sys; r=json.load(sys.stdin); print(json.dumps({k:r[k] for k in ("committed","version","status","latest_source_version","latest_complete_version")}, ensure_ascii=False))'

step "versions"
python3 -m chase ws versions "$WS" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(json.dumps([{ "v":v["version"],"batch":v["batch_id"],"status":v["status"]} for v in d["versions"]], ensure_ascii=False))'

step "diff v4 -> v5 (recursive Reach facts are gone; Order stays)"
python3 -m chase ws diff "$WS" --from 4 --to 5 --human

step "export v2 and v3, verify both independently"
python3 -m chase ws export "$WS" --version 2 -o "$WS-exp-v2"
python3 -m chase ws export "$WS" --version 3 -o "$WS-exp-v3" || true
python3 -m chase ws verify "$WS-exp-v2" | python3 -c 'import json,sys; r=json.load(sys.stdin); print("v2 export verify ok:", r["ok"])'
python3 -m chase ws verify "$WS-exp-v3" | python3 -c 'import json,sys; r=json.load(sys.stdin); print("v3 export verify ok:", r["ok"], "(claimed:", r["claimed_status"]+")")'

step "re-send b3 unchanged (idempotent replay: original receipt, no new version)"
python3 -m chase ws commit "$WS" --batch "$HERE/batch3.json" | python3 -c 'import json,sys; r=json.load(sys.stdin); print(json.dumps({k:r[k] for k in ("committed","idempotent_replay","version","status")}, ensure_ascii=False))' || true
python3 -m chase ws status "$WS"
