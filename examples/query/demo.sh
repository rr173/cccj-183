#!/usr/bin/env bash
# End-to-end demo of the version-bound declarative query entry point.
#
# Builds the hand-crafted workspace (known regions, repeated input unknowns,
# text "7" / int 7 homographs), produces complete/conflict/incomplete source
# versions, then runs queries bound to explicit versions and to
# latest-complete.  Everything is generated under $1 (default /tmp/ws-query-demo);
# nothing is written into the repository.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
WS="${1:-/tmp/ws-query-demo}"

rm -rf "$WS"
mkdir -p "$WS/out"

echo "== 1. workspace + four source versions (complete, complete, conflict, incomplete)"
python3 -I "$ROOT/run_cli.py" ws init   "$WS/ws" --program "$HERE/program.json"
python3 -I "$ROOT/run_cli.py" ws commit "$WS/ws" --batch "$HERE/batch1.json"
python3 -I "$ROOT/run_cli.py" ws commit "$WS/ws" --batch "$HERE/batch2.json"
python3 -I "$ROOT/run_cli.py" ws commit "$WS/ws" --batch "$HERE/batch3.json" || true
python3 -I "$ROOT/run_cli.py" ws commit "$WS/ws" --batch "$HERE/batch4.json" || true
python3 -I "$ROOT/run_cli.py" ws versions "$WS/ws"

echo
echo "== 2. east-region orders on latest-complete (resolves to v2, never v3/v4)"
python3 -I "$ROOT/run_cli.py" ws query "$WS/ws" \
  --query "$HERE/queries/east_orders.json" --version latest-complete

echo
echo "== 3. same question pinned to historical v1 (unknown-region customers cannot be confirmed)"
python3 -I "$ROOT/run_cli.py" ws query "$WS/ws" \
  --query "$HERE/queries/east_orders.json" --version 1

echo
echo "== 4. unknowns participate in joins on shared identity but never leak as known output"
echo "-- Customer x RegionNote on (customer, region): shared u1 joins, u2/u3 are distinct"
python3 -I "$ROOT/run_cli.py" ws query "$WS/ws" \
  --query "$HERE/queries/unknown_join.json" --version 1
echo "-- Customer self-join on the region column: same-unknown pairs only (c2/c4);"
echo "   distinct unknowns u1/u2/u3 never compare equal, known regions pair with themselves"
python3 -I "$ROOT/run_cli.py" ws query "$WS/ws" \
  --query "$HERE/queries/customer_pairs.json" --version 1

echo
echo "== 5. type isolation: text '7' vs int 7"
python3 -I "$ROOT/run_cli.py" ws query "$WS/ws" \
  --query "$HERE/queries/text_seven.json" --version 1
python3 -I "$ROOT/run_cli.py" ws query "$WS/ws" \
  --query "$HERE/queries/int_seven.json" --version 1

echo
echo "== 6. multi-relation join + DISTINCT, written to a file"
python3 -I "$ROOT/run_cli.py" ws query "$WS/ws" \
  --query "$HERE/queries/join_distinct.json" --version latest-complete \
  -o "$WS/out/join.json"
echo "--- file contents:"
cat "$WS/out/join.json"

echo
echo "== 7. rejected queries (exit code 5), no partial results"
python3 -I "$ROOT/run_cli.py" ws query "$WS/ws" \
  --query "$HERE/queries/bad_type_mismatch.json" --version 1 || echo "rejected as expected"
python3 -I "$ROOT/run_cli.py" ws query "$WS/ws" \
  --query "$HERE/queries/bad_syntax.json" --version 1 || echo "rejected as expected"

echo
echo "== 8. querying the conflict/incomplete versions is refused"
python3 -I "$ROOT/run_cli.py" ws query "$WS/ws" \
  --query "$HERE/queries/east_orders.json" --version 3 || echo "v3 refused as expected"
python3 -I "$ROOT/run_cli.py" ws query "$WS/ws" \
  --query "$HERE/queries/east_orders.json" --version 4 || echo "v4 refused as expected"

echo
echo "demo workspace kept at: $WS/ws"
