PY ?= python3

.PHONY: test check examples bench clean ingest-demo ingest-scale ws-query-demo ws-query-scale

test:
	$(PY) -I run_tests.py

check:
	$(PY) -I -m compileall -q chase tests scripts

examples:
	rm -rf /tmp/example-pkg
	$(PY) -m chase check examples/orders_known.json
	$(PY) -m chase complete examples/orders_known.json -o /tmp/example-pkg
	$(PY) -m chase verify /tmp/example-pkg
	$(PY) -m chase trace /tmp/example-pkg --null n0 || true

bench:
	$(PY) -m chase.bench --orders 50000 --customers 10000 --no-verify

ws-demo:
	bash examples/corrections/demo.sh /tmp/chase-ws-demo

ws-bench:
	$(PY) -m chase.wsbench --workspace /tmp/chase-ws-bench --orders 10000 --customers 2000 --corrections 8

ingest-demo:
	$(PY) -I scripts/ingest_demo.py

ingest-scale:
	$(PY) -I scripts/ingest_scale_profiled.py --rows 100000 --shards 4

ws-query-demo:
	bash examples/query/demo.sh /tmp/chase-ws-query-demo

ws-query-scale:
	$(PY) -I scripts/wsquery_scale.py --workspace /tmp/chase-ws-query-scale --orders 60000 --customers 12000

clean:
	rm -rf __pycache__ chase/__pycache__ tests/__pycache__ scripts/__pycache__ .pytest_cache
