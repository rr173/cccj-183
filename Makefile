PY ?= python3

.PHONY: test check examples bench clean

test:
	$(PY) run_tests.py

check:
	$(PY) -m compileall -q chase tests

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

clean:
	rm -rf __pycache__ chase/__pycache__ tests/__pycache__ .pytest_cache
