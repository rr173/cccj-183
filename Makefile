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

clean:
	rm -rf __pycache__ chase/__pycache__ tests/__pycache__ .pytest_cache
