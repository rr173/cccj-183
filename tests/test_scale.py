"""Scale test: generated workload, on-disk store, reported relation/trigger
counts and RSS.  Kept small enough for CI (a few hundred ms)."""

import io
import json
import os
import tempfile
import time
from contextlib import redirect_stdout

from chase.bench import generate, main as bench_main


def test_generated_workload_is_complete_and_fast():
    t0 = time.monotonic()
    buf = io.StringIO()
    with redirect_stdout(buf):
        rc = bench_main(["--orders", "400", "--customers", "80",
                         "--max-seconds", "60"])
    elapsed = time.monotonic() - t0
    assert rc == 0
    report = json.loads(buf.getvalue())
    assert report["status"] == "complete"
    assert report["verification"]["ok"], report["verification"]
    assert report["relations"]["Order"] == 400
    assert report["relations"]["Customer"] <= 80
    assert report["trigger_fires"] > 0
    assert report["rss_peak_kb"] > 0
    assert elapsed < 60


def test_disk_backed_store():
    with tempfile.TemporaryDirectory() as tmp:
        db = os.path.join(tmp, "work.sqlite")
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = bench_main(["--orders", "100", "--customers", "20", "--store", db])
        assert rc == 0
        report = json.loads(buf.getvalue())
        assert report["status"] == "complete"
        assert os.path.exists(db)


def test_generate_is_deterministic():
    a = generate(50, 10, 0.5, seed=3)
    b = generate(50, 10, 0.5, seed=3)
    assert a == b
    c = generate(50, 10, 0.5, seed=4)
    assert a != c


def test_no_known_addresses_still_completes():
    buf = io.StringIO()
    with redirect_stdout(buf):
        rc = bench_main(["--orders", "60", "--customers", "12",
                         "--known-fraction", "0.0"])
    assert rc == 0
    report = json.loads(buf.getvalue())
    assert report["status"] == "complete"
    assert report["unifications"] == 0
    assert report["live_null_classes"] == report["relations"]["Customer"]


def test_all_known_addresses_leaves_no_live_unknown():
    buf = io.StringIO()
    with redirect_stdout(buf):
        rc = bench_main(["--orders", "60", "--customers", "12",
                         "--known-fraction", "1.0"])
    assert rc == 0
    report = json.loads(buf.getvalue())
    assert report["status"] == "complete"
    assert report["live_null_classes"] == 0
    assert report["null_classes_absorbed_by_constants"] == report["relations"]["Customer"]
