"""Zero-dependency test runner (pytest is not installed in this environment).

Discovers ``test_*.py`` under tests/, runs every callable ``test_*`` and
reports pass/fail.  Supports ``-k substring``.
"""
import argparse
import importlib
import inspect
import os
import sys
import traceback

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)


def iter_tests(pattern=None):
    testdir = os.path.join(ROOT, "tests")
    for name in sorted(os.listdir(testdir)):
        if not (name.startswith("test_") and name.endswith(".py")):
            continue
        modname = "tests." + name[:-3]
        mod = importlib.import_module(modname)
        for attr in sorted(dir(mod)):
            if not attr.startswith("test_"):
                continue
            fn = getattr(mod, attr)
            if not callable(fn):
                continue
            full = f"{modname}::{attr}"
            if pattern and pattern not in full:
                continue
            yield full, fn


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("-k", default=None)
    ap.add_argument("-v", action="store_true")
    args = ap.parse_args(argv)
    passed = failed = 0
    failures = []
    for name, fn in iter_tests(args.k):
        try:
            fn()
            passed += 1
            if args.v:
                print(f"PASS {name}")
        except Exception:
            failed += 1
            failures.append((name, traceback.format_exc()))
            print(f"FAIL {name}")
    for name, tb in failures:
        print("\n" + "=" * 70)
        print(name)
        print(tb)
    print(f"\n{passed} passed, {failed} failed")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
