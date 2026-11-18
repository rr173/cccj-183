#!/usr/bin/env python3
"""Isolated-mode entry point for running the chase CLI.

Python's ``-I`` flag (used whenever the process reads untrusted supplier
files) removes the current directory and PYTHONPATH from ``sys.path``; this
wrapper adds only the trusted repository root explicitly and then delegates
to the normal CLI.  Run it as::

    python3 -I /workspace/run_cli.py ws init ...
    python3 -I /workspace/run_cli.py ingest stage ...
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from chase.cli import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
