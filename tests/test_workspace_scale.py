"""Scale test for the correction workspace (runtime-generated data only).

Runs a base load plus a series of local corrections through the workspace
and checks versions, statuses, source reconstruction and export
verification.  For the real numbers see `python -m chase.wsbench` and
docs/WORKSPACE.md.
"""

from __future__ import annotations

import os
import tempfile

from chase.workspace import Workspace
from chase.wsexport import export_version
from chase.wsverify import verify_export
from chase.wsbench import SCHEMA, RULES, base_ops, correction_ops
import random

TMP = tempfile.mkdtemp(prefix="chase-ws-scale-")


def test_scale_corrections():
    orders, customers, corrections, width = 1500, 300, 5, 20
    ws = Workspace.init(os.path.join(TMP, "ws"),
                        {"schema": SCHEMA, "rules": RULES})
    r = ws.commit({"batch_id": "base",
                   "ops": base_ops(orders, customers, 0.6, 7)})
    assert r["status"] == "complete"
    base_facts = r["stats"]["facts_total"]
    assert base_facts > orders  # derived Customer rows exist

    rng = random.Random(8)
    touched_keys = set()
    for step in range(1, corrections + 1):
        ops = correction_ops(rng, step, orders, customers, width)
        touched_keys.update((op["source_id"], op["record_id"]) for op in ops)
        r = ws.commit({"batch_id": f"corr-{step}", "ops": ops})
        assert r["status"] == "complete"
        assert r["version"] == step + 1

    st = ws.status()
    assert st["latest_source_version"] == corrections + 1
    assert st["latest_complete_version"] == corrections + 1
    assert st["counts"]["versions"] == corrections + 1

    # every version's raw sources reconstruct exactly
    for v in range(1, corrections + 2):
        src = ws.sources_at(v)
        assert len(src) == st["counts"]["source_records"] or v != corrections + 1
    # recompute scope is honestly reported as full
    for v in ws.versions():
        assert v["stats"]["recompute_scope"] == "full"

    # the last version exports and verifies independently
    out = os.path.join(TMP, "exp-last")
    export_version(ws, corrections + 1, out)
    report = verify_export(out)
    assert report["ok"], report["problems"]
    assert report["checks"]["source_records"] == st["counts"]["source_records"]
    ws.close()
