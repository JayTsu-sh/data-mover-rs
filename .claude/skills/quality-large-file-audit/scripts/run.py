#!/usr/bin/env python3
"""quality-large-file-audit skill runner."""

from __future__ import annotations

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_DIR = SCRIPT_DIR.parent
SHARED = SKILL_DIR.parent / "_shared"
sys.path.insert(0, str(SHARED))

from protocol_constants import PROJECT_ROOT  # noqa: E402

# Existing large files carried into the 0.4 workspace (pre-extraction line counts).
# Use full relative paths: identical leaf names belong to different crates.
BASELINE = {
    'crates/backend-cifs/src/backend/source_tests.rs': 1023,
    'crates/backend-local/src/backend/staged.rs': 1569,
    'crates/backend-local/src/backend/staged/tests.rs': 1311,
    'crates/backend-nfs/src/backend/client.rs': 2477,
    'crates/backend-nfs/src/backend/staged_tests.rs': 1466,
    'crates/storage-core/src/qos.rs': 1286,
    'crates/transfer/src/transfer/tests.rs': 1382,
    'crates/traversal/src/filter.rs': 5414,
}
NEW_FILE_LIMIT = 1000
GROWTH_WARN_PCT = 5
HIGH_GROWTH_PCT = 10


def main() -> int:
    src = Path(PROJECT_ROOT) / "src"
    if not src.exists():
        print(f"[skill audit-large] FAIL: {src} does not exist", file=sys.stderr)
        return 1

    fails = []
    warns = []
    print(f"{'file':<20} {'loc':>6} {'baseline':>9} {'status':<10}")
    print("-" * 50)
    root = Path(PROJECT_ROOT)
    sources = [*src.rglob("*.rs"), *(root / "crates").glob("*/src/**/*.rs")]
    for rs in sorted(sources):
        relative = rs.relative_to(root).as_posix()
        loc = sum(1 for _ in rs.open(encoding="utf-8"))
        baseline = BASELINE.get(relative)
        if baseline is None:
            status = "NEW"
            if loc > NEW_FILE_LIMIT:
                status = f"NEW>{NEW_FILE_LIMIT} FAIL"
                fails.append(f"{relative}: NEW file {loc} > {NEW_FILE_LIMIT}")
        else:
            growth = (loc - baseline) / baseline * 100
            if growth > HIGH_GROWTH_PCT:
                status = f"GROW +{growth:.1f}% WARN"
                warns.append(
                    f"{relative}: grew {growth:.1f}% (baseline {baseline}, now {loc})"
                )
            elif growth > GROWTH_WARN_PCT:
                status = f"GROW +{growth:.1f}% WARN"
                warns.append(f"{relative}: grew {growth:.1f}%")
            elif growth < -GROWTH_WARN_PCT:
                status = f"SHRINK {growth:.1f}%"
            else:
                status = "SAME"
        print(f"{relative:<20} {loc:>6} {str(baseline or '-'):>9} {status:<10}")

    if fails:
        print(f"\n[skill audit-large] FAIL ({len(fails)}):")
        for f in fails:
            print(f"  - {f}")
        return 1
    if warns:
        print(f"\n[skill audit-large] WARN ({len(warns)}):")
        for w in warns:
            print(f"  - {w}")
    print("\n[skill audit-large] PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
