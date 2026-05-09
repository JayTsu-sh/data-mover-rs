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

# Baseline 截至 2026-05-09 (所有现有 src/*.rs，含 <800 行的)
# 增长 / 缩减按 % 计算。新文件 (不在表内) 必须 ≤800 行。
BASELINE = {
    "filter.rs": 4849,
    "s3.rs": 3350,
    "nfs.rs": 3100,
    "cifs.rs": 2246,
    "storage_enum.rs": 1334,
    "local.rs": 1134,
    "acl.rs": 934,
    "qos.rs": 660,
    "lib.rs": 542,
    "dir_tree.rs": 460,
    "tar_pack.rs": 244,
    "error.rs": 164,
    "walk_scheduler.rs": 147,
    "time_util.rs": 128,
    "checksum.rs": 70,
    "url_redact.rs": 66,
    "async_receiver.rs": 31,
}
NEW_FILE_LIMIT = 800
GROWTH_WARN_PCT = 5
GROWTH_FAIL_PCT = 10


def main() -> int:
    src = Path(PROJECT_ROOT) / "src"
    if not src.exists():
        print(f"[skill audit-large] FAIL: {src} does not exist", file=sys.stderr)
        return 1

    fails = []
    warns = []
    print(f"{'file':<20} {'loc':>6} {'baseline':>9} {'status':<10}")
    print("-" * 50)
    for rs in sorted(src.glob("*.rs")):
        loc = sum(1 for _ in rs.open(encoding="utf-8"))
        baseline = BASELINE.get(rs.name)
        if baseline is None:
            status = "NEW"
            if loc > NEW_FILE_LIMIT:
                status = f"NEW>{NEW_FILE_LIMIT} FAIL"
                fails.append(f"{rs.name}: NEW file {loc} > {NEW_FILE_LIMIT}")
        else:
            growth = (loc - baseline) / baseline * 100
            if growth > GROWTH_FAIL_PCT:
                status = f"GROW +{growth:.1f}% FAIL"
                fails.append(f"{rs.name}: grew {growth:.1f}% (baseline {baseline}, now {loc})")
            elif growth > GROWTH_WARN_PCT:
                status = f"GROW +{growth:.1f}% WARN"
                warns.append(f"{rs.name}: grew {growth:.1f}%")
            elif growth < -GROWTH_WARN_PCT:
                status = f"SHRINK {growth:.1f}%"
            else:
                status = "SAME"
        print(f"{rs.name:<20} {loc:>6} {str(baseline or '-'):>9} {status:<10}")

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
