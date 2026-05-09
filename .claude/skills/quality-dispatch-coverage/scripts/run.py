#!/usr/bin/env python3
"""quality-dispatch-coverage skill runner."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_DIR = SCRIPT_DIR.parent
SHARED = SKILL_DIR.parent / "_shared"
sys.path.insert(0, str(SHARED))

from protocol_constants import PROJECT_ROOT  # noqa: E402

BACKEND_FILES = ["cifs.rs", "nfs.rs", "s3.rs", "local.rs"]
FN_RE = re.compile(r"^\s*pub\s+(?:async\s+)?fn\s+(\w+)\s*[<(]", re.MULTILINE)


def extract_pub_fns(path: Path) -> set[str]:
    return set(FN_RE.findall(path.read_text(encoding="utf-8")))


def load_baseline() -> dict[str, set[str]]:
    bp = SKILL_DIR / "baseline_holes.json"
    if not bp.exists():
        return {}
    data = json.loads(bp.read_text())
    return {fn: set(backends) for fn, backends in data.get("holes", {}).items()}


def main() -> int:
    src = Path(PROJECT_ROOT) / "src"
    enum_file = src / "storage_enum.rs"
    if not enum_file.exists():
        print(f"[skill dispatch-coverage] FAIL: {enum_file} missing", file=sys.stderr)
        return 1

    baseline = load_baseline()

    enum_fns = extract_pub_fns(enum_file)
    excluded = {
        "new",
        "create_storage",
        "create_storage_for_dest",
        "detect_storage_type",
        "from",
        "from_url",
        "kind",
        "url",
    }
    enum_fns -= excluded

    backend_fns: dict[str, set[str]] = {}
    for b in BACKEND_FILES:
        bp = src / b
        if not bp.exists():
            print(f"[skill dispatch-coverage] FAIL: {bp} missing", file=sys.stderr)
            return 1
        backend_fns[b] = extract_pub_fns(bp)

    new_holes: list[str] = []
    known_holes: list[str] = []
    enum_only: list[str] = []

    print(f"checking {len(enum_fns)} StorageEnum public methods against 4 backends")
    print(f"baseline: {len(baseline)} known holes (see baseline_holes.json)")
    print("-" * 70)

    for fn in sorted(enum_fns):
        gaps = {b for b in BACKEND_FILES if fn not in backend_fns[b]}
        if not gaps:
            print(f"  ✓ {fn}")
        elif len(gaps) == len(BACKEND_FILES):
            enum_only.append(fn)
            print(f"  ◇ {fn:<30} enum-only (no backend implements)")
        else:
            baseline_gaps = baseline.get(fn, set())
            unexpected = gaps - baseline_gaps
            if unexpected:
                gap_str = ", ".join(sorted(unexpected))
                base_str = ", ".join(sorted(baseline_gaps)) if baseline_gaps else "none"
                new_holes.append(f"{fn}: NEW gaps in {gap_str} (baseline excluded: {base_str})")
                print(f"  ✗ {fn:<30} NEW HOLE: {gap_str}")
            else:
                gap_str = ", ".join(sorted(gaps))
                known_holes.append(f"{fn}: known gaps in {gap_str}")
                print(f"  · {fn:<30} known baseline gap: {gap_str}")

    print(
        f"\n[skill dispatch-coverage] new holes: {len(new_holes)}, known: {len(known_holes)}, enum-only: {len(enum_only)}"
    )

    if new_holes:
        print("\nFAIL — net-new dispatch holes (regression from baseline):")
        for m in new_holes:
            print(f"  - {m}")
        print("\n如果是有意为之 → 把 backend 加入 baseline_holes.json。")
        print("如果是漏改 → 补 backend 实现 (或返回 UnsupportedType)。")
        return 1

    print("\n[skill dispatch-coverage] PASS — no net-new dispatch holes")
    if known_holes:
        print(f"  baseline: {len(known_holes)} known gaps (see baseline_holes.json)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
