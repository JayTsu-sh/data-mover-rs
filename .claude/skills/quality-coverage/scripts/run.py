#!/usr/bin/env python3
"""quality-coverage skill runner."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_DIR = SCRIPT_DIR.parent
SHARED = SKILL_DIR.parent / "_shared"
sys.path.insert(0, str(SHARED))

from protocol_constants import PROJECT_ROOT  # noqa: E402

THRESHOLD_PCT = 30.0  # 启动阈值，目标 60


def main() -> int:
    if shutil.which("cargo-llvm-cov") is None:
        print("[skill quality-coverage] SKIP: cargo-llvm-cov not installed", file=sys.stderr)
        print("  install: cargo install cargo-llvm-cov && rustup component add llvm-tools-preview")
        return 0  # skip 不算失败

    cmd = ["cargo", "llvm-cov", "--workspace", "--json"]
    print(f"[skill quality-coverage] $ {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True)
    if result.returncode != 0:
        print(result.stdout)
        print("[stderr]", result.stderr, file=sys.stderr)
        print("[skill quality-coverage] FAIL: cargo llvm-cov failed", file=sys.stderr)
        return 1

    try:
        data = json.loads(result.stdout)
        totals = data["data"][0]["totals"]
        line_pct = totals["lines"]["percent"]
    except (json.JSONDecodeError, KeyError, IndexError) as e:
        print(f"[skill quality-coverage] FAIL: could not parse coverage json: {e}", file=sys.stderr)
        return 1

    print(f"[skill quality-coverage] line coverage: {line_pct:.1f}%")
    if line_pct < THRESHOLD_PCT:
        print(f"[skill quality-coverage] FAIL: below threshold {THRESHOLD_PCT}%", file=sys.stderr)
        return 1
    print(f"[skill quality-coverage] PASS (≥ {THRESHOLD_PCT}%)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
