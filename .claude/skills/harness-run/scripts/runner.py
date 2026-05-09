#!/usr/bin/env python3
"""harness-run skill — 编排 skill 矩阵。

usage:
    python3 runner.py                  # 无外部环境 (CI 默认)
    python3 runner.py --include-network # 加跑 e2e-cifs/nfs/s3
    python3 runner.py --quick           # 只跑 quality + op
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

import yaml  # type: ignore[import-untyped]

SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_DIR = SCRIPT_DIR.parent
SKILLS_ROOT = SKILL_DIR.parent  # .claude/skills/


def run_skill(name: str) -> tuple[str, str, float]:
    """Returns (name, status, elapsed_sec). status ∈ {PASS, FAIL, SKIP}."""
    runner = SKILLS_ROOT / name / "scripts" / "run.py"
    if not runner.exists():
        return name, "MISSING", 0.0
    start = time.perf_counter()
    result = subprocess.run([sys.executable, str(runner)])
    elapsed = time.perf_counter() - start
    if result.returncode == 0:
        return name, "PASS", elapsed
    # quality-coverage 在 cargo-llvm-cov 缺失时返回 0 (skip 内置)，所以非 0 一律 FAIL
    return name, "FAIL", elapsed


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--include-network", action="store_true")
    ap.add_argument("--quick", action="store_true")
    args = ap.parse_args()

    matrix_path = SCRIPT_DIR / "matrix.yaml"
    matrix = yaml.safe_load(matrix_path.read_text())

    skills_to_run: list[str] = []
    if args.quick:
        skills_to_run = [s for s in matrix["no_network"] if s.startswith(("quality-", "op-"))]
    else:
        skills_to_run = list(matrix["no_network"])
        if args.include_network:
            skills_to_run += list(matrix["network"])

    print(f"[harness-run] running {len(skills_to_run)} skills: {skills_to_run}")
    print()

    results: list[tuple[str, str, float]] = []
    for name in skills_to_run:
        print(f"\n{'='*60}\n[harness-run] >>> {name}\n{'='*60}")
        results.append(run_skill(name))

    # 报告
    print("\n\n## data-mover-rs harness-run report\n")
    print(f"| {'skill':<28} | {'status':<7} | {'duration':>9} |")
    print(f"| {'-'*28} | {'-'*7} | {'-'*9} |")
    fails = 0
    for name, status, elapsed in results:
        print(f"| {name:<28} | {status:<7} | {elapsed:>7.1f}s |")
        if status == "FAIL":
            fails += 1
    total = len(results)
    print(f"\nVERDICT: {'FAIL' if fails else 'PASS'} ({total - fails}/{total})")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
