#!/usr/bin/env python3
"""op-filter-dsl skill runner — 跑 filter 模块的所有内嵌单测。"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_DIR = SCRIPT_DIR.parent
SHARED = SKILL_DIR.parent / "_shared"
sys.path.insert(0, str(SHARED))

from assertions import assert_exit_code  # noqa: E402
from protocol_constants import PROJECT_ROOT  # noqa: E402


def main() -> int:
    cmd = ["cargo", "test", "--lib", "filter::"]
    print(f"[skill op-filter-dsl] $ {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=PROJECT_ROOT)
    assert_exit_code("cargo test filter::", result.returncode)
    print("\n[skill op-filter-dsl] PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
