#!/usr/bin/env python3
"""op-cancel skill runner — 跑 cancellation 语义测试。"""

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
    cmd = ["cargo", "test", "--test", "test_copy_file_cancel"]
    print(f"[skill op-cancel] $ {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=PROJECT_ROOT)
    assert_exit_code("test_copy_file_cancel", result.returncode)
    print("\n[skill op-cancel] PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
