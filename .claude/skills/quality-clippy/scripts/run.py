#!/usr/bin/env python3
"""quality-clippy skill runner.

策略：对生产库、测试和 examples 统一运行 Clippy，并把所有 warning 作为硬错误。
不为测试代码放宽 unwrap/expect，也不使用 warning baseline。
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_DIR = SCRIPT_DIR.parent
SHARED = SKILL_DIR.parent / "_shared"
sys.path.insert(0, str(SHARED))

from protocol_constants import PROJECT_ROOT  # noqa: E402


def run_clippy(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    print(f"[skill quality-clippy] $ {' '.join(cmd)}")
    # encoding 显式 UTF-8：Windows 默认 GBK 会在 cargo 输出含非 GBK 字节时崩
    return subprocess.run(
        cmd, cwd=PROJECT_ROOT, capture_output=True, text=True,
        encoding="utf-8", errors="replace",
    )


def main() -> int:
    cmd = [
        "cargo", "clippy", "--all-targets", "--",
        "-D", "warnings",
        "-D", "clippy::unwrap_used",
        "-D", "clippy::expect_used",
        "-D", "clippy::dbg_macro",
        "-D", "clippy::todo",
        "-D", "clippy::unimplemented",
    ]
    result = run_clippy(cmd)
    output = (result.stdout or "") + (result.stderr or "")
    sys.stdout.write(output)

    if result.returncode != 0:
        print(
            f"[skill quality-clippy] FAIL: cargo exited with {result.returncode}",
            file=sys.stderr,
        )
        return result.returncode

    print("[skill quality-clippy] PASS (all targets have zero warnings)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
