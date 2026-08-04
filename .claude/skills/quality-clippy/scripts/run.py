#!/usr/bin/env python3
"""quality-clippy skill runner.

策略：
- 对生产库运行 cargo clippy，并把所有 warning 作为硬错误。
- 对 tests 和 examples 额外运行 clippy。测试代码允许 unwrap/expect，其余 lint
  暂按 warning 报告，待存量清零后切换为 -D warnings。
baseline 已清零；文件仅保留为状态记录，不再用于放行生产告警。
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
        "cargo", "clippy", "--lib", "--",
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

    targets_cmd = [
        "cargo", "clippy", "--tests", "--examples", "--",
        "-A", "clippy::unwrap_used",
        "-A", "clippy::expect_used",
    ]
    targets_result = run_clippy(targets_cmd)
    targets_output = (targets_result.stdout or "") + (targets_result.stderr or "")
    sys.stdout.write(targets_output)
    if targets_result.returncode != 0:
        print(
            f"[skill quality-clippy] FAIL: tests/examples clippy exited with "
            f"{targets_result.returncode}",
            file=sys.stderr,
        )
        return targets_result.returncode

    print("[skill quality-clippy] PASS (lib has zero warnings; tests/examples checked)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
