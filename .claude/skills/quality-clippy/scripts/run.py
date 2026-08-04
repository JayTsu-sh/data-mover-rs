#!/usr/bin/env python3
"""quality-clippy skill runner.

策略：
- 对生产库运行 cargo clippy；unwrap/expect/dbg/todo/unimplemented 始终作为硬错误，
  其余存量 warning 与 baseline 比较。
- 对 tests 和 examples 额外运行 clippy。测试代码允许 unwrap/expect，其余 lint
  暂按 warning 报告，待存量清零后切换为 -D warnings。
- 与 baseline 比较：存在 baseline_count.<sys.platform>.txt (如 win32) 时优先，
  否则用 baseline_count.txt (Linux 口径)。
- 总数 ≤ baseline → PASS (允许下降)。
- 总数 > baseline → FAIL (警告新增 = 回归)。
- 总数 < baseline-5 → 提示更新 baseline (clippy 修了，应该锁回去防回退)。

理由：项目已有约 173 条 backlog warning。强制 -D warnings 会让 harness 一直
红，不可用。先用 baseline 锁定现状，逐步下降。最终目标：baseline = 0，
然后 CI 改回 -D warnings。
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_DIR = SCRIPT_DIR.parent
SHARED = SKILL_DIR.parent / "_shared"
sys.path.insert(0, str(SHARED))

from protocol_constants import PROJECT_ROOT  # noqa: E402

# baseline 按平台分口径：Windows 多出 cfg(windows) 路径的 pedantic 警告，
# 与 Linux 录制的 baseline 不可比。存在 baseline_count.<platform>.txt 时优先。
_PLATFORM_BASELINE = SKILL_DIR / f"baseline_count.{sys.platform}.txt"
BASELINE_FILE = (
    _PLATFORM_BASELINE if _PLATFORM_BASELINE.exists() else SKILL_DIR / "baseline_count.txt"
)
WARNING_RE = re.compile(r"^(warning|error):", re.MULTILINE)
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def load_baseline() -> int:
    if not BASELINE_FILE.exists():
        return 0
    try:
        # utf-8-sig: PowerShell 的 echo/重定向可能写出带 BOM 的文件
        return int(BASELINE_FILE.read_text(encoding="utf-8-sig").strip())
    except ValueError:
        print(
            f"[skill quality-clippy] WARN: cannot parse {BASELINE_FILE}, treating baseline as 0",
            file=sys.stderr,
        )
        return 0


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
        "-D", "clippy::unwrap_used",
        "-D", "clippy::expect_used",
        "-D", "clippy::dbg_macro",
        "-D", "clippy::todo",
        "-D", "clippy::unimplemented",
    ]
    result = run_clippy(cmd)
    output = (result.stdout or "") + (result.stderr or "")
    sys.stdout.write(output)

    count = len(WARNING_RE.findall(ANSI_RE.sub("", output)))
    baseline = load_baseline()

    print(f"\n[skill quality-clippy] warnings+errors: {count} (baseline {baseline})")

    if result.returncode != 0:
        print(
            f"[skill quality-clippy] FAIL: cargo exited with {result.returncode}",
            file=sys.stderr,
        )
        return result.returncode

    if count > baseline:
        print(f"[skill quality-clippy] FAIL: count {count} > baseline {baseline} (regression)", file=sys.stderr)
        print(
            "如果新增是有意为之 (例如新代码引入了 backlog 标签的警告)：",
            file=sys.stderr,
        )
        print(f"  echo {count} > {BASELINE_FILE}", file=sys.stderr)
        return 1

    if count + 5 < baseline:
        print(
            f"[skill quality-clippy] HINT: count dropped by {baseline - count} — "
            f"update baseline to lock in: echo {count} > {BASELINE_FILE}"
        )

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

    print("[skill quality-clippy] PASS (lib ≤ baseline; tests/examples checked)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
