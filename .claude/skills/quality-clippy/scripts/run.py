#!/usr/bin/env python3
"""quality-clippy skill runner.

策略：
- 跑 cargo clippy --all-targets (不加 -D warnings)，捕获 warning + error 总数。
- 与 baseline (.claude/skills/quality-clippy/baseline_count.txt) 比较。
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

BASELINE_FILE = SKILL_DIR / "baseline_count.txt"
WARNING_RE = re.compile(r"^(warning|error):", re.MULTILINE)


def load_baseline() -> int:
    if not BASELINE_FILE.exists():
        return 0
    try:
        return int(BASELINE_FILE.read_text().strip())
    except ValueError:
        return 0


def main() -> int:
    cmd = ["cargo", "clippy", "--all-targets"]
    print(f"[skill quality-clippy] $ {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True)
    output = result.stdout + result.stderr
    sys.stdout.write(output)

    count = len(WARNING_RE.findall(output))
    baseline = load_baseline()

    print(f"\n[skill quality-clippy] warnings+errors: {count} (baseline {baseline})")

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

    print("[skill quality-clippy] PASS (≤ baseline)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
