#!/usr/bin/env python3
"""e2e-local skill runner — 无外部依赖，CI 必跑。"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_DIR = SCRIPT_DIR.parent
SHARED = SKILL_DIR.parent / "_shared"
sys.path.insert(0, str(SHARED))

from assertions import assert_exit_code  # noqa: E402
from protocol_constants import PROJECT_ROOT  # noqa: E402

TMP_DIR = Path("/tmp/data-mover-skill-local")


def run(label: str, cmd: list[str], cwd: str = PROJECT_ROOT, expected: int = 0) -> int:
    print(f"\n[skill e2e-local] $ {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd)
    assert_exit_code(label, result.returncode, expected)
    return result.returncode


def main() -> int:
    if TMP_DIR.exists():
        shutil.rmtree(TMP_DIR)
    TMP_DIR.mkdir(parents=True)

    try:
        run("cargo build --examples", ["cargo", "build", "--examples"])
        run("test_storage_type", ["cargo", "test", "--test", "test_storage_type"])
        run("test_copy_file_cancel", ["cargo", "test", "--test", "test_copy_file_cancel"])
        # examples 接受路径参数 — 这里只做"编译通过 + 能启动" 的烟雾测试
        # 真实运行需要写入测试 fixture，是后续增强项
        print("\n[skill e2e-local] all checks passed")
        return 0
    finally:
        if TMP_DIR.exists():
            shutil.rmtree(TMP_DIR, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
