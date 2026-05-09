#!/usr/bin/env python3
"""harness-run report formatter (separate from runner for reuse).

输入: stdin 接收 runner.py 的 stdout，输出 markdown。
当前 runner.py 已经直接输出 markdown，本 script 留作扩展点。
"""

from __future__ import annotations

import sys


def main() -> int:
    sys.stdout.write(sys.stdin.read())
    return 0


if __name__ == "__main__":
    sys.exit(main())
