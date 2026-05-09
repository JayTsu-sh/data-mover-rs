#!/usr/bin/env python3
"""e2e-s3 skill runner — 需要真 S3 endpoint。验证 404 → FileNotFound。"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_DIR = SCRIPT_DIR.parent
SHARED = SKILL_DIR.parent / "_shared"
sys.path.insert(0, str(SHARED))

from assertions import assert_exit_code  # noqa: E402
from env_loader import load_env, require  # noqa: E402
from protocol_constants import PROJECT_ROOT  # noqa: E402
from url_builder import s3_url  # noqa: E402


def run(label: str, cmd: list[str], expected: int = 0, capture: bool = False) -> str:
    print(f"\n[skill e2e-s3] $ {' '.join(cmd)}")
    if capture:
        result = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True)
        print(result.stdout)
        if result.stderr:
            print("[stderr]", result.stderr)
        assert_exit_code(label, result.returncode, expected)
        return result.stdout + result.stderr
    result = subprocess.run(cmd, cwd=PROJECT_ROOT)
    assert_exit_code(label, result.returncode, expected)
    return ""


def main() -> int:
    env = load_env(SKILL_DIR)
    require(env, "S3_HOST", "S3_BUCKET", "S3_AK", "S3_SK")

    host = env["S3_HOST"]
    bucket = env["S3_BUCKET"]
    ak = env["S3_AK"]
    sk = env["S3_SK"]
    use_https = env.get("S3_USE_HTTPS", "false").lower() == "true"
    prefix = env.get("S3_PREFIX", "test")

    run("build s3_walkdir", ["cargo", "build", "--example", "s3_walkdir"])

    url = s3_url(bucket, host, ak, sk, prefix=prefix, use_https=use_https)
    run("s3_walkdir bucket list", ["cargo", "run", "--example", "s3_walkdir", "--", url])

    # 404 验证：不存在的 key
    nonexistent = f"{prefix}/__skill_404_test_{int(time.time())}"
    bad_url = s3_url(bucket, host, ak, sk, prefix=nonexistent, use_https=use_https)
    output = run(
        "s3_walkdir 404 (expected FileNotFound, no retry)",
        ["cargo", "run", "--example", "s3_walkdir", "--", bad_url],
        expected=1,
        capture=True,
    )
    if "retry" in output.lower() or "backoff" in output.lower():
        print("[skill e2e-s3] FAIL: 404 triggered retry/backoff (commit 7eb3046 regression)", file=sys.stderr)
        return 1
    if "filenotfound" not in output.lower() and "file not found" not in output.lower() and "no such key" not in output.lower():
        print("[skill e2e-s3] FAIL: 404 did not map to FileNotFound", file=sys.stderr)
        return 1
    print("[skill e2e-s3] PASS: 404 → FileNotFound (no retry)")

    print("\n[skill e2e-s3] all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
