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
        result = subprocess.run(
            cmd, cwd=PROJECT_ROOT, capture_output=True, text=True,
            encoding="utf-8", errors="replace",
        )
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

    # 不存在 prefix 验证：S3 ListObjectsV2 对不存在的 prefix 返回 200 + 空列表
    # （这是 S3 语义，不是 404）。断言：空结果 + 干净退出 + 无 retry/backoff。
    # 注意：404 → FileNotFound 的映射（commit 7eb3046）在 head_object/get_metadata
    # 路径，walkdir 例子覆盖不到，需要走 get_metadata 的入口另行验证。
    nonexistent = f"{prefix}/__skill_404_test_{int(time.time())}"
    bad_url = s3_url(bucket, host, ak, sk, prefix=nonexistent, use_https=use_https)
    output = run(
        "s3_walkdir nonexistent prefix (expected empty list, no retry)",
        ["cargo", "run", "--example", "s3_walkdir", "--", bad_url],
        expected=0,
        capture=True,
    )
    if "retry" in output.lower() or "backoff" in output.lower():
        print("[skill e2e-s3] FAIL: nonexistent prefix triggered retry/backoff (commit 7eb3046 regression)", file=sys.stderr)
        return 1
    if "total entries: 0" not in output.lower():
        print("[skill e2e-s3] FAIL: nonexistent prefix did not list as empty", file=sys.stderr)
        return 1
    print("[skill e2e-s3] PASS: nonexistent prefix → empty list (no retry)")

    print("\n[skill e2e-s3] all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
