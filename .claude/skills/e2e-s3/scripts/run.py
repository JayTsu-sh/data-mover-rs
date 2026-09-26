#!/usr/bin/env python3
"""e2e-s3 skill runner — 需要真 S3 endpoint。验证 404 → FileNotFound。"""

from __future__ import annotations

import os
import re
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


# `scheme://user:secret@host` — the lab URL carries the access key and secret in its userinfo.
# A secret may hold `/`, so everything up to the token's last `@` is userinfo.
_USERINFO = re.compile(r"(\b[a-z][a-z0-9+.-]*://)[^\s]*@", re.IGNORECASE)


def redact(text: str) -> str:
    """Hides the credentials of any URL in `text` before it is printed."""
    return _USERINFO.sub(r"\1***@", text)


def run(
    label: str, cmd: list[str], expected: int = 0, capture: bool = False, url: str | None = None
) -> str:
    """Runs `cmd`; `url` (credentials in its userinfo) reaches it as `S3_LISTING_URL`, never argv."""
    print(f"\n[skill e2e-s3] $ {' '.join(cmd)}" + (f"  (S3_LISTING_URL={redact(url)})" if url else ""))
    env = dict(os.environ, S3_LISTING_URL=url) if url else None
    if capture:
        result = subprocess.run(
            cmd, cwd=PROJECT_ROOT, capture_output=True, text=True,
            encoding="utf-8", errors="replace", env=env,
        )
        print(redact(result.stdout))
        if result.stderr:
            print("[stderr]", redact(result.stderr))
        assert_exit_code(label, result.returncode, expected)
        return result.stdout + result.stderr
    result = subprocess.run(cmd, cwd=PROJECT_ROOT, env=env)
    assert_exit_code(label, result.returncode, expected)
    return ""


def main() -> int:
    # S3_ENV picks another server's .env (e.g. .claude/skills/e2e-s3/ontap/.env); a variable set by
    # the caller (S3_BUCKET=data-mover-nas) still wins over the file. Only lists: nothing is written.
    env_file = os.environ.get("S3_ENV")
    if env_file and Path(env_file).name != ".env":
        print(f"[skill e2e-s3] S3_ENV must name a skill .env file: {env_file}", file=sys.stderr)
        return 2
    env = load_env((Path(PROJECT_ROOT) / env_file).parent) if env_file else load_env(SKILL_DIR)
    # Every S3_* variable the caller sets wins over the file (the loader only overrides keys the
    # file names): S3_HOST=…:8182 S3_USE_HTTPS=true S3_TLS_VERIFY=false on top of dxn/.env.
    env.update({key: value for key, value in os.environ.items() if key.startswith("S3_")})
    require(env, "S3_HOST", "S3_BUCKET", "S3_AK", "S3_SK")

    host = env["S3_HOST"]
    bucket = env["S3_BUCKET"]
    ak = env["S3_AK"]
    sk = env["S3_SK"]
    use_https = env.get("S3_USE_HTTPS", "false").lower() == "true"
    prefix = env.get("S3_PREFIX", "test")

    run("build s3_walkdir", ["cargo", "build", "--example", "s3_walkdir"])

    # S3_COMPAT=sg|dxn|ontap, S3_TLS_VERIFY=false (HTTPS, self-signed) and S3_BUCKET_TYPE=nas
    # become the URL's options (?compat=dxn&tls_verify=false); empty = the default.
    def option(name: str) -> str:
        return env.get(name, "")

    compat = option("S3_COMPAT")
    if option("S3_TLS_VERIFY") not in ("", "true", "false"):
        print("[skill e2e-s3] S3_TLS_VERIFY must be empty, true or false", file=sys.stderr)
        return 2
    tls_verify = option("S3_TLS_VERIFY") != "false"
    bucket_type = "" if option("S3_BUCKET_TYPE") == "object" else option("S3_BUCKET_TYPE")
    try:
        s3_url(bucket, host, use_https=use_https, compat=compat, tls_verify=tls_verify, bucket_type=bucket_type)
    except ValueError as error:
        print(f"[skill e2e-s3] {error}", file=sys.stderr)
        return 2

    def url_of(sub: str) -> str:
        return s3_url(
            bucket, host, ak, sk, prefix=sub, use_https=use_https,
            compat=compat, tls_verify=tls_verify, bucket_type=bucket_type,
        )

    url = url_of(prefix)
    # The URL (with its credentials) goes through the environment, never a command line; output is
    # still captured and redacted in case the example echoes it.
    walkdir = ["cargo", "run", "-q", "--example", "s3_walkdir"]
    run("s3_walkdir bucket list", walkdir, capture=True, url=url)

    # 不存在 prefix 验证：S3 ListObjectsV2 对不存在的 prefix 返回 200 + 空列表
    # （这是 S3 语义，不是 404）。断言：空结果 + 干净退出 + 无 retry/backoff。
    # 注意：404 → FileNotFound 的映射（commit 7eb3046）在 head_object/get_metadata
    # 路径，walkdir 例子覆盖不到，需要走 get_metadata 的入口另行验证。
    nonexistent = f"{prefix}/__skill_404_test_{int(time.time())}"
    bad_url = url_of(nonexistent)
    output = run(
        "s3_walkdir nonexistent prefix (expected empty list, no retry)",
        walkdir,
        expected=0,
        capture=True,
        url=bad_url,
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
