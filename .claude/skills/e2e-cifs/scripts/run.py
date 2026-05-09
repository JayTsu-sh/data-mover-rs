#!/usr/bin/env python3
"""e2e-cifs skill runner — 需要真 SMB 服务器。"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_DIR = SCRIPT_DIR.parent
SHARED = SKILL_DIR.parent / "_shared"
sys.path.insert(0, str(SHARED))

from assertions import assert_exit_code  # noqa: E402
from env_loader import load_env, require  # noqa: E402
from protocol_constants import PROJECT_ROOT  # noqa: E402
from url_builder import cifs_url  # noqa: E402


def run(label: str, cmd: list[str]) -> int:
    print(f"\n[skill e2e-cifs] $ {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=PROJECT_ROOT)
    assert_exit_code(label, result.returncode)
    return result.returncode


def main() -> int:
    env = load_env(SKILL_DIR)
    require(env, "CIFS_HOST", "CIFS_SHARE", "CIFS_USER", "CIFS_PASS")

    host = env["CIFS_HOST"]
    port = int(env.get("CIFS_PORT", 445))
    share = env["CIFS_SHARE"]
    user = env["CIFS_USER"]
    password = env["CIFS_PASS"]
    anon_share = env.get("CIFS_ANON_SHARE", "").strip()

    run("build cifs_walkdir", ["cargo", "build", "--example", "cifs_walkdir"])
    run("build cifs_copy", ["cargo", "build", "--example", "cifs_copy"])

    matrix: list[tuple[str, str]] = []

    # smb2_only=true (默认)
    url_default = cifs_url(host, share, user, password, port=port, smb2_only=True, anon=False)
    matrix.append(("smb2_only=true", url_default))

    # smb2_only=false (老 NAS 兼容)
    url_smb1 = cifs_url(host, share, user, password, port=port, smb2_only=False, anon=False)
    matrix.append(("smb2_only=false", url_smb1))

    # 匿名 share (可选)
    if anon_share:
        url_anon = cifs_url(host, anon_share, port=port, smb2_only=True, anon=True)
        matrix.append(("anon=true", url_anon))

    for label, url in matrix:
        run(f"cifs_walkdir [{label}]", ["cargo", "run", "--example", "cifs_walkdir", "--", url])

    print("\n[skill e2e-cifs] matrix passed:", [m[0] for m in matrix])
    return 0


if __name__ == "__main__":
    sys.exit(main())
