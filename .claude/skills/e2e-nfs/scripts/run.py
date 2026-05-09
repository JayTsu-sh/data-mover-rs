#!/usr/bin/env python3
"""e2e-nfs skill runner — 需要真 NFS 服务器。验证 retry taxonomy。"""

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
from url_builder import nfs_url  # noqa: E402


def run(label: str, cmd: list[str], expected: int = 0, capture: bool = False) -> tuple[int, str]:
    print(f"\n[skill e2e-nfs] $ {' '.join(cmd)}")
    if capture:
        result = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True)
        print(result.stdout)
        if result.stderr:
            print("[stderr]", result.stderr)
        assert_exit_code(label, result.returncode, expected)
        return result.returncode, result.stdout + result.stderr
    result = subprocess.run(cmd, cwd=PROJECT_ROOT)
    assert_exit_code(label, result.returncode, expected)
    return result.returncode, ""


def main() -> int:
    env = load_env(SKILL_DIR)
    require(env, "NFS_HOST", "NFS_EXPORT")

    host = env["NFS_HOST"]
    port = int(env.get("NFS_PORT", 2049))
    export = env["NFS_EXPORT"]
    uid = int(env.get("NFS_UID", 1000))
    gid = int(env.get("NFS_GID", 1000))
    deny_dir = env.get("NFS_DENY_DIR", "").strip()

    run("build nfs_walkdir", ["cargo", "build", "--example", "nfs_walkdir"])
    run("build nfs_export", ["cargo", "build", "--example", "nfs_export"])
    run("build nfs_opt_dir", ["cargo", "build", "--example", "nfs_opt_dir"])

    url = nfs_url(host, export, port=port, uid=uid, gid=gid)
    run("nfs_walkdir", ["cargo", "run", "--example", "nfs_walkdir", "--", url])
    run("nfs_export", ["cargo", "run", "--example", "nfs_export", "--", url])
    run("nfs_opt_dir", ["cargo", "run", "--example", "nfs_opt_dir", "--", url])

    # Retry taxonomy 验证
    if deny_dir:
        deny_url = nfs_url(host, export, sub_path=deny_dir.lstrip("/"), port=port, uid=uid, gid=gid)
        # 期待 walkdir 失败 (PermissionDenied)，且 stdout/stderr 不包含 "retry" / "backoff"
        code, output = run(
            "nfs_walkdir DENY (expected PermissionDenied, no retry)",
            ["cargo", "run", "--example", "nfs_walkdir", "--", deny_url],
            expected=1,
            capture=True,
        )
        if "retry" in output.lower() or "backoff" in output.lower():
            print("[skill e2e-nfs] FAIL: deny path triggered retry/backoff (taxonomy bug)", file=sys.stderr)
            return 1
        if "permissiondenied" not in output.lower() and "permission denied" not in output.lower():
            print("[skill e2e-nfs] FAIL: deny path did not return PermissionDenied", file=sys.stderr)
            return 1
        print("[skill e2e-nfs] PASS: retry taxonomy honored (deny_list)")

    print("\n[skill e2e-nfs] all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
