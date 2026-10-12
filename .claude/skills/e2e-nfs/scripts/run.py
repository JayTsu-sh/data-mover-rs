#!/usr/bin/env python3
"""e2e-nfs skill runner — 需要真 NFS 服务器。"""

from __future__ import annotations

import os
import re
import shlex
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


def run(
    label: str,
    cmd: list[str],
    expected: int | None = 0,
    capture: bool = False,
    extra_env: dict[str, str] | None = None,
) -> tuple[int, str]:
    print(f"\n[skill e2e-nfs] $ {shlex.join(cmd)}")
    child_env = {**os.environ, **(extra_env or {})}
    # Only what this run asks for: a value left in the calling shell must not leak in.
    if extra_env is not None and "NFS_REAL_RO_PATH" not in extra_env:
        child_env.pop("NFS_REAL_RO_PATH", None)
    if capture:
        result = subprocess.run(
            cmd, cwd=PROJECT_ROOT, capture_output=True, text=True,
            encoding="utf-8", errors="replace", env=child_env,
        )
        print(result.stdout)
        if result.stderr:
            print("[stderr]", result.stderr)
        if expected is not None:
            assert_exit_code(label, result.returncode, expected)
        return result.returncode, result.stdout + result.stderr
    result = subprocess.run(cmd, cwd=PROJECT_ROOT, env=child_env)
    if expected is not None:
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
    # Extra URL query, e.g. `version=4.1&noresvport=true` (nfs-rs defaults to NFSv3, which a
    # `mount_rootonly` server refuses from behind NAT such as WSL2).
    options = env.get("NFS_OPTIONS", "").strip()
    # Walk only this sub-directory when set, rather than the whole export.
    walk_path = env.get("NFS_WALK_PATH", "").strip().strip("/")

    run("build storage_role_operations", ["cargo", "build", "--example", "storage_role_operations"])
    run("build nfs_export", ["cargo", "build", "--example", "nfs_export"])
    run("build nfs_opt_dir", ["cargo", "build", "--example", "nfs_opt_dir"])

    url = nfs_url(host, export, port=port, uid=uid, gid=gid, options=options)
    walk_url = nfs_url(host, export, sub_path=walk_path, port=port, uid=uid, gid=gid, options=options)
    traverse = ["cargo", "run", "--example", "storage_role_operations", "--", "--backend", "nfs", "traverse", "--quiet"]
    _, output = run("role traversal", traverse, capture=True, extra_env={"NFS_REAL_URL": walk_url})
    totals = next((line for line in output.splitlines() if line.startswith("totals ")), "")
    fields = dict(part.split("=", 1) for part in totals.split()[1:])
    if any(fields.get(key) != value for key, value in {"balanced": "true", "failed": "0", "listing_failures": "0", "interrupted": "0"}.items()):
        print("[skill e2e-nfs] FAIL: traversal did not complete with balanced, failure-free totals", file=sys.stderr)
        return 1
    run("nfs_export", ["cargo", "run", "--example", "nfs_export", "--", url])
    # Creates and removes only its own `dm-e2e-opt-dir-<nanos>` below the export.
    run("nfs_opt_dir", ["cargo", "run", "--example", "nfs_opt_dir", "--", url])
    # create_directory_all through levels the caller may not write (tests/nfs_mkdir_contract.rs):
    # creates and removes only its own `dm-mkdir-<nanos>` below the export; NFS_UID must be able to
    # chmod what it creates, and uids 4242 / 4243 must not be it. NFS_RO_PATH (optional) is an
    # existing directory on a read-only file system, `{scratch}` = the test's directory (ONTAP:
    # `{scratch}/.snapshot`); it is never written.
    mkdir_env = {"NFS_REAL_URL": url}
    ro_path = env.get("NFS_RO_PATH", "").strip()
    if ro_path:
        mkdir_env["NFS_REAL_RO_PATH"] = ro_path
    run(
        "nfs_mkdir_contract",
        ["cargo", "test", "--test", "nfs_mkdir_contract", "--", "--ignored", "--nocapture"],
        extra_env=mkdir_env,
    )

    # 拒绝访问：NFS_DENY_DIR 相对 export，NFS_UID/NFS_GID 必须无权读它（非 root，或服务器 squash root）。
    # 遍历应报权限错误（role CLI 的条目/列举失败可能仍以 0 退出）。「权限错误不重试」由 src/nfs.rs 的
    # test_is_retryable_with_invalidation_excludes_unrelated 单测覆盖：这个 example 不打 retry 日志，
    # 这里无从观察。
    if deny_dir:
        deny_url = nfs_url(
            host, export, sub_path=deny_dir.strip("/"), port=port, uid=uid, gid=gid, options=options
        )
        code, output = run(
            "role traversal DENY (expected permission error)",
            traverse,
            extra_env={"NFS_REAL_URL": deny_url},
            expected=None,
            capture=True,
        )
        # Reject build failures and crashes; accept only the role or NFS server's typed denial.
        if code not in (0, 1) or not re.search(r"\b(?:PermissionDenied|NFS3ERR_ACCES|NFS4ERR_ACCESS)\b", output):
            print("[skill e2e-nfs] FAIL: deny path did not report a typed NFS permission error", file=sys.stderr)
            return 1
        print("[skill e2e-nfs] PASS: deny path reports a permission error")

    print("\n[skill e2e-nfs] all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
