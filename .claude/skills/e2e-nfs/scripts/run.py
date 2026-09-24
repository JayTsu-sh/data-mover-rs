#!/usr/bin/env python3
"""e2e-nfs skill runner — 需要真 NFS 服务器。"""

from __future__ import annotations

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


def run(label: str, cmd: list[str], expected: int = 0, capture: bool = False) -> tuple[int, str]:
    print(f"\n[skill e2e-nfs] $ {shlex.join(cmd)}")
    if capture:
        result = subprocess.run(
            cmd, cwd=PROJECT_ROOT, capture_output=True, text=True,
            encoding="utf-8", errors="replace",
        )
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
    # Extra URL query, e.g. `version=4.1&noresvport=true` (nfs-rs defaults to NFSv3, which a
    # `mount_rootonly` server refuses from behind NAT such as WSL2).
    options = env.get("NFS_OPTIONS", "").strip()
    # Walk only this sub-directory when set, rather than the whole export.
    walk_path = env.get("NFS_WALK_PATH", "").strip().strip("/")

    run("build nfs_walkdir", ["cargo", "build", "--example", "nfs_walkdir"])
    run("build nfs_export", ["cargo", "build", "--example", "nfs_export"])
    run("build nfs_opt_dir", ["cargo", "build", "--example", "nfs_opt_dir"])

    url = nfs_url(host, export, port=port, uid=uid, gid=gid, options=options)
    walk_url = nfs_url(host, export, sub_path=walk_path, port=port, uid=uid, gid=gid, options=options)
    run("nfs_walkdir", ["cargo", "run", "--example", "nfs_walkdir", "--", "--url", walk_url])
    run("nfs_export", ["cargo", "run", "--example", "nfs_export", "--", url])
    # Creates and removes only its own `dm-e2e-opt-dir-<nanos>` below the export.
    run("nfs_opt_dir", ["cargo", "run", "--example", "nfs_opt_dir", "--", url])

    # 拒绝访问：NFS_DENY_DIR 相对 export，NFS_UID/NFS_GID 必须无权读它（非 root，或服务器 squash root）。
    # 遍历应报权限错误并以非 0 退出。「权限错误不重试」由 src/nfs.rs 的
    # test_is_retryable_with_invalidation_excludes_unrelated 单测覆盖：这个 example 不打 retry 日志，
    # 这里无从观察。
    if deny_dir:
        deny_url = nfs_url(
            host, export, sub_path=deny_dir.strip("/"), port=port, uid=uid, gid=gid, options=options
        )
        _, output = run(
            "nfs_walkdir DENY (expected permission error)",
            ["cargo", "run", "--example", "nfs_walkdir", "--", "--url", deny_url],
            expected=1,
            capture=True,
        )
        lowered = output.lower()
        # NFS3ERR_ACCES / NFS4ERR_ACCESS / "permission denied"
        if not any(word in lowered for word in ("acces", "permission")):
            print("[skill e2e-nfs] FAIL: deny path did not report a permission error", file=sys.stderr)
            return 1
        print("[skill e2e-nfs] PASS: deny path reports a permission error")

    print("\n[skill e2e-nfs] all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
