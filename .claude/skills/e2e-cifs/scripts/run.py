#!/usr/bin/env python3
"""e2e-cifs skill runner — 需要真 SMB 服务器 (role-based CIFS backend)。"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_DIR = SCRIPT_DIR.parent
SHARED = SKILL_DIR.parent / "_shared"
sys.path.insert(0, str(SHARED))

from assertions import assert_exit_code  # noqa: E402
from env_loader import load_env, require  # noqa: E402
from protocol_constants import PROJECT_ROOT  # noqa: E402

REQUIRED = ("CIFS_REAL_SERVER", "CIFS_REAL_SHARE", "CIFS_REAL_USER", "CIFS_REAL_PASS")


def run(label: str, cmd: list[str], env: dict[str, str]) -> int:
    print(f"\n[skill e2e-cifs] $ {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=PROJECT_ROOT, env=env)
    assert_exit_code(label, result.returncode)
    return result.returncode


def main() -> int:
    env = load_env(SKILL_DIR)
    require(env, *REQUIRED)

    child_env = dict(os.environ)
    for key in (*REQUIRED, "CIFS_REAL_SECOND_SERVER", "CIFS_POLICY_TEST_BYTES", "CIFS_PROBE_NFS_URL", "CIFS_REAL_GUEST_POLICY"):
        if key in env:
            child_env[key] = env[key]
    child_env.setdefault("CIFS_REAL_SECOND_SERVER", child_env["CIFS_REAL_SERVER"])
    child_env.setdefault(
        "DATA_MOVER_RECOVERY_DIR", tempfile.mkdtemp(prefix="data-mover-cifs-recovery-")
    )

    # role-based 传输入口必须能编译；实际传输由 policy contract 覆盖。
    run(
        "build cifs_mount_comparison",
        ["cargo", "build", "--release", "--example", "cifs_mount_comparison"],
        child_env,
    )
    for test in ("cifs_policy_contract", "cifs_namespace_contract", "cifs_capability_probe"):
        run(
            test,
            ["cargo", "test", "--release", "--test", test, "--", "--ignored", "--nocapture"],
            child_env,
        )

    print(
        "\n[skill e2e-cifs] passed: policy + namespace contracts on real share; "
        "see [probe] lines above for capability evidence"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
