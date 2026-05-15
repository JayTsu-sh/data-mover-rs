"""Protocol constants shared across e2e skills.

Keep credentials and host-specific things in .env files (gitignored).
This module only holds protocol-level defaults.
"""

from __future__ import annotations

from pathlib import Path


# ── CIFS / SMB ────────────────────────────────────────────────────────
class CIFS:
    DEFAULT_PORT = 445
    URL_SCHEME = "smb"
    # smb2_only=true: 直接 SMB2 NegotiateRequest，跳过 SMB1 多协议探测帧 (commit af0e017)
    DEFAULT_SMB2_ONLY = True
    # anon=true: 空密码 + 无签名 (commit 9b332aa)
    DEFAULT_ANON = False


# ── NFS ───────────────────────────────────────────────────────────────
class NFS:
    DEFAULT_PORT = 2049
    URL_SCHEME = "nfs"
    DEFAULT_UID = 1000
    DEFAULT_GID = 1000
    # retry taxonomy (commit 7eb3046)
    DENY_LIST_ERRNOS = ("EACCES", "EPERM")
    BACKOFF_ERRNOS = ("EAGAIN", "ECONNRESET", "ETIMEDOUT")


# ── S3 ────────────────────────────────────────────────────────────────
class S3:
    DEFAULT_PORT_HTTP = 80
    DEFAULT_PORT_HTTPS = 443
    URL_SCHEME_HTTP = "s3"
    URL_SCHEME_HTTPS = "s3+https"
    # 404 → FileNotFound 而不是 retry (commit 7eb3046)
    NOT_FOUND_TO_FILENOTFOUND = True


# ── Local ─────────────────────────────────────────────────────────────
class Local:
    URL_SCHEME = ""  # 无 scheme = local
    TMP_PREFIX = "/tmp/data-mover-test"


# ── 通用 ──────────────────────────────────────────────────────────────
COPY_PIPELINE_CAPACITY = 2  # storage_enum.rs 内置常数
TAR_PIPELINE_CAPACITY = 16  # storage_enum.rs 内置常数

CARGO_BIN = "cargo"
# Resolve PROJECT_ROOT relative to this file's location so the same scripts
# work both in local dev (Windows/Mac/Linux home checkouts) and on GitHub
# Actions runners (where the repo lives under /home/runner/work/<repo>/<repo>/).
# This file is at <repo>/.claude/skills/_shared/protocol_constants.py — go up
# 4 levels to reach <repo>.
PROJECT_ROOT = str(Path(__file__).resolve().parents[3])
