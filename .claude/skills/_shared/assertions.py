"""Common assertions for skill runners."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path


def assert_exit_code(label: str, code: int, expected: int = 0) -> None:
    if code != expected:
        print(f"[skill] FAIL {label}: exit code {code}, expected {expected}", file=sys.stderr)
        sys.exit(1)
    print(f"[skill] PASS {label}: exit code {code}")


def assert_file_exists(path: str | Path) -> None:
    p = Path(path)
    if not p.exists():
        print(f"[skill] FAIL: file does not exist: {p}", file=sys.stderr)
        sys.exit(1)
    print(f"[skill] PASS: file exists: {p}")


def assert_file_count(directory: str | Path, expected: int) -> None:
    d = Path(directory)
    actual = sum(1 for _ in d.rglob("*") if _.is_file())
    if actual != expected:
        print(f"[skill] FAIL: file count in {d}: {actual}, expected {expected}", file=sys.stderr)
        sys.exit(1)
    print(f"[skill] PASS: file count in {d}: {actual}")


def blake3_or_sha256(path: str | Path) -> str:
    """Use blake3 if available, fallback to sha256.

    The library uses blake3 (Cargo.toml). Skill harness keeps
    sha256 fallback so this script runs in any Python env.
    """
    try:
        import blake3 as _b3  # type: ignore[import-not-found]
        h = _b3.blake3()
    except ImportError:
        h = hashlib.sha256()
    p = Path(path)
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def assert_files_equal(a: str | Path, b: str | Path) -> None:
    ha = blake3_or_sha256(a)
    hb = blake3_or_sha256(b)
    if ha != hb:
        print(f"[skill] FAIL: hash mismatch\n  {a}: {ha}\n  {b}: {hb}", file=sys.stderr)
        sys.exit(1)
    print(f"[skill] PASS: files equal ({a} ↔ {b})")
