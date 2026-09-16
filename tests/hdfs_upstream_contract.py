#!/usr/bin/env python3
"""Validate and exercise the hdfs-native hsync dependency contract."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LOCK_SOURCE = re.compile(
    r"^git\+https://github\.com/JayTsu-sh/hdfs-native"
    r"\?rev=373739fc0fb69f3f3cd2a32db58170d8cf83a514"
    r"#([0-9a-f]{40})$"
)
EXPECTED_DEPENDENCY = {
    "git": "https://github.com/JayTsu-sh/hdfs-native",
    "rev": "373739fc0fb69f3f3cd2a32db58170d8cf83a514",
}


def resolved_commit(lock_data: bytes) -> str:
    lock = tomllib.loads(lock_data.decode())
    packages = [package for package in lock["package"] if package["name"] == "hdfs-native"]
    if len(packages) != 1:
        raise ValueError(f"expected one hdfs-native lock entry, found {len(packages)}")
    source = packages[0].get("source", "")
    match = LOCK_SOURCE.fullmatch(source)
    if match is None:
        raise ValueError(f"unexpected hdfs-native lock source: {source!r}")
    return match.group(1)


def validate_manifest() -> None:
    manifest = tomllib.loads((ROOT / "Cargo.toml").read_text())
    dependency = manifest["dependencies"]["hdfs-native"]
    if dependency != EXPECTED_DEPENDENCY:
        raise ValueError(
            "hdfs-native must pin the verified revision that provides durable FileWriter::hsync"
        )


def validate_hsync() -> None:
    metadata = subprocess.run(
        ["cargo", "metadata", "--locked", "--format-version", "1"],
        cwd=ROOT, check=True, capture_output=True, text=True,
    )
    package = next(p for p in json.loads(metadata.stdout)["packages"] if p["name"] == "hdfs-native")
    source = Path(package["manifest_path"]).parent / "src"
    connection = (source / "hdfs" / "connection.rs").read_text()
    writer = (source / "hdfs" / "block_writer.rs").read_text()
    file_writer = (source / "file.rs").read_text()
    client = (source / "client.rs").read_text()
    if "pub async fn hsync(&mut self)" not in file_writer:
        raise ValueError("HDFS checkpoints require FileWriter::hsync")
    if "self.last_block.as_ref().map(|block| block.b.clone())" not in file_writer:
        raise ValueError("HDFS recovery requires empty append close to complete the last block")
    if "pub async fn recover_lease(&self, src: &str)" not in client:
        raise ValueError("HDFS recovery requires the explicit lease-recovery client API")
    if "set_sync_block" not in connection or "wait_for_ack" not in writer:
        raise ValueError("HDFS hsync must send sync_block and wait for its pipeline ACK")


def previous_commit() -> str | None:
    result = subprocess.run(
        ["git", "show", "HEAD^:Cargo.lock"],
        cwd=ROOT,
        check=False,
        capture_output=True,
    )
    if result.returncode != 0:
        return None
    try:
        return resolved_commit(result.stdout)
    except ValueError:
        return None


def run(command: list[str]) -> None:
    subprocess.run(command, cwd=ROOT, check=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--nightly",
        action="store_true",
        help="also run the complete real HDFS lab contract",
    )
    parser.add_argument(
        "--run-id",
        help="validated nightly-* or release-* identifier required with --nightly",
    )
    args = parser.parse_args()

    validate_manifest()
    validate_hsync()
    current = resolved_commit((ROOT / "Cargo.lock").read_bytes())
    previous = previous_commit()
    if previous is not None and previous != current:
        print(f"hdfs-native resolved commit: {previous} -> {current}", flush=True)
    else:
        print(f"hdfs-native resolved commit: {current}", flush=True)

    run(["cargo", "test", "--locked", "--test", "hdfs_native_contract"])
    if args.nightly:
        if args.run_id is None:
            raise ValueError("--run-id is required with --nightly")
        run(["bash", "tests/lab/run-hdfs-smoke.sh", args.run_id])
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, subprocess.CalledProcessError, ValueError) as error:
        print(f"HDFS hsync contract failed: {error}", file=sys.stderr)
        sys.exit(1)
