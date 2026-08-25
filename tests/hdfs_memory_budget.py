#!/usr/bin/env python3
"""Validate the bounded-memory contract for real HDFS copy measurements."""

from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path


MIB = 1024 * 1024
COPY_CHANNEL_CHUNKS = 4
ACTIVE_CHUNKS = 2
FIXED_OVERHEAD_MIB = 96
# Keep the size-scaling guard below the absolute budget while allowing for
# page-level allocator variance between otherwise identical release runs.
GROWTH_ALLOWANCE_MIB = 72


def budget_mib(
    file_concurrency: int,
    chunk_mib: int,
    read_inflight: int,
    write_inflight: int,
) -> int:
    if min(file_concurrency, chunk_mib, read_inflight, write_inflight) < 1:
        raise ValueError("memory budget inputs must all be positive")
    # Each HDFS range can coexist as DataNode packet storage, hdfs-native's
    # aggregate BytesMut and the published Bytes. Add the common copy channel,
    # destination write window and two actively processed chunks.
    retained = (
        3 * read_inflight
        + write_inflight
        + COPY_CHANNEL_CHUNKS
        + ACTIVE_CHUNKS
    )
    return file_concurrency * retained * chunk_mib + FIXED_OVERHEAD_MIB


def validate(path: Path) -> None:
    with path.open(newline="") as stream:
        rows = list(csv.DictReader(stream))
    if not rows:
        raise ValueError("HDFS memory CSV contains no measurements")

    grouped: dict[tuple[str, str], list[tuple[int, int]]] = defaultdict(list)
    for row in rows:
        measured = int(row["max_rss_kib"])
        calculated_budget = budget_mib(
            int(row["file_concurrency"]),
            int(row["chunk_mib"]),
            int(row["read_inflight"]),
            int(row["write_inflight"]),
        )
        declared_budget = int(row["budget_mib"])
        if declared_budget != calculated_budget:
            raise ValueError(
                f"{row['profile']} {row['direction']} declared budget "
                f"{declared_budget} MiB does not match calculated budget "
                f"{calculated_budget} MiB"
            )
        budget = calculated_budget * 1024
        if measured > budget:
            raise ValueError(
                f"{row['profile']} {row['direction']} RSS {measured} KiB exceeds "
                f"budget {budget} KiB"
            )
        grouped[(row["profile"], row["direction"])].append(
            (int(row["bytes"]), measured)
        )

    for key, samples in grouped.items():
        if len(samples) != 2:
            raise ValueError(f"{key} must contain exactly two payload sizes")
        samples.sort()
        growth = samples[1][1] - samples[0][1]
        if growth > GROWTH_ALLOWANCE_MIB * 1024:
            raise ValueError(
                f"{key} RSS grew by {growth} KiB, above the "
                f"{GROWTH_ALLOWANCE_MIB} MiB allowance"
            )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("csv", type=Path)
    args = parser.parse_args()
    validate(args.csv)
    print(f"HDFS memory contract verified: {args.csv}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError) as error:
        print(f"HDFS memory contract failed: {error}", file=sys.stderr)
        sys.exit(1)
