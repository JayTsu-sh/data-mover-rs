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
SCALE_SMALL_BYTES = 1024**3 + 137
SCALE_LARGE_BYTES = 100 * 1024**3 + 137
SCALE_SHORT_SAMPLES = 6


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


def validate(path: Path, *, require_100_gib: bool = False) -> None:
    with path.open(newline="") as stream:
        rows = list(csv.DictReader(stream))
    if not rows:
        raise ValueError("HDFS memory CSV contains no measurements")
    run_ids = {row["run_id"] for row in rows}
    commits = {row["commit"] for row in rows}
    if len(run_ids) != 1 or "" in run_ids:
        raise ValueError("HDFS memory CSV must bind exactly one non-empty run id")
    if len(commits) != 1 or any(len(value) != 40 for value in commits):
        raise ValueError("HDFS memory CSV must bind exactly one full commit")

    grouped: dict[tuple[str, str, str], list[tuple[int, int, tuple[int, ...]]]] = defaultdict(list)
    has_100_gib = False
    for row in rows:
        size = int(row["bytes"])
        has_100_gib |= row["sample_set"] == "scale" and size == SCALE_LARGE_BYTES
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
        settings = (
            int(row["file_concurrency"]),
            int(row["chunk_mib"]),
            int(row["read_inflight"]),
            int(row["write_inflight"]),
        )
        grouped[(row["sample_set"], row["profile"], row["direction"])].append(
            (size, measured, settings)
        )

    if require_100_gib and not has_100_gib:
        raise ValueError("HDFS memory CSV must contain a real 100 GiB sample")

    for key, samples in grouped.items():
        samples.sort()
        if key[0] == "scale":
            if key[1:] != ("high", "hdfs-hdfs"):
                raise ValueError("100 GiB scale pair must use high HDFS-to-HDFS profile")
            short = [sample for sample in samples if sample[0] == SCALE_SMALL_BYTES]
            large = [sample for sample in samples if sample[0] == SCALE_LARGE_BYTES]
            if len(short) < SCALE_SHORT_SAMPLES or len(large) != 1:
                raise ValueError(
                    f"{key} must contain at least {SCALE_SHORT_SAMPLES} 1 GiB "
                    "samples and exactly one 100 GiB sample"
                )
            if any(sample[2] != large[0][2] for sample in short):
                raise ValueError(f"{key} scale samples must use identical settings")
            stable_short_peak = max(sample[1] for sample in short)
            if (large[0][1] - stable_short_peak) * 10 > stable_short_peak:
                raise ValueError(f"{key} RSS grows by more than 10% at 100 GiB")
            continue
        if len(samples) != 2:
            raise ValueError(f"{key} must contain exactly two payload sizes")
        if key[0] != "baseline":
            raise ValueError(f"unknown HDFS memory sample set: {key[0]}")
        growth = samples[1][1] - samples[0][1]
        if growth > GROWTH_ALLOWANCE_MIB * 1024:
            raise ValueError(
                f"{key} RSS grew by {growth} KiB, above the "
                f"{GROWTH_ALLOWANCE_MIB} MiB allowance"
            )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("csv", type=Path)
    parser.add_argument("--require-100-gib", action="store_true")
    args = parser.parse_args()
    validate(args.csv, require_100_gib=args.require_100_gib)
    print(f"HDFS memory contract verified: {args.csv}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError) as error:
        print(f"HDFS memory contract failed: {error}", file=sys.stderr)
        sys.exit(1)
