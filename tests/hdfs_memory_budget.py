#!/usr/bin/env python3
"""Validate the bounded-memory contract for real HDFS copy measurements."""

from __future__ import annotations

import argparse
import csv
import math
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
SCALE_SHORT_SAMPLES = 1


def validate_trace(
    path: Path, *, run_id: str, commit: str, peak_kib: int, budget_kib: int,
) -> list[str]:
    """Compare steady-state windows within each phase of ONE long-lived process.

    Use time, not sample indices: dropping samples must not move the windows.
    Warmup (first 10%) and shutdown (last 10%) never become the reference.
    A p90 window ignores isolated allocator spikes, not sustained growth. The
    whole-process high-water mark independently enforces the absolute budget.
    """
    phases: dict[str, list[tuple[float, int]]] = defaultdict(list)
    order = {"startup": 0, "transfer": 1, "verification": 2, "complete": 3}
    last_time, last_phase = -1.0, -1
    process_id = None
    with path.open(newline="") as stream:
        for row in csv.DictReader(stream):
            if row["run_id"] != run_id or row["commit"] != commit:
                raise ValueError("memory trace is not bound to the measured run and commit")
            elapsed, rss = float(row["elapsed_s"]), int(row["rss_kib"])
            pid = int(row["pid"])
            if pid <= 0 or (process_id is not None and pid != process_id):
                raise ValueError("memory trace must measure exactly one child process")
            process_id = pid
            phase = row["phase"]
            if (
                not math.isfinite(elapsed) or elapsed <= last_time or rss <= 0
                or phase not in order or order[phase] < last_phase
            ):
                raise ValueError("invalid or out-of-order memory trace sample")
            if rss > budget_kib:
                raise ValueError(f"memory trace RSS {rss} KiB exceeds budget {budget_kib} KiB")
            if rss > peak_kib:
                raise ValueError("memory trace RSS exceeds the reported process peak")
            phases[phase].append((elapsed, rss))
            last_time, last_phase = elapsed, order[phase]

    diagnostics = []
    for phase in ("transfer", "verification"):
        samples = phases[phase]
        if len(samples) < 50 or samples[-1][0] - samples[0][0] < 5:
            raise ValueError(f"{phase} memory trace has insufficient steady-state coverage")
        if any(b[0] - a[0] > 2 for a, b in zip(samples, samples[1:])):
            raise ValueError(f"{phase} memory trace has a sampling gap above 2 seconds")
        start = samples[0][0]
        duration = samples[-1][0] - start
        windows = []
        for low, high in ((0.1, 0.3), (0.7, 0.9)):
            window = sorted(
                rss for elapsed, rss in samples
                if low <= (elapsed - start) / duration <= high
            )
            if len(window) < 10:
                raise ValueError(f"{phase} memory trace has insufficient window samples")
            windows.append(window[math.ceil(0.9 * len(window)) - 1])
        early, late = windows
        growth = (late - early) / early * 100
        if (late - early) * 10 > early:
            raise ValueError(
                f"{phase} steady-state RSS grows by {growth:.2f}% (above 10%): "
                f"early p90={early} KiB, late p90={late} KiB"
            )
        diagnostics.append(
            f"{phase}: early p90={early} KiB, late p90={late} KiB, growth={growth:.2f}%"
        )
    return diagnostics


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


def validate(path: Path, *, require_100_gib: bool = False, trace: Path | None = None) -> None:
    with path.open(newline="") as stream:
        rows = list(csv.DictReader(stream))
    if not rows:
        raise ValueError("HDFS memory CSV contains no measurements")
    run_ids = {row["run_id"] for row in rows}
    commits = {row["commit"] for row in rows}
    if len(run_ids) != 1 or "" in run_ids:
        raise ValueError("HDFS memory CSV must bind exactly one non-empty run id")
    if len(commits) != 1 or any(
        len(value) != 40 or any(char not in "0123456789abcdef" for char in value)
        for value in commits
    ):
        raise ValueError("HDFS memory CSV must bind exactly one full commit")

    grouped: dict[tuple[str, str, str], list[tuple[int, int, tuple[int, ...]]]] = defaultdict(list)
    has_100_gib = False
    for row in rows:
        size = int(row["bytes"])
        has_100_gib |= row["sample_set"] == "scale" and size == SCALE_LARGE_BYTES
        measured = int(row["max_rss_kib"])
        if size <= 0 or measured <= 0:
            raise ValueError("HDFS memory sizes and RSS must be positive")
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
        if int(row.get("channel_chunks", COPY_CHANNEL_CHUNKS)) != COPY_CHANNEL_CHUNKS:
            raise ValueError("HDFS memory samples must use the fixed copy channel")
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
            if len(short) != SCALE_SHORT_SAMPLES or len(large) != 1 or len(samples) != 2:
                raise ValueError(
                    f"{key} must contain exactly {SCALE_SHORT_SAMPLES} 1 GiB "
                    "samples and exactly one 100 GiB sample"
                )
            if any(sample[2] != large[0][2] for sample in short):
                raise ValueError(f"{key} scale samples must use identical settings")
            if large[0][2] != (1, 2, 8, 16):
                raise ValueError(f"{key} scale samples must use the real high profile settings")
            if trace is None:
                raise ValueError("100 GiB growth gate requires a same-process memory trace")
            for diagnostic in validate_trace(
                trace, run_id=next(iter(run_ids)), commit=next(iter(commits)),
                peak_kib=large[0][1],
                budget_kib=budget_mib(*large[0][2]) * 1024,
            ):
                print(diagnostic)
            continue
        if len(samples) != 2:
            raise ValueError(f"{key} must contain exactly two payload sizes")
        if key[0] != "baseline":
            raise ValueError(f"unknown HDFS memory sample set: {key[0]}")
        if samples[0][0] == samples[1][0] or samples[0][2] != samples[1][2]:
            raise ValueError(f"{key} needs distinct sizes with identical settings")
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
    parser.add_argument("--trace", type=Path)
    args = parser.parse_args()
    validate(args.csv, require_100_gib=args.require_100_gib, trace=args.trace)
    print(f"HDFS memory contract verified: {args.csv}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"HDFS memory contract failed: {error}", file=sys.stderr)
        sys.exit(1)
