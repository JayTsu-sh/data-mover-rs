#!/usr/bin/env python3
"""Validate and summarize reproducible data-mover performance measurements."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path


FIELDS = {
    "schema_version",
    "run_id",
    "commit",
    "hardware_id",
    "dataset_id",
    "operation",
    "source",
    "destination",
    "concurrency",
    "chunk_bytes",
    "inflight",
    "repeat",
    "entries",
    "bytes",
    "elapsed_ms",
    "p95_scheduling_latency_ms",
    "max_rss_kib",
}
FIXED_FIELDS = (
    "schema_version",
    "run_id",
    "commit",
    "hardware_id",
    "dataset_id",
    "concurrency",
    "chunk_bytes",
    "inflight",
)
REQUIRED_OPERATIONS = {"copy-large", "copy-small", "scan-small"}
INTEGER_FIELDS = {
    "schema_version",
    "concurrency",
    "chunk_bytes",
    "inflight",
    "repeat",
    "entries",
    "bytes",
    "max_rss_kib",
}


def fail(message: str) -> "NoReturn":
    raise ValueError(message)


def load_samples(path: Path) -> list[dict[str, object]]:
    with path.open(newline="") as source:
        reader = csv.DictReader(source)
        missing = FIELDS.difference(reader.fieldnames or ())
        if missing:
            fail(f"missing measurement columns: {', '.join(sorted(missing))}")
        rows: list[dict[str, object]] = []
        for line, raw in enumerate(reader, start=2):
            row: dict[str, object] = dict(raw)
            try:
                for field in INTEGER_FIELDS:
                    row[field] = int(raw[field])
                row["elapsed_ms"] = float(raw["elapsed_ms"])
                row["p95_scheduling_latency_ms"] = (
                    None
                    if raw["p95_scheduling_latency_ms"] == ""
                    else float(raw["p95_scheduling_latency_ms"])
                )
            except (TypeError, ValueError) as error:
                fail(f"invalid numeric value at CSV line {line}: {error}")
            if (
                row["entries"] <= 0
                or row["elapsed_ms"] <= 0
                or (
                    row["p95_scheduling_latency_ms"] is not None
                    and row["p95_scheduling_latency_ms"] < 0
                )
            ):
                fail(f"entries and elapsed_ms must be positive and p95 non-negative at CSV line {line}")
            rows.append(row)
    if not rows:
        fail("measurement CSV is empty")
    return rows


def one_value(rows: list[dict[str, object]], field: str) -> object:
    values = {row[field] for row in rows}
    if len(values) != 1:
        fail(f"baseline mixes different {field} values: {sorted(values)}")
    return values.pop()


def summarize(samples_path: Path) -> dict[str, object]:
    rows = load_samples(samples_path)
    conditions = {field: one_value(rows, field) for field in FIXED_FIELDS}
    if conditions["schema_version"] != 1:
        fail(f"unsupported schema_version: {conditions['schema_version']}")
    expected_conditions = {
        "dataset_id": "data-mover-performance-v1",
        "concurrency": 1,
        "chunk_bytes": 2 * 1024 * 1024,
        "inflight": 8,
    }
    for field, expected in expected_conditions.items():
        if conditions[field] != expected:
            fail(f"{field} must be fixed at {expected}, got {conditions[field]}")
    commit = str(conditions["commit"])
    if len(commit) != 40 or any(character not in "0123456789abcdef" for character in commit):
        fail("commit must be a full lowercase 40-character Git SHA")

    operations = {str(row["operation"]) for row in rows}
    missing_operations = REQUIRED_OPERATIONS.difference(operations)
    if missing_operations:
        fail(f"baseline is missing required operations: {', '.join(sorted(missing_operations))}")
    if operations != REQUIRED_OPERATIONS:
        fail(f"baseline contains unexpected operations: {', '.join(sorted(operations - REQUIRED_OPERATIONS))}")

    backends = {"local", "nfs3", "nfs41", "s3"}
    expected_large = {(source, destination, repeat) for source in backends for destination in backends for repeat in (1, 2)}
    actual_large = {
        (str(row["source"]), str(row["destination"]), int(row["repeat"]))
        for row in rows
        if row["operation"] == "copy-large"
    }
    if actual_large != expected_large or sum(row["operation"] == "copy-large" for row in rows) != 32:
        fail("copy-large must contain exactly two repeats of the complete 4x4 directed matrix")
    if any(row["p95_scheduling_latency_ms"] is not None for row in rows if row["operation"] == "copy-large"):
        fail("copy-large scheduling latency is not applicable and must be empty")
    for operation, destination in (("scan-small", ""), ("copy-small", "local")):
        samples = [row for row in rows if row["operation"] == operation]
        repeats = {int(row["repeat"]) for row in samples}
        if (
            len(samples) != 5
            or repeats != {1, 2, 3, 4, 5}
            or any(row["source"] != "local" or row["destination"] != destination or row["entries"] != 100 for row in samples)
            or any(row["p95_scheduling_latency_ms"] is None for row in samples)
        ):
            fail(f"{operation} must contain five Local repeats of exactly 100 entries")

    groups: dict[tuple[str, str, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        groups[(str(row["operation"]), str(row["source"]), str(row["destination"]))].append(row)

    metrics = []
    for (operation, source, destination), samples in sorted(groups.items()):
        entry_rates = [
            int(sample["entries"]) * 1000.0 / float(sample["elapsed_ms"])
            for sample in samples
        ]
        p95_latencies = [
            float(sample["p95_scheduling_latency_ms"])
            for sample in samples
            if sample["p95_scheduling_latency_ms"] is not None
        ]
        throughput = [
            int(sample["bytes"]) * 1000.0 / float(sample["elapsed_ms"]) / 1048576
            for sample in samples
        ]
        metrics.append(
            {
                "operation": operation,
                "source": source,
                "destination": destination or None,
                "samples": len(samples),
                "entries_per_second": round(statistics.median(entry_rates), 3),
                "p95_scheduling_latency_ms": (
                    round(statistics.median(p95_latencies), 3) if p95_latencies else None
                ),
                "throughput_mib_per_second": round(statistics.median(throughput), 3),
                "peak_rss_kib": max(int(sample["max_rss_kib"]) for sample in samples),
            }
        )

    digest = hashlib.sha256(samples_path.read_bytes()).hexdigest()
    return {
        "schema_version": 1,
        "evidence": {"samples_file": samples_path.name, "samples_sha256": digest},
        "conditions": conditions,
        "metrics": metrics,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    subcommands = parser.add_subparsers(dest="command", required=True)
    summarize_parser = subcommands.add_parser("summarize")
    summarize_parser.add_argument("samples", type=Path)
    summarize_parser.add_argument("output", type=Path)
    args = parser.parse_args()
    try:
        report = summarize(args.samples)
        args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    except (OSError, ValueError) as error:
        print(f"performance baseline error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
