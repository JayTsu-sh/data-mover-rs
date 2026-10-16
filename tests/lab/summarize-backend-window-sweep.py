#!/usr/bin/env python3
"""Summarize retained samples; round zero is always excluded."""

import argparse
import csv
import math
from pathlib import Path
import statistics
import sys
from collections import defaultdict


def summarize(directory):
    for path in sorted(Path(directory).glob("*.csv")):
        groups = defaultdict(list)
        with path.open(newline="") as stream:
            reader = csv.DictReader(stream)
            if not {"round", "read", "write", "elapsed_ns"}.issubset(reader.fieldnames or []):
                continue
            for row in reader:
                if int(row["round"]) > 0:
                    groups[(row["policy"], int(row["read"]), int(row["write"]))].append(row)
        for key, rows in sorted(groups.items()):
            durations = sorted(int(row["elapsed_ns"]) / 1_000_000 for row in rows)
            rates = [int(row["bytes"]) / int(row["elapsed_ns"]) * 1e9 / 1048576 for row in rows]
            cpu = [float(row["user_s"]) + float(row["system_s"]) for row in rows]
            rss = [int(row["max_rss_kib"]) / 1024 for row in rows]
            yield [path.stem, *key, len(rows), rows[0]["bytes"],
                   f"{statistics.median(rates):.3f}",
                   f"{durations[math.ceil(.95 * len(rows)) - 1]:.3f}",
                   f"{statistics.median(cpu):.3f}",
                   f"{statistics.median(rss):.3f}", f"{max(rss):.3f}"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    writer = csv.writer(sys.stdout, lineterminator="\n")
    writer.writerow(["profile", "policy", "read", "write", "samples", "bytes",
                     "median_mib_s", "sample_p95_ms", "median_process_cpu_s",
                     "median_process_peak_rss_mib", "max_process_peak_rss_mib"])
    writer.writerows(summarize(args.directory))


if __name__ == "__main__":
    main()
