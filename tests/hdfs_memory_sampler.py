#!/usr/bin/env python3
"""Linux-only child RSS trace; the sampler and integrity inspector are excluded."""

from __future__ import annotations

import argparse
import csv
import math
import resource
import signal
import subprocess
import sys
import time
from pathlib import Path


def measure(args: argparse.Namespace) -> int:
    command = args.command
    if command[:1] == ["--"]:
        command = command[1:]
    if not command:
        raise ValueError("a child command is required")
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        raise ValueError("timeout must be positive and finite")
    if args.metrics.exists():
        raise ValueError("refusing to reuse stale success metrics")
    started = time.monotonic()
    sampled_peak = 0
    with args.trace.open("x", newline="") as trace, args.log.open("w") as log:
        writer = csv.writer(trace)
        writer.writerow(["run_id", "commit", "pid", "phase", "elapsed_s", "rss_kib"])
        trace.flush()
        with subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT) as child:
            try:
                phase = "startup"
                pending = ""
                with args.log.open() as reader:
                    while child.poll() is None:
                        pending += reader.read()
                        lines = pending.split("\n")
                        pending = lines.pop()
                        for line in lines:
                            if line.startswith("memory_phase\t"):
                                phase = line.split("\t", 1)[1]
                        try:
                            status = Path(f"/proc/{child.pid}/status").read_text()
                        except FileNotFoundError:
                            break
                        rss = next(
                            (int(line.split()[1]) for line in status.splitlines()
                             if line.startswith("VmRSS:")), 0,
                        )
                        sampled_peak = max(sampled_peak, rss)
                        elapsed = time.monotonic() - started
                        if rss > 0:
                            writer.writerow([
                                args.run_id, args.commit, child.pid, phase,
                                f"{elapsed:.6f}", rss,
                            ])
                            trace.flush()
                        if elapsed > args.timeout:
                            raise TimeoutError(f"memory measurement exceeded {args.timeout} seconds")
                        time.sleep(0.1)
                result = child.wait()
            except BaseException:
                child.kill()
                child.wait()
                raise
    if result != 0:
        return result if result > 0 else 128 - result
    # This fresh wrapper creates exactly one child; RUSAGE_CHILDREN includes its
    # actual high-water mark, including peaks between /proc samples, not ours.
    # Linux's asynchronous RSS counters can disagree slightly with wait's HWM;
    # retain the larger observation, never discard a sampled spike.
    peak = max(sampled_peak, resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss)
    args.metrics.write_text(f"{time.monotonic() - started:.6f},{peak}\n")
    return 0


def main() -> int:
    def interrupted(signum, _frame):
        raise InterruptedError(f"memory sampler received signal {signum}")

    signal.signal(signal.SIGTERM, interrupted)
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("trace", "metrics", "log"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--timeout", type=float, default=5400)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    return measure(parser.parse_args())


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, TimeoutError) as error:
        print(f"HDFS memory sampler failed: {error}", file=sys.stderr)
        sys.exit(1)
