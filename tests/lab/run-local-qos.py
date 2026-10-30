#!/usr/bin/env python3
"""Four-scenario Local QoS acceptance. Uses real files; exits 1 on any failed gate."""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import tempfile
import time

MIB = 1024 * 1024
MINIMUM_RATIO = 0.98
ROOT = Path(__file__).resolve().parents[2]


def envelope_excess(events, rate, resource):
    """Maximum traffic minus rate*time over EVERY interval between admissions.

    Include time zero and the left limit at each event, so an event at the
    beginning of a window is not silently dropped. One packet is unavoidable.
    """
    total = minimum = excess = 0.0
    for event in events:
        before = total - rate * event["seconds"]
        minimum = min(minimum, before)
        total += event["bytes"] if resource == "bandwidth" else 1
        excess = max(excess, total - rate * event["seconds"] - minimum)
    return excess


def trailing_rate(result, resource, window=5.0):
    events = result.get("admissions", result["events"])
    end = events[-1]["seconds"]
    selected = [e for e in events if e["seconds"] >= end - window]
    if len(selected) < 2:
        raise ValueError("steady-rate validation requires at least two admissions")
    # Anchor both ends at actual admissions and exclude the left event. Fixed
    # windows quantize low-IOPS/large-request sources by a whole packet.
    start = selected[0]["seconds"]
    total = sum((e["bytes"] if resource == "bandwidth" else 1) for e in selected[1:])
    return total / (end - start)


def validate_trace(result, resource, soft, hard, peak, burst):
    events = result.get("admissions", result["events"])
    packet = max(e["bytes"] for e in events) if resource == "bandwidth" else 1
    capacity = (hard - soft) * peak
    # No percentage slack on hard or soft envelopes. The single-packet
    # allowance describes discrete IO, not permission to overshoot the rate.
    hard_excess = envelope_excess(events, hard, resource)
    soft_excess = envelope_excess(events, soft, resource)
    steady = trailing_rate(result, resource)
    first = sum((e["bytes"] if resource == "bandwidth" else 1)
                for e in events if e["seconds"] <= peak) / peak
    epsilon = 1e-6
    return {
        "hard_envelope": hard_excess <= packet + epsilon,
        "soft_envelope": soft_excess <= capacity + packet + epsilon,
        "steady_ceiling": steady <= soft + packet / 5 + epsilon,
        "steady_window_after_peak": not burst or events[-1]["seconds"] >= 2 * peak + 5,
        "steady_at_least_98_percent": steady >= MINIMUM_RATIO * soft,
        "burst_observed": first > soft * 1.05 if burst else True,
        "hard_max_excess": hard_excess,
        "soft_max_excess": soft_excess,
        "packet_allowance": packet,
        "soft_credit_capacity": capacity,
        "steady_rate": steady,
        "first_half_second_rate": first,
    }


def fixture(path, size):
    # Non-sparse, actually written data. Generation is outside all measurements.
    block = os.urandom(MIB)
    with path.open("wb") as output:
        for _ in range(size // MIB):
            output.write(block)
        output.flush()
        os.fsync(output.fileno())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True,
                        help="scratch filesystem with at least 1 GiB free")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--binary", type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    args.work_dir.mkdir(parents=True, exist_ok=True)
    if args.binary is None:
        subprocess.run(["cargo", "build", "--release", "--locked", "--no-default-features",
                        "--features", "transfer,backend-local,qos-diagnostics", "--example", "local_qos_acceptance"],
                       cwd=ROOT, check=True)
        metadata = json.loads(subprocess.check_output(
            ["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"], cwd=ROOT))
        args.binary = Path(metadata["target_directory"]) / "release/examples/local_qos_acceptance"
    metadata = {
        "git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "git_status": subprocess.check_output(["git", "status", "--short"], cwd=ROOT, text=True),
        "kernel": platform.platform(), "cpu_count": os.cpu_count(),
        "rustc": subprocess.check_output(["rustc", "--version"], text=True).strip(),
        "filesystem": subprocess.check_output(["df", "-T", str(args.work_dir)], text=True),
        "runtime_workers": 4, "repetitions": 3, "minimum_ratio": MINIMUM_RATIO,
        "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
        "cache": "source fully hashed before every timed run; warm page cache",
        "read_mode": "Local ReadSource, inflight=1; admission + completion traces and full BLAKE3",
        "copy_mode": "public transfer, inflight=8, default policy/read-back; admission trace and independent hash after timing",
        "trace_scope": "exact QoS admission times; completion times retained separately, not used to infer dispatch spacing",
        "hard_contract": "all-interval envelope <= hard*time + one actual IO packet; no percent slack",
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    (args.output / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
    checks = []
    rows = []

    def gate(name, passed, **details):
        checks.append({"name": name, "passed": bool(passed), **details})
        print(f"{'PASS' if passed else 'FAIL'} {name} {details}", flush=True)

    with tempfile.TemporaryDirectory(prefix="qos-", dir=args.work_dir) as scratch:
        source = Path(scratch) / "source"
        destination = Path(scratch) / "destination"
        source.mkdir()
        destination.mkdir()
        for size in (64, 128, 256):
            fixture(source / f"{size}.bin", size * MIB)

        def measure(label, mode, resource, size, chunk, soft=None, hard=None, idle=0, combined=False):
            command = [str(args.binary), "--source", str(source), "--destination", str(destination),
                       "--path", f"{size}.bin", "--chunk-bytes", str(chunk), "--idle-ms", str(idle)]
            if mode == "copy":
                command.append("--copy")
            if soft is not None:
                key = "bytes" if resource == "bandwidth" else "iops"
                command.extend([f"--soft-{key}", str(soft), f"--hard-{key}", str(hard)])
            if combined:
                command.extend(["--soft-iops", "128", "--hard-iops", "256"])
            print(f"RUN {label} {mode} {resource}", flush=True)
            completed = subprocess.run(command, capture_output=True, text=True, timeout=120)
            (args.output / f"{label}-{mode}-{resource}.stderr").write_text(completed.stderr)
            completed.check_returncode()
            result = json.loads(completed.stdout)
            result.update(label=label, mode=mode, resource=resource, command=command)
            (args.output / f"{label}-{mode}-{resource}.json").write_text(json.dumps(result) + "\n")
            if soft is not None:
                if len(result["admissions"]) != result["operations"]:
                    raise ValueError("admission trace must cover every charged source operation")
                if sum(e["bytes"] for e in result["admissions"]) != result["bytes"]:
                    raise ValueError("admission trace byte count differs from verified payload")
            rows.append({k: v for k, v in result.items() if k not in ("events", "admissions", "command", "digest")})
            print(f"  {result['mibps']:.3f} MiB/s, {result['iops']:.3f} IOPS, {result['seconds']:.3f}s", flush=True)
            # Fresh destination on every run; no resumed/skipped copies in the benchmark.
            for path in destination.iterdir():
                if path.is_file():
                    path.unlink()
            return result

        for resource, chunk, soft, hard, size in [
            ("bandwidth", 2 * MIB, 16 * MIB, 32 * MIB, 128),
            ("iops", 64 * 1024, 128, 256, 64),
        ]:
            metric = "mibps" if resource == "bandwidth" else "iops"
            factor = MIB if resource == "bandwidth" else 1
            for mode in ("read", "copy"):
                base, high = [], []
                for repeat in range(3):
                    # Alternate order to reduce systematic ordering bias.
                    order = (False, True) if repeat % 2 == 0 else (True, False)
                    for limited in order:
                        rate = (1024 ** 4 if resource == "bandwidth" else 1_000_000_000) if limited else None
                        result = measure(f"{'high' if limited else 'baseline'}-{repeat}", mode, resource,
                                         256, chunk, rate, rate)
                        (high if limited else base).append(result[metric])
                baseline = statistics.median(base)
                ratio = statistics.median(high) / baseline
                gate(f"{resource}/{mode}/baseline_has_headroom", baseline * factor >= hard * 1.25,
                     baseline=baseline, hard=hard / factor)
                gate(f"{resource}/{mode}/high_qos_matches_baseline", ratio >= MINIMUM_RATIO,
                     ratio=ratio, baseline=baseline, high=statistics.median(high))
                for burst in (False, True):
                    result = measure("dual" if burst else "strict", mode, resource,
                                     size, chunk, soft, hard if burst else soft, 1000 if burst else 0)
                    actual = result[metric] * factor
                    gate(f"{resource}/{mode}/{'dual' if burst else 'strict'}/average_98_percent",
                         actual >= soft * MINIMUM_RATIO, actual=actual / factor, soft=soft / factor)
                    gate(f"{resource}/{mode}/{'dual' if burst else 'strict'}/hard_total",
                         actual <= (hard if burst else soft), actual=actual / factor)
                    trace = validate_trace(result, resource, soft, hard if burst else soft, 0.5, burst)
                    trace_checks = ["hard_envelope", "soft_envelope", "steady_ceiling",
                                    "steady_at_least_98_percent"]
                    if burst:
                        trace_checks.append("burst_observed")
                    for key in trace_checks:
                        gate(f"{resource}/{mode}/{'dual' if burst else 'strict'}/{key}", trace[key],
                             **{k: v for k, v in trace.items() if not isinstance(v, bool)})

        for mode in ("read", "copy"):
            result = measure("combined", mode, "bandwidth", 64, 64 * 1024,
                             8 * MIB, 16 * MIB, 1000, combined=True)
            gate(f"combined/{mode}/average_98_percent",
                 result["mibps"] >= 8 * MINIMUM_RATIO and result["iops"] >= 128 * MINIMUM_RATIO,
                 mibps=result["mibps"], iops=result["iops"])
            for resource, soft, hard in (("bandwidth", 8 * MIB, 16 * MIB), ("iops", 128, 256)):
                trace = validate_trace(result, resource, soft, hard, 0.5, True)
                for key in ("hard_envelope", "soft_envelope", "steady_ceiling",
                            "steady_at_least_98_percent", "burst_observed"):
                    gate(f"combined/{mode}/{resource}/{key}", trace[key])

    with (args.output / "measurements.csv").open("w") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    report = {"passed": all(c["passed"] for c in checks), "checks": checks}
    (args.output / "checks.json").write_text(json.dumps(report, indent=2) + "\n")
    print(f"Results: {args.output}; failed {sum(not c['passed'] for c in checks)}/{len(checks)}", flush=True)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
