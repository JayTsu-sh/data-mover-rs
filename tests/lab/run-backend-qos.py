#!/usr/bin/env python3
"""Real backend source -> Local QoS acceptance, using a private endpoint config."""
import argparse
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import time
import uuid
from qos_evidence import (compare_performance, concurrency_environment, redact as redact_secrets, sample_destination,
                          secret_values, strict_timing, verify_baseline_identity, verify_isolated_destination)

SPEC = importlib.util.spec_from_file_location("qos_validator", Path(__file__).with_name("run-local-qos.py"))
VALIDATOR = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VALIDATOR)
MIB = 1024 * 1024


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--phase", choices=("survey", "final", "stress"), required=True)
    parser.add_argument("--profiles", nargs="*")
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--performance-idle-ms", type=int, default=0, help="equal unmeasured idle before baseline/high samples")
    parser.add_argument("--performance-size-mib", type=int, help="longer baseline/high samples; limited cases keep their original size")
    parser.add_argument("--limit-duration-seconds", type=float, default=16,
                        help="minimum ideal strict-run duration; use a larger profile.size_mib for sustained qualification")
    parser.add_argument("--performance-control", action="store_true", help="measure an identical no-QoS A/A control; keep the raw high-QoS verdict")
    parser.add_argument("--isolated-destinations", action="store_true", help="fresh destination per copy; defer cleanup until all profiles finish")
    parser.add_argument("--cleanup-binary", type=Path, help="backend_qos_cleanup executable for isolated destination trees")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--baseline", type=Path, help="reuse a survey from this exact binary")
    parser.add_argument("--reuse-fixtures", action="store_true", help="use independently preseeded source fixtures")
    parser.add_argument("--retain-source", action="store_true", help="matrix lifecycle owns shared source cleanup")
    parser.add_argument("--measure-only", action="store_true", help="seed and measure small-read baselines before qualification")
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    if not math.isfinite(args.limit_duration_seconds) or args.limit_duration_seconds <= 0:
        parser.error("finite workload duration must be positive and finite")
    if args.repeats < 1 or (args.performance_control and args.repeats < 3):
        parser.error("positive repeats required; A/A controls require at least three repeats")
    if args.phase == "stress" and (args.performance_control or args.isolated_destinations):
        parser.error("A/A controls and isolated destinations require a copy phase")
    if args.isolated_destinations and (not args.cleanup_binary or not args.cleanup_binary.is_file()
                                        or not config.get("run_id")):
        parser.error("isolated destinations require --cleanup-binary and config.run_id")
    if args.isolated_destinations:
        for profile in config["profiles"]:
            if args.profiles and profile["name"] not in args.profiles:
                continue
            target = profile["env"].get("QOS_DESTINATION", str(args.root / "destination" / profile["name"]))
            verify_isolated_destination(target, config["run_id"])
    configuration_sha256 = hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()
    output = args.output or args.root / args.phase
    output.mkdir(parents=True, exist_ok=False)
    if args.baseline:
        if args.phase not in ("final", "stress"):
            raise ValueError("only limit qualification may reuse a survey")
        measured = json.loads((args.baseline / "environment.json").read_text())
        verify_baseline_identity(measured, configuration_sha256,
                                 hashlib.sha256(args.binary.read_bytes()).hexdigest(),
                                 args.repeats, args.performance_idle_ms,
                                 performance_control=args.performance_control,
                                 isolated_destinations=args.isolated_destinations,
                                 runtime_concurrency=concurrency_environment(os.environ),
                                 limit_duration_seconds=args.limit_duration_seconds)
    fixtures = args.root / "fixtures"
    fixtures.mkdir(exist_ok=True)
    fixture_sizes = {64, 128, *([args.performance_size_mib] if args.performance_size_mib else [])}
    for profile in config["profiles"]:
        if not args.profiles or profile["name"] in args.profiles:
            fixture_sizes.add(profile.get("size_mib", 64))
            fixture_sizes.add(profile.get("performance_size_mib", args.performance_size_mib or profile.get("size_mib", 64)))
    for size in sorted(fixture_sizes):
        path = fixtures / f"{size}.bin"
        if not path.exists():
            VALIDATOR.fixture(path, size * MIB)
    checks, summary = [], {}
    secrets = secret_values([os.environ, *(p["env"] for p in config["profiles"])])

    def redact(value):
        return redact_secrets(value, secrets)

    def gate(profile, case, name, passed, **details):
        row = dict(profile=profile, case=case, name=name, passed=bool(passed), **details)
        checks.append(row)
        print(f"{'PASS' if passed else 'FAIL'} {profile}/{case}/{name} {details}", flush=True)
        (output / "checks.json").write_text(json.dumps(checks, indent=2) + "\n")

    (output / "environment.json").write_text(json.dumps({
        "platform": platform.platform(), "runtime_workers": 4, "minimum_ratio": 0.98,
        "configuration_sha256": configuration_sha256,
        "phase": args.phase, "baseline_origin": str(args.baseline) if args.baseline else None,
        "repetitions": args.repeats, "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
        "performance_size_mib": args.performance_size_mib, "performance_idle_ms": args.performance_idle_ms, "high_admission_trace": False,
        "performance_control": args.performance_control, "isolated_destinations": args.isolated_destinations,
        "runtime_concurrency": concurrency_environment(os.environ),
        "limit_duration_seconds": args.limit_duration_seconds,
        "source_scope": "real source -> configured destination; per-profile transfer mode recorded",
        "hard_contract": "all admission intervals: hard * time + one bounded actual request",
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }, indent=2) + "\n")
    sample_session = uuid.uuid4().hex
    pending_cleanup = []
    try:
        for profile in config["profiles"]:
            name = profile["name"]
            source_kind = profile.get("source_kind", name)
            if args.profiles and name not in args.profiles:
                continue
            folder = output / name
            folder.mkdir()
            destination = args.root / "destination" / name
            destination.mkdir(parents=True, exist_ok=True)
            size = 128 if args.phase == "stress" else profile.get("size_mib", 64)
            performance_size = profile.get("performance_size_mib", args.performance_size_mib or size)
            chunk = 64 * 1024 if args.phase == "stress" else profile.get("chunk_bytes", 2 * MIB)
            env = {**os.environ, **profile["env"], "no_proxy": "*", "NO_PROXY": "*"}
            command = [str(args.binary), "--source", str(fixtures), "--destination", str(destination),
                       "--path", f"{size}.bin", "--chunk-bytes", str(chunk), "--copy",
                       "--transfer-profile", profile.get("transfer_profile", "default")]
            remote_destination = "QOS_DESTINATION" in profile["env"]
            if remote_destination:
                command += ["--destination-cleanup", "delete"]

            def run(case, extra=(), chunk_override=None):
                invocation = command.copy()
                invocation_env = env
                if args.phase == "stress":
                    invocation.remove("--copy")
                if chunk_override:
                    invocation[invocation.index("--chunk-bytes") + 1] = str(chunk_override)
                isolated = args.isolated_destinations and "--copy" in invocation and not any(
                    option in extra for option in ("--seed", "--cleanup"))
                if isolated:
                    sample_id = f"{sample_session}-{name}-{case}"
                    target = sample_destination(profile["env"].get("QOS_DESTINATION", str(destination)), sample_id)
                    invocation_env = {**env, "QOS_DESTINATION": target}
                    if "--destination-cleanup" in invocation:
                        invocation[invocation.index("--destination-cleanup") + 1] = "keep"
                    pending_cleanup.append((name, case, {**env, "QOS_SOURCE": target}))
                print(f"RUN {name}/{case}", flush=True)
                completed = subprocess.run(invocation + list(extra), env=invocation_env, capture_output=True,
                                           text=True, timeout=max(180, 3 * args.limit_duration_seconds))
                (folder / f"{case}.stderr").write_text(redact(completed.stderr))
                if completed.returncode:
                    raise RuntimeError(redact(completed.stderr[-4000:]))
                result = json.loads(completed.stdout)
                if "bytes" in result:
                    result["command"] = invocation + list(extra)
                    if isolated:
                        result["destination_sample"] = sample_id
                        result["destination_cleanup"] = "deferred"
                    (folder / f"{case}.json").write_text(json.dumps(result) + "\n")
                    if result["admissions"]:
                        assert len(result["admissions"]) == result["operations"]
                        assert sum(e["bytes"] for e in result["admissions"]) == result["bytes"]
                    print(f"  {result['mibps']:.4f} MiB/s {result['iops']:.4f} IOPS {result['seconds']:.4f}s", flush=True)
                    # Each copy must publish fresh data, never resume a prior timed attempt.
                    if args.phase != "stress" and not remote_destination and not isolated:
                        (destination / invocation[invocation.index("--path") + 1]).unlink()
                return result

            try:
                if not args.baseline and not args.reuse_fixtures:
                    if profile.get("remove_old_seed", False):
                        try:
                            run("remove-old-seed", ["--cleanup"])
                        except RuntimeError as exc:
                            if "NotFound" not in str(exc):
                                raise
                    run("seed", ["--seed"])
                if not args.baseline:
                    command[command.index("--path") + 1] = f"{performance_size}.bin"
                    if performance_size != size and not args.reuse_fixtures:
                        run("seed-performance", ["--seed"])
                base, high, control = [], [], []
                for repeat in range(args.repeats):
                    order = ["baseline", "high"] if repeat % 2 == 0 else ["high", "baseline"]
                    if args.performance_control:
                        arms = ["baseline", "control", "high"]
                        position = repeat % len(arms)
                        order = arms[position:] + arms[:position]
                    for arm in order:
                        limited = arm == "high"
                        extra = ["--soft-bytes", str(1024 ** 4), "--hard-bytes", str(1024 ** 4),
                                 "--soft-iops", "1000000000", "--hard-iops", "1000000000",
                                 "--admission-trace", "disabled"] if limited else []
                        extra += ["--idle-ms", str(args.performance_idle_ms)]
                        case = f"{arm}-{repeat}"
                        if args.baseline:
                            path = args.baseline / name / f"{case}.json"
                            result = json.loads(path.read_text())
                            shutil.copy2(path, folder / path.name)
                        else:
                            result = run(case, extra)
                        {"baseline": base, "high": high, "control": control}[arm].append(result)
                if not args.baseline and performance_size != size and not args.retain_source:
                    run("cleanup-performance", ["--cleanup"])
                command[command.index("--path") + 1] = f"{size}.bin"
                comparison = compare_performance([r["mibps"] for r in base], [r["mibps"] for r in high],
                                                 [r["mibps"] for r in control] if args.performance_control else None)
                baseline, high_rate = comparison["baseline_mibps"], comparison["high_mibps"]
                cap = high[0]["effective_read_bytes"]
                assert all(r["effective_read_bytes"] == cap and r["bytes"] == base[0]["bytes"] for r in base + high + control)
                assert all(r["operations"] == base[0]["operations"] for r in high)
                gate(name, "high", "matches_baseline_98_percent", high_rate >= baseline * 0.98,
                     baseline_mibps=baseline, high_mibps=high_rate, ratio=high_rate / baseline)
                if args.performance_control:
                    gate(name, "high", "baseline_repeatable_98_percent", comparison["control_repeatable"],
                         control_ratio=comparison["control_ratio"], interval_95=comparison["control_ratio_interval_95"],
                         classification=comparison["classification"])
                    gate(name, "high", "high_ratio_precision_98_percent", comparison["high_ratio_interval_95"][0] >= .98,
                         interval_95=comparison["high_ratio_interval_95"])
                # Rates chosen against baseline headroom, then held fixed throughout
                # strict/dual tests. Large-request IOPS means real protocol requests.
                soft_bytes = max(1, int(min(profile.get("soft_mibps", 4) * MIB, baseline * MIB / 8)))
                soft_iops = 128 if args.phase == "stress" else max(1, math.floor(
                    (size * MIB // cap) / args.limit_duration_seconds))
                gate(name, "capacity", "bandwidth_headroom", baseline * MIB >= soft_bytes * 2 * 1.25)
                baseline_iops = statistics.median(r["iops"] for r in base)
                gate(name, "capacity", "iops_headroom", baseline_iops >= soft_iops * 2 * 1.25)
                summary[name] = dict(baseline_mibps=baseline, baseline_iops=baseline_iops,
                                     high_mibps=high_rate, high_ratio=high_rate / baseline,
                                     request_cap_bytes=cap, soft_bytes=soft_bytes, soft_iops=soft_iops,
                                     source_kind=source_kind, destination_kind=profile.get("destination_kind", "local"),
                                     transfer_profile=profile.get("transfer_profile", "default"),
                                     size_mib=size, performance_size_mib=base[0]["bytes"] // MIB, cases={})
                if args.performance_control:
                    summary[name]["performance_comparison"] = comparison
                if args.phase == "survey" or args.measure_only:
                    continue
                cases = [
                    ("bandwidth-strict", "bandwidth", soft_bytes, soft_bytes),
                    ("bandwidth-dual", "bandwidth", soft_bytes, soft_bytes * 2),
                    ("iops-strict", "iops", soft_iops, soft_iops),
                    ("iops-dual", "iops", soft_iops, soft_iops * 2),
                ]
                for case, resource, soft, hard in cases:
                    if args.phase == "stress" and resource != "iops":
                        continue
                    peak = max(0.5, 2 * cap / soft) if resource == "bandwidth" else 2.0
                    dual = hard > soft
                    key = "bytes" if resource == "bandwidth" else "iops"
                    extra = [f"--soft-{key}", str(soft), f"--hard-{key}", str(hard),
                             "--peak-ms", str(math.ceil(peak * 1000)),
                             "--idle-ms", str(math.ceil(peak * 1000 + 100)) if dual else "0"]
                    result = run(case, extra)
                    if source_kind == "s3" and resource == "bandwidth":
                        expected_requests = (result["bytes"] + cap - 1) // cap
                        gate(name, case, "s3_payload_requests_no_inflation",
                             result["operations"] == expected_requests,
                             actual=result["operations"], expected=expected_requests,
                             request_cap_bytes=cap)
                    actual = result["bytes"] / result["seconds"] if key == "bytes" else result["iops"]
                    trace = VALIDATOR.validate_trace(result, resource, soft, hard, peak, dual)
                    gate(name, case, "end_to_end_at_least_98_percent", actual >= soft * 0.98,
                         actual=actual, soft=soft, ratio=actual / soft)
                    for check in ("hard_envelope", "soft_envelope", "steady_ceiling", "steady_at_least_98_percent"):
                        gate(name, case, check, trace[check], steady_rate=trace["steady_rate"])
                    if dual:
                        gate(name, case, "steady_window_after_peak", trace["steady_window_after_peak"])
                        gate(name, case, "burst_observed", trace["burst_observed"])
                    summary[name]["cases"][case] = dict(actual=actual, ratio=actual / soft, trace=trace)
                    if not dual:
                        summary[name]["cases"][case]["timing"] = strict_timing(result, resource, soft)
                        if args.phase != "stress":
                            ideal = summary[name]["cases"][case]["timing"]["ideal_seconds"]
                            gate(name, case, "minimum_ideal_duration", ideal >= args.limit_duration_seconds,
                                 ideal_seconds=ideal, minimum_seconds=args.limit_duration_seconds)
                if args.phase == "stress":
                    continue
                # Joint limits must describe compatible resources. File backends
                # split into 10-ms hard quanta; S3 preserves its bounded GET size.
                joint_soft = soft_bytes if source_kind != "s3" else cap
                grant = cap if source_kind == "s3" else min(cap, math.ceil(joint_soft * 2 / 100))
                joint_iops = max(1, round(joint_soft / grant))
                extra = ["--soft-bytes", str(joint_soft), "--hard-bytes", str(joint_soft * 2),
                         "--soft-iops", str(joint_iops), "--hard-iops", str(joint_iops * 2),
                         "--peak-ms", "2000", "--idle-ms", "2100"]
                result = run("combined", extra)
                if source_kind == "s3":
                    expected_requests = (result["bytes"] + cap - 1) // cap
                    gate(name, "combined", "s3_payload_requests_no_inflation",
                         result["operations"] == expected_requests,
                         actual=result["operations"], expected=expected_requests,
                         request_cap_bytes=cap)
                summary[name]["cases"]["combined"] = {}
                for resource, soft in (("bandwidth", joint_soft), ("iops", joint_iops)):
                    trace = VALIDATOR.validate_trace(result, resource, soft, soft * 2, 2, True)
                    actual = result["bytes"] / result["seconds"] if resource == "bandwidth" else result["iops"]
                    gate(name, "combined", f"{resource}_end_to_end_98_percent", actual >= soft * 0.98,
                         ratio=actual / soft)
                    for check in ("hard_envelope", "soft_envelope", "steady_ceiling", "steady_window_after_peak", "steady_at_least_98_percent", "burst_observed"):
                        gate(name, "combined", f"{resource}_{check}", trace[check])
                    summary[name]["cases"]["combined"][resource] = dict(ratio=actual / soft, trace=trace)
            except Exception as exc:
                gate(name, "execution", "completed", False, error=redact(str(exc)))
            finally:
                (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
                if args.phase in ("final", "stress") and not args.measure_only and not args.retain_source:
                    try:
                        run("cleanup", ["--cleanup"])
                        gate(name, "cleanup", "source_file_removed", True)
                    except Exception as exc:
                        gate(name, "cleanup", "source_file_removed", False, error=redact(str(exc)))
    finally:
        for name, case, cleanup_env in pending_cleanup:
            try:
                completed = subprocess.run([str(args.cleanup_binary), "--run-id", config["run_id"], "--tree"],
                                           env=cleanup_env, capture_output=True, text=True, timeout=180)
                (output / name / f"{case}.cleanup.stderr").write_text(redact(completed.stderr))
                gate(name, case, "isolated_destination_removed", completed.returncode == 0,
                     **({"error": redact(completed.stderr[-4000:])} if completed.returncode else {}))
            except Exception as exc:
                gate(name, case, "isolated_destination_removed", False, error=redact(str(exc)))
    print(f"FINISHED {sum(c['passed'] for c in checks)}/{len(checks)} checks passed", flush=True)
    return 0 if all(c["passed"] for c in checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
