#!/usr/bin/env python3
"""Run all four QoS scenarios on seven regressions and twenty directed cross pairs."""
import argparse
import concurrent.futures
import importlib.util
import json
import math
import os
from pathlib import Path
import subprocess
import time

from cross_qos_matrix import profiles, validate_override
from qos_evidence import redact as redact_secrets, secret_values

SPEC = importlib.util.spec_from_file_location("qos_validator", Path(__file__).with_name("run-local-qos.py"))
VALIDATOR = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VALIDATOR)
MIB = 1024 * 1024


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--stage", choices=("warmup", "seed", "survey", "limits", "all"), default="all")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--limit-duration-seconds", type=float, default=16)
    parser.add_argument("--limit-size-mib", type=int, default=128)
    parser.add_argument("--performance-control", action="store_true")
    parser.add_argument("--isolated-destinations", action="store_true")
    parser.add_argument("--cleanup-binary", type=Path)
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    matrix = profiles(config)
    if (args.limit_size_mib < 1 or not math.isfinite(args.limit_duration_seconds)
            or args.limit_duration_seconds <= 0):
        parser.error("finite workload size and duration must be positive and finite")
    for row in matrix:
        row["size_mib"] = args.limit_size_mib
    args.root.mkdir(parents=True, exist_ok=True)
    fixtures = args.root / "fixtures"
    fixtures.mkdir(exist_ok=True)
    (args.root / "warm-destination").mkdir(exist_ok=True)
    secrets = secret_values([os.environ, *(endpoint.get("env", {}) for endpoint in
                            [*config["sources"].values(), *config["destinations"].values()])])

    def redact(value):
        return redact_secrets(value, secrets)

    private = args.root / "matrix.private.json"
    private.write_text(json.dumps({"run_id": config["run_id"], "profiles": matrix}))
    private.chmod(0o600)
    checks = []

    def gate(name, passed, **details):
        checks.append(dict(name=name, passed=bool(passed), **details))
        (args.root / f"{args.stage}-checks.json").write_text(json.dumps(checks, indent=2) + "\n")
        print(f"{'PASS' if passed else 'FAIL'} {name} {details}", flush=True)

    def fixture(size):
        path = fixtures / f"{size}.bin"
        if not path.exists():
            VALIDATOR.fixture(path, size * MIB)

    def invoke(label, env, path, extra=()):
        invocation = [str(args.binary), "--source", str(fixtures), "--destination", str(args.root / "warm-destination"),
                      "--path", path, "--chunk-bytes", "2097152", *extra]
        result = subprocess.run(invocation, env={**os.environ, **env, "NO_PROXY": "*", "no_proxy": "*"},
                                capture_output=True, text=True, timeout=180)
        folder = args.root / "preparation"
        folder.mkdir(exist_ok=True)
        (folder / f"{label}.stderr").write_text(redact(result.stderr))
        if result.returncode:
            raise RuntimeError(redact(result.stderr[-4000:]))
        data = json.loads(result.stdout)
        data["command"] = invocation
        (folder / f"{label}.json").write_text(json.dumps(data) + "\n")
        return data

    if args.stage in ("warmup", "all"):
        fixture(16)
        for kind, source in config["sources"].items():
            invoke(f"warm-seed-{kind}", source["env"], "16.bin", ["--seed"])
        try:
            for row in matrix:
                try:
                    data = invoke(f"warm-{row['name']}", row["env"], "16.bin", ["--copy", "--transfer-profile", row["transfer_profile"], "--destination-cleanup", "delete"])
                    gate(row["name"], data["bytes"] == 16 * MIB, digest=data["digest"])
                except Exception as exc:
                    gate(row["name"], False, error=str(exc))
        finally:
            for kind, source in config["sources"].items():
                invoke(f"warm-clean-{kind}", source["env"], "16.bin", ["--cleanup"])
        if not all(c["passed"] for c in checks):
            return 1
    if args.stage in ("seed", "all"):
        for kind, source in config["sources"].items():
            sizes = {args.limit_size_mib} | {row["performance_size_mib"] for row in matrix if row["source_kind"] == kind}
            for size in sorted(sizes):
                fixture(size)
                try:
                    invoke(f"seed-{kind}-{size}", source["env"], f"{size}.bin", ["--seed"])
                    gate(f"seed-{kind}-{size}", True)
                except Exception as exc:
                    gate(f"seed-{kind}-{size}", False, error=str(exc))
        if not all(c["passed"] for c in checks):
            return 1

    base = ["python3", str(Path(__file__).with_name("run-backend-qos.py")), "--config", str(private),
            "--root", str(args.root), "--binary", str(args.binary), "--repeats", str(args.repeats),
            "--limit-duration-seconds", str(args.limit_duration_seconds),
            "--reuse-fixtures", "--retain-source"]
    if args.performance_control:
        base += ["--performance-control"]
    if args.isolated_destinations:
        base += ["--isolated-destinations"]
    if args.cleanup_binary:
        base += ["--cleanup-binary", str(args.cleanup_binary)]
    if args.stage in ("survey", "all"):
        with (args.root / "survey.log").open("w") as log:
            result = subprocess.run(base + ["--phase", "survey", "--output", str(args.root / "survey")], stdout=log, stderr=subprocess.STDOUT)
        gate("serial-performance-survey", result.returncode == 0)
    if args.stage in ("limits", "all"):
        if ready_file := config.get("baseline_ready_file"):
            while not Path(ready_file).exists():
                time.sleep(1)
        overrides_path = args.root / "baseline-overrides.private.json"
        overrides = json.loads(overrides_path.read_text()) if overrides_path.exists() else {}
        def qualify(row):
            name = row["name"]
            case_base = base.copy()
            baseline = args.root / "survey"
            if name in overrides:
                replacement = json.loads(Path(overrides[name]["config"]).read_text())
                revised = next(item for item in replacement["profiles"] if item["name"] == name)
                validate_override(row, revised)
                case_base[case_base.index("--config") + 1] = overrides[name]["config"]
                baseline = Path(overrides[name]["baseline"])
                repetitions = overrides[name].get("repetitions", args.repeats)
                case_base[case_base.index("--repeats") + 1] = str(repetitions)
                case_base += ["--performance-idle-ms", str(overrides[name].get("performance_idle_ms", 0))]

            with (args.root / f"limits-{name}.log").open("w") as log:
                result = subprocess.run(case_base + ["--phase", "final", "--profiles", name,
                                               "--baseline", str(baseline),
                                               "--output", str(args.root / "limits" / name)], stdout=log, stderr=subprocess.STDOUT)
            return name, result.returncode
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
            pending = [pool.submit(qualify, row) for row in matrix]
            for job in concurrent.futures.as_completed(pending):
                name, code = job.result()
                gate(name, code == 0)
    return 0 if all(c["passed"] for c in checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
