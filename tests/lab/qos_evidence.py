"""Protect credentials and the provenance of QoS lab artifacts."""
import math
from pathlib import Path
import re
import random
import statistics
from urllib.parse import unquote, urlsplit, urlunsplit


def secret_values(environments):
    secrets = set()
    for environment in environments:
        for key, value in environment.items():
            if any(part in key.upper() for part in
                   ("PASS", "SECRET", "_AK", "_SK", "TOKEN", "ACCESS_KEY", "API_KEY")):
                secrets.add(value)
            try:
                parsed = urlsplit(value)
                if parsed.password:
                    secrets.update((parsed.password, unquote(parsed.password)))
                    if parsed.username:
                        secrets.update((parsed.username, unquote(parsed.username)))
            except ValueError:
                pass
    return sorted((value for value in secrets if value), key=len, reverse=True)


def redact(value, secrets):
    for secret in secrets:
        value = value.replace(secret, "<REDACTED>")
    return value


def verify_baseline_identity(measured, configuration_sha256, binary_sha256, repetitions, idle_ms,
                             *, performance_control=False, isolated_destinations=False,
                             runtime_concurrency=None, limit_duration_seconds=16):
    if measured.get("configuration_sha256") != configuration_sha256:
        raise ValueError("baseline and source/destination configuration differ")
    if measured["binary_sha256"] != binary_sha256:
        raise ValueError("baseline and candidate binary differ")
    if measured["repetitions"] != repetitions:
        raise ValueError("baseline and candidate repetition counts differ")
    if measured.get("performance_idle_ms", 0) != idle_ms:
        raise ValueError("baseline and candidate unmeasured idle differ")
    if measured.get("performance_control", False) != performance_control:
        raise ValueError("baseline and candidate A/A controls differ")
    if measured.get("isolated_destinations", False) != isolated_destinations:
        raise ValueError("baseline and candidate destination lifecycles differ")
    if measured.get("runtime_concurrency", {}) != (runtime_concurrency or {}):
        raise ValueError("baseline and candidate inherited concurrency settings differ")
    if measured.get("limit_duration_seconds", 16) != limit_duration_seconds:
        raise ValueError("baseline and candidate finite workload duration differ")


def concurrency_environment(environment):
    names = ["DATA_MOVER_INFLIGHT", "DATA_MOVER_READ_INFLIGHT", "DATA_MOVER_WRITE_INFLIGHT"]
    names += [f"DATA_MOVER_{backend}_{direction}_INFLIGHT"
              for backend in ("LOCAL", "NFS", "CIFS", "S3", "HDFS")
              for direction in ("READ", "WRITE")]
    return {name: environment[name] for name in names if name in environment}


def sample_destination(endpoint, sample_id):
    """Append an owned sample prefix without changing URL credentials or options."""
    if not re.fullmatch(r"[A-Za-z0-9_-]+", sample_id):
        raise ValueError("a safe, single-component sample identifier is required")
    suffix = "qos-samples/" + sample_id
    if "://" in endpoint or endpoint.startswith(("smb:", "s3:", "s3@")):
        parsed = urlsplit(endpoint)
        return urlunsplit(parsed._replace(path=parsed.path.rstrip("/") + "/" + suffix))
    return str(Path(endpoint) / suffix)


def verify_isolated_destination(endpoint, run_id):
    """Reject an uncleanable destination before a timed copy can create data."""
    if (len(run_id) > 88 or ".." in run_id or
            not re.fullmatch(r"(?:nightly|release)-[A-Za-z0-9_.-]+", run_id)):
        raise ValueError("a valid isolated lab run identifier is required")
    remote = "://" in endpoint or endpoint.startswith(("smb:", "s3:", "s3@"))
    if "://" in endpoint:
        if endpoint.split("://", 1)[0] not in ("nfs", "smb", "hdfs", "s3", "s3+https"):
            raise ValueError("unsupported isolated destination scheme")
        # S3 credentials may contain slashes; its parser separates at the last @.
        authority_path = endpoint.split("://", 1)[1].rsplit("@", 1)[-1]
        authority_path = authority_path.split("?", 1)[0].split("#", 1)[0]
        path = authority_path[authority_path.find("/"):] if "/" in authority_path else ""
        path = unquote(path.split("?", 1)[0].split("#", 1)[0])
    elif remote:
        path = unquote(endpoint.split(":", 1)[1])
    else:
        path = endpoint
    parts = re.split(r"[/\\]", path)
    if run_id not in parts or any(part in (".", "..") or "\0" in part for part in parts):
        raise ValueError("destination must contain the exact isolated run path component")
    if not remote:
        absolute = Path(path).absolute()
        index = absolute.parts.index(run_id)
        expected = Path(*absolute.parts[:index]).resolve().joinpath(*absolute.parts[index:])
        if absolute.resolve() != expected:
            raise ValueError("destination cannot follow a symlink at or below the isolated run root")


def select_profiles(profiles, requested):
    """Fail before fixture creation if the requested workload is empty or ambiguous."""
    names = [profile["name"] for profile in profiles]
    if len(names) != len(set(names)):
        raise ValueError("profile names must be unique")
    if requested is not None and (not requested or len(requested) != len(set(requested))):
        raise ValueError("requested profiles must be nonempty and unique")
    if requested is not None and set(requested) - set(names):
        raise ValueError("unknown requested profile")
    selected = [profile for profile in profiles if requested is None or profile["name"] in requested]
    if not selected:
        raise ValueError("at least one profile is required")
    if any(not re.fullmatch(r"[A-Za-z0-9_.-]+", name) or name in (".", "..") for name in names):
        raise ValueError("profile names must be safe path components")
    return selected


def strict_timing(result, resource, rate):
    """Decompose a strict run; diagnostics never replace its end-to-end verdict.

    The last admission precedes the last read, upload and publication. Even
    perfect pacing needs an ideal duration of 49 times that tail to reach 98%.
    This is an observed timing bound, not a prediction of future storage latency.
    It is inapplicable to dual-rate runs with accumulated soft credit.
    """
    events = result["admissions"]
    seconds = result["seconds"]
    if (resource not in ("bandwidth", "iops") or not math.isfinite(rate) or rate <= 0
            or not math.isfinite(seconds) or seconds <= 0 or not events
            or len(events) != result["operations"]
            or sum(event["bytes"] for event in events) != result["bytes"]):
        raise ValueError("strict timing requires a complete finite admission trace and positive rate")
    previous = 0.
    for event in events:
        at = event["seconds"]
        if not math.isfinite(at) or not previous <= at <= seconds or event["bytes"] <= 0:
            raise ValueError("admission trace must be ordered and contained in the measurement")
        previous = at
    units = result["bytes"] if resource == "bandwidth" else result["operations"]
    first_units = events[0]["bytes"] if resource == "bandwidth" else 1
    ideal = units / rate
    span = events[-1]["seconds"] - events[0]["seconds"]
    tail = seconds - events[-1]["seconds"]
    extra = ideal / .98 - ideal
    return dict(ideal_seconds=ideal, end_to_end_ratio=ideal / seconds,
                end_to_end_passed=ideal >= seconds * .98,
                first_admission_seconds=events[0]["seconds"],
                last_admission_seconds=events[-1]["seconds"],
                active_admission_ratio=(units - first_units) / (span * rate) if span else None,
                active_admission_drift_seconds=span - (units - first_units) / rate,
                post_admission_seconds=tail, extra_seconds_allowed=extra,
                tail_exceeds_entire_extra_budget=tail > extra,
                ideal_seconds_needed_for_observed_tail=tail * 49)


def compare_performance(baseline, high, control=None):
    """Keep the raw 98% verdict; a noisy identical baseline cannot qualify it."""
    series = [baseline, high] + ([] if control is None else [control])
    if not baseline or any(len(values) != len(baseline) for values in series):
        raise ValueError("performance arms must have equal nonempty sample counts")
    if any(not math.isfinite(value) or value <= 0 for values in series for value in values):
        raise ValueError("performance samples must be positive finite rates")
    baseline_rate, high_rate = statistics.median(baseline), statistics.median(high)
    ratio = high_rate / baseline_rate
    passed = high_rate >= baseline_rate * .98
    result = dict(baseline_mibps=baseline_rate, high_mibps=high_rate, ratio=ratio, passed=passed)
    if control is not None:
        control_rate = statistics.median(control)
        control_ratio = control_rate / baseline_rate
        # Resample whole blocks: A, control-A and high share the same local load
        # history. A matching median alone can hide very noisy identical arms.
        rng = random.Random(20261030)
        control_ratios = []
        high_ratios = []
        for _ in range(4000):
            indices = rng.choices(range(len(baseline)), k=len(baseline))
            base = statistics.median(baseline[i] for i in indices)
            control_ratios.append(statistics.median(control[i] for i in indices) / base)
            high_ratios.append(statistics.median(high[i] for i in indices) / base)
        control_ratios.sort()
        high_ratios.sort()
        control_interval = [control_ratios[99], control_ratios[3900]]
        high_interval = [high_ratios[99], high_ratios[3900]]
        repeatable = control_interval[0] >= .98 and control_interval[1] <= 1 / .98
        result.update(control_mibps=control_rate, control_ratio=control_ratio,
                      control_ratio_interval_95=control_interval, high_ratio_interval_95=high_interval,
                      interval_method="paired-block percentile bootstrap, 4000 draws, seed 20261030; diagnostic precision guard",
                      control_repeatable=repeatable,
                      qualified=passed and repeatable and high_interval[0] >= .98,
                      classification=("environment_not_repeatable" if not repeatable else
                                      "insufficient_precision" if high_interval[0] < .98 <= high_interval[1] else
                                      "passed" if passed else "below_98_percent"))
    return result
