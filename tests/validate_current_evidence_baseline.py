#!/usr/bin/env python3
"""Validate the historical real-environment evidence ledger."""

import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import yaml


PROFILES = {"local", "nfs3", "nfs40", "nfs41", "cifs_fas2750", "s3_standard", "s3_dxn", "hdfs"}
STATUSES = {"passed", "failed", "missing"}
COMMON_FIELDS = {
    "evidence_kind", "repository", "exact_commit", "source_profile",
    "destination_profile", "mode", "fixture_set", "outcome", "artifact_links",
    "environment_fingerprint",
}
GATE_FIELDS = COMMON_FIELDS | {
    "gate_id", "repository", "exact_commit", "dependency_commits", "run_id",
    "started_at", "completed_at", "outcome", "artifact_links",
}
OBSERVATION_FIELDS = COMMON_FIELDS | {"observation_id", "recorded_at", "limitations"}


def invalid(message: str) -> None:
    raise ValueError(message)


def mapping(value, context: str) -> dict:
    if not isinstance(value, dict):
        invalid(f"{context} must be a mapping")
    return value


def non_empty_string(value, context: str) -> str:
    if not isinstance(value, str) or not value.strip():
        invalid(f"{context} must be a non-empty string")
    return value


def timestamp(value, context: str) -> datetime:
    text = non_empty_string(value, context)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        invalid(f"{context} must be an ISO-8601 timestamp")
    if parsed.tzinfo is None:
        invalid(f"{context} must include a timezone")
    return parsed


def validate_links(value, context: str) -> None:
    if not isinstance(value, list) or not value:
        invalid(f"{context} must be a non-empty list")
    for link in value:
        text = non_empty_string(link, context)
        parsed = urlparse(text)
        if parsed.scheme == "https" and (not parsed.netloc or not parsed.path):
            invalid(f"{context} contains a malformed HTTPS link")
        if parsed.scheme == "artifact" and not parsed.path:
            invalid(f"{context} contains an empty artifact reference")
        if parsed.scheme not in {"https", "artifact"}:
            invalid(f"{context} contains an unsupported link")


def validate_record(record: dict, profile_name: str, index: int, captured_at: datetime) -> str:
    context = f"{profile_name}: evidence {index}"
    record = mapping(record, context)
    kind = record.get("evidence_kind")
    required = GATE_FIELDS if kind == "gate_result" else OBSERVATION_FIELDS if kind == "device_observation" else None
    if required is None:
        invalid(f"{context}: invalid evidence_kind {kind!r}")
    missing_fields = required.difference(record)
    if missing_fields:
        invalid(f"{context} missing {', '.join(sorted(missing_fields))}")
    for field in COMMON_FIELDS - {"artifact_links"}:
        non_empty_string(record.get(field), f"{context}.{field}")
    if len(record["exact_commit"]) != 40 or any(character not in "0123456789abcdef" for character in record["exact_commit"]):
        invalid(f"{context}.exact_commit must be a full lowercase commit SHA")
    if record["source_profile"] != profile_name or record["destination_profile"] != profile_name:
        invalid(f"{context} profile identity does not match {profile_name}")
    validate_links(record["artifact_links"], f"{context}.artifact_links")
    if kind == "gate_result":
        mapping(record["dependency_commits"], f"{context}.dependency_commits")
        for dependency, revision in record["dependency_commits"].items():
            non_empty_string(dependency, f"{context}.dependency_commits key")
            revision = non_empty_string(revision, f"{context}.dependency_commits.{dependency}")
            if len(revision) != 40 or any(character not in "0123456789abcdef" for character in revision):
                invalid(f"{context}.dependency_commits.{dependency} must be a full lowercase commit SHA")
        started = timestamp(record["started_at"], f"{context}.started_at")
        completed = timestamp(record["completed_at"], f"{context}.completed_at")
        if completed < started:
            invalid(f"{context}.completed_at precedes started_at")
        if completed > captured_at:
            invalid(f"{context}.completed_at exceeds ledger captured_at")
        non_empty_string(record["gate_id"], f"{context}.gate_id")
        non_empty_string(record["run_id"], f"{context}.run_id")
    else:
        if timestamp(record["recorded_at"], f"{context}.recorded_at") > captured_at:
            invalid(f"{context}.recorded_at exceeds ledger captured_at")
        non_empty_string(record["observation_id"], f"{context}.observation_id")
        if not isinstance(record["limitations"], list) or not record["limitations"]:
            invalid(f"{context}.limitations must be a non-empty list")
        for limitation in record["limitations"]:
            non_empty_string(limitation, f"{context}.limitations")
    return record["outcome"]


def validate(path: Path) -> None:
    document = mapping(yaml.safe_load(path.read_text()), "document")
    if document.get("schema_version") != 1:
        invalid("schema_version must be 1")
    captured_at = timestamp(document.get("captured_at"), "captured_at")
    policy = mapping(document.get("policy", {}), "policy")
    if policy.get("normative") is not False or policy.get("does_not_create_compatibility_requirement") is not True:
        invalid("ledger must be explicitly non-normative and non-compatibility-forming")
    non_empty_string(policy.get("meaning"), "policy.meaning")
    profiles = mapping(document.get("profiles", {}), "profiles")
    if set(profiles) != PROFILES:
        invalid(f"profiles must be exactly: {', '.join(sorted(PROFILES))}")

    for name, profile in profiles.items():
        profile = mapping(profile, name)
        required_profile_fields = {"status", "environment_fingerprint", "evidence", "verified_scope", "gaps"}
        missing_profile_fields = required_profile_fields.difference(profile)
        if missing_profile_fields:
            invalid(f"{name}: missing {', '.join(sorted(missing_profile_fields))}")
        status = profile.get("status")
        if status not in STATUSES:
            invalid(f"{name}: invalid status {status!r}")
        non_empty_string(profile.get("environment_fingerprint"), f"{name}.environment_fingerprint")
        evidence = profile.get("evidence", [])
        gaps = profile.get("gaps", [])
        verified_scope = profile.get("verified_scope", [])
        if not isinstance(evidence, list) or not isinstance(gaps, list) or not isinstance(verified_scope, list):
            invalid(f"{name}: evidence, verified_scope, and gaps must be lists")
        for field_name, values in (("verified_scope", verified_scope), ("gaps", gaps)):
            for value in values:
                non_empty_string(value, f"{name}.{field_name}")
        if status == "missing":
            if evidence or verified_scope or not gaps:
                invalid(f"{name}: missing status requires gaps, empty verified_scope, and no real evidence")
            continue
        if not evidence:
            invalid(f"{name}: {status} status requires evidence")
        if not verified_scope:
            invalid(f"{name}: {status} status requires non-empty verified_scope")
        outcomes = set()
        for index, record in enumerate(evidence):
            if mapping(record, f"{name}: evidence {index}").get("environment_fingerprint") != profile["environment_fingerprint"]:
                invalid(f"{name}: evidence {index} environment fingerprint does not match profile")
            outcomes.add(validate_record(record, name, index, captured_at))
        expected = "passed" if status == "passed" else "failed"
        if outcomes != {expected}:
            invalid(f"{name}: every evidence outcome must match status {status}")


def main() -> int:
    try:
        validate(Path(sys.argv[1]))
    except (IndexError, OSError, ValueError, yaml.YAMLError) as error:
        print(f"evidence baseline error: {error}", file=sys.stderr)
        return 2
    print("current real-environment evidence baseline: 8 profiles validated")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
