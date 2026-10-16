#!/usr/bin/env python3
"""Validate architecture profiles, capabilities, directed cells, and evidence revisions."""

import re
import subprocess
import sys
from itertools import product
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
REQUIRED_CAPABILITIES = {
    "connectivity", "traversal_complete", "sequential_read", "range_read",
    "staged_destination", "ordered_write", "out_of_order_write", "truncate_overwrite",
    "durable_checkpoint", "resume", "restart_upload", "publication", "cancellation",
    "native_transfer", "content_readback", "directory", "symlink", "hardlink_topology",
    "rename", "delete", "acl", "xattr", "tags", "uid_gid_mode", "timestamps",
    "backend_fact_reconstruction", "safe_protocol_retry",
}
EVIDENCE_LAYER_PREFIXES = {
    "data_mover": "DM-DIRECTED",
    "terrasync_single_process": "TS-SINGLE",
    "terrasync_two_process_quic": "TS-REMOTE",
}


def invalid(message: str) -> None:
    raise ValueError(message)


def mapping(value, context: str) -> dict:
    if not isinstance(value, dict):
        invalid(f"{context} must be a mapping")
    return value


def sequence(value, context: str) -> list:
    if not isinstance(value, list):
        invalid(f"{context} must be a list")
    return value


def text(value, context: str) -> str:
    if not isinstance(value, str) or not value.strip():
        invalid(f"{context} must be a non-empty string")
    return value


def load(path: Path, context: str) -> dict:
    return mapping(yaml.safe_load(path.read_text()), context)


def capability_state(value, context: str) -> tuple[str, dict]:
    if isinstance(value, str):
        return value, {}
    details = mapping(value, context)
    return text(details.get("state"), f"{context}.state"), details


def authoritative_gates() -> set[str]:
    source = (ROOT / "docs" / "architecture" / "acceptance-gates.md").read_text()
    return set(re.findall(r"`([A-Z][A-Z0-9/-]+)`", source))


def validate_capabilities(document: dict) -> tuple[list[str], dict[str, set[str]]]:
    profiles = mapping(document.get("profiles"), "capability profiles")
    states = set(sequence(document.get("states"), "capability states"))
    profile_gates = {}
    registered_gates = authoritative_gates()
    for profile_name, raw_profile in profiles.items():
        profile = mapping(raw_profile, f"capability profile {profile_name}")
        gates = set(sequence(profile.get("gates"), f"{profile_name}.gates"))
        if not gates:
            invalid(f"{profile_name} must declare gates")
        for gate in gates:
            text(gate, f"{profile_name}.gates entry")
            if gate not in registered_gates:
                invalid(f"{profile_name} gate {gate} is not declared in acceptance-gates.md")
        profile_gates[profile_name] = gates
        capabilities = mapping(profile.get("capabilities"), f"{profile_name}.capabilities")
        if set(capabilities) != REQUIRED_CAPABILITIES:
            missing = sorted(REQUIRED_CAPABILITIES - set(capabilities))
            extra = sorted(set(capabilities) - REQUIRED_CAPABILITIES)
            invalid(f"{profile_name} capability fields are incomplete; missing={missing}, extra={extra}")
        for name, value in capabilities.items():
            state, details = capability_state(value, f"{profile_name}.{name}")
            if state not in states:
                invalid(f"{profile_name}.{name} has invalid capability state {state!r}")
            if state == "uncertified":
                gate = text(details.get("gate"), f"{profile_name}.{name}.gate")
                if gate not in gates:
                    invalid(f"{profile_name}.{name} gate {gate} is not declared by profile")
            if state == "instance_operation_result" and len(details) < 2:
                invalid(f"{profile_name}.{name} must describe instance operation results")
    return list(profiles), profile_gates


def validate_semantics(cell: dict, document: dict, context: str) -> None:
    semantic_states = set(sequence(document.get("semantic_states"), "semantic states"))
    native_states = set(sequence(document.get("native_states"), "native states"))
    if cell.get("native") not in native_states:
        invalid(f"{context}.native has invalid state")
    required = mapping(document.get("validation"), "directed validation")
    for family, fields_key in (("metadata", "require_metadata_fields"), ("namespace", "require_namespace_fields")):
        values = mapping(cell.get(family), f"{context}.{family}")
        fields = sequence(required.get(fields_key), f"validation.{fields_key}")
        if set(values) != set(fields):
            invalid(f"{context}.{family} fields are incomplete")
        if any(value not in semantic_states for value in values.values()):
            invalid(f"{context}.{family} has invalid semantic state")


def validate_directed(document: dict, profiles: list[str], profile_gates: dict[str, set[str]]) -> int:
    directed_profiles = sequence(document.get("profiles"), "directed profiles")
    if set(directed_profiles) != set(profiles) or len(directed_profiles) != len(profiles):
        invalid("capability and directed profile sets differ")
    cells = sequence(document.get("cells"), "directed cells")
    expected_count = mapping(document.get("validation"), "directed validation").get("expected_cell_count")
    if expected_count != len(profiles) ** 2 or len(cells) != expected_count:
        invalid(f"directed matrix must contain exactly {len(profiles) ** 2} cells")
    layers = sequence(document.get("evidence_layers"), "evidence layers")
    if set(layers) != set(EVIDENCE_LAYER_PREFIXES) or len(layers) != len(EVIDENCE_LAYER_PREFIXES):
        invalid(f"evidence layers must be exactly {sorted(EVIDENCE_LAYER_PREFIXES)}")
    seen = set()
    projected_gates = set()
    cell_states = set(sequence(document.get("cell_states"), "cell states"))
    for index, raw_cell in enumerate(cells):
        cell = mapping(raw_cell, f"cell {index}")
        pair = (cell.get("source"), cell.get("destination"))
        if pair in seen:
            invalid(f"duplicate ordered pair {pair[0]} -> {pair[1]}")
        seen.add(pair)
        validate_cell(cell, index, profiles, profile_gates, cell_states, document)
        projected_gates.update(
            f"{EVIDENCE_LAYER_PREFIXES[layer]}/{cell['gate_key']}" for layer in layers
        )
    if seen != set(product(profiles, repeat=2)):
        invalid("directed matrix does not contain the complete ordered profile product")
    expected_gates = len(cells) * len(EVIDENCE_LAYER_PREFIXES)
    if len(projected_gates) != expected_gates:
        invalid(f"directed matrix must project exactly {expected_gates} unique gates")
    return len(projected_gates)


def validate_cell(cell, index, profiles, profile_gates, cell_states, document) -> None:
    context = f"cell {index}"
    source, destination = cell.get("source"), cell.get("destination")
    if source not in profiles or destination not in profiles:
        invalid(f"{context} references unknown profile")
    expected_key = f"{source}__{destination}"
    if cell.get("gate_key") != expected_key:
        invalid(f"{context}.gate_key must be {expected_key}")
    if cell.get("status") not in cell_states or cell.get("resume") not in cell_states:
        invalid(f"{context} has invalid cell state")
    if cell.get("resume") == "uncertified":
        gate = text(cell.get("resume_gate"), f"{context}.resume_gate")
        if gate not in profile_gates[source] | profile_gates[destination]:
            invalid(f"{context}.resume_gate is not declared by either profile")
    validate_semantics(cell, document, context)


def exact_commit_report(document: dict, profiles: list[str]) -> list[str]:
    evidence_profiles = mapping(document.get("profiles"), "evidence profiles")
    if set(evidence_profiles) != set(profiles):
        invalid("capability and evidence profile sets differ")
    report = []
    for profile in profiles:
        records = sequence(mapping(evidence_profiles[profile], profile).get("evidence"), f"{profile}.evidence")
        if not records:
            report.append(f"{profile}@missing")
            continue
        for record in records:
            evidence = mapping(record, f"{profile}.evidence record")
            revision = text(evidence.get("exact_commit"), "exact_commit")
            if len(revision) != 40 or any(character not in "0123456789abcdef" for character in revision):
                invalid("exact_commit must be a full lowercase SHA")
            repository = text(evidence.get("repository"), f"{profile}.repository")
            if repository != "JayTsu-sh/data-mover-rs":
                invalid(f"{profile} evidence repository is not independently verifiable")
            resolved = subprocess.run(
                ["git", "cat-file", "-e", f"{revision}^{{commit}}"],
                cwd=ROOT,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
            if resolved.returncode != 0:
                invalid(f"exact_commit {revision} does not resolve to a repository commit")
        report.append(f"{profile}@{records[-1]['exact_commit'][:12]}")
    return report


def main() -> int:
    try:
        capabilities = load(Path(sys.argv[1]), "capability matrix")
        directed = load(Path(sys.argv[2]), "directed matrix")
        evidence = load(Path(sys.argv[3]), "evidence ledger")
        profiles, profile_gates = validate_capabilities(capabilities)
        projected = validate_directed(directed, profiles, profile_gates)
        report = exact_commit_report(evidence, profiles)
    except (IndexError, OSError, ValueError, yaml.YAMLError) as error:
        print(f"architecture matrix error: {error}", file=sys.stderr)
        return 2
    print(f"architecture matrix: {len(profiles)} profiles, {len(profiles) ** 2} cells, {projected} projected gates")
    print("exact commits: " + ", ".join(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
