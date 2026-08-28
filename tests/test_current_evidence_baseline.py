#!/usr/bin/env python3
"""Contract tests for the non-normative current real-environment ledger."""

import subprocess
import tempfile
import unittest
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
VALIDATOR = ROOT / "tests" / "validate_current_evidence_baseline.py"
BASELINE = ROOT / "docs" / "architecture" / "current-real-environment-evidence.yaml"


class CurrentEvidenceBaselineTest(unittest.TestCase):
    def run_validator(self, path: Path) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["python3", str(VALIDATOR), str(path)],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )

    def test_baseline_is_complete_and_machine_validated(self):
        result = self.run_validator(BASELINE)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("8 profiles", result.stdout)

    def test_ledger_is_explicitly_non_normative(self):
        text = BASELINE.read_text()
        self.assertIn("normative: false", text)
        self.assertIn("does_not_create_compatibility_requirement: true", text)

    def test_rejects_missing_profile(self):
        document = yaml.safe_load(BASELINE.read_text())
        document["profiles"].pop("nfs40")
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "evidence.yaml"
            path.write_text(yaml.safe_dump(document))
            result = self.run_validator(path)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("profiles must be exactly", result.stderr)

    def test_rejects_history_promoted_to_normative_contract(self):
        document = yaml.safe_load(BASELINE.read_text())
        document["policy"]["normative"] = True
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "evidence.yaml"
            path.write_text(yaml.safe_dump(document))
            result = self.run_validator(path)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("non-normative", result.stderr)

    def test_rejects_non_mapping_document(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "evidence.yaml"
            path.write_text("- not-a-mapping\n")
            result = self.run_validator(path)
        self.assertEqual(result.returncode, 2)
        self.assertIn("document must be a mapping", result.stderr)

    def test_rejects_incomplete_gate_record(self):
        document = yaml.safe_load(BASELINE.read_text())
        document["profiles"]["local"]["evidence"][0]["exact_commit"] = None
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "evidence.yaml"
            path.write_text(yaml.safe_dump(document))
            result = self.run_validator(path)
        self.assertEqual(result.returncode, 2)
        self.assertIn("exact_commit must be a non-empty string", result.stderr)

    def test_rejects_cross_profile_evidence(self):
        document = yaml.safe_load(BASELINE.read_text())
        document["profiles"]["local"]["evidence"][0]["source_profile"] = "nfs3"
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "evidence.yaml"
            path.write_text(yaml.safe_dump(document))
            result = self.run_validator(path)
        self.assertEqual(result.returncode, 2)
        self.assertIn("profile identity", result.stderr)

    def test_rejects_ambiguous_profile_outcomes(self):
        document = yaml.safe_load(BASELINE.read_text())
        document["profiles"]["local"]["evidence"][0]["outcome"] = "failed"
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "evidence.yaml"
            path.write_text(yaml.safe_dump(document))
            result = self.run_validator(path)
        self.assertEqual(result.returncode, 2)
        self.assertIn("every evidence outcome", result.stderr)

    def test_rejects_invalid_profile_envelope(self):
        document = yaml.safe_load(BASELINE.read_text())
        document["profiles"]["nfs40"]["environment_fingerprint"] = {"not": "a string"}
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "evidence.yaml"
            path.write_text(yaml.safe_dump(document))
            result = self.run_validator(path)
        self.assertEqual(result.returncode, 2)
        self.assertIn("environment_fingerprint must be a non-empty string", result.stderr)

    def test_rejects_malformed_artifact_link(self):
        document = yaml.safe_load(BASELINE.read_text())
        document["profiles"]["local"]["evidence"][0]["artifact_links"] = ["https:not-a-url"]
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "evidence.yaml"
            path.write_text(yaml.safe_dump(document))
            result = self.run_validator(path)
        self.assertEqual(result.returncode, 2)
        self.assertIn("malformed HTTPS link", result.stderr)

    def test_rejects_scope_that_contradicts_status(self):
        for profile, scope, expected in (
            ("nfs40", ["copy"], "empty verified_scope"),
            ("local", [], "non-empty verified_scope"),
        ):
            with self.subTest(profile=profile):
                document = yaml.safe_load(BASELINE.read_text())
                document["profiles"][profile]["verified_scope"] = scope
                with tempfile.TemporaryDirectory() as temporary:
                    path = Path(temporary) / "evidence.yaml"
                    path.write_text(yaml.safe_dump(document))
                    result = self.run_validator(path)
                self.assertEqual(result.returncode, 2)
                self.assertIn(expected, result.stderr)


if __name__ == "__main__":
    unittest.main()
