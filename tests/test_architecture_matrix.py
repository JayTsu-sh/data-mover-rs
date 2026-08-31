#!/usr/bin/env python3
"""Contract tests for the architecture capability and directed gate matrices."""

import subprocess
import tempfile
import unittest
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
VALIDATOR = ROOT / "tests" / "validate_architecture_matrix.py"
DOCS = ROOT / "docs" / "architecture"
CAPABILITIES = DOCS / "backend-capability-matrix.yaml"
DIRECTED = DOCS / "directed-transfer-matrix.yaml"
EVIDENCE = DOCS / "current-real-environment-evidence.yaml"


class ArchitectureMatrixTest(unittest.TestCase):
    def run_validator(
        self, capabilities: Path = CAPABILITIES, directed: Path = DIRECTED, evidence: Path = EVIDENCE
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["python3", str(VALIDATOR), str(capabilities), str(directed), str(evidence)],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )

    def mutated(self, source: Path, mutate):
        document = yaml.safe_load(source.read_text())
        mutate(document)
        temporary = tempfile.TemporaryDirectory()
        path = Path(temporary.name) / source.name
        path.write_text(yaml.safe_dump(document, sort_keys=False))
        self.addCleanup(temporary.cleanup)
        return path

    def test_repository_matrix_is_complete_and_reports_exact_commits(self):
        result = self.run_validator()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("8 profiles, 64 cells, 192 projected gates", result.stdout)
        self.assertIn("cifs_fas2750@0a4be15f3509", result.stdout)

    def test_ci_runs_the_cross_file_matrix_contract(self):
        workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text()
        self.assertIn("python3 tests/validate_architecture_matrix.py", workflow)
        self.assertIn("python3 -m unittest tests/test_architecture_matrix.py", workflow)

    def test_rejects_duplicate_ordered_pair(self):
        directed = self.mutated(
            DIRECTED,
            lambda document: document["cells"].__setitem__(1, document["cells"][0].copy()),
        )
        result = self.run_validator(directed=directed)
        self.assertEqual(result.returncode, 2)
        self.assertIn("duplicate ordered pair", result.stderr)

    def test_rejects_missing_cell(self):
        directed = self.mutated(DIRECTED, lambda document: document["cells"].pop())
        result = self.run_validator(directed=directed)
        self.assertEqual(result.returncode, 2)
        self.assertIn("exactly 64 cells", result.stderr)

    def test_rejects_gate_key_that_does_not_match_the_ordered_pair(self):
        def mutate(document):
            document["cells"][0]["gate_key"] = "local__nfs3"

        directed = self.mutated(DIRECTED, mutate)
        result = self.run_validator(directed=directed)
        self.assertEqual(result.returncode, 2)
        self.assertIn("gate_key must be local__local", result.stderr)

    def test_rejects_unknown_capability_state(self):
        capabilities = self.mutated(
            CAPABILITIES,
            lambda document: document["profiles"]["local"]["capabilities"].__setitem__(
                "sequential_read", "maybe"
            ),
        )
        result = self.run_validator(capabilities=capabilities)
        self.assertEqual(result.returncode, 2)
        self.assertIn("invalid capability state", result.stderr)

    def test_rejects_missing_capability_field(self):
        def mutate(document):
            del document["profiles"]["local"]["capabilities"]["sequential_read"]

        capabilities = self.mutated(CAPABILITIES, mutate)
        result = self.run_validator(capabilities=capabilities)
        self.assertEqual(result.returncode, 2)
        self.assertIn("capability fields are incomplete", result.stderr)

    def test_rejects_profile_gate_absent_from_authoritative_gate_document(self):
        def mutate(document):
            document["profiles"]["local"]["gates"][0] = "DM-LOCAL-CONTRCAT"

        capabilities = self.mutated(CAPABILITIES, mutate)
        result = self.run_validator(capabilities=capabilities)
        self.assertEqual(result.returncode, 2)
        self.assertIn("is not declared in acceptance-gates.md", result.stderr)

    def test_rejects_uncertified_capability_without_profile_gate(self):
        def mutate(document):
            document["profiles"]["cifs_fas2750"]["capabilities"]["resume"]["gate"] = "DM-UNKNOWN"

        capabilities = self.mutated(CAPABILITIES, mutate)
        result = self.run_validator(capabilities=capabilities)
        self.assertEqual(result.returncode, 2)
        self.assertIn("is not declared by profile", result.stderr)

    def test_rejects_profile_set_drift(self):
        directed = self.mutated(DIRECTED, lambda document: document["profiles"].remove("nfs40"))
        result = self.run_validator(directed=directed)
        self.assertEqual(result.returncode, 2)
        self.assertIn("profile sets differ", result.stderr)

    def test_rejects_non_exact_evidence_commit(self):
        def mutate(document):
            document["profiles"]["cifs_fas2750"]["evidence"][0]["exact_commit"] = "0a4be15"

        evidence = self.mutated(EVIDENCE, mutate)
        result = self.run_validator(evidence=evidence)
        self.assertEqual(result.returncode, 2)
        self.assertIn("exact_commit must be a full lowercase SHA", result.stderr)

    def test_rejects_nonexistent_repository_commit(self):
        def mutate(document):
            document["profiles"]["cifs_fas2750"]["evidence"][0]["exact_commit"] = "0" * 40

        evidence = self.mutated(EVIDENCE, mutate)
        result = self.run_validator(evidence=evidence)
        self.assertEqual(result.returncode, 2)
        self.assertIn("does not resolve to a repository commit", result.stderr)

    def test_rejects_incomplete_evidence_layers(self):
        directed = self.mutated(DIRECTED, lambda document: document["evidence_layers"].pop())
        result = self.run_validator(directed=directed)
        self.assertEqual(result.returncode, 2)
        self.assertIn("evidence layers must be exactly", result.stderr)


if __name__ == "__main__":
    unittest.main()
