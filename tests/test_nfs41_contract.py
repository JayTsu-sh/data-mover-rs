#!/usr/bin/env python3
"""Static safety and wiring contract for the real NFSv4.1 gate."""

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class Nfs41ContractTest(unittest.TestCase):
    def test_runner_selects_exact_v41_and_two_real_servers(self):
        runner = (ROOT / "tests/lab/run-nfs41-contract.sh").read_text()
        self.assertIn("validate_run_id", runner)
        self.assertIn("version=4.1", runner)
        self.assertNotIn("version=4,", runner)
        self.assertIn("LAB_SOURCE_DATA", runner)
        self.assertIn("LAB_DEST_DATA", runner)
        self.assertIn("--dialect nfs41", runner)
        self.assertIn("--stale-ready-file", runner)
        self.assertIn("old_stale_inode", runner)
        self.assertIn("new_stale_inode", runner)

    def test_shared_contract_covers_stateful_failure_and_recovery_paths(self):
        contract = (ROOT / "examples/nfs3_contract.rs").read_text()
        self.assertIn("Nfs41", contract)
        self.assertIn("validate_cancel_and_restart", contract)
        self.assertIn("validate_recovery", contract)
        self.assertIn("validate_acl", contract)

    def test_nightly_runs_the_independent_gate(self):
        nightly = (ROOT / ".github/workflows/nightly.yml").read_text()
        self.assertIn("ArchitectureReady NFSv4.1 contract", nightly)
        self.assertIn('tests/lab/run-nfs41-contract.sh "$RUN_ID"', nightly)


if __name__ == "__main__":
    unittest.main()
