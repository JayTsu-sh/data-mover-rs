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
        contract = (ROOT / "tests/lab/tools/nfs3_contract.rs").read_text()
        self.assertIn("Nfs41", contract)
        self.assertIn("validate_cancel_and_restart", contract)
        self.assertIn("validate_recovery", contract)
        self.assertIn("validate_acl", contract)

    def test_nightly_runs_the_independent_gate(self):
        nightly = (ROOT / ".github/workflows/nightly.yml").read_text()
        self.assertIn("ArchitectureReady NFSv4.1 contract", nightly)
        self.assertIn('tests/lab/run-nfs41-contract.sh "$RUN_ID"', nightly)

    def test_source_lookup_keeps_the_native_session_failure_classification(self):
        client = (ROOT / "src/storage/backends/nfs/client.rs").read_text()
        lookup = client.split("async fn role_lookup_with_attrs(", 1)[1].split(
            "/// Optimized role-only read primitive.", 1
        )[0]
        self.assertIn(".role_lookup_fh(", lookup)
        self.assertNotIn(".lookup_fh(", lookup)

    def test_cancelled_attempt_reconnects_before_restart_and_checks_content(self):
        contract = (ROOT / "tests/lab/tools/nfs3_contract.rs").read_text()
        probe = contract.split("async fn validate_cancel_and_restart(", 1)[1].split(
            "/// Recovery kept at the destination", 1
        )[0]
        discard = probe.index("failure.discard_stage().await?")
        reconnect = probe.index("connect_nfs(source_url, false)", discard)
        restart = probe.index("let outcome = transfer(", reconnect)
        self.assertLess(discard, reconnect)
        self.assertLess(reconnect, restart)
        self.assertIn("connect_nfs(destination_url, true)", probe)
        self.assertIn("outcome.blake3", probe)
        self.assertIn("outcome.read_back", probe)
        self.assertIn("PrepareFact::Fresh", probe)
        self.assertIn("original.encode_snapshot() != reconnected.encode_snapshot()", probe)
        self.assertIn("copy_support::hash(&destination", probe)
        self.assertIn("Ok((source, destination))", probe)


if __name__ == "__main__":
    unittest.main()
