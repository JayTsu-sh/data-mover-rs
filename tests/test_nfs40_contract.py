#!/usr/bin/env python3
"""Static safety and wiring contract for the real NFSv4.0 gate."""

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class Nfs40ContractTest(unittest.TestCase):
    def test_runner_selects_exact_v40_and_isolates_the_run(self):
        runner = (ROOT / "tests/lab/run-nfs40-contract.sh").read_text()
        self.assertIn("validate_run_id", runner)
        self.assertIn("version=4.0", runner)
        self.assertNotIn("version=4,", runner)
        self.assertIn('run_root="data-mover-ci/$run_id"', runner)
        self.assertIn("--dialect nfs40", runner)

    def test_endpoint_has_explicit_defaults(self):
        common = (ROOT / "tests/lab/common.sh").read_text()
        self.assertIn('LAB_NFS40_DATA="${LAB_NFS40_DATA:-10.131.7.201}"', common)
        self.assertIn('LAB_NFS40_EXPORT="${LAB_NFS40_EXPORT:-/jay_nfs}"', common)

    def test_contract_exercises_acl_operations_and_typed_unsupported(self):
        contract = (ROOT / "examples/nfs3_contract.rs").read_text()
        self.assertIn("MetadataObservation::Value", contract)
        self.assertIn("ObservationMode::Required", contract)
        self.assertIn("MetadataMutation::Acl", contract)
        self.assertIn("FailureClass::Unsupported", contract)
        self.assertIn("Transience::Permanent", contract)
        self.assertIn("nfs_rs::NfsError::Unsupported", contract)
        self.assertIn("mount.getacl", contract)
        self.assertIn("mount.setacl", contract)

    def test_contract_exercises_real_cancellation_and_restart_upload(self):
        contract = (ROOT / "examples/nfs3_contract.rs").read_text()
        self.assertIn("validate_cancel_and_restart", contract)
        self.assertIn("failure.discard_stage().await?", contract)
        self.assertIn("Resumability::Enabled, None", contract)
        self.assertIn("has_recoverable_stage", contract)
        self.assertIn("cancel.cancel()", contract)

    def test_contract_replaces_a_real_v40_file_handle(self):
        contract = (ROOT / "examples/nfs3_contract.rs").read_text()
        self.assertIn("validate_nfs4_stale_retry", contract)
        self.assertIn("stale fixture did not replace the file identity", contract)
        self.assertIn("MetadataMutation::NumericOwnership", contract)

    def test_nightly_runs_the_dedicated_gate(self):
        nightly = (ROOT / ".github/workflows/nightly.yml").read_text()
        self.assertIn("ArchitectureReady NFSv4.0 contract", nightly)
        self.assertIn('tests/lab/run-nfs40-contract.sh "$RUN_ID"', nightly)


if __name__ == "__main__":
    unittest.main()
