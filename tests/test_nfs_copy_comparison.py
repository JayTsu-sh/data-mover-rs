#!/usr/bin/env python3
"""Contract for the real Linux-nfsd NFS copy architecture comparison."""

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "tests" / "lab" / "run-nfs-copy-comparison.sh"
BENCHMARK = ROOT / "examples" / "nfs_copy_comparison.rs"


class NfsCopyComparisonTest(unittest.TestCase):
    def test_runner_compares_only_raw_legacy_and_optimized_at_required_sizes(self):
        runner = RUNNER.read_text()

        self.assertIn("nfs_copy_comparison", runner)
        self.assertIn("4096", runner)
        self.assertIn("4 * 1024 * 1024", runner)
        self.assertIn("40 * 1024 * 1024", runner)
        self.assertIn("1024 * 1024 * 1024", runner)
        self.assertIn("legacy", runner)
        self.assertIn("optimized", runner)
        self.assertNotIn("legacy-durable", runner)
        self.assertIn("version=4.1", runner)

    def test_runner_rotates_order_and_can_fail_on_regression(self):
        runner = RUNNER.read_text()

        self.assertIn("NFS_COPY_PERF_PAYLOADS", runner)
        self.assertIn("NFS_COPY_PERF_MAX_REGRESSION_PERCENT", runner)
        self.assertIn("NFS_COPY_PERF_READ_INFLIGHT", runner)
        self.assertIn("NFS_COPY_PERF_WRITE_INFLIGHT", runner)
        self.assertIn("performance regression", runner)
        self.assertIn("implementations=(legacy optimized)", runner)
        self.assertIn("implementations=(optimized legacy)", runner)
        self.assertIn('DATA_MOVER_NFS_READ_INFLIGHT="$read_inflight"', runner)
        self.assertIn('DATA_MOVER_NFS_WRITE_INFLIGHT="$write_inflight"', runner)

    def test_benchmark_uses_both_public_copy_seams_without_durability_shim(self):
        benchmark = BENCHMARK.read_text()

        self.assertIn("StorageEnum::copy_file", benchmark)
        self.assertIn("connect_backend", benchmark)
        self.assertIn("transfer(request)", benchmark)
        self.assertIn("enable_integrity_check: true", benchmark)
        self.assertNotIn("sync_data", benchmark)
        self.assertNotIn("LegacyDurable", benchmark)

    def test_optimized_copy_exposes_checkpointed_and_atomic_replace_recovery_modes(self):
        runner = RUNNER.read_text()
        benchmark = BENCHMARK.read_text()

        self.assertIn("NFS_COPY_PERF_RECOVERY", runner)
        self.assertIn('--transfer-policy "$recovery"', runner)
        self.assertIn("TransferMode::Checkpointed => TransferPolicy::Checkpointed", benchmark)
        self.assertIn("TransferMode::AtomicReplace => TransferPolicy::AtomicReplace", benchmark)


if __name__ == "__main__":
    unittest.main()
