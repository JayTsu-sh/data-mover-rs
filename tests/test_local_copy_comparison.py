#!/usr/bin/env python3
"""Contract for the real-filesystem Local copy architecture comparison."""

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "tests" / "lab" / "run-local-copy-comparison.sh"
BENCHMARK = ROOT / "examples" / "local_copy_comparison.rs"


class LocalCopyComparisonTest(unittest.TestCase):
    def test_runner_compares_both_public_paths_at_required_sizes(self):
        runner = RUNNER.read_text()

        self.assertIn("local_copy_comparison", runner)
        self.assertIn("4096", runner)
        self.assertIn("40 * 1024 * 1024", runner)
        self.assertIn("1024 * 1024 * 1024", runner)
        self.assertIn("legacy", runner)
        self.assertIn("legacy-durable", runner)
        self.assertIn("optimized", runner)
        self.assertIn("sha256sum", runner)

    def test_runner_can_select_a_tight_payload_and_fail_on_regression(self):
        runner = RUNNER.read_text()

        self.assertIn("LOCAL_COPY_PERF_PAYLOADS", runner)
        self.assertIn("LOCAL_COPY_PERF_MAX_REGRESSION_PERCENT", runner)
        self.assertIn("LOCAL_COPY_PERF_MAX_FIXED_OVERHEAD_MS", runner)
        self.assertIn("performance regression", runner)
        self.assertIn("fixed overhead", runner)
        self.assertIn('sync -d "$destination/$label.bin"', runner)

    def test_benchmark_uses_the_legacy_and_optimized_public_seams(self):
        benchmark = BENCHMARK.read_text()

        self.assertIn("StorageEnum::copy_file", benchmark)
        self.assertIn("LegacyDurable", benchmark)
        self.assertIn("sync_data", benchmark)
        self.assertIn("connect_backend", benchmark)
        self.assertIn("transfer(request)", benchmark)
        self.assertIn("enable_integrity_check: true", benchmark)


if __name__ == "__main__":
    unittest.main()
