#!/usr/bin/env python3
"""Contract for the isolated Local destination write benchmark."""

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "tests" / "lab" / "run-local-write-benchmark.sh"
BENCHMARK = ROOT / "examples" / "local_write_benchmark.rs"


class LocalWriteBenchmarkTest(unittest.TestCase):
    def test_runner_covers_chunk_concurrency_and_filesystem_matrix(self):
        runner = RUNNER.read_text()

        self.assertIn("65536 262144 1048576 2097152 4194304 8388608", runner)
        self.assertIn('"1 2 4 8"', runner)
        self.assertIn("ext4=/tmp xfs=/work tmpfs=/dev/shm", runner)
        self.assertIn("submit_ns,sync_ns,total_ns,write_calls,short_writes", runner)

    def test_benchmark_matches_the_production_positional_write_shape(self):
        benchmark = BENCHMARK.read_text()

        self.assertIn("create_new(true)", benchmark)
        self.assertNotIn("set_len(args.total_bytes)", benchmark)
        self.assertIn("file.write_at(&data[written..], position)", benchmark)
        self.assertIn("writes.spawn_blocking", benchmark)
        self.assertIn("sync_file.sync_all()", benchmark)
        self.assertIn("std::fs::remove_file(&args.file)", benchmark)


if __name__ == "__main__":
    unittest.main()
