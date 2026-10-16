import csv
import tempfile
import unittest
from pathlib import Path

from hdfs_memory_budget import budget_mib, validate


class HdfsMemoryBudgetTests(unittest.TestCase):
    def test_real_runner_wires_the_exact_scale_pair(self) -> None:
        runner = (Path(__file__).parent / "lab" / "run-hdfs-memory-e2e.sh").read_text()
        self.assertIn("scale_small_size=$((1024 * 1024 * 1024 + 137))", runner)
        self.assertIn("scale_large_size=$((100 * 1024 * 1024 * 1024 + 137))", runner)
        self.assertIn("run_case scale high hdfs-hdfs scale-1g", runner)
        self.assertIn("for scale_attempt in {1..6}", runner)
        self.assertIn("run_case scale high hdfs-hdfs scale-100g", runner)
        self.assertIn("--require-100-gib", runner)
        self.assertIn("resuming durable partial", runner)

    def test_budget_is_derived_from_windows_chunks_and_file_concurrency(self) -> None:
        self.assertEqual(budget_mib(1, 2, 1, 1), 116)
        self.assertEqual(budget_mib(1, 2, 4, 1), 134)
        self.assertEqual(budget_mib(1, 2, 8, 16), 188)
        self.assertEqual(budget_mib(2, 2, 8, 16), 280)

    def test_validator_accepts_bounded_two_size_samples(self) -> None:
        path = self._csv([(262_144, 70_000), (2_097_152, 75_000)])
        validate(path)

        allocator_variance = self._csv(
            [(262_144, 113_260), (2_097_152, 179_272)],
            profile="high",
            read=8,
            write=16,
            budget=188,
        )
        validate(allocator_variance)

    def test_validator_rejects_budget_and_growth_violations(self) -> None:
        over_budget = self._csv([(262_144, 130_000), (2_097_152, 130_000)])
        with self.assertRaisesRegex(ValueError, "exceeds budget"):
            validate(over_budget)
        growing = self._csv(
            [(262_144, 80_000), (2_097_152, 155_000)],
            profile="high",
            read=8,
            write=16,
            budget=188,
        )
        with self.assertRaisesRegex(ValueError, "RSS grew"):
            validate(growing)

    def test_validator_requires_one_real_100_gib_sample_when_requested(self) -> None:
        hundred_gib = 100 * 1024 * 1024 * 1024 + 137
        path = self._csv(
            [(1024**3 + 137, 75_000)] * 6 + [(hundred_gib, 76_000)],
            profile="high",
            read=8,
            write=16,
            budget=188,
            direction="hdfs-hdfs",
            sample_set="scale",
        )
        validate(path, require_100_gib=True)

        missing = self._csv(
            [(1024**3 + 137, 70_000), (2 * 1024**3 + 137, 75_000)],
            profile="high",
            direction="hdfs-hdfs",
            sample_set="scale",
        )
        with self.assertRaisesRegex(ValueError, "100 GiB"):
            validate(missing, require_100_gib=True)

        unstable = self._csv(
            [(1024**3 + 137, 75_000)] * 6 + [(hundred_gib, 84_000)],
            profile="high",
            direction="hdfs-hdfs",
            sample_set="scale",
        )
        with self.assertRaisesRegex(ValueError, "10%"):
            validate(unstable, require_100_gib=True)

    def test_scale_guard_uses_repeated_short_transfer_peak(self) -> None:
        hundred_gib = 100 * 1024 * 1024 * 1024 + 137
        short = 1024**3 + 137
        path = self._csv(
            [
                (short, 75_000),
                (short, 88_000),
                (short, 104_868),
                (short, 96_980),
                (short, 89_848),
                (short, 120_876),
                (hundred_gib, 132_064),
            ],
            profile="high",
            read=8,
            write=16,
            budget=188,
            direction="hdfs-hdfs",
            sample_set="scale",
        )

        validate(path, require_100_gib=True)

    def _csv(
        self,
        samples: list[tuple[int, int]],
        *,
        profile: str = "serial",
        read: int = 1,
        write: int = 1,
        budget: int = 116,
        sample_set: str = "baseline",
        direction: str = "local-hdfs",
    ) -> Path:
        temporary = tempfile.NamedTemporaryFile(mode="w", newline="", delete=False)
        self.addCleanup(Path(temporary.name).unlink, missing_ok=True)
        writer = csv.DictWriter(
            temporary,
            fieldnames=[
                "run_id",
                "commit",
                "profile",
                "sample_set",
                "direction",
                "bytes",
                "max_rss_kib",
                "budget_mib",
                "file_concurrency",
                "chunk_mib",
                "read_inflight",
                "write_inflight",
            ],
        )
        writer.writeheader()
        for size, rss in samples:
            writer.writerow(
                {
                    "run_id": "unit-test",
                    "commit": "0" * 40,
                    "profile": profile,
                    "sample_set": sample_set,
                    "direction": direction,
                    "bytes": size,
                    "max_rss_kib": rss,
                    "budget_mib": budget,
                    "file_concurrency": 1,
                    "chunk_mib": 2,
                    "read_inflight": read,
                    "write_inflight": write,
                }
            )
        temporary.close()
        return Path(temporary.name)


if __name__ == "__main__":
    unittest.main()
