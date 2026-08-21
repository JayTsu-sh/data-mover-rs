import csv
import tempfile
import unittest
from pathlib import Path

from hdfs_memory_budget import budget_mib, validate


class HdfsMemoryBudgetTests(unittest.TestCase):
    def test_budget_is_derived_from_windows_chunks_and_file_concurrency(self) -> None:
        self.assertEqual(budget_mib(1, 2, 1, 1), 116)
        self.assertEqual(budget_mib(1, 2, 4, 1), 134)
        self.assertEqual(budget_mib(1, 2, 8, 16), 188)
        self.assertEqual(budget_mib(2, 2, 8, 16), 280)

    def test_validator_accepts_bounded_two_size_samples(self) -> None:
        path = self._csv([(262_144, 70_000), (2_097_152, 75_000)])
        validate(path)

    def test_validator_rejects_budget_and_growth_violations(self) -> None:
        over_budget = self._csv([(262_144, 130_000), (2_097_152, 130_000)])
        with self.assertRaisesRegex(ValueError, "exceeds budget"):
            validate(over_budget)
        growing = self._csv(
            [(262_144, 80_000), (2_097_152, 150_000)],
            profile="high",
            read=8,
            write=16,
            budget=188,
        )
        with self.assertRaisesRegex(ValueError, "RSS grew"):
            validate(growing)

    def _csv(
        self,
        samples: list[tuple[int, int]],
        *,
        profile: str = "serial",
        read: int = 1,
        write: int = 1,
        budget: int = 116,
    ) -> Path:
        temporary = tempfile.NamedTemporaryFile(mode="w", newline="", delete=False)
        self.addCleanup(Path(temporary.name).unlink, missing_ok=True)
        writer = csv.DictWriter(
            temporary,
            fieldnames=[
                "profile",
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
                    "profile": profile,
                    "direction": "local-hdfs",
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
