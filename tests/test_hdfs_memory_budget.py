import csv
import tempfile
import unittest
from pathlib import Path

from hdfs_memory_budget import budget_mib, validate


class _TraceFixture(unittest.TestCase):
    def _trace(self, transfer, verification=None):
        temporary = tempfile.NamedTemporaryFile(mode="w", newline="", delete=False)
        self.addCleanup(Path(temporary.name).unlink, missing_ok=True)
        writer = csv.writer(temporary)
        writer.writerow(["run_id", "commit", "pid", "phase", "elapsed_s", "rss_kib"])
        elapsed = 0
        for phase, values in [("transfer", transfer), ("verification", transfer if verification is None else verification)]:
            for rss in values:
                writer.writerow(["unit-test", "0" * 40, 1234, phase, elapsed, rss])
                elapsed += 0.1
        temporary.close()
        return Path(temporary.name)

    def check_trace(self, path, peak=180_000):
        from hdfs_memory_budget import validate_trace
        validate_trace(path, run_id="unit-test", commit="0" * 40,
                       peak_kib=peak, budget_kib=budget_mib(1, 2, 8, 16) * 1024)


class HdfsMemoryTraceTests(_TraceFixture):
    def test_warmup_and_bounded_allocator_peak_do_not_imply_growth(self):
        # The process warms up, reaches a stable platform, then releases buffers.
        values = [70_000] * 10 + [160_000] * 80 + [100_000] * 10
        values[50] = 180_000
        self.check_trace(self._trace(values))

    def test_sustained_growth_fails_even_below_the_absolute_budget(self):
        for phase in ["transfer", "verification"]:
            with self.subTest(phase=phase):
                stable = [110_000] * 100
                growing = [110_000 + index * 600 for index in range(100)]
                trace = self._trace(growing if phase == "transfer" else stable,
                                    growing if phase == "verification" else stable)
                with self.assertRaisesRegex(ValueError, "10%"):
                    self.check_trace(trace)

    def test_missing_short_and_unbound_evidence_fails_closed(self):
        for values in [[100_000] * 10, [100_000] * 100]:
            trace = self._trace(values)
            if len(values) == 100:
                trace.write_text(trace.read_text().replace("unit-test", "wrong-run"))
            with self.assertRaises(ValueError):
                self.check_trace(trace)

    def test_absolute_budget_and_reported_peak_cannot_hide_trace_spikes(self):
        values = [100_000] * 100
        values[0] = 212_000
        with self.assertRaisesRegex(ValueError, "budget"):
            self.check_trace(self._trace(values), peak=212_000)
        values[0] = 181_000
        with self.assertRaisesRegex(ValueError, "peak"):
            self.check_trace(self._trace(values))

    def test_ten_percent_boundary_is_not_relaxed(self):
        self.check_trace(self._trace([100_000] * 50 + [110_000] * 50))
        with self.assertRaisesRegex(ValueError, "10%"):
            self.check_trace(self._trace([100_000] * 50 + [110_001] * 50))

    def test_missing_phase_gaps_nan_and_reordered_data_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "coverage"):
            self.check_trace(self._trace([100_000] * 100, []))
        for old, new in [("transfer", "unknown"), (",0.1,", ",nan,"),
                         (",0.1,", ",100.1,"), (",1234,", ",5678,")]:
            with self.subTest(new=new):
                trace = self._trace([100_000] * 100)
                trace.write_text(trace.read_text().replace(old, new, 1))
                with self.assertRaises(ValueError):
                    self.check_trace(trace)


class HdfsMemoryBudgetTests(_TraceFixture):
    def test_real_runner_wires_the_exact_scale_pair(self) -> None:
        runner = (Path(__file__).parent / "lab" / "run-hdfs-memory-e2e.sh").read_text()
        self.assertIn("scale_small_size=$((1024 * 1024 * 1024 + 137))", runner)
        self.assertIn("scale_large_size=$((100 * 1024 * 1024 * 1024 + 137))", runner)
        self.assertIn("run_case scale high hdfs-hdfs scale-1g", runner)
        self.assertNotIn("for scale_attempt", runner)
        self.assertIn("tests/hdfs_memory_sampler.py", runner)
        self.assertIn('--trace "$trace_output"', runner)
        self.assertIn("--report-phases", runner)
        self.assertIn("run_case scale high hdfs-hdfs scale-100g", runner)
        self.assertIn("--require-100-gib", runner)
        self.assertIn("resuming durable partial", runner)
        root = Path(__file__).resolve().parents[1]
        for workflow in ["nightly.yml", "release-validation.yml"]:
            with self.subTest(workflow=workflow):
                self.assertIn("hdfs-memory.csv.samples.csv",
                              (root / ".github" / "workflows" / workflow).read_text())

    def test_budget_is_derived_from_windows_chunks_and_file_concurrency(self) -> None:
        self.assertEqual(budget_mib(1, 2, 1, 1), 132)
        self.assertEqual(budget_mib(1, 2, 4, 1), 150)
        self.assertEqual(budget_mib(1, 2, 8, 16), 204)
        self.assertEqual(budget_mib(2, 2, 8, 16), 296)

    def test_validator_accepts_bounded_two_size_samples(self) -> None:
        path = self._csv([(262_144, 70_000), (2_097_152, 75_000)])
        validate(path)

        allocator_variance = self._csv(
            [(262_144, 113_260), (2_097_152, 179_272)],
            profile="high",
            read=8,
            write=16,
            budget=204,
        )
        validate(allocator_variance)

    def test_high_profile_headroom_keeps_an_exact_absolute_limit(self) -> None:
        for peak in [196_460, 204 * 1024, 204 * 1024 + 1]:
            with self.subTest(peak=peak):
                path = self._csv(
                    [(262_144, 160_000), (2_097_152, peak)],
                    profile="high", read=8, write=16, budget=204,
                )
                if peak <= 204 * 1024:
                    validate(path)
                else:
                    with self.assertRaisesRegex(ValueError, "exceeds budget"):
                        validate(path)

    def test_validator_rejects_absolute_budget_violations(self) -> None:
        over_budget = self._csv([(262_144, 140_000), (2_097_152, 140_000)])
        with self.assertRaisesRegex(ValueError, "exceeds budget"):
            validate(over_budget)

    def test_baseline_independent_peaks_are_not_sustained_growth(self) -> None:
        # Nightly #191: both processes satisfy the unchanged 204 MiB budget.
        # Their high-water marks are not two windows of one process's lifetime.
        for direction in ["local-hdfs", "hdfs-hdfs", "hdfs-local"]:
            with self.subTest(direction=direction):
                path = self._csv(
                    [(256 * 1024**2 + 137, 73_408),
                     (2 * 1024**3 + 137, 160_764)],
                    profile="high", read=8, write=16, budget=204,
                    direction=direction,
                )
                validate(path)

    def test_baseline_still_requires_distinct_sizes_and_identical_settings(self) -> None:
        same_size = self._csv([(262_144, 70_000), (262_144, 75_000)])
        with self.assertRaisesRegex(ValueError, "distinct sizes"):
            validate(same_size)
        mismatched = self._csv([(262_144, 70_000), (2_097_152, 75_000)])
        with mismatched.open(newline="") as stream:
            rows = list(csv.DictReader(stream))
        rows[1].update(read_inflight=4, budget_mib=150)
        with mismatched.open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        with self.assertRaisesRegex(ValueError, "identical settings"):
            validate(mismatched)

    def test_validator_requires_one_real_100_gib_sample_when_requested(self) -> None:
        hundred_gib = 100 * 1024 * 1024 * 1024 + 137
        path = self._csv(
            [(1024**3 + 137, 75_000), (hundred_gib, 76_000)],
            profile="high",
            read=8,
            write=16,
            budget=204,
            direction="hdfs-hdfs",
            sample_set="scale",
        )
        self._append_complete_baseline(path)
        trace = self._trace([75_000] * 100)
        validate(path, require_100_gib=True, trace=trace)
        with self.assertRaisesRegex(ValueError, "trace"):
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
            [(1024**3 + 137, 75_000), (hundred_gib, 170_000)],
            profile="high",
            read=8,
            write=16,
            budget=204,
            direction="hdfs-hdfs",
            sample_set="scale",
        )
        self._append_complete_baseline(unstable)
        with self.assertRaisesRegex(ValueError, "10%"):
            validate(unstable, require_100_gib=True,
                     trace=self._trace([100_000 + i * 600 for i in range(100)]))

    def test_cross_process_peaks_are_not_used_as_a_growth_estimator(self) -> None:
        hundred_gib = 100 * 1024 * 1024 * 1024 + 137
        short = 1024**3 + 137
        path = self._csv(
            [
                (short, 75_000),
                (hundred_gib, 132_064),
            ],
            profile="high",
            read=8,
            write=16,
            budget=204,
            direction="hdfs-hdfs",
            sample_set="scale",
        )
        self._append_complete_baseline(path)

        validate(path, require_100_gib=True, trace=self._trace([130_000] * 100))

    def test_full_gate_rejects_missing_baseline_coverage(self) -> None:
        path = self._csv(
            [(1024**3 + 137, 75_000),
             (100 * 1024**3 + 137, 132_064)],
            profile="high", read=8, write=16, budget=204,
            direction="hdfs-hdfs", sample_set="scale",
        )
        with self.assertRaisesRegex(ValueError, "baseline coverage"):
            validate(path, require_100_gib=True,
                     trace=self._trace([130_000] * 100))

    def _append_complete_baseline(self, path: Path) -> None:
        profiles = {
            "serial": (1, 1, 132),
            "default": (4, 1, 150),
            "high": (8, 16, 204),
        }
        with path.open("a", newline="") as stream:
            writer = csv.DictWriter(
                stream,
                fieldnames=[
                    "run_id", "commit", "profile", "sample_set", "direction",
                    "bytes", "max_rss_kib", "budget_mib", "file_concurrency",
                    "chunk_mib", "read_inflight", "write_inflight",
                ],
            )
            for profile, (read, write, budget) in profiles.items():
                for direction in ["local-hdfs", "hdfs-hdfs", "hdfs-local"]:
                    for size in [256 * 1024**2 + 137, 2 * 1024**3 + 137]:
                        writer.writerow({
                            "run_id": "unit-test", "commit": "0" * 40,
                            "profile": profile, "sample_set": "baseline",
                            "direction": direction, "bytes": size,
                            "max_rss_kib": 70_000, "budget_mib": budget,
                            "file_concurrency": 1, "chunk_mib": 2,
                            "read_inflight": read, "write_inflight": write,
                        })

    def _csv(
        self,
        samples: list[tuple[int, int]],
        *,
        profile: str = "serial",
        read: int = 1,
        write: int = 1,
        budget: int = 132,
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
