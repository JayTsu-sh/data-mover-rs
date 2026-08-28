#!/usr/bin/env python3
"""Behavioral contract for the reproducible performance-baseline report."""

import csv
import json
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPORTER = ROOT / "tests" / "lab" / "performance_baseline.py"
RUNNER = ROOT / "tests" / "lab" / "run-performance-baseline.sh"
NIGHTLY = ROOT / ".github" / "workflows" / "nightly.yml"


class PerformanceBaselineTest(unittest.TestCase):
    def test_runner_fixes_conditions_and_exercises_public_scan_and_copy(self):
        runner = RUNNER.read_text()
        self.assertIn("PERF_HARDWARE_ID", runner)
        self.assertIn("chunk_bytes=$((2 * 1024 * 1024))", runner)
        self.assertIn("inflight=8", runner)
        self.assertIn("--operation \"$operation\"", runner)
        self.assertIn("run-inflight-benchmark.sh", runner)
        self.assertIn("performance_baseline.py summarize", runner)

    def test_nightly_signs_off_lab_runner_and_always_retains_evidence(self):
        workflow = NIGHTLY.read_text()
        self.assertEqual(workflow.count("uses: actions/attest@v4"), 1)
        upload = workflow.index("- name: Upload performance baseline")
        self.assertIn("if: always()", workflow[upload : upload + 160])
        signing_job = workflow.index("attest-performance-baseline:")
        signing = workflow[signing_job:]
        self.assertNotIn("id-token: write", workflow[:signing_job])
        self.assertIn("needs: storage-contracts", signing)
        self.assertIn("runs-on: ubuntu-latest", signing)
        self.assertIn("id-token: write", signing)
        self.assertIn("attestations: write", signing)
        self.assertIn("artifact-metadata: write", signing)
        self.assertIn("uses: actions/download-artifact@v8", signing)
        self.assertIn("uses: actions/attest@v4", signing)

    def write_samples(self, directory: Path, *, hardware_id: str = "fas2750-lab") -> Path:
        path = directory / "samples.csv"
        fields = [
            "schema_version",
            "run_id",
            "commit",
            "hardware_id",
            "dataset_id",
            "operation",
            "source",
            "destination",
            "concurrency",
            "chunk_bytes",
            "inflight",
            "repeat",
            "entries",
            "bytes",
            "elapsed_ms",
            "p95_scheduling_latency_ms",
            "max_rss_kib",
        ]
        prefix = [1, "nightly-42-1", "a" * 40, hardware_id, "data-mover-performance-v1"]
        suffix = [1, 2097152, 8]
        rows = []
        for source in ("local", "nfs3", "nfs41", "s3"):
            for destination in ("local", "nfs3", "nfs41", "s3"):
                for repeat in (1, 2):
                    rows.append(prefix + ["copy-large", source, destination] + suffix + [repeat, 1, 104857600, 1000, "", 65536])
        for repeat in range(1, 6):
            rows.append(prefix + ["copy-small", "local", "local"] + suffix + [repeat, 100, 102400, 100, 2, 66000])
            rows.append(prefix + ["scan-small", "local", ""] + suffix + [repeat, 100, 102400, 50, 1, 64000])
        with path.open("w", newline="") as output:
            writer = csv.writer(output)
            writer.writerow(fields)
            writer.writerows(rows)
        return path

    def run_reporter(self, samples: Path, output: Path) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["python3", str(REPORTER), "summarize", str(samples), str(output)],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )

    def test_summarizes_fixed_conditions_and_required_metrics(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            output = directory / "baseline.json"
            result = self.run_reporter(self.write_samples(directory), output)

            self.assertEqual(result.returncode, 0, result.stderr)
            report = json.loads(output.read_text())
            self.assertEqual(report["schema_version"], 1)
            self.assertEqual(report["conditions"]["hardware_id"], "fas2750-lab")
            self.assertEqual(report["conditions"]["chunk_bytes"], 2097152)
            copy = next(metric for metric in report["metrics"] if metric["operation"] == "copy-small")
            self.assertEqual(copy["entries_per_second"], 1000.0)
            self.assertEqual(copy["p95_scheduling_latency_ms"], 2.0)
            large = next(metric for metric in report["metrics"] if metric["operation"] == "copy-large")
            self.assertEqual(large["throughput_mib_per_second"], 100.0)
            self.assertEqual(large["peak_rss_kib"], 65536)

    def test_rejects_results_from_different_hardware(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            samples = self.write_samples(directory)
            with samples.open("a", newline="") as output:
                csv.writer(output).writerow(
                    [1, "nightly-42-1", "a" * 40, "other-host", "data-mover-performance-v1", "scan-small", "local", "", 1, 2097152, 8, 6, 1, 1, 1, 1, 1]
                )

            result = self.run_reporter(samples, directory / "baseline.json")

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("hardware_id", result.stderr)

    def test_rejects_incomplete_baseline(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            samples = self.write_samples(directory)
            with samples.open() as source:
                rows = list(csv.DictReader(source))
            with samples.open("w", newline="") as output:
                writer = csv.DictWriter(output, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(row for row in rows if row["operation"] != "scan-small")

            result = self.run_reporter(samples, directory / "baseline.json")

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("scan-small", result.stderr)


if __name__ == "__main__":
    unittest.main()
