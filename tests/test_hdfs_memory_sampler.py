"""Exercise the real process sampler without Cargo, HDFS or root access."""

import csv
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SAMPLER = Path(__file__).with_name("hdfs_memory_sampler.py")


class MemorySamplerTests(unittest.TestCase):
    def run_child(self, script, *options):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        result = subprocess.run(
            [sys.executable, str(SAMPLER), "--trace", str(root / "trace.csv"),
             "--metrics", str(root / "metrics"), "--log", str(root / "log"),
             "--run-id", "sampler-test", "--commit", "a" * 40, *options,
             "--", sys.executable, "-c", script],
            capture_output=True, text=True, timeout=25,
        )
        return result, root

    def test_samples_only_child_rss_and_keeps_phase_and_identity(self):
        result, root = self.run_child(
            "import time; print('memory_phase\\ttransfer', flush=True); "
            "payload=bytearray(24*1024*1024); time.sleep(.3); "
            "print('memory_phase\\tverification', flush=True); time.sleep(.3)"
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        with (root / "trace.csv").open() as stream:
            rows = list(csv.DictReader(stream))
        self.assertTrue({"transfer", "verification"} <= {row["phase"] for row in rows})
        self.assertTrue(all(row["run_id"] == "sampler-test" for row in rows))
        self.assertTrue(all(row["commit"] == "a" * 40 for row in rows))
        elapsed, peak = (root / "metrics").read_text().strip().split(",")
        self.assertGreaterEqual(float(elapsed), .6)
        self.assertGreater(int(peak), 24 * 1024)
        self.assertGreaterEqual(int(peak), max(int(row["rss_kib"]) for row in rows))

    def test_failure_and_timeout_never_emit_success_metrics(self):
        for script, options in [("raise SystemExit(7)", ()),
                                ("import time; time.sleep(30)", ("--timeout", ".2"))]:
            with self.subTest(script=script):
                result, root = self.run_child(script, *options)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse((root / "metrics").exists())
                self.assertTrue((root / "trace.csv").exists())

    def test_real_retained_allocations_are_detected_below_the_absolute_budget(self):
        result, root = self.run_child(
            "import time\n"
            "print('memory_phase\\ttransfer', flush=True)\n"
            "retained = [bytearray(12*1024*1024)]\n"
            "for _ in range(70):\n"
            "    retained.append(bytearray(256*1024)); time.sleep(.1)\n"
            "print('memory_phase\\tverification', flush=True)\n"
            "time.sleep(7)\n"
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        peak = int((root / "metrics").read_text().split(",")[1])
        from hdfs_memory_budget import validate_trace
        with self.assertRaisesRegex(ValueError, "transfer steady-state RSS.*10%"):
            validate_trace(root / "trace.csv", run_id="sampler-test", commit="a" * 40,
                           peak_kib=peak, budget_kib=204 * 1024)


if __name__ == "__main__":
    unittest.main()
