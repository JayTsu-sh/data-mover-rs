"""Execute the lab's request-size setup without accessing any storage backend."""
import os
from pathlib import Path
import subprocess
import unittest

ROOT = Path(__file__).resolve().parents[1]


class QosLabContract(unittest.TestCase):
    def test_s3_keeps_normal_gets_and_other_sources_exercise_small_requests(self):
        source = (ROOT / "tests/lab/run-qos-e2e.sh").read_text()
        start = source.index("  # S3 keeps normal ranged GETs")
        setup = source[start:source.index("  if ! /usr/bin/time", start)]
        count = next(line for line in source.splitlines() if line.startswith("  expected_iops="))
        for backend, expected in [("local", "65536 257"), ("nfs3", "65536 257"),
                                  ("nfs41", "65536 257"), ("s3", "8388608 3"),
                                  ("hdfs", "65536 257")]:
            with self.subTest(backend=backend):
                result = subprocess.run(
                    ["bash", "-euc", setup + count + '\nprintf "%s %s" "$burst_bytes" "$expected_iops"'],
                    env={**os.environ, "backend": backend, "size": str(16 * 1024 * 1024 + 137)},
                    capture_output=True, text=True, check=True,
                )
                self.assertEqual(result.stdout, expected)


if __name__ == "__main__":
    unittest.main()
