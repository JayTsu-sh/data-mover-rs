"""Run the actual lab state guards against interruption and resume evidence."""
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class ResumeLabContract(unittest.TestCase):
    def run_guard(self, marker, state):
        script = (ROOT / "tests/lab/run-resume-e2e.sh").read_text()
        start = script.index(marker)
        guard = script[start:].split("\n", 1)[1].split("\nPY", 1)[0]
        with tempfile.TemporaryDirectory() as directory:
            state_file = Path(directory) / "state.json"
            state_file.write_text(json.dumps(state))
            return subprocess.run(
                ["python3", "-", str(state_file)], input=guard,
                text=True, capture_output=True,
            )

    def interrupt_state(self, streamed=None):
        size = 96 * 1024 * 1024 + 123
        return {
            "schema": 2, "size": size, "identity": "transfer-identity",
            "source_observation": "a" * 64, "recovery_at_destination": True,
            "source_streamed_bytes": size if streamed is None else streamed,
            "reused_bytes": None,
        }

    def test_accepts_recoverable_interruption_after_source_finished(self):
        # The source can finish while destination writes/publication are pending.
        # Its byte count does not establish the receiver's reusable byte count.
        for streamed in (64 * 1024 * 1024, None):
            with self.subTest(streamed=streamed):
                result = self.run_guard('if python3 - "$state_file"',
                                        self.interrupt_state(streamed))
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_rejects_invalid_interruption_evidence(self):
        for change in ({"source_streamed_bytes": 0},
                       {"source_streamed_bytes": 96 * 1024 * 1024 + 124},
                       {"recovery_at_destination": False}, {"reused_bytes": 1},
                       {"schema": 1}, {"source_observation": ""}):
            with self.subTest(change=change):
                state = {**self.interrupt_state(), **change}
                self.assertNotEqual(self.run_guard('if python3 - "$state_file"',
                                                   state).returncode, 0)

    def test_resume_still_requires_partial_destination_reuse(self):
        state = self.interrupt_state()
        for reused, valid in ((0, False), (64 * 1024 * 1024, True),
                              (state["size"], False)):
            with self.subTest(reused=reused):
                state["reused_bytes"] = reused
                result = self.run_guard('python3 - "$local_root/resume-', state)
                self.assertEqual(result.returncode == 0, valid, result.stderr)


if __name__ == "__main__":
    unittest.main()
