import importlib.util
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location(
    "run_cargo_test", Path(__file__).with_name("run_cargo_test.py")
)
RUNNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNNER)


class CargoTestSelectionTests(unittest.TestCase):
    def test_missing_filter_never_reports_success(self):
        with patch.object(RUNNER.subprocess, "run", return_value=subprocess.CompletedProcess(
            [], 0, "0 tests, 0 benchmarks\n"
        )) as run:
            self.assertEqual(RUNNER.main(["missing", "--exact"]), 2)
            self.assertEqual(run.call_count, 1)

    def test_listing_failure_is_not_masked(self):
        with patch.object(RUNNER.subprocess, "run", return_value=subprocess.CompletedProcess(
            [], 101, ""
        )):
            self.assertEqual(RUNNER.main(["selected"]), 101)

    def test_an_ignored_only_selection_is_not_a_pass(self):
        with patch.object(RUNNER.subprocess, "run", side_effect=[
            subprocess.CompletedProcess([], 0, "selected: test\n"),
            subprocess.CompletedProcess([], 0, "test result: ok. 0 passed; 0 failed; 1 ignored;\n"),
        ]):
            self.assertEqual(RUNNER.main(["selected"]), 2)

    def test_live_selection_preserves_target_and_flags(self):
        with patch.object(RUNNER.subprocess, "run", side_effect=[
            subprocess.CompletedProcess([], 0, "selected: test\n"),
            subprocess.CompletedProcess([], 0, "test result: ok. 1 passed; 0 failed; 0 ignored;\n"),
        ]) as run:
            self.assertEqual(RUNNER.main([
                "selected", "--test", "contract", "--ignored", "--exact"
            ]), 0)
            listed = run.call_args_list[0].args[0]
            executed = run.call_args_list[1].args[0]
            self.assertEqual(listed[:-1], executed[:-1])
            self.assertIn("--locked", executed)
            self.assertEqual(executed[3:5], ["--test", "contract"])
            self.assertIn("--ignored", executed)
            self.assertIn("--exact", executed)


if __name__ == "__main__":
    unittest.main()
