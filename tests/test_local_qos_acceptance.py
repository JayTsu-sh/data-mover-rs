"""Check that the real-backend acceptance validator cannot hide overshoots."""
import importlib.util
from pathlib import Path
import unittest

RUNNER = Path(__file__).resolve().parent / "lab/run-local-qos.py"
SPEC = importlib.util.spec_from_file_location("local_qos_acceptance", RUNNER)
LAB = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(LAB)


class LocalQosValidator(unittest.TestCase):
    def test_hard_envelope_detects_a_burst_after_idle(self):
        events = [{"seconds": 10.0, "bytes": 10}, {"seconds": 10.001, "bytes": 10}]
        # The lifetime average is only 2 B/s; the short interval violates 100 B/s.
        self.assertAlmostEqual(LAB.envelope_excess(events, 100, "bandwidth"), 19.9)

    def test_one_discrete_packet_is_allowed_without_percentage_slack(self):
        events = [{"seconds": 0.1 * i, "bytes": 10} for i in range(1, 101)]
        self.assertAlmostEqual(LAB.envelope_excess(events, 100, "bandwidth"), 10)
        events.append({"seconds": 10.001, "bytes": 10})
        self.assertGreater(LAB.envelope_excess(events, 100, "bandwidth"), 10)

    def test_iops_counts_real_operations_instead_of_bytes(self):
        events = [{"seconds": 1.0, "bytes": 1048576},
                  {"seconds": 1.001, "bytes": 1}]
        self.assertAlmostEqual(LAB.envelope_excess(events, 100, "iops"), 1.9)

    def test_steady_rate_excludes_startup_burst(self):
        events = [{"seconds": i / 10, "bytes": 20 if i <= 50 else 9}
                  for i in range(1, 101)]
        self.assertGreater(sum(e["bytes"] for e in events) / 10, 100)
        self.assertEqual(LAB.trailing_rate({"events": events}, "bandwidth"), 90)

    def test_sustained_overshoot_exceeds_bounded_soft_credit(self):
        events = [{"seconds": i / 10, "bytes": 20} for i in range(1, 101)]
        result = LAB.validate_trace({"events": events}, "bandwidth", 100, 200, 0.5, True)
        self.assertTrue(result["hard_envelope"])
        self.assertFalse(result["soft_envelope"])
        self.assertFalse(result["steady_ceiling"])

    def test_admission_spacing_is_independent_of_read_completion_jitter(self):
        admissions = [{"seconds": 0.1, "bytes": 10}, {"seconds": 0.2, "bytes": 10}]
        completions = [{"seconds": 0.11, "bytes": 10}, {"seconds": 0.205, "bytes": 10}]
        self.assertGreater(LAB.envelope_excess(completions, 100, "bandwidth"), 10)
        result = LAB.validate_trace({"admissions": admissions, "events": completions},
                                    "bandwidth", 100, 100, 0.5, False)
        self.assertTrue(result["hard_envelope"])
        self.assertTrue(result["soft_envelope"])
        self.assertTrue(result["steady_at_least_98_percent"])


    def test_old_95_percent_pass_is_rejected_by_new_98_percent_floor(self):
        events = [{"seconds": i, "bytes": 97} for i in range(1, 12)]
        result = LAB.validate_trace({"events": events}, "bandwidth", 100, 200, 0.5, False)
        self.assertEqual(result["steady_rate"], 97)
        self.assertFalse(result["steady_at_least_98_percent"])

    def test_low_iops_large_requests_use_complete_service_intervals(self):
        events = [{"seconds": i * 2, "bytes": 8 * 1024 * 1024} for i in range(1, 9)]
        self.assertEqual(LAB.trailing_rate({"events": events}, "bandwidth"), 4 * 1024 * 1024)

    def test_short_fixture_cannot_claim_a_post_burst_steady_window(self):
        events = [{"seconds": i / 10, "bytes": 10} for i in range(1, 61)]
        result = LAB.validate_trace({"events": events}, "bandwidth", 100, 200, 2, True)
        self.assertFalse(result["steady_window_after_peak"])


if __name__ == "__main__":
    unittest.main()
