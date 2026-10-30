"""Ensure the real directed matrix cannot silently miss a backend pair."""
import importlib.util
import contextlib
import io
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("cross_qos_matrix", Path(__file__).parent / "lab/cross_qos_matrix.py")
MATRIX = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MATRIX)
EVIDENCE_SPEC = importlib.util.spec_from_file_location("qos_evidence", Path(__file__).parent / "lab/qos_evidence.py")
EVIDENCE = importlib.util.module_from_spec(EVIDENCE_SPEC)
EVIDENCE_SPEC.loader.exec_module(EVIDENCE)


def config():
    return {"sources": {kind: {"env": {"QOS_SOURCE": f"source/{kind}"}} for kind in MATRIX.SOURCES},
            "destinations": {kind: {"endpoint": f"destination/{kind}/{{pair}}"} for kind in MATRIX.FAMILIES}}


class CrossQosMatrix(unittest.TestCase):
    def test_strict_timing_distinguishes_paced_reads_from_publication_tail(self):
        result = {"bytes": 128 * 1024 * 1024, "operations": 16, "seconds": 16.52,
                  "admissions": [{"seconds": float(i), "bytes": 8 * 1024 * 1024}
                                 for i in range(1, 17)]}
        timing = EVIDENCE.strict_timing(result, "iops", 1)
        self.assertFalse(timing["end_to_end_passed"])
        self.assertAlmostEqual(timing["active_admission_ratio"], 1)
        self.assertAlmostEqual(timing["post_admission_seconds"], .52)
        self.assertAlmostEqual(timing["extra_seconds_allowed"], 16 / .98 - 16)
        self.assertTrue(timing["tail_exceeds_entire_extra_budget"])
        self.assertAlmostEqual(timing["ideal_seconds_needed_for_observed_tail"], 25.48)
        bandwidth = EVIDENCE.strict_timing(result, "bandwidth", 8 * 1024 * 1024)
        self.assertEqual(bandwidth, timing)

    def test_strict_timing_does_not_hide_slow_admission_or_change_the_verdict(self):
        result = {"bytes": 16, "operations": 16, "seconds": 20.1,
                  "admissions": [{"seconds": i * 1.25, "bytes": 1}
                                 for i in range(1, 17)]}
        timing = EVIDENCE.strict_timing(result, "iops", 1)
        self.assertFalse(timing["end_to_end_passed"])
        self.assertAlmostEqual(timing["active_admission_ratio"], .8)
        self.assertFalse(timing["tail_exceeds_entire_extra_budget"])
        result["seconds"] = 16.1
        result["admissions"] = [{"seconds": float(i), "bytes": 1} for i in range(1, 17)]
        self.assertTrue(EVIDENCE.strict_timing(result, "iops", 1)["end_to_end_passed"])

    def test_strict_timing_rejects_incomplete_or_invalid_trace(self):
        result = {"bytes": 2, "operations": 2, "seconds": 2.1,
                  "admissions": [{"seconds": 1., "bytes": 1}, {"seconds": 2., "bytes": 1}]}
        for change in ({"operations": 3}, {"bytes": 3}, {"seconds": 1.5},
                       {"admissions": []}, {"admissions": list(reversed(result["admissions"]))}):
            with self.assertRaises(ValueError):
                EVIDENCE.strict_timing({**result, **change}, "iops", 1)

    def test_noisy_aa_control_cannot_qualify_or_be_attributed_to_qos(self):
        result = EVIDENCE.compare_performance([40, 40, 40], [35, 35, 35], [35, 35, 35])
        self.assertFalse(result["passed"])
        self.assertFalse(result["control_repeatable"])
        self.assertEqual(result["classification"], "environment_not_repeatable")
        self.assertAlmostEqual(result["ratio"], .875)
        # A favorable QoS point estimate also cannot validate a noisy environment.
        result = EVIDENCE.compare_performance([40, 40, 40], [42, 42, 42], [35, 35, 35])
        self.assertTrue(result["passed"])
        self.assertFalse(result["qualified"])

    def test_stable_control_preserves_the_98_percent_failure(self):
        result = EVIDENCE.compare_performance([40, 40, 40], [39, 39, 39], [40, 40, 40])
        self.assertTrue(result["control_repeatable"])
        self.assertEqual(result["classification"], "below_98_percent")
        self.assertFalse(result["qualified"])
        result = EVIDENCE.compare_performance([40, 40, 40], [39.6, 39.6, 39.6], [40, 40, 40])
        self.assertTrue(result["qualified"])

    def test_matching_aa_medians_do_not_hide_insufficient_precision(self):
        baseline = [30, 40, 30, 40, 30, 40]
        control = [40, 30, 40, 30, 40, 30]
        result = EVIDENCE.compare_performance(baseline, baseline, control)
        self.assertEqual(result["control_ratio"], 1)
        self.assertFalse(result["control_repeatable"])
        self.assertFalse(result["qualified"])
        self.assertEqual(result, EVIDENCE.compare_performance(baseline, baseline, control))

    def test_isolated_sample_endpoint_preserves_authentication_and_nfs_options(self):
        original = "nfs://server/export:/ci/nightly-test/destination?version=4.1&uid=0"
        self.assertEqual(EVIDENCE.sample_destination(original, "session-high-0"),
                         "nfs://server/export:/ci/nightly-test/destination/qos-samples/session-high-0?version=4.1&uid=0")
        self.assertEqual(EVIDENCE.sample_destination("s3://user:p%40ss@bucket.host/prefix", "a"),
                         "s3://user:p%40ss@bucket.host/prefix/qos-samples/a")
        self.assertEqual(EVIDENCE.sample_destination("smb:nightly-test/destination", "a"),
                         "smb:nightly-test/destination/qos-samples/a")
        self.assertEqual(EVIDENCE.sample_destination("/tmp/nightly-test/destination", "a"),
                         "/tmp/nightly-test/destination/qos-samples/a")
        for invalid in ["../source", "a/b", "..", "", "a?version=3"]:
            with self.assertRaises(ValueError):
                EVIDENCE.sample_destination(original, invalid)

    def test_import_binds_control_and_destination_lifecycle(self):
        measured = {"configuration_sha256": "config", "binary_sha256": "binary",
                    "repetitions": 6, "performance_idle_ms": 1000,
                    "performance_control": True, "isolated_destinations": True}
        EVIDENCE.verify_baseline_identity(measured, "config", "binary", 6, 1000,
                                          performance_control=True, isolated_destinations=True)
        for control, isolated in [(False, True), (True, False)]:
            with self.assertRaises(ValueError):
                EVIDENCE.verify_baseline_identity(measured, "config", "binary", 6, 1000,
                                                  performance_control=control, isolated_destinations=isolated)

    def test_import_cannot_change_inherited_s3_write_concurrency(self):
        measured = {"configuration_sha256": "config", "binary_sha256": "binary", "repetitions": 6,
                    "runtime_concurrency": {"DATA_MOVER_S3_WRITE_INFLIGHT": "4"}}
        EVIDENCE.verify_baseline_identity(measured, "config", "binary", 6, 0,
                                          runtime_concurrency={"DATA_MOVER_S3_WRITE_INFLIGHT": "4"})
        with self.assertRaisesRegex(ValueError, "concurrency"):
            EVIDENCE.verify_baseline_identity(measured, "config", "binary", 6, 0,
                                              runtime_concurrency={"DATA_MOVER_S3_WRITE_INFLIGHT": "8"})
        self.assertEqual(EVIDENCE.concurrency_environment({
            "DATA_MOVER_S3_WRITE_INFLIGHT": "8", "DATA_MOVER_READ_INFLIGHT": "4",
            "S3_SK": "secret", "OTHER": "value"}),
            {"DATA_MOVER_READ_INFLIGHT": "4", "DATA_MOVER_S3_WRITE_INFLIGHT": "8"})

    def test_sustained_limit_duration_is_bound_to_imported_baseline(self):
        measured = {"configuration_sha256": "config", "binary_sha256": "binary", "repetitions": 6,
                    "limit_duration_seconds": 64}
        EVIDENCE.verify_baseline_identity(measured, "config", "binary", 6, 0,
                                          limit_duration_seconds=64)
        with self.assertRaisesRegex(ValueError, "duration"):
            EVIDENCE.verify_baseline_identity(measured, "config", "binary", 6, 0,
                                              limit_duration_seconds=16)

    def test_isolation_rejects_an_endpoint_that_cannot_be_safely_cleaned(self):
        EVIDENCE.verify_isolated_destination("s3://user:pass@bucket.host/ci/nightly-test/destination", "nightly-test")
        for endpoint, run_id in [("s3://user:pass@bucket.host/ci/another-run", "nightly-test"),
                                  ("/tmp/nightly-test-other", "nightly-test"),
                                  ("/tmp/nightly-test", "nightly-../test")]:
            with self.assertRaises(ValueError):
                EVIDENCE.verify_isolated_destination(endpoint, run_id)

    def test_isolation_rejects_parent_paths_and_credentials_disguised_as_paths(self):
        for endpoint in ["/tmp/nightly-test/../shared", "/tmp/nightly-test/../../",
                         "s3://user:pass@bucket.host/ci/nightly-test/%2e%2e/shared",
                         "nfs://host/export:/ci/nightly-test/%2E%2E%2fshared",
                         "smb:nightly-test/../shared", "s3@bucket:nightly-test/../shared",
                         "s3://user:pass/nightly-test/x@bucket.host/shared",
                         "s3://user:pass@bucket.host?prefix=/nightly-test/",
                         "file://host/nightly-test/link", "NFS://host/nightly-test/link",
                         "/tmp/x://host/nightly-test"]:
            with self.subTest(endpoint=endpoint), self.assertRaises(ValueError):
                EVIDENCE.verify_isolated_destination(endpoint, "nightly-test")
        EVIDENCE.verify_isolated_destination(
            "s3://user:pass/word@bucket.host/ci/nightly-test/destination", "nightly-test")

    def test_isolation_checks_the_actual_local_root_and_preserves_a_sibling(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            shared = root / "shared"
            shared.mkdir()
            sentinel = shared / "sentinel"
            sentinel.write_bytes(b"keep")
            run = root / "nightly-test"
            run.symlink_to(shared, target_is_directory=True)
            with self.assertRaises(ValueError):
                EVIDENCE.verify_isolated_destination(str(run), "nightly-test")
            run.unlink()
            run.mkdir()
            (run / "destination").symlink_to(shared, target_is_directory=True)
            with self.assertRaises(ValueError):
                EVIDENCE.verify_isolated_destination(str(run / "destination"), "nightly-test")
            EVIDENCE.verify_isolated_destination(str(run / "fresh-destination"), "nightly-test")
            self.assertEqual(sentinel.read_bytes(), b"keep")

    def test_invalid_profile_selection_fails_before_creating_any_fixture(self):
        lab = Path(__file__).parent / "lab"
        with patch.object(sys, "path", [str(lab), *sys.path]):
            spec = importlib.util.spec_from_file_location("profile_validation_runner", lab / "run-backend-qos.py")
            runner = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(runner)
        local = {"name": "local", "env": {}}
        invalid = [([], None), ([local], ["typo"]), ([local, local], None),
                   ([local], []), ([local], ["local", "local"]),
                   ([{"name": "../outside", "env": {}}], None)]
        for profiles, requested in invalid:
            with self.subTest(profiles=profiles, requested=requested), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                config_file = root / "config.json"
                config_file.write_text(json.dumps({"profiles": profiles}))
                argv = ["runner", "--config", str(config_file), "--root", str(root / "work"),
                        "--binary", str(root / "binary"), "--phase", "final"]
                if requested is not None:
                    argv += ["--profiles", *requested]
                with patch.object(sys, "argv", argv), patch.object(runner.VALIDATOR, "fixture") as fixture, \
                        patch.object(runner.subprocess, "run") as invoke, contextlib.redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit) as result:
                        runner.main()
                self.assertEqual(result.exception.code, 2)
                fixture.assert_not_called()
                invoke.assert_not_called()
                self.assertFalse((root / "work").exists())

    def test_profile_selection_preserves_only_the_requested_configured_workloads(self):
        profiles = [{"name": "local"}, {"name": "nfs4.1"}]
        self.assertEqual(EVIDENCE.select_profiles(profiles, None), profiles)
        self.assertEqual(EVIDENCE.select_profiles(profiles, ["nfs4.1"]), profiles[1:])

    def test_runner_defers_exact_sample_cleanup_and_keeps_aa_arms_unshaped(self):
        self.check_runner_workload(128, 16)

    def test_fresh_final_phase_seeds_the_source_before_measuring(self):
        lab = Path(__file__).parent / "lab"
        with patch.object(sys, "path", [str(lab), *sys.path]):
            spec = importlib.util.spec_from_file_location("fresh_final_runner", lab / "run-backend-qos.py")
            runner = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(runner)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            binary = root / "binary"
            binary.write_bytes(b"acceptance")
            config_file = root / "config.json"
            config_file.write_text(json.dumps({"profiles": [{
                "name": "local", "size_mib": 512, "env": {"QOS_SOURCE": str(root / "source")}}]}))
            invocations = []

            def invoke(command, **kwargs):
                invocations.append(command)
                if "--seed" in command:
                    return SimpleNamespace(returncode=0, stdout='{"seeded":true}', stderr="")
                raise RuntimeError("stop after observing the first timed invocation")

            argv = ["runner", "--config", str(config_file), "--root", str(root),
                    "--binary", str(binary), "--phase", "final", "--retain-source"]
            with patch.object(sys, "argv", argv), patch.object(runner.subprocess, "run", side_effect=invoke), \
                    patch.object(runner.platform, "platform", return_value="test-platform"), \
                    patch.object(runner.VALIDATOR, "fixture", side_effect=lambda path, size: path.touch()), \
                    contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(runner.main(), 1)
            self.assertGreaterEqual(len(invocations), 2)
            self.assertIn("--seed", invocations[0])
            self.assertNotIn("--seed", invocations[1])

    def test_sustained_fixture_keeps_the_original_iops_instead_of_a_16_second_run(self):
        self.check_runner_workload(512, 64)
        self.check_runner_workload(1024, 128)

    def check_runner_workload(self, size_mib, duration):
        lab = Path(__file__).parent / "lab"
        with patch.object(sys, "path", [str(lab), *sys.path]):
            spec = importlib.util.spec_from_file_location("backend_qos_runner", lab / "run-backend-qos.py")
            runner = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(runner)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "nightly-test"
            root.mkdir()
            binary, cleanup = root / "binary", root / "cleanup"
            binary.write_bytes(b"acceptance")
            cleanup.write_bytes(b"cleanup")
            config_file = root / "config.json"
            config_file.write_text(json.dumps({"run_id": "nightly-test", "profiles": [
                {"name": "local-to-s3", "source_kind": "local", "size_mib": size_mib,
                 "performance_size_mib": 512, "env": {
                    "QOS_SOURCE": str(root / "source"),
                    "QOS_DESTINATION": "s3://user:pass@bucket.host/ci/nightly-test/destination"}}]}))
            copies, removed = [], []

            def invoke(command, *, env, **kwargs):
                if command[0] == str(cleanup):
                    self.assertEqual(len(copies), 9, "cleanup must follow every timed copy")
                    self.assertIn(env["QOS_SOURCE"], [item[0] for item in copies])
                    self.assertNotIn(env["QOS_SOURCE"], removed)
                    removed.append(env["QOS_SOURCE"])
                    return SimpleNamespace(returncode=0, stdout='{"cleaned":true}', stderr="")
                fixture = Path(command[command.index("--source") + 1]) / command[command.index("--path") + 1]
                self.assertTrue(fixture.is_file(), "configured performance fixture must exist before seeding")
                if "--seed" in command:
                    return SimpleNamespace(returncode=0, stdout='{"seeded":true}', stderr="")
                target = env["QOS_DESTINATION"]
                self.assertNotIn(target, [item[0] for item in copies])
                self.assertEqual(command[command.index("--destination-cleanup") + 1], "keep")
                high = "--soft-bytes" in command
                self.assertEqual(high, "-high-" in target)
                copies.append((target, high))
                return SimpleNamespace(returncode=0, stderr="", stdout=json.dumps({
                    "bytes": 512 * 1024 ** 2, "seconds": 512 / (99 if high else 100), "mibps": 99 if high else 100,
                    "iops": 49.5 if high else 50, "operations": 256, "effective_read_bytes": 2 * 1024 ** 2,
                    "admissions": [], "digest": "full-hash"}))

            argv = ["runner", "--config", str(config_file), "--root", str(root), "--binary", str(binary),
                    "--phase", "survey", "--repeats", "3", "--retain-source",
                    "--limit-duration-seconds", str(duration),
                    "--performance-control", "--isolated-destinations", "--cleanup-binary", str(cleanup)]
            output = io.StringIO()
            with patch.object(sys, "argv", argv), patch.object(runner.subprocess, "run", side_effect=invoke), \
                    patch.object(runner.platform, "platform", return_value="test-platform"), \
                    patch.object(runner.VALIDATOR, "fixture", side_effect=lambda path, size: path.touch()), \
                    contextlib.redirect_stdout(output):
                status = runner.main()
            self.assertEqual(status, 0, output.getvalue())
            self.assertEqual(len(removed), 9)
            self.assertEqual(sum(high for _, high in copies), 3)
            measurements = json.loads((root / "survey/local-to-s3/control-0.json").read_text())
            self.assertEqual(measurements["destination_cleanup"], "deferred")
            self.assertNotIn("--soft-bytes", measurements["command"])
            summary = json.loads((root / "survey/summary.json").read_text())
            self.assertEqual(summary["local-to-s3"]["soft_iops"], 4)
            environment = json.loads((root / "survey/environment.json").read_text())
            self.assertEqual(environment["limit_duration_seconds"], duration)

    def test_baseline_import_cannot_select_fewer_repeats_or_change_idle(self):
        measured = {"configuration_sha256": "config", "binary_sha256": "binary",
                    "repetitions": 15, "performance_idle_ms": 1000}
        EVIDENCE.verify_baseline_identity(measured, "config", "binary", 15, 1000)
        for arguments in [("other", "binary", 15, 1000), ("config", "other", 15, 1000),
                          ("config", "binary", 5, 1000), ("config", "binary", 15, 0)]:
            with self.assertRaises(ValueError):
                EVIDENCE.verify_baseline_identity(measured, *arguments)

    def test_url_credentials_and_encoded_passwords_are_redacted(self):
        secrets = EVIDENCE.secret_values([{"QOS_DESTINATION": "s3://user:p%40ss@example/bucket",
                                           "S3_SK": "secret-key"}])
        text = EVIDENCE.redact("s3://user:p%40ss@example/bucket p@ss secret-key", secrets)
        self.assertEqual(text, "s3://<REDACTED>:<REDACTED>@example/bucket <REDACTED> <REDACTED>")

    def test_all_twenty_directed_cross_family_pairs_and_seven_regressions(self):
        rows = MATRIX.profiles(config())
        pairs = {(row["source_kind"], row["destination_kind"]) for row in rows}
        self.assertEqual(len(rows), 23)
        self.assertEqual(len(pairs), 23)
        expected = {(source, destination) for source in MATRIX.FAMILIES
                    for destination in MATRIX.FAMILIES if source != destination}
        self.assertTrue(expected <= pairs)
        self.assertTrue({(source, "local") for source in MATRIX.SOURCES} <= pairs)
        self.assertNotIn(("s3", "s3"), pairs)  # native server-side bytes are unshaped

    def test_source_family_is_not_inferred_from_route_label(self):
        rows = MATRIX.profiles(config())
        row = next(row for row in rows if row["name"] == "s3-to-cifs")
        self.assertEqual(row["source_kind"], "s3")
        self.assertEqual(row["chunk_bytes"], 8 * 1024 * 1024)
        self.assertEqual(row["env"]["QOS_DESTINATION"], "destination/cifs/s3-to-cifs")
        self.assertEqual(row["transfer_profile"], "cross")

    def test_larger_fixture_override_cannot_change_the_route_or_finite_limits(self):
        original = MATRIX.profiles(config())[1]
        revised = {**original, "performance_size_mib": 512}
        MATRIX.validate_override(original, revised)
        with self.assertRaisesRegex(ValueError, "changed the route"):
            MATRIX.validate_override(original, {**revised, "env": {"QOS_SOURCE": "different-source"}})
        with self.assertRaisesRegex(ValueError, "changed the route"):
            MATRIX.validate_override(original, {**revised, "soft_mibps": 1})

    def test_conflicting_process_global_credentials_are_rejected(self):
        data = config()
        data["sources"]["s3"]["env"]["S3_HOST"] = "source-server"
        data["destinations"]["hdfs"]["env"] = {"S3_HOST": "different-server"}
        with self.assertRaisesRegex(ValueError, "conflicting endpoint environment"):
            MATRIX.profiles(data)


if __name__ == "__main__":
    unittest.main()
