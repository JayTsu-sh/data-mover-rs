#!/usr/bin/env python3
"""Static safety contract for the destructive nightly HDFS fault harness."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
COMMON = (ROOT / "tests/lab/hdfs-fault-common.sh").read_text()
RUNNER = (ROOT / "tests/lab/run-hdfs-fault-e2e.sh").read_text()


def test_exact_lab_topology_is_validated_before_service_mutation() -> None:
    for expected in (
        "HDFS_NAMENODE_VMID=301",
        "HDFS_DATANODE1_VMID=302",
        "HDFS_DATANODE2_VMID=303",
        'grep -Fxq "name: $expected_name"',
        "ipconfig0:",
        "hostname",
    ):
        assert expected in COMMON
    assert 'validate_hdfs_fault_target "$target"' in COMMON


def test_harness_never_mutates_virtual_machines_or_unrelated_units() -> None:
    combined = COMMON + RUNNER
    for forbidden in ("qm stop", "qm shutdown", "qm reset", "pct stop", "poweroff"):
        assert forbidden not in combined
    assert "hadoop-namenode.service" in COMMON
    assert "hadoop-datanode.service" in COMMON
    assert "systemctl '$action' '$service'" in COMMON


def test_exit_path_restores_health_before_confined_cleanup() -> None:
    assert "trap 'cleanup_fault_run $?' EXIT" in RUNNER
    cleanup = RUNNER.index("cleanup_fault_run()")
    restore = RUNNER.index("restore_hdfs_services", cleanup)
    confined_delete = RUNNER.index("nightly_lab_cleanup_confined_run_root", cleanup)
    assert restore < confined_delete
    assert "wait_hdfs_cluster_ready" in COMMON
    assert "FSNamesystemState" in COMMON
    assert 'bean["FSState"]' in COMMON
    assert 'bean["NumLiveDataNodes"]' in COMMON
    assert "dfsadmin" not in COMMON


def test_failure_commands_must_finish_before_their_deadline() -> None:
    assert "status == 124" in RUNNER
    assert "did not return within" in RUNNER
    assert 'grep -Fq "$LAB_HDFS_RUN_ROOT"' in RUNNER


def test_mutation_checkpoints_are_killed_and_independently_inspected() -> None:
    for phase in ("create", "close", "append", "rename", "metadata"):
        assert f"kill_mutation_at_checkpoint {phase} " in RUNNER
    assert 'kill -STOP "$pid"' in RUNNER
    assert 'kill -KILL "$pid"' in RUNNER
    assert "status != 137" in RUNNER
    assert "wait_for_inspect append-killed.bin" in RUNNER


def test_replica_failover_read_uses_a_start_barrier_before_socket_detection() -> None:
    assert '--ready-file "$single_ready" --go-file "$single_go"' in RUNNER
    assert ': >"$single_go"' in RUNNER
    assert "ss -Hnt" in RUNNER
    assert 'grep -Fq "$LAB_HDFS_DATANODE1_DATA:9866"' in RUNNER
    assert "LAB_HDFS_DATANODE2_DATA" in RUNNER


def test_kerberos_checks_use_the_instance_credential_probe() -> None:
    assert "/opt/hadoop/bin/hdfs" not in RUNNER
    assert '--path close-killed.bin' in RUNNER
    assert '--path metadata-killed.bin --print-mode' in RUNNER


def test_cluster_recovery_is_followed_by_a_real_write_probe() -> None:
    assert "wait_for_hdfs_write()" in RUNNER
    assert '--path readiness.bin --phase seed' in RUNNER
    assert RUNNER.count("wait_for_hdfs_write") >= 4


if __name__ == "__main__":
    tests = [value for name, value in globals().copy().items() if name.startswith("test_")]
    for test in tests:
        test()
    print(f"{len(tests)} HDFS fault harness tests passed")
