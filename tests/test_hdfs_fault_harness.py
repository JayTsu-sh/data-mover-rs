#!/usr/bin/env python3
"""Static safety contract for the destructive nightly HDFS fault harness."""

import os
import shutil
from pathlib import Path
import subprocess
import tempfile


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


def test_pve_topology_check_uses_runner_scoped_ssh_credentials() -> None:
    assert 'ssh_lab_root "$LAB_HDFS_PVE_HOST"' in COMMON
    assert 'root@"$LAB_HDFS_PVE_HOST"' not in COMMON


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


def test_ha_restore_restarts_zkfc_and_returns_the_direct_namenode_to_active() -> None:
    assert "restore_hdfs_ha_primary()" in COMMON
    assert "systemctl reset-failed hadoop-zkfc.service" in COMMON
    assert "systemctl start hadoop-zkfc.service" in COMMON
    assert "-failover '$secondary_id' '$primary_id'" in COMMON
    assert "restore_hdfs_ha_primary || status=1" in COMMON
    assert "hdfs_service_action start namenode\nrestore_hdfs_services" in RUNNER


def test_ha_restore_waits_for_rpc_startup_and_confirms_the_active_state() -> None:
    # Run the actual remote administration script, including its shell error handling.
    start = COMMON.index("restore_hdfs_ha_primary() {")
    end = COMMON.index("\n}", start) + 2
    restore_template = COMMON[start:end]
    for mode in (
        "active", "startup", "failover", "unavailable", "stalled", "authentication", "topology",
    ):
        with tempfile.TemporaryDirectory(prefix="hdfs-ha-restore-") as directory:
            root = Path(directory)
            hdfs = root / "hdfs"
            hdfs.write_text(r'''#!/usr/bin/env bash
set -eu
case "$*" in
  *getconf*principal*) echo test-principal ;;
  *getconf*nameservices*) echo test-nameservice ;;
  *getServiceState*)
    echo query >>"$TRACE"
    if [[ "$MODE" == stalled ]]; then command sleep 5; fi
    if [[ "$MODE" == unavailable || ( "$MODE" == startup && ! -e "$RECOVERED" ) ]]; then
      touch "$RECOVERED"
      echo 'HA RPC connection refused during startup' >&2
      exit 1
    fi
    if [[ "$MODE" == failover && ! -e "$ACTIVE" ]]; then
      echo standby
    else
      echo active
    fi ;;
  *-failover*)
    echo failover >>"$TRACE"
    if [[ "$MODE" == startup ]]; then
      echo 'HA service RPC connection refused during startup' >&2
      exit 1
    fi
    if [[ ! -e "$RECOVERED" ]]; then
      touch "$RECOVERED"
      echo 'HA failover RPC not ready' >&2
      exit 1
    fi
    touch "$ACTIVE" ;;
  *) exit 2 ;;
esac
''')
            hdfs.chmod(0o755)
            # Keep the real outer timeout but shorten it in deadline tests.
            restore = restore_template.replace("/opt/hadoop/bin/hdfs", str(hdfs))
            script = r'''set -euo pipefail
LAB_HDFS_HA_NAMENODE1_HOST=primary LAB_HDFS_HA_NAMENODE2_HOST=secondary
LAB_HDFS_HA_NAMENODE1_ID=nn1 LAB_HDFS_HA_NAMENODE2_ID=nn2
LAB_HDFS_HA_NAMENODE1_NAME=hdfs-namenode LAB_HDFS_HA_NAMENODE2_NAME=hdfs-namenode2
RUN_ID=nightly-ha-restore-contract
kinit() { [[ "$MODE" != authentication ]]; }
kdestroy() { echo credentials-cleaned >>"$TRACE"; }
sleep() { command sleep 0.01; }
export -f kinit kdestroy sleep
ssh_hdfs() {
  local command="$2"
  if [[ "$command" != *haadmin* ]]; then
    [[ "$MODE" != topology ]]
    return
  fi
  # Replace sudo's user switch with a local invocation of the same env/timeout/bash chain.
  command="${command/sudo -n -u hdfs /}"
  if [[ "$MODE" == unavailable || "$MODE" == stalled ]]; then
    command="${command/120s/1s}"
  fi
  bash -c "$command"
}
'''
            # restore_hdfs_services catches this function's status, disabling the caller's errexit.
            script += restore + "\nstatus=0; restore_hdfs_ha_primary || status=$?; exit \"$status\"\n"
            result = subprocess.run(
                ["bash", "-c", script], capture_output=True, text=True, timeout=5,
                env={**os.environ, "MODE": mode, "TRACE": str(root / "trace"),
                     "RECOVERED": str(root / "recovered"), "ACTIVE": str(root / "active")},
            )
            trace = (root / "trace").read_text().splitlines() if (root / "trace").exists() else []
            if mode in ("active", "startup", "failover"):
                assert result.returncode == 0, (mode, result.stderr)
                assert trace[-1] == "credentials-cleaned", (mode, trace)
                if mode == "startup":
                    assert trace.count("query") >= 2, trace
                    assert "failover" not in trace, trace
                if mode == "failover":
                    assert trace.count("failover") == 2, trace
                    assert trace[-2] == "query", trace
            else:
                assert result.returncode != 0, (mode, trace)
                if mode in ("authentication", "topology"):
                    assert "query" not in trace, trace
                elif mode == "unavailable":
                    assert trace.count("query") > 1, trace
                    assert "within 120 seconds" in result.stderr
                else:
                    assert trace.count("query") == 1, trace
                    assert "within 120 seconds" in result.stderr


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
    assert 'readiness_path="readiness-${BASHPID}-${attempt}.bin"' in RUNNER
    assert '--path "$readiness_path" --phase seed' in RUNNER
    assert RUNNER.count("wait_for_hdfs_write") >= 4


def test_killed_append_explicitly_recovers_its_lease_before_exact_content_validation() -> None:
    killed = RUNNER.index("kill_mutation_at_checkpoint append datanodes append-killed.bin")
    recover = RUNNER.index("recover_killed_append append-killed.bin", killed)
    inspect = RUNNER.index("wait_for_inspect append-killed.bin", recover)
    assert killed < recover < inspect
    assert '[[ "$after_append" == "$before_append" ]]' in RUNNER
    assert '--phase recover-lease' in RUNNER
    assert "--kill-after=1s" in RUNNER
    assert 'redact_log "$log_file" | tee -a "$artifact" >&2' in RUNNER


def test_recovery_helpers_execute_in_order_and_preserve_failure_evidence() -> None:
    # Execute the actual shell functions, not a Python copy of their logic.
    functions = ""
    for name in ("redact_log", "recover_killed_append", "wait_for_inspect"):
        start = RUNNER.index(f"{name}() {{")
        end = RUNNER.index("\n}", start) + 2
        functions += RUNNER[start:end] + "\n"
    script = "set -euo pipefail\n" + functions + r'''
artifact="$local_root/artifact"
hdfs_storage=hdfs://private-principal@example.invalid/isolated-root
mutation_binary=mutation_probe
inspect_binary=inspect_probe
timeout() { printf '%s\n' "$*" >>"$local_root/timeouts"; shift 2; "$@"; }
sleep() { SECONDS=$((SECONDS + 100)); }
mutation_probe() {
  [[ "$*" == *"--phase recover-lease"* ]]
  if [[ "$failure_mode" == recovery ]]; then
    echo 'recovery diagnostic hdfs://private-principal@example.invalid/isolated-root'
    return 7
  fi
  touch "$local_root/recovered"
}
inspect_probe() {
  test -f "$local_root/recovered"
  touch "$local_root/inspected"
  if [[ "$failure_mode" == inspection ]]; then
    echo 'inspection diagnostic hdfs://private-principal@example.invalid/isolated-root' >&2
    return 8
  fi
  printf '4194321\tseed-digest\n'
}
recover_killed_append append-killed.bin
output="$(wait_for_inspect append-killed.bin)"
[[ "$output" == $'4194321\tseed-digest' ]]
'''
    for mode in ("none", "recovery", "inspection"):
        with tempfile.TemporaryDirectory(prefix="hdfs-fault-contract-") as directory:
            result = subprocess.run(
                ["bash", "-c", script], capture_output=True, text=True, timeout=5,
                env={**os.environ, "local_root": directory, "failure_mode": mode},
            )
            root = Path(directory)
            if mode == "none":
                assert result.returncode == 0, result.stderr
                assert (root / "inspected").exists()
            else:
                assert result.returncode != 0
                assert f"{mode} diagnostic" in (root / "artifact").read_text()
                assert "private-principal" not in (root / "artifact").read_text()
                if mode == "recovery":
                    assert not (root / "inspected").exists()
            assert "--kill-after=1s 125s" in (root / "timeouts").read_text()


def test_independent_cleanup_restores_only_fault_marked_runs_before_any_deletion() -> None:
    cleanup = (ROOT / "tests/lab/cleanup-run.sh").read_text()
    for mode in ("ordinary", "interrupted", "restore-failed"):
        with tempfile.TemporaryDirectory(prefix="hdfs-cancel-cleanup-") as directory:
            sandbox = Path(directory)
            run_id = f"nightly-cancel-contract-{sandbox.name}"
            run_root = Path("/tmp/data-mover-lab") / run_id
            run_root.mkdir(parents=True, exist_ok=False)
            try:
                if mode != "ordinary":
                    marker = run_root / "hdfs-fault/restore-required"
                    marker.parent.mkdir()
                    marker.touch()
                (sandbox / "cleanup-run.sh").write_text(cleanup)
                (sandbox / "common.sh").write_text(r'''
LAB_SOURCE_MGMT=source LAB_DEST_MGMT=dest LAB_WORKER_MGMT=worker
LAB_NFS3_EXPORT=/v3 LAB_NFS41_EXPORT=/v4
LAB_S3_ACCESS_KEY= LAB_S3_SECRET_KEY=
validate_run_id() { [[ "$1" == nightly-cancel-contract-* ]]; }
prepare_hdfs_kerberos() { :; }
hdfs_run_root() { printf '/tmp/data-mover-nightly/%s/hdfs' "$1"; }
ssh_lab() { echo remote-delete >>"$TRACE"; }
ssh_lab_root() { echo remote-delete >>"$TRACE"; }
cargo() {
  echo hdfs-delete >>"$TRACE"
  [[ "$MODE" == ordinary ]] || test -f "$RESTORED"
}
''')
                (sandbox / "hdfs-fault-common.sh").write_text(r'''
restore_hdfs_services() {
  echo restore >>"$TRACE"
  [[ "$RUN_ID" == "$run_id" ]] || return 2
  [[ "$MODE" != restore-failed ]] || return 1
  touch "$RESTORED"
}
''')
                result = subprocess.run(
                    ["bash", str(sandbox / "cleanup-run.sh"), run_id],
                    capture_output=True, text=True, timeout=5,
                    env={**os.environ, "MODE": mode, "RUN_ID": "different-inherited-run",
                         "TRACE": str(sandbox / "trace"),
                         "RESTORED": str(sandbox / "restored")},
                )
                trace = (sandbox / "trace").read_text().splitlines()
                if mode == "restore-failed":
                    assert result.returncode != 0
                    assert trace == ["restore"]
                    assert run_root.exists()
                else:
                    assert result.returncode == 0, result.stderr
                    assert "hdfs-delete" in trace
                    assert not run_root.exists()
                    if mode == "ordinary":
                        assert "restore" not in trace
                    else:
                        assert trace[0] == "restore"
            finally:
                if run_root.exists():
                    shutil.rmtree(run_root)


def test_fault_recovery_marker_precedes_service_changes() -> None:
    marker = RUNNER.index('touch "$local_root/restore-required"')
    mutations = RUNNER.index("for target in namenode datanode1 datanode2;")
    assert marker < mutations


if __name__ == "__main__":
    tests = [value for name, value in globals().copy().items() if name.startswith("test_")]
    for test in tests:
        test()
    print(f"{len(tests)} HDFS fault harness tests passed")
