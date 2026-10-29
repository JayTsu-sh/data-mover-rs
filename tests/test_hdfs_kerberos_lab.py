#!/usr/bin/env python3
"""Static contract for keytab-backed HDFS nightly authentication."""

from pathlib import Path
import os
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parents[1]
COMMON = (ROOT / "tests/lab/common.sh").read_text()
HEALTH_CHECK = (ROOT / "tests/lab/health-check.sh").read_text()
SMOKE = (ROOT / "tests/lab/run-hdfs-smoke.sh").read_text()
S3_METADATA = (ROOT / "tests/lab/run-hdfs-s3-metadata.sh").read_text()
SUPPORT = (ROOT / "tests/lab/tools/support/hdfs.rs").read_text()


def test_keytab_is_required_and_process_cache_is_poisoned() -> None:
    assert "require_hdfs_kerberos_credentials" in COMMON
    assert '[[ -r "$LAB_HDFS_KEYTAB" ]]' in COMMON
    assert 'KRB5CCNAME="FILE:$runtime_dir/data-mover-invalid-$run_id.ccache"' in COMMON
    for runner in (SMOKE, S3_METADATA):
        assert "prepare_hdfs_kerberos" in runner
        assert "kinit" not in runner


def test_examples_construct_client_scoped_keytab_configuration() -> None:
    assert 'var_os("LAB_HDFS_CONFIG_DIR")' in SUPPORT
    assert 'var_os("LAB_HDFS_KEYTAB")' in SUPPORT
    assert "HdfsKerberosCredentials {" in SUPPORT
    assert "keytab: Some(" in SUPPORT
    assert "KRB5CCNAME" not in SUPPORT


def test_health_check_requires_canonical_hdfs_names() -> None:
    assert "getent ahostsv4" in HEALTH_CHECK
    for hostname in (
        "hdfs-namenode.hdfs.local",
        "hdfs-namenode2.hdfs.local",
        "hdfs-datanode1.hdfs.local",
        "hdfs-datanode2.hdfs.local",
    ):
        assert hostname in HEALTH_CHECK


def test_hdfs_smoke_retries_after_transient_cluster_io_failure() -> None:
    assert 'HDFS_SMOKE_ATTEMPTS="${HDFS_SMOKE_ATTEMPTS:-2}"' in SMOKE
    assert "cleanup_hdfs_run_root" in SMOKE
    assert "attempt == HDFS_SMOKE_ATTEMPTS" in SMOKE


def test_hdfs_root_accepts_only_configured_port_free_nameservices() -> None:
    with tempfile.TemporaryDirectory() as directory:
        Path(directory, "hdfs-site.xml").write_text(
            "<configuration><property><name>dfs.nameservices</name>"
            "<value>lab-ha, other-ha</value></property></configuration>"
        )
        env = {
            **os.environ,
            "LAB_HDFS_CONFIG_DIR": directory,
            "LAB_HDFS_ADMIN_USER": "hdfs/runner@LAB.LOCAL",
        }
        for location, authority in (
            ("hdfs://user@lab-ha/", "lab-ha"),
            ("hdfs://user@host:9000/", "host:9000"),
            ("hdfs://user@unknown/", None),
            ("hdfs://user:secret@lab-ha/", None),
        ):
            result = subprocess.run(
                ["bash", "-c", 'source tests/lab/common.sh; hdfs_run_root nightly-test'],
                cwd=ROOT,
                env={**env, "LAB_HDFS_LOCATION": location},
                text=True,
                capture_output=True,
                check=False,
            )
            if authority is None:
                assert result.returncode != 0
            else:
                assert result.returncode == 0, result.stderr
                assert result.stdout.strip() == (
                    f"hdfs://hdfs%2Frunner%40LAB.LOCAL@{authority}"
                    "/tmp/data-mover-nightly/nightly-test/hdfs"
                )


def test_hdfs_smoke_keeps_failure_status_after_successful_cleanup() -> None:
    with tempfile.TemporaryDirectory() as directory:
        cargo = Path(directory, "cargo")
        cargo.write_text(
            '#!/bin/sh\nif [ "$4" = nightly_lab_cleanup_confined_run_root ]; '
            'then exit 0; else exit 42; fi\n'
        )
        cargo.chmod(0o700)
        keytab = Path(directory, "keytab")
        keytab.touch()
        result = subprocess.run(
            ["bash", "tests/lab/run-hdfs-smoke.sh", "nightly-test"],
            cwd=ROOT,
            env={
                **os.environ,
                "PATH": directory + os.pathsep + os.environ["PATH"],
                "LAB_HDFS_CONFIG_DIR": directory,
                "LAB_HDFS_KEYTAB": str(keytab),
                "LAB_HDFS_LOCATION": "hdfs://user@host:9000/",
                "HDFS_SMOKE_ATTEMPTS": "1",
            },
            check=False,
        )
        assert result.returncode == 42


if __name__ == "__main__":
    tests = [
        test_keytab_is_required_and_process_cache_is_poisoned,
        test_examples_construct_client_scoped_keytab_configuration,
        test_health_check_requires_canonical_hdfs_names,
        test_hdfs_smoke_retries_after_transient_cluster_io_failure,
        test_hdfs_root_accepts_only_configured_port_free_nameservices,
        test_hdfs_smoke_keeps_failure_status_after_successful_cleanup,
    ]
    for test in tests:
        test()
    print(f"{len(tests)} HDFS Kerberos lab tests passed")
