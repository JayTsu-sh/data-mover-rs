#!/usr/bin/env python3
"""Static contract for keytab-backed HDFS nightly authentication."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
COMMON = (ROOT / "tests/lab/common.sh").read_text()
HEALTH_CHECK = (ROOT / "tests/lab/health-check.sh").read_text()
SMOKE = (ROOT / "tests/lab/run-hdfs-smoke.sh").read_text()
S3_METADATA = (ROOT / "tests/lab/run-hdfs-s3-metadata.sh").read_text()
SUPPORT = (ROOT / "examples/hdfs_support/mod.rs").read_text()


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


if __name__ == "__main__":
    tests = [
        test_keytab_is_required_and_process_cache_is_poisoned,
        test_examples_construct_client_scoped_keytab_configuration,
        test_health_check_requires_canonical_hdfs_names,
    ]
    for test in tests:
        test()
    print(f"{len(tests)} HDFS Kerberos lab tests passed")
