#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
export LAB_HDFS_LOCATION LAB_HDFS_ADMIN_USER LAB_HDFS_CONFIG_DIR LAB_HDFS_KEYTAB

run_id="${1:?run id required (nightly-* or release-*)}"
validate_run_id "$run_id"
prepare_hdfs_kerberos "$run_id"
export LAB_HDFS_RUN_ROOT
LAB_HDFS_RUN_ROOT="$(hdfs_run_root "$run_id")"

cleanup_hdfs_run() {
  local test_status="$1"
  local cleanup_status=0
  cargo test --test hdfs_native_contract nightly_lab_cleanup_confined_run_root -- \
    --ignored --exact --test-threads=1 || cleanup_status=$?
  if (( test_status != 0 )); then
    exit "$test_status"
  fi
  exit "$cleanup_status"
}

trap 'cleanup_hdfs_run $?' EXIT

cargo test --test hdfs_native_contract nightly_lab_ -- --ignored --test-threads=1 \
  --skip nightly_lab_copies_between_s3_and_hdfs \
  --skip nightly_lab_cleanup_confined_run_root
