#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
export LAB_HDFS_LOCATION LAB_HDFS_ADMIN_USER LAB_HDFS_CONFIG_DIR LAB_HDFS_KEYTAB

run_id="${1:?run id required (nightly-* or release-*)}"
validate_run_id "$run_id"
prepare_hdfs_kerberos "$run_id"
export LAB_HDFS_RUN_ROOT
LAB_HDFS_RUN_ROOT="$(hdfs_run_root "$run_id")"
HDFS_SMOKE_ATTEMPTS="${HDFS_SMOKE_ATTEMPTS:-2}"

cleanup_hdfs_run_root() {
  cargo test --test hdfs_native_contract nightly_lab_cleanup_confined_run_root -- \
    --ignored --exact --test-threads=1
}

cleanup_hdfs_run() {
  local test_status="$1"
  local cleanup_status=0
  cleanup_hdfs_run_root || cleanup_status=$?
  if (( test_status != 0 )); then
    exit "$test_status"
  fi
  exit "$cleanup_status"
}

trap 'cleanup_hdfs_run $?' EXIT

run_hdfs_contract() {
  cargo test --test hdfs_native_contract nightly_lab_ -- --ignored --test-threads=1 \
    --skip nightly_lab_copies_between_s3_and_hdfs \
    --skip nightly_lab_cleanup_confined_run_root
}

attempt=1
while (( attempt <= HDFS_SMOKE_ATTEMPTS )); do
  if run_hdfs_contract; then
    exit 0
  else
    status=$?
  fi
  if (( attempt == HDFS_SMOKE_ATTEMPTS )); then
    exit "$status"
  fi
  echo "HDFS contract attempt $attempt failed; cleaning the isolated root before retry" >&2
  cleanup_hdfs_run_root || true
  sleep 5
  ((attempt++))
done
