#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
export LAB_HDFS_LOCATION LAB_HDFS_ADMIN_USER LAB_HDFS_CONFIG_DIR LAB_HDFS_KEYTAB

require_s3_credentials
run_id="${1:?run id required (nightly-* or release-*)}"
validate_run_id "$run_id"
prepare_hdfs_kerberos "$run_id"
export LAB_HDFS_RUN_ROOT
LAB_HDFS_RUN_ROOT="$(hdfs_run_root "$run_id")"
export LAB_S3_HDFS_LOCATION="s3://$LAB_S3_ACCESS_KEY:$LAB_S3_SECRET_KEY@$LAB_S3_BUCKET.$LAB_SOURCE_DATA:9000/ci/$run_id/hdfs-metadata/source"
prefix="ci/$run_id/hdfs-metadata/source"
cleanup() {
  local test_status="$1"
  local cleanup_status=0
  python3 "$(dirname "$0")/s3_helper.py" delete-prefix \
    --endpoint "$LAB_SOURCE_DATA" --bucket "$LAB_S3_BUCKET" --prefix "$prefix/" || cleanup_status=$?
  python3 "$(dirname "$0")/s3_helper.py" abort-multipart-prefix \
    --endpoint "$LAB_SOURCE_DATA" --bucket "$LAB_S3_BUCKET" --prefix "$prefix/" || cleanup_status=$?
  cargo test --test hdfs_native_contract nightly_lab_cleanup_confined_run_root -- \
    --ignored --exact --test-threads=1 || cleanup_status=$?
  if (( test_status != 0 )); then
    exit "$test_status"
  fi
  exit "$cleanup_status"
}
trap 'cleanup $?' EXIT

cargo test --test hdfs_native_contract \
  nightly_lab_copies_between_s3_and_hdfs -- --ignored --test-threads=1
