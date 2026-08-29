#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
require_s3_credentials
run_id="${1:?run id required (nightly-* or release-*)}"
validate_run_id "$run_id"
prefix="ci/$run_id/s3-architecture"
export LAB_S3_ARCHITECTURE_URL="s3://$LAB_S3_ACCESS_KEY:$LAB_S3_SECRET_KEY@$LAB_S3_BUCKET.$LAB_SOURCE_DATA:9000"
export LAB_S3_ARCHITECTURE_KEY="$prefix/published.bin"
cleanup() {
  local status="$1"
  python3 "$(dirname "$0")/s3_helper.py" delete-prefix --endpoint "$LAB_SOURCE_DATA" --bucket "$LAB_S3_BUCKET" --prefix "$prefix/" || true
  python3 "$(dirname "$0")/s3_helper.py" abort-multipart-prefix --endpoint "$LAB_SOURCE_DATA" --bucket "$LAB_S3_BUCKET" --prefix "$prefix/" || true
  exit "$status"
}
trap 'cleanup $?' EXIT
cargo test --test s3_architecture_contract standard_s3_architecture_roles_stage_publish_and_read_back -- --ignored --exact --test-threads=1
cargo test --lib s3::role_protocol::tests::standard_s3_invalid_manifest_is_aborted_and_restartable -- --ignored --exact --test-threads=1
cargo test --lib s3::role_protocol::tests::standard_s3_native_multipart_copy_uses_owned_upload -- --ignored --exact --test-threads=1
