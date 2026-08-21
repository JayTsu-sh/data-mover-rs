#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

run_id="${1:?run id required}"
validate_run_id "$run_id"
prepare_hdfs_kerberos "$run_id"
export LAB_HDFS_RUN_ROOT
LAB_HDFS_RUN_ROOT="$(hdfs_run_root "$run_id")"

for host in "$LAB_SOURCE_MGMT" "$LAB_DEST_MGMT" "$LAB_WORKER_MGMT"; do
  ssh_lab "$host" "rm -rf -- '/var/lib/terrasync-ci/$run_id'"
done

if [[ -n "${LAB_S3_ACCESS_KEY:-}" && -n "${LAB_S3_SECRET_KEY:-}" ]]; then
  python3 "$(dirname "$0")/s3_helper.py" abort-multipart-prefix \
    --endpoint "$LAB_SOURCE_DATA" --bucket "$LAB_S3_BUCKET" --prefix "ci/$run_id/"
  python3 "$(dirname "$0")/s3_helper.py" abort-multipart-prefix \
    --endpoint "$LAB_DEST_DATA" --bucket "$LAB_S3_BUCKET" --prefix "ci/$run_id/"
  python3 "$(dirname "$0")/s3_helper.py" delete-prefix \
    --endpoint "$LAB_SOURCE_DATA" --bucket "$LAB_S3_BUCKET" --prefix "ci/$run_id/"
  python3 "$(dirname "$0")/s3_helper.py" delete-prefix \
    --endpoint "$LAB_DEST_DATA" --bucket "$LAB_S3_BUCKET" --prefix "ci/$run_id/"
fi

rm -rf -- "/tmp/data-mover-lab/$run_id"

cargo test --test hdfs_native_contract nightly_lab_cleanup_confined_run_root -- \
  --ignored --exact --test-threads=1

for host in "$LAB_SOURCE_MGMT" "$LAB_DEST_MGMT"; do
  ssh_lab_root "$host" \
    "rm -rf -- '$LAB_NFS3_EXPORT/ci/$run_id' '$LAB_NFS41_EXPORT/ci/$run_id'"
done
