#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

run_id="${1:?run id required}"
validate_run_id "$run_id"
require_s3_credentials

for host in "$LAB_SOURCE_MGMT" "$LAB_DEST_MGMT" "$LAB_WORKER_MGMT"; do
  ssh_lab "$host" "mkdir -p /var/lib/terrasync-ci/$run_id"
done

for host in "$LAB_SOURCE_MGMT" "$LAB_DEST_MGMT"; do
  ssh_lab "$host" \
    "mkdir -p '$LAB_NFS3_EXPORT/ci/$run_id' '$LAB_NFS41_EXPORT/ci/$run_id' && chmod 0777 '$LAB_NFS3_EXPORT/ci/$run_id' '$LAB_NFS41_EXPORT/ci/$run_id'"
done

python3 "$(dirname "$0")/s3_helper.py" ensure-bucket \
  --endpoint "$LAB_SOURCE_DATA" --bucket "$LAB_S3_BUCKET"
python3 "$(dirname "$0")/s3_helper.py" ensure-bucket \
  --endpoint "$LAB_DEST_DATA" --bucket "$LAB_S3_BUCKET"

echo "$run_id"
