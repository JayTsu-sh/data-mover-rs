#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

run_id="${1:?run id required}"
validate_run_id "$run_id"
timeout 5 bash -c "</dev/tcp/$LAB_DXN_V40_DATA/2049"

run_root="data-mover-ci/$run_id"
source_root="$run_root/source"
destination_root="$run_root/destination"
mount_url="nfs://$LAB_DXN_V40_DATA$LAB_DXN_V40_EXPORT?version=4.0&noresvport=true&uid=0&gid=0"
source_url="nfs://$LAB_DXN_V40_DATA$LAB_DXN_V40_EXPORT:/$source_root?version=4.0&noresvport=true&uid=0&gid=0"
destination_url="nfs://$LAB_DXN_V40_DATA$LAB_DXN_V40_EXPORT:/$destination_root?version=4.0&noresvport=true&uid=0&gid=0"

cargo run --quiet --locked --example nfs3_contract -- \
  --dialect nfs40 \
  --seed-mount "$mount_url" \
  --seed-root "$source_root" \
  --source "$source_url" \
  --destination "$destination_url"

echo "DM-NFS40-CONTRACT passed for $run_id"
