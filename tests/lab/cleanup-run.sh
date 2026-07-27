#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

run_id="${1:?run id required}"
validate_run_id "$run_id"

for host in "$LAB_SOURCE_MGMT" "$LAB_DEST_MGMT" "$LAB_WORKER_MGMT"; do
  ssh_lab "$host" "rm -rf -- '/var/lib/terrasync-ci/$run_id'"
done

for host in "$LAB_SOURCE_MGMT" "$LAB_DEST_MGMT"; do
  ssh_lab "$host" \
    "rm -rf -- '$LAB_NFS3_EXPORT/ci/$run_id' '$LAB_NFS41_EXPORT/ci/$run_id'"
done
