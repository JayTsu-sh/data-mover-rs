#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

run_id="${1:?run id required}"
validate_run_id "$run_id"

payload="data-mover-${run_id}-$(date -u +%Y%m%dT%H%M%SZ)"
file_name="payload.txt"

for version in nfs3 nfs41; do
  if [[ "$version" == "nfs3" ]]; then
    export_path="$LAB_NFS3_EXPORT"
  else
    export_path="$LAB_NFS41_EXPORT"
  fi
  source_url="nfs://$LAB_SOURCE_DATA${export_path}:/ci/$run_id"
  destination_url="nfs://$LAB_DEST_DATA${export_path}:/ci/$run_id"

  ssh_lab "$LAB_SOURCE_MGMT" \
    "printf '%s\n' '$payload-$version' > '$export_path/ci/$run_id/$file_name'"

  cargo run --quiet --locked --example storage_copy -- \
    --source "$source_url" \
    --destination "$destination_url" \
    --path "$file_name"

  source_hash="$(ssh_lab "$LAB_SOURCE_MGMT" \
    "sha256sum '$export_path/ci/$run_id/$file_name' | cut -d' ' -f1")"
  destination_hash="$(ssh_lab "$LAB_DEST_MGMT" \
    "sha256sum '$export_path/ci/$run_id/$file_name' | cut -d' ' -f1")"

  [[ "$source_hash" == "$destination_hash" ]] || {
    echo "$version checksum mismatch" >&2
    exit 1
  }
  echo "$version copy verified: $source_hash"
done
