#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

run_id="${1:?run id required}"
validate_run_id "$run_id"
require_s3_credentials

local_root="/tmp/data-mover-lab/$run_id"
mkdir -p "$local_root/source" "$local_root/destination" "$local_root/seed"

storage_url() {
  local role="$1"
  local backend="$2"
  local host export_path
  if [[ "$role" == "source" ]]; then
    host="$LAB_SOURCE_DATA"
  else
    host="$LAB_DEST_DATA"
  fi

  case "$backend" in
    local) printf '%s/%s' "$local_root" "$role" ;;
    nfs3)
      export_path="$LAB_NFS3_EXPORT"
      printf 'nfs://%s%s:/ci/%s?version=3&noresvport=true' \
        "$host" "$export_path" "$run_id"
      ;;
    nfs41)
      export_path="$LAB_NFS41_EXPORT"
      printf 'nfs://%s%s:/ci/%s?version=4.1&noresvport=true' \
        "$host" "$export_path" "$run_id"
      ;;
    s3)
      printf 's3://%s:%s@%s.%s:9000/ci/%s/%s' \
        "$LAB_S3_ACCESS_KEY" "$LAB_S3_SECRET_KEY" "$LAB_S3_BUCKET" "$host" "$run_id" "$role"
      ;;
  esac
}

seed_source() {
  local backend="$1"
  local key="$2"
  local seed_path="$local_root/seed/$key"

  python3 -c '
import pathlib
import sys
size = 12 * 1024 * 1024 + 123
chunk = bytes(range(251)) * 4096
path = pathlib.Path(sys.argv[1])
with path.open("wb") as output:
    remaining = size
    while remaining:
        piece = chunk[:min(len(chunk), remaining)]
        output.write(piece)
        remaining -= len(piece)
' "$seed_path"

  if [[ "$backend" == "local" ]]; then
    cp "$seed_path" "$local_root/source/$key"
    return
  fi

  # Seed remote sources through data-mover instead of writing exports as root.
  # Root-owned NFS fixtures carry uid/gid 0 into NAS metadata; an NFS -> Local
  # resume would then correctly attempt to preserve that ownership but fail on
  # the unprivileged lab runner with EPERM before exercising the data path.
  cargo run --quiet --locked --example storage_copy -- \
    --source "$local_root/seed" \
    --destination "$(storage_url source "$backend")" \
    --path "$key"
}

backends=(local nfs3 nfs41 s3)
for source_backend in "${backends[@]}"; do
  for destination_backend in "${backends[@]}"; do
    key="resume-${source_backend}-to-${destination_backend}.bin"
    seed_source "$source_backend" "$key"

    common_args=(
      --source "$(storage_url source "$source_backend")"
      --destination "$(storage_url destination "$destination_backend")"
      --path "$key"
    )
    cargo run --quiet --locked --example storage_resume -- \
      "${common_args[@]}" --phase interrupt
    cargo run --quiet --locked --example storage_resume -- \
      "${common_args[@]}" --phase resume

    echo "resume $source_backend -> $destination_backend verified"
  done
done
