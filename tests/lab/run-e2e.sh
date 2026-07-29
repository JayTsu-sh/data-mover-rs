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
  local value="$3"
  printf '%s' "$value" > "$local_root/seed/$key"
  chmod 0640 "$local_root/seed/$key"
  touch -m -d '@1700000000.123456789' "$local_root/seed/$key"
  if [[ "$backend" == "local" ]]; then
    cp --preserve=mode,ownership,timestamps \
      "$local_root/seed/$key" "$local_root/source/$key"
    return
  fi

  cargo run --quiet --locked --example storage_copy -- \
    --source "$local_root/seed" \
    --destination "$(storage_url source "$backend")" \
    --path "$key"
}

destination_hash() {
  local backend="$1"
  local key="$2"
  case "$backend" in
    local) sha256sum "$local_root/destination/$key" | cut -d' ' -f1 ;;
    nfs3)
      ssh_lab_root "$LAB_DEST_MGMT" \
        "sha256sum '$LAB_NFS3_EXPORT/ci/$run_id/$key' | cut -d' ' -f1"
      ;;
    nfs41)
      ssh_lab_root "$LAB_DEST_MGMT" \
        "sha256sum '$LAB_NFS41_EXPORT/ci/$run_id/$key' | cut -d' ' -f1"
      ;;
    s3)
      python3 "$(dirname "$0")/s3_helper.py" sha256 \
        --endpoint "$LAB_DEST_DATA" --bucket "$LAB_S3_BUCKET" \
        --key "ci/$run_id/destination/$key"
      ;;
  esac
}

backends=(local nfs3 nfs41 s3)
for source_backend in "${backends[@]}"; do
  for destination_backend in "${backends[@]}"; do
    key="${source_backend}-to-${destination_backend}.txt"
    value="data-mover-$run_id-$source_backend-to-$destination_backend"
    expected_hash="$(printf '%s' "$value" | sha256sum | cut -d' ' -f1)"

    seed_source "$source_backend" "$key" "$value"
    cargo run --quiet --locked --example storage_copy -- \
      --source "$(storage_url source "$source_backend")" \
      --destination "$(storage_url destination "$destination_backend")" \
      --path "$key"

    actual_hash="$(destination_hash "$destination_backend" "$key")"
    [[ "$actual_hash" == "$expected_hash" ]] || {
      echo "$source_backend -> $destination_backend checksum mismatch" >&2
      exit 1
    }
    echo "$source_backend -> $destination_backend verified: $actual_hash"
  done
done
