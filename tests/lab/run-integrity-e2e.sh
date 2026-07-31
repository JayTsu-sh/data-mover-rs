#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

run_id="${1:?run id required}"
validate_run_id "$run_id"
require_s3_credentials

local_root="/tmp/data-mover-lab/$run_id"

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
    *)
      echo "unsupported lab backend: $backend" >&2
      return 2
      ;;
  esac
}

check_path() {
  local source_backend="$1"
  local destination_backend="$2"
  local key="$3"
  local mode="${4:-full}"
  cargo run --quiet --locked --example storage_integrity_check -- \
    --source "$(storage_url source "$source_backend")" \
    --destination "$(storage_url destination "$destination_backend")" \
    --path "$key" \
    --mode "$mode"
}

expect_mismatch() {
  local expected_pattern="$1"
  local description="$2"
  shift 2
  local log="$local_root/integrity-negative-${description}.log"

  if check_path "$@" >"$log" 2>&1; then
    echo "integrity check accepted $description" >&2
    exit 1
  fi
  grep -Eq "$expected_pattern" "$log" || {
    echo "integrity check reported the wrong result for $description" >&2
    sed -n '1,40p' "$log" >&2
    exit 1
  }
}

replace_destination_byte() {
  local backend="$1"
  local key="$2"
  local value="$3"
  local replacement="${value:0:5}X${value:6}"

  case "$backend" in
    local)
      printf 'X' | dd of="$local_root/destination/$key" \
        bs=1 seek=5 conv=notrunc status=none
      ;;
    nfs3)
      ssh_lab_root "$LAB_DEST_MGMT" \
        "printf X | dd of='$LAB_NFS3_EXPORT/ci/$run_id/$key' bs=1 seek=5 conv=notrunc status=none"
      ;;
    nfs41)
      ssh_lab_root "$LAB_DEST_MGMT" \
        "printf X | dd of='$LAB_NFS41_EXPORT/ci/$run_id/$key' bs=1 seek=5 conv=notrunc status=none"
      ;;
    s3)
      python3 "$(dirname "$0")/s3_helper.py" put \
        --endpoint "$LAB_DEST_DATA" --bucket "$LAB_S3_BUCKET" \
        --key "ci/$run_id/destination/$key" --value "$replacement"
      ;;
  esac
}

append_destination_byte() {
  local backend="$1"
  local key="$2"
  local value="$3"

  case "$backend" in
    local)
      printf 'Y' >> "$local_root/destination/$key"
      ;;
    nfs3)
      ssh_lab_root "$LAB_DEST_MGMT" \
        "printf Y >> '$LAB_NFS3_EXPORT/ci/$run_id/$key'"
      ;;
    nfs41)
      ssh_lab_root "$LAB_DEST_MGMT" \
        "printf Y >> '$LAB_NFS41_EXPORT/ci/$run_id/$key'"
      ;;
    s3)
      python3 "$(dirname "$0")/s3_helper.py" put \
        --endpoint "$LAB_DEST_DATA" --bucket "$LAB_S3_BUCKET" \
        --key "ci/$run_id/destination/$key" --value "${value:0:5}X${value:6}Y"
      ;;
  esac
}

# Re-read the complete directed matrix produced by run-e2e.sh. This covers all
# four same-protocol paths and all twelve cross-protocol paths.
backends=(local nfs3 nfs41 s3)
for source_backend in "${backends[@]}"; do
  for destination_backend in "${backends[@]}"; do
    key="${source_backend}-to-${destination_backend}.txt"
    check_path "$source_backend" "$destination_backend" "$key"
  done
done

# The existing large NFSv4.1 copy is bigger than one session request. Reading
# it independently verifies negotiated effective rsize in the integrity path.
check_path nfs41 nfs41 "nfs41-large-copy.bin"

# Quick mode must not read content. Preserve metadata after corrupting one byte,
# then require Quick to pass and Full to report the exact first bad offset.
mismatch_key="local-to-local.txt"
source_file="$local_root/source/$mismatch_key"
destination_file="$local_root/destination/$mismatch_key"
printf 'X' | dd of="$destination_file" bs=1 seek=5 conv=notrunc status=none
touch -r "$source_file" "$destination_file"
check_path local local "$mismatch_key" quick

expect_mismatch 'Content.*offset: 5' "local-same-size-content" \
  local local "$mismatch_key" full

# Negative cross-protocol ring: every configured backend participates as both
# an integrity reader and a mutated destination. Full must identify the first
# corrupt byte in equal-sized files. After extending the destination, Quick
# must reject the size mismatch without reading file data.
negative_checks=(
  "local nfs3"
  "nfs3 s3"
  "s3 nfs41"
  "nfs41 local"
)
for check in "${negative_checks[@]}"; do
  read -r source_backend destination_backend <<< "$check"
  key="${source_backend}-to-${destination_backend}.txt"
  value="data-mover-$run_id-$source_backend-to-$destination_backend"
  description="${source_backend}-to-${destination_backend}"

  replace_destination_byte "$destination_backend" "$key" "$value"
  expect_mismatch 'Content.*offset: 5' "$description-content" \
    "$source_backend" "$destination_backend" "$key" full

  append_destination_byte "$destination_backend" "$key" "$value"
  expect_mismatch 'Size.*src: [0-9]+, dest: [0-9]+' "$description-size" \
    "$source_backend" "$destination_backend" "$key" quick
done

echo "positive and negative integrity checks verified across lab protocols"
