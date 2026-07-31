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

copy_local_fixture() {
  local source_root="$1"
  local role="$2"
  local backend="$3"
  local key="$4"

  if [[ "$backend" == "local" ]]; then
    cp "$source_root/$key" "$local_root/$role/$key"
    return
  fi

  cargo run --quiet --locked --example storage_copy -- \
    --source "$source_root" \
    --destination "$(storage_url "$role" "$backend")" \
    --path "$key"
}

seed_large_negative_case() {
  local source_backend="$1"
  local destination_backend="$2"
  local key="$3"
  local fixture_root="$4"

  cp "$local_root/seed/nfs41-large-copy.bin" "$fixture_root/$key"
  copy_local_fixture "$fixture_root" source "$source_backend" "$key"
  cargo run --quiet --locked --example storage_copy -- \
    --source "$(storage_url source "$source_backend")" \
    --destination "$(storage_url destination "$destination_backend")" \
    --path "$key"
}

replace_destination_byte() {
  local backend="$1"
  local key="$2"
  local fixture_root="$3"
  local mismatch_offset="$4"

  printf 'X' | dd of="$fixture_root/$key" \
    bs=1 seek="$mismatch_offset" conv=notrunc status=none
  copy_local_fixture "$fixture_root" destination "$backend" "$key"
}

append_destination_byte() {
  local backend="$1"
  local key="$2"
  local fixture_root="$3"

  printf 'Y' >> "$fixture_root/$key"
  copy_local_fixture "$fixture_root" destination "$backend" "$key"
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
# an integrity reader and a mutated destination. The 12 MiB + 123 byte fixtures
# and an unaligned mismatch after 6 MiB cross the default NFS (1 MiB), Local
# (2 MiB), and S3 (5 MiB) read boundaries. Full must identify the global offset
# in equal-sized files. After extending the destination, Quick must reject the
# size mismatch without reading file data.
negative_fixture_root="$local_root/integrity-negative-fixtures"
mkdir -p "$negative_fixture_root"
mismatch_offset=$((6 * 1024 * 1024 + 17))
negative_checks=(
  "local nfs3"
  "nfs3 s3"
  "s3 nfs41"
  "nfs41 local"
)
for check in "${negative_checks[@]}"; do
  read -r source_backend destination_backend <<< "$check"
  key="integrity-negative-${source_backend}-to-${destination_backend}.bin"
  description="${source_backend}-to-${destination_backend}"

  seed_large_negative_case \
    "$source_backend" "$destination_backend" "$key" "$negative_fixture_root"
  check_path "$source_backend" "$destination_backend" "$key" full

  replace_destination_byte \
    "$destination_backend" "$key" "$negative_fixture_root" "$mismatch_offset"
  expect_mismatch "Content.*offset: $mismatch_offset" "$description-content" \
    "$source_backend" "$destination_backend" "$key" full

  append_destination_byte "$destination_backend" "$key" "$negative_fixture_root"
  expect_mismatch 'Size.*src: [0-9]+, dest: [0-9]+' "$description-size" \
    "$source_backend" "$destination_backend" "$key" quick
done

echo "positive and negative integrity checks verified across lab protocols"
