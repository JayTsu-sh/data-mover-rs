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

# Same-backend checks exercise every configured reader. Representative
# cross-protocol checks cover NAS/S3 boundaries without repeating all 16 copies.
checks=(
  "local local"
  "nfs3 nfs3"
  "nfs41 nfs41"
  "s3 s3"
  "local s3"
  "s3 local"
  "nfs3 s3"
  "s3 nfs41"
)
for check in "${checks[@]}"; do
  read -r source_backend destination_backend <<< "$check"
  key="${source_backend}-to-${destination_backend}.txt"
  check_path "$source_backend" "$destination_backend" "$key"
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

mismatch_log="$local_root/integrity-mismatch.log"
if check_path local local "$mismatch_key" full >"$mismatch_log" 2>&1; then
  echo "Full integrity check accepted corrupted content" >&2
  exit 1
fi
grep -Eq 'Content.*offset: 5' "$mismatch_log" || {
  echo "Full integrity check did not report mismatch offset 5" >&2
  sed -n '1,40p' "$mismatch_log" >&2
  exit 1
}

echo "independent integrity checks verified across lab protocols"
