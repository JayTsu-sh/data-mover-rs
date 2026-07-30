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

# Exercise negotiated NFSv4.1 rsize/wsize with a payload larger than a single
# session request. The small matrix fixtures above validate routing and
# metadata, but are not large enough to detect an invalid effective wsize.
nfs41_key="nfs41-large-copy.bin"
nfs41_seed="$local_root/seed/$nfs41_key"
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
' "$nfs41_seed"
ssh_lab_root "$LAB_SOURCE_MGMT" \
  "cat > '$LAB_NFS41_EXPORT/ci/$run_id/$nfs41_key'" < "$nfs41_seed"

cargo run --quiet --locked --example storage_copy -- \
  --source "$(storage_url source nfs41)" \
  --destination "$(storage_url destination nfs41)" \
  --path "$nfs41_key"

expected_hash="$(sha256sum "$nfs41_seed" | cut -d' ' -f1)"
actual_hash="$(destination_hash nfs41 "$nfs41_key")"
[[ "$actual_hash" == "$expected_hash" ]] || {
  echo "large nfs41 -> nfs41 checksum mismatch" >&2
  exit 1
}
echo "large nfs41 -> nfs41 verified: $actual_hash"

# Exercise the same-endpoint CopyObject fast path with a key that requires
# x-amz-copy-source percent-encoding. The regular S3 matrix copies between two
# endpoints and therefore uses the streaming path instead.
s3_special_key="s3 special %?# 中文.txt"
s3_special_value="data-mover-$run_id-same-endpoint-special-key"
seed_source s3 "$s3_special_key" "$s3_special_value"
same_endpoint_destination="s3://$LAB_S3_ACCESS_KEY:$LAB_S3_SECRET_KEY@$LAB_S3_BUCKET.$LAB_SOURCE_DATA:9000/ci/$run_id/same-endpoint-destination"
cargo run --quiet --locked --example storage_copy -- \
  --source "$(storage_url source s3)" \
  --destination "$same_endpoint_destination" \
  --path "$s3_special_key"

expected_hash="$(printf '%s' "$s3_special_value" | sha256sum | cut -d' ' -f1)"
actual_hash="$(
  python3 "$(dirname "$0")/s3_helper.py" sha256 \
    --endpoint "$LAB_SOURCE_DATA" --bucket "$LAB_S3_BUCKET" \
    --key "ci/$run_id/same-endpoint-destination/$s3_special_key"
)"
[[ "$actual_hash" == "$expected_hash" ]] || {
  echo "same-endpoint S3 special-key checksum mismatch" >&2
  exit 1
}
echo "same-endpoint S3 special-key CopyObject verified: $actual_hash"

# Exercise S3 object rename as a server-side CopyObject followed by deletion of
# the source. Special characters also verify that rename uses the encoded
# x-amz-copy-source path.
s3_rename_source="rename source %?# 中文.txt"
s3_rename_destination="renamed destination %?# 中文.txt"
s3_rename_value="data-mover-$run_id-s3-small-object-rename"
seed_source s3 "$s3_rename_source" "$s3_rename_value"

cargo run --quiet --locked --example storage_rename -- \
  --storage "$(storage_url source s3)" \
  --from "$s3_rename_source" \
  --to "$s3_rename_destination"

expected_hash="$(printf '%s' "$s3_rename_value" | sha256sum | cut -d' ' -f1)"
actual_hash="$(
  python3 "$(dirname "$0")/s3_helper.py" sha256 \
    --endpoint "$LAB_SOURCE_DATA" --bucket "$LAB_S3_BUCKET" \
    --key "ci/$run_id/source/$s3_rename_destination"
)"
[[ "$actual_hash" == "$expected_hash" ]] || {
  echo "S3 small-object rename checksum mismatch" >&2
  exit 1
}
if python3 "$(dirname "$0")/s3_helper.py" exists \
  --endpoint "$LAB_SOURCE_DATA" --bucket "$LAB_S3_BUCKET" \
  --key "ci/$run_id/source/$s3_rename_source"; then
  echo "S3 small-object rename left the source object behind" >&2
  exit 1
fi
echo "S3 small-object rename verified: $actual_hash"

# Force the multipart S3 rename path with lab-sized limits. The test verifies
# object bytes, user and system metadata, tags, special-character CopySource
# encoding, and deletion of the source object.
DM_S3_RENAME_TEST_URL="$(storage_url source s3)" \
  cargo test --quiet --locked \
    s3::tests::multipart_rename_preserves_content_and_metadata_when_lab_is_configured \
    -- --exact
echo "S3 multipart rename verified"
