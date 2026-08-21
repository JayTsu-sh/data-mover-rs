#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

run_id="${1:?run id required}"
output="${2:-inflight-benchmark.csv}"
validate_run_id "$run_id"
prepare_hdfs_kerberos "$run_id"
require_s3_credentials

local_root="/tmp/data-mover-lab/$run_id"
fixture="$local_root/seed/inflight-benchmark.bin"
target_directory="$(cargo metadata --locked --no-deps --format-version 1 | \
  python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
binary="$target_directory/release/examples/storage_copy"
size=$((128 * 1024 * 1024 + 137))
mkdir -p "$local_root/source" "$local_root/destination" "$local_root/seed"
[[ -x "$binary" ]] || cargo build --release --locked --example storage_copy

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
        "$LAB_S3_ACCESS_KEY" "$LAB_S3_SECRET_KEY" "$LAB_S3_BUCKET" "$host" \
        "$run_id" "$role"
      ;;
  esac
}

env_backend() {
  case "$1" in
    local) printf LOCAL ;;
    nfs3|nfs41) printf NFS ;;
    s3) printf S3 ;;
  esac
}

# The offset-derived fixture is incompressible enough for protocol comparison
# and detects reordered or duplicated chunks through storage_copy's hash check.
python3 - "$fixture" "$size" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
size = int(sys.argv[2])
state = 0x9e3779b97f4a7c15
block = bytearray(1024 * 1024)
with path.open("wb") as output:
    remaining = size
    while remaining:
        for index in range(len(block)):
            state ^= state >> 12
            state ^= (state << 25) & 0xffffffffffffffff
            state ^= state >> 27
            block[index] = (state * 0x2545f4914f6cdd1d) & 0xff
        length = min(len(block), remaining)
        output.write(block[:length])
        remaining -= length
PY

backends=(local nfs3 nfs41 s3)
key="inflight-benchmark.bin"
for backend in "${backends[@]}"; do
  if [[ "$backend" == local ]]; then
    cp "$fixture" "$local_root/source/$key"
  else
    "$binary" --source "$local_root/seed" \
      --destination "$(storage_url source "$backend")" --path "$key"
  fi
done
printf '%s\n' \
  'source,destination,inflight,repeat,bytes,elapsed_s,user_s,system_s,max_rss_kib,cpu_percent,throughput_mib_s' \
  > "$output"

run_case() {
  local source_backend="$1"
  local destination_backend="$2"
  local depth="$3"
  local repeat="$4"
  local read_var="DATA_MOVER_$(env_backend "$source_backend")_READ_INFLIGHT"
  local write_var="DATA_MOVER_$(env_backend "$destination_backend")_WRITE_INFLIGHT"
  local metrics log elapsed user system rss cpu throughput
  metrics="$(mktemp)"
  log="$(mktemp)"

  if ! /usr/bin/time -o "$metrics" -f '%e,%U,%S,%M,%P' \
    env "$read_var=$depth" "$write_var=$depth" \
    "$binary" --source "$(storage_url source "$source_backend")" \
      --destination "$(storage_url destination "$destination_backend")" \
      --path "$key" > "$log" 2>&1; then
    cat "$log" >&2
    rm -f "$metrics" "$log"
    return 1
  fi
  IFS=, read -r elapsed user system rss cpu < "$metrics"
  cpu="${cpu%%%}"
  throughput="$(awk -v bytes="$size" -v elapsed="$elapsed" \
    'BEGIN { printf "%.3f", bytes / 1048576 / elapsed }')"
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$source_backend" "$destination_backend" "$depth" "$repeat" "$size" \
    "$elapsed" "$user" "$system" "$rss" "$cpu" "$throughput" | tee -a "$output"
  rm -f "$metrics" "$log"
}

for source_backend in "${backends[@]}"; do
  for destination_backend in "${backends[@]}"; do
    for depth in 1 2 4 8 16; do
      run_case "$source_backend" "$destination_backend" "$depth" 1
    done
    # Reverse the second pass so systematic temperature/cache drift does not
    # always favor the largest queue depth.
    for depth in 16 8 4 2 1; do
      run_case "$source_backend" "$destination_backend" "$depth" 2
    done
  done
done
