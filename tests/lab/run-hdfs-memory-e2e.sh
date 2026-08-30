#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

run_id="${1:?run id required}"
output="${2:-hdfs-memory.csv}"
validate_run_id "$run_id"
prepare_hdfs_kerberos "$run_id"
export LAB_HDFS_RUN_ROOT
LAB_HDFS_RUN_ROOT="$(hdfs_run_root "$run_id")"

local_root="${RUNNER_TEMP:-/tmp}/data-mover-lab/$run_id/hdfs-memory"
source_root="$local_root/source"
destination_root="$local_root/destination"
target_directory="$(cargo metadata --locked --no-deps --format-version 1 | \
  python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
commit="$(git rev-parse HEAD)"
binary="$target_directory/release/examples/storage_copy"
inspector="$target_directory/release/examples/storage_inspect"
mkdir -p "$source_root" "$destination_root"
cargo build --release --locked --example storage_copy --example storage_inspect

small_size=$((256 * 1024 * 1024 + 137))
large_size=$((2 * 1024 * 1024 * 1024 + 137))
scale_small_size=$((1024 * 1024 * 1024 + 137))
scale_large_size=$((100 * 1024 * 1024 * 1024 + 137))

generate_fixture() {
  local path="$1"
  local size="$2"
  python3 - "$path" "$size" <<'PY'
import hashlib
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
size = int(sys.argv[2])
block_size = 1024 * 1024
with path.open("wb") as output:
    block_index = 0
    remaining = size
    while remaining:
        digest = hashlib.blake2b(block_index.to_bytes(8, "little")).digest()
        block = (digest * (block_size // len(digest) + 1))[:block_size]
        length = min(block_size, remaining)
        output.write(block[:length])
        remaining -= length
        block_index += 1
PY
}

for specification in "small:$small_size" "large:$large_size"; do
  label="${specification%%:*}"
  size="${specification#*:}"
  generate_fixture "$source_root/$label.bin" "$size"
  "$binary" --source "$source_root" \
    --destination "$LAB_HDFS_RUN_ROOT/memory/source" --path "$label.bin"
done

for specification in "scale-1g:$scale_small_size" "scale-100g:$scale_large_size"; do
  label="${specification%%:*}"
  size="${specification#*:}"
  truncate -s "$size" "$source_root/$label.bin"
  "$binary" --source "$source_root" \
    --destination "$LAB_HDFS_RUN_ROOT/memory/source" --path "$label.bin"
done

printf '%s\n' \
  'run_id,commit,sample_set,profile,direction,payload,bytes,read_inflight,write_inflight,chunk_mib,channel_chunks,file_concurrency,budget_mib,elapsed_s,max_rss_kib,throughput_mib_s' \
  > "$output"

run_case() {
  local sample_set="$1" profile="$2" direction="$3" payload="$4" size="$5" read="$6" write="$7"
  local source destination destination_probe expected_probe metrics log elapsed rss throughput budget
  case "$direction" in
    local-hdfs)
      source="$source_root"
      destination="$LAB_HDFS_RUN_ROOT/memory/$profile/local-hdfs-$payload"
      ;;
    hdfs-hdfs)
      source="$LAB_HDFS_RUN_ROOT/memory/source"
      destination="$LAB_HDFS_RUN_ROOT/memory/$profile/hdfs-hdfs-$payload"
      ;;
    hdfs-local)
      source="$LAB_HDFS_RUN_ROOT/memory/source"
      destination="$destination_root/$profile/hdfs-local-$payload"
      ;;
  esac
  if [[ "$direction" == "hdfs-local" ]]; then
    mkdir -p "$destination"
  fi
  metrics="$(mktemp)"
  log="$(mktemp)"
  if ! /usr/bin/time -o "$metrics" -f '%e,%M' \
    env DATA_MOVER_READ_INFLIGHT="$read" DATA_MOVER_WRITE_INFLIGHT="$write" \
      DATA_MOVER_HDFS_READ_INFLIGHT="$read" DATA_MOVER_HDFS_WRITE_INFLIGHT="$write" \
      "$binary" --source "$source" --destination "$destination" \
        --path "$payload.bin" > "$log" 2>&1; then
    sed -E 's#hdfs://[^@]+@#hdfs://<redacted>@#g' "$log" >&2
    return 1
  fi
  IFS=, read -r elapsed rss < "$metrics"
  expected_probe="$("$inspector" --storage "$source_root" --path "$payload.bin")"
  destination_probe="$("$inspector" --storage "$destination" --path "$payload.bin")"
  [[ "$destination_probe" == "$expected_probe" ]]
  throughput="$(awk -v bytes="$size" -v elapsed="$elapsed" \
    'BEGIN { printf "%.3f", bytes / 1048576 / elapsed }')"
  budget="$(python3 -c \
    'import sys; sys.path.insert(0, "tests"); from hdfs_memory_budget import budget_mib; print(budget_mib(1, 2, int(sys.argv[1]), int(sys.argv[2])))' \
    "$read" "$write")"
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,2,4,1,%s,%s,%s,%s\n' \
    "$run_id" "$commit" "$sample_set" "$profile" "$direction" "$payload" "$size" "$read" "$write" \
    "$budget" "$elapsed" "$rss" "$throughput" | tee -a "$output"
}

for profile_spec in serial:1:1 default:4:1 high:8:16; do
  IFS=: read -r profile read_inflight write_inflight <<< "$profile_spec"
  for direction in local-hdfs hdfs-hdfs hdfs-local; do
    run_case baseline "$profile" "$direction" small "$small_size" "$read_inflight" "$write_inflight"
    run_case baseline "$profile" "$direction" large "$large_size" "$read_inflight" "$write_inflight"
  done
done

run_case scale high hdfs-hdfs scale-1g "$scale_small_size" 8 16
run_case scale high hdfs-hdfs scale-100g "$scale_large_size" 8 16

python3 tests/hdfs_memory_budget.py --require-100-gib "$output"
