#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
output=${LOCAL_WRITE_BENCHMARK_OUTPUT:-/tmp/local-write-benchmark.csv}
total_bytes=${LOCAL_WRITE_BENCHMARK_TOTAL_BYTES:-268435456}
rounds=${LOCAL_WRITE_BENCHMARK_ROUNDS:-3}
roots=${LOCAL_WRITE_BENCHMARK_ROOTS:-"ext4=/tmp xfs=/work tmpfs=/dev/shm"}
chunks=${LOCAL_WRITE_BENCHMARK_CHUNKS:-"65536 262144 1048576 2097152 4194304 8388608"}
inflight_values=${LOCAL_WRITE_BENCHMARK_INFLIGHT:-"1 2 4 8"}
target_dir=$(
  cargo metadata --no-deps --format-version 1 --manifest-path "$repo_root/Cargo.toml" \
    | python3 -c 'import json, sys; print(json.load(sys.stdin)["target_directory"])'
)
binary="$target_dir/release/examples/local_write_benchmark"

cargo build --release --example local_write_benchmark --manifest-path "$repo_root/Cargo.toml"
mkdir -p "$(dirname "$output")"
printf '%s\n' 'filesystem,root,round,total_bytes,chunk_bytes,inflight,submit_ns,sync_ns,total_ns,write_calls,short_writes,peak_rss_kib' >"$output"

for root_spec in $roots; do
  label=${root_spec%%=*}
  root=${root_spec#*=}
  if [[ "$label" == "$root_spec" || ! -d "$root" || ! -w "$root" ]]; then
    echo "invalid writable benchmark root: $root_spec" >&2
    exit 1
  fi
  scratch=$(mktemp -d "$root/data-mover-local-write.XXXXXX")
  trap 'rm -f "$scratch"/sample-*.bin; rmdir "$scratch" 2>/dev/null || true' EXIT

  "$binary" --file "$scratch/sample-warmup.bin" --total-bytes "$total_bytes" --chunk-bytes 2097152 --inflight 4 >/dev/null
  for ((round = 1; round <= rounds; round++)); do
    for chunk in $chunks; do
      for inflight in $inflight_values; do
        sample=$(
          "$binary" \
            --file "$scratch/sample-${round}-${chunk}-${inflight}.bin" \
            --total-bytes "$total_bytes" \
            --chunk-bytes "$chunk" \
            --inflight "$inflight"
        )
        declare -A fields=()
        while IFS='=' read -r key value; do
          fields["$key"]=$value
        done < <(tr '\t' '\n' <<<"$sample")
        printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
          "$label" "$root" "$round" \
          "${fields[total_bytes]}" "${fields[chunk_bytes]}" "${fields[inflight]}" \
          "${fields[submit_ns]}" "${fields[sync_ns]}" "${fields[total_ns]}" \
          "${fields[write_calls]}" "${fields[short_writes]}" "${fields[peak_rss_kib]}" \
          >>"$output"
      done
    done
  done
  rmdir "$scratch"
  trap - EXIT
done

echo "$output"
