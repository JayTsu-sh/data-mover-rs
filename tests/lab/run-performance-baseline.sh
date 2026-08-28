#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

run_id="${1:?run id required}"
samples="${2:-performance-baseline.csv}"
report="${3:-performance-baseline.json}"
hardware_id="${PERF_HARDWARE_ID:?PERF_HARDWARE_ID must identify the unchanged lab hardware}"
validate_run_id "$run_id"
[[ "$hardware_id" =~ ^[A-Za-z0-9._-]{1,80}$ ]] || {
  echo "invalid PERF_HARDWARE_ID" >&2
  exit 2
}

commit="$(git rev-parse HEAD)"
dataset_id="data-mover-performance-v1"
chunk_bytes=$((2 * 1024 * 1024))
inflight=8
concurrency=1
root="/tmp/data-mover-lab/$run_id/performance-baseline"
large_csv="$(mktemp)"
small_binary="$(cargo metadata --locked --no-deps --format-version 1 | \
  python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')/release/examples/storage_small_benchmark"
trap 'rm -f "$large_csv"' EXIT

tests/lab/run-inflight-benchmark.sh "$run_id" "$large_csv"

header='schema_version,run_id,commit,hardware_id,dataset_id,operation,source,destination,concurrency,chunk_bytes,inflight,repeat,entries,bytes,elapsed_ms,p95_scheduling_latency_ms,max_rss_kib'
printf '%s\n' "$header" > "$samples"
awk -F, -v run="$run_id" -v commit="$commit" -v hardware="$hardware_id" \
  -v dataset="$dataset_id" -v chunk="$chunk_bytes" -v concurrency="$concurrency" \
  -v inflight="$inflight" '
  NR > 1 && $3 == inflight {
    printf "1,%s,%s,%s,%s,copy-large,%s,%s,%s,%s,%s,%s,1,%s,%.3f,,%s\n",
      run, commit, hardware, dataset, $1, $2, concurrency, chunk, inflight, $4,
      $5, $6 * 1000, $9
  }' "$large_csv" >> "$samples"

mkdir -p "$root/small-source" "$root/small-destination"
for index in $(seq 0 99); do
  printf 'fixed-small-entry-%06d\n' "$index" > "$root/small-source/entry-$index.bin"
done
[[ -x "$small_binary" ]] || cargo build --release --locked --example storage_small_benchmark

run_small() {
  local operation="$1"
  local repeat="$2"
  local metrics output elapsed p95 rss bytes destination_label destination=()
  metrics="$(mktemp)"
  output="$(mktemp)"
  if [[ "$operation" == copy ]]; then
    destination=(--destination "$root/small-destination")
    destination_label=local
  else
    destination_label=
  fi
  /usr/bin/time -o "$metrics" -f '%M' env \
    DATA_MOVER_READ_INFLIGHT="$inflight" DATA_MOVER_WRITE_INFLIGHT="$inflight" \
    "$small_binary" --operation "$operation" --source "$root/small-source" \
      "${destination[@]}" --expected-entries 100 --chunk-bytes "$chunk_bytes" > "$output"
  elapsed="$(sed -n 's/.*elapsed_ms=\([^[:space:]]*\).*/\1/p' "$output")"
  p95="$(sed -n 's/.*p95_scheduling_latency_ms=//p' "$output")"
  rss="$(cat "$metrics")"
  bytes="$(find "$root/small-source" -type f -printf '%s\n' | awk '{ total += $1 } END { print total + 0 }')"
  printf '1,%s,%s,%s,%s,%s-small,local,%s,%s,%s,%s,%s,100,%s,%s,%s,%s\n' \
    "$run_id" "$commit" "$hardware_id" "$dataset_id" "$operation" \
    "$destination_label" "$concurrency" "$chunk_bytes" \
    "$inflight" "$repeat" "$bytes" "$elapsed" "$p95" "$rss" >> "$samples"
  rm -f "$metrics" "$output"
}

for repeat in 1 2 3 4 5; do
  run_small scan "$repeat"
  run_small copy "$repeat"
done

python3 tests/lab/performance_baseline.py summarize "$samples" "$report"
