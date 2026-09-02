#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

run_id="${1:?run id required}"
output="${2:-local-copy-comparison.csv}"
repeats="${LOCAL_COPY_PERF_REPEATS:-3}"
selected_payloads="${LOCAL_COPY_PERF_PAYLOADS:-4KiB,40MiB,1GiB}"
max_regression_percent="${LOCAL_COPY_PERF_MAX_REGRESSION_PERCENT:-}"
max_fixed_overhead_ms="${LOCAL_COPY_PERF_MAX_FIXED_OVERHEAD_MS:-5}"
validate_run_id "$run_id"
[[ "$repeats" =~ ^[1-9][0-9]*$ ]] || {
  echo "LOCAL_COPY_PERF_REPEATS must be a positive integer" >&2
  exit 2
}
if [[ -n "$max_regression_percent" ]] &&
  ! [[ "$max_regression_percent" =~ ^([0-9]+([.][0-9]+)?|[.][0-9]+)$ ]]; then
  echo "LOCAL_COPY_PERF_MAX_REGRESSION_PERCENT must be a non-negative number" >&2
  exit 2
fi
[[ "$max_fixed_overhead_ms" =~ ^([0-9]+([.][0-9]+)?|[.][0-9]+)$ ]] || {
  echo "LOCAL_COPY_PERF_MAX_FIXED_OVERHEAD_MS must be a non-negative number" >&2
  exit 2
}

base_root="${LOCAL_COPY_PERF_ROOT:-/tmp/data-mover-local-performance}"
[[ -n "$base_root" && "$base_root" = /* && "$base_root" != / ]] || {
  echo "LOCAL_COPY_PERF_ROOT must be an absolute non-root path" >&2
  exit 2
}
root="$base_root/$run_id"
source_root="$root/source"
destination_root="$root/destination"
target_directory="$(cargo metadata --locked --no-deps --format-version 1 | \
  python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
binary="$target_directory/release/examples/local_copy_comparison"
chunk_bytes=$((2 * 1024 * 1024))
read_inflight=8
write_inflight=8

cleanup() {
  rm -rf -- "$root"
}
trap cleanup EXIT
rm -rf -- "$root"
mkdir -p "$source_root" "$destination_root"
cargo build --release --locked --example local_copy_comparison

IFS=, read -r -a labels <<< "$selected_payloads"
sizes=()
for label in "${labels[@]}"; do
  case "$label" in
    4KiB) sizes+=(4096) ;;
    40MiB) sizes+=($((40 * 1024 * 1024))) ;;
    1GiB) sizes+=($((1024 * 1024 * 1024))) ;;
    *)
      echo "unsupported LOCAL_COPY_PERF_PAYLOADS value: $label" >&2
      exit 2
      ;;
  esac
done
(( ${#labels[@]} > 0 )) || {
  echo "LOCAL_COPY_PERF_PAYLOADS must select at least one payload" >&2
  exit 2
}
for index in "${!sizes[@]}"; do
  fixture="$source_root/${labels[$index]}.bin"
  dd if=/dev/urandom of="$fixture" bs=1M iflag=fullblock \
    count=$(( (${sizes[$index]} + 1024 * 1024 - 1) / (1024 * 1024) )) status=none
  truncate -s "${sizes[$index]}" "$fixture"
done

printf '%s\n' \
  'implementation,payload,repeat,bytes,chunk_bytes,read_inflight,write_inflight,elapsed_ns,elapsed_ms,throughput_mib_s,user_s,system_s,max_rss_kib,cpu_percent' \
  > "$output"

run_case() {
  local implementation="$1" label="$2" size="$3" repeat="$4"
  local destination="$destination_root/$implementation-$label-$repeat"
  local metrics log elapsed_ns elapsed_ms throughput user system rss cpu
  metrics="$(mktemp)"
  log="$(mktemp)"
  mkdir -p "$destination"

  if ! /usr/bin/time -o "$metrics" -f '%U,%S,%M,%P' \
    "$binary" --implementation "$implementation" \
      --source "$source_root" --destination "$destination" --path "$label.bin" \
      --chunk-bytes "$chunk_bytes" --read-inflight "$read_inflight" \
      --write-inflight "$write_inflight" > "$log" 2>&1; then
    cat "$log" >&2
    rm -f "$metrics" "$log"
    return 1
  fi
  elapsed_ns="$(sed -n 's/.*elapsed_ns=\([0-9][0-9]*\).*/\1/p' "$log")"
  [[ -n "$elapsed_ns" ]] || {
    cat "$log" >&2
    echo "benchmark did not report elapsed_ns" >&2
    return 1
  }
  IFS=, read -r user system rss cpu < "$metrics"
  cpu="${cpu%%%}"
  elapsed_ms="$(awk -v ns="$elapsed_ns" 'BEGIN { printf "%.6f", ns / 1000000 }')"
  throughput="$(awk -v bytes="$size" -v ns="$elapsed_ns" \
    'BEGIN { printf "%.3f", bytes / 1048576 / (ns / 1000000000) }')"

  source_sha="$(sha256sum "$source_root/$label.bin" | awk '{print $1}')"
  destination_sha="$(sha256sum "$destination/$label.bin" | awk '{print $1}')"
  [[ "$source_sha" == "$destination_sha" ]] || {
    echo "$implementation $label digest mismatch" >&2
    return 1
  }

  # Isolate samples: the legacy non-resumable path does not include a durability barrier.
  # Flush its dirty pages outside the measured interval so the next implementation does not
  # inherit and pay for the previous sample's writeback debt.
  sync -d "$destination/$label.bin"

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$implementation" "$label" "$repeat" "$size" "$chunk_bytes" \
    "$read_inflight" "$write_inflight" "$elapsed_ns" "$elapsed_ms" \
    "$throughput" "$user" "$system" "$rss" "$cpu" | tee -a "$output"
  rm -rf -- "$destination"
  rm -f "$metrics" "$log"
}

for index in "${!sizes[@]}"; do
  for ((repeat = 1; repeat <= repeats; repeat++)); do
    case $((repeat % 3)) in
      1) implementations=(legacy legacy-durable optimized) ;;
      2) implementations=(legacy-durable optimized legacy) ;;
      0) implementations=(optimized legacy legacy-durable) ;;
    esac
    for implementation in "${implementations[@]}"; do
      run_case "$implementation" "${labels[$index]}" "${sizes[$index]}" "$repeat"
    done
  done
done

python3 - "$output" "$selected_payloads" "$max_regression_percent" \
  "$max_fixed_overhead_ms" <<'PY'
import csv
import statistics
import sys

with open(sys.argv[1], newline="") as source:
    rows = list(csv.DictReader(source))
payloads = sys.argv[2].split(",")
maximum_regression = float(sys.argv[3]) if sys.argv[3] else None
maximum_fixed_overhead = float(sys.argv[4])
regressions = []

print(
    "payload,legacy_median_ms,legacy_durable_median_ms,optimized_median_ms,"
    "optimized_vs_raw,optimized_vs_durable"
)
for payload in payloads:
    medians = {}
    for implementation in ("legacy", "legacy-durable", "optimized"):
        samples = [
            float(row["elapsed_ms"])
            for row in rows
            if row["payload"] == payload and row["implementation"] == implementation
        ]
        if not samples:
            raise SystemExit(f"missing {implementation} samples for {payload}")
        medians[implementation] = statistics.median(samples)
    raw_speedup = medians["legacy"] / medians["optimized"]
    durable_speedup = medians["legacy-durable"] / medians["optimized"]
    print(
        f'{payload},{medians["legacy"]:.3f},{medians["legacy-durable"]:.3f},'
        f'{medians["optimized"]:.3f},{raw_speedup:.3f}x,{durable_speedup:.3f}x'
    )
    if maximum_regression is not None:
        if payload == "4KiB":
            overhead = medians["optimized"] - medians["legacy-durable"]
            if overhead > maximum_fixed_overhead:
                regressions.append(
                    f"{payload}: fixed overhead {overhead:.3f} ms exceeds "
                    f"{maximum_fixed_overhead:.3f} ms"
                )
        else:
            regression = (
                medians["optimized"] / medians["legacy-durable"] - 1.0
            ) * 100.0
            if regression > maximum_regression:
                regressions.append(f"{payload}: {regression:.1f}%")

if regressions:
    raise SystemExit(
        "performance regression exceeds "
        f"{maximum_regression:.1f}%: " + ", ".join(regressions)
    )
PY
