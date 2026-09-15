#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

run_id="${1:?run id required}"
output="${2:-nfs-copy-comparison.csv}"
repeats="${NFS_COPY_PERF_REPEATS:-3}"
selected_payloads="${NFS_COPY_PERF_PAYLOADS:-4KiB,4MiB,40MiB,1GiB}"
max_regression_percent="${NFS_COPY_PERF_MAX_REGRESSION_PERCENT:-}"
recovery="${NFS_COPY_PERF_RECOVERY:-checkpointed}"
read_inflight="${NFS_COPY_PERF_READ_INFLIGHT:-8}"
write_inflight="${NFS_COPY_PERF_WRITE_INFLIGHT:-8}"
validate_run_id "$run_id"
[[ "$repeats" =~ ^[1-9][0-9]*$ ]] || {
  echo "NFS_COPY_PERF_REPEATS must be a positive integer" >&2
  exit 2
}
if [[ -n "$max_regression_percent" ]] &&
  ! [[ "$max_regression_percent" =~ ^([0-9]+([.][0-9]+)?|[.][0-9]+)$ ]]; then
  echo "NFS_COPY_PERF_MAX_REGRESSION_PERCENT must be a non-negative number" >&2
  exit 2
fi
[[ "$read_inflight" =~ ^[1-9][0-9]*$ ]] || {
  echo "NFS_COPY_PERF_READ_INFLIGHT must be a positive integer" >&2
  exit 2
}
[[ "$write_inflight" =~ ^[1-9][0-9]*$ ]] || {
  echo "NFS_COPY_PERF_WRITE_INFLIGHT must be a positive integer" >&2
  exit 2
}

local_root="${NFS_COPY_PERF_LOCAL_ROOT:-/tmp/data-mover-nfs-performance}/$run_id"
[[ -n "$local_root" && "$local_root" = /* && "$local_root" != / ]] || {
  echo "NFS_COPY_PERF_LOCAL_ROOT must resolve to an absolute non-root path" >&2
  exit 2
}
target_directory="$(cargo metadata --locked --no-deps --format-version 1 | \
  python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
binary="$target_directory/release/examples/nfs_copy_comparison"
seed_binary="$target_directory/release/examples/storage_copy"
source_url="nfs://$LAB_SOURCE_DATA$LAB_NFS41_EXPORT:/ci/$run_id/source?version=4.1&noresvport=true"
destination_root_url="nfs://$LAB_DEST_DATA$LAB_NFS41_EXPORT:/ci/$run_id/destination?version=4.1&noresvport=true"
chunk_bytes=$((2 * 1024 * 1024))
export DATA_MOVER_NFS_READ_INFLIGHT="$read_inflight"
export DATA_MOVER_NFS_WRITE_INFLIGHT="$write_inflight"

cleanup() {
  if [[ -x "$binary" ]]; then
    "$binary" cleanup --url "$source_url" >/dev/null 2>&1 || true
    "$binary" cleanup --url "$destination_root_url" >/dev/null 2>&1 || true
  fi
  rm -rf -- "$local_root"
}
trap cleanup EXIT
rm -rf -- "$local_root"
mkdir -p "$local_root"
cargo build --release --locked --example nfs_copy_comparison --example storage_copy

IFS=, read -r -a labels <<< "$selected_payloads"
sizes=()
for label in "${labels[@]}"; do
  case "$label" in
    4KiB) sizes+=(4096) ;;
    4MiB) sizes+=($((4 * 1024 * 1024))) ;;
    40MiB) sizes+=($((40 * 1024 * 1024))) ;;
    1GiB) sizes+=($((1024 * 1024 * 1024))) ;;
    *)
      echo "unsupported NFS_COPY_PERF_PAYLOADS value: $label" >&2
      exit 2
      ;;
  esac
done
(( ${#labels[@]} > 0 )) || {
  echo "NFS_COPY_PERF_PAYLOADS must select at least one payload" >&2
  exit 2
}

for index in "${!sizes[@]}"; do
  fixture="$local_root/${labels[$index]}.bin"
  dd if=/dev/urandom of="$fixture" bs=1M iflag=fullblock \
    count=$(( (${sizes[$index]} + 1024 * 1024 - 1) / (1024 * 1024) )) status=none
  truncate -s "${sizes[$index]}" "$fixture"
  "$seed_binary" --source "$local_root" --destination "$source_url" \
    --path "${labels[$index]}.bin" --chunk-bytes "$chunk_bytes"
done

printf '%s\n' \
  'implementation,payload,repeat,bytes,chunk_bytes,read_inflight,write_inflight,elapsed_ns,elapsed_ms,throughput_mib_s,user_s,system_s,max_rss_kib,cpu_percent' \
  > "$output"

run_case() {
  local implementation="$1" label="$2" size="$3" repeat="$4"
  local destination_url metrics log elapsed_ns elapsed_ms throughput user system rss cpu
  destination_url="nfs://$LAB_DEST_DATA$LAB_NFS41_EXPORT:/ci/$run_id/destination/$implementation-$label-$repeat?version=4.1&noresvport=true"
  metrics="$(mktemp)"
  log="$(mktemp)"

  if ! /usr/bin/time -o "$metrics" -f '%U,%S,%M,%P' \
    "$binary" copy --implementation "$implementation" \
      --source "$source_url" --destination "$destination_url" --path "$label.bin" \
      --chunk-bytes "$chunk_bytes" --read-inflight "$read_inflight" \
      --write-inflight "$write_inflight" --transfer-policy "$recovery" > "$log" 2>&1; then
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

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$implementation" "$label" "$repeat" "$size" "$chunk_bytes" \
    "$read_inflight" "$write_inflight" "$elapsed_ns" "$elapsed_ms" \
    "$throughput" "$user" "$system" "$rss" "$cpu" | tee -a "$output"
  "$binary" cleanup --url "$destination_url"
  rm -f "$metrics" "$log"
}

for index in "${!sizes[@]}"; do
  for ((repeat = 1; repeat <= repeats; repeat++)); do
    if (( repeat % 2 == 1 )); then
      implementations=(legacy optimized)
    else
      implementations=(optimized legacy)
    fi
    for implementation in "${implementations[@]}"; do
      run_case "$implementation" "${labels[$index]}" "${sizes[$index]}" "$repeat"
    done
  done
done

python3 - "$output" "$selected_payloads" "$max_regression_percent" <<'PY'
import csv
import statistics
import sys

with open(sys.argv[1], newline="") as source:
    rows = list(csv.DictReader(source))
payloads = sys.argv[2].split(",")
maximum_regression = float(sys.argv[3]) if sys.argv[3] else None
regressions = []

print("payload,legacy_median_ms,optimized_median_ms,optimized_vs_legacy")
for payload in payloads:
    medians = {}
    for implementation in ("legacy", "optimized"):
        samples = [
            float(row["elapsed_ms"])
            for row in rows
            if row["payload"] == payload and row["implementation"] == implementation
        ]
        if not samples:
            raise SystemExit(f"missing {implementation} samples for {payload}")
        medians[implementation] = statistics.median(samples)
    speedup = medians["legacy"] / medians["optimized"]
    print(
        f'{payload},{medians["legacy"]:.3f},{medians["optimized"]:.3f},'
        f'{speedup:.3f}x'
    )
    if maximum_regression is not None:
        regression = (medians["optimized"] / medians["legacy"] - 1.0) * 100.0
        if regression > maximum_regression:
            regressions.append(f"{payload}: {regression:.1f}%")

if regressions:
    raise SystemExit(
        "performance regression exceeds "
        f"{maximum_regression:.1f}%: " + ", ".join(regressions)
    )
PY
