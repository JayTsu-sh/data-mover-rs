#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

run_id="${1:?run id required}"
output="${2:-qos-e2e.csv}"
validate_run_id "$run_id"

# Keep these values deliberately small enough for a nightly gate, while making
# the payload span multiple storage chunks and the burst smaller than every
# backend's normal source-read chunk.
soft_rate_mib="${QOS_LAB_SOFT_RATE_MIB:-8}"
hard_rate_mib="${QOS_LAB_HARD_RATE_MIB:-$soft_rate_mib}"
peak_duration_ms="${QOS_LAB_PEAK_DURATION_MS:-500}"
profile="${QOS_LAB_PROFILE:-bandwidth}"
iops_mode="${QOS_LAB_IOPS_MODE:-strict}"
[[ "$profile" == "bandwidth" || "$profile" == "iops" ]]
[[ "$iops_mode" == "strict" || "$iops_mode" == "soft-hard" ]]
[[ "$soft_rate_mib" =~ ^[1-9][0-9]*$ ]]
[[ "$hard_rate_mib" =~ ^[1-9][0-9]*$ ]]
[[ "$peak_duration_ms" =~ ^[0-9]+$ ]]
((hard_rate_mib >= soft_rate_mib))
soft_rate_bytes=$((soft_rate_mib * 1024 * 1024))
hard_rate_bytes=$((hard_rate_mib * 1024 * 1024))
burst_bytes=$((64 * 1024))
iops_limit=10000
size=$((16 * 1024 * 1024 + 137))

if [[ "${2:-}" == "--contract-only" ]]; then
  [[ "$burst_bytes" -lt $((2 * 1024 * 1024)) ]]
  [[ "$size" -gt $((3 * 5 * 1024 * 1024)) ]]
  echo "QoS lab contract verified: bandwidth and IOPS profiles, 5 source backends, sub-chunk burst, multi-range payload"
  exit 0
fi

prepare_hdfs_kerberos "$run_id"
export LAB_HDFS_RUN_ROOT
LAB_HDFS_RUN_ROOT="$(hdfs_run_root "$run_id")"
require_s3_credentials

local_root="/tmp/data-mover-lab/$run_id"
fixture="$local_root/seed/qos-e2e.bin"
target_directory="$(cargo metadata --locked --no-deps --format-version 1 | \
  python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
binary="$target_directory/release/examples/storage_copy"
mkdir -p "$local_root/source" "$local_root/destination" "$local_root/seed"
if [[ ! -x "$binary" ]] ||
  [[ -n "$(find src examples -type f -newer "$binary" -print -quit)" ]] ||
  [[ Cargo.toml -nt "$binary" || Cargo.lock -nt "$binary" ]]; then
  cargo build --release --locked --example storage_copy
fi

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
        "$LAB_S3_ACCESS_KEY" "$LAB_S3_SECRET_KEY" "$LAB_S3_BUCKET" \
        "$host" "$run_id" "$role"
      ;;
    hdfs) printf '%s/qos/%s' "$LAB_HDFS_RUN_ROOT" "$role" ;;
  esac
}

# Deterministic, non-sparse fixture; storage_copy independently verifies the
# complete content while publishing each destination.
python3 - "$fixture" "$size" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
remaining = int(sys.argv[2])
state = 0x9e3779b97f4a7c15
block = bytearray(1024 * 1024)
with path.open("wb") as output:
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
fixture_hash="$(sha256sum "$fixture" | cut -d' ' -f1)"

backends=(local nfs3 nfs41 s3 hdfs)
for backend in "${backends[@]}"; do
  key="qos-${backend}.bin"
  if [[ "$backend" == "local" ]]; then
    cp "$fixture" "$local_root/source/$key"
  elif [[ "$backend" == "s3" ]]; then
    python3 "$(dirname "$0")/s3_helper.py" put-file \
      --endpoint "$LAB_SOURCE_DATA" --bucket "$LAB_S3_BUCKET" \
      --key "ci/$run_id/source/$key" --file "$fixture"
  else
    cp "$fixture" "$local_root/seed/$key"
    "$binary" --source "$local_root/seed" \
      --destination "$(storage_url source "$backend")" --path "$key"
  fi
done

printf '%s\n' \
  'profile,source,soft_mibps,hard_mibps,bandwidth_peak_duration_ms,soft_iops,hard_iops,iops_peak_duration_ms,bytes,baseline_seconds,baseline_mibps,baseline_iops,qos_seconds,qos_mibps,qos_total_bytes,qos_total_iops,qos_actual_iops,expected_iops' \
  > "$output"

for backend in "${backends[@]}"; do
  key="qos-${backend}.bin"
  baseline_metrics="$(mktemp)"
  baseline_log="$(mktemp)"
  qos_metrics="$(mktemp)"
  qos_log="$(mktemp)"

  if ! /usr/bin/time -o "$baseline_metrics" -f '%e' \
    "$binary" --source "$(storage_url source "$backend")" \
      --destination "$(storage_url destination local)" --path "$key" \
      >"$baseline_log" 2>&1; then
    echo "$backend baseline copy failed" >&2
    cat "$baseline_log" >&2
    exit 1
  fi
  baseline_seconds="$(cat "$baseline_metrics")"
  baseline_mibps="$(awk -v bytes="$size" -v elapsed="$baseline_seconds" \
    'BEGIN { printf "%.6f", bytes / 1048576 / elapsed }')"

  if [[ "$backend" == "s3" ]]; then
    expected_iops=$(((size + 5 * 1024 * 1024 - 1) / (5 * 1024 * 1024)))
  else
    expected_iops=$(((size + burst_bytes - 1) / burst_bytes))
  fi
  baseline_iops="$(awk -v operations="$expected_iops" -v elapsed="$baseline_seconds" \
    'BEGIN { printf "%.6f", operations / elapsed }')"

  if [[ "$profile" == "iops" ]]; then
    # The high bandwidth ceiling keeps bandwidth shaping out of the critical
    # path. The small request cap gives stream/file backends enough protocol
    # operations to measure; S3 deliberately retains its natural 5 MiB GETs.
    soft_rate_mib=256
    hard_rate_mib=256
    peak_duration_ms=0
    if [[ "$backend" == "s3" ]]; then
      iops_limit=2
    else
      iops_limit=128
    fi
    hard_iops_limit="$iops_limit"
    iops_peak_duration_ms=0
    if [[ "$iops_mode" == "soft-hard" ]]; then
      hard_iops_limit=$((iops_limit * 2))
      iops_peak_duration_ms=500
    fi
  else
    iops_limit=10000
    hard_iops_limit="$iops_limit"
    iops_peak_duration_ms=0
  fi
  soft_rate_bytes=$((soft_rate_mib * 1024 * 1024))
  hard_rate_bytes=$((hard_rate_mib * 1024 * 1024))

  # A slow source can satisfy a QoS wall-clock bound without the limiter doing
  # any work. Treat that environment as inconclusive instead of a false pass.
  if [[ "$profile" == "bandwidth" ]]; then
    awk -v measured="$baseline_mibps" -v configured="$hard_rate_mib" \
      'BEGIN { exit !(measured >= configured * 1.25) }' || {
        echo "$backend baseline ${baseline_mibps} MiB/s is too slow to validate ${hard_rate_mib} MiB/s hard QoS" >&2
        exit 1
      }
  else
    awk -v measured="$baseline_iops" -v configured="$iops_limit" \
      'BEGIN { exit !(measured >= configured * 1.25) }' || {
        echo "$backend baseline ${baseline_iops} IOPS is too slow to validate ${iops_limit} IOPS QoS" >&2
        exit 1
      }
  fi

  qos_args=(
    --qos-bandwidth "${soft_rate_mib}MiB/s"
    --qos-burst-bytes "$burst_bytes"
    --qos-iops "$iops_limit"
  )
  if [[ "$profile" == "iops" ]]; then
    if ((hard_iops_limit > iops_limit)); then
      qos_args+=(
        --qos-hard-iops "$hard_iops_limit"
        --qos-iops-peak-duration-ms "$iops_peak_duration_ms"
      )
    fi
  elif ((hard_rate_mib > soft_rate_mib)); then
    qos_args+=(
      --qos-hard-bandwidth "${hard_rate_mib}MiB/s"
      --qos-peak-duration-ms "$peak_duration_ms"
    )
  fi
  if ! /usr/bin/time -o "$qos_metrics" -f '%e' \
    "$binary" --source "$(storage_url source "$backend")" \
      --destination "$(storage_url destination local)" --path "$key" \
      "${qos_args[@]}" \
      >"$qos_log" 2>&1; then
    echo "$backend QoS copy failed" >&2
    cat "$qos_log" >&2
    exit 1
  fi
  qos_seconds="$(cat "$qos_metrics")"
  stats_line="$(awk -F '\t' '$1 == "qos_stats" { print; found=1 } END { exit !found }' "$qos_log")"
  qos_total_bytes="$(awk -F '[=\t]' '{print $3}' <<<"$stats_line")"
  qos_total_iops="$(awk -F '[=\t]' '{print $5}' <<<"$stats_line")"
  qos_mibps="$(awk -v bytes="$size" -v elapsed="$qos_seconds" \
    'BEGIN { printf "%.6f", bytes / 1048576 / elapsed }')"
  destination_hash="$(sha256sum "$local_root/destination/$key" | cut -d' ' -f1)"

  [[ "$qos_total_bytes" == "$size" ]] || {
    echo "$backend source byte count mismatch: expected $size, got $qos_total_bytes" >&2
    exit 1
  }
  [[ "$destination_hash" == "$fixture_hash" ]] || {
    echo "$backend QoS destination checksum mismatch" >&2
    exit 1
  }
  [[ "$qos_total_iops" == "$expected_iops" ]] || {
    echo "$backend source IOPS mismatch: expected $expected_iops, got $qos_total_iops" >&2
    cat "$qos_log" >&2
    exit 1
  }
  qos_actual_iops="$(awk -v operations="$qos_total_iops" -v elapsed="$qos_seconds" \
    'BEGIN { printf "%.6f", operations / elapsed }')"
  if [[ "$profile" == "bandwidth" ]]; then
    # External elapsed time includes setup and destination verification, so it
    # may be slower but must never imply throughput above the hard source limit.
    awk -v elapsed="$qos_seconds" -v bytes="$size" -v rate="$hard_rate_bytes" \
      'BEGIN { minimum = bytes / rate; exit !(elapsed + 0.02 >= minimum) }'
    awk -v elapsed="$qos_seconds" -v bytes="$size" -v rate="$soft_rate_bytes" \
      'BEGIN { expected = bytes / rate; exit !(elapsed <= expected * 3 + 5) }' || {
        echo "$backend bandwidth QoS run was unexpectedly slow (${qos_seconds}s)" >&2
        exit 1
      }
  else
    # The first operation is immediately available; every later protocol
    # operation occupies one strict IOPS schedule slot.
    awk -v elapsed="$qos_seconds" -v operations="$qos_total_iops" -v rate="$hard_iops_limit" \
      'BEGIN { minimum = (operations - 1) / rate; exit !(elapsed + 0.02 >= minimum) }'
    awk -v elapsed="$qos_seconds" -v operations="$qos_total_iops" -v rate="$iops_limit" \
      'BEGIN { expected = (operations - 1) / rate; exit !(elapsed <= expected * 3 + 5) }' || {
        echo "$backend IOPS QoS run was unexpectedly slow (${qos_seconds}s)" >&2
        exit 1
      }
  fi

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$profile" "$backend" "$soft_rate_mib" "$hard_rate_mib" "$peak_duration_ms" \
    "$iops_limit" "$hard_iops_limit" "$iops_peak_duration_ms" "$size" \
    "$baseline_seconds" "$baseline_mibps" "$baseline_iops" \
    "$qos_seconds" "$qos_mibps" "$qos_total_bytes" "$qos_total_iops" \
    "$qos_actual_iops" "$expected_iops" | tee -a "$output"
  rm -f "$baseline_metrics" "$baseline_log" "$qos_metrics" "$qos_log"
done

echo "real-backend source $profile QoS verified; measurements: $output"
