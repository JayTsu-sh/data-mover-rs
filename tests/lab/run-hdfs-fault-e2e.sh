#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/hdfs-fault-common.sh"

run_id="${1:?run id required}"
artifact="${2:-hdfs-fault.log}"
validate_run_id "$run_id"
prepare_hdfs_kerberos "$run_id"
export LAB_HDFS_RUN_ROOT
LAB_HDFS_RUN_ROOT="$(hdfs_run_root "$run_id")"
: >"$artifact"

test_status=0
cleanup_fault_run() {
  local original_status="$1" cleanup_status=0
  restore_hdfs_services || cleanup_status=1
  cargo test --test hdfs_native_contract nightly_lab_cleanup_confined_run_root -- \
    --ignored --exact --test-threads=1 || cleanup_status=1
  if (( original_status != 0 )); then exit "$original_status"; fi
  exit "$cleanup_status"
}
trap 'cleanup_fault_run $?' EXIT

for target in namenode datanode1 datanode2; do
  validate_hdfs_fault_target "$target"
done
restore_hdfs_services

local_root="/tmp/data-mover-lab/$run_id/hdfs-fault"
mkdir -p "$local_root/source"
fixture="$local_root/source/replicated.bin"
python3 - "$fixture" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
chunk = bytes(range(251)) * 4096
remaining = 256 * 1024 * 1024 + 137
with path.open("wb") as output:
    while remaining:
        piece = chunk[:min(remaining, len(chunk))]
        output.write(piece)
        remaining -= len(piece)
PY

target_directory="$(cargo metadata --locked --no-deps --format-version 1 | \
  python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
cargo build --locked --example storage_copy --example storage_inspect \
  --example hdfs_fault_mutation
copy_binary="$target_directory/debug/examples/storage_copy"
inspect_binary="$target_directory/debug/examples/storage_inspect"
mutation_binary="$target_directory/debug/examples/hdfs_fault_mutation"
hdfs_storage="$LAB_HDFS_RUN_ROOT/fault"

redact_log() {
  sed -E 's#(hdfs://)[^@[:space:]]+@#\1<redacted>@#g' "$1"
}

run_expect_failure() {
  local label="$1" timeout_seconds="$2" log_file="$3" status
  shift 3
  set +e
  timeout "${timeout_seconds}s" "$@" >"$log_file" 2>&1
  status=$?
  set -e
  if (( status == 0 )); then
    echo "$label unexpectedly succeeded" >&2
    return 1
  fi
  if (( status == 124 )); then
    echo "$label did not return within ${timeout_seconds}s" >&2
    redact_log "$log_file" >&2
    return 1
  fi
  redact_log "$log_file" >>"$artifact"
  if grep -Fq "$LAB_HDFS_RUN_ROOT" "$log_file"; then
    echo "$label exposed the absolute HDFS run root" >&2
    return 1
  fi
}

wait_for_hdfs_write() {
  local attempt readiness_path
  for attempt in {1..60}; do
    # An outage can leave the previous probe path waiting for lease recovery.
    # Use a fresh path so this checks whether new writes work after recovery.
    readiness_path="readiness-${BASHPID}-${attempt}.bin"
    if timeout 15s "$mutation_binary" --storage "$hdfs_storage" \
      --path "$readiness_path" --phase seed >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "HDFS data plane did not become writable" >&2
  return 1
}

kill_mutation_at_checkpoint() {
  local phase="$1" target="$2" path="$3" destination="${4:-}"
  local ready="$local_root/$phase.ready" go="$local_root/$phase.go"
  local log_file="$local_root/$phase.log" pid status
  local -a args
  rm -f -- "$ready" "$go"
  args=(--storage "$hdfs_storage" --path "$path" --phase "$phase" \
    --ready-file "$ready" --go-file "$go")
  if [[ -n "$destination" ]]; then
    args+=(--destination-path "$destination")
  fi
  "$mutation_binary" "${args[@]}" >"$log_file" 2>&1 &
  pid=$!
  for _ in {1..400}; do
    [[ -f "$ready" ]] && break
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.05
  done
  if [[ ! -f "$ready" ]]; then
    redact_log "$log_file" >&2
    wait "$pid" 2>/dev/null || true
    echo "$phase mutation did not reach its checkpoint" >&2
    return 1
  fi
  if [[ "$target" == datanodes ]]; then
    hdfs_service_action stop datanode1
    hdfs_service_action stop datanode2
    wait_hdfs_service datanode1 inactive
    wait_hdfs_service datanode2 inactive
  else
    hdfs_service_action stop "$target"
    wait_hdfs_service "$target" inactive
  fi
  kill -STOP "$pid"
  : >"$go"
  kill -KILL "$pid" 2>/dev/null || true
  set +e
  wait "$pid"
  status=$?
  set -e
  if (( status != 137 )); then
    redact_log "$log_file" >&2
    echo "$phase mutation was not terminated by SIGKILL (status $status)" >&2
    return 1
  fi
  restore_hdfs_services
  wait_for_hdfs_write
  echo "$phase checkpoint: client SIGKILL and cluster recovery verified" >>"$artifact"
}

wait_for_inspect() {
  local path="$1" output
  for _ in {1..90}; do
    if output="$("$inspect_binary" --storage "$hdfs_storage" --path "$path" 2>/dev/null)"; then
      printf '%s\n' "$output"
      return 0
    fi
    sleep 1
  done
  echo "HDFS path did not become independently readable: $path" >&2
  return 1
}

wait_for_hdfs_write
"$copy_binary" --source "$local_root/source" --destination "$hdfs_storage" \
  --path replicated.bin
expected="$("$inspect_binary" --storage "$local_root/source" --path replicated.bin)"

single_output="$local_root/single-datanode.out"
single_error="$local_root/single-datanode.err"
single_ready="$local_root/single-datanode.ready"
single_go="$local_root/single-datanode.go"
rm -f -- "$single_ready" "$single_go"
"$inspect_binary" --storage "$hdfs_storage" --path replicated.bin \
  --ready-file "$single_ready" --go-file "$single_go" \
  --chunk-delay-ms 500 \
  >"$single_output" 2>"$single_error" &
inspect_pid=$!
for _ in {1..600}; do
  [[ -f "$single_ready" ]] && break
  kill -0 "$inspect_pid" 2>/dev/null || break
  sleep 0.05
done
if [[ ! -f "$single_ready" ]]; then
  kill "$inspect_pid" 2>/dev/null || true
  wait "$inspect_pid" 2>/dev/null || true
  echo "replica failover read did not reach its start barrier" >&2
  exit 1
fi
: >"$single_go"
active_datanode=
for _ in {1..2000}; do
  sockets="$(ss -Hnt 2>/dev/null | grep -E \
    "(${LAB_HDFS_DATANODE1_DATA//./\\.}|${LAB_HDFS_DATANODE2_DATA//./\\.}):9866" || true)"
  if grep -Fq "$LAB_HDFS_DATANODE1_DATA:9866" <<<"$sockets"; then
    active_datanode=datanode1
    break
  fi
  if grep -Fq "$LAB_HDFS_DATANODE2_DATA:9866" <<<"$sockets"; then
    active_datanode=datanode2
    break
  fi
  kill -0 "$inspect_pid" 2>/dev/null || break
  sleep 0.005
done
if [[ -z "$active_datanode" ]]; then
  kill "$inspect_pid" 2>/dev/null || true
  wait "$inspect_pid" 2>/dev/null || true
  echo "could not resolve the active DataNode connection" >&2
  exit 1
fi
hdfs_service_action stop "$active_datanode"
wait_hdfs_service "$active_datanode" inactive
for _ in {1..900}; do
  kill -0 "$inspect_pid" 2>/dev/null || break
  sleep 0.1
done
if kill -0 "$inspect_pid" 2>/dev/null; then
  kill "$inspect_pid" 2>/dev/null || true
  wait "$inspect_pid" 2>/dev/null || true
  echo "replica failover read did not finish within 90 seconds" >&2
  exit 1
fi
wait "$inspect_pid"
actual="$(cat "$single_output")"
[[ "$actual" == "$expected" ]]
echo "active DataNode outage: replica failover verified" >>"$artifact"
hdfs_service_action start "$active_datanode"
wait_hdfs_service "$active_datanode" active

hdfs_service_action stop datanode1
hdfs_service_action stop datanode2
wait_hdfs_service datanode1 inactive
wait_hdfs_service datanode2 inactive
run_expect_failure "all DataNodes unavailable" 45 "$local_root/all-datanodes.log" \
  "$inspect_binary" --storage "$hdfs_storage" --path replicated.bin
hdfs_service_action start datanode1
hdfs_service_action start datanode2
wait_hdfs_service datanode1 active
wait_hdfs_service datanode2 active
wait_hdfs_cluster_ready
wait_for_hdfs_write
actual="$(timeout 90s "$inspect_binary" --storage "$hdfs_storage" --path replicated.bin)"
[[ "$actual" == "$expected" ]]
echo "all DataNodes outage: bounded failure and recovery verified" >>"$artifact"

hdfs_service_action stop namenode
wait_hdfs_service namenode inactive
run_expect_failure "NameNode unavailable" 35 "$local_root/namenode.log" \
  "$inspect_binary" --storage "$hdfs_storage" --path replicated.bin
hdfs_service_action start namenode
wait_hdfs_service namenode active
wait_hdfs_cluster_ready
wait_for_hdfs_write
actual="$(timeout 90s "$inspect_binary" --storage "$hdfs_storage" --path replicated.bin)"
[[ "$actual" == "$expected" ]]
echo "NameNode outage: bounded failure and recovery verified" >>"$artifact"

kill_mutation_at_checkpoint create namenode create-killed.bin
run_expect_failure "killed create final path" 15 "$local_root/create-inspect.log" \
  "$inspect_binary" --storage "$hdfs_storage" --path create-killed.bin

kill_mutation_at_checkpoint close namenode close-killed.bin
"$inspect_binary" --storage "$hdfs_storage" --path close-killed.bin >/dev/null

"$mutation_binary" --storage "$hdfs_storage" --path append-killed.bin --phase seed
before_append="$("$inspect_binary" --storage "$hdfs_storage" --path append-killed.bin)"
kill_mutation_at_checkpoint append datanodes append-killed.bin
after_append="$(wait_for_inspect append-killed.bin)"
before_size="${before_append%%$'\t'*}"
after_size="${after_append%%$'\t'*}"
(( after_size >= before_size && after_size <= before_size * 2 ))

"$mutation_binary" --storage "$hdfs_storage" --path rename-source.bin --phase seed
rename_source="$("$inspect_binary" --storage "$hdfs_storage" --path rename-source.bin)"
kill_mutation_at_checkpoint rename namenode rename-source.bin rename-destination.bin
[[ "$("$inspect_binary" --storage "$hdfs_storage" --path rename-source.bin)" == "$rename_source" ]]
run_expect_failure "killed rename destination" 15 "$local_root/rename-inspect.log" \
  "$inspect_binary" --storage "$hdfs_storage" --path rename-destination.bin

"$mutation_binary" --storage "$hdfs_storage" --path metadata-killed.bin --phase seed
kill_mutation_at_checkpoint metadata namenode metadata-killed.bin
mode="$("$inspect_binary" --storage "$hdfs_storage" --path metadata-killed.bin --print-mode)"
[[ "$mode" == 644 ]]
