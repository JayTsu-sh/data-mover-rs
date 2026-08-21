#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

run_id="${1:?run id required}"
validate_run_id "$run_id"
prepare_hdfs_kerberos "$run_id"
require_s3_credentials
export LAB_HDFS_RUN_ROOT
LAB_HDFS_RUN_ROOT="$(hdfs_run_root "$run_id")"

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
    hdfs) printf '%s/%s' "$LAB_HDFS_RUN_ROOT" "$role" ;;
  esac
}

seed_source() {
  local backend="$1"
  local key="$2"
  local seed_path="$local_root/seed/$key"

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
' "$seed_path"

  if [[ "$backend" == "local" ]]; then
    cp "$seed_path" "$local_root/source/$key"
    return
  fi

  # Seed remote sources through data-mover instead of writing exports as root.
  # Root-owned NFS fixtures carry uid/gid 0 into NAS metadata; an NFS -> Local
  # resume would then correctly attempt to preserve that ownership but fail on
  # the unprivileged lab runner with EPERM before exercising the data path.
  "$copy_binary" \
    --source "$local_root/seed" \
    --destination "$(storage_url source "$backend")" \
    --path "$key"
}

target_directory="$(cargo metadata --locked --no-deps --format-version 1 | \
  python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
cargo build --locked --example storage_copy --example storage_inspect --example storage_resume
copy_binary="$target_directory/debug/examples/storage_copy"
inspect_binary="$target_directory/debug/examples/storage_inspect"
resume_binary="$target_directory/debug/examples/storage_resume"

backends=(local nfs3 nfs41 s3 hdfs)
for source_backend in "${backends[@]}"; do
  for destination_backend in "${backends[@]}"; do
    key="resume-${source_backend}-to-${destination_backend}.bin"
    seed_source "$source_backend" "$key"

    common_args=(
      --source "$(storage_url source "$source_backend")"
      --destination "$(storage_url destination "$destination_backend")"
      --path "$key"
    )
    delete_args=()
    if [[ "$source_backend:$destination_backend" == "local:hdfs" || \
          "$source_backend:$destination_backend" == "hdfs:local" ]]; then
      delete_args=(--delete-source)
    fi
    state_file="$local_root/interrupt-$source_backend-$destination_backend.json"
    log_file="$local_root/interrupt-$source_backend-$destination_backend.log"
    "$resume_binary" "${common_args[@]}" --phase interrupt \
      --state-file "$state_file" --hold-after-interrupt >"$log_file" 2>&1 &
    interrupt_pid=$!
    state_ready=false
    for _ in {1..120}; do
      if python3 - "$state_file" <<'PY' 2>/dev/null
import json
import pathlib
import sys
state = json.loads(pathlib.Path(sys.argv[1]).read_text())
size = state["size"]
missing = state["missing"]
assert size > 0 and missing
assert all(0 <= start < end <= size for start, end in missing)
assert 0 < sum(end - start for start, end in missing) < size
assert isinstance(state["handle"], dict) and len(state["handle"]) == 1
if "Hdfs" in state["handle"]:
    assert len(missing) == 1 and 0 < missing[0][0] < missing[0][1] == size
PY
      then
        state_ready=true
        break
      fi
      kill -0 "$interrupt_pid" 2>/dev/null || break
      sleep 0.25
    done
    if [[ "$state_ready" != true ]]; then
      sed -E 's#(s3://)[^@]+@#\1<redacted>@#g; s#(hdfs://)[^@]+@#\1<redacted>@#g' \
        "$log_file" >&2
      kill "$interrupt_pid" 2>/dev/null || true
      wait "$interrupt_pid" 2>/dev/null || true
      echo "interrupt state was not durably published" >&2
      exit 1
    fi
    kill -KILL "$interrupt_pid"
    if wait "$interrupt_pid" 2>/dev/null; then
      echo "interrupt process unexpectedly exited successfully" >&2
      exit 1
    fi

    "$resume_binary" "${common_args[@]}" "${delete_args[@]}" --phase resume \
      --prior-state-file "$state_file" \
      --state-file "$local_root/resume-$source_backend-$destination_backend.json"
    expected_probe="$("$inspect_binary" --storage "$local_root/seed" --path "$key")"
    destination_probe="$("$inspect_binary" \
      --storage "$(storage_url destination "$destination_backend")" --path "$key")"
    [[ "$destination_probe" == "$expected_probe" ]]

    echo "resume $source_backend -> $destination_backend verified"
  done
done
