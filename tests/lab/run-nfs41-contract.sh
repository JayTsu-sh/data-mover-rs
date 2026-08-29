#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

run_id="${1:?run id required}"
validate_run_id "$run_id"
timeout 5 bash -c "</dev/tcp/$LAB_SOURCE_DATA/2049"
timeout 5 bash -c "</dev/tcp/$LAB_DEST_DATA/2049"

run_root="ci/$run_id"
source_root="$run_root/source"
destination_root="$run_root/destination"
mount_url="nfs://$LAB_SOURCE_DATA$LAB_NFS41_EXPORT?version=4.1&noresvport=true&uid=0&gid=0"
source_url="nfs://$LAB_SOURCE_DATA$LAB_NFS41_EXPORT:/$source_root?version=4.1&noresvport=true&uid=0&gid=0"
destination_url="nfs://$LAB_DEST_DATA$LAB_NFS41_EXPORT:/$destination_root?version=4.1&noresvport=true&uid=0&gid=0"

barrier_dir="$(mktemp -d)"
ready_file="$barrier_dir/ready"
go_file="$barrier_dir/go"
contract_pid=""
cleanup() {
  [[ -z "$contract_pid" ]] || kill "$contract_pid" 2>/dev/null || true
  rm -rf -- "$barrier_dir"
}
trap cleanup EXIT

cargo run --quiet --locked --example nfs3_contract -- \
  --dialect nfs41 \
  --seed-mount "$mount_url" \
  --seed-root "$source_root" \
  --source "$source_url" \
  --destination "$destination_url" \
  --stale-ready-file "$ready_file" \
  --stale-go-file "$go_file" &
contract_pid=$!
for _ in {1..300}; do
  [[ -f "$ready_file" ]] && break
  kill -0 "$contract_pid" 2>/dev/null || wait "$contract_pid"
  sleep 0.1
done
[[ -f "$ready_file" ]]

native_root="$LAB_NFS41_EXPORT/$source_root"
old_stale_inode="$(ssh_lab_root "$LAB_SOURCE_MGMT" "stat -c '%i' '$native_root/stale/fixture.bin'")"
ssh_lab_root "$LAB_SOURCE_MGMT" \
  "mv '$native_root/stale' '$native_root/stale-old' && mkdir '$native_root/stale' && printf '%s' 'stale-handle-fixture' > '$native_root/stale/fixture.bin' && rm -rf -- '$native_root/stale-old'"
new_stale_inode="$(ssh_lab_root "$LAB_SOURCE_MGMT" "stat -c '%i' '$native_root/stale/fixture.bin'")"
[[ "$old_stale_inode" != "$new_stale_inode" ]]
touch "$go_file"
wait "$contract_pid"
contract_pid=""

echo "DM-NFS41-CONTRACT passed for $run_id"
