#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

run_id="${1:?run id required}"
validate_run_id "$run_id"
source_root="$LAB_NFS3_EXPORT/ci/$run_id"
destination_root="$LAB_NFS3_EXPORT/ci/$run_id"

ssh_lab "$LAB_SOURCE_MGMT" \
  "mkdir -p '$source_root/dir' '$source_root/stale' && printf '%s' 'architecture-ready-nfs3-fixture' > '$source_root/fixture.bin' && printf '%s' 'nested' > '$source_root/dir/nested.bin' && printf '%s' 'stale-handle-fixture' > '$source_root/stale/fixture.bin' && ln -sfn fixture.bin '$source_root/fixture.link'"
ssh_lab "$LAB_DEST_MGMT" "mkdir -p '$destination_root'"

source_url="nfs://$LAB_SOURCE_DATA$LAB_NFS3_EXPORT:/ci/$run_id?version=3&noresvport=true"
destination_url="nfs://$LAB_DEST_DATA$LAB_NFS3_EXPORT:/ci/$run_id?version=3&noresvport=true"
barrier_dir="$(mktemp -d)"
ready_file="$barrier_dir/ready"
go_file="$barrier_dir/go"
trap 'rm -rf -- "$barrier_dir"' EXIT
cargo run --quiet --locked --example nfs3_contract -- \
  --source "$source_url" \
  --destination "$destination_url" \
  --stale-ready-file "$ready_file" \
  --stale-go-file "$go_file" &
contract_pid=$!
for _ in {1..300}; do
  [[ -f "$ready_file" ]] && break
  sleep 0.1
done
[[ -f "$ready_file" ]]
old_stale_inode="$(ssh_lab_root "$LAB_SOURCE_MGMT" "stat -c '%i' '$source_root/stale/fixture.bin'")"
ssh_lab_root "$LAB_SOURCE_MGMT" \
  "mv '$source_root/stale' '$source_root/stale-old' && mkdir '$source_root/stale' && printf '%s' 'stale-handle-fixture' > '$source_root/stale/fixture.bin' && rm -rf -- '$source_root/stale-old'"
new_stale_inode="$(ssh_lab_root "$LAB_SOURCE_MGMT" "stat -c '%i' '$source_root/stale/fixture.bin'")"
[[ "$old_stale_inode" != "$new_stale_inode" ]]
touch "$go_file"
wait "$contract_pid"

expected_fixture="$(printf '%s' 'architecture-ready-nfs3-fixture' | sha256sum | cut -d' ' -f1)"
actual_fixture="$(ssh_lab_root "$LAB_DEST_MGMT" "sha256sum '$destination_root/copied.bin' | cut -d' ' -f1")"
[[ "$actual_fixture" == "$expected_fixture" ]]
expected_recovery="$(printf '%s' 'nfs3 durable recovery contract' | sha256sum | cut -d' ' -f1)"
actual_recovery="$(ssh_lab_root "$LAB_DEST_MGMT" "sha256sum '$destination_root/recovered.bin' | cut -d' ' -f1")"
[[ "$actual_recovery" == "$expected_recovery" ]]

echo "DM-NFS3-CONTRACT passed for $run_id"
