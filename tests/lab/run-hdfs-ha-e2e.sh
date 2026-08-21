#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

run_id="${1:?run id required}"
validate_run_id "$run_id"
prepare_hdfs_kerberos "$run_id"

set +e
validation="$(python3 tests/hdfs_ha_gate.py --run-id "$run_id" 2>&1)"
validation_status=$?
set -e
if (( validation_status == 3 )); then
  echo "$validation"
  exit 0
fi
if (( validation_status != 0 )); then
  echo "$validation" >&2
  exit "$validation_status"
fi

IFS=$'\t' read -r nameservice ha_root <<<"$validation"
[[ -f "$LAB_HDFS_HA_CONFIG_DIR/core-site.xml" ]]
[[ -f "$LAB_HDFS_HA_CONFIG_DIR/hdfs-site.xml" ]]

target_directory="$(cargo metadata --locked --no-deps --format-version 1 | \
  python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
cargo build --locked --example hdfs_ha_probe
probe="$target_directory/debug/examples/hdfs_ha_probe"
local_root="/tmp/data-mover-lab/$run_id/hdfs-ha"
mkdir -p "$local_root"
seeded=0
original_active=0

node_value() {
  local number="$1" field="$2" variable
  variable="LAB_HDFS_HA_NAMENODE${number}_${field}"
  printf '%s\n' "${!variable}"
}

validate_ha_node() {
  local number="$1" vmid host name service config
  vmid="$(node_value "$number" VMID)"
  host="$(node_value "$number" HOST)"
  name="$(node_value "$number" NAME)"
  service="$(node_value "$number" SERVICE)"
  config="$(ssh -o BatchMode=yes -o ConnectTimeout=10 root@"$LAB_HDFS_PVE_HOST" \
    "qm config '$vmid'")"
  grep -Fxq "name: $name" <<<"$config"
  grep -Eq "^ipconfig0: .*ip=${host//./\\.}/" <<<"$config"
  ssh_hdfs "$host" \
    "test \"\$(hostname)\" = '$name' && \
     sudo -n systemctl cat '$service' hadoop-zkfc.service >/dev/null"
}

ha_state() {
  local number="$1" host node_id remote_cache
  host="$(node_value "$number" HOST)"
  node_id="$(node_value "$number" ID)"
  remote_cache="/tmp/data-mover-ha-state-${run_id}-${node_id}.ccache"
  ssh_hdfs "$host" \
    "sudo -n -u hdfs env JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
      HADOOP_CONF_DIR=/etc/hadoop HADOOP_LOG_DIR=/var/log/hadoop \
      KRB5CCNAME='FILE:$remote_cache' bash -c \
      'trap \"kdestroy >/dev/null 2>&1 || true\" EXIT; \
       principal=\"\$(/opt/hadoop/bin/hdfs getconf -confKey dfs.namenode.kerberos.principal)\"; \
       kinit -kt /etc/security/keytabs/hdfs.service.keytab \"\$principal\"; \
       /opt/hadoop/bin/hdfs haadmin -ns '$nameservice' -getServiceState '$node_id''" \
    2>/dev/null
}

service_action() {
  local action="$1" number="$2" host service
  [[ "$action" == start || "$action" == stop ]]
  validate_ha_node "$number"
  host="$(node_value "$number" HOST)"
  service="$(node_value "$number" SERVICE)"
  ssh_hdfs "$host" "sudo -n systemctl '$action' '$service'"
  if [[ "$action" == start ]]; then
    ssh_hdfs "$host" \
      "sudo -n systemctl reset-failed hadoop-zkfc.service; \
       sudo -n systemctl start hadoop-zkfc.service"
  fi
}

wait_unit_active() {
  local number="$1" host service
  host="$(node_value "$number" HOST)"
  service="$(node_value "$number" SERVICE)"
  for _ in {1..90}; do
    ssh_hdfs "$host" "systemctl is-active --quiet '$service'" 2>/dev/null && return 0
    sleep 1
  done
  return 1
}

wait_exact_ha_pair() {
  local first second
  for _ in {1..120}; do
    first="$(ha_state 1 || true)"
    second="$(ha_state 2 || true)"
    if [[ "$first:$second" == active:standby || "$first:$second" == standby:active ]]; then
      printf '%s\t%s\n' "$first" "$second"
      return 0
    fi
    sleep 1
  done
  echo "HA pair did not converge to one active and one standby" >&2
  return 1
}

graceful_failover_to() {
  local number="$1" host node_id source_number source_id remote_cache
  host="$(node_value "$number" HOST)"
  node_id="$(node_value "$number" ID)"
  if [[ "$number" == 1 ]]; then source_number=2; else source_number=1; fi
  source_id="$(node_value "$source_number" ID)"
  remote_cache="/tmp/data-mover-ha-restore-${run_id}-${node_id}.ccache"
  ssh_hdfs "$host" \
    "sudo -n -u hdfs env JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
      HADOOP_CONF_DIR=/etc/hadoop HADOOP_LOG_DIR=/var/log/hadoop \
      KRB5CCNAME='FILE:$remote_cache' bash -c \
      'trap \"kdestroy >/dev/null 2>&1 || true\" EXIT; \
       principal=\"\$(/opt/hadoop/bin/hdfs getconf -confKey dfs.namenode.kerberos.principal)\"; \
       kinit -kt /etc/security/keytabs/hdfs.service.keytab \"\$principal\"; \
       /opt/hadoop/bin/hdfs haadmin -failover '$source_id' '$node_id''" >/dev/null
}

cleanup_ha_run() {
  local original_status="$1" cleanup_status=0
  for number in 1 2; do
    service_action start "$number" || cleanup_status=1
    wait_unit_active "$number" || cleanup_status=1
  done
  wait_exact_ha_pair >/dev/null || cleanup_status=1
  if (( seeded == 1 )); then
    "$probe" --storage "$LAB_HDFS_HA_LOCATION" --config-dir "$LAB_HDFS_HA_CONFIG_DIR" \
      --action cleanup || cleanup_status=1
  fi
  if (( original_active != 0 )) && \
    [[ "$(ha_state "$original_active" || true)" != active ]]; then
    graceful_failover_to "$original_active" || cleanup_status=1
    wait_exact_ha_pair >/dev/null || cleanup_status=1
  fi
  if (( original_status != 0 )); then exit "$original_status"; fi
  exit "$cleanup_status"
}
trap 'cleanup_ha_run $?' EXIT

validate_ha_node 1
validate_ha_node 2
states="$(wait_exact_ha_pair)"
IFS=$'\t' read -r state1 state2 <<<"$states"
if [[ "$state1" == active ]]; then active=1; else active=2; fi
original_active="$active"

"$probe" --storage "$LAB_HDFS_HA_LOCATION" --config-dir "$LAB_HDFS_HA_CONFIG_DIR" \
  --action seed
seeded=1
expected="$("$probe" --storage "$LAB_HDFS_HA_LOCATION" \
  --config-dir "$LAB_HDFS_HA_CONFIG_DIR" --action inspect)"

ready="$local_root/metadata-loop.ready"
"$probe" --storage "$LAB_HDFS_HA_LOCATION" --config-dir "$LAB_HDFS_HA_CONFIG_DIR" \
  --action metadata-loop --ready-file "$ready" >"$local_root/metadata-loop.log" 2>&1 &
probe_pid=$!
for _ in {1..400}; do
  [[ -f "$ready" ]] && break
  kill -0 "$probe_pid" 2>/dev/null || break
  sleep 0.05
done
[[ -f "$ready" ]]

service_action stop "$active"
if (( active == 1 )); then remaining_active=2; else remaining_active=1; fi
for _ in {1..90}; do
  [[ "$(ha_state "$remaining_active" || true)" == active ]] && break
  sleep 1
done
[[ "$(ha_state "$remaining_active" || true)" == active ]]
actual="$(timeout 120s "$probe" --storage "$LAB_HDFS_HA_LOCATION" \
  --config-dir "$LAB_HDFS_HA_CONFIG_DIR" --action inspect)"
[[ "$actual" == "$expected" ]]

for _ in {1..1200}; do
  kill -0 "$probe_pid" 2>/dev/null || break
  sleep 0.1
done
if kill -0 "$probe_pid" 2>/dev/null; then
  kill "$probe_pid" 2>/dev/null || true
  wait "$probe_pid" 2>/dev/null || true
  echo "HA metadata loop did not finish after failover" >&2
  exit 1
fi
wait "$probe_pid"
echo "HDFS HA acceptance passed through logical NameService $nameservice"
