#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

readonly HDFS_NAMENODE_SERVICE=hadoop-namenode.service
readonly HDFS_DATANODE_SERVICE=hadoop-datanode.service
readonly HDFS_NAMENODE_VMID=301
readonly HDFS_DATANODE1_VMID=302
readonly HDFS_DATANODE2_VMID=303

hdfs_fault_target() {
  case "${1:?HDFS fault target required}" in
    namenode) printf '%s\t%s\t%s\n' "$HDFS_NAMENODE_VMID" "$LAB_HDFS_NAMENODE_HOST" "$HDFS_NAMENODE_SERVICE" ;;
    datanode1) printf '%s\t%s\t%s\n' "$HDFS_DATANODE1_VMID" "$LAB_HDFS_DATANODE1_HOST" "$HDFS_DATANODE_SERVICE" ;;
    datanode2) printf '%s\t%s\t%s\n' "$HDFS_DATANODE2_VMID" "$LAB_HDFS_DATANODE2_HOST" "$HDFS_DATANODE_SERVICE" ;;
    *) echo "unknown HDFS fault target" >&2; return 2 ;;
  esac
}

validate_hdfs_fault_target() {
  local target="$1" vmid host service expected_name config
  IFS=$'\t' read -r vmid host service < <(hdfs_fault_target "$target")
  case "$target" in
    namenode) expected_name=hdfs-namenode ;;
    datanode1) expected_name=hdfs-datanode1 ;;
    datanode2) expected_name=hdfs-datanode2 ;;
  esac
  config="$(ssh -o BatchMode=yes -o ConnectTimeout=10 root@"$LAB_HDFS_PVE_HOST" \
    "qm config '$vmid'")"
  grep -Fxq "name: $expected_name" <<<"$config"
  grep -Eq "^ipconfig0: .*ip=${host//./\\.}/" <<<"$config"
  ssh_hdfs "$host" \
    "test \"\$(hostname)\" = '$expected_name' && sudo -n systemctl cat '$service' >/dev/null"
}

hdfs_service_action() {
  local action="$1" target="$2" vmid host service
  case "$action" in start|stop) ;; *) echo "unsafe service action" >&2; return 2 ;; esac
  validate_hdfs_fault_target "$target"
  IFS=$'\t' read -r vmid host service < <(hdfs_fault_target "$target")
  ssh_hdfs "$host" "sudo -n systemctl '$action' '$service'"
}

wait_hdfs_service() {
  local target="$1" expected="$2" vmid host service
  IFS=$'\t' read -r vmid host service < <(hdfs_fault_target "$target")
  for _ in {1..60}; do
    if ssh_hdfs "$host" "systemctl is-active --quiet '$service'" 2>/dev/null; then
      [[ "$expected" == active ]] && return 0
    else
      [[ "$expected" == inactive ]] && return 0
    fi
    sleep 1
  done
  echo "HDFS $target did not become $expected" >&2
  return 1
}

wait_hdfs_cluster_ready() {
  local report
  for _ in {1..90}; do
    if report="$(curl --noproxy '*' --insecure --fail --silent --show-error \
      --max-time 5 \
      "https://$LAB_HDFS_NAMENODE_HOST:$LAB_HDFS_NAMENODE_HTTPS_PORT/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState" 2>/dev/null \
      | python3 -c 'import json,sys; bean=json.load(sys.stdin)["beans"][0]; print(bean["FSState"], bean["NumLiveDataNodes"])' 2>/dev/null)" \
      && [[ "$report" == 'Operational 2' ]]; then
      return 0
    fi
    sleep 1
  done
  echo "HDFS cluster did not become operational with two live DataNodes" >&2
  return 1
}

restore_hdfs_services() {
  local status=0
  for target in namenode datanode1 datanode2; do
    hdfs_service_action start "$target" || status=1
  done
  for target in namenode datanode1 datanode2; do
    wait_hdfs_service "$target" active || status=1
  done
  wait_hdfs_cluster_ready || status=1
  return "$status"
}
