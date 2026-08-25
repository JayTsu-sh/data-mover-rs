#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

require_host_address() {
  local name="$1" expected_address="$2"
  local resolved

  resolved="$(getent ahostsv4 "$name" | awk 'NR == 1 { print $1 }')"
  [[ "$resolved" == "$expected_address" ]] || {
    echo "unhealthy HDFS name resolution: $name resolved to ${resolved:-nothing}, expected $expected_address" >&2
    return 1
  }
}

# Kerberos service principals and dfs.client.use.datanode.hostname require the
# runner to resolve these canonical names. Checking only the NameNode IP lets
# metadata-only operations pass while every real data transfer fails later.
require_host_address hdfs-namenode.hdfs.local "$LAB_HDFS_NAMENODE_HOST"
require_host_address hdfs-datanode1.hdfs.local "$LAB_HDFS_DATANODE1_DATA"
require_host_address hdfs-datanode2.hdfs.local "$LAB_HDFS_DATANODE2_DATA"

if [[ -n "${LAB_HDFS_HA_NAMENODE2_HOST:-}" ]]; then
  require_host_address hdfs-namenode2.hdfs.local "$LAB_HDFS_HA_NAMENODE2_HOST"
fi

for host in "$LAB_SOURCE_MGMT" "$LAB_DEST_MGMT" "$LAB_WORKER_MGMT"; do
  ssh_lab "$host" "test -w /var/lib/terrasync-ci"
done

for host in "$LAB_SOURCE_MGMT" "$LAB_DEST_MGMT"; do
  versions="$(ssh_lab "$host" "sudo -n /usr/local/sbin/terrasync-lab-nfs-status")"
  grep -q -- "+3" <<<"$versions"
  grep -q -- "+4.1" <<<"$versions"
done

for endpoint in \
  "http://$LAB_SOURCE_DATA:9000/" \
  "http://$LAB_DEST_DATA:9000/" \
  "http://$LAB_WORKER_DATA:9000/"; do
  status="$(curl --noproxy '*' --silent --output /dev/null --write-out '%{http_code}' \
    --connect-timeout 5 --max-time 10 "$endpoint")"
  [[ "$status" =~ ^[234][0-9][0-9]$ ]] || {
    echo "unhealthy S3 endpoint: $endpoint (HTTP $status)" >&2
    exit 1
  }
done

echo "terrasync lab is healthy"
