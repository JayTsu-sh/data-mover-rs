#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

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
