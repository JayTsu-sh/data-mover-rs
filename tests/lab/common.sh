#!/usr/bin/env bash
set -euo pipefail

LAB_SSH_USER="${LAB_SSH_USER:-ci-runner}"
LAB_ROOT_SSH_USER="${LAB_ROOT_SSH_USER:-root}"
LAB_SSH_KEY="${LAB_SSH_KEY:-/home/github-runner/.ssh/terrasync_lab}"
LAB_SOURCE_MGMT="${LAB_SOURCE_MGMT:-10.131.9.12}"
LAB_DEST_MGMT="${LAB_DEST_MGMT:-10.131.9.13}"
LAB_WORKER_MGMT="${LAB_WORKER_MGMT:-10.131.9.14}"
LAB_SOURCE_DATA="${LAB_SOURCE_DATA:-10.10.1.12}"
LAB_DEST_DATA="${LAB_DEST_DATA:-10.10.1.13}"
LAB_WORKER_DATA="${LAB_WORKER_DATA:-10.10.1.14}"
LAB_NFS3_EXPORT="${LAB_NFS3_EXPORT:-/srv/nfs/v3}"
LAB_NFS41_EXPORT="${LAB_NFS41_EXPORT:-/srv/nfs/v4}"
LAB_S3_BUCKET="${LAB_S3_BUCKET:-terrasync-ci}"
LAB_HDFS_LOCATION="${LAB_HDFS_LOCATION:-hdfs://root@10.131.9.30:9000/}"
LAB_HDFS_ADMIN_USER="${LAB_HDFS_ADMIN_USER:-hdfs/hdfs-namenode@HDFS.LOCAL}"
LAB_HDFS_CONFIG_DIR="${LAB_HDFS_CONFIG_DIR:-}"
LAB_HDFS_KEYTAB="${LAB_HDFS_KEYTAB:-}"
LAB_HDFS_PVE_HOST="${LAB_HDFS_PVE_HOST:-10.131.9.20}"
LAB_HDFS_SSH_USER="${LAB_HDFS_SSH_USER:-ubuntu}"
LAB_HDFS_NAMENODE_HOST="${LAB_HDFS_NAMENODE_HOST:-10.131.9.30}"
LAB_HDFS_NAMENODE_HTTPS_PORT="${LAB_HDFS_NAMENODE_HTTPS_PORT:-9871}"
LAB_HDFS_DATANODE1_HOST="${LAB_HDFS_DATANODE1_HOST:-10.131.9.31}"
LAB_HDFS_DATANODE2_HOST="${LAB_HDFS_DATANODE2_HOST:-10.131.9.32}"
LAB_HDFS_DATANODE1_DATA="${LAB_HDFS_DATANODE1_DATA:-10.10.1.31}"
LAB_HDFS_DATANODE2_DATA="${LAB_HDFS_DATANODE2_DATA:-10.10.1.32}"

load_s3_credentials_from_rustfs() {
  if [[ -n "${LAB_S3_ACCESS_KEY:-}" || -n "${LAB_S3_SECRET_KEY:-}" ]]; then
    return
  fi

  local -a source_credentials destination_credentials
  mapfile -t source_credentials < <(
    ssh_lab_root "$LAB_SOURCE_MGMT" \
      ". /etc/default/rustfs; printf '%s\\n%s\\n' \"\$RUSTFS_ACCESS_KEY\" \"\$RUSTFS_SECRET_KEY\""
  )
  mapfile -t destination_credentials < <(
    ssh_lab_root "$LAB_DEST_MGMT" \
      ". /etc/default/rustfs; printf '%s\\n%s\\n' \"\$RUSTFS_ACCESS_KEY\" \"\$RUSTFS_SECRET_KEY\""
  )
  [[ "${#source_credentials[@]}" == 2 && "${#destination_credentials[@]}" == 2 ]] || {
    echo "failed to load complete RustFS credentials" >&2
    return 2
  }
  [[ "${source_credentials[0]}" == "${destination_credentials[0]}" &&
    "${source_credentials[1]}" == "${destination_credentials[1]}" ]] || {
    echo "source and destination RustFS credentials differ" >&2
    return 2
  }
  export LAB_S3_ACCESS_KEY="${source_credentials[0]}"
  export LAB_S3_SECRET_KEY="${source_credentials[1]}"
}

require_s3_credentials() {
  load_s3_credentials_from_rustfs
  : "${LAB_S3_ACCESS_KEY:?LAB_S3_ACCESS_KEY is required}"
  : "${LAB_S3_SECRET_KEY:?LAB_S3_SECRET_KEY is required}"
}

require_hdfs_kerberos_credentials() {
  : "${LAB_HDFS_CONFIG_DIR:?LAB_HDFS_CONFIG_DIR is required}"
  : "${LAB_HDFS_KEYTAB:?LAB_HDFS_KEYTAB is required}"
  [[ -d "$LAB_HDFS_CONFIG_DIR" ]] || {
    echo "LAB_HDFS_CONFIG_DIR is not a readable directory" >&2
    return 2
  }
  [[ -r "$LAB_HDFS_KEYTAB" ]] || {
    echo "LAB_HDFS_KEYTAB is not readable" >&2
    return 2
  }
}

isolate_hdfs_process_cache() {
  local run_id="$1"
  local runtime_dir="${RUNNER_TEMP:-/tmp}"
  export KRB5CCNAME="FILE:$runtime_dir/data-mover-invalid-$run_id.ccache"
}

prepare_hdfs_kerberos() {
  local run_id="$1"
  require_hdfs_kerberos_credentials
  isolate_hdfs_process_cache "$run_id"
  export LAB_HDFS_LOCATION LAB_HDFS_ADMIN_USER LAB_HDFS_CONFIG_DIR LAB_HDFS_KEYTAB KRB5CCNAME
}

ssh_lab() {
  local host="$1"
  shift
  ssh -i "$LAB_SSH_KEY" \
    -o BatchMode=yes \
    -o ConnectTimeout=10 \
    -o StrictHostKeyChecking=accept-new \
    "$LAB_SSH_USER@$host" "$@"
}

ssh_lab_root() {
  local host="$1"
  shift
  ssh -i "$LAB_SSH_KEY" \
    -o BatchMode=yes \
    -o ConnectTimeout=10 \
    -o StrictHostKeyChecking=accept-new \
    "$LAB_ROOT_SSH_USER@$host" "$@"
}

ssh_hdfs() {
  local host="$1"
  shift
  ssh -i "$LAB_SSH_KEY" \
    -o BatchMode=yes \
    -o ConnectTimeout=10 \
    -o StrictHostKeyChecking=accept-new \
    "$LAB_HDFS_SSH_USER@$host" "$@"
}

validate_run_id() {
  local run_id="$1"
  [[ "$run_id" =~ ^(nightly|release)-[A-Za-z0-9._-]{1,80}$ ]] || {
    echo "unsafe run id: $run_id" >&2
    return 2
  }
  [[ "$run_id" != *..* ]] || {
    echo "unsafe run id: $run_id" >&2
    return 2
  }
}

hdfs_run_root() {
  local run_id="$1"
  validate_run_id "$run_id"
  python3 - "$LAB_HDFS_LOCATION" "$LAB_HDFS_ADMIN_USER" "$run_id" <<'PY'
import re
import sys
import urllib.parse

location, admin_user, run_id = sys.argv[1:]
parsed = urllib.parse.urlsplit(location)
if (
    parsed.scheme != "hdfs"
    or parsed.password is not None
    or parsed.hostname is None
    or parsed.port is None
    or parsed.query
    or parsed.fragment
):
    raise SystemExit("LAB_HDFS_LOCATION must be an explicit password-free direct HDFS URL")
kind = run_id.split("-", 1)[0]
if kind not in {"nightly", "release"}:
    raise SystemExit("invalid HDFS lab run kind")
user = urllib.parse.quote(admin_user, safe="")
host = f"[{parsed.hostname}]" if ":" in parsed.hostname else parsed.hostname
root = f"/tmp/data-mover-{kind}/{run_id}/hdfs"
print(f"hdfs://{user}@{host}:{parsed.port}{root}")
PY
}
