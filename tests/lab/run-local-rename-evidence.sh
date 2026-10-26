#!/usr/bin/env bash
# Run on an authorized Linux mount host as root. Credentials stay in a pre-existing
# mode-0600 file; no password is accepted on the command line or written to logs.
# CIFS_CREDENTIALS=/private/cifs.credentials timeout 600 bash "$0" /path/to/local_rename_evidence
set -euo pipefail
binary=$(realpath "${1:?usage: run-local-rename-evidence.sh /path/to/local_rename_evidence}")
: "${CIFS_CREDENTIALS:?set CIFS_CREDENTIALS to a private mount.cifs credentials file}"
[[ -x "$binary" && -f "$CIFS_CREDENTIALS" ]]
[[ $(id -u) == 0 ]]
server=${MOUNT_TEST_SERVER:-10.128.61.200}
share=${MOUNT_TEST_SHARE:-ontap_lisaauto_cifs}
export_path=${MOUNT_TEST_EXPORT:-/ontap_lisaauto_nfs}
run=data-mover-rename-$(date -u +%Y%m%dT%H%M%SZ)-$$
base=/mnt/$run
state=$(mktemp -d "/tmp/$run.XXXXXX")
mkdir "$base"
active= intermediate=
fixture=$run
cleanup() {
    local rc=$?
    trap - EXIT INT TERM
    if [[ -n "$active" ]] && mountpoint -q "$active"; then
        if [[ -d "$active/$fixture" && "$fixture" == data-mover-rename-* ]]; then
            timeout 30 rm -rf -- "$active/$fixture" || true
        fi
        timeout 30 umount "$active" || rc=1
    fi
    if [[ -n "$intermediate" ]] && mountpoint -q "$intermediate"; then
        timeout 30 umount "$intermediate" || rc=1
    fi
    if ! mountpoint -q "$base/mount" && ! mountpoint -q "$base/intermediate"; then
        timeout 10 rm -rf -- "$base" "$state"
    else
        echo "CLEANUP INCOMPLETE owned_mount_root=$base state=$state" >&2
        rc=1
    fi
    exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT TERM
mkdir "$base/mount" "$base/intermediate"
mount_profile() {
    case "$1" in
        cifs-serverino|cifs-noserverino)
            local ino=serverino
            [[ "$1" == cifs-noserverino ]] && ino=noserverino
            timeout 30 mount -t cifs "//$server/$share" "$base/mount" \
                -o "credentials=$CIFS_CREDENTIALS,vers=3.1.1,$ino,cache=none,actimeo=0,nosharesock"
            ;;
        nfs3) timeout 30 mount -t nfs "$server:$export_path" "$base/mount" \
            -o vers=3,proto=tcp,resvport,actimeo=0,nosharecache ;;
        nfs41) timeout 30 mount -t nfs "$server:$export_path" "$base/mount" \
            -o vers=4.1,proto=tcp,resvport,actimeo=0,nosharecache ;;
    esac
    active=$base/mount
}
for profile in cifs-serverino cifs-noserverino nfs3 nfs41; do
    echo "PROFILE $profile host=$(hostname) kernel=$(uname -r)"
    mount_profile "$profile"
    findmnt -rn -T "$active" -o TARGET,FSTYPE,SOURCE,OPTIONS
    mkdir "$active/$fixture"
    args=(--root "$active/$fixture" --checkpoint "$state/$profile.json" --scope "$server:$([[ $profile == cifs-* ]] && echo "$share" || echo "$export_path")")
    case "$profile" in
        cifs-serverino) args+=(--expect-evidence --allow-heuristic) ;;
        cifs-noserverino) args+=(--expect-evidence) ;;
    esac
    timeout 90 "$binary" "${args[@]}" --phase before
    # The previous process exited: no open descriptors or backend objects survive.
    timeout 30 umount "$active"
    active=
    # Occupy the freed anonymous device slot to make dev churn observable, where supported.
    timeout 30 mount -t tmpfs -o size=1m tmpfs "$base/intermediate"
    intermediate=$base/intermediate
    mount_profile "$profile"
    findmnt -rn -T "$active" -o TARGET,FSTYPE,SOURCE,OPTIONS
    timeout 90 "$binary" "${args[@]}" --phase after
    timeout 30 rm -rf -- "$active/$fixture"
    [[ ! -e "$active/$fixture" ]]
    timeout 30 umount "$active"
    active=
    timeout 30 umount "$intermediate"
    intermediate=
    echo "PROFILE $profile PASS cleaned=true full_unmount=true separate_processes=true"
done
echo 'RESULT mounted CIFS serverino/noserverino + NFSv3/v4.1 PASS'
