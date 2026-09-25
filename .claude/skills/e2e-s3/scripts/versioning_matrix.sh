#!/usr/bin/env bash
# ADR-0006 C17 versioning matrix against a real server (examples/transfer_resume.rs). Creates two
# TEMPORARY buckets on the .env's server — data-mover-c17-<run> (versioning enabled) and
# data-mover-c17-lock-<run> (created with Object Lock) — and on exit deletes both completely:
# legal holds off, every version and delete marker by id, every open upload, then the bucket. It
# never touches the .env's own bucket and never sets a retention (only removable legal holds).
#
# Every case writes under its own prefix and prints what that prefix holds afterwards:
# versions of the final key, delete markers, `.data-mover-*` entries, and whether the outcome's
# destination_version is the key's latest version. Expected in a versioned bucket: one version per
# copy, 0 markers, 0 artifact entries, version=latest. The cancel / SIGKILL resume rows come from
# .claude/skills/_shared/resume_matrix.sh (S3_BUCKET_OVERRIDE) and print the same counts.
#
# Env: .claude/skills/e2e-s3/.env (S3_HOST, S3_AK, S3_SK). SKIP_RESUME=1 skips the resume matrix.
set -u
ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
cd "$ROOT"
set -a; . .claude/skills/e2e-s3/.env; set +a
RUN=${RUN:-$(date +%s)}
# RUN names the buckets and the work directory: nothing that could leave either (no "/" or "..").
[[ $RUN =~ ^[a-z0-9-]{1,30}$ ]] || { echo "RUN must match [a-z0-9-]{1,30}: $RUN" >&2; exit 1; }
VB=data-mover-c17-$RUN
LB=data-mover-c17-lock-$RUN
SCHEME=http; [ "${S3_USE_HTTPS:-}" = true ] && SCHEME=https
H="$SCHEME://$S3_HOST"
# The credentials reach curl through a config on a file descriptor, never on its command line.
esc() { local v=${1//\\/\\\\}; printf '%s' "${v//\"/\\\"}"; }
s3c() { curl -s --aws-sigv4 "aws:amz:us-east-1:s3" -K <(printf 'user = "%s:%s"\n' "$(esc "$S3_AK")" "$(esc "$S3_SK")") "$@"; }
# Only the run's own temporary buckets are ever created, and only a bucket this run created (its
# PUT answered 200 — not 409, an existing bucket) is ever emptied or deleted.
CREATED=()
ours() { case "$1" in "$VB"|"$LB") ;; *) echo "refusing bucket $1" >&2; return 1 ;; esac; }
created() { [[ " ${CREATED[*]} " == *" $1 "* ]]; }
create() { # bucket, extra curl args...
  local code; code=$(s3c -o /dev/null -w '%{http_code}' -X PUT "$H/$1" "${@:2}")
  echo "create $1: $code"
  [ "$code" = 200 ] || { echo "bucket $1 was not created by this run; stopping" >&2; exit 1; }
  CREATED+=("$1")
}
# Percent-encodes a key (keeping "/") or a query value.
enc() { python3 -c 'import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1], safe=sys.argv[2]))' "$1" "${2:-}"; }
WORK=/tmp/data-mover-c17-$RUN
BIN=target/debug/examples/transfer_resume
build=$(cargo build -q --example transfer_resume 2>&1) || { echo "$build" >&2; exit 1; }
mkdir -p "$WORK/src"

md5b64() { python3 -c 'import hashlib,base64,sys; print(base64.b64encode(hashlib.md5(open(sys.argv[1],"rb").read()).digest()).decode())' "$1"; }
# key<TAB>versionId<TAB>marker(0/1)<TAB>latest(0/1) of every entry under prefix $2 of bucket $1.
entries() {
  local km="" vm="" out
  while :; do
    out=$(s3c "$H/$1?versions&prefix=$(enc "$2")&key-marker=$(enc "$km")&version-id-marker=$(enc "$vm")")
    python3 -c 'import sys, xml.etree.ElementTree as ET
root = ET.fromstring(sys.argv[1]); ns = root.tag[:root.tag.index("}") + 1]
for tag, marker in (("Version", False), ("DeleteMarker", True)):
    for e in root.iter(ns + tag):
        print("%s\t%s\t%d\t%d" % (e.findtext(ns + "Key"), e.findtext(ns + "VersionId"), marker, e.findtext(ns + "IsLatest") == "true"))' "$out"
    grep -q '<IsTruncated>true' <<<"$out" || break
    km=$(grep -o '<NextKeyMarker>[^<]*' <<<"$out" | sed 's/<NextKeyMarker>//')
    vm=$(grep -o '<NextVersionIdMarker>[^<]*' <<<"$out" | sed 's/<NextVersionIdMarker>//')
  done
}
# key<TAB>uploadId of the uploads open in bucket $1 under prefix $2 (MinIO: exact key only).
uploads() {
  s3c "$H/$1?uploads&prefix=$(enc "$2")" | python3 -c 'import sys, xml.etree.ElementTree as ET
try: root = ET.fromstring(sys.stdin.read())
except ET.ParseError: sys.exit()
ns = root.tag[:root.tag.index("}") + 1] if root.tag.startswith("{") else ""
for u in root.iter(ns + "Upload"): print(u.findtext(ns + "Key") + "\t" + u.findtext(ns + "UploadId"))'
}
nuke() { # bucket, deletes it completely — only one this run created
  ours "$1" || return 1
  created "$1" || { echo "cleanup: $1 not created by this run, left alone"; return 0; }
  if ! s3c -o /dev/null -f -I "$H/$1"; then
    s3c "$H/" | grep -q "<Name>$1</Name>" && echo "cleanup: $1 NOT CHECKED (HEAD failed)" || echo "cleanup: $1 absent"
    return 0
  fi
  echo '<LegalHold xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>OFF</Status></LegalHold>' >"$WORK/hold_off.xml"
  if s3c "$H/$1?object-lock" | grep -q '<ObjectLockEnabled>Enabled'; then
    entries "$1" "" | while IFS=$'\t' read -r k v m _; do
      [ "$m" = 1 ] || s3c -o /dev/null -X PUT "$H/$1/$(enc "$k" /)?legal-hold&versionId=$(enc "$v")" \
        -H "Content-MD5: $(md5b64 "$WORK/hold_off.xml")" --data-binary @"$WORK/hold_off.xml"
    done
  fi
  # MinIO lists uploads by exact key only: ask for every key the bucket ever held and every key
  # the run wrote, then bucket-wide (stores that list by prefix).
  { entries "$1" "" | cut -f1; printf '%s\n' "${KEYS[@]}"; echo; } | sort -u | while read -r k; do
    uploads "$1" "$k"
  done | sort -u | while IFS=$'\t' read -r k id; do
    s3c -o /dev/null -X DELETE "$H/$1/$(enc "$k" /)?uploadId=$(enc "$id")"
  done
  for _ in 1 2 3; do
    entries "$1" "" | while IFS=$'\t' read -r k v _ _; do
      s3c -o /dev/null -X DELETE "$H/$1/$(enc "$k" /)?versionId=$(enc "$v")"
    done
  done
  local code; code=$(s3c -o /dev/null -w '%{http_code}' -X DELETE "$H/$1")
  if s3c "$H/" | grep -q "<Name>$1</Name>"; then echo "cleanup: $1 STILL PRESENT (delete=$code)"; else echo "cleanup: $1 deleted"; fi
}
KEYS=()
cleanup() { nuke "$VB"; nuke "$LB"; rm -rf "$WORK"; }
trap cleanup EXIT

seed() { # name, bytes
  "$BIN" --source "$WORK/src" --source-path "$1" --seed-bytes "$2" --destination "$WORK/probe" \
    --destination-path "$1" --policy atomic --read-back off >/dev/null 2>&1
}
copy() { # bucket, args... — prints the JSON line
  env S3_BUCKET="$1" "$BIN" "${@:2}" 2>>"$WORK/stderr.log" | tail -1
}
field() { python3 -c 'import json,sys
try: v = json.loads(sys.argv[1]).get(sys.argv[2])
except Exception: v = "?"
print("null" if v is None else v)' "$1" "$2"; }
# What prefix $2/ of bucket $1 holds for final key $2/$3, against the outcome's version $4.
verdict() {
  local rows; rows=$(entries "$1" "$2/")
  python3 -c 'import sys
rows = [r.split("\t") for r in sys.argv[1].splitlines() if r]
key, version = sys.argv[2], sys.argv[3]
finals = [r for r in rows if r[0] == key and r[2] == "0"]
latest = next((r[1] for r in finals if r[3] == "1"), None)
markers = sum(1 for r in rows if r[2] == "1")
artifacts = sum(1 for r in rows if ".data-mover-" in r[0])
match = "n/a" if version in ("null", "?") else ("yes" if version == latest else "NO")
print("   final versions=%d delete markers=%d artifact entries=%d version=latest: %s (latest=%s)" % (len(finals), markers, artifacts, match, latest))' "$rows" "$2/$3" "$4"
}
case_() { # label, bucket, prefix, key, args...
  local label=$1 bucket=$2 prefix=$3 key=$4; shift 4
  KEYS+=("$prefix/$key")
  local line; line=$(copy "$bucket" --destination "s3:$prefix" --destination-path "$key" "$@")
  printf '%-40s result=%s prepare=%s reused=%s destination_version=%s\n' "$label" "$(field "$line" result)" \
    "$(field "$line" prepare)" "$(field "$line" reused_bytes)" "$(field "$line" destination_version)"
  LAST=$line
  verdict "$bucket" "$prefix" "$key" "$(field "$line" destination_version)"
}
versioning() { # bucket, Enabled|Suspended
  printf '<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>%s</Status></VersioningConfiguration>' "$2" >"$WORK/v.xml"
  s3c -o /dev/null -w "versioning $1 $2: %{http_code}\n" -X PUT "$H/$1?versioning" --data-binary @"$WORK/v.xml"
}

ours "$VB" && ours "$LB" || exit 1
create "$VB"
versioning "$VB" Enabled
create "$LB" -H 'x-amz-bucket-object-lock-enabled: true'
for entry in m4:4194304 m20:20971520 m200:209715200 h1:104857600 h2:104857600; do seed "${entry%%:*}" "${entry#*:}"; done
LOCAL=(--source "$WORK/src")

echo "-- versioned bucket: one version per copy, nothing else"
case_ "checkpointed 4 MiB (single PUT)" "$VB" small obj "${LOCAL[@]}" --source-path m4 --policy checkpointed
case_ "checkpointed 200 MiB (pointer)" "$VB" large obj "${LOCAL[@]}" --source-path m200 --policy checkpointed
case_ "direct 4 MiB" "$VB" direct-small obj "${LOCAL[@]}" --source-path m4 --policy direct
case_ "direct 20 MiB (multipart)" "$VB" direct-large obj "${LOCAL[@]}" --source-path m20 --policy direct

echo "-- native S3 -> S3 to the final key (C18): one version, no temp-key version or marker"
case_ "native 4 MiB" "$VB" native-small obj --source s3:small --source-path obj --policy checkpointed
case_ "native 200 MiB" "$VB" native-large obj --source s3:large --source-path obj --policy checkpointed

echo "-- v1 -> v2 by --source-version"
case_ "source v1" "$VB" hist src "${LOCAL[@]}" --source-path h1 --policy checkpointed
V1=$(field "$LAST" destination_version)
case_ "source v2" "$VB" hist src "${LOCAL[@]}" --source-path h2 --policy checkpointed
V2=$(field "$LAST" destination_version)
S3SRC=(--source "s3:hist" --source-path src --client-shaped --policy checkpointed)
case_ "copy of v1" "$VB" hist dst "${S3SRC[@]}" --source-version "$V1"
D1=$(field "$LAST" destination_version)
case_ "copy of v2" "$VB" hist dst "${S3SRC[@]}" --source-version "$V2"
D2=$(field "$LAST" destination_version)
order=$(entries "$VB" hist/dst | awk -F'\t' '$1 == "hist/dst" && $3 == 0 {print $2}' | paste -sd' ')
for pair in "$D1:h1" "$D2:h2"; do
  s3c -o "$WORK/check" "$H/$VB/hist/dst?versionId=${pair%%:*}"
  cmp -s "$WORK/check" "$WORK/src/${pair#*:}" && same=yes || same=NO
  echo "   version ${pair%%:*} holds ${pair#*:}: $same"
done
echo "   listed newest first: $order (expected: $D2 $D1)"

echo "-- v1 -> v2 by --source-version, native (C18: CopyObject / UploadPartCopy with ?versionId=)"
NSRC=(--source "s3:hist" --source-path src --policy checkpointed)
case_ "native copy of v1" "$VB" hist ndst "${NSRC[@]}" --source-version "$V1"
N1=$(field "$LAST" destination_version)
case_ "native copy of v2" "$VB" hist ndst "${NSRC[@]}" --source-version "$V2"
N2=$(field "$LAST" destination_version)
order=$(entries "$VB" hist/ndst | awk -F'\t' '$1 == "hist/ndst" && $3 == 0 {print $2}' | paste -sd' ')
for pair in "$N1:h1" "$N2:h2"; do
  s3c -o "$WORK/check" "$H/$VB/hist/ndst?versionId=${pair%%:*}"
  cmp -s "$WORK/check" "$WORK/src/${pair#*:}" && same=yes || same=NO
  echo "   version ${pair%%:*} holds ${pair#*:}: $same"
done
echo "   listed newest first: $order (expected: $N2 $N1)"

echo "-- interrupted copy of v1 (cancel after 3 s at 20 MiB/s), then resumed"
case_ "cut" "$VB" hist-cut dst "${S3SRC[@]}" --source-version "$V1" --bandwidth 20971520 --cancel-after-ms 3000
case_ "resumed" "$VB" hist-cut dst "${S3SRC[@]}" --source-version "$V1"
s3c -o "$WORK/check" "$H/$VB/hist-cut/dst"; cmp -s "$WORK/check" "$WORK/src/h1" && echo "   content = v1: yes" || echo "   content = v1: NO"

if [ "${SKIP_RESUME:-0}" != 1 ]; then
  echo "-- resume matrix (cancel / SIGKILL at 6 s, 200 MiB) into $VB"
  KEYS+=("resume/resume-$RUN/cancel" "resume/resume-$RUN/kill")
  RUN=$RUN S3_BUCKET_OVERRIDE=$VB DEST=s3:resume bash .claude/skills/_shared/resume_matrix.sh 2>&1 |
    grep -E '^\[|interrupted|resumed' | grep -Ev 'destination endpoint=|transfer identity='
fi

echo "-- Object Lock: a legal hold on the pointer while the copy runs"
KEYS+=(locked/obj)
env S3_BUCKET="$LB" RUST_LOG=warn "$BIN" "${LOCAL[@]}" --source-path m200 --destination s3:locked \
  --destination-path obj --policy checkpointed --bandwidth 20971520 >"$WORK/lock.out" 2>"$WORK/lock.err" &
pid=$!
held=no
for _ in $(seq 1 100); do
  row=$(entries "$LB" locked/ | awk -F'\t' '$1 ~ /\.upload$/ && $3 == 0' | head -1)
  if [ -n "$row" ]; then
    k=$(cut -f1 <<<"$row"); v=$(cut -f2 <<<"$row")
    echo '<LegalHold xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>ON</Status></LegalHold>' >"$WORK/hold_on.xml"
    code=$(s3c -o /dev/null -w '%{http_code}' -X PUT "$H/$LB/$k?legal-hold&versionId=$v" \
      -H "Content-MD5: $(md5b64 "$WORK/hold_on.xml")" --data-binary @"$WORK/hold_on.xml")
    held="yes ($code)"; break
  fi
  sleep 0.1
done
wait $pid
line=$(tail -1 "$WORK/lock.out")
echo "locked pointer (hold placed: $held)         result=$(field "$line" result) destination_version=$(field "$line" destination_version)"
echo "   warnings: $(grep -c 'WARN' "$WORK/lock.err") ($(grep -o 'could not delete the S3 upload pointer[^;]*' "$WORK/lock.err" | head -1))"
verdict "$LB" locked obj "$(field "$line" destination_version)"
echo "   (expected here: 1 delete marker, 2 artifact entries — the held pointer version and the marker over it)"

echo "-- suspended versioning: destination_version null"
versioning "$VB" Suspended
case_ "suspended checkpointed 4 MiB" "$VB" susp-small obj "${LOCAL[@]}" --source-path m4 --policy checkpointed
case_ "suspended checkpointed 200 MiB" "$VB" susp-large obj "${LOCAL[@]}" --source-path m200 --policy checkpointed
case_ "suspended direct 20 MiB" "$VB" susp-direct obj "${LOCAL[@]}" --source-path m20 --policy direct
