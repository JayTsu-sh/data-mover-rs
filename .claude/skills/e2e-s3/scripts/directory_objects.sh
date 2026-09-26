#!/usr/bin/env bash
# P7 F5: S3 "directory objects" — keys ending in '/' (with or without content) — against a real
# server: the role-based traversal lists each as a file entry of its own directory, the legacy
# walks report the ones with content, S3→S3 copies recreate them (single PUT, multipart with the
# .upload pointer in the PARENT, cut and resumed, native), and S3→Local refuses them at Preflight.
#
# Bucket selection (as traversal_versions.sh; every bucket must be named data-mover-*):
# - default: creates ONE temporary versioned bucket <BUCKET_PREFIX>-<run> on the .env's server and
#   deletes it completely on exit (every version, marker and upload, then the bucket). CreateBucket
#   always sends a body or Content-Length: 0 (StorageGRID answers 411 without it).
# - S3_VERSIONED_BUCKET=<name>: an existing bucket of ours with versioning Enabled.
# - S3_UNVERSIONED_BUCKET=<name>: an existing bucket of ours that never had versioning.
# Existing buckets: everything under dm-<run>/, emptied on exit, bucket and settings untouched.
#
# Seeded under the run's prefix: dirobj/ (5 B) + dirobj/child, lonely/ (6 B, no children),
# emptydir/ (0 B) + emptydir/child, a console-style empty folder e/ (0 B), plain.txt, and big/
# (BIG_MB MiB, default 200, uploaded from a local file: a multipart write of a directory object).
# Versioned buckets also get z/ = 0 B, delete marker, 0 B (all-zero history) and m/ = 5 B, 0 B,
# delete marker, 3 B, delete marker (mixed history).
#
# Checks:
# - Current traversal (--sizes): every seeded key once, own objects as `File <d>/ size=<n>`, the
#   zero-byte ones included; balanced totals, no failure; --order blocks identical at c=1/4/32.
# - Traversal roots: --path dirobj reports `failure scope=Child dirobj/ Unsupported` and does not
#   emit dirobj/; --path emptydir reports and emits nothing of emptydir/.
# - All (versioned): dirobj/ one 5 B version; z/ 0 B, marker, 0 B; m/ 5 B, 0 B, marker, 3 B,
#   marker (latest last).
# - Legacy walkdir / walkdir_2: an ERROR naming each own object that holds content (dirobj/,
#   lonely/, big/, and m/ in a versioned bucket), none for the zero-byte ones.
# - Copies (transfer_resume, same bucket): dirobj/ and emptydir/ streamed (single PUT), dirobj/
#   native, big/ streamed cut by --cancel-after-ms then resumed (pointer in copy/, none inside
#   copy/big/, one open upload on copy/big/), big/ native; each compared byte for byte; S3→Local of
#   dirobj/ fails at Preflight on the destination and writes no file (transfer_resume itself creates
#   the parent directory, as a caller does).
#
# D6=1 (the ONTAP NAS bucket, S3_UNVERSIONED_BUCKET=data-mover-nas): instead of the checks above,
# writes zero-byte and content `d/` keys before and after their children and prints what the
# server keeps (HEAD of each key, the parent's delimiter listing, the role traversal). Nothing about
# the store is asserted (it decides D6; the run still fails if the traversal itself fails). Clean-up
# deletes the keys, then the directories post-order.
#
# Env: S3_ENV (default .claude/skills/e2e-s3/.env), BUCKET_PREFIX (default data-mover-f5),
# S3_LOCATION (CreateBucket LocationConstraint, DXN), BIG_MB. Exit 0 only when every check passed
# and the clean-up left nothing.
set -u
ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
cd "$ROOT"
S3_ENV=${S3_ENV:-.claude/skills/e2e-s3/.env}
set -a; . "$S3_ENV"; set +a
RUN=${RUN:-$(date +%s)}
[[ $RUN =~ ^[a-z0-9-]{1,30}$ ]] || { echo "RUN must match [a-z0-9-]{1,30}: $RUN" >&2; exit 1; }
BIG_MB=${BIG_MB:-200}
# The cut lands after 6 s at 20 MiB/s: big/ must be larger than that and than one 8 MiB PUT.
[[ $BIG_MB =~ ^[0-9]+$ ]] && [ "$BIG_MB" -ge 150 ] || { echo "BIG_MB must be at least 150" >&2; exit 1; }
D6=${D6:-0}
[ -n "${S3_VERSIONED_BUCKET:-}" ] && [ -n "${S3_UNVERSIONED_BUCKET:-}" ] &&
  { echo "set S3_VERSIONED_BUCKET or S3_UNVERSIONED_BUCKET, not both" >&2; exit 1; }
if [ -n "${S3_VERSIONED_BUCKET:-}" ]; then MODE=versioned; VB=$S3_VERSIONED_BUCKET
elif [ -n "${S3_UNVERSIONED_BUCKET:-}" ]; then MODE=unversioned; VB=$S3_UNVERSIONED_BUCKET
else
  MODE=temporary; BUCKET_PREFIX=${BUCKET_PREFIX:-data-mover-f5}
  [[ $BUCKET_PREFIX =~ ^data-mover-[a-z0-9-]{1,20}$ ]] || { echo "BUCKET_PREFIX must be data-mover-<[a-z0-9-]>: $BUCKET_PREFIX" >&2; exit 1; }
  VB=$BUCKET_PREFIX-$RUN
fi
[[ $VB =~ ^data-mover-[a-z0-9.-]{1,40}$ ]] || { echo "refusing bucket $VB: not one of ours (data-mover-*)" >&2; exit 1; }
[ "$D6" = 1 ] && [ "$MODE" != unversioned ] && { echo "D6=1 needs S3_UNVERSIONED_BUCKET (the NAS bucket)" >&2; exit 1; }
P=dm-$RUN/
VERSIONED=1; [ "$MODE" = unversioned ] && VERSIONED=0
SCHEME=http; [ "${S3_USE_HTTPS:-}" = true ] && SCHEME=https
H="$SCHEME://$S3_HOST"
esc() { local v=${1//\\/\\\\}; printf '%s' "${v//\"/\\\"}"; }
s3c() { curl -s --aws-sigv4 "aws:amz:us-east-1:s3" -K <(printf 'user = "%s:%s"\n' "$(esc "$S3_AK")" "$(esc "$S3_SK")") "$@"; }
enc() { python3 -c 'import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1], safe=sys.argv[2]))' "$1" "${2:-}"; }
WORK=/tmp/$VB-f5-work-$RUN
LOCAL=/tmp/$VB-f5-local-$RUN
mkdir -p "$WORK" "$LOCAL"
CREATED=0; WROTE=0; FAILED=0
FAILED_LISTING='!listing-failed'
check() { # name, got, want
  if [ "$2" = "$3" ]; then printf '%-44s ok\n' "$1"; else printf '%-44s FAIL got=[%s] want=[%s]\n' "$1" "$2" "$3"; FAILED=1; fi
}

# key<TAB>versionId<TAB>kind<TAB>size of every entry under the run's prefix, in server order.
entries() {
  local km="" vm="" out
  while :; do
    # StorageGRID refuses an empty version-id-marker (400): send the markers only once set.
    out=$(s3c "$H/$VB?versions&prefix=$(enc "${1:-$P}")${km:+&key-marker=$(enc "$km")}${vm:+&version-id-marker=$(enc "$vm")}")
    python3 -c 'import sys, xml.etree.ElementTree as ET
try: root = ET.fromstring(sys.stdin.read())
except ET.ParseError: root = None
ns = root.tag[:root.tag.index("}") + 1] if root is not None and root.tag.startswith("{") else ""
if root is None or root.tag != ns + "ListVersionsResult": print(sys.argv[1]); sys.exit(1)
for e in root:
    if e.tag in (ns + "Version", ns + "DeleteMarker"):
        kind = "M" if e.tag == ns + "DeleteMarker" else "V"
        print("%s\t%s\t%s\t%s" % (e.findtext(ns + "Key"), e.findtext(ns + "VersionId"), kind, e.findtext(ns + "Size") or "-"))' "$FAILED_LISTING" <<<"$out" || break
    grep -q '<IsTruncated>true' <<<"$out" || break
    km=$(grep -o '<NextKeyMarker>[^<]*' <<<"$out" | sed 's/<NextKeyMarker>//')
    vm=$(grep -o '<NextVersionIdMarker>[^<]*' <<<"$out" | sed 's/<NextVersionIdMarker>//')
  done
}
# Every key under $1 (default: the run's prefix), ListObjectsV2 without delimiter.
keys() {
  local token="" out
  while :; do
    out=$(s3c "$H/$VB?list-type=2&prefix=$(enc "${1:-$P}")${token:+&continuation-token=$(enc "$token")}")
    python3 -c 'import sys, xml.etree.ElementTree as ET
try: root = ET.fromstring(sys.stdin.read())
except ET.ParseError: root = None
ns = root.tag[:root.tag.index("}") + 1] if root is not None and root.tag.startswith("{") else ""
if root is None or root.tag != ns + "ListBucketResult": print(sys.argv[1]); sys.exit(1)
for e in root.iter(ns + "Contents"): print(e.findtext(ns + "Key"))' "$FAILED_LISTING" <<<"$out" || break
    token=$(grep -o '<NextContinuationToken>[^<]*' <<<"$out" | sed 's/<NextContinuationToken>//')
    [ -n "$token" ] || break
  done
}
ours() { [[ $1 != "$FAILED_LISTING" && $1 == "$P"* ]]; }
uploads() { # key<TAB>uploadId of every open upload under $1 (default: the run's prefix); a reply
  # that is not an upload listing prints $FAILED_LISTING instead (fail closed, as entries / keys)
  s3c "$H/$VB?uploads&prefix=$(enc "${1:-$P}")" | python3 -c 'import sys, xml.etree.ElementTree as ET
try: root = ET.fromstring(sys.stdin.read())
except ET.ParseError: root = None
ns = root.tag[:root.tag.index("}") + 1] if root is not None and root.tag.startswith("{") else ""
if root is None or root.tag != ns + "ListMultipartUploadsResult": print(sys.argv[1]); sys.exit(0)
for u in root.iter(ns + "Upload"): print("%s\t%s" % (u.findtext(ns + "Key"), u.findtext(ns + "UploadId")))' "$FAILED_LISTING"
}
bucket_gone() { [ "$(s3c -o /dev/null -w '%{http_code}' -I "$H/$1")" = 404 ]; }
abort_uploads() {
  uploads | while IFS=$'\t' read -r k id; do
    ours "$k" && s3c -o /dev/null -X DELETE "$H/$VB/$(enc "$k" /)?uploadId=$(enc "$id")"
  done
}
empty_versioned() {
  for _ in 1 2 3; do
    entries | while IFS=$'\t' read -r k v _ _; do
      ours "$k" && s3c -o /dev/null -X DELETE "$H/$VB/$(enc "$k" /)?versionId=$(enc "$v")"
    done
  done
}
empty_unversioned() {
  for _ in 1 2 3; do
    keys | while read -r k; do ours "$k" && s3c -o /dev/null -X DELETE "$H/$VB/$(enc "$k" /)"; done
  done
  # A NAS bucket keeps the directories of deleted keys: remove them deepest first.
  for _ in 1 2 3; do
    keys "${P%/}" | grep -v "^$FAILED_LISTING" | python3 -c 'import sys
dirs = set()
for key in sys.stdin.read().split():
    parts = key.split("/")[:-1]
    for i in range(1, len(parts) + 1): dirs.add("/".join(parts[:i]) + "/")
for d in sorted(dirs, key=lambda d: -d.count("/")): print(d)' | while read -r d; do
      ours "$d" && s3c -o /dev/null -X DELETE "$H/$VB/$(enc "$d" /)"
    done
    for d in $(printf '%s\n' "${NAS_DIRS[@]:-}" | awk 'NF' | awk -F/ '{print NF, $0}' | sort -rn | cut -d' ' -f2-); do
      s3c -o /dev/null -X DELETE "$H/$VB/$(enc "$d" /)"
    done
    s3c -o /dev/null -X DELETE "$H/$VB/$(enc "$P" /)"
  done
}
cleanup() {
  if [ "$MODE" = temporary ]; then
    [ "$CREATED" = 1 ] || { rm -rf "$WORK" "$LOCAL"; return; }
    abort_uploads; empty_versioned
    local code; code=$(s3c -o /dev/null -w '%{http_code}' -X DELETE "$H/$VB")
    if bucket_gone "$VB"; then echo "cleanup: $VB deleted"
    else echo "cleanup: $VB STILL PRESENT (delete=$code)"; FAILED=1; fi
    echo "buckets now: $(s3c "$H/" | grep -o '<Name>[^<]*' | sed 's/<Name>//' | tr '\n' ' ')"
  elif [ "$WROTE" = 1 ]; then
    abort_uploads
    local left
    if [ "$VERSIONED" = 1 ]; then empty_versioned; left=$(entries)
    else empty_unversioned; left=$(keys "${P%/}"); fi
    left=$(printf '%s\n%s' "$left" "$(uploads | grep -v '^$')" | awk 'NF')
    if grep -qxF "$FAILED_LISTING" <<<"$left"; then echo "cleanup: $VB/$P NOT CHECKED (listing failed)"; FAILED=1
    elif [ -z "$left" ]; then echo "cleanup: $VB/$P emptied (bucket kept)"
    else echo "cleanup: $VB/$P STILL HOLDS: $(tr '\n' ' ' <<<"$left")"; FAILED=1; fi
  fi
  rm -rf "$WORK" "$LOCAL"
}
trap 'rc=$?; cleanup; exit $(( rc || FAILED ))' EXIT
trap 'exit 130' INT TERM

purge() { # every record under $1 (below the run's prefix): probe leftovers out of the traversal's way
  local k v
  if [ "$VERSIONED" = 1 ]; then
    entries "$P$1" | while IFS=$'\t' read -r k v _ _; do ours "$k" && s3c -o /dev/null -X DELETE "$H/$VB/$(enc "$k" /)?versionId=$(enc "$v")"; done
  else
    keys "$P$1" | while read -r k; do ours "$k" && s3c -o /dev/null -X DELETE "$H/$VB/$(enc "$k" /)"; done
  fi
}
put() { # key (below the run's prefix), body
  printf '%s' "$2" >"$WORK/body"
  local code; code=$(s3c -o /dev/null -w '%{http_code}' -X PUT "$H/$VB/$(enc "$P$1" /)" --data-binary @"$WORK/body")
  [ "$code" = 200 ] || { echo "PUT $1: $code" >&2; FAILED=1; }
}
del() { s3c -o /dev/null -X DELETE "$H/$VB/$(enc "$P$1" /)"; }
versioning() {
  printf '<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>%s</Status></VersioningConfiguration>' "$1" >"$WORK/v.xml"
  s3c -o /dev/null -w "versioning $1: %{http_code}\n" -X PUT "$H/$VB?versioning" --data-binary @"$WORK/v.xml"
}

build=$(cargo build -q --example storage_role_operations --example s3_listing --example transfer_resume 2>&1) || { echo "$build" >&2; exit 1; }
if [ "$MODE" = temporary ]; then
  BODY=(-H 'Content-Length: 0')
  [ -n "${S3_LOCATION:-}" ] && BODY=(--data-binary "<CreateBucketConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><LocationConstraint>$S3_LOCATION</LocationConstraint></CreateBucketConfiguration>")
  # Only a bucket that does not exist yet: some stores answer 200 to CreateBucket on one we own.
  bucket_gone "$VB" || { echo "bucket $VB already exists; stopping" >&2; exit 1; }
  code=$(s3c -o "$WORK/create.out" -w '%{http_code}' -X PUT "$H/$VB" "${BODY[@]}")
  echo "create $VB: $code"
  [ "$code" = 200 ] || { cat "$WORK/create.out" >&2; echo "bucket $VB was not created by this run; stopping" >&2; exit 1; }
  CREATED=1
  versioning Enabled
else
  status=$(s3c -w '\n%{http_code}' "$H/$VB?versioning")
  code=$(tail -1 <<<"$status"); status=$(grep -o '<Status>[^<]*' <<<"$status" | sed 's/<Status>//')
  echo "bucket $VB: versioning=${status:-never enabled} (GET $code), prefix $P"
  [ "$code" = 200 ] || { [ "$MODE" = unversioned ] && [ "$code" = 501 ]; } ||
    { echo "cannot read the versioning of $VB; stopping" >&2; exit 1; }
  [ "$MODE" = versioned ] && [ "$status" != Enabled ] && { echo "refusing $VB: needs versioning Enabled" >&2; exit 1; }
  [ "$MODE" = unversioned ] && [ -n "$status" ] && { echo "refusing $VB: needs a bucket that never had versioning" >&2; exit 1; }
  if [ "$VERSIONED" = 1 ]; then held=$(entries); else held=$(keys "${P%/}"); fi
  [ -z "$held" ] || { echo "prefix $VB/$P is not empty, or its listing failed; stopping" >&2; exit 1; }
  WROTE=1
fi

case "${S3_COMPAT:-}" in sg) COMPAT=+sg ;; dxn) COMPAT=+dxn ;; *) COMPAT= ;; esac
TLS=; [ "${S3_USE_HTTPS:-}" = true ] && TLS=+https
export S3_LISTING_URL="s3$COMPAT$TLS://$S3_AK:$S3_SK@$VB.$S3_HOST/$P"
export S3_BUCKET=$VB
ROLE=target/debug/examples/storage_role_operations
LEG=target/debug/examples/s3_listing
TR=target/debug/examples/transfer_resume
traverse() { # stdout file, args...
  "$ROLE" --backend s3 --concurrency 8 traverse "${@:2}" >"$1" 2>"$1.err" \
    || { echo "traverse ${*:2} failed:" >&2; cat "$1.err" >&2; FAILED=1; }
}

if [ "$D6" = 1 ]; then
  # D6: what the NAS bucket keeps for zero-byte and content d/ keys, written before and after
  # their children. Nothing asserted; clean-up removes keys, then directories post-order.
  NAS_DIRS=()
  observe() { # label, dir name
    local d=$2
    echo "-- $1"
    printf '   HEAD %-10s %s\n' "$d/" "$(s3c -o /dev/null -I -w '%{http_code}' "$H/$VB/$(enc "$P$d/" /)")"
    printf '   HEAD %-10s %s\n' "$d" "$(s3c -o /dev/null -I -w '%{http_code}' "$H/$VB/$(enc "$P$d" /)")"
    printf '   HEAD %-10s %s\n' "$d/c" "$(s3c -o /dev/null -I -w '%{http_code}' "$H/$VB/$(enc "$P$d/c" /)")"
    printf '   GET  %-10s %s bytes\n' "$d/" "$(s3c -o /dev/null -w '%{http_code} %{size_download}' "$H/$VB/$(enc "$P$d/" /)")"
    s3c "$H/$VB?list-type=2&delimiter=%2F&prefix=$(enc "$P")" | python3 -c 'import sys, xml.etree.ElementTree as ET
root = ET.fromstring(sys.stdin.read()); ns = root.tag[:root.tag.index("}") + 1] if root.tag.startswith("{") else ""
p = sys.argv[1]; d = sys.argv[2]
keys = [(e.findtext(ns + "Key")[len(p):], e.findtext(ns + "Size")) for e in root.iter(ns + "Contents")]
prefixes = [x.findtext(ns + "Prefix")[len(p):] for x in root.iter(ns + "CommonPrefixes")]
print("   parent lists: objects=%s prefixes=%s" % ([k for k in keys if k[0].startswith(d)], [x for x in prefixes if x.startswith(d)]))' "$P" "$d"
    s3c "$H/$VB?list-type=2&delimiter=%2F&prefix=$(enc "$P$d/")" | python3 -c 'import sys, xml.etree.ElementTree as ET
root = ET.fromstring(sys.stdin.read()); ns = root.tag[:root.tag.index("}") + 1] if root.tag.startswith("{") else ""
p = sys.argv[1]
print("   %s/ lists: objects=%s prefixes=%s" % (sys.argv[2], [(e.findtext(ns + "Key")[len(p):], e.findtext(ns + "Size")) for e in root.iter(ns + "Contents")],
      [x.findtext(ns + "Prefix")[len(p):] for x in root.iter(ns + "CommonPrefixes")]))' "$P" "$d"
  }
  for spec in "zfirst:0:first" "zafter:0:after" "cfirst:5:first" "cafter:5:after"; do
    IFS=: read -r d bytes order <<<"$spec"
    body=$(head -c "$bytes" /dev/zero | tr '\0' 'x')
    NAS_DIRS+=("$P$d/")
    code1= code2=
    printf '%s' "$body" >"$WORK/body"
    if [ "$order" = first ]; then
      code1=$(s3c -o /dev/null -w '%{http_code}' -X PUT "$H/$VB/$(enc "$P$d/" /)" --data-binary @"$WORK/body")
      code2=$(s3c -o /dev/null -w '%{http_code}' -X PUT "$H/$VB/$(enc "$P$d/c" /)" --data-binary 'c')
    else
      code2=$(s3c -o /dev/null -w '%{http_code}' -X PUT "$H/$VB/$(enc "$P$d/c" /)" --data-binary 'c')
      code1=$(s3c -o /dev/null -w '%{http_code}' -X PUT "$H/$VB/$(enc "$P$d/" /)" --data-binary @"$WORK/body")
    fi
    observe "$d/ ($bytes B) written $order: PUT $d/=$code1 PUT $d/c=$code2" "$d"
  done
  traverse "$WORK/nas" --versions current --sizes
  echo "-- role traversal of the NAS prefix:"; sed 's/^/   /' "$WORK/nas"; sed 's/^/   stderr: /' "$WORK/nas.err"
  echo "RESULT: D6 recorded (no assertions)"
  exit 0
fi

put dirobj/ hello; put dirobj/child c
put lonely/ lonely
put emptydir/ ''; put emptydir/child c
put e/ ''
put plain.txt p
if [ "$VERSIONED" = 1 ]; then
  put z/ ''; del z/; put z/ ''
  put m/ fives; put m/ ''; del m/; put m/ 333; del m/
fi
BIG=$((BIG_MB * 1024 * 1024))
# Does the store take a multipart upload to a key ending in '/'? MinIO RELEASE.2023-03-20 does not
# (400 XMinioInvalidObjectName, though a single PUT of the same key succeeds).
# And does a completed one keep the key? RustFS 1.0.0 stores it without the '/' (HEAD x/ 404,
# HEAD x 200). MPU_DIR: 1 kept, 0 refused, 2 stored under another key.
probe=$(s3c -w '\n%{http_code}' -X POST "$H/$VB/$(enc "${P}probe/" /)?uploads")
if [ "$(tail -1 <<<"$probe")" = 200 ]; then
  id=$(grep -o '<UploadId>[^<]*' <<<"$probe" | sed 's/<UploadId>//')
  printf 'probe' >"$WORK/part"
  etag=$(s3c -D - -o /dev/null -X PUT "$H/$VB/$(enc "${P}probe/" /)?partNumber=1&uploadId=$(enc "$id")" --data-binary @"$WORK/part" | tr -d '\r' | sed -n 's/^[Ee][Tt][Aa][Gg]: *//p')
  printf '<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>' "$etag" >"$WORK/complete.xml"
  s3c -o /dev/null -X POST "$H/$VB/$(enc "${P}probe/" /)?uploadId=$(enc "$id")" --data-binary @"$WORK/complete.xml"
  if [ "$(s3c -o /dev/null -I -w '%{http_code}' "$H/$VB/$(enc "${P}probe/" /)")" = 200 ]; then MPU_DIR=1
  else
    MPU_DIR=2
    echo "store completes a multipart upload to probe/ under another key: HEAD probe/ $(s3c -o /dev/null -I -w '%{http_code}' "$H/$VB/$(enc "${P}probe/" /)"), HEAD probe $(s3c -o /dev/null -I -w '%{http_code}' "$H/$VB/$(enc "${P}probe" /)")"
  fi
else
  MPU_DIR=0
  echo "store refuses multipart uploads to keys ending in '/': $(tail -1 <<<"$probe") $(grep -o '<Code>[^<]*' <<<"$probe" | sed 's/<Code>//')"
fi
purge probe
# In a versioned bucket, does the store keep versions of a key ending in '/'? RustFS 1.0.0 does not
# (every write is the "null" version, a DELETE removes it). DIR_VERSIONED: 1 yes, 0 no.
DIR_VERSIONED=$VERSIONED
if [ "$VERSIONED" = 1 ]; then
  n=$(entries | awk -F'\t' -v k="${P}z/" '$1 == k' | grep -c .)
  if [ "$n" != 3 ]; then DIR_VERSIONED=0; echo "store keeps $n record(s) of z/ (written 0 B, deleted, 0 B): no versions of keys ending in '/'"; fi
fi
if [ "$MPU_DIR" = 1 ]; then
  out=$("$TR" --source "$LOCAL/src" --source-path big --seed-bytes "$BIG" --destination "s3:$P" --destination-path big/ 2>"$WORK/seed.err")
  check "seed big/ ($BIG_MB MiB, multipart to a d/ key)" "$(jq -r .result <<<"$out" 2>/dev/null)" ok
  [ "$(jq -r .result <<<"$out" 2>/dev/null)" = ok ] || { echo "   $out"; tail -3 "$WORK/seed.err" | sed 's/^/   /'; }
else
  head -c "$BIG" /dev/urandom >"$WORK/big"
  check "seed big/ ($BIG_MB MiB, one PUT)" "$(s3c -o /dev/null -w '%{http_code}' -X PUT "$H/$VB/$(enc "${P}big/" /)" --data-binary @"$WORK/big")" 200
  rm -f "$WORK/big"
  out=$("$TR" --source "$LOCAL/src" --source-path big --seed-bytes "$BIG" --destination "s3:${P}seed/" --destination-path big/ 2>"$WORK/seed.err")
  if [ "$MPU_DIR" = 0 ]; then
    check "multipart write of a d/ key refused, typed" "$(jq -r '.result + " " + .phase' <<<"$out") $(jq -r .failure <<<"$out" | grep -c '(Unsupported, Permanent)')" "failed Prepare 1"
  else
    # The upload completes, but not at the key: the read-back of the key fails, typed.
    check "multipart write of a d/ key caught at read-back" "$(jq -r '.result + " " + .phase' <<<"$out") $(jq -r .failure <<<"$out" | grep -c 'at big/ (NotFound')" "failed Verify 1"
    echo "   the store kept it as: $(keys "${P}seed/" | tr '\n' ' ')"
  fi
  purge seed/
fi

echo "== role traversal, current objects"
traverse "$WORK/cur" --versions current --sizes
sed 's/^/   /' "$WORK/cur"; sed 's/^/   stderr: /' "$WORK/cur.err"
if [ "$VERSIONED" = 1 ]; then
  # m/'s newest record is a delete marker: what the store's current listing says about it.
  echo "   raw ListObjectsV2 of ${P}m/: $(s3c "$H/$VB?list-type=2&delimiter=%2F&prefix=$(enc "${P}m/")" | grep -oE '<(Key|Size|Prefix)>[^<]*' | tr '\n' ' ')"
  echo "   raw root CommonPrefixes: $(s3c "$H/$VB?list-type=2&delimiter=%2F&prefix=$(enc "$P")" | grep -oE '<Prefix>[^<]*' | tr '\n' ' ')"
fi
expected_current="Directory big size=-
Directory dirobj size=-
Directory e size=-
Directory emptydir size=-
Directory lonely size=-
File big/ size=$BIG
File dirobj/ size=5
File dirobj/child size=1
File e/ size=0
File emptydir/ size=0
File emptydir/child size=1
File lonely/ size=6
File plain.txt size=1"
[ "$VERSIONED" = 1 ] && expected_current=$(printf '%s\nDirectory z size=-\nFile z/ size=0' "$expected_current" | sort)
# MinIO still lists the prefix m/ of the deleted directory object m/ (a store quirk, reported):
# the directory m is then an empty block, never an entry m/.
echo "   store lists the prefix of the deleted directory object m/: $(grep -c '^Directory m size=-$' "$WORK/cur")"
check "current entries" "$(grep -E '^(File|Directory) ' "$WORK/cur" | grep -v '^Directory m size=-$' | sort)" "$(sort <<<"$expected_current")"
check "current totals balanced, no failure" "$(grep -c '^totals balanced=true' "$WORK/cur") $(grep -c '^failure' "$WORK/cur.err")" "1 0"
check "current outcome" "$(grep -o 'outcome=Completed[^ ]*' "$WORK/cur" | head -1)" "outcome=Completed(TraversalCompletion"
for c in 1 4 32; do
  "$ROLE" --backend s3 --concurrency "$c" traverse --versions current --order blocks --sizes >"$WORK/blocks-$c" 2>/dev/null || FAILED=1
done
check "blocks identical at c=1/4/32" "$([ -s "$WORK/blocks-1" ] && cmp -s "$WORK/blocks-1" "$WORK/blocks-4" && cmp -s "$WORK/blocks-1" "$WORK/blocks-32" && echo yes)" yes

echo "== traversal roots"
traverse "$WORK/root-dirobj" --versions current --path dirobj --sizes
check "root dirobj: own object reported" "$(grep -c '^failure scope=Child dirobj/ Unsupported' "$WORK/root-dirobj.err")" 1
check "root dirobj: entries" "$(grep -E '^(File|Directory) ' "$WORK/root-dirobj" | tr '\n' ';')" "File dirobj/child size=1;"
traverse "$WORK/root-empty" --versions current --path emptydir --sizes
check "root emptydir: nothing of emptydir/" "$(grep -c 'emptydir/ ' "$WORK/root-empty" "$WORK/root-empty.err" | awk -F: '{s+=$2} END {print s}') $(grep -E '^(File|Directory) ' "$WORK/root-empty" | tr '\n' ';')" "0 File emptydir/child size=1;"

if [ "$VERSIONED" = 1 ]; then
  echo "== role traversal, every version"
  traverse "$WORK/all" --versions all
  grep -E ' (dirobj|z|m|e)/ ' "$WORK/all" | sed 's/^/   /'
  hist() { grep -E "^File $1 version=" "$WORK/all" | sed -E 's/.* latest=([01]) marker=([01]) size=(\S+).*/\1\2\3/' | tr '\n' ' '; }
  check "all dirobj/ (5 B)" "$(hist dirobj/)" "105 "
  check "all e/ (0 B)" "$(hist e/)" "100 "
  if [ "$DIR_VERSIONED" = 1 ]; then
    check "all z/ (0 B, marker, 0 B)" "$(hist z/)" "000 01- 100 "
    check "all m/ (5, 0, marker, 3, marker)" "$(hist m/)" "005 000 01- 003 11- "
  else
    check "all z/ (the one record the store keeps)" "$(hist z/)" "100 "
    check "all m/ (deleted for good by the store)" "$(hist m/)" ""
  fi
  check "all totals balanced, no failure" "$(grep -c '^totals balanced=true' "$WORK/all") $(grep -c '^failure' "$WORK/all.err")" "1 0"
fi

echo "== legacy walkdir / walkdir_2"
"$LEG" >"$WORK/walk" 2>"$WORK/walk.err"; "$LEG" --walkdir-2 >"$WORK/walk2" 2>"$WORK/walk2.err"
reported() { grep '^ERROR' "$1" | grep -oE 'S3 directory object \S+ holds [0-9]+ bytes' | awk '{print $4"="$6}' | sort | tr '\n' ' '; }
want="big/=$BIG dirobj/=5 lonely/=6 "
[ "$DIR_VERSIONED" = 1 ] && want="big/=$BIG dirobj/=5 lonely/=6 m/=5 "
check "walkdir reports own objects with content" "$(reported "$WORK/walk")" "$want"
check "walkdir_2 reports own objects with content" "$(reported "$WORK/walk2")" "$want"
check "legacy: no entry at a key ending in /" "$(grep -cE '^(ENTRY|FILE) \S+/ ' "$WORK/walk" "$WORK/walk2" | awk -F: '{s+=$2} END {print s}')" 0
check "legacy: other errors" "$(grep '^ERROR' "$WORK/walk" "$WORK/walk2" | grep -vc 'S3 directory object')" 0

echo "== copies"
copy() { # label, want result, transfer_resume args...
  local out; out=$("$TR" "${@:3}" 2>"$WORK/copy.err")
  echo "   $1: $(jq -c '{result, route, prepare, reused_bytes, phase, side, failure}' <<<"$out" 2>/dev/null || echo "$out")"
  LAST=$out
  check "$1" "$(jq -r .result <<<"$out" 2>/dev/null)" "$2"
}
compare() { # label, source path, destination prefix, destination path
  local out; out=$("$TR" --source "s3:$P" --source-path "$2" --destination "s3:$P$3" --destination-path "$4" --compare 2>&1)
  check "$1 equal" "$(jq -r .content_equal <<<"$out" 2>/dev/null)" true
}
copy "streamed dirobj/" ok --source "s3:$P" --source-path dirobj/ --destination "s3:${P}copy/" --destination-path dirobj/ --client-shaped
compare "streamed dirobj/" dirobj/ copy/ dirobj/
copy "streamed emptydir/ (0 B)" ok --source "s3:$P" --source-path emptydir/ --destination "s3:${P}copy/" --destination-path emptydir/ --client-shaped
compare "streamed emptydir/" emptydir/ copy/ emptydir/
copy "native dirobj/" ok --source "s3:$P" --source-path dirobj/ --destination "s3:${P}native/" --destination-path dirobj/
# Native wherever the endpoint copies server-side; the StorageGRID profile streams.
[ "${S3_COMPAT:-}" = sg ] && want_route=Streaming || want_route=Native
check "native dirobj/ route" "$(jq -r .route <<<"$LAST")" "$want_route"
compare "native dirobj/" dirobj/ native/ dirobj/
BIGARGS=(--source "s3:$P" --source-path big/ --destination "s3:${P}copy/" --destination-path big/ --client-shaped)
if [ "$MPU_DIR" = 1 ]; then
copy "streamed big/ cut" failed "${BIGARGS[@]}" --bandwidth 20971520 --cancel-after-ms 6000
pointers=$(keys "${P}copy/.data-mover-" | grep -c '\.upload$')
inside=$(keys "${P}copy/big/" | grep -c .)
open=$(uploads "${P}copy/big/" | awk -F'\t' -v k="${P}copy/big/" '$1 == k' | grep -c .)
check "cut: pointer in copy/, nothing in copy/big/, 1 upload" "$pointers $inside $open" "1 0 1"
copy "streamed big/ resumed" ok "${BIGARGS[@]}"
check "resumed big/ prepare" "$(jq -r .prepare <<<"$LAST" | cut -d' ' -f1)" Resumed
compare "resumed big/" big/ copy/ big/
check "after resume: no pointer, no upload" "$(keys "${P}copy/.data-mover-" | grep -c .) $(uploads "${P}copy/big/" | grep -c .)" "0 0"
copy "native big/" ok --source "s3:$P" --source-path big/ --destination "s3:${P}native/" --destination-path big/
compare "native big/" big/ native/ big/
check "native: no pointer left" "$(keys "${P}native/.data-mover-" | grep -c .)" 0
else
[ "$MPU_DIR" = 0 ] && WANT='(Unsupported, Permanent)' || WANT='at big/ (NotFound'
copy "streamed big/ (multipart refused or rekeyed)" failed "${BIGARGS[@]}"
check "streamed big/ failed, typed" "$(jq -r .failure <<<"$LAST" | grep -c -- "$WANT")" 1
copy "native big/ (multipart refused or rekeyed)" failed --source "s3:$P" --source-path big/ --destination "s3:${P}native/" --destination-path big/
if [ "$MPU_DIR" = 0 ]; then
  check "native big/ failed, typed" "$(jq -r .failure <<<"$LAST" | grep -c -- "$WANT")" 1
  check "failed: no pointer left" "$(keys "${P}copy/.data-mover-" | grep -c .) $(keys "${P}native/.data-mover-" | grep -c .)" "0 0"
else
  # RustFS: whatever the native copy of a d/ source fails with is recorded, not asserted; a stage
  # it keeps for a resume keeps its pointer (in native/, never inside native/big/).
  echo "   native big/ on this store: $(jq -r .failure <<<"$LAST"); pointers left: copy/ $(keys "${P}copy/.data-mover-" | grep -c .), native/ $(keys "${P}native/.data-mover-" | grep -c .)"
  check "failed: no pointer inside copy/big/ or native/big/" "$( { keys "${P}copy/big/"; keys "${P}native/big/"; } | grep -c .data-mover-)" 0
fi
[ "$MPU_DIR" = 2 ] && echo "   the store kept the copies as: $( { keys "${P}copy/"; keys "${P}native/"; } | grep -E '/big$' | tr '\n' ' ')"
fi
copy "S3→Local dirobj/" failed --source "s3:$P" --source-path dirobj/ --destination "$LOCAL/dst" --destination-path dirobj/
check "S3→Local refused at" "$(jq -r '.phase + " " + .side' <<<"$LAST")" "Preflight Destination"
check "S3→Local failure" "$(jq -r .failure <<<"$LAST" | grep -c 'Prepare failed at dirobj/ (Unsupported, Permanent)')" 1
# transfer_resume creates the parent directory itself (as terrasync does); the engine writes no file.
check "S3→Local wrote no file" "$(find "$LOCAL/dst" -type f 2>/dev/null | wc -l)" 0


[ "$FAILED" = 0 ] && echo "RESULT: pass" || echo "RESULT: FAIL"
