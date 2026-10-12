#!/usr/bin/env bash
# P7 F5: S3 "directory objects" — keys ending in '/' (with or without content) — against a real
# server: the role-based traversal lists each as a file entry of its own directory, and
# S3→S3 copies recreate them by ONE request whatever their size
# (P7 D13: one PUT, or one CopyObject natively; never multipart, never resumed), and S3→Local
# refuses them at Preflight.
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
# (BIG_MB MiB, default 200, written by data-mover from a local file: one PUT, D13).
# Versioned buckets also get z/ = 0 B, delete marker, 0 B (all-zero history) and m/ = 5 B, 0 B,
# delete marker, 3 B, delete marker (mixed history).
#
# Checks:
# - Current traversal (--sizes): every seeded key once, own objects as `File <d>/ size=<n>`, the
#   zero-byte ones included; balanced totals, no failure; --order blocks identical at c=1/4/32.
# - Traversal roots: --path dirobj reports `failure scope=Child dirobj/ Unsupported` and does not
#   emit dirobj/; --path emptydir reports and emits nothing of emptydir/.
# - Histories (versioned): each own object one entry; dirobj/ one 5 B version; z/ 0 B, marker, 0 B;
#   m/ 5 B, 0 B, marker, 3 B, marker (latest last), listed although deleted now.
# - Copies (transfer_resume, same bucket): dirobj/ and emptydir/ streamed (single PUT), dirobj/
#   native; D13 — every write of a directory object is one request (its ETag has no -<parts>), a
#   real file at the same key without the '/' (copy/mid, copy/big, native/big) is never touched
#   (RustFS stores a completed multipart x/ as x): mid/ (12 MiB, over the 8 MiB single-PUT
#   threshold) Local→S3 under each policy, big/ streamed cut by --cancel-after-ms (leaves nothing:
#   no object, pointer or upload) then run again (starts over: Fresh), big/ native; each compared
#   byte for byte. S3→Local of dirobj/ fails at Preflight on the destination and writes no file
#   (transfer_resume itself creates the parent directory, as a caller does). > 5 GiB is refused
#   before any I/O: covered by unit tests, not uploaded here.
#
# Versioned buckets also replay (P7 H1, examples/s3_history_replay.rs) the histories of m/, z/ and
# dirobj/ oldest to newest into a versioned destination — a second temporary bucket <bucket>-r in
# temporary mode, replay/ under the run's prefix otherwise — and compare each key's records by kind and
# size in order; print what a second marker in a row gives (merged / written) and DXN's null-version
# probe (temporary destination only); and check that an unversioned bucket (UNVERSIONED_BUCKET, default
# data-mover-test) and the suspended temporary destination skip every marker, never write one.
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
source .claude/skills/_shared/cargo_examples.sh
S3_ENV=${S3_ENV:-.claude/skills/e2e-s3/.env}
set -a; . "$S3_ENV"; set +a
. .claude/skills/_shared/s3_url.sh
s3_url check check >/dev/null || exit 2   # a bad S3_COMPAT / S3_TLS_VERIFY fails before any request
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
    # StorageGRID refuses an empty version-id-marker (400): the first page asks no marker, a
    # later one the key marker and, only when the server named one, the version marker.
    out=$(s3c "$H/$VB?versions&prefix=$(enc "${1:-$P}")${km:+&key-marker=$(enc "$km")${vm:+&version-id-marker=$(enc "$vm")}}")
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
    # A truncated page without a key marker would ask the first page again, forever.
    [ -n "$km" ] || { echo "$FAILED_LISTING"; break; }
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
cleanup_replay() { # the temporary replay destination: every version and marker, then the bucket
  [ -n "${REPLAY_CREATED:-}" ] || return
  local saved=$VB saved_p=$P; VB=$DB; P=
  for _ in 1 2 3; do
    entries "" | while IFS=$'\t' read -r k v _ _; do
      [ "$k" != "$FAILED_LISTING" ] && s3c -o /dev/null -X DELETE "$H/$VB/$(enc "$k" /)?versionId=$(enc "$v")"
    done
  done
  s3c -o /dev/null -X DELETE "$H/$VB"
  if bucket_gone "$VB"; then echo "cleanup: $VB deleted"; else echo "cleanup: $VB STILL PRESENT"; FAILED=1; fi
  VB=$saved; P=$saved_p
}
cleanup() {
  cleanup_replay
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
    # A store that cannot list uploads at all (the ONTAP NAS bucket's policy) was seen so at the
    # start: only then is the upload check skipped.
    [ "${UPLOADS_LISTABLE:-1}" = 1 ] && left=$(printf '%s\n%s' "$left" "$(uploads | grep -v '^$')" | awk 'NF')
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

cargo_build_examples storage_role_operations transfer_resume s3_history_replay || exit $?
ROLE=$(cargo_example_binary storage_role_operations) || exit 1
TR=$(cargo_example_binary transfer_resume) || exit 1
HR=$(cargo_example_binary s3_history_replay) || exit 1
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
  UPLOADS_LISTABLE=1
  grep -qxF "$FAILED_LISTING" <<<"$(uploads)" && { UPLOADS_LISTABLE=0; echo "the store lists no multipart uploads here: the clean-up cannot check them"; }
  [ -z "$held" ] || { echo "prefix $VB/$P is not empty, or its listing failed; stopping" >&2; exit 1; }
  WROTE=1
fi

S3_LISTING_URL=$(s3_url "$VB" "$P") || exit 2
export S3_LISTING_URL
export S3_BUCKET=$VB
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
  traverse "$WORK/nas" --current --sizes
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
# Recorded, not asserted (why data-mover never sends one, D13): does the store take a multipart
# upload to a key ending in '/'? MinIO RELEASE.2023-03-20 does not (400 XMinioInvalidObjectName,
# though a single PUT of the same key succeeds). And does a completed one keep the key? RustFS
# 1.0.0 stores it without the '/' (HEAD x/ 404, HEAD x 200).
probe=$(s3c -w '\n%{http_code}' -X POST "$H/$VB/$(enc "${P}probe/" /)?uploads")
if [ "$(tail -1 <<<"$probe")" = 200 ]; then
  id=$(grep -o '<UploadId>[^<]*' <<<"$probe" | sed 's/<UploadId>//')
  printf 'probe' >"$WORK/part"
  etag=$(s3c -D - -o /dev/null -X PUT "$H/$VB/$(enc "${P}probe/" /)?partNumber=1&uploadId=$(enc "$id")" --data-binary @"$WORK/part" | tr -d '\r' | sed -n 's/^[Ee][Tt][Aa][Gg]: *//p')
  printf '<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>' "$etag" >"$WORK/complete.xml"
  s3c -o /dev/null -X POST "$H/$VB/$(enc "${P}probe/" /)?uploadId=$(enc "$id")" --data-binary @"$WORK/complete.xml"
  if [ "$(s3c -o /dev/null -I -w '%{http_code}' "$H/$VB/$(enc "${P}probe/" /)")" = 200 ]; then
    echo "store keeps a multipart upload to probe/ at its key"
  else
    echo "store completes a multipart upload to probe/ under another key: HEAD probe/ $(s3c -o /dev/null -I -w '%{http_code}' "$H/$VB/$(enc "${P}probe/" /)"), HEAD probe $(s3c -o /dev/null -I -w '%{http_code}' "$H/$VB/$(enc "${P}probe" /)")"
  fi
else
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
# The ETag of an object one request wrote is its MD5 (or at least not a multipart -<parts> one): a
# heuristic that holds on the stores listed in SKILL.md; the untouched real files below decide.
one_request() { local e; e=$(s3c -I "$H/$VB/$(enc "$1" /)" | tr -d '\r' | sed -n 's/^[Ee][Tt][Aa][Gg]: *//p'); [ -n "$e" ] && [[ $e != *-* ]] && echo yes || echo "no (ETag ${e:-none})"; }
out=$("$TR" --source "$LOCAL/src" --source-path big --seed-bytes "$BIG" --destination "s3:$P" --destination-path big/ 2>"$WORK/seed.err")
check "seed big/ ($BIG_MB MiB, one PUT to a d/ key)" "$(jq -r .result <<<"$out" 2>/dev/null) $(one_request "${P}big/")" "ok yes"
[ "$(jq -r .result <<<"$out" 2>/dev/null)" = ok ] || { echo "   $out"; tail -3 "$WORK/seed.err" | sed 's/^/   /'; }

echo "== role traversal, current objects"
traverse "$WORK/cur" --current --sizes
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
# The directory m of the deleted directory object m/: a versioned bucket is listed with histories
# (P7 C11b), where m/ still has records, so its prefix m is listed (--current then prints no m/);
# on a store that keeps no versions of m/ it is gone. Either way never an entry m/ here.
echo "   directory m listed beside the deleted directory object m/: $(grep -c '^Directory m size=-$' "$WORK/cur")"
check "current entries" "$(grep -E '^(File|Directory) ' "$WORK/cur" | grep -v '^Directory m size=-$' | sort)" "$(sort <<<"$expected_current")"
check "current totals balanced, no failure" "$(grep -c '^totals balanced=true' "$WORK/cur") $(grep -c '^failure' "$WORK/cur.err")" "1 0"
check "current outcome" "$(grep -o 'outcome=Completed[^ ]*' "$WORK/cur" | head -1)" "outcome=Completed(TraversalCompletion"
for c in 1 4 32; do
  "$ROLE" --backend s3 --concurrency "$c" traverse --current --order blocks --sizes >"$WORK/blocks-$c" 2>/dev/null || FAILED=1
done
check "blocks identical at c=1/4/32" "$([ -s "$WORK/blocks-1" ] && cmp -s "$WORK/blocks-1" "$WORK/blocks-4" && cmp -s "$WORK/blocks-1" "$WORK/blocks-32" && echo yes)" yes

echo "== traversal roots"
traverse "$WORK/root-dirobj" --current --path dirobj --sizes
check "root dirobj: own object reported" "$(grep -c '^failure scope=Child dirobj/ Unsupported' "$WORK/root-dirobj.err")" 1
check "root dirobj: entries" "$(grep -E '^(File|Directory) ' "$WORK/root-dirobj" | tr '\n' ';')" "File dirobj/child size=1;"
traverse "$WORK/root-empty" --current --path emptydir --sizes
check "root emptydir: nothing of emptydir/" "$(grep -c 'emptydir/ ' "$WORK/root-empty" "$WORK/root-empty.err" | awk -F: '{s+=$2} END {print s}') $(grep -E '^(File|Directory) ' "$WORK/root-empty" | tr '\n' ';')" "0 File emptydir/child size=1;"

if [ "$VERSIONED" = 1 ]; then
  echo "== role traversal, each key with its history"
  traverse "$WORK/all"
  grep -E ' (dirobj|z|m|e)/ ' "$WORK/all" | sed 's/^/   /'
  hist() { grep -E "^Record $1 " "$WORK/all" | sed -E 's/.* latest=([01]) marker=([01]) size=(\S+).*/\1\2\3/' | tr '\n' ' '; }
  # Each key ending in '/' is one entry of its directory, whatever its history (m/ deleted now too).
  for k in dirobj/ e/ z/; do check "one entry $k" "$(grep -cE "^File $k " "$WORK/all")" 1; done
  [ "$DIR_VERSIONED" = 1 ] && check "one entry m/ (deleted now)" "$(grep -cE '^File m/ ' "$WORK/all")" 1
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
# D13: one request per directory object whatever its size — never multipart, never resumed —
# and a real file at the same key without the '/' is never touched (RustFS stores a completed
# multipart x/ as x).
REAL='a real file'
put copy/mid "$REAL"; put copy/big "$REAL"; put native/big "$REAL"
MID=$((12 * 1024 * 1024))
for policy in checkpointed atomic direct; do
  copy "mid/ (12 MiB) Local→S3, $policy" ok --source "$LOCAL/src" --source-path mid --seed-bytes "$MID" --destination "s3:${P}copy/" --destination-path mid/ --policy "$policy"
  check "mid/ ($policy): one request" "$(one_request "${P}copy/mid/")" yes
  out=$("$TR" --source "$LOCAL/src" --source-path mid --destination "s3:${P}copy/" --destination-path mid/ --compare 2>&1)
  check "mid/ ($policy) equal" "$(jq -r .content_equal <<<"$out" 2>/dev/null)" true
done
BIGARGS=(--source "s3:$P" --source-path big/ --destination "s3:${P}copy/" --destination-path big/ --client-shaped)
copy "streamed big/ cut" failed "${BIGARGS[@]}" --bandwidth 20971520 --cancel-after-ms 6000
check "cut: no object, pointer or upload left" "$(keys "${P}copy/big/" | grep -c .) $(keys "${P}copy/.data-mover-" | grep -c .) $(uploads "${P}copy/big/" | grep -c .)" "0 0 0"
copy "streamed big/ again" ok "${BIGARGS[@]}"
check "again: starts over" "$(jq -r .prepare <<<"$LAST" | cut -d' ' -f1)" Fresh
check "streamed big/: one request" "$(one_request "${P}copy/big/")" yes
compare "streamed big/" big/ copy/ big/
copy "native big/" ok --source "s3:$P" --source-path big/ --destination "s3:${P}native/" --destination-path big/
check "native big/ route" "$(jq -r .route <<<"$LAST")" "$want_route"
check "native big/: one request" "$(one_request "${P}native/big/")" yes
compare "native big/" big/ native/ big/
check "real files copy/mid, copy/big, native/big untouched" "$(for k in copy/mid copy/big native/big; do s3c "$H/$VB/$(enc "$P$k" /)"; echo; done | sort -u)" "$REAL"
check "no pointer, no upload left" "$(keys "${P}copy/.data-mover-" | grep -c .) $(keys "${P}native/.data-mover-" | grep -c .) $(uploads "${P}copy/" | grep -c .) $(uploads "${P}native/" | grep -c .)" "0 0 0 0"
copy "S3→Local dirobj/" failed --source "s3:$P" --source-path dirobj/ --destination "$LOCAL/dst" --destination-path dirobj/
check "S3→Local refused at" "$(jq -r '.phase + " " + .side' <<<"$LAST")" "Preflight Destination"
check "S3→Local failure" "$(jq -r .failure <<<"$LAST" | grep -c 'Prepare failed at dirobj/ (Unsupported, Permanent)')" 1
# transfer_resume creates the parent directory itself (as terrasync does); the engine writes no file.
check "S3→Local wrote no file" "$(find "$LOCAL/dst" -type f 2>/dev/null | wc -l)" 0


if [ "$VERSIONED" = 1 ]; then
  echo "== history replay (P7 H1)"
  # The destination: a second temporary bucket (temporary mode), else the same bucket under replay/.
  if [ "$MODE" = temporary ]; then
    DB=$VB-r
    bucket_gone "$DB" && code=$(s3c -o "$WORK/create2.out" -w '%{http_code}' -X PUT "$H/$DB" "${BODY[@]}") || code=exists
    echo "create $DB: $code"
    if [ "$code" = 200 ]; then
      REPLAY_CREATED=1
      # DXN's null-version quirk: a key whose only record is the null version, versioning then
      # enabled and a version written, and a delete marker replayed onto it.
      s3c -o /dev/null -X PUT "$H/$DB/nullkey" --data-binary 'null!'
      VB_SAVED=$VB; VB=$DB; versioning Enabled; VB=$VB_SAVED
      s3c -o /dev/null -X PUT "$H/$DB/nullkey" --data-binary 'second'
      echo "   null-version probe: $("$HR" --source "s3:$P" --destination "s3@$DB:" --path nullkey --delete-marker 2>&1 | tail -1)"
      DEST="s3@$DB:"; DQ=; DVB=$DB
    else
      cat "$WORK/create2.out" >&2; FAILED=1; DEST=; DVB=
    fi
  else
    DEST="s3@$VB:${P}replay/"; DQ=${P}replay/; DVB=$VB
  fi
  if [ -n "$DEST" ]; then
    "$HR" --source "s3:$P" --destination "$DEST" --path m/ --path z/ --path dirobj/ --client-shaped >"$WORK/replay" 2>"$WORK/replay.err"
    sed 's/^/   /' "$WORK/replay"
    [ "$DIR_VERSIONED" = 1 ] && want_records=9 || want_records=2
    check "replay: records, none failed" "$(grep -oE 'records=[0-9]+ .* failed=[0-9]+' "$WORK/replay" | sed -E 's/records=([0-9]+) .* failed=/\1 /')" "$want_records 0"
    # Compare each key's records at the source and the destination by kind and size, in order,
    # as the role traversal of a versioned bucket orders them (the server's order, or inferred).
    shape() { # storage URL root, key
      S3_LISTING_URL="$1" "$ROLE" --backend s3 --concurrency 4 traverse 2>/dev/null |
        grep -E "^Record $2 " | sed -E 's/.* marker=([01]) size=(\S+).*/\1:\2/' | tr '\n' ' '
    }
    DURL=$(s3_url "$DVB" "$DQ") || exit 2
    for key in m/ z/ dirobj/; do
      src=$(shape "$S3_LISTING_URL" "$key"); dst=$(shape "$DURL" "$key")
      echo "   $key source: $src"; echo "   $key destination: $dst"
      # A store that keeps no versions of d/ keys (RustFS) deleted m/ for good: nothing on either side.
      [ "$DIR_VERSIONED" = 0 ] && [ -z "$src" ] && want_shape= || want_shape=${src:-<no source records>}
      check "replay $key: same kinds and sizes in order" "$dst" "$want_shape"
    done
    echo "   second marker in a row on m/: $("$HR" --source "s3:$P" --destination "$DEST" --path m/ --delete-marker 2>&1 | tail -1)"
    echo "   raw destination m/: $(VB=$DVB entries "${DQ}m/" | awk -F'\t' -v k="${DQ}m/" '$1 == k {print $3":"$4}' | tr '\n' ' ')"
    # An unversioned bucket (data-mover-test) gets the latest state only (D12); on the temporary
    # destination, once suspended, a marker is refused.
    UB=${UNVERSIONED_BUCKET:-data-mover-test}
    "$HR" --source "s3:$P" --destination "s3@$UB:${P}refusal/" --path m/ --client-shaped >"$WORK/refusal" 2>&1
    sed 's/^/   unversioned: /' "$WORK/refusal"
    # D12: the latest state only — m/'s newest record is a delete marker, so nothing is written.
    [ "$DIR_VERSIONED" = 1 ] && want_skipped=1 || want_skipped=0
    check "unversioned destination: history not rebuilt" "$(grep -c 'history not rebuilt' "$WORK/refusal") $(grep -c -- '-> skipped' "$WORK/refusal") $(grep -cE -- '-> (written|copied)' "$WORK/refusal")" "1 $want_skipped 0"
    check "unversioned destination: m/ not written" "$(VB=$UB keys "${P}refusal/" | grep -c .)" 0
    for k in $(VB=$UB keys "${P}refusal/"); do ours "$k" && s3c -o /dev/null -X DELETE "$H/$UB/$(enc "$k" /)"; done
    s3c -o /dev/null -X DELETE "$H/$UB/$(enc "${P}refusal/m/" /)"; s3c -o /dev/null -X DELETE "$H/$UB/$(enc "${P}refusal/" /)"
    if [ "$MODE" = temporary ] && [ -n "${REPLAY_CREATED:-}" ]; then
      VB_SAVED=$VB; VB=$DB; versioning Suspended; VB=$VB_SAVED
      out=$("$HR" --source "s3:$P" --destination "$DEST" --path m/ --delete-marker 2>&1 | tail -1)
      echo "   suspended: $out"
      check "suspended destination: marker skipped" "$(grep -c -- '-> skipped' <<<"$out")" 1
    fi
  fi
fi
[ "$FAILED" = 0 ] && echo "RESULT: pass" || echo "RESULT: FAIL"
