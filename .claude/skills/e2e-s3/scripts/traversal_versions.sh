#!/usr/bin/env bash
# ADR-0006 C22 / P7 C11b: the role-based S3 traversal against a real server
# (examples/storage_role_operations/traverse.rs): on a versioned bucket each key is ONE entry with
# its history (`File <key> history=<segment>/<complete> records=<n>[ order=inferred]`, then one
# `Record <key> <segment>.<n> version=<id|null> latest=<0|1> marker=<0|1> size=<n|->` line per
# version and delete marker), and `--current` prints the current objects only; plus the legacy
# walkdir / walkdir_2 (examples/s3_listing.rs). Three ways to pick the bucket:
#
# - Default: creates ONE temporary bucket on the .env's server, <BUCKET_PREFIX>-<run> (versioning
#   enabled), and on exit deletes it completely: every version and delete marker by id, every open
#   upload, then the bucket. It never touches the .env's own bucket.
# - S3_VERSIONED_BUCKET=<name>: an existing bucket of ours whose versioning is Enabled (ONTAP's
#   data-mover-test-ver: our user cannot create buckets). Refused unless the name starts with
#   data-mover- and GetBucketVersioning answers Enabled. Everything is written under a unique
#   prefix dm-<run>/, and on exit every version, delete marker and upload under that prefix is
#   deleted; the bucket and its versioning setting are left as they were (no suspended-versioning
#   step, so no "null" version checks).
# - S3_UNVERSIONED_BUCKET=<name>: an existing bucket of ours that never had versioning (MinIO /
#   RustFS / DXN / ONTAP data-mover-test, the ONTAP NAS bucket data-mover-nas). Refused unless the
#   name starts with data-mover- and GetBucketVersioning names no status. The same keys under
#   dm-<run>/, current objects only; on exit every object under the prefix is deleted, then the
#   directories a NAS bucket keeps for the written keys (deepest first); the bucket stays.
#
# The keys: k = v1 (2 B), v2 (3 B), a delete marker, v3 (4 B); gone = one version, then a delete
# marker; .data-mover-x and d/.data-mover-stage/y (artifacts); a zero-byte d/ marker and d/e;
# sub/deep/f; p/a000..p/a998 and p/z written three times (1, 2, 3 B), so p/z's versions straddle
# the first 1 000-entry page of ListObjectVersions; then (temporary bucket only), with versioning
# suspended, n written twice (one "null" version).
#
# The zero-byte d/ is a directory object (P7 F5, D8): one zero-byte file entry d/ of d (--current),
# one record of size 0 (histories); the legacy walks still skip it (only a d/ holding content is
# reported).
#
# Checks (versioned): k and gone are ONE entry each; k's records are 2, 3, marker, 4 with only the
# last latest; gone (deleted now, still listed) a version and a latest marker; p/z one entry of
# three records 1, 2, 3 across the first 1 000-entry page; n version=null latest=1; no artifact;
# --current lists k (4 B) without gone and no Record line; --order blocks gives the same lines at
# concurrency 1, 4 and 32; Unordered the same entries and records, each key once. Legacy
# walkdir / walkdir_2 (versioning re-enabled first: a suspended bucket is walked by ListObjectsV2):
# k as v1, v2, marker, v3 (ids as written), gone as its version then a latest marker (D4), p/z
# whole across the page, no artifact. Unversioned: Current and the legacy walks list each key once
# (k 4 B, p/z 3 B, no gone, no artifact, 999 p/a*); blocks identical at 1, 4, 32.
#
# EXPECT_DIR_MARKER (default 1): how many current entries the zero-byte d/ gives (P7 F5); 0 for a
# store that keeps no such object.
#
# History order: the entry of a key with a delete marker (k, gone) must carry ` order=inferred`
# exactly when EXPECT_ORDER=inferred (RustFS 1.0.0 lists each page's delete markers before its
# versions, P7 existing issue 11); every other entry never does.
#
# Also: whether the server honours `delimiter` (raw request, printed).
#
# Env: S3_ENV (default .claude/skills/e2e-s3/.env; e.g. .claude/skills/e2e-s3/dxn/.env) with S3_HOST,
# S3_AK, S3_SK (S3_USE_HTTPS / S3_COMPAT / S3_TLS_VERIFY shape the URL, .claude/skills/_shared/s3_url.sh). BUCKET_PREFIX names the temporary
# bucket (default data-mover-c22 → <prefix>-<run>); S3_LOCATION, when set, is the CreateBucket
# LocationConstraint (DXN). EXPECT_ORDER=server (default) | inferred. Exit status 0 only when every
# check passed and the bucket (temporary) or the prefix (existing bucket) is gone.
set -u
ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
cd "$ROOT"
S3_ENV=${S3_ENV:-.claude/skills/e2e-s3/.env}
set -a; . "$S3_ENV"; set +a
. .claude/skills/_shared/s3_url.sh
s3_url check check >/dev/null || exit 2   # a bad S3_COMPAT / S3_TLS_VERIFY fails before any request
RUN=${RUN:-$(date +%s)}
[[ $RUN =~ ^[a-z0-9-]{1,30}$ ]] || { echo "RUN must match [a-z0-9-]{1,30}: $RUN" >&2; exit 1; }
EXPECT_ORDER=${EXPECT_ORDER:-server}
case "$EXPECT_ORDER" in server|inferred) ;; *) echo "EXPECT_ORDER must be server or inferred" >&2; exit 1 ;; esac
[ -n "${S3_VERSIONED_BUCKET:-}" ] && [ -n "${S3_UNVERSIONED_BUCKET:-}" ] &&
  { echo "set S3_VERSIONED_BUCKET or S3_UNVERSIONED_BUCKET, not both" >&2; exit 1; }
if [ -n "${S3_VERSIONED_BUCKET:-}" ]; then MODE=versioned; VB=$S3_VERSIONED_BUCKET
elif [ -n "${S3_UNVERSIONED_BUCKET:-}" ]; then MODE=unversioned; VB=$S3_UNVERSIONED_BUCKET
else MODE=temporary; fi
if [ "$MODE" = temporary ]; then
  BUCKET_PREFIX=${BUCKET_PREFIX:-data-mover-c22}
  [[ $BUCKET_PREFIX =~ ^data-mover-[a-z0-9-]{1,20}$ ]] || { echo "BUCKET_PREFIX must be data-mover-<[a-z0-9-]>: $BUCKET_PREFIX" >&2; exit 1; }
  VB=$BUCKET_PREFIX-$RUN; P=
else
  # Only a bucket of ours: every lab bucket we may write is named data-mover-* (test-environments.md).
  [[ $VB =~ ^data-mover-[a-z0-9.-]{1,40}$ ]] || { echo "refusing bucket $VB: not one of ours (data-mover-*)" >&2; exit 1; }
  P=dm-$RUN/
fi
VERSIONED=1; [ "$MODE" = unversioned ] && VERSIONED=0
SCHEME=http; [ "${S3_USE_HTTPS:-}" = true ] && SCHEME=https
H="$SCHEME://$S3_HOST"
# The credentials reach curl through a config on a file descriptor, never on its command line.
esc() { local v=${1//\\/\\\\}; printf '%s' "${v//\"/\\\"}"; }
s3c() { curl -s --aws-sigv4 "aws:amz:us-east-1:s3" -K <(printf 'user = "%s:%s"\n' "$(esc "$S3_AK")" "$(esc "$S3_SK")") "$@"; }
enc() { python3 -c 'import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1], safe=sys.argv[2]))' "$1" "${2:-}"; }
WORK=/tmp/$VB-work-$RUN
mkdir -p "$WORK"
CREATED=0
WROTE=0
FAILED=0

# A listing reply that is not a listing prints this line instead of its entries (and the listing
# stops): a guard or a clean-up check reading it fails closed, never as "empty".
FAILED_LISTING='!listing-failed'
# key<TAB>versionId of every entry under the run's prefix (the whole temporary bucket).
entries() {
  local km="" vm="" out
  while :; do
    # StorageGRID refuses an empty version-id-marker (400): the first page asks no marker, a
    # later one the key marker and, only when the server named one, the version marker.
    out=$(s3c "$H/$VB?versions&prefix=$(enc "$P")${km:+&key-marker=$(enc "$km")${vm:+&version-id-marker=$(enc "$vm")}}")
    # On stdin: a 1 000-entry page is too long for one argument.
    python3 -c 'import sys, xml.etree.ElementTree as ET
try: root = ET.fromstring(sys.stdin.read())
except ET.ParseError: root = None
ns = root.tag[:root.tag.index("}") + 1] if root is not None and root.tag.startswith("{") else ""
if root is None or root.tag != ns + "ListVersionsResult": print(sys.argv[1]); sys.exit(1)
for tag in ("Version", "DeleteMarker"):
    for e in root.iter(ns + tag):
        print("%s\t%s" % (e.findtext(ns + "Key"), e.findtext(ns + "VersionId")))' "$FAILED_LISTING" <<<"$out" || break
    grep -q '<IsTruncated>true' <<<"$out" || break
    km=$(grep -o '<NextKeyMarker>[^<]*' <<<"$out" | sed 's/<NextKeyMarker>//')
    vm=$(grep -o '<NextVersionIdMarker>[^<]*' <<<"$out" | sed 's/<NextVersionIdMarker>//')
    # A truncated page without a key marker would ask the first page again, forever.
    [ -n "$km" ] || { echo "$FAILED_LISTING"; break; }
  done
}
# Every key under the run's prefix (ListObjectsV2, no delimiter), one per line.
keys() {
  local token="" out
  while :; do
    out=$(s3c "$H/$VB?list-type=2&prefix=$(enc "$P")${token:+&continuation-token=$(enc "$token")}")
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
# Whether key $1 is the run's to delete: under its prefix (every key of a temporary bucket), never
# the failed-listing line. A server that ignored `prefix` must not make the clean-up reach further.
ours() { [[ $1 != "$FAILED_LISTING" && $1 == "$P"* ]]; }
abort_uploads() {
  s3c "$H/$VB?uploads&prefix=$(enc "$P")" | grep -o '<Key>[^<]*</Key><UploadId>[^<]*' | sed 's/<Key>//; s/<\/Key><UploadId>/\t/' |
    while IFS=$'\t' read -r k id; do
      ours "$k" && s3c -o /dev/null -X DELETE "$H/$VB/$(enc "$k" /)?uploadId=$(enc "$id")"
    done
}
empty_versioned() {
  for _ in 1 2 3; do
    entries | while IFS=$'\t' read -r k v; do
      ours "$k" && s3c -o /dev/null -X DELETE "$H/$VB/$(enc "$k" /)?versionId=$(enc "$v")"
    done
  done
}
# An unversioned bucket: every object, then the directories a NAS bucket keeps for the written
# keys (their parents, deepest first, and the prefix itself).
empty_unversioned() {
  for _ in 1 2 3; do
    keys | while read -r k; do ours "$k" && s3c -o /dev/null -X DELETE "$H/$VB/$(enc "$k" /)"; done
  done
  python3 -c 'import sys
dirs = set()
for key in sys.argv[2:]:
    parts = (sys.argv[1] + key).split("/")[:-1]
    for i in range(1, len(parts) + 1): dirs.add("/".join(parts[:i]) + "/")
for d in sorted(dirs, key=lambda d: -d.count("/")): print(d)' "$P" k gone .data-mover-x d/.data-mover-stage/y d/e sub/deep/f p/z n |
    while read -r d; do s3c -o /dev/null -X DELETE "$H/$VB/$(enc "$d" /)"; done
}
cleanup() {
  if [ "$MODE" = temporary ]; then
    [ "$CREATED" = 1 ] || { rm -rf "$WORK"; return; }
    abort_uploads; empty_versioned
    local code; code=$(s3c -o /dev/null -w '%{http_code}' -X DELETE "$H/$VB")
    if s3c "$H/" | grep -q "<Name>$VB</Name>"; then echo "cleanup: $VB STILL PRESENT (delete=$code)"; FAILED=1
    else echo "cleanup: $VB deleted"; fi
    echo "buckets now: $(s3c "$H/" | grep -o '<Name>[^<]*' | sed 's/<Name>//' | tr '\n' ' ')"
  elif [ "$WROTE" = 1 ]; then
    abort_uploads
    local left
    if [ "$VERSIONED" = 1 ]; then empty_versioned; left=$(entries)
    else empty_unversioned; left=$(keys); fi
    if grep -qxF "$FAILED_LISTING" <<<"$left"; then echo "cleanup: $VB/$P NOT CHECKED (listing failed)"; FAILED=1
    elif [ -z "$left" ]; then echo "cleanup: $VB/$P emptied (bucket kept)"
    else echo "cleanup: $VB/$P STILL HOLDS $(grep -c . <<<"$left") entries"; FAILED=1; fi
  fi
  rm -rf "$WORK"
}
# The script's own exit status (a failed build, a bucket this run did not create) survives the
# cleanup; a clean exit still fails when a check or the cleanup did.
trap 'rc=$?; cleanup; exit $(( rc || FAILED ))' EXIT

# Every write appends key<TAB>x-amz-version-id to $WORK/ids (the order the versions were made).
written() { printf '%s\t%s\n' "$1" "$(tr -d '\r' <"$WORK/headers" | sed -n 's/^x-amz-version-id: *//Ip')" >>"$WORK/ids"; }
put() { # key (below the run's prefix), body
  printf '%s' "$2" >"$WORK/body"
  local code; code=$(s3c -D "$WORK/headers" -o /dev/null -w '%{http_code}' -X PUT "$H/$VB/$(enc "$P$1" /)" --data-binary @"$WORK/body")
  [ "$code" = 200 ] || { echo "PUT $1: $code" >&2; FAILED=1; }
  written "$1"
}
del() { s3c -D "$WORK/headers" -o /dev/null -X DELETE "$H/$VB/$(enc "$P$1" /)"; written "$1"; }
versioning() {
  printf '<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>%s</Status></VersioningConfiguration>' "$1" >"$WORK/v.xml"
  s3c -o /dev/null -w "versioning $1: %{http_code}\n" -X PUT "$H/$VB?versioning" --data-binary @"$WORK/v.xml"
}

build=$(cargo build -q --example storage_role_operations --example s3_listing 2>&1) || { echo "$build" >&2; exit 1; }
if [ "$MODE" = temporary ]; then
  LOCATION=()
  [ -n "${S3_LOCATION:-}" ] && LOCATION=(--data-binary "<CreateBucketConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><LocationConstraint>$S3_LOCATION</LocationConstraint></CreateBucketConfiguration>")
  code=$(s3c -o "$WORK/create.out" -w '%{http_code}' -X PUT "$H/$VB" "${LOCATION[@]}")
  echo "create $VB: $code"
  [ "$code" = 200 ] || { cat "$WORK/create.out" >&2; echo "bucket $VB was not created by this run; stopping" >&2; exit 1; }
  CREATED=1
  versioning Enabled
else
  status=$(s3c -w '\n%{http_code}' "$H/$VB?versioning")
  code=$(tail -1 <<<"$status"); status=$(grep -o '<Status>[^<]*' <<<"$status" | sed 's/<Status>//')
  echo "bucket $VB: versioning=${status:-never enabled} (GET $code), prefix $P"
  # A bucket without versioning support (ONTAP's NAS bucket) answers 501: never versioned.
  [ "$code" = 200 ] || { [ "$MODE" = unversioned ] && [ "$code" = 501 ]; } ||
    { echo "cannot read the versioning of $VB; stopping" >&2; exit 1; }
  if [ "$MODE" = versioned ] && [ "$status" != Enabled ]; then
    echo "refusing $VB: S3_VERSIONED_BUCKET needs versioning Enabled" >&2; exit 1
  fi
  if [ "$MODE" = unversioned ] && [ -n "$status" ]; then
    echo "refusing $VB: S3_UNVERSIONED_BUCKET needs a bucket that never had versioning" >&2; exit 1
  fi
  # Versions and delete markers count too: a prefix of old versions is not empty.
  if [ "$VERSIONED" = 1 ]; then held=$(entries); else held=$(keys); fi
  [ -z "$held" ] || { echo "prefix $VB/$P is not empty, or its listing failed; stopping" >&2; exit 1; }
  WROTE=1
fi
put k v1; put k 'v2!'; del k; put k 'v3!!'
put gone g; del gone
put .data-mover-x a; put d/.data-mover-stage/y a; put d/ ''; put d/e e; put sub/deep/f f
for i in $(seq -w 0 998); do put "p/a$i" x; done
put p/z 1; put p/z 22; put p/z 333
if [ "$MODE" = temporary ]; then
  versioning Suspended
  put n first; put n second
else
  put n second
fi
[ "$VERSIONED" = 1 ] && echo "server entries: $(entries | wc -l)" || echo "server keys: $(keys | wc -l)"

# Raw, as examples/endpoint_support builds it: the URL parser splits the key pair at the first
# ':' and the host at the last '@' and does not percent-decode. Only in this script's environment.
S3_LISTING_URL=$(s3_url "$VB" "$P") || exit 2
export S3_LISTING_URL
BIN=target/debug/examples/storage_role_operations
traverse() { # concurrency, args...
  "$BIN" --backend s3 --concurrency "$1" traverse "${@:2}" 2>"$WORK/stderr" \
    || { echo "traverse $* failed:" >&2; cat "$WORK/stderr" >&2; return 1; }
}
# The checks below count directory events, which only Blocks delivers: pin it. A versioned bucket
# lists each key with its history by itself; an unversioned one its current objects.
if [ "$VERSIONED" = 1 ]; then
  traverse 8 --order blocks >"$WORK/all" || FAILED=1
else
  : >"$WORK/all"
fi
traverse 8 --current --order blocks >"$WORK/current" || FAILED=1
for c in 1 4 32; do traverse "$c" --order blocks >"$WORK/blocks-$c" || FAILED=1; done
# Unordered (the default) delivers the same entries and records, each key once: compare the whole
# set, and that no key comes twice.
for c in 1 32; do traverse "$c" >"$WORK/unordered-$c" || FAILED=1; done
role_entries() { grep -E '^(File|Directory|Symlink|Record) ' "$1" | sort; }
# Each key once: this fixture's histories are far below the 100 000 records a key is segmented at.
twice() { grep -E '^File ' "$1" | awk '{print $2}' | sort | uniq -d | wc -l; }
for c in 1 32; do
  if cmp -s <(role_entries "$WORK/blocks-1") <(role_entries "$WORK/unordered-$c") \
    && [ "$(twice "$WORK/unordered-$c")" = 0 ] && [ "$(twice "$WORK/blocks-1")" = 0 ] \
    && ! grep -q '^listed ' "$WORK/unordered-$c"; then
    echo "unordered c=$c: same entries and records as blocks, each key once, no directory events"
  else
    echo "unordered c=$c: differs from blocks"; FAILED=1
  fi
done
[ "$VERSIONED" = 1 ] && { echo "-- histories (without p/a*)"; grep -v ' p/a' "$WORK/all"; }
echo "-- current (without p/a*)"; grep -v ' p/a' "$WORK/current"
cmp -s "$WORK/blocks-1" "$WORK/blocks-4" && cmp -s "$WORK/blocks-1" "$WORK/blocks-32" \
  && echo "blocks: identical at concurrency 1, 4, 32 ($(wc -l <"$WORK/blocks-1") lines)" \
  || { echo "blocks: output differs between concurrency levels"; FAILED=1; }
python3 - "$WORK/all" "$WORK/current" "$MODE" "$EXPECT_ORDER" "${EXPECT_DIR_MARKER:-1}" <<'PY' || FAILED=1
import re, sys
all_, cur, mode, expect, dir_marker = sys.argv[1:6]
def rows(path, name):
    # Entry lines of `name`: (segment, complete, records, order), all None without a history.
    out = []
    for line in open(path):
        m = re.match(r"File (\S+)(?: history=(\d+)/(\d) records=(\d+)(?: order=(\S+))?)?$", line.strip())
        if m and m.group(1) == name:
            out.append(m.groups()[1:])
    return out
def records(path, name):
    # Record lines of `name`, in order: (version, latest, marker, size).
    out = []
    for line in open(path):
        m = re.match(r"Record (\S+) \d+\.\d+ version=(\S+) latest=(\d) marker=(\d) size=(\S+)$", line.strip())
        if m and m.group(1) == name:
            out.append(m.groups()[1:])
    return out
def count(path, pred):
    return sum(1 for l in open(path) if pred(l))
checks = {
    "current k": (len(rows(cur, "k")), 1),
    "current gone": (len(rows(cur, "gone")), 0),
    "current p/z": (len(rows(cur, "p/z")), 1),
    "current p/a count": (count(cur, lambda l: l.startswith("File p/a")), 999),
    "current no records": (count(cur, lambda l: l.startswith("Record ")), 0),
    "current no history": (count(cur, lambda l: " history=" in l), 0),
    "current d/e": (len(rows(cur, "d/e")), 1),
    "current sub/deep/f": (len(rows(cur, "sub/deep/f")), 1),
    "current n": (len(rows(cur, "n")), 1),
    "current listed dirs": (count(cur, lambda l: l.startswith("listed ")), 5),
    "current complete": (count(cur, lambda l: l.startswith("listed ") and " Complete balanced=true " in l), 5),
}
if mode != "unversioned":
    # Keys with a delete marker (k, gone) are the ones a store that groups markers apart from
    # versions (RustFS) cannot order by the listing; every other key's order is the server's.
    def orders(name):
        want = expect if name in ("k", "gone") else "server"
        return ([r[3] or "server" for r in rows(all_, name)], [want] * len(rows(all_, name)))
    checks.update({
        "one entry each": ([len(rows(all_, n)) for n in ("k", "gone", "p/z", "n")], [1, 1, 1, 1]),
        "complete, one segment": ([rows(all_, n)[0][:2] for n in ("k", "gone", "p/z") if rows(all_, n)], [("0", "1")] * 3),
        "all k": ([(r[1], r[2], r[3]) for r in records(all_, "k")], [("0", "0", "2"), ("0", "0", "3"), ("0", "1", "-"), ("1", "0", "4")]),
        "all k records=": ([r[2] for r in rows(all_, "k")], ["4"]),
        "all gone": ([(r[1], r[2]) for r in records(all_, "gone")], [("0", "0"), ("1", "1")]),
        "all p/z": ([(r[1], r[3]) for r in records(all_, "p/z")], [("0", "1"), ("0", "2"), ("1", "3")]),
        "all p/a count": (count(all_, lambda l: l.startswith("File p/a")), 999),
        "all d/e": (len(rows(all_, "d/e")), 1),
        "all d/ marker": ([r[3] for r in records(all_, "d/")], ["0"]),
        "all sub/deep/f": (len(rows(all_, "sub/deep/f")), 1),
        "listed dirs": (count(all_, lambda l: l.startswith("listed ")), 5),
        "listings complete": (count(all_, lambda l: l.startswith("listed ") and " Complete balanced=true " in l), 5),
        "order k": orders("k"),
        "order gone": orders("gone"),
        "order p/z": orders("p/z"),
        "order n": orders("n"),
    })
    if mode == "temporary":
        checks["all n"] = ([(r[0], r[1]) for r in records(all_, "n")], [("null", "1")])
    else:
        checks["all n"] = ([(r[1], r[2]) for r in records(all_, "n")], [("1", "0")])
files = (all_, cur) if mode != "unversioned" else (cur,)
checks.update({
    "no artifacts": (sum(".data-mover" in l for f in files for l in open(f)), 0),
    # P7 F5 (D8): the zero-byte marker d/ is a zero-byte file entry of d, once per listing
    # (EXPECT_DIR_MARKER=0 for a store that keeps no such object, the ONTAP NAS bucket).
    "current d/ marker": (len(rows(cur, "d/")), int(dir_marker)),
    "totals balanced": (sum(l.startswith("totals balanced=true ") for f in files for l in open(f)), len(files)),
    "completed": (sum("outcome=Completed" in l for f in files for l in open(f)), len(files)),
})
bad = 0
for name, (got, want) in checks.items():
    ok = got == want
    bad += not ok
    print("%-20s %s%s" % (name, "ok" if ok else "FAIL", "" if ok else " got=%r want=%r" % (got, want)))
sys.exit(1 if bad else 0)
PY
echo "-- raw listings of the run's root with delimiter=/ (does the server honour it?)"
QUERIES=("list-type=2&delimiter=%2F&prefix=$(enc "$P")")
[ "$VERSIONED" = 1 ] && QUERIES=("versions&delimiter=%2F&prefix=$(enc "$P")" "${QUERIES[@]}")
for q in "${QUERIES[@]}"; do
  s3c "$H/$VB?$q" | python3 -c 'import sys, xml.etree.ElementTree as ET
root = ET.fromstring(sys.stdin.read()); ns = root.tag[:root.tag.index("}") + 1] if root.tag.startswith("{") else ""
p = sys.argv[2]
keys = [e.findtext(ns + "Key")[len(p):] for tag in ("Version", "DeleteMarker", "Contents") for e in root.iter(ns + tag)]
prefixes = [x.findtext(ns + "Prefix")[len(p):] for x in root.iter(ns + "CommonPrefixes")]
deep = sum("/" in k for k in keys)
print("%-24s entries=%d distinct keys=%s keys with a /=%d CommonPrefixes=%s truncated=%s -> delimiter %s" % (
    sys.argv[1].split("&")[0], len(keys), sorted(set(keys)) if len(set(keys)) < 10 else len(set(keys)), deep, prefixes,
    root.findtext(ns + "IsTruncated"), "honoured" if prefixes and not deep else "IGNORED"))' "$q" "$P"
done

echo "-- legacy walkdir / walkdir_2"
# A suspended bucket is walked by ListObjectsV2: the temporary one is re-enabled first.
[ "$MODE" = temporary ] && versioning Enabled
LEG=target/debug/examples/s3_listing
"$LEG" >"$WORK/walk" 2>"$WORK/walk.err" || { echo "walkdir failed:"; tail -5 "$WORK/walk.err"; FAILED=1; }
"$LEG" --walkdir-2 >"$WORK/walk2" 2>"$WORK/walk2.err" || { echo "walkdir_2 failed:"; tail -5 "$WORK/walk2.err"; FAILED=1; }
echo "walkdir (without p/a*):"; grep -v ' p/a' "$WORK/walk" | sed 's/^/   /'
echo "walkdir_2 (without p/a*):"; grep -v ' p/a' "$WORK/walk2" | sed 's/^/   /'
python3 - "$WORK/walk" "$WORK/walk2" "$WORK/ids" "$MODE" "$EXPECT_ORDER" <<'PY2' || FAILED=1
import re, sys
mode, expect = sys.argv[4], sys.argv[5]
def rows(path, kind):
    out, orders = {}, {}
    for line in open(path):
        m = re.match(kind + r" (\S+) (\S+) latest=(\d) marker=(\d) count=(\S+)(?: order=(\S+))?$", line.strip())
        if m:
            out.setdefault(m.group(1), []).append(m.groups()[1:5])
            orders.setdefault(m.group(1), []).append(m.group(6) or "server")
    return out, orders
w, wo = rows(sys.argv[1], "ENTRY")
w2, w2o = rows(sys.argv[2], "FILE")
checks = {
    "walkdir p/a count": (sum(1 for p in w if p.startswith("p/a")), 999),
    "walkdir_2 p/a count": (sum(1 for p in w2 if p.startswith("p/a")), 999),
    "legacy no artifacts": (sum(".data-mover" in l for f in sys.argv[1:3] for l in open(f)), 0),
    "legacy no errors": (sum(l.startswith(("ERROR", "TIMEOUT")) for f in sys.argv[1:3] for l in open(f)), 0),
}
if mode == "unversioned":
    checks.update({
        "legacy order": (sorted({o for rows in (wo, w2o) for v in rows.values() for o in v}), ["server"]),
        "walkdir keys once": ({k: len(v) for k, v in w.items() if k in ("k", "gone", "p/z", "n", "d/e", "sub/deep/f")},
                              {"k": 1, "p/z": 1, "n": 1, "d/e": 1, "sub/deep/f": 1}),
        "walkdir_2 keys once": ({k: len(v) for k, v in w2.items() if k in ("k", "gone", "p/z", "n", "d/e", "sub/deep/f")},
                                {"k": 1, "p/z": 1, "n": 1, "d/e": 1, "sub/deep/f": 1}),
    })
else:
    ids = {}
    for line in open(sys.argv[3]):
        k, _, v = line.rstrip("\n").partition("\t")
        ids.setdefault(k, []).append(v)
    k, g, z = ids["k"], ids["gone"], ids["p/z"]
    def order(o, name):
        want = expect if name in ("k", "gone") else "server"
        return (o.get(name), [want] * len(o.get(name, [])))
    checks.update({
        "walkdir k": (w.get("k"), [(k[0], "0", "0", "4"), (k[1], "0", "0", "4"), (k[2], "0", "1", "4"), (k[3], "1", "0", "4")]),
        "walkdir gone (D4)": (w.get("gone"), [(g[0], "0", "0", "2"), (g[1], "1", "1", "2")]),
        "walkdir p/z": (w.get("p/z"), [(z[0], "0", "0", "3"), (z[1], "0", "0", "3"), (z[2], "1", "0", "3")]),
        "walkdir n": ([r[1:] for r in w.get("n", [])], [("1", "0", "1")]),
        "walkdir_2 k": (w2.get("k"), [(k[0], "0", "0", "3"), (k[1], "0", "0", "3"), (k[3], "1", "0", "3")]),
        "walkdir_2 gone (D4)": (w2.get("gone"), [(g[0], "0", "0", "1")]),
        "walkdir_2 p/z": (w2.get("p/z"), [(z[0], "0", "0", "3"), (z[1], "0", "0", "3"), (z[2], "1", "0", "3")]),
        "walkdir order k": order(wo, "k"),
        "walkdir order gone": order(wo, "gone"),
        "walkdir order p/z": order(wo, "p/z"),
        "walkdir_2 order k": order(w2o, "k"),
        "walkdir_2 order gone": order(w2o, "gone"),
        "walkdir_2 order p/z": order(w2o, "p/z"),
    })
bad = 0
for name, (got, want) in checks.items():
    ok = got == want
    bad += not ok
    print("%-20s %s%s" % (name, "ok" if ok else "FAIL", "" if ok else " got=%r want=%r" % (got, want)))
sys.exit(1 if bad else 0)
PY2
[ "$FAILED" = 0 ] && echo "RESULT: pass" || echo "RESULT: FAIL"
