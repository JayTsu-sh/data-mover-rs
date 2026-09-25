#!/usr/bin/env bash
# ADR-0006 C22: the role-based S3 traversal in both version modes against a real server
# (examples/storage_role_operations.rs `traverse --versions current|all`). Creates ONE temporary
# bucket on the .env's server, data-mover-c22-<run> (versioning enabled), and on exit deletes it
# completely: every version and delete marker by id, every open upload, then the bucket. It never
# touches the .env's own bucket.
#
# The bucket holds: k = v1 (2 B), v2 (3 B), a delete marker, v3 (4 B); gone = one version, then a
# delete marker; .data-mover-x and d/.data-mover-stage/y (artifacts); a zero-byte d/ marker and
# d/e; sub/deep/f; p/a000..p/a998 and p/z written three times (1, 2, 3 B), so p/z's versions
# straddle the first 1 000-entry page of ListObjectVersions; then, with versioning suspended, n
# written twice (one "null" version).
#
# Checks: All lists k as 2, 3, marker, 4 with only the last latest, gone as a version and a latest
# marker, p/z as 1, 2, 3, n as version=null latest=1, no artifact; Current lists k (4 B) without
# gone; --order name-bytes gives the same lines at concurrency 1, 4 and 32.
#
# Env: .claude/skills/e2e-s3/.env (S3_HOST, S3_AK, S3_SK). Exit status 0 only when every check
# passed and the bucket is gone.
set -u
ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
cd "$ROOT"
set -a; . .claude/skills/e2e-s3/.env; set +a
RUN=${RUN:-$(date +%s)}
[[ $RUN =~ ^[a-z0-9-]{1,30}$ ]] || { echo "RUN must match [a-z0-9-]{1,30}: $RUN" >&2; exit 1; }
VB=data-mover-c22-$RUN
SCHEME=http; [ "${S3_USE_HTTPS:-}" = true ] && SCHEME=https
H="$SCHEME://$S3_HOST"
# The credentials reach curl through a config on a file descriptor, never on its command line.
esc() { local v=${1//\\/\\\\}; printf '%s' "${v//\"/\\\"}"; }
s3c() { curl -s --aws-sigv4 "aws:amz:us-east-1:s3" -K <(printf 'user = "%s:%s"\n' "$(esc "$S3_AK")" "$(esc "$S3_SK")") "$@"; }
enc() { python3 -c 'import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1], safe=sys.argv[2]))' "$1" "${2:-}"; }
WORK=/tmp/data-mover-c22-$RUN
mkdir -p "$WORK"
CREATED=0
FAILED=0

# key<TAB>versionId of every entry of the bucket.
entries() {
  local km="" vm="" out
  while :; do
    out=$(s3c "$H/$VB?versions&key-marker=$(enc "$km")&version-id-marker=$(enc "$vm")")
    # On stdin: a 1 000-entry page is too long for one argument.
    python3 -c 'import sys, xml.etree.ElementTree as ET
root = ET.fromstring(sys.stdin.read()); ns = root.tag[:root.tag.index("}") + 1]
for tag in ("Version", "DeleteMarker"):
    for e in root.iter(ns + tag):
        print("%s\t%s" % (e.findtext(ns + "Key"), e.findtext(ns + "VersionId")))' <<<"$out"
    grep -q '<IsTruncated>true' <<<"$out" || break
    km=$(grep -o '<NextKeyMarker>[^<]*' <<<"$out" | sed 's/<NextKeyMarker>//')
    vm=$(grep -o '<NextVersionIdMarker>[^<]*' <<<"$out" | sed 's/<NextVersionIdMarker>//')
  done
}
cleanup() {
  [ "$CREATED" = 1 ] || { rm -rf "$WORK"; return; }
  s3c "$H/$VB?uploads" | grep -o '<Key>[^<]*</Key><UploadId>[^<]*' | sed 's/<Key>//; s/<\/Key><UploadId>/\t/' |
    while IFS=$'\t' read -r k id; do s3c -o /dev/null -X DELETE "$H/$VB/$(enc "$k" /)?uploadId=$(enc "$id")"; done
  for _ in 1 2 3; do
    entries | while IFS=$'\t' read -r k v; do
      s3c -o /dev/null -X DELETE "$H/$VB/$(enc "$k" /)?versionId=$(enc "$v")"
    done
  done
  local code; code=$(s3c -o /dev/null -w '%{http_code}' -X DELETE "$H/$VB")
  if s3c "$H/" | grep -q "<Name>$VB</Name>"; then echo "cleanup: $VB STILL PRESENT (delete=$code)"; FAILED=1
  else echo "cleanup: $VB deleted"; fi
  echo "buckets now: $(s3c "$H/" | grep -o '<Name>[^<]*' | sed 's/<Name>//' | tr '\n' ' ')"
  rm -rf "$WORK"
}
# The script's own exit status (a failed build, a bucket this run did not create) survives the
# cleanup; a clean exit still fails when a check or the cleanup did.
trap 'rc=$?; cleanup; exit $(( rc || FAILED ))' EXIT

put() { # key, body
  printf '%s' "$2" >"$WORK/body"
  local code; code=$(s3c -o /dev/null -w '%{http_code}' -X PUT "$H/$VB/$(enc "$1" /)" --data-binary @"$WORK/body")
  [ "$code" = 200 ] || { echo "PUT $1: $code" >&2; FAILED=1; }
}
del() { s3c -o /dev/null -X DELETE "$H/$VB/$(enc "$1" /)"; }
versioning() {
  printf '<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>%s</Status></VersioningConfiguration>' "$1" >"$WORK/v.xml"
  s3c -o /dev/null -w "versioning $1: %{http_code}\n" -X PUT "$H/$VB?versioning" --data-binary @"$WORK/v.xml"
}

build=$(cargo build -q --example storage_role_operations 2>&1) || { echo "$build" >&2; exit 1; }
code=$(s3c -o /dev/null -w '%{http_code}' -X PUT "$H/$VB")
echo "create $VB: $code"
[ "$code" = 200 ] || { echo "bucket $VB was not created by this run; stopping" >&2; exit 1; }
CREATED=1
versioning Enabled
put k v1; put k 'v2!'; del k; put k 'v3!!'
put gone g; del gone
put .data-mover-x a; put d/.data-mover-stage/y a; put d/ ''; put d/e e; put sub/deep/f f
for i in $(seq -w 0 998); do put "p/a$i" x; done
put p/z 1; put p/z 22; put p/z 333
versioning Suspended
put n first; put n second
echo "server entries: $(entries | wc -l)"

# Raw, as examples/endpoint_support builds it: the URL parser splits the key pair at the first
# ':' and the host at the last '@' and does not percent-decode. Only in this script's environment.
export S3_LISTING_URL="s3://$S3_AK:$S3_SK@$VB.$S3_HOST/"
BIN=target/debug/examples/storage_role_operations
traverse() { # concurrency, args...
  "$BIN" --backend s3 --concurrency "$1" traverse "${@:2}" 2>"$WORK/stderr" \
    || { echo "traverse $* failed:" >&2; cat "$WORK/stderr" >&2; return 1; }
}
traverse 8 --versions all >"$WORK/all" || FAILED=1
traverse 8 --versions current >"$WORK/current" || FAILED=1
for c in 1 4 32; do traverse "$c" --versions all --order name-bytes >"$WORK/sorted-$c" || FAILED=1; done
echo "-- all (without p/a*)"; grep -v ' p/a' "$WORK/all"
echo "-- current (without p/a*)"; grep -v ' p/a' "$WORK/current"
cmp -s "$WORK/sorted-1" "$WORK/sorted-4" && cmp -s "$WORK/sorted-1" "$WORK/sorted-32" \
  && echo "name-bytes: identical at concurrency 1, 4, 32 ($(wc -l <"$WORK/sorted-1") lines)" \
  || { echo "name-bytes: output differs between concurrency levels"; FAILED=1; }
python3 - "$WORK/all" "$WORK/current" <<'PY' || FAILED=1
import re, sys
def rows(path, name):
    out = []
    for line in open(path):
        m = re.match(r"File (\S+)(?: version=(\S+) latest=(\d) marker=(\d) size=(\S+))?$", line.strip())
        if m and m.group(1) == name:
            out.append(m.groups()[1:])
    return out
all_, cur = sys.argv[1], sys.argv[2]
checks = {
    "all k": ([(r[1], r[2], r[3]) for r in rows(all_, "k")], [("0", "0", "2"), ("0", "0", "3"), ("0", "1", "-"), ("1", "0", "4")]),
    "all gone": ([(r[1], r[2]) for r in rows(all_, "gone")], [("0", "0"), ("1", "1")]),
    "all p/z": ([(r[1], r[3]) for r in rows(all_, "p/z")], [("0", "1"), ("0", "2"), ("1", "3")]),
    "all n": ([(r[0], r[1]) for r in rows(all_, "n")], [("null", "1")]),
    "all p/a count": (len([l for l in open(all_) if " p/a" in l]), 999),
    "current k": (len(rows(cur, "k")), 1),
    "current gone": (len(rows(cur, "gone")), 0),
    "current p/a count": (len([l for l in open(cur) if " p/a" in l]), 999),
    "no artifacts": (sum(".data-mover" in l for f in (all_, cur) for l in open(f)), 0),
    "all d/e": (len(rows(all_, "d/e")), 1),
    "current d/e": (len(rows(cur, "d/e")), 1),
    "no d/ marker": (sum(re.search(r" d/( |$)", l) is not None for f in (all_, cur) for l in open(f)), 0),
    "sub/deep/f": (len(rows(all_, "sub/deep/f")) + len(rows(cur, "sub/deep/f")), 2),
    "listed dirs": (sum(l.startswith("listed ") for l in open(all_)), 5),
    "completed": (sum("outcome=Completed" in l for f in (all_, cur) for l in open(f)), 2),
}
bad = 0
for name, (got, want) in checks.items():
    ok = got == want
    bad += not ok
    print("%-18s %s%s" % (name, "ok" if ok else "FAIL", "" if ok else " got=%r want=%r" % (got, want)))
sys.exit(1 if bad else 0)
PY
[ "$FAILED" = 0 ] && echo "RESULT: pass" || echo "RESULT: FAIL"
