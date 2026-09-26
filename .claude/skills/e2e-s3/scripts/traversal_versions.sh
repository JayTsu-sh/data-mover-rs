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
# Also: whether the server honours `delimiter` on ListObjectVersions (raw request, printed), and the
# legacy walkdir / walkdir_2 (examples/s3_listing, versioning re-enabled first: a suspended bucket
# is walked by ListObjectsV2) — k as v1, v2, marker, v3 (ids as written), gone as its version then a
# latest marker (D4), p/z whole across the page, no artifact.
#
# Env: S3_ENV (default .claude/skills/e2e-s3/.env; e.g. .claude/skills/e2e-s3/dxn/.env) with S3_HOST,
# S3_AK, S3_SK (S3_COMPAT / S3_USE_HTTPS pick the URL scheme). BUCKET_PREFIX names the bucket (default
# data-mover-c22 → <prefix>-<run>); S3_LOCATION, when set, is the CreateBucket LocationConstraint
# (DXN). Exit status 0 only when every check passed and the bucket is gone.
set -u
ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
cd "$ROOT"
S3_ENV=${S3_ENV:-.claude/skills/e2e-s3/.env}
set -a; . "$S3_ENV"; set +a
RUN=${RUN:-$(date +%s)}
[[ $RUN =~ ^[a-z0-9-]{1,30}$ ]] || { echo "RUN must match [a-z0-9-]{1,30}: $RUN" >&2; exit 1; }
BUCKET_PREFIX=${BUCKET_PREFIX:-data-mover-c22}
[[ $BUCKET_PREFIX =~ ^data-mover-[a-z0-9-]{1,20}$ ]] || { echo "BUCKET_PREFIX must be data-mover-<[a-z0-9-]>: $BUCKET_PREFIX" >&2; exit 1; }
VB=$BUCKET_PREFIX-$RUN
SCHEME=http; [ "${S3_USE_HTTPS:-}" = true ] && SCHEME=https
H="$SCHEME://$S3_HOST"
# The credentials reach curl through a config on a file descriptor, never on its command line.
esc() { local v=${1//\\/\\\\}; printf '%s' "${v//\"/\\\"}"; }
s3c() { curl -s --aws-sigv4 "aws:amz:us-east-1:s3" -K <(printf 'user = "%s:%s"\n' "$(esc "$S3_AK")" "$(esc "$S3_SK")") "$@"; }
enc() { python3 -c 'import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1], safe=sys.argv[2]))' "$1" "${2:-}"; }
WORK=/tmp/$BUCKET_PREFIX-$RUN
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

# Every write appends key<TAB>x-amz-version-id to $WORK/ids (the order the versions were made).
written() { printf '%s\t%s\n' "$1" "$(tr -d '\r' <"$WORK/headers" | sed -n 's/^x-amz-version-id: *//Ip')" >>"$WORK/ids"; }
put() { # key, body
  printf '%s' "$2" >"$WORK/body"
  local code; code=$(s3c -D "$WORK/headers" -o /dev/null -w '%{http_code}' -X PUT "$H/$VB/$(enc "$1" /)" --data-binary @"$WORK/body")
  [ "$code" = 200 ] || { echo "PUT $1: $code" >&2; FAILED=1; }
  written "$1"
}
del() { s3c -D "$WORK/headers" -o /dev/null -X DELETE "$H/$VB/$(enc "$1" /)"; written "$1"; }
versioning() {
  printf '<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>%s</Status></VersioningConfiguration>' "$1" >"$WORK/v.xml"
  s3c -o /dev/null -w "versioning $1: %{http_code}\n" -X PUT "$H/$VB?versioning" --data-binary @"$WORK/v.xml"
}

build=$(cargo build -q --example storage_role_operations --example s3_listing 2>&1) || { echo "$build" >&2; exit 1; }
LOCATION=()
[ -n "${S3_LOCATION:-}" ] && LOCATION=(--data-binary "<CreateBucketConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><LocationConstraint>$S3_LOCATION</LocationConstraint></CreateBucketConfiguration>")
code=$(s3c -o "$WORK/create.out" -w '%{http_code}' -X PUT "$H/$VB" "${LOCATION[@]}")
echo "create $VB: $code"
[ "$code" = 200 ] || { cat "$WORK/create.out" >&2; echo "bucket $VB was not created by this run; stopping" >&2; exit 1; }
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
case "${S3_COMPAT:-}" in sg) COMPAT=+sg ;; dxn) COMPAT=+dxn ;; *) COMPAT= ;; esac
TLS=; [ "${S3_USE_HTTPS:-}" = true ] && TLS=+https
export S3_LISTING_URL="s3$COMPAT$TLS://$S3_AK:$S3_SK@$VB.$S3_HOST/"
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
    "listings complete": (sum(l.startswith("listed ") and " Complete balanced=true " in l for l in open(all_)), 5),
    "totals balanced": (sum(l.startswith("totals balanced=true ") for f in (all_, cur) for l in open(f)), 2),
    "completed": (sum("outcome=Completed" in l for f in (all_, cur) for l in open(f)), 2),
}
bad = 0
for name, (got, want) in checks.items():
    ok = got == want
    bad += not ok
    print("%-18s %s%s" % (name, "ok" if ok else "FAIL", "" if ok else " got=%r want=%r" % (got, want)))
sys.exit(1 if bad else 0)
PY
echo "-- raw root listings with delimiter=/ (does the server honour it?)"
for q in "versions&delimiter=%2F" "list-type=2&delimiter=%2F"; do
  s3c "$H/$VB?$q" | python3 -c 'import sys, xml.etree.ElementTree as ET
root = ET.fromstring(sys.stdin.read()); ns = root.tag[:root.tag.index("}") + 1] if root.tag.startswith("{") else ""
keys = [e.findtext(ns + "Key") for tag in ("Version", "DeleteMarker", "Contents") for e in root.iter(ns + tag)]
prefixes = [p.findtext(ns + "Prefix") for p in root.iter(ns + "CommonPrefixes")]
deep = sum("/" in k for k in keys)
print("%-24s entries=%d distinct keys=%s keys with a /=%d CommonPrefixes=%s truncated=%s -> delimiter %s" % (
    sys.argv[1], len(keys), sorted(set(keys)) if len(set(keys)) < 10 else len(set(keys)), deep, prefixes,
    root.findtext(ns + "IsTruncated"), "honoured" if prefixes and not deep else "IGNORED"))' "$q"
done

echo "-- legacy walkdir / walkdir_2 (versioning re-enabled: a suspended bucket is walked by ListObjectsV2)"
versioning Enabled
LEG=target/debug/examples/s3_listing
"$LEG" >"$WORK/walk" 2>"$WORK/walk.err" || { echo "walkdir failed:"; tail -5 "$WORK/walk.err"; FAILED=1; }
"$LEG" --walkdir-2 >"$WORK/walk2" 2>"$WORK/walk2.err" || { echo "walkdir_2 failed:"; tail -5 "$WORK/walk2.err"; FAILED=1; }
echo "walkdir (without p/a*):"; grep -v ' p/a' "$WORK/walk" | sed 's/^/   /'
echo "walkdir_2 (without p/a*):"; grep -v ' p/a' "$WORK/walk2" | sed 's/^/   /'
python3 - "$WORK/walk" "$WORK/walk2" "$WORK/ids" <<'PY2' || FAILED=1
import re, sys
def rows(path, kind):
    out = {}
    for line in open(path):
        m = re.match(kind + r" (\S+) (\S+) latest=(\d) marker=(\d) count=(\S+)$", line.strip())
        if m:
            out.setdefault(m.group(1), []).append(m.groups()[1:])
    return out
ids = {}
for line in open(sys.argv[3]):
    k, _, v = line.rstrip("\n").partition("\t")
    ids.setdefault(k, []).append(v)
w, w2 = rows(sys.argv[1], "ENTRY"), rows(sys.argv[2], "FILE")
k, g, z = ids["k"], ids["gone"], ids["p/z"]
checks = {
    "walkdir k": (w.get("k"), [(k[0], "0", "0", "4"), (k[1], "0", "0", "4"), (k[2], "0", "1", "4"), (k[3], "1", "0", "4")]),
    "walkdir gone (D4)": (w.get("gone"), [(g[0], "0", "0", "2"), (g[1], "1", "1", "2")]),
    "walkdir p/z": (w.get("p/z"), [(z[0], "0", "0", "3"), (z[1], "0", "0", "3"), (z[2], "1", "0", "3")]),
    "walkdir n": ([r[1:] for r in w.get("n", [])], [("1", "0", "1")]),
    "walkdir p/a count": (sum(1 for p in w if p.startswith("p/a")), 999),
    "walkdir_2 k": (w2.get("k"), [(k[0], "0", "0", "3"), (k[1], "0", "0", "3"), (k[3], "1", "0", "3")]),
    "walkdir_2 gone (D4)": (w2.get("gone"), [(g[0], "0", "0", "1")]),
    "walkdir_2 p/z": (w2.get("p/z"), [(z[0], "0", "0", "3"), (z[1], "0", "0", "3"), (z[2], "1", "0", "3")]),
    "walkdir_2 p/a count": (sum(1 for p in w2 if p.startswith("p/a")), 999),
    "legacy no artifacts": (sum(".data-mover" in l for f in sys.argv[1:3] for l in open(f)), 0),
    "legacy no errors": (sum(l.startswith(("ERROR", "TIMEOUT")) for f in sys.argv[1:3] for l in open(f)), 0),
}
bad = 0
for name, (got, want) in checks.items():
    ok = got == want
    bad += not ok
    print("%-20s %s%s" % (name, "ok" if ok else "FAIL", "" if ok else " got=%r want=%r" % (got, want)))
sys.exit(1 if bad else 0)
PY2
[ "$FAILED" = 0 ] && echo "RESULT: pass" || echo "RESULT: FAIL"
