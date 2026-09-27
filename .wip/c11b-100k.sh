#!/bin/bash
# c11b-100k.sh: MinIO temp versioned bucket; one key with N versions (default 100000) plus a mixed
# history; RSS of the traversal (new vs old --versions all), --current == old --versions current;
# then deletes every version and the bucket. Never prints credentials.
set -u
SP=/tmp/claude-1000/-home-keyee-Projects-data-mover-rs/1a33a43a-894b-4998-9b23-2feeca5f1173/scratchpad
cd $SP/c11b-run
set -a; . .claude/skills/e2e-s3/.env; set +a
. .claude/skills/_shared/s3_url.sh
N=${N:-100000}
VB=${VB:-data-mover-c11b-$(date +%s)}
H="http://$S3_HOST"
W=$SP/c11b-100k; mkdir -p $W; chmod 700 $W
esc() { local v=${1//\\/\\\\}; printf '%s' "${v//\"/\\\"}"; }
printf 'user = "%s:%s"\n' "$(esc "$S3_AK")" "$(esc "$S3_SK")" > $W/curl.cfg; chmod 600 $W/curl.cfg
s3c() { curl -s --aws-sigv4 "aws:amz:us-east-1:s3" -K $W/curl.cfg "$@"; }
export -f s3c; export W H VB
cleanup() {
  echo "== cleanup $VB"
  local km="" vm="" out n=0
  while :; do
    s3c "$H/$VB?versions&max-keys=1000" > $W/list.xml
    python3 - "$W/del.xml" "$W/list.xml" <<'PY' || break
import sys, xml.etree.ElementTree as ET
r = ET.fromstring(open(sys.argv[2]).read()); ns = r.tag[:r.tag.index("}")+1] if r.tag.startswith("{") else ""
objs = [(e.findtext(ns+"Key"), e.findtext(ns+"VersionId")) for t in ("Version","DeleteMarker") for e in r.iter(ns+t)]
if not objs: sys.exit(1)
from xml.sax.saxutils import escape
open(sys.argv[1],"w").write("<Delete><Quiet>true</Quiet>" + "".join("<Object><Key>%s</Key><VersionId>%s</VersionId></Object>" % (escape(k), escape(v)) for k,v in objs) + "</Delete>")
PY
    md5=$(python3 -c "import hashlib,base64,sys; print(base64.b64encode(hashlib.md5(open(sys.argv[1],'rb').read()).digest()).decode())" $W/del.xml)
    s3c -o /dev/null -X POST "$H/$VB?delete" -H "Content-MD5: $md5" --data-binary @$W/del.xml
    n=$((n+1)); [ $n -gt 200 ] && break
  done
  echo "delete bucket: $(s3c -o /dev/null -w '%{http_code}' -X DELETE "$H/$VB")"
  echo "head bucket: $(s3c -o /dev/null -I -w '%{http_code}' "$H/$VB")"
  rm -f $W/curl.cfg
}
trap cleanup EXIT
[ -n "${CLEAN_ONLY:-}" ] && exit 0
if [ -z "${REUSE:-}" ]; then
echo "create $VB: $(s3c -o /dev/null -w '%{http_code}' -X PUT "$H/$VB")"
printf '<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>' > $W/v.xml
echo "versioning: $(s3c -o /dev/null -w '%{http_code}' -X PUT "$H/$VB?versioning" --data-binary @$W/v.xml)"
put() { s3c -o /dev/null -X PUT "$H/$VB/$1" --data-binary "$2"; }
del() { s3c -o /dev/null -X DELETE "$H/$VB/$1"; }
fi
[ -z "${REUSE:-}" ] && { put mixed/k v1; put mixed/k v22; del mixed/k; put mixed/k v333; put mixed/gone g; del mixed/gone; put mixed/plain p; put top t; }
T0=$(date +%s)
seq 1 ${WRITE:-$N} | xargs -P 48 -I{} bash -c 's3c -o /dev/null -X PUT "$H/$VB/big/k" --data-binary x'
echo "wrote ${WRITE:-$N} versions of big/k in $(( $(date +%s) - T0 )) s"
export S3_LISTING_URL=$(s3_url "$VB" "")
NEW=target/debug/examples/storage_role_operations; OLD=$SP/c11b-bin/sro-old
for o in blocks unordered; do
  /usr/bin/time -f "new $o maxrss_kb=%M wall=%e" $NEW --backend s3 --concurrency 8 traverse --order $o --quiet > $W/new-$o.quiet 2> $W/new-$o.time; tail -1 $W/new-$o.time; grep -o 'entries=[0-9]* failures=[0-9]*' $W/new-$o.quiet
  /usr/bin/time -f "old $o --versions all maxrss_kb=%M wall=%e" $OLD --backend s3 --concurrency 8 traverse --order $o --versions all --quiet > $W/old-$o.quiet 2> $W/old-$o.time; tail -1 $W/old-$o.time; grep -o 'entries=[0-9]* failures=[0-9]*' $W/old-$o.quiet
done
$NEW --backend s3 --concurrency 8 traverse --order blocks > $W/new.out 2>$W/new.err
grep -E '^File ' $W/new.out; echo "Record lines: $(grep -c '^Record ' $W/new.out) (big/k: $(grep -c '^Record big/k ' $W/new.out))"
grep '^totals' $W/new.out
# --current equals the old --versions current, entry lines sorted
$NEW --backend s3 --concurrency 8 traverse --order blocks --current --sizes --identity > $W/cur-new 2>/dev/null
$OLD --backend s3 --concurrency 8 traverse --order blocks --versions current --sizes --identity > $W/cur-old 2>/dev/null
if cmp -s <(grep -E '^(File|Directory) ' $W/cur-new | sort) <(grep -E '^(File|Directory) ' $W/cur-old | sort); then
  echo "--current == old --versions current: yes ($(grep -cE '^(File|Directory) ' $W/cur-new) lines)"
else echo "--current == old --versions current: NO"; diff <(grep -E '^(File|Directory) ' $W/cur-new | sort) <(grep -E '^(File|Directory) ' $W/cur-old | sort) | head; fi
