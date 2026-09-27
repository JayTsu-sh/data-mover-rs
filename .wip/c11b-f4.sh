#!/bin/bash
# c11b-f4.sh: ONTAP data-mover-test-ver (existing bucket, unique prefix): p/éb/x and mkl/z with N versions
# (default 1000); role traverse must give one entry mkl/z with N records in the server's order (raw
# ListObjectVersions reversed). Deletes every version under the prefix afterwards. No credentials printed.
set -u
SP=/tmp/claude-1000/-home-keyee-Projects-data-mover-rs/1a33a43a-894b-4998-9b23-2feeca5f1173/scratchpad
cd $SP/c11b-run
set -a; . .claude/skills/e2e-s3/ontap/.env; set +a
. .claude/skills/_shared/s3_url.sh
N=${N:-1000}; VB=data-mover-test-ver; P=dm-f4-$(date +%s)/
H="http://$S3_HOST"; W=$SP/c11b-f4; mkdir -p $W; chmod 700 $W
esc() { local v=${1//\\/\\\\}; printf '%s' "${v//\"/\\\"}"; }
printf 'user = "%s:%s"\n' "$(esc "$S3_AK")" "$(esc "$S3_SK")" > $W/curl.cfg; chmod 600 $W/curl.cfg
s3c() { curl -s --aws-sigv4 "aws:amz:us-east-1:s3" -K $W/curl.cfg "$@"; }
enc() { python3 -c 'import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1], safe=sys.argv[2]))' "$1" "${2:-}"; }
export -f s3c; export W H VB P
raw() { # key<TAB>version of every entry under the prefix, newest first per key, all pages
  local km="" vm="" out
  : > $W/raw.txt
  while :; do
    s3c "$H/$VB?versions&prefix=$(enc "$P")${km:+&key-marker=$(enc "$km")}${vm:+&version-id-marker=$(enc "$vm")}" > $W/page.xml
    python3 - $W/page.xml >> $W/raw.txt <<'PY'
import sys, xml.etree.ElementTree as ET
r = ET.fromstring(open(sys.argv[1]).read()); ns = r.tag[:r.tag.index("}")+1] if r.tag.startswith("{") else ""
for e in r:
    if e.tag in (ns+"Version", ns+"DeleteMarker"): print(e.findtext(ns+"Key")+"\t"+e.findtext(ns+"VersionId"))
PY
    grep -q '<IsTruncated>true' $W/page.xml || break
    km=$(grep -o '<NextKeyMarker>[^<]*' $W/page.xml | sed 's/<NextKeyMarker>//'); vm=$(grep -o '<NextVersionIdMarker>[^<]*' $W/page.xml | sed 's/<NextVersionIdMarker>//')
    [ -n "$km" ] || break
  done
}
cleanup() {
  raw
  while IFS=$'\t' read -r k v; do [[ $k == "$P"* ]] && s3c -o /dev/null -X DELETE "$H/$VB/$(enc "$k" /)?versionId=$(enc "$v")"; done < $W/raw.txt
  raw; echo "cleanup: $(grep -c . $W/raw.txt) entries left under $P"; rm -f $W/curl.cfg
}
trap cleanup EXIT
echo "prefix $P"
s3c -o /dev/null -w "put p/éb/x %{http_code}\n" -X PUT "$H/$VB/$(enc "${P}p/éb/x" /)" --data-binary x
seq 1 $N | xargs -P 16 -I{} bash -c 's3c -o /dev/null -X PUT "$H/$VB/${P}mkl/z" --data-binary {}'
raw; echo "raw entries: $(grep -c . $W/raw.txt)"
export S3_LISTING_URL=$(s3_url "$VB" "$P")
target/debug/examples/storage_role_operations --backend s3 --concurrency 8 traverse --order blocks > $W/out 2> $W/err
grep -E '^File ' $W/out; grep '^totals' $W/out | cut -c1-200; grep -c . $W/err
grep -E '^Record mkl/z ' $W/out | sed -E 's/.* version=(\S+) .*/\1/' > $W/role.ids
grep -P "^${P}mkl/z\t" $W/raw.txt | cut -f2 | tac > $W/raw.ids
cmp -s $W/role.ids $W/raw.ids && echo "mkl/z: $(wc -l < $W/role.ids) records, the server's order reversed: yes" || echo "mkl/z order: DIFFERS"
grep -q 'order=inferred' $W/out && echo "inferred: yes (unexpected)" || echo "inferred: none"
