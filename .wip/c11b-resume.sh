#!/bin/bash
# c11b-resume.sh <minio|ontapver>: traversal_resume.sh (cancel and kill, both orders) on a versioned S3 bucket
# with every object written twice (histories of two versions) plus deleted keys. MinIO: data-mover-c11b-r-<ts>
# temp bucket (deleted after); ONTAP: data-mover-test-ver under dm-c11-c11b-<ts>/ (emptied after).
set -u
S=/tmp/claude-1000/-home-keyee-Projects-data-mover-rs/1a33a43a-894b-4998-9b23-2feeca5f1173/scratchpad
C=$S/c11b-run; K=$C/.claude/skills; BIN=$C/target/debug/examples/storage_role_operations
TS=$(date +%s); R=dm-c11-c11b-$TS
. $K/_shared/s3_url.sh
unset S3_BUCKET_TYPE S3_COMPAT S3_USE_HTTPS S3_TLS_VERIFY
case $1 in
  minio) set -a; . $K/e2e-s3/.env; set +a; B=data-mover-c11b-r-$TS;;
  ontapver) set -a; . $K/e2e-s3/ontap/.env; set +a; B=data-mover-test-ver;;
esac
H="http://$S3_HOST"; W=$S/c11b-resume/$1-$TS; mkdir -p $W; chmod 700 $W
esc() { local v=${1//\\/\\\\}; printf '%s' "${v//\"/\\\"}"; }
printf 'user = "%s:%s"\n' "$(esc "$S3_AK")" "$(esc "$S3_SK")" > $W/curl.cfg; chmod 600 $W/curl.cfg
s3c() { curl -s --aws-sigv4 "aws:amz:us-east-1:s3" -K $W/curl.cfg "$@"; }
cleanup() {
  python3 $S/c11s3del.py "$B" "$R/" 2>&1 | tail -1
  if [ $1 = minio ]; then echo "delete bucket $B: $(s3c -o /dev/null -w '%{http_code}' -X DELETE "$H/$B")"; fi
  rm -f $W/curl.cfg
}
trap 'cleanup '$1 EXIT
if [ $1 = minio ]; then
  echo "create $B: $(s3c -o /dev/null -w '%{http_code}' -X PUT "$H/$B")"
  printf '<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>' > $W/v.xml
  echo "versioning: $(s3c -o /dev/null -w '%{http_code}' -X PUT "$H/$B?versioning" --data-binary @$W/v.xml)"
fi
export S3_LISTING_URL=$(s3_url "$B" "$R/")
seed() {
  for a in 0 1 2 3 4 5 6 7; do for s in 0 1; do
    timeout 900 $BIN --backend s3 --concurrency 16 seed --path "a$a/s$s" --files 20 --bytes 3 > /dev/null 2>&1 || echo "seed a$a/s$s failed"
  done; done
  timeout 900 $BIN --backend s3 --concurrency 16 seed --path "c/c/c/c/c/c/c/c" --files 5 --bytes 3 > /dev/null 2>&1 || echo "seed chain failed"
  timeout 1800 $BIN --backend s3 --concurrency 32 seed --path "wide" --files 300 --bytes 1 > /dev/null 2>&1 || echo "seed wide failed"
}
seed; seed
for k in a0/s0/file-0.log a1/s1/file-1.log wide/file-0.log wide/file-1.log; do s3c -o /dev/null -X DELETE "$H/$B/$R/$k"; done
for mode in cancel kill; do for o in unordered blocks; do
  echo "--- $1 $mode $o"
  BIN=$BIN BACKEND=s3 ORDER=$o MODE=$mode OUT=$W/$mode-$o CANCEL_AFTER=320 EVERY=0 KILL_AFTER=${KILL_AFTER:-4} \
    timeout 5400 bash $K/_shared/traversal_resume.sh 2>&1 | grep -E 'RESULT|lost|invented|refused'
done; done
echo "full entries: $(grep -c '^File' $W/cancel-blocks/full.entries) records: $(grep -c '^Record' $W/cancel-blocks/full.entries)"
