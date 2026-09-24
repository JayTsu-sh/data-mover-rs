#!/usr/bin/env bash
# Role-based S3 write-strategy matrix against a real server (examples/s3_staged_copy.rs).
# Prints one line per case plus what it left behind (stage objects, incomplete multipart uploads),
# then removes everything under its own prefix. Not an assertion suite: it records a baseline to
# compare write strategies across commits.
#
# Env: .claude/skills/e2e-s3/.env (S3_HOST, S3_BUCKET, S3_AK, S3_SK). Only keys under
# staged-<run>/ are written or deleted.
set -u
ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
cd "$ROOT"
set -a; . .claude/skills/e2e-s3/.env; set +a
RUN=${RUN:-$(date +%s)}
PREFIX=staged-$RUN
WORK=/tmp/data-mover-s3-staged-$RUN
export DATA_MOVER_RECOVERY_DIR=$WORK/recovery
mkdir -p "$WORK/src" "$DATA_MOVER_RECOVERY_DIR"
BIN=target/debug/examples/s3_staged_copy
cargo build -q --example s3_staged_copy 2>&1 | grep -v -e binrw -e future-incompat -e '^note'
B="http://$S3_HOST/$S3_BUCKET"
SIG=(--aws-sigv4 "aws:amz:us-east-1:s3" --user "$S3_AK:$S3_SK")

stage_objects() { curl -s "${SIG[@]}" "$B?list-type=2&prefix=$PREFIX/" | grep -o '<Key>[^<]*</Key>' | grep -c '\.data-mover-stage/'; }
# MinIO 2023 lists multipart uploads only for an exact key, so a prefix count is 0 there whatever
# exists; the interrupt cases below read the exact key and upload id from the recovery record.
open_uploads() { curl -s "${SIG[@]}" "$B?uploads&prefix=$PREFIX/" | grep -c '<UploadId>'; }
# The S3 recovery identity inside a record is the stage token `<key>\0<upload id>`; reads it exactly
# (a `strings` scrape of the binary record can pick up the wrong run of printable bytes).
record_token() { # record -> "key upload_id"
  python3 - "$1" <<'PY'
import re, sys
data = open(sys.argv[1], 'rb').read()
m = re.search(rb'(\.data-mover-stage/[0-9a-f]{64}/[0-9a-f]{64})\x00([A-Za-z0-9+/=_.-]+)', data)
print(f"{m.group(1).decode()} {m.group(2).decode()}" if m else "")
PY
}
# Parts of the upload a recovery record points at: "<parts>" or "gone" (NoSuchUpload), "none" (no record).
recorded_upload() {
  local record; record=$(ls "$DATA_MOVER_RECOVERY_DIR"/*.state 2>/dev/null | head -1)
  [ -z "$record" ] && { echo none; return; }
  local key id; read -r key id <<<"$(record_token "$record")"
  [ -z "$key" ] && { echo unparsed; return; }
  local out; out=$(curl -s "${SIG[@]}" "$B/$PREFIX/$key?uploadId=$id")
  if echo "$out" | grep -q NoSuchUpload; then echo gone; else echo "$out" | grep -c '<PartNumber>'; fi
}
# Aborts the upload a leftover recovery record points at (MinIO cannot find it by prefix).
abort_recorded() {
  for record in "$DATA_MOVER_RECOVERY_DIR"/*.state; do
    [ -f "$record" ] || continue
    local key id; read -r key id <<<"$(record_token "$record")"
    [ -n "$key" ] && curl -s -o /dev/null "${SIG[@]}" -X DELETE "$B/$PREFIX/$key?uploadId=$id"
  done
}
case_() { # label, args...
  local label=$1; shift
  local line; line=$("$BIN" "$@" 2>&1 | tail -1)
  printf '%-44s %s  | stage=%s uploads=%s\n' "$label" "$line" "$(stage_objects)" "$(open_uploads)"
}

SIZES=("z0:0" "k1:1024" "m8:8388608" "m8p1:8388609" "m200:209715200")
# RESUME_ONLY=1 runs only the interrupt-then-resume cases.
RESUME_ONLY=${RESUME_ONLY:-0}
echo "run=$RUN prefix=$PREFIX"
for entry in "${SIZES[@]}"; do
  name=${entry%%:*}; bytes=${entry#*:}
  "$BIN" --source "$WORK/src" --source-path "$name" --seed-bytes "$bytes" --destination "$WORK/seed-probe" \
    --destination-path "$name" --policy atomic --read-back off --identity "seed-$RUN-$name" >/dev/null 2>&1
done
[ "$RESUME_ONLY" = 1 ] || for policy in checkpointed atomic direct; do
  for rb in on off; do
    for entry in "${SIZES[@]}"; do
      name=${entry%%:*}
      case_ "$policy rb=$rb $name" --source "$WORK/src" --source-path "$name" \
        --destination "s3:$PREFIX" --destination-path "$policy-$rb-$name" \
        --policy "$policy" --read-back "$rb" --identity "m-$RUN-$policy-$rb-$name"
    done
  done
done

echo "-- native S3 -> S3"
[ "$RESUME_ONLY" = 1 ] || for name in k1 m200; do
  case_ "native $name" --source "s3:$PREFIX" --source-path "atomic-off-$name" \
    --destination "s3:$PREFIX" --destination-path "native-$name" --policy checkpointed \
    --identity "n-$RUN-$name"
done

echo "-- interrupt (cancel) then resume, checkpointed m200 at 20 MiB/s"
rm -f "$DATA_MOVER_RECOVERY_DIR"/*.state
case_ "cancel after 3 s" --source "$WORK/src" --source-path m200 --destination "s3:$PREFIX" \
  --destination-path resume-cancel --policy checkpointed --identity "r-$RUN-cancel" \
  --bandwidth 20971520 --cancel-after-ms 3000
echo "   recorded upload after cancel: parts=$(recorded_upload)"
case_ "resume (same identity)" --source "$WORK/src" --source-path m200 --destination "s3:$PREFIX" \
  --destination-path resume-cancel --policy checkpointed --identity "r-$RUN-cancel"
echo "   recorded upload after resume: parts=$(recorded_upload)"
abort_recorded; rm -f "$DATA_MOVER_RECOVERY_DIR"/*.state

echo "-- interrupt (SIGKILL) then resume"
timeout -s KILL 3 "$BIN" --source "$WORK/src" --source-path m200 --destination "s3:$PREFIX" \
  --destination-path resume-kill --policy checkpointed --identity "r-$RUN-kill" \
  --bandwidth 20971520 >/dev/null 2>&1
printf '%-44s %s  | stage=%s uploads=%s\n' "killed after 3 s" "exit=$?" "$(stage_objects)" "$(open_uploads)"
echo "   recorded upload after kill: parts=$(recorded_upload)"
case_ "resume (same identity)" --source "$WORK/src" --source-path m200 --destination "s3:$PREFIX" \
  --destination-path resume-kill --policy checkpointed --identity "r-$RUN-kill"

echo "   recorded upload after resume: parts=$(recorded_upload)"
abort_recorded

echo "-- cleanup of $PREFIX/"
curl -s "${SIG[@]}" "$B?uploads&prefix=$PREFIX/" | sed 's/<Upload>/\n<Upload>/g' | grep '^<Upload>' | while read -r u; do
  k=$(echo "$u" | grep -o '<Key>[^<]*' | sed 's/<Key>//'); id=$(echo "$u" | grep -o '<UploadId>[^<]*' | sed 's/<UploadId>//')
  curl -s -o /dev/null "${SIG[@]}" -X DELETE "$B/$k?uploadId=$id"; done
while :; do
  keys=$(curl -s "${SIG[@]}" "$B?list-type=2&prefix=$PREFIX/" | grep -o '<Key>[^<]*</Key>' | sed 's/<[^>]*>//g')
  [ -z "$keys" ] && break
  echo "$keys" | while read -r k; do curl -s -o /dev/null "${SIG[@]}" -X DELETE "$B/$k"; done
done
echo "left: objects=$(curl -s "${SIG[@]}" "$B?list-type=2&prefix=$PREFIX/" | grep -c '<Key>') uploads=$(open_uploads)"
rm -rf "$WORK"
