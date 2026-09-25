#!/usr/bin/env bash
# Role-based S3 write-strategy matrix against a real server (examples/transfer_resume.rs).
# Prints one line per case plus what it left behind (`.data-mover-*` objects, local recovery
# records, uploads open on the case's key), then removes everything under its own prefix. Not an
# assertion suite: it records a baseline to compare write strategies across commits — except the Direct rows, which since ADR-0006 C14c must
# succeed: each is downloaded and compared with its source, and the uploads left on its exact key
# are counted (`direct: equal=yes key_uploads=0` is the expected verdict) — and the native rows,
# which since C18 write the final key: `native: equal=yes stage_objects=0 key_uploads=0`.
#
# Env: .claude/skills/e2e-s3/.env (S3_HOST, S3_BUCKET, S3_AK, S3_SK); PREFIX overrides the key
# prefix (default staged-<run>). Only keys under that prefix are written or deleted.
set -u
ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
cd "$ROOT"
set -a; . .claude/skills/e2e-s3/.env; set +a
# `a-bucket` holds Milvus data on the lab MinIO; never write there.
[ "${S3_BUCKET:-}" = a-bucket ] && { echo "refusing to write to bucket a-bucket" >&2; exit 1; }
RUN=${RUN:-$(date +%s)}
# The run's prefix: its cleanup deletes every object and upload under it, so it must look like one
# of ours (an inherited generic PREFIX must never be taken).
PREFIX=${S3_MATRIX_PREFIX:-staged-$RUN}
case "$PREFIX" in
  staged-*|data-mover-*) ;;
  *) echo "S3_MATRIX_PREFIX must start with staged- or data-mover-: $PREFIX" >&2; exit 1 ;;
esac
WORK=/tmp/data-mover-s3-staged-$RUN
# Only to count records: since ADR-0006 C15c an S3 destination keeps its recovery state at the
# destination and writes none here.
export DATA_MOVER_RECOVERY_DIR=$WORK/recovery
mkdir -p "$WORK/src" "$DATA_MOVER_RECOVERY_DIR"
BIN=target/debug/examples/transfer_resume
build=$(cargo build -q --example transfer_resume 2>&1) || { echo "$build" >&2; exit 1; }
SCHEME=http; [ "${S3_USE_HTTPS:-}" = true ] && SCHEME=https
B="$SCHEME://$S3_HOST/$S3_BUCKET"
# The credentials reach curl through a config on a file descriptor, never on its command line.
esc() { local v=${1//\\/\\\\}; printf '%s' "${v//\"/\\\"}"; }
s3c() { curl -s --aws-sigv4 "aws:amz:us-east-1:s3" -K <(printf 'user = "%s:%s"\n' "$(esc "$S3_AK")" "$(esc "$S3_SK")") "$@"; }

# `.data-mover-*` objects under the run's prefix: the `.upload` pointers of uploads on the final key
# (ADR-0006 C15b). Native copies write the final key since C18: no `.data-mover-stage/` object.
artifacts() { s3c "$B?list-type=2&prefix=$PREFIX/" | grep -o '<Key>[^<]*</Key>' | grep -c '\.data-mover-'; }
# Local recovery records: since ADR-0006 C15c an S3 destination keeps none (always 0).
records() { find "$DATA_MOVER_RECOVERY_DIR" -name '*.state' 2>/dev/null | wc -l; }
# The multipart uploads open on exactly key $1 (under the prefix), one id per line:
# ListMultipartUploads with the key as prefix, then an exact-key filter (MinIO 2023 lists exact
# keys only, AWS / Ceph / StorageGRID by prefix). A reply that is not a listing prints "?".
key_upload_ids() {
  s3c "$B?uploads&prefix=$PREFIX/$1" | python3 -c 'import sys, xml.etree.ElementTree as ET
try: root = ET.fromstring(sys.stdin.read())
except ET.ParseError: print("?"); sys.exit()
ns = root.tag[:root.tag.index("}") + 1] if root.tag.startswith("{") else ""
if root.tag != ns + "ListMultipartUploadsResult": print("?"); sys.exit()
for upload in root.iter(ns + "Upload"):
    if upload.findtext(ns + "Key") == sys.argv[1]: print(upload.findtext(ns + "UploadId"))' "$PREFIX/$1"
}
# The count, or "?" when the listing failed.
key_uploads() {
  local ids; ids=$(key_upload_ids "$1")
  if grep -qx '?' <<<"$ids"; then echo '?'; else grep -c . <<<"$ids"; fi
}
# Every destination key the run wrote: the cleanup aborts the uploads open on each.
KEYS=()
remember_key() { [[ " ${KEYS[*]} " == *" $1 "* ]] || KEYS+=("$1"); }
case_() { # label, destination key, args...
  local label=$1 key=$2; shift 2
  remember_key "$key"
  local line; line=$("$BIN" "$@" 2>&1 | tail -1)
  printf '%-44s %s  | artifacts=%s records=%s key_uploads=%s\n' "$label" "$line" "$(artifacts)" \
    "$(records)" "$(key_uploads "$key")"
}
# A native copy writes the final key (ADR-0006 C18): it must equal its source, and neither a temp
# key under `.data-mover-stage/` (the path C19 removed; `stage_objects` guards that none is ever
# written again) nor an upload on the key may be left.
native_verdict() { # destination key, source file
  local equal=no stage
  s3c -o "$WORK/check" "$B/$PREFIX/$1" && cmp -s "$WORK/check" "$2" && equal=yes
  rm -f "$WORK/check"
  stage=$(s3c "$B?list-type=2&prefix=$PREFIX/" | grep -o '<Key>[^<]*</Key>' | grep -c '\.data-mover-stage/')
  echo "   native: equal=$equal stage_objects=$stage key_uploads=$(key_uploads "$1")"
}
# Direct writes the final key itself: the object must equal its source and leave no upload on
# that exact key.
direct_verdict() { # destination key, source file
  local equal=no
  s3c -o "$WORK/check" "$B/$PREFIX/$1" && cmp -s "$WORK/check" "$2" && equal=yes
  rm -f "$WORK/check"
  echo "   direct: equal=$equal key_uploads=$(key_uploads "$1")"
}
# What an interrupted checkpointed upload left at the destination: its pointer and the uploads on
# its key (ADR-0006 C15c/C16: one pointer + one open upload after a cut that was not discarded).
left_at() { # destination key
  local pointers
  pointers=$(s3c "$B?list-type=2&prefix=$PREFIX/" | grep -o '<Key>[^<]*</Key>' | grep -c '\.upload<')
  echo "   left at the destination: prefix pointers=$pointers key_uploads=$(key_uploads "$1") records=$(records)"
}

SIZES=("z0:0" "k1:1024" "m8:8388608" "m8p1:8388609" "m20:20971520" "m200:209715200")
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
      case_ "$policy rb=$rb $name" "$policy-$rb-$name" --source "$WORK/src" --source-path "$name" \
        --destination "s3:$PREFIX" --destination-path "$policy-$rb-$name" \
        --policy "$policy" --read-back "$rb" --identity "m-$RUN-$policy-$rb-$name"
      [ "$policy" = direct ] && direct_verdict "$policy-$rb-$name" "$WORK/src/$name"
    done
  done
done

echo "-- native S3 -> S3"
[ "$RESUME_ONLY" = 1 ] || for name in k1 m200; do
  case_ "native $name" "native-$name" --source "s3:$PREFIX" --source-path "atomic-off-$name" \
    --destination "s3:$PREFIX" --destination-path "native-$name" --policy checkpointed \
    --identity "n-$RUN-$name"
  native_verdict "native-$name" "$WORK/src/$name"
done

# The cut lands after the first 64 MiB checkpoint (the parts the service acknowledged, up to four
# 8 MiB parts behind the reads): 12 s at 20 MiB/s, so the cancelled stage is recoverable. An earlier
# SIGKILL resumes too (the pointer is written when the upload begins, ADR-0006 C16).
echo "-- interrupt (cancel) then resume, checkpointed m200 at 20 MiB/s"
case_ "cancel after 12 s" resume-cancel --source "$WORK/src" --source-path m200 --destination "s3:$PREFIX" \
  --destination-path resume-cancel --policy checkpointed --identity "r-$RUN-cancel" \
  --bandwidth 20971520 --cancel-after-ms 12000
left_at resume-cancel
case_ "resume (same identity)" resume-cancel --source "$WORK/src" --source-path m200 --destination "s3:$PREFIX" \
  --destination-path resume-cancel --policy checkpointed --identity "r-$RUN-cancel"
left_at resume-cancel

echo "-- interrupt (SIGKILL) then resume"
remember_key resume-kill
timeout -s KILL 12 "$BIN" --source "$WORK/src" --source-path m200 --destination "s3:$PREFIX" \
  --destination-path resume-kill --policy checkpointed --identity "r-$RUN-kill" \
  --bandwidth 20971520 >/dev/null 2>&1
printf '%-44s %s  | artifacts=%s records=%s key_uploads=%s\n' "killed after 12 s" "exit=$?" "$(artifacts)" \
  "$(records)" "$(key_uploads resume-kill)"
left_at resume-kill
case_ "resume (same identity)" resume-kill --source "$WORK/src" --source-path m200 --destination "s3:$PREFIX" \
  --destination-path resume-kill --policy checkpointed --identity "r-$RUN-kill"
left_at resume-kill

echo "-- cleanup of $PREFIX/"
# Uploads on every key the run wrote (MinIO finds them by exact key only), then any a prefix
# listing still shows (stores that list by prefix), then every object.
for key in "${KEYS[@]}"; do
  key_upload_ids "$key" | grep -v '^?$' | while read -r id; do
    s3c -o /dev/null -X DELETE "$B/$PREFIX/$key?uploadId=$id"; done
done
s3c "$B?uploads&prefix=$PREFIX/" | sed 's/<Upload>/\n<Upload>/g' | grep '^<Upload>' | while read -r u; do
  k=$(echo "$u" | grep -o '<Key>[^<]*' | sed 's/<Key>//'); id=$(echo "$u" | grep -o '<UploadId>[^<]*' | sed 's/<UploadId>//')
  s3c -o /dev/null -X DELETE "$B/$k?uploadId=$id"; done
while :; do
  keys=$(s3c "$B?list-type=2&prefix=$PREFIX/" | grep -o '<Key>[^<]*</Key>' | sed 's/<[^>]*>//g')
  [ -z "$keys" ] && break
  echo "$keys" | while read -r k; do s3c -o /dev/null -X DELETE "$B/$k"; done
done
open=0
for key in "${KEYS[@]}"; do
  n=$(key_uploads "$key"); if [ "$n" = '?' ]; then open='?'; break; fi; open=$((open + n))
done
echo "left: objects=$(s3c "$B?list-type=2&prefix=$PREFIX/" | grep -c '<Key>') key_uploads=$open"
rm -rf "$WORK"
