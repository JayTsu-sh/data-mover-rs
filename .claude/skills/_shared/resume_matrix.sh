#!/usr/bin/env bash
# Container-restart resume matrix (ADR-0006): interrupt a transfer, wipe every piece of local state,
# resume from a fresh process with a fresh HOME, and report what was reused and what was left behind.
# Not an assertion suite: it records how each backend behaves, before and after the migration.
#
# Usage: DEST=<endpoint> bash .claude/skills/_shared/resume_matrix.sh
#   DEST   destination endpoint, as examples/transfer_resume.rs takes it: a local directory,
#          nfs://host/export[:root]?..., smb:<sub-path> (e2e-cifs .env), s3:<prefix> (e2e-s3 .env,
#          prefix required), hdfs://... (LAB_HDFS_* env)
#   SIZE   bytes of the source file (default 200 MiB); BW source bandwidth in bytes/s while the first
#          run is cut (default 20 MiB/s); CUT_MS when the cut lands (default 6000, so the first
#          64 MiB checkpoint is durable at the default BW — except on S3, whose checkpoint counts
#          only the parts the service acknowledged, up to four 8 MiB parts behind the reads: use
#          CUT_MS=12000 there, or the resume reports `Restarted { StageWithoutPointer }`)
#   KEEP_STATE=1  keep the local recovery records across the restart (still a fresh process and
#          HOME). Neither run names the transfer: the resume can only find the record through the
#          identity data-mover derives (ADR-0006 C5), so "streamed" below SIZE proves it did.
# Writes only under <DEST>/resume-<run>/ and removes all of it at the end: final files, orphaned
# stages and checkpoints, and (S3) every multipart upload still open on the run's keys.
#
# Both runs derive the transfer identity from the endpoints and paths (no --identity); each line
# reports whether the resumed run derived the interrupted run's identity and endpoint.
# "prepare" / "reused_bytes" say what the resuming run found at the destination, and the content
# check compares the final file with the source by BLAKE3.
# "streamed" is what the resuming run read from the source (it runs with read-back off and a
# non-limiting budget, so the engine counts it): SIZE means nothing was reused. "records" is how
# many recovery records the interrupted run left locally. For a destination that still uses the
# local recovery store, 0 means the cut came before the first checkpoint; a destination that keeps
# its recovery state beside the final file (ADR-0006: Local from C8, NFS C10, CIFS C11, S3 C15c)
# always shows 0. For S3 "destination artifacts" is the `.data-mover-*` objects (the `.upload`
# pointer) plus the multipart uploads open on the run's final keys.
set -u
ROOT=$(cd "$(dirname "$0")/../../.." && pwd)
cd "$ROOT"
for env in .claude/skills/e2e-s3/.env .claude/skills/e2e-cifs/.env; do
  [ -f "$env" ] && { set -a; . "$env"; set +a; }
done
: "${DEST:?set DEST to the destination endpoint}"
SIZE=${SIZE:-209715200}; BW=${BW:-20971520}; CUT_MS=${CUT_MS:-6000}; KEEP_STATE=${KEEP_STATE:-0}
case "$KEEP_STATE" in 0|1) ;; *) echo "KEEP_STATE must be 0 or 1" >&2; exit 1 ;; esac
UNLIMITED=10737418240
RUN=${RUN:-$(date +%s)}
DIR=resume-$RUN
WORK=/tmp/data-mover-resume-$RUN
BIN=target/debug/examples/transfer_resume
if [[ "$DEST" == s3:* ]]; then
  PREFIX=${DEST#s3:}; PREFIX=${PREFIX#/}; PREFIX=${PREFIX%/}
  [ -n "$PREFIX" ] || { echo "DEST=s3:<prefix> needs a non-empty prefix" >&2; exit 1; }
  # `a-bucket` holds Milvus data on the lab MinIO; never write there.
  [ "${S3_BUCKET:-}" = a-bucket ] && { echo "refusing to write to bucket a-bucket" >&2; exit 1; }
  SCHEME=http; [ "${S3_USE_HTTPS:-}" = true ] && SCHEME=https
  B="$SCHEME://$S3_HOST/$S3_BUCKET"; SIG=(--aws-sigv4 "aws:amz:us-east-1:s3" --user "$S3_AK:$S3_SK")
  RUNKEY=$PREFIX/$DIR
  # Stage keys are relative to the backend prefix, so the run directory is the endpoint itself.
  TARGET=(--destination "s3:$RUNKEY"); path_of() { echo "$1"; }
else
  TARGET=(--destination "$DEST"); path_of() { echo "$DIR/$1"; }
fi
build=$(cargo build -q --example transfer_resume 2>&1) || { echo "$build" >&2; exit 1; }
mkdir -p "$WORK/src"

# Everything a fresh container would still have: the binary, PATH, and the backend credentials.
# Fills FRESH_ENV (an array: PATH may contain spaces).
fresh_env() {
  local home; home=$(mktemp -d "$WORK/home-XXXX")
  FRESH_ENV=("HOME=$home" "PATH=$PATH")
  local name
  for name in $(compgen -e); do
    case "$name" in S3_*|CIFS_REAL_*|LAB_HDFS_*|RUST_LOG) FRESH_ENV+=("$name=${!name}") ;; esac
  done
}
s3_keys() { curl -s "${SIG[@]}" "$B?list-type=2&prefix=$RUNKEY/" | grep -o '<Key>[^<]*</Key>' | sed 's/<[^>]*>//g'; }
# The multipart uploads open on exactly key $1, one id per line: ListMultipartUploads with the key
# as prefix, then an exact-key filter (MinIO lists exact keys only, AWS / Ceph / StorageGRID by
# prefix). A reply that is not a listing prints "?".
key_uploads() {
  curl -s "${SIG[@]}" "$B?uploads&prefix=$1" | python3 -c 'import sys, xml.etree.ElementTree as ET
try: root = ET.fromstring(sys.stdin.read())
except ET.ParseError: print("?"); sys.exit()
ns = root.tag[:root.tag.index("}") + 1] if root.tag.startswith("{") else ""
if root.tag != ns + "ListMultipartUploadsResult": print("?"); sys.exit()
for upload in root.iter(ns + "Upload"):
    if upload.findtext(ns + "Key") == sys.argv[1]: print(upload.findtext(ns + "UploadId"))' "$1"
}
# The final keys of the run: one per interrupt mode.
S3_RUN_KEYS=(cancel kill)
# `.data-mover-*` names in the run directory. S3 adds the multipart uploads open on the run's keys:
# an open upload is not an object.
artifacts() {
  if [[ "$DEST" == s3:* ]]; then
    local objects open=0 key ids
    objects=$(s3_keys | grep -c '\.data-mover-')
    for key in "${S3_RUN_KEYS[@]}"; do
      ids=$(key_uploads "$RUNKEY/$key")
      # A failed listing is "?", not an open upload.
      if grep -qx '?' <<<"$ids"; then open='?'; break; fi
      open=$((open + $(grep -c . <<<"$ids")))
    done
    echo "$objects objects + $open open uploads"
  else
    "$BIN" --destination "$DEST" --list-artifacts "$DIR" 2>/dev/null |
      python3 -c 'import json,sys; print(len(json.load(sys.stdin)["artifacts"]))' 2>/dev/null || echo "?"
  fi
}
records() { find "$1" -name '*.state' 2>/dev/null | wc -l; }
# The destination endpoint a run derived (ADR-0006 C4); empty when the run printed no JSON.
endpoint_of() { python3 -c 'import json,sys
try: print(json.loads(sys.argv[1]).get("destination_endpoint",""))
except Exception: print("")' "$1"; }
# The transfer identity a run derived (ADR-0006 C5), from the start line it writes as soon as the
# request is built — so a run killed mid-transfer has one too (one killed while connecting does not).
identity_of() { python3 -c 'import json,sys
for line in open(sys.argv[1], errors="replace"):
    try: event = json.loads(line)
    except Exception: continue
    if event.get("event") == "start": print(event.get("identity", "")); break' "$1"; }

"$BIN" --source "$WORK/src" --source-path src --seed-bytes "$SIZE" --destination "$WORK/probe" \
  --destination-path src --policy atomic --read-back off >"$WORK/seed.out" 2>&1 ||
  { cat "$WORK/seed.out" >&2; exit 1; }
echo "run=$RUN dest=$DEST size=$SIZE bw=$BW cut_ms=$CUT_MS keep_state=$KEEP_STATE"
for mode in cancel kill; do
  state=$WORK/state-$mode
  export DATA_MOVER_RECOVERY_DIR=$state
  args=(--source "$WORK/src" --source-path src "${TARGET[@]}" --destination-path "$(path_of "$mode")"
        --policy checkpointed)
  if [ $mode = cancel ]; then
    "$BIN" "${args[@]}" --bandwidth "$BW" --cancel-after-ms "$CUT_MS" >"$WORK/first.out" 2>&1
    first=$(tail -1 "$WORK/first.out")
  else
    timeout -s KILL "$(printf '%d.%03d' $((CUT_MS / 1000)) $((CUT_MS % 1000)))" \
      "$BIN" "${args[@]}" --bandwidth "$BW" >"$WORK/first.out" 2>&1
    status=$?
    if [ $status = 137 ]; then first='{"result":"killed"}'; else first="exit $status: $(tail -1 "$WORK/first.out")"; fi
  fi
  first_identity=$(identity_of "$WORK/first.out")
  echo "[$mode] interrupted: $first"
  echo "[$mode]   local records=$(records "$state")  destination artifacts=$(artifacts)"
  # The container restarts: nothing local survives, unless KEEP_STATE=1 keeps the records.
  unset DATA_MOVER_RECOVERY_DIR
  fresh_env
  if [ "$KEEP_STATE" = 1 ]; then FRESH_ENV+=("DATA_MOVER_RECOVERY_DIR=$state"); else rm -rf "$state"; fi
  env -i "${FRESH_ENV[@]}" "$BIN" "${args[@]}" --read-back off --bandwidth "$UNLIMITED" >"$WORK/second.out" 2>&1
  second=$(tail -1 "$WORK/second.out")
  echo "[$mode] resumed:     $second"
  echo "[$mode]   destination artifacts after=$(artifacts)"
  # The fresh process must derive the endpoint the interrupted one did (a killed run prints none).
  seen=$(endpoint_of "$first"); again=$(endpoint_of "$second")
  if [ -n "$seen" ]; then same=$([ "$seen" = "$again" ] && echo yes || echo NO); else same="n/a (no output)"; fi
  echo "[$mode]   destination endpoint=$again  same as interrupted run: $same"
  ours=$(identity_of "$WORK/second.out")
  if [ -n "$first_identity" ]; then same=$([ "$first_identity" = "$ours" ] && echo yes || echo NO); else same="n/a (no start line)"; fi
  echo "[$mode]   transfer identity=$ours  same as interrupted run: $same"
  streamed=$(python3 -c 'import json,sys
try: print(json.loads(sys.argv[1]).get("source_streamed_bytes","?"))
except Exception: print("?")' "$second")
  echo "[$mode]   streamed=$streamed of $SIZE"
  # What prepare found at the destination and how much it reused (ADR-0006 C7e); Resumed with
  # reused + streamed = SIZE is a resume, Fresh / Restarted re-copied everything.
  python3 -c 'import json,sys
try: out = json.loads(sys.argv[1])
except Exception: out = {}
print("[%s]   prepare=%s reused_bytes=%s" % (sys.argv[2], out.get("prepare", "?"), out.get("reused_bytes", "?")))' "$second" "$mode"
  same=$("$BIN" --source "$WORK/src" --source-path src "${TARGET[@]}" --destination-path "$(path_of "$mode")" \
    --compare 2>&1 | tail -1 | python3 -c 'import json,sys
try: print("yes" if json.load(sys.stdin)["content_equal"] else "NO")
except Exception: print("? (no comparison)")')
  echo "[$mode]   destination BLAKE3 equals source: $same"
  rm -rf "$state"
done

echo "-- cleanup"
if [[ "$DEST" == s3:* ]]; then
  for key in "${S3_RUN_KEYS[@]}"; do
    key_uploads "$RUNKEY/$key" | grep -v '^?$' | while read -r id; do
      curl -s -o /dev/null "${SIG[@]}" -X DELETE "$B/$RUNKEY/$key?uploadId=$id"; done
  done
  s3_keys | while read -r k; do curl -s -o /dev/null "${SIG[@]}" -X DELETE "$B/$k"; done
  echo "left under $RUNKEY/: $(s3_keys | wc -l) objects, open uploads on the run's keys: $(artifacts | sed 's/.*+ //')"
else
  "$BIN" --destination "$DEST" --remove-run "$DIR" 2>&1 | tail -1
  # Listing the removed run directory must now fail with NotFound.
  left=$("$BIN" --destination "$DEST" --list-artifacts "$DIR" 2>&1 | tail -1)
  # SMB reports a missing directory as raw STATUS_OBJECT_NAME_NOT_FOUND (0xC0000034 = 3221225524).
  case "$left" in *NotFound*|*NOENT*|*3221225524*) echo "left: nothing ($DIR/ removed)";; *) echo "left: $left";; esac
fi
rm -rf "$WORK"
