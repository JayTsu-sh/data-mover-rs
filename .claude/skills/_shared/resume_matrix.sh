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
#          64 MiB checkpoint is durable at the default BW)
#   KEEP_STATE=1  keep the local recovery records across the restart (still a fresh process and
#          HOME). Neither run names the transfer: the resume can only find the record through the
#          identity data-mover derives (ADR-0006 C5), so "streamed" below SIZE proves it did.
# Writes only under <DEST>/resume-<run>/ and removes all of it at the end: final files, orphaned
# stages and checkpoints, and (S3) the multipart uploads the interrupted runs recorded.
#
# Both runs derive the transfer identity from the endpoints and paths (no --identity); each line
# reports whether the resumed run derived the interrupted run's identity and endpoint.
# "streamed" is what the resuming run read from the source (it runs with read-back off and a
# non-limiting budget, so the engine counts it): SIZE means nothing was reused. "records" is how
# many recovery records the interrupted run left locally — 0 means the cut came before the first
# checkpoint and the run says nothing about resume.
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
# `.data-mover-*` names in the run directory. S3 adds the recorded multipart uploads still open: an
# open upload is not an object, and MinIO lists uploads by exact key only.
artifacts() {
  if [[ "$DEST" == s3:* ]]; then
    local objects open=0 key id
    objects=$(s3_keys | grep -c '\.data-mover-')
    [ -f "$WORK/s3-uploads" ] && while read -r key id; do
      [ "$(curl -s -o /dev/null -w '%{http_code}' "${SIG[@]}" "$B/$RUNKEY/$key?uploadId=$id")" = 200 ] && open=$((open + 1))
    done <"$WORK/s3-uploads"
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
# S3 today: the orphaned upload is only findable through the local record.
remember_s3_upload() {
  [[ "$DEST" == s3:* ]] || return 0
  local record; record=$(ls "$1"/*.state 2>/dev/null | head -1)
  [ -n "$record" ] && python3 - "$record" >>"$WORK/s3-uploads" <<'PY'
import sys
data = open(sys.argv[1], 'rb').read()
start = data.find(b'.data-mover-stage/')
# The stage token `<key>\0<upload id>` is stored with a 4-byte little-endian length in front.
token = data[start:start + int.from_bytes(data[start - 4:start], 'little')] if start >= 4 else b''
key, _, upload = token.partition(b'\0')
if key and upload: print(key.decode(), upload.decode())
PY
}

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
  remember_s3_upload "$state"
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
  # A failed resume may have opened an upload of its own: record it before its state goes.
  remember_s3_upload "$state"
  rm -rf "$state"
done

echo "-- cleanup"
if [[ "$DEST" == s3:* ]]; then
  [ -f "$WORK/s3-uploads" ] && while read -r key id; do
    curl -s -o /dev/null "${SIG[@]}" -X DELETE "$B/$RUNKEY/$key?uploadId=$id"; done <"$WORK/s3-uploads"
  s3_keys | while read -r k; do curl -s -o /dev/null "${SIG[@]}" -X DELETE "$B/$k"; done
  echo "left under $RUNKEY/: $(s3_keys | wc -l) objects, recorded uploads: $(artifacts | sed 's/.*+ //')"
else
  "$BIN" --destination "$DEST" --remove-run "$DIR" 2>&1 | tail -1
  # Listing the removed run directory must now fail with NotFound.
  left=$("$BIN" --destination "$DEST" --list-artifacts "$DIR" 2>&1 | tail -1)
  # SMB reports a missing directory as raw STATUS_OBJECT_NAME_NOT_FOUND (0xC0000034 = 3221225524).
  case "$left" in *NotFound*|*NOENT*|*3221225524*) echo "left: nothing ($DIR/ removed)";; *) echo "left: $left";; esac
fi
rm -rf "$WORK"
