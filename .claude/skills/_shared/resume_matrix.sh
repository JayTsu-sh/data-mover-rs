#!/usr/bin/env bash
# Container-restart resume matrix (ADR-0006): interrupt a transfer, resume it from a fresh process
# with a fresh HOME, and report what was reused and what was left behind. It records how each
# backend behaves; the one thing it asserts is that no run writes under its fresh HOME.
#
# Usage: DEST=<endpoint> bash .claude/skills/_shared/resume_matrix.sh
#   DEST   destination endpoint, as examples/transfer_resume.rs takes it: a local directory,
#          nfs://host/export[:root]?..., smb:<sub-path> (e2e-cifs .env), s3:<prefix> (e2e-s3 .env,
#          prefix required), hdfs://... (LAB_HDFS_* env)
#   SIZE   bytes of the source file (default 200 MiB); BW source bandwidth in bytes/s while the first
#          run is cut (default 20 MiB/s); CUT_MS when the cut lands (default 6000, so the first
#          64 MiB checkpoint is durable at the default BW; on S3 the pointer is written when the
#          upload begins (ADR-0006 C16), so a SIGKILL resumes even before that checkpoint)
#   S3_BUCKET_OVERRIDE  (s3: only) another bucket of the same server, e.g. a temporary versioned
#          one (ADR-0006 C17), instead of the .env's S3_BUCKET — never by editing the .env
# Writes only under <DEST>/resume-<run>/ and removes all of it at the end: final files, orphaned
# stages and checkpoints, and (S3) every multipart upload still open on the run's keys.
#
# Both runs derive the transfer identity from the endpoints and paths (no --identity); each line
# reports whether the resumed run derived the interrupted run's identity and endpoint.
# "prepare" / "reused_bytes" say what the resuming run found at the destination, and the content
# check compares the final file with the source by BLAKE3.
# "streamed" is what the resuming run read from the source (it runs with read-back off and a
# non-limiting budget, so the engine counts it): SIZE means nothing was reused. For S3
# "destination artifacts" is the `.data-mover-*` objects (the `.upload` pointer) plus the multipart
# uploads open on the run's final keys.
# Nothing is kept where data-mover runs (ADR-0006 C21): both runs get their own fresh, empty HOME
# (and nothing else of this machine's environment but PATH and the backend credentials), and each
# line "fresh HOME" reports what a run left there — "empty" is the acceptance criterion; anything
# else is listed and makes the script exit 1 at the end.
set -u
ROOT=$(cd "$(dirname "$0")/../../.." && pwd)
cd "$ROOT"
for env in .claude/skills/e2e-s3/.env .claude/skills/e2e-cifs/.env; do
  [ -f "$env" ] && { set -a; . "$env"; set +a; }
done
: "${DEST:?set DEST to the destination endpoint}"
SIZE=${SIZE:-209715200}; BW=${BW:-20971520}; CUT_MS=${CUT_MS:-6000}
UNLIMITED=10737418240
RUN=${RUN:-$(date +%s)}
DIR=resume-$RUN
WORK=/tmp/data-mover-resume-$RUN
BIN=target/debug/examples/transfer_resume
if [[ "$DEST" == s3:* ]]; then
  [ -n "${S3_BUCKET_OVERRIDE:-}" ] && S3_BUCKET=$S3_BUCKET_OVERRIDE
  PREFIX=${DEST#s3:}; PREFIX=${PREFIX#/}; PREFIX=${PREFIX%/}
  [ -n "$PREFIX" ] || { echo "DEST=s3:<prefix> needs a non-empty prefix" >&2; exit 1; }
  # `a-bucket` holds Milvus data on the lab MinIO; never write there.
  [ "${S3_BUCKET:-}" = a-bucket ] && { echo "refusing to write to bucket a-bucket" >&2; exit 1; }
  SCHEME=http; [ "${S3_USE_HTTPS:-}" = true ] && SCHEME=https
  B="$SCHEME://$S3_HOST/$S3_BUCKET"
  # The credentials reach curl through a config on a file descriptor, never on its command line.
  esc() { local v=${1//\\/\\\\}; printf '%s' "${v//\"/\\\"}"; }
  s3c() { curl -s --aws-sigv4 "aws:amz:us-east-1:s3" -K <(printf 'user = "%s:%s"\n' "$(esc "$S3_AK")" "$(esc "$S3_SK")") "$@"; }
  RUNKEY=$PREFIX/$DIR
  # Stage keys are relative to the backend prefix, so the run directory is the endpoint itself.
  TARGET=(--destination "s3:$RUNKEY"); path_of() { echo "$1"; }
else
  TARGET=(--destination "$DEST"); path_of() { echo "$DIR/$1"; }
fi
build=$(cargo build -q --example transfer_resume 2>&1) || { echo "$build" >&2; exit 1; }
mkdir -p "$WORK/src"

# Everything a fresh container would still have: the binary, PATH, and the backend credentials.
# Fills FRESH_ENV (an array: PATH may contain spaces) and FRESH_HOME, a new empty directory.
fresh_env() {
  FRESH_HOME=$(mktemp -d "$WORK/home-XXXX")
  FRESH_ENV=("HOME=$FRESH_HOME" "PATH=$PATH")
  local name
  for name in $(compgen -e); do
    case "$name" in S3_*|CIFS_REAL_*|LAB_HDFS_*|RUST_LOG) FRESH_ENV+=("$name=${!name}") ;; esac
  done
}
s3_keys() { s3c "$B?list-type=2&prefix=$RUNKEY/" | grep -o '<Key>[^<]*</Key>' | sed 's/<[^>]*>//g'; }
# The multipart uploads open on exactly key $1, one id per line: ListMultipartUploads with the key
# as prefix, then an exact-key filter (MinIO lists exact keys only, AWS / Ceph / StorageGRID by
# prefix). A reply that is not a listing prints "?".
key_uploads() {
  s3c "$B?uploads&prefix=$1" | python3 -c 'import sys, xml.etree.ElementTree as ET
try: root = ET.fromstring(sys.stdin.read())
except ET.ParseError: print("?"); sys.exit()
ns = root.tag[:root.tag.index("}") + 1] if root.tag.startswith("{") else ""
if root.tag != ns + "ListMultipartUploadsResult": print("?"); sys.exit()
for upload in root.iter(ns + "Upload"):
    if upload.findtext(ns + "Key") == sys.argv[1]: print(upload.findtext(ns + "UploadId"))' "$1"
}
# ListObjectVersions under the run (ADR-0006 C17): versions of final key $1, delete markers and
# `.data-mover-*` entries anywhere under the run. An unversioned bucket lists each object once, as
# version "null"; a versioned one must show exactly one version of the key and nothing else.
s3_versions() {
  s3c "$B?versions&prefix=$RUNKEY/" | python3 -c 'import sys, xml.etree.ElementTree as ET
try: root = ET.fromstring(sys.stdin.read())
except ET.ParseError: print("versions=?"); sys.exit()
ns = root.tag[:root.tag.index("}") + 1] if root.tag.startswith("{") else ""
versions = [(v.findtext(ns + "Key"), v.findtext(ns + "VersionId")) for v in root.iter(ns + "Version")]
markers = [m.findtext(ns + "Key") for m in root.iter(ns + "DeleteMarker")]
final = [v for k, v in versions if k == sys.argv[1]]
artifacts = [k for k, _ in versions if ".data-mover-" in k] + [k for k in markers if ".data-mover-" in k]
print("final versions=%d markers=%d artifact entries=%d" % (len(final), len(markers), len(artifacts)))' "$1"
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
# What a run left in its fresh HOME (ADR-0006: nothing is kept where data-mover runs): sets
# HOME_VERDICT to "empty" or to the paths it created. Not called in $(…): a subshell would lose
# HOME_DIRTY, which fails the script at the end when anything was left (or the HOME is unreadable).
HOME_DIRTY=0
home_check() {
  local left
  if ! left=$(cd "$1" 2>/dev/null && find . -mindepth 1 | sort); then
    HOME_DIRTY=1; HOME_VERDICT="? (cannot list $1)"; return
  fi
  if [ -z "$left" ]; then HOME_VERDICT=empty
  else HOME_DIRTY=1; HOME_VERDICT="NOT EMPTY: $(tr '\n' ' ' <<<"$left")"; fi
}
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
echo "run=$RUN dest=$DEST size=$SIZE bw=$BW cut_ms=$CUT_MS"
for mode in cancel kill; do
  args=(--source "$WORK/src" --source-path src "${TARGET[@]}" --destination-path "$(path_of "$mode")"
        --policy checkpointed)
  fresh_env
  if [ $mode = cancel ]; then
    env -i "${FRESH_ENV[@]}" "$BIN" "${args[@]}" --bandwidth "$BW" --cancel-after-ms "$CUT_MS" \
      >"$WORK/first.out" 2>&1
    first=$(tail -1 "$WORK/first.out")
  else
    timeout -s KILL "$(printf '%d.%03d' $((CUT_MS / 1000)) $((CUT_MS % 1000)))" \
      env -i "${FRESH_ENV[@]}" "$BIN" "${args[@]}" --bandwidth "$BW" >"$WORK/first.out" 2>&1
    status=$?
    if [ $status = 137 ]; then first='{"result":"killed"}'; else first="exit $status: $(tail -1 "$WORK/first.out")"; fi
  fi
  first_identity=$(identity_of "$WORK/first.out")
  echo "[$mode] interrupted: $first"
  home_check "$FRESH_HOME"
  echo "[$mode]   fresh HOME=$HOME_VERDICT  destination artifacts=$(artifacts)"
  # The container restarts: a fresh process with another fresh HOME.
  fresh_env
  env -i "${FRESH_ENV[@]}" "$BIN" "${args[@]}" --read-back off --bandwidth "$UNLIMITED" >"$WORK/second.out" 2>&1
  second=$(tail -1 "$WORK/second.out")
  echo "[$mode] resumed:     $second"
  home_check "$FRESH_HOME"
  echo "[$mode]   fresh HOME=$HOME_VERDICT  destination artifacts after=$(artifacts)"
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
  [[ "$DEST" == s3:* ]] && echo "[$mode]   $(s3_versions "$RUNKEY/$mode")"
done

echo "-- cleanup"
if [[ "$DEST" == s3:* ]]; then
  for key in "${S3_RUN_KEYS[@]}"; do
    key_uploads "$RUNKEY/$key" | grep -v '^?$' | while read -r id; do
      s3c -o /dev/null -X DELETE "$B/$RUNKEY/$key?uploadId=$id"; done
  done
  s3_keys | while read -r k; do s3c -o /dev/null -X DELETE "$B/$k"; done
  echo "left under $RUNKEY/: $(s3_keys | wc -l) objects, open uploads on the run's keys: $(artifacts | sed 's/.*+ //')"
else
  "$BIN" --destination "$DEST" --remove-run "$DIR" 2>&1 | tail -1
  # Listing the removed run directory must now fail with NotFound.
  left=$("$BIN" --destination "$DEST" --list-artifacts "$DIR" 2>&1 | tail -1)
  # SMB reports a missing directory as raw STATUS_OBJECT_NAME_NOT_FOUND (0xC0000034 = 3221225524).
  case "$left" in *NotFound*|*NOENT*|*3221225524*) echo "left: nothing ($DIR/ removed)";; *) echo "left: $left";; esac
fi
rm -rf "$WORK"
if [ "$HOME_DIRTY" = 1 ]; then echo "FAIL: a run wrote under its fresh HOME" >&2; exit 1; fi
echo "fresh HOME: every run left it empty"
