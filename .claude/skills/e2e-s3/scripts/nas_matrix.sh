#!/usr/bin/env bash
# The ONTAP NAS bucket profile (`?compat=ontap&bucket_type=nas`, P7 §14.2) against the real bucket
# `data-mover-nas`, through examples/transfer_resume.rs. One line per case and a verdict
# (`ok` / `FAIL: …`); the last line is `RESULT: pass` or `RESULT: fail (n)`.
#
#   S3_ENV=.claude/skills/e2e-s3/ontap/.env bash .claude/skills/e2e-s3/scripts/nas_matrix.sh
#
# Env: the ONTAP .env (S3_HOST, S3_AK, S3_SK); the script sets S3_BUCKET=data-mover-nas,
# S3_COMPAT=ontap, S3_BUCKET_TYPE=nas itself. OBJECT_BUCKET (default data-mover-test) is the object
# bucket of the same SVM for the cross-bucket rows. NAS_BIG (default 1073741824) is the size of
# the large row; the volume snapshots hourly at :05, so the run deletes everything it wrote
# before it ends (GB objects must not outlive the next snapshot). Only keys under
# nas-<run>/ are written; empty directories are removed deepest first (a NAS bucket keeps them).
# SKIP_NATIVE=1 skips the same-bucket native rows, SKIP_BIG=1 the large rows. The last rows put the
# NAS profile on the object bucket (it works, with weaker guarantees) and leave it out on the NAS
# bucket (it fails as before, with a hint to add the parameters).
set -u
ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
cd "$ROOT"
source .claude/skills/_shared/cargo_examples.sh
S3_ENV=${S3_ENV:-.claude/skills/e2e-s3/ontap/.env}
set -a; . "$S3_ENV"; set +a
export S3_BUCKET=data-mover-nas S3_COMPAT=ontap S3_BUCKET_TYPE=nas
OBJECT_BUCKET=${OBJECT_BUCKET:-data-mover-test}
[[ $OBJECT_BUCKET =~ ^data-mover-[a-z0-9.-]+$ ]] || { echo "refusing bucket $OBJECT_BUCKET" >&2; exit 1; }
RUN=${RUN:-$(date +%s)}
# RUN names the key prefix and the work directory: a plain word, nothing a path could climb out of.
[[ $RUN =~ ^[a-z0-9-]+$ ]] || { echo "RUN must be [a-z0-9-]+: $RUN" >&2; exit 1; }
PREFIX=nas-$RUN
NAS_BIG=${NAS_BIG:-1073741824}
WORK=/tmp/data-mover-nas-$RUN
mkdir -p "$WORK/src" "$WORK/out"
cargo_build_examples transfer_resume || exit $?
BIN=$(cargo_example_binary transfer_resume) || exit 1
SCHEME=http; [ "${S3_USE_HTTPS:-}" = true ] && SCHEME=https
B="$SCHEME://$S3_HOST/$S3_BUCKET"
OB="$SCHEME://$S3_HOST/$OBJECT_BUCKET"
esc() { local v=${1//\\/\\\\}; printf '%s' "${v//\"/\\\"}"; }
s3c() { curl -s --aws-sigv4 "aws:amz:us-east-1:s3" -K <(printf 'user = "%s:%s"\n' "$(esc "$S3_AK")" "$(esc "$S3_SK")") "$@"; }
etag() { s3c -I "$1" | tr -d '\r' | awk -F': ' 'tolower($1)=="etag"{print $2}'; }
FAILS=0
verdict() { # label, condition-result ("" = ok, else the reason)
  if [ -z "$2" ]; then echo "   => ok"; else echo "   => FAIL: $2"; FAILS=$((FAILS + 1)); fi
}
field() { python3 -c 'import json,sys; d=json.loads(sys.argv[1]); print(d.get(sys.argv[2], ""))' "$1" "$2" 2>/dev/null; }
run() { "$BIN" "$@" 2>/dev/null | tail -1; }
seed() { "$BIN" --source "$WORK/src" --source-path "$1" --seed-bytes "$2" --destination "$WORK/probe" \
  --destination-path "$1" --policy atomic --read-back off --identity "seed-$RUN-$1" >/dev/null 2>&1; }
same_as() { # object URL, local file
  s3c -o "$WORK/check" "$1" && cmp -s "$WORK/check" "$2" && { rm -f "$WORK/check"; return 0; }
  rm -f "$WORK/check"; return 1
}

# Everything under the run's prefix goes, however the run ends (a timeout, a failed command): the
# volume snapshots hourly, and a GB object must not outlive the next snapshot.
cleanup() {
  for _ in 1 2 3; do
    keys=$(s3c -f "$B?list-type=2&prefix=$PREFIX/" | grep -o '<Key>[^<]*</Key>' | sed 's/<[^>]*>//g')
    [ -z "$keys" ] && break
    echo "$keys" | while read -r k; do
      case "$k" in "$PREFIX/"*) s3c -o /dev/null -X DELETE "$B/$k" ;; esac
    done
  done
  for dir in "$PREFIX/d" "$PREFIX/clash" "$PREFIX/a" "$PREFIX"; do s3c -o /dev/null -X DELETE "$B/$dir/"; done
  for key in from-nas on-object; do s3c -o /dev/null -X DELETE "$OB/$PREFIX/$key"; done
  s3c -o /dev/null -X DELETE "$OB/$PREFIX/"
  rm -rf "$WORK"
}
trap cleanup EXIT
echo "server $S3_HOST bucket $S3_BUCKET run=$RUN prefix=$PREFIX"
for entry in z0:0 k1:1024 m8p1:8388609 m200:209715200; do seed "${entry%%:*}" "${entry#*:}"; done

echo "-- local -> NAS: sizes x policies x read-back"
for entry in z0 k1 m8p1 m200; do
  for policy in checkpointed atomic direct; do
    for rb in on off; do
      [ "$entry" = m200 ] && [ "$rb" = off ] && [ "$policy" != atomic ] && continue
      key="$policy-$rb-$entry"
      line=$(run --source "$WORK/src" --source-path "$entry" --destination "s3:$PREFIX" \
        --destination-path "$key" --policy "$policy" --read-back "$rb" --identity "d-$RUN-$key")
      echo "$key: $line"
      why=
      [ "$(field "$line" result)" = ok ] || why="not ok"
      expected=Disabled; [ "$policy" = checkpointed ] && expected=UnsupportedByDestination
      [ -z "$why" ] && [ "$(field "$line" recovery)" != "$expected" ] && why="recovery $(field "$line" recovery)"
      method="EtagBracketed { reason: ConditionalReadsUnsupported }"; [ "$rb" = off ] && method=NotPerformed
      [ -z "$why" ] && [ "$(field "$line" read_back_method)" != "$method" ] && why="read_back_method"
      [ -z "$why" ] && [ "$(field "$line" upload_digest)" != Matched ] && why="upload_digest"
      [ -z "$why" ] && ! same_as "$B/$PREFIX/$key" "$WORK/src/$entry" && why="content differs"
      verdict "$key" "$why"
    done
  done
done

if [ "${SKIP_BIG:-0}" != 1 ]; then
  echo "-- local -> NAS: $NAS_BIG bytes, read-back on (deleted at once: hourly snapshot at :05)"
  seed big "$NAS_BIG"
  rm -f "$WORK/probe/big"
  line=$(run --source "$WORK/src" --source-path big --destination "s3:$PREFIX" --destination-path big \
    --policy atomic --read-back on --identity "d-$RUN-big")
  echo "big: $line"
  why=; [ "$(field "$line" result)" = ok ] || why="not ok"
  [ -z "$why" ] && ! same_as "$B/$PREFIX/big" "$WORK/src/big" && why="content differs"
  verdict big "$why"
  if [ "${SKIP_NATIVE:-0}" != 1 ]; then
    line=$(run --source "s3:$PREFIX" --source-path big --destination "s3:$PREFIX" --destination-path big-copy \
      --policy atomic --read-back on --identity "n-$RUN-big")
    echo "native big: $line"
    why=; [ "$(field "$line" native_requests)" = 1 ] || why="native_requests=$(field "$line" native_requests)"
    same_as "$B/$PREFIX/big-copy" "$WORK/src/big" || why="$why content differs"
    verdict "native big" "$why"
  fi
  s3c -o /dev/null -X DELETE "$B/$PREFIX/big"
  s3c -o /dev/null -X DELETE "$B/$PREFIX/big-copy"
  why=; [ -z "$(etag "$B/$PREFIX/big")$(etag "$B/$PREFIX/big-copy")" ] || why="a large object is still there"
  verdict "large objects deleted" "$why"
fi

echo "-- over 5 GiB: refused at Preflight, nothing written"
truncate -s 5368709121 "$WORK/src/huge"
line=$(run --source "$WORK/src" --source-path huge --destination "s3:$PREFIX" --destination-path huge \
  --policy atomic --read-back off --identity "d-$RUN-huge")
echo "huge: $line"
why=; [ "$(field "$line" phase)" = Preflight ] || why="phase $(field "$line" phase)"
[ -n "$(etag "$B/$PREFIX/huge")" ] && { why="$why object written"; s3c -o /dev/null -X DELETE "$B/$PREFIX/huge"; }
verdict huge "$why"
rm -f "$WORK/src/huge"

echo "-- key names the bucket would rewrite: refused at Preflight"
long=$(printf 'n%.0s' $(seq 1 256))
for name in "a/../x" "a/./y" "a//b" "d/$long"; do
  line=$(run --source "$WORK/src" --source-path k1 --destination "s3:$PREFIX" --destination-path "$name" \
    --policy atomic --read-back off --identity "d-$RUN-name")
  echo "${name:0:24}: $(field "$line" phase) $(field "$line" failure | cut -c1-160)"
  why=; [ "$(field "$line" phase)" = Preflight ] || why="phase $(field "$line" phase)"
  verdict name "$why"
done
[ -n "$(etag "$B/x")$(etag "$B/$PREFIX/x")$(etag "$B/$PREFIX/a/y")$(etag "$B/$PREFIX/a/b")" ] && {
  echo "   => FAIL: a rewritten key exists"; FAILS=$((FAILS + 1)); }

echo "-- file / directory clash: a name conflict (Conflict, not NotFound)"
run --source "$WORK/src" --source-path k1 --destination "s3:$PREFIX" --destination-path clash \
  --policy atomic --read-back off --identity "d-$RUN-clash" >/dev/null
line=$(run --source "$WORK/src" --source-path k1 --destination "s3:$PREFIX" --destination-path clash/x \
  --policy atomic --read-back off --identity "d-$RUN-clash-x")
echo "clash/x: $(field "$line" failure | cut -c1-200)"
why=; grep -q Conflict <<<"$(field "$line" failure)" || why="not a Conflict"
verdict clash "$why"

echo "-- interrupted before publication: the old object is intact, the rerun is Fresh"
run --source "$WORK/src" --source-path k1 --destination "s3:$PREFIX" --destination-path held \
  --policy atomic --read-back off --identity "d-$RUN-held-old" >/dev/null
old=$(etag "$B/$PREFIX/held")
[ -n "$old" ] || { echo "   => FAIL: no old object to keep"; FAILS=$((FAILS + 1)); }
line=$(run --source "$WORK/src" --source-path m200 --destination "s3:$PREFIX" --destination-path held \
  --policy checkpointed --read-back off --identity "d-$RUN-held" --bandwidth 20971520 --cancel-after-ms 4000)
echo "cancel at 4 s: $line"
why=; [ "$(field "$line" final_destination_changed)" = False ] || why="changed=$(field "$line" final_destination_changed)"
[ "$(etag "$B/$PREFIX/held")" = "$old" ] || why="$why old object replaced"
verdict cancel "$why"
timeout -s KILL 4 "$BIN" --source "$WORK/src" --source-path m200 --destination "s3:$PREFIX" \
  --destination-path held --policy checkpointed --read-back off --identity "d-$RUN-held" \
  --bandwidth 20971520 >/dev/null 2>&1
echo "SIGKILL at 4 s: exit=$?"
sleep 2
why=; [ "$(etag "$B/$PREFIX/held")" = "$old" ] || why="old object replaced"
verdict kill "$why"
line=$(run --source "$WORK/src" --source-path m200 --destination "s3:$PREFIX" --destination-path held \
  --policy checkpointed --read-back on --identity "d-$RUN-held")
echo "rerun: $line"
why=; [ "$(field "$line" prepare)" = Fresh ] || why="prepare $(field "$line" prepare)"
[ "$(field "$line" recovery)" = UnsupportedByDestination ] || why="$why recovery"
same_as "$B/$PREFIX/held" "$WORK/src/m200" || why="$why content differs"
verdict rerun "$why"

echo "-- NAS as a source"
line=$(run --source "s3:$PREFIX" --source-path atomic-on-m200 --destination "$WORK/out" \
  --destination-path m200 --policy atomic --read-back on --identity "s-$RUN-m200")
echo "NAS -> local m200: $line"
why=; [ "$(field "$line" result)" = ok ] || why="not ok"
cmp -s "$WORK/out/m200" "$WORK/src/m200" || why="$why content differs"
verdict source "$why"
( sleep 3; s3c -o /dev/null -T "$WORK/src/k1" "$B/$PREFIX/atomic-off-m200" ) &
line=$(run --source "s3:$PREFIX" --source-path atomic-off-m200 --destination "$WORK/out" \
  --destination-path replaced --policy atomic --read-back off --identity "s-$RUN-replaced" --bandwidth 20971520)
wait
echo "replaced mid-read: $(field "$line" failure | cut -c1-200)"
why=; grep -q Conflict <<<"$(field "$line" failure)" || why="not a Conflict"
verdict replaced "$why"

if [ "${SKIP_NATIVE:-0}" != 1 ]; then
  echo "-- same-bucket native copy (one CopyObject); NAS <-> object bucket streams"
  for entry in k1 m200; do
    line=$(run --source "s3:$PREFIX" --source-path "atomic-on-$entry" --destination "s3:$PREFIX" \
      --destination-path "native-$entry" --policy checkpointed --read-back on --identity "n-$RUN-$entry")
    echo "native $entry: $line"
    why=; [ "$(field "$line" route)" = Native ] || why="route $(field "$line" route)"
    [ "$(field "$line" native_requests)" = 1 ] || why="$why native_requests=$(field "$line" native_requests)"
    [ "$(field "$line" recovery)" = UnsupportedByDestination ] || why="$why recovery"
    same_as "$B/$PREFIX/native-$entry" "$WORK/src/$entry" || why="$why content differs"
    verdict "native $entry" "$why"
  done
  line=$(run --source "s3:$PREFIX" --source-path atomic-on-k1 --destination "s3@$OBJECT_BUCKET:$PREFIX" \
    --destination-path from-nas --policy atomic --read-back on --identity "x-$RUN-1")
  echo "NAS -> $OBJECT_BUCKET: $line"
  why=; [ "$(field "$line" route)" = Streaming ] || why="route $(field "$line" route)"
  same_as "$OB/$PREFIX/from-nas" "$WORK/src/k1" || why="$why content differs"
  verdict "nas->object" "$why"
  line=$(run --source "s3@$OBJECT_BUCKET:$PREFIX" --source-path from-nas --destination "s3:$PREFIX" \
    --destination-path from-object --policy atomic --read-back on --identity "x-$RUN-2")
  echo "$OBJECT_BUCKET -> NAS: $line"
  why=; [ "$(field "$line" route)" = Streaming ] || why="route $(field "$line" route)"
  same_as "$B/$PREFIX/from-object" "$WORK/src/k1" || why="$why content differs"
  verdict "object->nas" "$why"
  s3c -o /dev/null -X DELETE "$OB/$PREFIX/from-nas"
fi

echo "-- the parameter on an object bucket ($OBJECT_BUCKET): works, weaker guarantees"
line=$(S3_BUCKET=$OBJECT_BUCKET run --source "$WORK/src" --source-path m8p1 --destination "s3:$PREFIX" \
  --destination-path on-object --policy checkpointed --read-back on --identity "o-$RUN-1")
echo "nas profile on $OBJECT_BUCKET: $line"
why=; [ "$(field "$line" result)" = ok ] || why="not ok"
[ "$(field "$line" recovery)" = UnsupportedByDestination ] || why="$why recovery"
same_as "$OB/$PREFIX/on-object" "$WORK/src/m8p1" || why="$why content differs"
verdict "profile on object bucket" "$why"
s3c -o /dev/null -X DELETE "$OB/$PREFIX/on-object"

echo "-- the parameter left out on the NAS bucket: today's failures, with the hint"
line=$(S3_BUCKET_TYPE= run --source "$WORK/src" --source-path m8p1 --destination "s3:$PREFIX" \
  --destination-path no-profile --policy atomic --read-back off --identity "h-$RUN-1")
echo "no profile, 8 MiB+1: $(field "$line" failure | cut -c1-120) | $(field "$line" diagnostic)"
why=; grep -q 'bucket_type=nas' <<<"$(field "$line" diagnostic)" || why="no hint"
verdict "hint" "$why"

echo "-- cleanup of $PREFIX/ (objects, then directories deepest first)"
trap - EXIT
cleanup
left='?'
listing=$(s3c -f "$B?list-type=2&delimiter=/&prefix=$PREFIX") &&
  left=$(grep -o '<Key>\|<CommonPrefixes>' <<<"$listing" | wc -l)
echo "left under $PREFIX: $left"
[ "$left" = 0 ] || { echo "   => FAIL: left $left"; FAILS=$((FAILS + 1)); }
if [ "$FAILS" = 0 ]; then echo "RESULT: pass"; else echo "RESULT: fail ($FAILS)"; fi
