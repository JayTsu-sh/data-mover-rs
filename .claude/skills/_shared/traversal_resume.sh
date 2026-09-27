#!/usr/bin/env bash
# traversal_resume.sh — resumable traversal on a real backend (P7 C11): one uninterrupted traversal,
# then the same traversal interrupted and resumed from its checkpoint until one run completes; the
# entries the interrupted runs delivered must be exactly the uninterrupted run's (nothing lost,
# nothing invented). Prints how many were delivered twice and how large the checkpoints were.
#
#   BIN=<storage_role_operations binary> BACKEND=local|nfs|cifs|s3 OUT=<empty dir> \
#     [ROOT=<backend root>] [TPATH=<sub-path>] [ORDER=unordered|blocks] [CONC=8] \
#     [MODE=cancel|kill] [CANCEL_AFTER=<entries>] [KILL_AFTER=<seconds>] [EVERY=<seconds>] \
#     [MAX_ROUNDS=50] [EXTRA="<more traverse flags>"] traversal_resume.sh
#
# MODE=cancel (default): each run is cancelled after CANCEL_AFTER entries, drains, and ends with its
#   final checkpoint; the next run resumes from it.
# MODE=kill: each run is killed (SIGKILL) after KILL_AFTER seconds, checkpointing every EVERY seconds;
#   only the entries printed before the last `checkpoint` line count, the next run resumes from the
#   last checkpoint file — a crash, not a pause.
# The backend's environment (NFS_REAL_URL, CIFS_REAL_*, S3_LISTING_URL) must already be exported.
# A versioned S3 bucket's keys come with their histories (P7 C11b): each record is its own `Record`
# line and is compared too, so the union must hold every record of every key (a key's history is
# delivered again from its first segment after a resume).
# Every run is bounded by `timeout`. Exit 0 only when the union equals the uninterrupted run.
set -u
: "${BIN:?}" "${BACKEND:?}" "${OUT:?}"
ORDER=${ORDER:-unordered}; CONC=${CONC:-8}; ROOT=${ROOT:-}; TPATH=${TPATH:-}
MODE=${MODE:-cancel}; CANCEL_AFTER=${CANCEL_AFTER:-1000}; KILL_AFTER=${KILL_AFTER:-5}
EVERY=${EVERY:-1}; MAX_ROUNDS=${MAX_ROUNDS:-50}; read -r -a extra <<< "${EXTRA:-}"
mkdir -p "$OUT"
common=(--backend "$BACKEND" --root "$ROOT" --concurrency "$CONC" traverse --order "$ORDER" --path "$TPATH" "${extra[@]}")
entries() { grep -E '^(File|Directory|Symlink|Special|Record) ' "$@"; }

timeout 3600 "$BIN" "${common[@]}" > "$OUT/full.out" 2> "$OUT/full.err"
grep -q 'outcome=Completed' "$OUT/full.out" || { echo "RESULT FAIL full run did not complete"; tail -3 "$OUT/full.err"; exit 1; }
entries "$OUT/full.out" | sort > "$OUT/full.entries"

rm -f "$OUT/ck" "$OUT/delivered"
: > "$OUT/delivered"
round=0
while :; do
  round=$((round + 1))
  [ "$round" -gt "$MAX_ROUNDS" ] && { echo "RESULT FAIL no completion after $MAX_ROUNDS rounds"; exit 1; }
  args=(--checkpoint-every "$EVERY" --checkpoint-out "$OUT/ck")
  [ -f "$OUT/ck" ] && args+=(--resume-from "$OUT/ck")
  if [ "$MODE" = kill ]; then
    timeout -s KILL "$KILL_AFTER" "$BIN" "${common[@]}" "${args[@]}" > "$OUT/r$round.out" 2> "$OUT/r$round.err"
  else
    timeout 3600 "$BIN" "${common[@]}" "${args[@]}" --cancel-after-entries "$CANCEL_AFTER" \
      > "$OUT/r$round.out" 2> "$OUT/r$round.err"
  fi
  # Entries count up to the last checkpoint line (a completed run: all of them).
  if grep -q 'outcome=Completed' "$OUT/r$round.out"; then
    entries "$OUT/r$round.out" >> "$OUT/delivered"
    break
  fi
  last=$(grep -n '^checkpoint bytes=' "$OUT/r$round.out" | tail -1 | cut -d: -f1)
  [ -n "$last" ] && head -n "$last" "$OUT/r$round.out" | entries >> "$OUT/delivered"
  grep -q 'Resume' "$OUT/r$round.err" && { echo "RESULT FAIL refused: $(tail -1 "$OUT/r$round.err")"; exit 1; }
done

python3 - "$OUT" "$round" <<'PY'
import sys, re, glob, os
out, rounds = sys.argv[1], int(sys.argv[2])
full = [l.rstrip('\n') for l in open(f"{out}/full.entries")]
got = [l.rstrip('\n') for l in open(f"{out}/delivered")]
fs, gs = set(full), set(got)
runs = {f: open(f).read() for f in glob.glob(f"{out}/r*.out")}
sizes = [int(m.group(1)) for text in runs.values() for m in re.finditer(r'^checkpoint bytes=(\d+)', text, re.M)]
# Checkpoints a cancelled round emitted besides its final one: at least one means it resumed past a periodic one.
periodic = [len(re.findall(r'^checkpoint bytes=', t, re.M)) - 1 for t in runs.values() if 'outcome=Completed' not in t]
# Every line of the full run is unique: a `Record` line carries <segment>.<n>, and the entry lines of
# one key's segments differ in `history=`. Changing either print format must keep that.
ok = fs == gs and len(full) == len(fs)
print(f"RESULT {'PASS' if ok else 'FAIL'} rounds={rounds} full={len(full)} delivered={len(got)} "
      f"duplicates={len(got) - len(gs)} lost={len(fs - gs)} invented={len(gs - fs)} "
      f"checkpoints={len(sizes)} ck_max_bytes={max(sizes, default=0)} "
      f"min_periodic_before_cancel={min(periodic, default=0)}")
for line in sorted(fs - gs)[:5]: print("  lost", line)
for line in sorted(gs - fs)[:5]: print("  invented", line)
sys.exit(0 if ok else 1)
PY
