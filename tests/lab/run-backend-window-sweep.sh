#!/usr/bin/env bash
# Run on the designated test host. Endpoints must name an isolated test namespace.
# Credentials belong in exported environment variables, never in endpoint arguments.
set -euo pipefail
binary="${1:?binary}"
backend="${2:?LOCAL, NFS, CIFS or S3}"
source_endpoint="${3:?source endpoint}"
destination_endpoint="${4:?destination endpoint}"
path="${5:?fixture path}"
output="${6:?result directory}"
rounds="${ROUNDS:-7}"
case "$backend" in
  LOCAL) candidates=(1:8 2:8 4:8 8:8 4:1 4:2 4:4) ;;
  NFS) candidates=(2:8 4:8 8:8 4:4 4:16) ;;
  CIFS) candidates=(4:8 8:8 16:8 8:4 8:16) ;;
  S3) candidates=(1:4 2:4 4:4 8:4 4:1 4:2 4:5 4:8) ;;
  *) echo 'unsupported backend' >&2; exit 2 ;;
esac
if [[ -n "${WINDOW_CANDIDATES:-}" ]]; then
  read -r -a candidates <<< "$WINDOW_CANDIDATES"
fi
for candidate in "${candidates[@]}"; do
  [[ "$candidate" =~ ^[1-9][0-9]*:[1-9][0-9]*$ ]] || exit 2
done
read -r -a policies <<< "${SWEEP_POLICIES:-atomic-replace checkpointed}"
for policy in "${policies[@]}"; do
  [[ "$policy" == atomic-replace || "$policy" == checkpointed ]] || exit 2
done
[[ "$rounds" =~ ^[1-9][0-9]*$ ]] || exit 2
mkdir -p "$output"
csv="$output/samples.csv"
[[ ! -e "$csv" ]] || { echo 'refusing to overwrite samples' >&2; exit 2; }
printf '%s\n' 'policy,round,read,write,bytes,elapsed_ns,user_s,system_s,max_rss_kib' > "$csv"
for policy in "${policies[@]}"; do
  # Round zero is an explicit warmup, retained separately from the formal samples.
  for ((round=0; round<=rounds; round++)); do
    for ((index=0; index<${#candidates[@]}; index++)); do
      slot=$index
      if ((round % 2)); then slot=$((${#candidates[@]} - index - 1)); fi
      IFS=: read -r reads writes <<< "${candidates[$slot]}"
      label="$policy-r$round-read$reads-write$writes"
      if ! env "DATA_MOVER_${backend}_READ_INFLIGHT=$reads" \
          "DATA_MOVER_${backend}_WRITE_INFLIGHT=$writes" \
          timeout 120 /usr/bin/time -o "$output/$label.time" -f '%U,%S,%M' \
          "$binary" --source "$source_endpoint" --destination "$destination_endpoint" \
          --path "$path" --policy "$policy" > "$output/$label.log" 2>&1; then
        echo "failed: $label (see private run log)" >&2
        exit 1
      fi
      values=$(awk '{ for(i=1;i<=NF;i++) { split($i,a,"="); v[a[1]]=a[2] } }
        END { if(v["bytes"]=="" || v["elapsed_ns"]=="") exit 1;
              printf "%s,%s",v["bytes"],v["elapsed_ns"] }' "$output/$label.log")
      timing=$(<"$output/$label.time")
      printf '%s,%s,%s,%s,%s,%s\n' "$policy" "$round" "$reads" "$writes" \
        "$values" "$timing" >> "$csv"
    done
    echo "completed: $backend $policy round $round/$rounds"
  done
done
