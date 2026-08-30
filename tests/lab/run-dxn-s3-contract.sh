#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

run_id="${1:-missing-run-id}"
endpoint="${LAB_DXN_S3_ENDPOINT:-http://10.131.7.201:8184}"
bucket="${LAB_DXN_S3_BUCKET:-test-agent-s3-bucket-202608301331}"
prefix=""
evidence_path="${DXN_EVIDENCE_PATH:-dxn-s3-evidence.json}"
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

cleanup() {
  local status="$1"
  RUN_ID="$run_id" STARTED_AT="$started_at" EVIDENCE_PATH="$evidence_path" \
    ENDPOINT="$endpoint" python3 - "$status" <<'PY'
import datetime
import json
import os
import subprocess
import sys

status = int(sys.argv[1])
report = {
    "schema_version": 1,
    "gate_id": "DM-DXN-CONTRACT",
    "candidate_sha": subprocess.check_output(
        ["git", "rev-parse", "HEAD"], text=True
    ).strip(),
    "run_id": os.environ["RUN_ID"],
    "environment_fingerprint": f"shared-real-dxn/{os.environ['ENDPOINT']}",
    "started_at": os.environ["STARTED_AT"],
    "completed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "outcome": "passed" if status == 0 else "failed",
}
with open(os.environ["EVIDENCE_PATH"], "w", encoding="utf-8") as handle:
    json.dump(report, handle, indent=2)
    handle.write("\n")
PY
  if [[ -n "$prefix" && -n "${LAB_DXN_S3_ACCESS_KEY:-}" && -n "${LAB_DXN_S3_SECRET_KEY:-}" ]]; then
    LAB_S3_ACCESS_KEY="$LAB_DXN_S3_ACCESS_KEY" \
      LAB_S3_SECRET_KEY="$LAB_DXN_S3_SECRET_KEY" \
      python3 "$(dirname "$0")/s3_helper.py" delete-prefix \
        --endpoint "$endpoint" --bucket "$bucket" --prefix "$prefix/" || true
  fi
  exit "$status"
}
trap 'cleanup $?' EXIT

[[ "$run_id" != "missing-run-id" ]] || {
  echo "run id required (nightly-* or release-*)" >&2
  exit 2
}
validate_run_id "$run_id"
: "${LAB_DXN_S3_ACCESS_KEY:?LAB_DXN_S3_ACCESS_KEY is required}"
: "${LAB_DXN_S3_SECRET_KEY:?LAB_DXN_S3_SECRET_KEY is required}"
prefix="ci/$run_id/dxn-s3-architecture"
export LAB_DXN_S3_ARCHITECTURE_URL="s3+dxn://${LAB_DXN_S3_ACCESS_KEY}:${LAB_DXN_S3_SECRET_KEY}@${bucket}.${endpoint#http://}"
export LAB_DXN_S3_ARCHITECTURE_KEY="$prefix/published.bin"

cargo test --test s3_architecture_contract dxn_s3_architecture_roles_and_known_limits \
  -- --ignored --exact --test-threads=1
cargo test --lib s3::dxn::tests::multipart_rename_limit_preserves_source_when_lab_is_configured \
  -- --exact --test-threads=1
cargo test s3::storagegrid::tests::standard_s3_delete_objects_does_not_add_storagegrid_content_md5 -- \
  --exact --test-threads=1
cargo test s3::storagegrid::tests::dxn_delete_objects_adds_signed_content_md5_without_stripping_x_id -- \
  --exact --test-threads=1
