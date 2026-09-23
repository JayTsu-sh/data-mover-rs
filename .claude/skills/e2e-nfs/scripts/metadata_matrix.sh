#!/usr/bin/env bash
# ACL / xattr copy-policy matrix against real shares (examples/nfs_metadata_copy.rs).
# Every case asserts its own outcome; exits non-zero if any case fails. Cleans up after itself.
#
# Env (the skill's .env, plus the e2e-cifs .env for the CIFS rung):
#   NFS_META_V41_EXPORT  nfs://host/export?version=4.1&uid=0&gid=0&...   required, writable, NFSv4.1+
#   NFS_META_V3_URL      nfs://host/export:/dir?uid=0&gid=0&...           optional, NFSv3, read-only use
#   NFS_META_V3_FILE     an existing file under NFS_META_V3_URL          required with the above
#   CIFS_REAL_*          from .claude/skills/e2e-cifs/.env               optional CIFS destination rung
set -u
ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
cd "$ROOT"
for env in .claude/skills/e2e-nfs/.env .claude/skills/e2e-cifs/.env; do
  [ -f "$env" ] && { set -a; source "$env"; set +a; }
done
: "${NFS_META_V41_EXPORT:?set NFS_META_V41_EXPORT in .claude/skills/e2e-nfs/.env}"
RUN=${RUN:-$(date +%s)}
V41="${NFS_META_V41_EXPORT%%\?*}:/dm-meta-$RUN?${NFS_META_V41_EXPORT#*\?}"
V41_ROOT="${NFS_META_V41_EXPORT%%\?*}:/?${NFS_META_V41_EXPORT#*\?}"
CIFS="cifs:dm-meta-$RUN"
BIN=target/debug/examples/nfs_metadata_copy
cargo build -q --example nfs_metadata_copy --example storage_role_operations 2>&1 \
  | grep -v -e binrw -e future-incompat -e '^note'
pass=0; fail=0
case_() { # name, expected exit (0|1), grep marker, args...
  local name=$1 want=$2 marker=$3; shift 3
  out=$("$BIN" "$@" 2>&1); code=$?
  if { [ "$want" = 0 ] && [ $code = 0 ]; } || { [ "$want" = 1 ] && [ $code != 0 ]; }; then
    if printf '%s' "$out" | grep -q -- "$marker"; then echo "PASS $name"; pass=$((pass+1)); return; fi
  fi
  echo "FAIL $name (exit $code, want marker '$marker')"; printf '%s\n' "$out" | sed 's/^/    /'; fail=$((fail+1))
}
echo "run=$RUN"
if [ -n "${NFS_META_V3_URL:-}" ]; then
case_ "v3->v4.1 acl+xattrs asked: copied, both skipped (source cannot read)" 0 "copied " \
  --source "$NFS_META_V3_URL" --source-path "$NFS_META_V3_FILE" --destination "$V41" --destination-path v3-asked \
  --acl --xattrs --expect copied --expect-acl unsupported --expect-xattrs unsupported
else echo "SKIP NFSv3 rung (NFS_META_V3_URL unset)"; fi
case_ "v4.1->v4.1 acl asked, principal mark: carried" 0 "ACLs equal and marked: 4" \
  --source "$V41" --source-path src-principal --seed-bytes 4096 --mark-acl principal \
  --destination "$V41" --destination-path acl-principal --acl --expect copied --expect-acl applied --verify-acl
case_ "v4.1->v4.1 acl asked, EVERYONE@ WRITE_ACL mark: carried" 0 "ACLs equal and marked: 3" \
  --source "$V41" --source-path src-everyone --seed-bytes 4096 --mark-acl everyone-write-acl \
  --destination "$V41" --destination-path acl-everyone --acl --expect copied --expect-acl applied --verify-acl
case_ "negative control: acl not asked loses the mark" 1 "lost the Principal mark" \
  --source "$V41" --source-path src-omit --seed-bytes 4096 --mark-acl principal \
  --destination "$V41" --destination-path omit-principal --expect copied --expect-acl omitted-by-policy --verify-acl
case_ "v4.1->v4.1 xattrs asked: copied, skipped (export did not negotiate named attributes)" 0 "copied 4096" \
  --source "$V41" --source-path src-principal --destination "$V41" --destination-path xattrs-asked \
  --xattrs --expect copied --expect-xattrs unsupported
if [ -n "${CIFS_REAL_SERVER:-}" ]; then
case_ "v4.1->cifs acl asked: copied, skipped (NfsV4 vs Windows SD, no conversion)" 0 "copied 4096" \
  --source "$V41" --source-path src-principal --destination "$CIFS" --destination-path acl-asked \
  --acl --expect copied --expect-acl unsupported
else echo "SKIP CIFS rung (CIFS_REAL_* unset)"; fi
case_ "guard: seeding an existing path is refused" 1 "already exists" \
  --source "$V41" --source-path src-principal --seed-bytes 1 --destination "$V41" --destination-path never \
  --expect copied
case_ "guard: copy onto itself is refused" 1 "same file" \
  --source "$V41" --source-path src-principal --destination "$V41" --destination-path src-principal --expect copied
case_ "guard: verify without mark is rejected" 1 "mark-acl" \
  --source "$V41" --source-path src-principal --destination "$V41" --destination-path x --expect copied --verify-acl

# Cleanup: the run directory on each share. Case outcomes above do not depend on it.
NFS_REAL_URL="$V41_ROOT" target/debug/examples/storage_role_operations --backend nfs \
  delete-tree --path "dm-meta-$RUN" --delete-root >/dev/null 2>&1 || echo "WARN NFS cleanup of dm-meta-$RUN failed"
if [ -n "${CIFS_REAL_SERVER:-}" ]; then
  target/debug/examples/storage_role_operations --backend cifs \
    delete-tree --path "dm-meta-$RUN" --delete-root >/dev/null 2>&1 || echo "WARN CIFS cleanup of dm-meta-$RUN failed"
fi
echo "RESULT pass=$pass fail=$fail run=$RUN"
[ "$fail" = 0 ]
