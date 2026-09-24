# ADR-0006: Destination-resident recovery, derived identities, and S3 without rename

Status: accepted 2026-09-24; implementation in progress (commit sequence C1–C22 below).
Supersedes the recovery-store parts of [ADR-0001](0001-local-transfer-execution-and-recovery.md)
and of the architecture document's §10 once C21 lands.

## Context

- data-mover runs in non-persistent containers. The engine's recovery store
  (`src/transfer/recovery_store.rs`: `<binding>.state` record, `<binding>.lock` lease,
  `.lease-namespace.lock`, rooted at `DATA_MOVER_RECOVERY_DIR` / `XDG_STATE_HOME` / `HOME`) is lost
  on every restart, so no backend can resume after one, and the stages it pointed at become orphans.
- Stage names carry a random UUID (`storage/artifacts.rs`), so a stage cannot be found without that
  local record.
- `BackendIdentity::stable_id` is a caller label (`"source"`, `"destination"`, `"fixture"`), so the
  recovery binding is not reproducible and the same export gets different keys in different roles.
- The S3 destination stages to a central `<prefix>/.data-mover-stage/<binding>/<hash>` key and publishes
  by server-side copy; small objects pay a full multipart upload plus a copy (C0 baseline,
  `.claude/docs/storage-s3.md`).
- Versioned S3 buckets need exact per-version handling on both ends.

### Baseline before the change (C2, 2026-09-24)

`.claude/skills/_shared/resume_matrix.sh` with 200 MiB, the first run limited to 20 MiB/s and cut at
6 s (cancel, then SIGKILL) — about 100 MiB streamed, one local recovery record written — then every
local file deleted and the transfer resumed from a fresh process with a fresh `HOME`, read-back off.

| Destination | Resume | Source bytes streamed by the resume | Left at the destination per interruption |
|---|---|---|---|
| Local | ok | 200 MiB of 200 MiB | stage + checkpoint + claim |
| NFS (ONTAP, v4.1) | ok | 200 MiB of 200 MiB | stage + checkpoint |
| CIFS (FAS2750) | ok | 200 MiB of 200 MiB | stage + checkpoint |
| S3 (MinIO `RELEASE.2023-03-20`) | ok | 200 MiB of 200 MiB | one open multipart upload |

Nothing is reused and the leftovers accumulate. Local, NFS and CIFS leftovers are `.data-mover-*`
siblings a directory listing finds; the S3 upload is not an object and MinIO lists uploads only by
exact key, so only the lost local record named it. HDFS runs on the lab runner and is recorded with C12.

## Decision

### Nothing is kept where data-mover runs

Everything a resume needs lives at the destination, next to the final file. The recovery store,
its lease, the `Publishing` state, `recover` / `RecoverRequest` / `RecoveryIdentity` /
`claim_token` and `DATA_MOVER_RECOVERY_DIR` are removed.

### Three identities

1. **Endpoint identity**, derived by data-mover from each `BackendConfig` (callers no longer supply a
   label; `stable_id` becomes this value). Protocol + address + share or bucket + optional prefix, never
   credentials:
   `nfs://host[:port]/export[/prefix]` (NFS version and options excluded), `smb://server[:port]/share[/prefix]`,
   `s3://endpoint[:port]/bucket[/prefix]` (keys, TLS and compatibility profile excluded),
   `hdfs://nameservice|namenode:port[/prefix]`, `file:///canonical-path` (no hostname).
   Normalized: DNS hosts lowercased, IPv6 in canonical `[addr]` form, default ports dropped, path names
   percent-encoded. An IP address and a hostname for the same server are different endpoints.
   Per-scheme rules (`src/storage/endpoint.rs`, C4a): NFS export and prefix are read the way the backend
   uses them (the export as nfs-rs parses its URL, the prefix literally with `..` confined to the export);
   SMB server and share are case-insensitive; **S3 prefixes are literal** — keys are not paths, so `a//`,
   `a/` and `/a/` stay distinct and only the backend's trailing `/` is removed — and http/https and ports
   80/443 are one endpoint; **HDFS keeps its port** (no port marks a NameService, whose name keeps its
   case); Local is the realpath, so a volume mounted at another path is another endpoint.
   `storage::endpoint_identity(&BackendConfig)` derives it without connecting (C4b).
2. **TransferIdentity**, derived by default:
   `blake3(source endpoint, source path, source version selector, destination endpoint, final path)`.
   It names "this file goes to that file" for logs, reports and caller records, and excludes the source
   version, so it is stable across reschedules and source updates. A caller may override it, which
   opts out of cross-job resume.
3. **Recovery binding**: `TransferIdentity` + the observed source identity/version + size + content
   version. It is stored inside the destination pointer and decides resume versus restart.

### Source version selector

`SourceVersion::Current` (default) copies whatever is current at describe time and pins its versionId.
`SourceVersion::Id(versionId)` copies one S3 version (`HEAD`/`GET ?versionId=`; reads, the metadata
HEAD and native copy are pinned to it). The selector is encoded with a tag byte in the identity, so no
versionId can collide with `Current`. Non-S3 sources accept only `Current`; `Id` fails with
`Unsupported` before any destination write. A deleted version is a per-entry `NotFound`.
History migration (documented, not a feature): the caller submits `Id` transfers oldest to newest,
one at a time; each creates one destination version; delete-marker replication is the caller's choice.

### Destination artifacts

Temporary files and pointers sit in the final file's parent directory, named only from the final file
name: `.data-mover-<hash(final name)>.{stage|checkpoint|claim|pointer|upload}`, no UUID. The pointer
(for Local/NFS/CIFS a new checkpoint record version, for HDFS a `.pointer` side file, for S3 an
`.upload` object) holds the full binding. At prepare the engine asks the destination, with no local
state:

| Found at the destination | Action |
|---|---|
| pointer and stage, binding equal | resume from the re-observed durable prefix |
| binding differs, pointer corrupt, or restart requested | clean up in place, start from zero |
| pointer without stage (probably published) | delete the pointer, start from zero |
| stage without pointer | delete the stage, start from zero |

The outcome reports `Fresh`, `Resumed { bytes }` or `Restarted { reason }`. Exclusivity rests on the
caller contract that one destination key is never written by two transfers at once, plus an in-process
per-key guard; Local keeps its flock claim and HDFS its lease. NFS/CIFS claim renames and HDFS
`.claimed` files are removed.

### S3 destination

- Objects up to the threshold T (default 8 MiB, configurable within [5 MiB, 5 GiB]) are one
  `PutObject` to the final key with Content-MD5, no resume.
- Larger objects are a multipart upload on the final key, part size `max(8 MiB, ceil(size / 10000))`,
  at most 5 GiB per part and 10 000 parts (both measured on MinIO), Content-MD5 per part. A
  resume pointer is written only for `Checkpointed` objects over 64 MiB. Publication is
  `CompleteMultipartUpload` with a composite-ETag check; discard is Abort; the final key is never
  deleted. Orphan uploads on the final key are aborted before a new one starts.
- Read-back verification moves after publication (`verification_point()`): by our versionId in
  versioned buckets, otherwise with `If-Match` on our ETag; if our version is not current the transfer
  reports `Conflict` and `final_destination_changed`.
- `Direct`: small objects PUT, large objects complete inside `write` and abort there on failure.
- Native S3→S3: up to 64 MiB one `CopyObject` to the final key, above it `UploadPartCopy` (64 MiB
  parts, 6 in parallel), completed at publication.
- Versioned buckets: pointers are deleted by their own versionId, no unversioned DELETE is ever sent
  to a final key, an ambiguous Complete is reconciled via `ListObjectVersions`, an Object Lock refusal
  to delete a pointer is a warning, and the outcome carries the destination versionId.

### S3 traversal and versions

Both the legacy listing and the role-based S3 traversal support versioned buckets. The role-based
traversal gets a version mode: `Current` (default; delete marker = absent) or `All` (every version
with versionId, latest flag and delete-marker flag, oldest to newest per key). Both filter
`.data-mover-*` names.

## Consequences

- Public API breaks (terrasync adapts): configs lose `identity`; `TransferRequest::new` loses the
  identity argument and gains `with_source_version` / `with_identity_override`; `TransferOutcome` gains
  `identity`, `prepare`, `reused_bytes`, `destination_version`; the two recovery `TransferPhase`
  variants go; `DATA_MOVER_RECOVERY_DIR` goes.
- Configs lose `identity`; `S3Storage::architecture_storage` / `HDFSStorage::architecture_storage` /
  `nfs::create_nfs_role_storage` lose their identity argument and `cifs::create_cifs_role_storage` is
  crate-private (C4b). `BackendIdentity::new` stays public for fixtures and snapshot decoding.
- Observation snapshots stop storing the backend identity (format v5, C4c): every entry of a scan shares
  it, so `ObservedEntry::decode_snapshot(bytes, &BackendIdentity)` takes it from the caller
  (`Storage::identity()` or `storage::endpoint_identity`). A 4-byte endpoint fingerprint makes another
  endpoint or kind fail with `BackendMismatch` (for a whole generation: the endpoint moved or is spelled
  differently — no previous generation), distinct from a corrupted key (`IdentityMismatch`). v4 snapshots
  still decode with the identity they carry (kind checked); re-encoding such an entry keeps that old
  identity, so callers copy v4 bytes forward. Measured on `m1-source/d000/d000` (134,735 NFS entries):
  217.3 B per entry in v5, 255.3 B with the endpoint stored, 223.3 B before C4.
- `EntryIdentityKey` and observation snapshots change once (derived `stable_id`); terrasync's first
  incremental run after the upgrade is effectively full.
- S3 content becomes visible before read-back; per-part Content-MD5, the part-list check and the
  composite ETag run before publication.
- A crash between publication and pointer deletion costs one re-copy.
- Every listing hides `.data-mover-*` names (any path segment), so a user object named that way is no
  longer seen as a source entry; the same name must be hidden on every backend, or a mirror could treat
  it as an extra destination file.
- Stages in flight in the old format are not recognized: drain transfers before upgrading; manual
  cleanup of `.data-mover-*` stages, `.claim-*` names and S3 `.data-mover-stage/` is documented.

## Commit sequence

C1 this ADR · C2 cross-backend resume example and container-restart matrix, baseline · C3 legacy S3
listing filters `.data-mover-*` · C3b the same for the legacy Local / NFS / HDFS listings and the CIFS /
HDFS role-based namespace and traversal (one shared `ARTIFACT_PREFIX`; directory deletes still remove
artifacts — legacy NFS walks them, role Local / CIFS sweep an artifact-only directory) · C4 endpoint identity (C4a
derivation, C4b wiring, C4c snapshots without the identity) · C5 TransferIdentity and binding v3 · C6 source
version selector (C6a: native identity ignores the `"null"` version) · C7 destination discovery seam ·
C8 Local · C9 Local reserved-name cleanup · C10 NFS · C11 CIFS · C12 HDFS · C13 verification point ·
C14 S3 small objects, threshold, Direct · C15 S3 multipart on the final key · C16 resume granularity ·
C17 S3 versioning · C18 native copy to the final key · C19 remove the temp-key path · C20 history
migration docs · C21 remove the recovery store · C22 role-based S3 traversal with version modes.
Every commit passes `make ci`; each backend is verified on real systems with an interrupted transfer
resumed from a fresh process with no local state.
