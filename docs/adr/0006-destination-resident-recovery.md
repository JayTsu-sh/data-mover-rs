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

After C5 (2026-09-24, same matrix, no `--identity`: both runs derive the transfer identity) the table
above is unchanged, and on every destination the resumed run derived the interrupted run's identity,
killed runs included. `KEEP_STATE=1` keeps the local records across the restart (still a fresh process
and `HOME`); neither run names the transfer, so a resume can only find its record through the derived
identity — and does:

| Destination | Streamed by the resume, cancel / kill (of 200 MiB) |
|---|---|
| Local (a separate run the same day) | 97.8 / 136.0 MiB |
| NFS (ONTAP, v4.1) | 136.0 / 136.0 MiB |
| CIFS (FAS2750) | 135.6 / 135.2 MiB |
| S3 (MinIO) | 128.0 / 160.0 MiB |

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
   It names "this file goes to that file" for logs, reports and caller records. It includes the
   selector (`Current` or a named version) but not the version `Current` resolves to, so it is stable
   across reschedules and source updates. A caller may override it, which
   opts out of cross-job resume.
   Encoding (C5, `src/transfer/identity.rs`): `blake3("data-mover/transfer-identity/v1\0" ‖ source kind
   ‖ source endpoint ‖ source path ‖ selector ‖ destination kind ‖ destination endpoint ‖ final path)`,
   every variable field prefixed with its u64 little-endian length; paths are literal; the selector is
   `0x00` for `Current` and `0x01 ‖ len(versionId) ‖ versionId` for `Id` (C6c), so adding it changed
   no `Current` identity. An override is
   `blake3("data-mover/transfer-identity/override/v1\0" ‖ len(label) ‖ label)`; the two domains
   differ, so an override can never equal a derived identity. 32 bytes, shown as 64 lowercase hex digits; it holds no
   credentials. Frozen test vectors (`Current`, a named version, `"null"`, a label), computed independently
   of the code, pin the encoding.
3. **Recovery binding**: `TransferIdentity` + the source path and observed identity + size + content
   version + the destination. It is stored inside the destination pointer and decides resume versus restart.
   Binding v3 (C5): `blake3("data-mover/recovery-binding/v3\0" ‖ identity ‖ source path ‖ source
   identity key ‖ size? ‖ content version? ‖ destination kind ‖ destination endpoint ‖ final path)`,
   variable fields length-prefixed, optional ones tagged present/absent (v2 wrote an unknown size as
   `u64::MAX`). The source path and the destination repeat what a derived identity already names,
   because an override label names neither, and the source identity key does not always name the
   entry: an S3 object's is its versionId or ETag, not its key. A frozen vector pins it too.
   v2 bindings never equal v3 ones: a transfer interrupted before the upgrade restarts from zero, and
   what it left — recovery-store `<binding>.state` / `.lock` files, stage files named from the binding,
   S3 stages under `.data-mover-stage/<binding>/` with their incomplete multipart uploads — is orphaned.
   Drain transfers before upgrading.

### Source version selector

`SourceVersion::Current` (default) copies whatever is current at describe time and pins its versionId.
`SourceVersion::Id(versionId)` copies one S3 version (`HEAD`/`GET ?versionId=`; reads, the metadata
HEAD and native copy are pinned to it). The selector is encoded with a tag byte in the identity, so no
versionId can collide with `Current`. Non-S3 sources accept only `Current`; `Id` fails with
`Unsupported` before any destination write. A deleted version is a per-entry `NotFound`.
As built (C6b/C6c): `TransferRequest::with_source_version` (an identity override wins in either
order); the engine checks at preflight that an `Id` is well formed (non-empty, ≤ 1024 bytes, no NUL →
`InvalidInput`) and that the source keeps versions (`Unsupported`). `Current` pins only a real
versionId — an object without one (none, empty, `"null"`) stays guarded by its ETag. `Id("null")` is
sent as is. A delete marker answers a versioned HEAD with 405 and becomes a per-entry `NotFound`; a
store that answers `Id(v)` with another version is refused (`Unsupported`) rather than copied under
the wrong name. Known limit: an id that passes preflight but the store rejects as malformed (MinIO
wants a UUID) answers the versioned HEAD with a bare 400, which carries no code and so keeps the
session-level mapping — S3 answers expired tokens and wrong regions with 400 too. One mistyped id
therefore fails the session, not just its entry. Follow-up: on a versioned HEAD 400, repeat the
request as a ranged GET, whose error body names `InvalidArgument`.
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

As built (C7b): the name digest is the first 16 bytes of `blake3("data-mover/artifact-name/v1\0" ‖
u64le(len) ‖ final name)` in hex (names ≤ 55 bytes, 59 with the fixed `.tmp` temporary); the pointer is
`DMDPTR01` ‖ flags ‖ reserved ‖ u16 extension length ‖ binding ‖ transfer identity ‖ u64 durable prefix
‖ extension (≤ 4 KiB) ‖ `blake3` of everything before (`src/storage/pointer.rs`). Both are on-disk
formats a newer binary must still read, so each carries its version (`v1`, `01`) and has a frozen
vector. The name uses the final name as spelled: on a case-insensitive destination another spelling
of the same file finds nothing and restarts, and the old artifacts wait for the reserved-name cleanup.

As built (C7c, `src/storage/discovery.rs`): the table gains two rows and a fixed precedence — nothing
→ fresh; a requested restart → clean; a corrupt pointer; a pointer without a stage; a stage without a
pointer; another transfer's pointer (`OtherTransfer`, checked before the binding, which hashes the
identity); a changed binding; a stage proving less than the pointer's prefix (`StageBehindPointer`);
otherwise resume. A stage's length in this table is always the prefix it durably proves, never a file
length that may be sparse. A clean-up removes the pointer before the stage, so a crash between the two
leaves a stage without a pointer, which the next prepare cleans. The seam is a transition flag on the
destination (`recovery_at_destination`, `false` until each backend moves) and a separate
`DestinationPrepareRequest`, so `PrepareRequest` and its callers are unchanged until C21.

As built (C7e): the engine asks the destination once per transfer (`recovery_at_destination`); a
destination that says yes is prepared only through `prepare_at_destination`, and no transfer ever
reaches the local recovery store for it (every store call is gated on the stage). Before preparing,
the engine takes an in-process lease on (destination endpoint, final path); a second transfer of the
same file fails at Prepare with `Conflict` / transient, and the stage holds the lease until it is
published or discarded — also inside a failure that keeps it. Only `Checkpointed` with checkpoints
may continue a stage; every other policy asks for a restart. A resumed stage that fails verification
is cleaned up in place (its pointer would otherwise resume it on every retry, across restarts). The
old path reports its facts too: a recovered stage is `Resumed`, a published record without its stage
is `Restarted { PointerWithoutStage }`, an atomic replace that discarded a stage is
`Restarted { Requested }`. `TransferOutcome` gains `prepare` and `reused_bytes`.

As built (C8, Local): the Local destination keeps its recovery state beside the final file:
`.data-mover-<d>.stage`, `.data-mover-<d>.pointer` (the pointer is the checkpoint record: `DMDPTR01`
with a durable prefix and the extension `DMLSTG03`; a pointer without either is refused and cleaned
as corrupt, so a possibly sparse stage length is never a resume offset) and `.data-mover-<d>.claim`,
flock'd from prepare until the stage is published or discarded. Prepare takes the claim before
discovery (a held claim → `Conflict` / transient; after locking, the claim's inode is compared with
the name so a lock on an unlinked claim does not count), opens artifact names only as regular files
(a symlink or directory there → `Conflict`), and on resume truncates the stage to the pointer's prefix
and restores its owner's write permission (metadata applied before a crash may have removed it). The
pointer is written only after the stage's data is synced, through the fixed `.tmp`, which is removed
before an exclusive create so a planted symlink is never written through. Publication renames the
stage over the final file, then removes the pointer, the temporary and the claim, and lets the claim go
only once nothing else can fail; a clean-up by a stage that no longer holds its claim removes nothing.
The claim is per kernel: Local paths on network or drvfs mounts rely on the caller contract and the
in-process lease. The old Local `recover` / DMLRCV01 path is no longer reached (removed in C8d).
A second concurrent transfer of one Local file now fails at `Prepare` (`Conflict`, transient) where
it used to fail at `RecoveryRegistration`.

As built (C9): there is no directory-wide sweep — the claim is per kernel, so another host's live
stage for a different file would look stale, and listing per prepare is quadratic in a large
directory. A Local prepare instead removes, under its claim and by name only, every leftover derived
from its own final name that is not its live stage, pointer or claim (a lone pointer temporary, other
kinds and temporaries). A `Direct` write first looks for the file's stage and pointer (three
`lstat`s); if any is there it runs a restarting prepare under the claim and discards it, reporting
`Restarted { Requested }`. A live stage of another process refuses the direct write with
`Conflict` / transient once its stage file exists (between that process taking its claim and
creating its stage, the caller contract applies); any other failure to clean up is logged and the
in-place write goes ahead, leaving the leftovers. Something that is not a file at a swept name is a
`Conflict`, as at the stage and pointer names. Random-name stages from before C8, and artifacts under another spelling on a
case-insensitive volume, are left to the drain-before-upgrade rule and `delete_tree`.

As built (C10, NFS): the NFS destination keeps `.data-mover-<d>.stage` and `.data-mover-<d>.pointer`
(through the fixed `.tmp`) beside the final file and no claim file: NFS has no lock every host honours.
The pointer's extension is `DMNSTG01` followed by a 16-byte nonce drawn at every prepare; its durable
prefix is written only after the stage handle's `checkpoint()` (COMMIT with a matching verifier, or
FILE_SYNC) proved it. A resume takes the stage over by rewriting the pointer with its own nonce before
it truncates the stage to the prefix. Before a later pointer rewrite, the publication rename, and a
clean-up, a stage reads the pointer back; another nonce, or no pointer after it wrote one, is a
`Conflict` (permanent: retrying would take the stage back and forth) and the names are left alone.
The fence is a check, not a lock, and a stage prepared without a pointer has nothing to fence with
until its first checkpoint; both are two writers of one key, which the caller contract excludes, and
read-back verification catches mixed bytes. A lost RENAME reply is settled by reading back (pointer)
or by the final file's content (publication). `rmdir` and `rename` now make the directory-handle
cache forget the path and everything below it (C10a). Verified on ONTAP NFSv4.1 (contract and resume
matrix); NFSv3 on real hardware is still open. The old NFS random-name stage, `DMNCKP01` checkpoint,
`DMNRCV03` recover and claim rename are no longer reached (removed in C10d).

As built (C11, CIFS): the same design as NFS — `.data-mover-<d>.stage` and `.pointer` beside the
final file, no claim file, a `DMCSTG01 ‖ nonce` fence. The smb-rs facade offers no share-mode or lease
control, so a server-enforced claim (a handle opened without sharing, dying with its session) is a
follow-up that needs a new facade API. The pointer records only flushed bytes (both writers FLUSH
before it, whether or not publication asks for durability). The facade cannot shorten a file, so a
resume does not truncate: the writer rewrites from the prefix to the source's size. A stage can only
be longer than that if something outside data-mover wrote to it (the binding pins the source size,
and the writers stop there); with read-back verification such a stage fails as `Corruption` and is
cleaned up, without it the extra tail would be published — a missing safeguard to add (refuse a stage
longer than the source at prepare). CIFS transfers in flight at the upgrade (random-name stages,
`.checkpoint`, `.claim-*`, local store records) are not resumed: drain before upgrading (D6). Artifact names are checked
without following links (`open_metadata`: a reparse point is not a regular file). Final paths with a
backslash, a colon, or an empty, `.`, `..` or artifact segment are refused (C11a). Verified on the
FAS2750: e2e-cifs twice and the resume matrix (192 MiB, cancel and SIGKILL, local state wiped).
The old CIFS random-name stage, `DMCCKP01` checkpoint, `data-mover:cifs-recovery:v1` recover and
claim rename are no longer reached (removed in C11d).

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
  identity argument and gains `with_identity_override` / `identity()` (C5) and `with_source_version`
  (C6); `TransferIdentity::new` becomes `from_label`; `ExpertSourceRequest::new` loses the identity and
  `ExpertDestinationRequest::new` derives it (`with_identity_override`, session `identity()`);
  `TransferOutcome` gains `identity` (C5), `prepare`, `reused_bytes`, `destination_version`; the two recovery `TransferPhase`
  variants go; `DATA_MOVER_RECOVERY_DIR` goes.
- Storage roles (C6b): `SourceVersion` (`model`), `ReadRequest.version` (a public field, so every
  literal names it), `ReadSource::supports_source_versions` / `describe_version`,
  `Metadata::observe_copy_bound_version`, `SourceDescriptor::version()` (the version the describe
  pinned). Sources without versions keep the defaults and refuse `Id` at describe, read and metadata
  observation. S3 describes with the `ETag` as the content version, so every S3-source binding changes
  once (covered by the C5 drain). **`Current` now pins**: on a versioned bucket a transfer finishes
  with the version it described even if a newer one appears, where a later read used to fail with
  `Conflict`; objects without a real version (none, empty, `"null"`) are not pinned and behave as
  before. The expert halves copy `Current` only.
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
