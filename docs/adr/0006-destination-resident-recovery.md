# ADR-0006: Destination-resident recovery, derived identities, and S3 without rename

Status: accepted 2026-09-24; implementation in progress (commit sequence C1–C22 below).
Supersedes the recovery-store parts of [ADR-0001](0001-local-transfer-execution-and-recovery.md)
and of the architecture document's §10 (the store was removed in C21).

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
As built (C20): the procedure and its limits are written down in the architecture document ("S3
version history migration") and `.claude/docs/storage-s3.md`. The legacy listing already carried
the versionId, latest flag and delete-marker flag on every `S3Entry` (public fields and `EntryEnum`
getters) and filtered `.data-mover-*` names (C3), but it grouped each `ListObjectVersions` page on
its own: a key split across two pages (1 000 entries a page) came out as two groups — newest part
first, a per-page `version_count`, and in `walkdir_2` two entries marked latest. Both listings now
hold back the page's greatest key until the listing moves past it (not `NextKeyMarker`: MinIO
answers it with a token such as `p/z[minio_cache:v2,return:]`) and order each key oldest first,
entries of one millisecond by listing order with the latest last (`src/s3/version_listing.rs`).
Unchanged: a key whose newest entry is a delete marker is not listed at all, and a bucket with
versioning suspended is listed without versions. Verified on MinIO (VM 102, temporary bucket
`data-mover-c20-<run>`, deleted afterwards): `v1, v2, marker, v3` lists as those four in order in
`walkdir` (versions only in `walkdir_2`), a deleted key and an artifact are absent, and a key whose
three versions straddle the first page boundary (999 keys before it) lists as `z1, z2, z3`,
`count=3`, one latest (before: `z3, z1, z2`). `versioning_matrix.sh` (temporary buckets, deleted)
unchanged: v1 then v2 by `--source-version`, streamed and native, each gave two versions in order
equal to their sources. Follow-up (older than C20): a `ListObjectVersions` / `ListObjectsV2` page
that fails in `walkdir` is only logged — the walk ends that prefix without an error message, so a
caller enumerating history cannot tell a short listing from a complete one (`walkdir_2` reports it
in the page's errors).

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
`DestinationPrepareRequest`, so `PrepareRequest` and its callers are unchanged until C21 (which kept
`PrepareRequest` as it is: `prepare_direct` takes it and `DestinationPrepareRequest` wraps it).

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

As built (C12a): the decision table gains two rules for every backend. A stage longer than the source
(`prepare.source.size`, when known) is cleaned up as `Restarted { StageBeyondSource }` — a row after
the binding check and before `StageBehindPointer`; the binding
pins the source's size and a writer stops there, so only an outside writer makes it longer. And a
backend may opt in (`continues_from_stage`) to resuming from the length its stage proves rather than
from the pointer's prefix, which stays a lower bound (`StageBehindPointer` still applies): only for a
stage whose length is itself proven and that cannot be shortened — HDFS after lease recovery.

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
and the writers stop there); such a stage is cleaned up at prepare (`Restarted { StageBeyondSource }`,
a decision-table row added in C12a for every backend) instead of being resumed. CIFS transfers in flight at the upgrade (random-name stages,
`.checkpoint`, `.claim-*`, local store records) are not resumed: drain before upgrading (D6). Artifact names are checked
without following links (`open_metadata`: a reparse point is not a regular file). Final paths with a
backslash, a colon, or an empty, `.`, `..` or artifact segment are refused (C11a). Verified on the
FAS2750: e2e-cifs twice and the resume matrix (192 MiB, cancel and SIGKILL, local state wiped).
The old CIFS random-name stage, `DMCCKP01` checkpoint, `data-mover:cifs-recovery:v1` recover and
claim rename are no longer reached (removed in C11d).

As built (C12b, HDFS — switch still off): `.data-mover-<d>.stage` and `.pointer` beside the final file,
no claim file, a `DMHSTG01 ‖ nonce` fence as on NFS and CIFS. The HDFS lease cannot stand in for a
claim — it exists only while a writer has the file open and belongs to the process, not the
transfer. The pointer records the last hsync'd prefix as a lower bound; a resume continues from the
stage's length after forced lease recovery (`continues_from_stage`, C12a), which also fences the dead
writer. Lease recovery runs only for a stage the same discovery is going to resume (its pointer
decodes and matches this transfer), never before a clean-up. A lost publication reply counts as done
only when the stage is gone and the final file has the expected size and BLAKE3. Real-machine
verification (resume matrix on the Kerberos lab through the runner) is still to do; the switch
stays off until it passes (C12c).

As built (C12c, HDFS — switch on, breaking): the HDFS destination answers `recovery_at_destination()`,
so every non-`Direct` HDFS transfer goes through the in-process lease and `prepare_at_destination`,
and nothing is recorded where data-mover runs; `Direct` takes the lease and goes through
`prepare_direct`. The engine marks a `Direct` stage at-destination too; HDFS keeps
its `.stage` / `.pointer` paths only for a resident stage (`at_destination && !direct`), so a direct
write still goes to the final path. The old `prepare` / `recover` / `recovery_identity` stay until
C12d removes them. Breaking: HDFS transfers in flight at the upgrade (`.part` / `.claimed` names
and local records) are not resumed — drain first (D6); their `.part` / `.claimed` files stay on
HDFS, hidden from listings, until removed by hand. A failed pointer write at the first checkpoint
fails the transfer with no recoverable stage; the stage it leaves has no pointer, and the next
transfer cleans it up and starts over (`a_failed_first_pointer_leaves_nothing_to_resume`). Verified on the Kerberos lab (runner VM 102,
principal `hdfs/terrasync-runner`, NameNode 10.131.9.30:9000, run root
`/tmp/data-mover-nightly/nightly-c12c-<ts>/hdfs`, removed afterwards): the resume matrix (200 MiB,
20 MiB/s, 6 s cut, local state wiped) — cancel left no local record and two artifacts, resumed
`Resumed { 106326012 }` with 103389188 streamed; SIGKILL resumed `Resumed { 106267648 }` with
103447552 streamed; both equal by BLAKE3 with no artifact left. `hdfs_architecture_contract` (3) and
the HDFS smoke suite (19 `nightly_lab_*`, including the engine policy and chunk-boundary cases) pass.
The old HDFS random `.part` stage, `hdfs-recovery-v1` recovery identity, `.claimed` claim rename and
the writer's store registration are no longer reached (removed in C12d).

As built (C13): a destination says when read-back verification reads it
(`StagedDestination::verification_point(stage)`: `BeforePublish` by default, `AfterPublish` for one
that writes at the final name — S3 from C14). For `AfterPublish` the engine applies metadata,
publishes, then verifies the final object; a failure there — including cancellation — cannot be
undone and is reported as a `Verify` failure with `final_destination_changed` and no stage. The
expert destination half does the same. `PublicationEvidence` gains `version` (and becomes
`#[non_exhaustive]`), and `TransferOutcome` gains `destination_version`.

As built (C14b, S3 small objects): `S3BackendConfig.single_put_threshold` (`None` = 8 MiB; outside
[5 MiB, 5 GiB] an S3 `BackendConnectError` "invalid configuration" at connect, before any network
I/O). A source of known size ≤ T gets a
"single" stage: prepare starts no upload, the stage's state (buffer, pending tags, write facts) lives
in `PreparedStage::backend_state` rather than the adapter's stage map, so an abandoned stage frees its
bytes, and recovery is off (`disable_recovery`). The engine then releases any local recovery record
for the binding instead of registering one (`EffectiveRecovery::SkippedBelowCheckpointThreshold`); a
single-stage identity handed to `recover` anyway starts a fresh single stage. `write` buffers (more
than the source size is `InvalidInput`); `publish` is one `PutObject` with Content-MD5 to the final
key. A definite refusal (`BadDigest` → transient `Corruption`, 4xx) leaves the final unchanged; any
other failure is settled by HEAD — our size and our MD5 `ETag` count as published (facts from the
HEAD), otherwise the failure reports `final_destination_changed`. Tags applied to the stage are set
on the object right after the PUT. `verification_point` is `AfterPublish`; verify HEADs the current
object (another `ETag` or version → `Conflict`), then reads it by our versionId, or with `If-Match` on
our `ETag` when there is none. Discard of an unpublished single stage touches nothing. Sizes above T,
unknown sizes and native S3→S3 copies (a single stage handed to the native path moves to the temp
key) keep the temp-key multipart path until C15/C18 (both done: C15c, and C18 for native copies);
`Direct` stays refused until C14c.

As built (C14c, S3 `Direct`): the S3 destination supports `Direct` (the engine's generic
`supports_direct` check now passes; native copies are never used for `Direct`). `prepare_direct`
refuses an object that is its own source (`Conflict`), then gives a known size ≤ T a single stage
and anything else a multipart stage, both marked `direct`, without recovery or durable publication.
`write` does the whole job at the final key: a single stage buffers and sends its `PutObject`
(C14b's send, reconciliation included); a multipart stage aborts the uploads an earlier writer left
on the final key (`list_uploads`, best effort), begins its upload there — inside `write`, so a
transfer failing before it writes leaves no upload — sends every part with `Content-MD5`, and
completes. When every part came back with its MD5 as `ETag` (not so under SSE-KMS) and the
completion `ETag` has the multipart form, it must equal the composite of the part `ETag`s (else a
permanent `Corruption`; MinIO matches). Any failed completion — a lost reply, or a retried one
answered `NoSuchUpload` — counts as done when the final object has our size and that composite.
Any failure aborts the upload inside `write` (tried twice: the engine keeps no failed `Direct`
stage to discard, so an upload that still cannot be aborted is left to the next `Direct` write of
the key, or to a lifecycle rule). `publish` only returns
the write's facts (`PublicationEvidence.version`), `verification_point` is `AfterPublish` and
verify reads the final object pinned by version or `If-Match` (C14b's code, generalised);
`discard` aborts an upload still open and never deletes the final key. Metadata goes to the final
object through the metadata role.

As built (C15a, multipart building blocks — no behaviour change): `upload_part` sends `Content-MD5`
(a mismatch is `BadDigest`, a transient `Corruption`); `complete_multipart` reports the object's
`ETag` and version (`S3WriteFacts`); `list_uploads(key)` lists the uploads in progress on exactly
that key (`ListMultipartUploads` with the key as prefix, then an exact-key filter — MinIO lists
exact keys only, AWS / Ceph / StorageGRID by prefix). `composite_etag` computes
`"<md5 of the binary part MD5s>-<n>"`, or `None` when a part `ETag` is not a quoted 32-hex MD5
(SSE-KMS). `InvalidPart` / `InvalidPartOrder` map to a permanent `Conflict`, `EntityTooSmall` to a
permanent `Corruption`. The temp-key path passes part MD5s and ignores the completion's facts.

As built (C15b, S3 multipart on the final key — switch still off): `prepare_at_destination` gives an
object of known size ≤ T a C14b single stage after looking only at the pointer (HEAD, then GET if one
is there: a leftover is deleted with every upload on the key and reported as `Restarted` — `Requested`
for a restart request or a pointer of this binding, else `PointerCorrupt` / `OtherTransfer` /
`BindingChanged`; no upload listing, for cost). A larger or unknown-size object runs `discover` and
gets a multipart upload **on the final key**. Its pointer is the `.data-mover-<d>.upload` object beside
the key: `DMDPTR01` **without a durable prefix** (the service's `ListParts` is the durable record, and a
pointer rewritten at every checkpoint would leave a version each time in a versioned bucket) with the
extension `DMS3UP01 ‖ nonce16 ‖ u64le part size ‖ u16le len ‖ upload id` (upload id non-empty UTF-8,
part size in [5 MiB, 5 GiB] and able to hold the source in 10 000 parts; anything else is refused and
cleaned as `PointerCorrupt`). It is written with one `PutObject` carrying `Content-MD5` — atomic, no
temporary; a failed PUT whose object reads back byte for byte counts as written — read with a HEAD and
a ranged GET pinned to it, and deleted with `DeleteObject`. `observe_stage` with an accepted pointer is
`ListParts` of its upload (`NoSuchUpload` → no stage) reduced to the **contiguous prefix**: parts
`1..=k`, each exactly the pointer's part size and within the source's size, the last one shorter only
if it ends exactly at the source's size, stopping at the source's size. **A gap is not corruption**
(parts complete out of order, so a killed writer leaves some; the temp-key path still treats a gap as a
permanent `Corruption` and aborts): the parts after it are uploaded again, and uploading a part number
again replaces it; a part that would pass the source's end is re-uploaded too, so `StageBeyondSource`
cannot arise. Without an accepted pointer the stage is "any upload listed on the exact key"
(`StageWithoutPointer` cleans it); `remove_stage` aborts every upload on the key. `continues_from_stage`
stays false: a pointer without a durable prefix already resumes from what the stage proves. A resume
rewrites the pointer with a new nonce (take-over) and then aborts any other upload on the key; the
stage's state (upload id, part size, the prefix's (number, `ETag`)s, fence, tags, completion facts)
lives in `PreparedStage::backend_state`. A fresh or restarted stage begins its upload after discovery
cleaned the key; a `recoverable` one writes its pointer at once (since C16 so does a resumable one of
known size over the interval), any other at its first deferred
checkpoint — the first time the parts the service acknowledged in this `write` reach the interval —
which also turns its recovery on. S3 declares the 64 MiB automatic interval only while the switch is
on, so a checkpointed transfer of at most 64 MiB never writes a pointer (D3); with the switch off the
store path's planning (register from the start) is unchanged. `write` streams the parts after the
prefix and does not complete. `verification_point` is `AfterPublish`. `publish` checks the fence (the
pointer must carry our nonce, or be absent if we never wrote one; otherwise a permanent `Conflict`, the
final key unchanged) and completes with every (number, `ETag`); the composite check runs when every
part this `write` sent came back with its MD5 (the parts of one upload share its encryption, so the
resumed prefix answers the same way; with no part sent there is nothing to check). A failed completion
is settled by `ListParts`: still listed → nothing completed (`final_destination_changed` false, stage
kept); `NoSuchUpload` → our size and composite `ETag` at the final key count as published (no version
claimed, as C14c), anything else is a `Conflict` with the final key changed. Tags applied before
publication are set after the completion; then the pointer is deleted (if we found ours there) and
the evidence carries the completion's version. Verify reads back pinned by version or `If-Match` (E1).
`discard` checks the fence and, while the upload is still ours, deletes the pointer and then aborts
the upload; it never touches the final key. A native copy refuses an upload on the final key
(`Unsupported`) until C18 (C18 fills it with `UploadPartCopy`), and such a stage has no local recovery
identity. The fence is a check, not a
lock, as on NFS / CIFS / HDFS. Real-machine verification (the 200 MiB cancel / SIGKILL matrix) comes
with C15c, which turns the switch on.

As built (C15c, S3 — switch on, breaking): every S3 connection answers `recovery_at_destination()`
with `true`, so an S3 destination is prepared only through `prepare_at_destination` under the
in-process lease, nothing is recorded where data-mover runs, and the automatic interval is 64 MiB
(D3). The temp-key path stays reachable only from tests (`with_recovery_at_destination(false)`) until
C19. Four fixes came first. (1) A native S3→S3 pair on the at-destination route takes the
destination's ordinary ephemeral prepare (the temp key, copied to the final key at publication),
marked at-destination and `Fresh` and holding the lease: a native copy cannot fill an upload on the
final key until C18 (C18 replaced this branch: see As built (C18)), and every native copy over T would
otherwise fail `Unsupported`; a native plan
keeps no recovery state, so nothing is recorded anywhere, and leftovers of an earlier streaming
attempt on the key wait for that key's next streaming prepare. (2) A fresh upload writes its pointer
at prepare only when it is `recoverable` **and** its known size exceeds the automatic interval (or is
unknown); a smaller one is not resumable. The expert destination half asks for a recoverable prepare
of every checkpointed object over one chunk, so without this it wrote a pointer for every object over
T. (3) A failed completion leaves the final key unchanged only when the service refused it before it
could commit — a definite 4xx refusal (`InvalidPart`, `InvalidPartOrder`, `EntityTooSmall`, access,
signature), never `NoSuchUpload`, which also answers a retried completion that already committed. Any
other failure (a reset, a timeout, a 5xx) reports `final_destination_changed` even while `ListParts`
still lists the upload, because a completion whose reply timed out can still finish on the server
(a later discard's abort then gets `NoSuchUpload`, which clean-up takes as done). The `NoSuchUpload`
reconciliation is unchanged. A failed `Direct` write already reports the final key changed (the
engine marks every failed direct stage so). (4) MinIO issues an upload id as
base64url(`<deployment id>.<uuid>`) but lists the bare uuid (RELEASE.2023-03-20) and accepts either:
the resume's "abort every other upload on the key" compared the two byte for byte and aborted its own
upload, so every resume failed with `NoSuchUpload` on MinIO; ids are now compared under both
spellings (`same_upload`). Breaking: S3 transfers in flight on the temp-key path at the upgrade
(`.data-mover-stage/` objects and local store records) are not resumed — drain first (D6); in a
versioned destination bucket each object over 64 MiB leaves a pointer version and a delete marker
until C17. Every S3 transfer now takes the engine's per-file lease, so two transfers of one key in a
process fail at `Prepare` (`Conflict`, transient). Verified on MinIO (VM 102): the resume matrix (200
MiB, 20 MiB/s, `CUT_MS=12000`, local state wiped) — cancel and SIGKILL both left no local record, one
pointer and one open upload, resumed `Resumed { 150994944 }` / `Resumed { 125829120 }` with reused +
streamed = 200 MiB, equal BLAKE3, and nothing left. At the default 6 s cut the service had
acknowledged less than 64 MiB (the checkpoint counts acknowledged parts, up to four parts behind the
reads), so both restarted as `StageWithoutPointer`.

As built (C16, S3 resume granularity): measured on MinIO after C15c (200 MiB, 20 MiB/s), what a cut
loses. A cancellation loses less than one part: 161061888 bytes streamed at a 12 s cut, 159383552
(19 parts) resumed — the parts in flight finish. A SIGKILL loses the parts in flight (up to four 8 MiB
parts plus the one being read): 9 s → 75497472 resumed, 12 s → 117440512. Both are what the parts
allow. The loss that was not: a writer killed before its first checkpoint — the first 64 MiB the
service acknowledged, which at a slow source can take minutes — left an upload without a pointer,
which the next prepare could only abort (`StageWithoutPointer`), so a container restarted in that
window started over. A fresh upload that may be resumed (`ResumeMode::Discover`: the engine arms a
deferred checkpoint for it) and whose known size is over the automatic interval now writes its pointer
when it begins, like a `recoverable` one. Which objects get a pointer is unchanged (D3: checkpointed
objects over 64 MiB, one pointer `PutObject` each), and the stage's recovery still turns on only at
that checkpoint: before it, a failure reports no recoverable stage and a discard deletes the pointer
and aborts the upload as before; a failure that is not discarded (a writer that dies there, or a
caller that drops the failure) leaves the pointer, and the next prepare resumes from the listed parts.
The upload is also fenced from the start. Consequences: such an undiscarded failure leaves a visible
`.data-mover-*.upload` object even with no part sent (a bucket lifecycle rule aborts the upload, the
pointer goes at the key's next prepare as `PointerWithoutStage`), and in a versioned bucket a failure
before the first checkpoint that is discarded now leaves a pointer version and a delete marker too
(until C17). At a 6 s cut (where C15c
restarted from zero) MinIO now resumed 75497472 bytes after a cancellation and 41943040 after a
SIGKILL, BLAKE3 equal, nothing left.

As built (C17, S3 versioning): the S3 protocol gains `delete_version` (`DeleteObject` with a
`versionId`: the version is gone for good, no delete marker; a version the store does not hold is
fine; Object Lock refusing it is a permanent `PermissionDenied` of the entry — AWS answers 403
`AccessDenied`, MinIO 400 `InvalidRequest` "Object is WORM protected"; a 405 is `Unsupported`, not
the delete-marker `NotFound` a versioned read gets) and `list_versions`
(`ListObjectVersions` with the key as prefix, exact-key entries only). A write keeps the version id as
the response spelled it (`S3WriteFacts.reported_version`, `"null"` included). The `.upload` pointer is
deleted by the version its PUT reported; a PUT whose reply was lost and whose object reads back byte
for byte takes the version the read-back HEAD saw (the bytes carry this prepare's nonce). Discovery
deletes a leftover pointer by the version it read, and a resume, once its own pointer is written,
deletes the version it replaced — otherwise that one would become current again when ours goes; if
that fails (logged), its own pointer is later hidden with a plain delete instead, one marker covering
both. A replaced `"null"` is left alone when our write reported no real version: it was the one
`"null"` version, which our write overwrote. After any delete by id the pointer is read again: the
SDK's retry of a `PutObject` that committed but lost its reply leaves a byte-identical version below
the one we know, so a current pointer holding the bytes just deleted (or those a resume replaced) is
deleted by its version too, at most four times; another writer's pointer is left alone. `"null"` (suspended versioning) is deleted as `versionId=null`; a bucket that reports
no version keeps the plain `DeleteObject`. A pointer version that cannot be deleted by id (Object Lock,
a policy without `DeleteObjectVersion`, a store without delete-by-version, a refused `"null"`) is hidden
behind a delete marker with a warning: the transfer
still succeeds and reports its version, and the next prepare finds no pointer. No path sends an
unversioned `DeleteObject` to a final key (only the pointer and the temp key are ever deleted so); the
in-memory S3 logs plain deletes and the versioning tests assert none reached a final key. A completion
that was not refused before it could commit and whose upload `ListParts` reports gone is settled
through the key's versions: the **latest** entry must be a version with our size and composite `ETag`,
and its version is claimed — under the caller contract (one writer per key) the gone upload committed
and nothing was written after it (the residual cases: anything that aborts our upload before it
completes — a lifecycle rule, another writer's prepare aborting uploads on the key — leaves an
identical earlier latest version to be claimed, and a later identical write by another writer would
be claimed too; the bytes match either way); a store that cannot list versions, or lists none,
falls back to the HEAD and claims none. `Direct` aborts first: an upload already gone completed and is settled the same way; one that
could still be aborted never completed, so an identical earlier object still counts (C14c) but no
version is claimed. The C15c rule for `final_destination_changed` is unchanged. Suspended versioning
reports `"null"` (MinIO's completion reports no version at all): no `destination_version`. A native
S3→S3 copy still goes through the temp key until C18 (since C18 only with the switch off); the temp key
(this stage's alone) is now deleted
entry by entry as its version listing shows it — the `"null"` version and markers included, and
nothing at all when the listing is empty — so no full-size copy stays behind a marker, and a `CopyObject` that succeeded
reports the final key's current version (one LIST and one HEAD more per native publication, until
C18, which writes the final key and reports the version its own `CopyObject` / completion returned). Still
open: pointer versions and markers left before C17 stay hidden under their markers, and a
single `PutObject` whose reply was lost still claims the HEAD's version (C14b). Verified on MinIO (VM 102) with `versioning_matrix.sh` in two temporary buckets (one
versioned, one with Object Lock; both deleted afterwards, versions, markers, holds and uploads
included; run three times, the last two after review fixes, with the same results): 4 MiB and 200 MiB checkpointed copies, `Direct`
4 MiB / 20 MiB and native S3→S3 4 MiB / 200 MiB each left one version of the key, no delete marker
and no `.data-mover-*` entry, and reported that version; v1 then v2 copied
by `--source-version` (100 MiB each, streamed) gave two versions in that order, each equal to its
source; an `Id` copy cancelled at 3 s resumed `Resumed { 16777216 }` into one version; the resume
matrix (200 MiB, 20 MiB/s, 6 s cut) resumed `Resumed { 75497472 }` after a cancellation and
`Resumed { 41943040 }` after a SIGKILL, BLAKE3 equal, one version, no marker, no artifact; a legal
hold placed on the pointer version during a 200 MiB copy left the transfer successful with its
version, one warning and the pointer behind one marker; with versioning suspended every write path
reported no version and left one `"null"` version.

As built (C18, native S3→S3 to the final key): the engine's at-destination native branch no longer
takes the destination's ephemeral prepare (the temp key). It asks the pair's destination endpoint
(`NativeEndpoint::prepare_native`, crate-private) with a `DestinationPrepareRequest` — `Discover` for a
`Checkpointed` copy, `Restart` otherwise, never `recoverable` — and the S3 endpoint prepares at the
final key, reporting `Fresh`, `Resumed` or `Restarted` like a streamed prepare. The S3 protocol gains
`copy_from` (`CopyObject` pinned by `x-amz-copy-source-if-match` to the bound `ETag` and, when the
binding has one, `?versionId=`; reports the copy's `ETag` and version) and `upload_part_copy`
(`UploadPartCopy` pinned the same way; returns the part's `ETag`). A source of at most 64 MiB is a
single stage that records the source instead of bytes; before it only the pointer is looked at (a
leftover is removed with the uploads on the key, as before a single `PutObject`), and publication is
one `CopyObject` to the final key: a refusal the service answered before copying (a changed source is
412 → `Conflict`, a gone one `NotFound`, access …) leaves the final key unchanged; any other failure is
settled by HEAD — the source's size and `ETag` count as copied, otherwise the final key is reported
changed. The fill counts that copy (its bytes and one request) — the engine takes native counts only
from the fill — so a copy refused at publication still reports them. A larger source is an upload on
the final key prepared through the same discovery as a streamed one: parts of `max(64 MiB,
ceil(size / 10 000))` (5 GiB at most), filled with `UploadPartCopy`, `min(6, InflightLimits.operations)` in flight (the operation bound
reaches the fill through `copy_into_stage`), and completed at
publication by the streamed upload's code — fence, the C15c/C17
rules for a failed or ambiguous completion (`ListObjectVersions`, version claimed), tags, pointer deleted
by version, `AfterPublish` read-back pinned to the completion. The completion is **not** held to the
composite of the part `ETag`s: those are the store's own, not MD5s computed here, so nothing proves
they are MD5s (SSE-KMS), and a mismatch would fail a committed copy as `Corruption`; the read-back
checks the content, and the composite still identifies our object when an ambiguous completion is
settled (MinIO reports each copied part's MD5 and completes with their composite, measured). A
`Checkpointed` copy of known size over
the interval writes its pointer when the upload begins (the C16 rule), and its recovery turns on once
the parts this fill copied reach the interval; an atomic one writes none and is never recoverable. A
cancellation starts no more parts and waits for those in flight, so every copied part counts for the
next attempt; a failed part fails the copy at once; the failure keeps the stage (discard: pointer by
version, then abort). **Part sizes across routes**: a resume continues at the part size the pointer
records, whichever route wrote it — a native copy continues a streamed upload with `UploadPartCopy`
parts of 8 MiB (more requests, no client memory), a streamed attempt continues a native upload with
64 MiB parts, with as many in flight as fit in the bytes of the four parts it would plan itself (at
least one: 128 MiB in all, not 320 MiB; the writer's part buffers are outside `InflightLimits.bytes`,
which its rustdoc now says); no route restarts over a part size, and the C15c open point (a
native copy left an earlier streaming attempt's pointer and upload behind) is closed: a native prepare
resumes what it may and cleans up the rest (`OtherTransfer`, `BindingChanged`, `Requested` for an atomic
copy …). `EffectiveRecovery` still reports `NotApplicableNative`; `prepare` / `reused_bytes` say what was
reused. Nothing in the at-destination native route reaches the temp key; the temp-key code (and its
extra LIST + HEAD per publication) is reachable only with `recovery_at_destination` off, from tests, and
goes in C19. The `CopyObject` sends `x-amz-metadata-directive` and `x-amz-tagging-directive` `REPLACE`
with no metadata and no tags, so no copy carries the source's user metadata, content type or tags —
as the `UploadPartCopy` and streamed routes carry none — and the destination's metadata does not depend
on the size; tags the metadata plan asks for are set afterwards. MinIO accepts `REPLACE` without a
content type and stores `binary/octet-stream`, as it does for the other routes (measured: a 4 MiB
source stored with `text/plain`, `x-amz-meta-origin` and the tag `class=gold` gave a native copy with
`binary/octet-stream`, no user metadata and no tag; a 200 MiB native copy after the change still
compared equal). A 416 `InvalidRange`
(the source no longer holds the range) is a permanent `Conflict`. Verified on MinIO (VM 102,
`data-mover-test`, prefix `data-mover-c18-<ts>`, removed afterwards: 0 objects, 0 uploads): native
copies of 4 MiB, 64 MiB, 65 MiB and 200 MiB (1, 1, 2, 4 native requests) each equal to the source
(`--compare` BLAKE3) with nothing else left; with read-back off 200 MiB took 0.31 s and 1 GiB (16 parts)
1.14 s, so an interruption must land within about a second: a 1 GiB copy cancelled at 300 ms had copied
402653184 bytes (the six parts in flight finished) and left one pointer and one upload, and resumed
`Resumed { 402653184 }` with ten more part copies; cancelled at 500 ms / 700 ms it resumed `Resumed {
805306368 }`; SIGKILL at 0.8 s resumed `Resumed { 402653184 }` (at 0.6 s `Resumed { 0 }` — the upload and
pointer but no part; at 0.45 s nothing had been prepared, `Fresh`), each equal to the source and leaving
nothing. MinIO gives a `CopyObject` of an object uploaded in parts a new plain-MD5 `ETag` (source
`"…-8"`, copy `"39d5…"`), as AWS does, so a lost `CopyObject` reply for such a source reports the final
key changed; a single-part source keeps its `ETag`. `staged_matrix.sh`: the native k1 and m200 rows are
`equal=yes stage_objects=0 key_uploads=0`. `versioning_matrix.sh` (temporary buckets, deleted): native
4 MiB and 200 MiB each one version, no marker, no artifact, `destination_version` the latest; v1 then v2
(100 MiB each) copied natively by `--source-version` gave two versions, newest first v2 then v1, each
equal to its source version.
The S3 temp-key path — `.data-mover-stage/` temp objects, publication by `CopyObject` from the temp key
and the temp key's delete by version (C17), the temp-key native copy (`S3Protocol::copy_object` /
`native_copy`), the store-era recovery identity and `recover` — and the test-only
`with_recovery_at_destination(false)` are no longer reached (removed in C19); S3's `prepare`,
`recovery_identity` and `recover` answer `Unsupported`. Temp keys left before C15c, and their uploads,
are not cleaned up automatically (`.claude/docs/storage-s3.md` says how to remove them).

As built (C21, breaking): the local recovery store is gone — `src/transfer/recovery_store.rs`
(`<binding>.state` records with the `Publishing` state, `<binding>.lock` leases,
`.lease-namespace.lock`, rooted at `DATA_MOVER_RECOVERY_DIR` → `XDG_STATE_HOME` → `HOME`) and
everything only it needed: the engine's store branches (`run_with_store`, `discard_prior_recovery`,
`register_prepared_stage`, the recover / fresh-prepare selection, the store-side native and expert
prepares, the `mark_publishing` / `complete` calls around publication and discard), the deferred
checkpoint's registration (`CheckpointRegistration`, `DeferredCheckpoint.registration`,
`engine/automatic.rs`), the stage's `registration_owned` / `recovery_lease`, and
`RecoveryRegistrationFailure` / `RecoveryRegistrar` / `RecoveryContext`. The transition flag
`StagedDestination::recovery_at_destination()` goes with it (every backend answered `true` since
C15c): the engine always takes the at-destination route, so no path that runs changed. Public API
removed: `storage::{RecoverRequest, RecoveryIdentity, RecoveryValueError}`; the `StagedDestination`
methods `prepare`, `prepare_ephemeral`, `recovery_identity`, `handoff_recovery`, `recover` and
`recovery_at_destination` (`prepare_at_destination` is now required — an implementor outside the
crate must provide it); `TransferPhase::{RecoveryRegistration, RecoveryCompletion}`. `EffectiveRecovery`
keeps every variant (all are still reported). `PreparedStage.at_destination` stays: backends use it to
tell a stage prepared at the destination (or marked by the engine for `Direct`) from one built by
hand. `.claude/skills/_shared/resume_matrix.sh` now runs the interrupted run too in a fresh process
with its own empty `HOME` (`env -i`, only `PATH` and the backend credentials) and fails when any run
leaves anything there — the acceptance criterion "nothing is written where data-mover runs"; its
`KEEP_STATE` / local-record count and the S3 matrix's `records=` column went with the store. Verified
(200 MiB, 20 MiB/s, 6 s cut; CIFS 192 MiB), every run's fresh `HOME` empty, BLAKE3 equal, nothing
left: Local `Resumed { 106745444 }` after a cancellation and `Resumed { 67109120 }` after a SIGKILL;
MinIO (VM 102) `Resumed { 75497472 }` / `Resumed { 41943040 }`, one version, no marker, 0 objects and 0
uploads under the run prefix, e2e-s3 passing; FAS2750 CIFS `Resumed { 67528552 }` both, e2e-cifs
passing; ONTAP NFSv4.1 `Resumed { 67109120 }` both (a first run's resumed cancellation failed once
with a transient NFS write `Protocol` error while the Local matrix ran on the same host, and passed on
the rerun). HDFS was checked only by the in-memory tests (the lab runner was not used for this step).

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
