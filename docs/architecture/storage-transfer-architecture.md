# Storage and Transfer Architecture

Status: ArchitectureReady normative specification

Decision source: data-mover-rs Wayfinder map, issues 98–107

Scope: target architecture for data-mover-rs and its contract with terrasync-rs

This document is the authority for responsibilities, interfaces, lifecycle semantics,
module dependencies, and migration disposition. Capability facts are authoritative in
`backend-capability-matrix.yaml`; directed source/destination requirements are authoritative
in `directed-transfer-matrix.yaml`; executable evidence gates are authoritative in
`acceptance-gates.md`.

## 1. Goals and invariants

The target architecture shall:

1. expose five explicit backend kinds: `Local`, `Nfs`, `Cifs`, `S3`, and `Hdfs`;
2. keep backend-specific behavior inside the owning backend adapter;
3. keep `StorageEnum` thin and free of operation-by-operation dispatch;
4. make streaming through bounded inflight buffers the universal copy baseline;
5. preserve `FinalDestination` until verified publication;
6. resume only backend-observed reusable work and restart otherwise;
7. distinguish entry-operation failures from backend-session failures;
8. acquire ACL, xattr, and tags only when an explicit observation plan requests them;
9. keep data-mover independent of terrasync product, wire, database, and job concepts;
10. replace historical interfaces directly, without compatibility adapters or dual truths.

## 2. Responsibility contract

The dependency direction is strict:

```text
terrasync-rs ──depends on──> data-mover-rs
data-mover-rs ──must not know──> terrasync-rs
```

### data-mover-rs owns

- connected backend clients, protocol handles, caches, and negotiated facts;
- backend identity — the canonical endpoint derived from each connection config (target, see
  [ADR-0006](../adr/0006-destination-resident-recovery.md)) — capabilities, and typed
  unsupported/uncertified results;
- storage traversal and immutable observations;
- source streaming, staged writes, checkpoint validation, verification, and publication;
- per-transfer recovery state kept **at the destination**, next to the final file (target, ADR-0006;
  until C21 the engine still uses a local recovery store), plus claims and cleanup;
- derived transfer identity and source version selection (`SourceVersion::{Current, Id}`);
- deterministic metadata conversion and semantic-loss reporting;
- source-only transfer QoS enforcement and neutral transfer outcomes;
- versioned opaque observation snapshot and recovery-identity codecs.

### terrasync-rs owns

- jobs, scheduling, retries, redo, requeue, progress, and operator policy;
- database schema, query indexes, scan generations, and deletion detection;
- local worker queues and cross-process QUIC framing, paging, session indexes, and acks;
- change classification, conflict policy, metadata-loss policy, and reporting;
- tar manifests, NDX/tree synchronization, and product events;
- credential sourcing/rotation and selection of neutral data-mover policies.

terrasync persists opaque observation snapshots but does not receive recovery identities, staged
paths, upload IDs, parts, offsets, hashes, or checkpoint ranges. Its only per-job recovery input to
data-mover is `TransferPolicy`. Under ADR-0006 data-mover keeps no state where it runs either: it may run
in a non-persistent container, and a resume after a restart is found at the destination. The caller
contract that one destination key is never written by two transfers at once carries the exclusivity
that a local lease provided.

## 3. Target module tree and dependency rules

The target remains one crate with six public deep modules and one crate-private runtime:

```text
src/
├── model/                 # neutral values, identities, observations, errors
├── storage/               # StorageEnum, role interfaces, backend directories
│   └── backends/{local,nfs,cifs,s3,hdfs}/
├── traversal/             # one observation-stream interface
├── metadata/              # observation values, conversion, loss reports
├── transfer/              # request, state machine, outcomes, recovery policy
├── integrity/             # BLAKE3 evidence and independent byte-stream validation
└── runtime/               # crate-private inflight, task, QoS, and native planning
```

Allowed dependencies:

```text
model       -> none
storage     -> model
traversal   -> model + storage roles
metadata    -> model + storage Metadata role
integrity   -> model + storage read roles
transfer    -> model + storage roles + metadata + integrity
runtime     -> model only; high-level modules may use runtime utilities
backend dir -> model + its protocol dependency + storage role definitions
```

During the in-crate legacy migration, `storage/factory.rs` is the sole composition boundary allowed
to construct the NFS, CIFS, S3, and HDFS protocol owners before adapting them into storage roles.
No other target-module file may import those legacy modules.

Forbidden dependencies:

- backend directories importing one another;
- public modules importing terrasync types;
- transfer or traversal matching on backend kind to select behavior;
- runtime importing storage, traversal, metadata, transfer, or integrity;
- `StorageEnum` containing generic operation implementations;
- public exposure of protocol handles, multipart parts, inflight chunks, or native planners.

Each backend directory exposes a facade; its protocol codecs, handles, retry rules,
checkpoint representation, and device quirks remain private. CI shall enforce these
directions and forbid product terms such as `.terrasync-part` in data-mover.

## 4. Storage construction and the thin runtime seam

`BackendKind` is closed:

```rust
pub enum BackendKind { Local, Nfs, Cifs, S3, Hdfs }
```

Callers provide a typed `BackendConfig`; backend kind is never inferred from an arbitrary
path. Construction returns a connected `Storage`/`StorageEnum` instance whose only public
responsibilities are identity, diagnostics, clone lifecycle, capability description, and
role lending. The identity is the backend's canonical endpoint, derived by data-mover from its
config (ADR-0006, `src/storage/endpoint.rs`); callers never supply it.

```rust
impl StorageEnum {
    pub fn kind(&self) -> BackendKind;
    pub fn identity(&self) -> &BackendIdentity;
    pub fn capabilities(&self) -> &BackendCapabilities;
    pub fn read_source(&self) -> Result<Arc<dyn ReadSource>, CapabilityUnavailable>;
    pub fn staged_destination(&self) -> Result<Arc<dyn StagedDestination>, CapabilityUnavailable>;
    pub fn namespace(&self) -> Result<Arc<dyn Namespace>, CapabilityUnavailable>;
    pub fn metadata(&self) -> Result<Arc<dyn Metadata>, CapabilityUnavailable>;
}
```

Each lending method contains exactly one exhaustive backend match. Operation methods never
recreate the five-backend matrix.

Capability availability is instance-specific and typed: `Supported`,
`Unsupported(reason)`, or `Uncertified(gate)`. Uncertified operations are disabled in
production and may be enabled only by the named validation gate.

## 5. Deep role interfaces

The following are behavioral sketches, not source-compatible signatures. Implementations
may refine lifetimes and concrete stream types without changing the stated interface.

### ReadSource

Owns source identity, stable descriptors, and bounded sequential/range streaming. It hides
protocol request sizing, handles, retries, and connection details.

```rust
trait ReadSource: Send + Sync {
    async fn describe(&self, path: &StoragePath) -> Result<SourceDescriptor, SourceFailure>;
    async fn read(&self, request: ReadRequest) -> Result<ByteStream, SourceFailure>;
}
```

`ReadRequest` includes a path, optional range, cancellation, and optional shared source QoS.
The stream is bounded and reports source-attributed failures.

### StagedDestination

Owns the entire unpublished destination lifecycle, including recovery truth.

```rust
trait StagedDestination: Send + Sync {
    async fn prepare(&self, request: PrepareRequest) -> Result<PreparedStage, DestinationFailure>;
    async fn write(&self, stage: &PreparedStage, input: ByteStream) -> Result<WriteEvidence, DestinationFailure>;
    async fn observe_checkpoint(&self, stage: &PreparedStage) -> Result<CheckpointObservation, DestinationFailure>;
    async fn verify(&self, stage: &PreparedStage, request: VerifyRequest) -> Result<VerificationEvidence, DestinationFailure>;
    async fn apply_metadata(&self, stage: &PreparedStage, mutation: MetadataMutation, cancel: CancellationToken) -> Result<(), DestinationFailure>;
    async fn publish(&self, stage: &PreparedStage, request: PublishRequest) -> Result<PublicationEvidence, DestinationFailure>;
    async fn discard(&self, stage: PreparedStage) -> Result<(), DestinationFailure>;
}
```

The adapter decides ordered versus out-of-order writes, persistence barriers, staged naming,
metadata mutation of unpublished state, and publication mechanics. Its private stage path, file
handle, or object key never leaves the backend. `FinalDestination` is never a partial-write or
metadata-mutation target.

For NFS, the common adapter consumes the dialect and negotiated `rsize`/`wsize` facts from
`nfs-rs`. Mount/session concurrency, protocol request scheduling, and protocol-level retry remain
owned by `nfs-rs`; data-mover adds no second mount semaphore or blanket request retry. The adapter
only bounds transfer chunks, applies source QoS/cancellation, and invalidates cached handles for
the explicitly classified stale-handle, bad-handle, and concurrent-lookup cases. NFSv3, v4.0, and
v4.1 retain independent capability evidence and real-environment gates. NFSv4 ACLs use a private,
versioned, lossless codec under the NFS metadata adapter.

### Namespace

Owns coherent namespace semantics: stat/list, create/delete, rename, and supported link
operations. Fine-grained semantics are typed; providing the role does not imply that every
verb exists. An unavailable operation fails preflight before remote side effects.

### Metadata

Owns backend metadata observation and application plus backend-private encoding. ACL,
xattr, tags, ownership/mode, and timestamps are separate capabilities. It never performs an
implicit source refetch to fill observations omitted during scanning.

### NativeTransfer

This role is crate-private. Its planner receives neutral source/destination descriptors and
required guarantees and returns `Planned`, `NotApplicable`, or `Rejected`. Currently only a
conditional S3-to-S3 `CopyObject`/multipart-copy adapter may implement it. Availability is
per connected pair, not per `BackendKind::S3`. All other backends use streaming and provide
no empty native adapter. Standard S3 pairs compare an opaque affinity derived from the connected
endpoint and compatibility profile; buckets and prefixes remain protocol-owned source/destination
facts. Eligible copies bind the observed ETag/version, prepare an unpublished stage through the
ordinary destination role, and only then let the native endpoint fill that stage. S3 native copy,
including multipart copy, never participates in streaming checkpoint recovery; a failed attempt is
reissued through native operation semantics. Both forms use the ordinary BLAKE3 verification and
publication lifecycle. Strict client-shaped payload, a different endpoint,
or a non-standard S3 profile selects streaming before remote mutation. A native operation failure
retains stage cleanup authority and is never silently retried through streaming.

## 6. Observation model and traversal

### ObservedEntry

`ObservedEntry` is an immutable point-in-time storage observation:

```rust
pub struct ObservedEntry {
    identity_key: EntryIdentityKey,
    backend_kind: BackendKind,
    path: StoragePath,
    kind: EntryKind,
    size: Option<u64>,
    modified: Option<StorageTimestamp>,
    source_identity: SourceIdentity,
    metadata: MetadataObservations,
    backend_fact: PrivateBackendEntryFacts,
}
```

Neutral/common facts are available through accessors; backend facts are not inspectable by
external callers. `EntryKind` is neutral (`File`, `Directory`, `Symlink`, or another
explicitly modelled kind). A destination selects operations from neutral kind and requested
policy; it does not match backend facts to implement cross-protocol behavior. Only the
crate-private same-backend native planner may consume private backend facts.

`EntryIdentityKey` is a fixed 32-byte BLAKE3 digest of a small stable `SourceIdentity`; it
is not a content hash. Backend facts are losslessly snapshot-encoded by data-mover through a
versioned opaque codec. Terrasync stores the bytes and separate query columns, then returns
the snapshot unchanged when reconstruction is required. Reconstruction never re-queries the
backend, but it needs the endpoint the scan ran on: snapshots (schema v5, ADR-0006 C4c) do not
store the backend identity, so `ObservedEntry::decode_snapshot(bytes, &identity)` takes it from
the caller (`Storage::identity()`, or `storage::endpoint_identity(&config)` offline).
`SnapshotDecodeError::BackendMismatch` for a whole generation means the endpoint moved or is
spelled differently — treat it as no previous generation, not corruption. v4 snapshots keep
their own identity; copy them forward as bytes rather than re-encoding them.

### Optional observations

`ObservationPlan` defaults to `Omit` for ACL, xattr, and tags. Each requested family chooses:

- `InlineOnly`: use facts already returned by the listing/stat operation;
- `BestEffort`: permit additional calls and report per-entry observation failure;
- `Required`: permit additional calls and fail that entry when unavailable.

No optional family is fetched unconditionally. Each result is explicitly `Value`,
`NotRequested`, `NotApplicable`, `Unsupported`, or `Failed`; `Value(empty)` is distinct from
absence. An omitted or failed observation cannot be lazily fetched by later metadata
application.

The Local implementation obtains ownership/mode and timestamps from the entry's existing stat
result. POSIX access/default ACL and xattr requests use separate capability-opened file-descriptor
calls and therefore run only for `BestEffort` or `Required`; ACL storage attributes are excluded
from the general xattr family. Optional metadata never follows a symlink target.
The observation plan is carried once by the traversal request and applies to every admitted entry.
A traversal may also carry an admission policy (`TraversalFilter`) and a depth bound. The policy
decides, per enumerated child, whether the entry is emitted, whether the directory is listed, and
whether the policy still applies below it; a directory that is not emitted may still be listed, and
a subtree admitted unconditionally is never consulted again. The policy is expressed as a trait, so
traversal stays independent of any expression language. When a policy needs the modification time,
the traversal observes the entry before deciding and, if the plan omitted timestamps, raises them to
`InlineOnly` for that traversal; it never evaluates a policy against an absent timestamp. A listing
that already returns timestamps satisfies a timestamps-only plan without a second call.
All states, provenance, empty values, and failures are retained in the opaque entry snapshot.

### Traversal contract

There is one bounded, cancellable, backpressured item stream paired with mandatory completion
evidence:

```rust
Stream<Item = Result<ObservedEntry, EntryOperationFailure>>
```

An entry-operation failure is an item and includes entry identity/path and operation. A
backend-session failure terminates the stream. Cancellation is a distinct terminal outcome, not
an entry or backend error. EOF alone never proves that enumeration was complete: callers must
await the session completion and receive `Ok(TraversalCompletion)` before committing a complete
generation or enabling deletion. Paging and backend concurrency are hidden in the traversal
implementation.

The default `Admission` order is depth-first by blocks: all children of one listed directory,
in the order the backend listed them, then the block of each subdirectory descended into, in the
same order, recursively. A directory's children therefore always follow the directory, and one
listing's children are contiguous. Directory listings are prefetched concurrently ahead of the
admission point (deepest first): at most `min(max_inflight_operations, 64)` listings are in
flight, separate from the observation window, and at most twice that many listings, in flight
and finished together, wait for the admission point. Memory is therefore bounded by
depth × fan-out for the pending path plus those listings (each a whole directory), never by the
width of a tree level.
A session failure reported by a prefetched listing ends the traversal as soon as it arrives,
including before entries admitted ahead of it have been delivered. Listings and observations may finish out of order, but
cannot change delivery order: sequence numbers are assigned only in admission order. Both the
admission window and item channel are bounded, so a slow consumer backpressures enumeration
without allowing the reorder buffer to grow without limit. Backend operations already started
are never aborted on cancellation or termination; they run to completion in the background so
any handle they opened is closed.

Terrasync projects this same stream in three ways:

- pure scan: database writes, statistics, completeness gate, generation commit, deletion;
- local copy: bounded shared work queue, without wire indexes;
- remote copy: snapshot encoding, paging, session-local `ndx`, QUIC advertisement and done.

`ndx` is ephemeral correlation, never a persistent identity.

## 7. Metadata conversion

`MetadataObservations` contains independent optional ACL, xattr, tags, uid/gid/mode, and
timestamp observations, each with observed/omitted/failed state and provenance.

Data-mover performs deterministic conversion from an observed source semantic model to the
target backend's `Metadata` request. The mapping decision is `Exact`, `Lossy(losses)`,
`RequiresExternalMapping`, or `Unsupported`. Terrasync may supply a `PrincipalMapper` for
enterprise identity mappings; data-mover never guesses across numeric IDs, names, or SIDs.
The immutable metadata plan is compiled before target side effects.
`compile_metadata_plan` consumes only the captured `MetadataObservations`, an explicit
`MetadataTarget` capability description, family policies, and an optional mapper. It never
re-reads source storage and contains no backend-name or protocol-pair switch. The resulting
`MetadataPlan` holds backend-neutral `MetadataMutation` values; transfer applies it through the
stage-owning destination once per planned family and stops on cancellation or the first storage
failure. Standalone metadata operations use the ordinary `Metadata` role with the same mutations.
The partial `MetadataApplicationReport` therefore remains distinct from the compile-time
`LossReport`.

The ordinary Local-to-Local transfer entry preserves the same baseline file metadata as the
legacy `StorageEnum` copy path: numeric ownership and mode plus modified time. Local atime is not
copied because ordinary reads can change it and it is not part of the Local preservation contract. It
captures and compiles that plan before preparing destination state, applies the mutations to the
stage before publication, and returns the application report for both `Checkpointed` and `AtomicReplace`.
`Checkpointed` synchronizes the staged metadata before its durable rename; `AtomicReplace` applies the same
metadata without adding data or directory synchronization.

The directed matrix classifies the intended preservation result of each family as:

- `Preserved`
- `Mapped`
- `DroppedWithLoss`
- `Unsupported`
- `Failed`

Actual application outcomes are `Applied`, `PreservedByNativeTransfer`, `OmittedByPolicy`,
`NotObserved`, `Unsupported`, or `Failed`. Semantic loss and operation failure are never
conflated. Data-mover returns a structured loss report; terrasync selects `RequireExact`,
`AllowKnownLoss`, `BestEffort`, or `Omit`. Destination-side metadata reads return native
observations and require no terrasync conversion. S3 prefixes are not called directories,
and hardlink topology is outside this architecture. Object tags are a first-class bounded,
redacted observation family in the entry snapshot (since schema v4); they are not encoded as
xattrs.

## 8. Transfer interfaces

Ordinary callers use one high-level operation:

```rust
pub async fn transfer(request: TransferRequest) -> Result<TransferOutcome, TransferFailure>;
```

`TransferRequest` contains neutral source/destination descriptors, `TransferIdentity`,
overwrite/publication requirements, resumability, verification policy, metadata policy,
cancellation, inflight bounds, and optional shared source QoS.

`TransferIdentity` is caller-supplied and stable across attempts of one logical transfer; it
is not a job ID and contains no backend state. Every execution receives a distinct attempt
identity. A `TransferOutcome` includes both identities, selected data path, prepare fact,
total and reused bytes, verification evidence, publication guarantee, per-family metadata
results, warnings, `final_destination_changed`, and source-deletion-safety. A
`TransferFailure` includes stage, side, stable kind, operation retryability, checkpoint
disposition, `final_destination_changed`, and `source_changed`, preserving protocol causes in
its error source chain.

Expert callers such as terrasync remote transfer use a stateful session that exposes source
streaming and staged-destination commands without exposing protocol handles. High-level and
expert flows invoke the same state machine and backend role implementations. Correctness
policy becomes immutable after preflight; runtime controls cannot change guarantees.

The expert source session revalidates the advertised `ObservedEntry`, negotiates its maximum
chunk, reads the source sequentially, hashes the complete logical content, and emits only bytes
after the destination-reported durable prefix. The expert destination session independently
prepares or recovers backend-owned state after data-mover durably records its opaque identity,
before returning its write offset. A caller-owned bounded transport carries only byte chunks and
neutral source evidence between the two halves. The destination then observes its checkpoint,
verifies complete staged content, applies its precompiled metadata plan to that stage, and
publishes through the same backend roles and failure phases as the high-level operation. A metadata
failure is destination-attributed, stops at the first failed family, leaves `FinalDestination`
unchanged, and retains stage cleanup/recovery authority. Prepared destination sessions have explicit
discard ownership
for remote cancellation before payload.

## 9. Transfer state machine

```text
Created -> Preflighted -> Planned
        -> Prepared(Fresh | Resumable | Restarted) -> RecoveryPersisted -> Transferring -> Verified -> MetadataApplied -> Published
        -> CommitReady                           -> Verified -> MetadataApplied -> Published
        -> AlreadyCommitted
        -> Completed | CommittedWithWarnings
```

`Cancelled` and `Failed` are structured terminal states attributed to their stage;
`FinalConflict` is a typed planning/preparation failure. Every transition emits neutral
facts but progress is not checkpoint truth. `Prepared` produces the current opaque recovery
identity; `RecoveryPersisted` means data-mover durably recorded it. Backend checkpoint
advancement remains private and never asks the caller to persist ranges, offsets, or parts.

### Preflight

Preflight resolves role availability, instance capabilities, source identity, destination
policy, metadata semantics, verification guarantees, native applicability, and requested
QoS. A capability failure is typed and precedes remote mutation.

### Staging and transfer

Local, NFS, CIFS, and HDFS source prefetch reserve shared chunk, byte and operation capacity before reading.
On the ordered path the payload reservation passes to the output queue and is released when
consumed; the operation reservation ends when the read is submitted to that queue. On the
positioned path the reservation is shared by the write-input queue and digest reordering until
both release it. A full budget prevents further source allocation,
while available capacity still permits concurrent reads. Sources without this admission interface
are polled serially with producer-owned reservations acquired before reading.

The default path reads sequentially and may write out of order through bounded inflight
buffers. Buffer count and bytes are explicit. Queued, read, or gap-separated bytes are
progress only. A staged destination remains invisible as `FinalDestination`.

Sources and staged destinations may opt into neutral positioned payload streams carrying an
absolute offset and `Bytes`. Local, NFS, and CIFS implement both roles: completed reads can reach
positioned writes without waiting for earlier reads, including mixed transfers among these
backends. HDFS sources also offer completion-ordered positioned reads, so HDFS-to-Local/NFS/CIFS
uses this same path. HDFS destinations retain ordered input and a single append writer; all
sources keep their ordered interface for this combination. Other role combinations retain the ordered stream.
The engine validates nonoverlapping coverage and computes the source digest in file order;
digest reordering and the write-input queue share each pre-read admission, so a missing prefix
cannot create an unbounded payload buffer. Payload sharing and write splitting do not copy bytes.
These destinations track acknowledged contiguous ranges independently of stage file length.
At a periodic checkpoint they drain issued writes and persist only a contiguous prefix confirmed
by their durability barrier: Local data synchronization, CIFS FLUSH, or NFS stable-write/COMMIT
and verifier validation. NFS keeps its FILE_SYNC fast path and UNSTABLE replay data; an ordinary
WRITE acknowledgement alone cannot advance a durable recovery record. Completed writes beyond
a hole cannot advance recovery. Checkpoint intervals are minimum progress triggers, not write
boundaries: a chunk need not be split at the threshold. Use `contiguous_prefix >= next_threshold`,
never equality, total acknowledged bytes, or maximum written offset. If filling a hole crosses
several thresholds, drain the issued writes and persist the actual durable contiguous prefix
once, then set the next threshold to that recorded prefix plus the interval. Do not issue one
barrier per skipped threshold. Sparse suffixes may be synchronized by the same barrier but are
not included in the recovery prefix; NFS must retain or commit their UNSTABLE receipts even
when those offsets are beyond the recorded prefix. Existing EOF eligibility rules still apply. Final coverage, digest verification when
enabled, metadata application, and publication retain their existing lifecycle requirements.

The Local streaming implementation lends both roles before source description or destination
mutation, selects the backend-neutral `Streaming` data path, and admits sequential ranges through
the shared chunk/byte/operation inflight runtime. Local range streams independently cap each
emitted chunk at 2 MiB; the caller, inflight byte budget, QoS, or remaining range may negotiate a
smaller read. The Local staged destination independently submits input pieces up to 8 MiB as one
positional write and zero-copy splits larger input pieces into writes no larger than 8 MiB. The
64 MiB automatic-checkpoint interval is an independent recovery-policy constant and does not
change when this write-task ceiling is tuned.
Completion at this stage requires a re-observed durable prefix equal to
the described source size; the staged file remains unpublished for verification and publication.

### Verification

With read-back verification enabled (the default), copy-time generic transfer computes source BLAKE3 during the initial complete sequential
read, then sequentially rereads the complete durable staged destination and compares its
BLAKE3 before publication. A destination whose staged object cannot be read before publication (S3
under ADR-0006: parts are invisible until `CompleteMultipartUpload`) declares its verification point
after publication; it rereads exactly the version it wrote and reports a mismatch or a newer current
version with `final_destination_changed`. Recovery includes reused content in the complete verification.
The Local implementation performs both passes through bounded buffers, checks cancellation
between staged reads, and returns a recoverable unpublished stage on verification failure.
Backend-native success is evidence only when the adapter declares `NativeStorageGuarantee`;
size and ETag alone are insufficient. Standalone content validation compares two byte
streams and stops on the first mismatch or read failure, cancelling the other reader.

### Publication

Publication occurs only after required verification. File publication has one behavior: replace the
final destination with the staged object. Local and rename-capable backends use atomic replacement;
other backends retain their documented reconciliation rules. The request API does not expose
`FailIfExists` or `VerifyOrSkip`, and backends do not stat or hash an existing final merely to select
a publication policy. Existing final content remains unchanged until successful publication.
Publication failures explicitly distinguish pre-commit recoverable staged state from failures
after the final path changed. Post-commit cleanup or durability failures never claim that the
stage is recoverable or that `FinalDestination` is unchanged; they retain a separate idempotent
cleanup handle for staged artifacts.

### Cancellation and failure

Cancellation stops at a safe point and normally preserves a valid checkpoint. A discard
policy deletes only staged state whose ownership is proven. Failures carry lifecycle stage,
source/destination attribution, retry safety, recoverability, and failure scope.

Required metadata is applied to backend-owned staged content before publication. A required
metadata failure leaves the final destination unchanged and returns the partial family report;
best-effort/omit behavior is already fixed in the compiled plan. Transfer never exposes a
post-publication metadata workaround, never deletes its source, and returns only a neutral
source-deletion-safety fact for terrasync policy.

## 10. Recovery contract

**Transition.** [ADR-0006](../adr/0006-destination-resident-recovery.md) replaces the local recovery
store described below: artifacts are named from the final file name in its parent directory, the
destination holds a pointer with the full recovery binding, and prepare decides resume or restart from
what it finds there (resume on an equal binding; otherwise clean up in place and start from zero,
reporting why). The recovery binding becomes `TransferIdentity` (derived from source endpoint, source
path, version selector, destination endpoint and final path) plus the observed source version. The
local store, its lease, the `Publishing` state and `DATA_MOVER_RECOVERY_DIR` remain in force until
commit C21 removes them.

Local namespace durability uses a shared directory-handle helper. On Windows it opens the
existing directory capability with read/write access and `FILE_FLAG_BACKUP_SEMANTICS` before
`sync_all()`; read-only directory handles cannot satisfy `FlushFileBuffers`. Publication,
checkpoints, claim cleanup, new parent directories, and recovery-store registration retain
real directory synchronization, and synchronization failures remain errors.

`TransferPolicy` offers `Checkpointed` (the default), `AtomicReplace`, and Local/HDFS `Direct`. Local Direct writes the final inode without staging, rename, checkpoints, or final persistence barriers; see [ADR-0003](../adr/0003-transfer-policy-direct.md). HDFS uses ordered append and close-based checkpoint barriers; see [ADR-0004](../adr/0004-hdfs-transfer-policies.md). AtomicReplace restarts from zero without
creating checkpoints; Local and NFS skip final persistence barriers but retain staging,
cancellation checks and atomic publication. Success in AtomicReplace mode does not promise crash
durability. Other protocols may retain their required persistence operations. It is the only
recovery control accepted from terrasync. Data-mover selects the effective behavior after route and
source-read planning and reports it as `EffectiveRecovery`.

For eligible multi-source-chunk streaming with `Checkpointed`, data-mover opens its private recovery store,
exclusively claims the transfer binding, recovers or prepares backend-owned staged state, and
atomically persists the backend's versioned opaque identity. Ordinary Local, NFS and HDFS defer registration until
the first durable checkpoint; destinations without deferred support register before payload. Successful publication
and explicit discard clear that record. Before publication, the record atomically enters a
`Publishing` state. After a restart, only that state may reconcile a missing stage by clearing the
ambiguous record and starting fresh; an ordinary missing staged object remains a strict failure.
Invalid persisted records fail validation; they are never
silently interpreted by terrasync.

A streaming transfer that fits within one effective source-read chunk uses an ephemeral stage even
under `Checkpointed`, because it has no useful intermediate checkpoint. This decision does not use
the destination write ceiling: a destination may split that one source chunk into multiple writes.
All native copies report `NotApplicableNative` and do not open recovery state. Restart upload is an
explicit data-mover lifecycle action that discards its owned failed stage and state before starting
from zero.

A checkpoint is valid only when the backend re-observes the staged state, binds it to the
same transfer/source/destination, independently observes reusable bytes or parts, and meets
the requested verification guarantee. `RecoveryIdentity` is versioned, integrity-checked,
opaque, and validated before any destructive action.

Recovery bindings also include a source content-version observation, separate from object identity.
Local captures mtime and ctime from the same stat as its descriptor (mtime and creation time on
non-Unix platforms); NFS captures the server's mtime and ctime. Atime is excluded. A same-size
in-place edit therefore selects a fresh binding instead of reusing an old durable prefix, even
when read-back verification is disabled.

The Local identity binds the transfer identity, source identity/path/size/version, destination backend
identity, final destination, stage token, and checkpoint record. Its checkpoint records only a contiguous prefix after the
staged file has crossed a persistence barrier. Recoverable Local writes pause input after each
backend-owned 256 MiB interval, drain inflight positional writes, verify the completed range is
contiguous, synchronize file data and length, and atomically replace the checkpoint record before
issuing later offsets. Resume rereads the complete source sequentially
for BLAKE3 but emits writes only after the re-observed durable prefix. A supplied identity has
strict recovery semantics: a missing, invalid, conflicting, or mismatched backend state fails
without deleting unknown state or restarting implicitly. Observing a recovery identity is
non-mutating. Only explicit failure handoff transfers recovery authority out of the current
process; NFS releases its adapter-local claim during that handoff. Local holds a persistent
per-stage claim file under an exclusive OS file lock. NFS recovery additionally requires a
caller-persisted, per-attempt claim token: the adapter derives a deterministic claimed path from
the opaque identity and token, then atomically renames the old stage. The same token makes a
lost result idempotently re-enterable after process restart; different tokens ensure competing
processes cannot acquire the same lifecycle authority.

Backend mechanisms:

- Local/NFS/CIFS: persisted contiguous durable prefix; writes beyond a gap are not reusable;
- S3/DXN: service-enumerated multipart parts named by the validated upload identity; no adapter
  claim (it relied on a conditional PUT that MinIO `RELEASE.2023-03-20` rejects with 404, failing
  every resume there) — exclusivity is the engine's per-host recovery lease, and one destination
  key is never written by two transfers at once (caller contract);
- HDFS: deterministic request-bound partial and continuous verified tail append.

The HDFS architecture adapter is split at a protocol boundary: `storage/backends/hdfs`
owns neutral facts, traversal, range reads, staged publication, and metadata semantics,
while the existing `HDFSStorage` owns Hadoop clients, Kerberos/HA configuration, and
protocol error translation. Each transfer attempt uses an isolated, deterministic-after-
creation same-directory partial, bounded chunks, BLAKE3 readback, and rename publication.
An opaque recovery identity binds that partial to the source, destination, and expected
size. Recovery atomically renames it to a claim-token-derived path, re-observes the HDFS
continuous durable prefix, and appends only the remaining tail. Invalid identities fail without
deleting unknown state or starting a replacement stage. HDFS
string owner/group, replication, block size, and mode are persisted as backend facts;
the current neutral ownership observation cannot yet expose string principals. ACL
and xattr are reported unsupported until public dependency APIs are bound, rather
than triggering unconditional scan-time calls or claiming unevidenced support.

All destinations support restart. Resume is advertised only for combinations with reliable
source positioning and validated backend checkpoint truth. CIFS resume remains Uncertified
until smb-rs passes the FAS2750 gate. Unknown staged ownership is report-only and never
deleted automatically.

## 11. Source-only transfer QoS

QoS is a shared transfer-group source-read budget. Terrasync chooses the immutable group
policy; data-mover enforces it at actual source reads. It supports soft average bandwidth,
hard ceiling, bounded peak credit/duration, source-read IOPS, fairness, and cancellation-aware
waits. Historical constructors and unevidenced hot update are removed.

During the ordered migration, the target transfer seam never calls the historical `QosManager`.
That legacy manager remains confined to the old copy/convenience surface until the post-matrix
removal ticket #150 deletes those public paths, as required by the parent migration sequence; no
adapter bridges it into `SourceQosGroup` and no new caller may depend on it.

The public seam is an immutable `SourceQosPolicy` held by a cloneable `SourceQosGroup`.
Each `TransferRequest` may join one group and receives a private per-attempt meter while all
clones contend on the same FIFO admission scheduler. `ReadRequest` carries that private budget
to the source adapter; the adapter may reduce a protocol read to the admitted sub-range and
records bytes only after the source returns them. Cancellation while queued releases the waiter
without charging an operation or payload. Neither the destination role nor verification receives
the source budget.

Rules:

- newly client-streamed payload is bandwidth-charged once at source admission;
- an actual retried source read consumes source IOPS again;
- destination writes/persist/publication are not shaped or charged;
- traversal, metadata observation, and standalone validation need separate budgets;
- resume charges only repeated/new source work; restart charges all rereads;
- internal chunks and channel operations are not IOPS.

S3 native copy remains allowed with QoS enabled. Server-internal payload is not client-shaped.
Outcomes distinguish logical payload, client-streamed shaped bytes, native bytes, source read
operations, and native requests, and explicitly report native payload as unshaped. A caller
requiring client-shaped payload explicitly disables native transfer.
The same accounting snapshot is available on `TransferFailure`, so cancellation and later-phase
failures report the source work already performed. Resume and restart create new per-attempt
meters against the shared group; every source reread required for BLAKE3 verification is charged,
while reuse of the destination durable prefix never creates destination-side QoS charges.

## 12. Failure taxonomy

`EntryOperationFailure` identifies one entry and one operation while the backend session
remains usable. It may flow to terrasync for continue/stop policy.

`BackendSessionFailure` means connectivity, authentication, configured-root access, or an
equivalent session-wide loss. It terminates the affected traversal or transfer session and
is never duplicated once per entry.

`TransferFailure` additionally records lifecycle phase, source/destination attribution,
transience, safe protocol replay, staged-state disposition, and any validated recovery
identity. A backend performs only safe protocol retries; terrasync owns job retry decisions.

## 13. Legacy disposition and migration

| Historical surface | Disposition |
|---|---|
| `StorageType`, path type detection | Replace with `BackendKind` and typed config |
| operation-heavy `StorageEnum` | Replace with thin identity/role-lending seam |
| `EntryEnum`, `NASEntry`, `S3Entry`, `HDFSEntry` | Replace with `ObservedEntry` and private typed facts |
| `walkdir`, `walkdir_2`, both iterators, `AsyncReceiver` | Replace with one observation stream |
| NDX/pages/tree events | Move to terrasync |
| `StorageEntryMessage`, `ErrorEvent`, `ChangeKind` | Move to terrasync |
| `copy_file`, `copy_file_resumable`, three-stage compatibility surface | Replace with `transfer` and expert session |
| `ResumeContext` | Replace with opaque `RecoveryIdentity` |
| `StreamHandle`, `DataChunk`, commit callbacks | Make crate-private runtime details or delete |
| `storage_resume_compat`, `hdfs_legacy_resume`, `.terrasync-part` | Delete |
| scattered metadata/copy ACL/xattr helpers | Replace with Metadata role; backend codecs private |
| whole-file byte convenience and scattered hash helpers | Delete; retain integrity module contracts |
| NFS export and S3 bucket discovery | Keep on concrete backend interfaces |
| verified StorageGRID/DXN/NFS/HDFS/CIFS quirks | Keep inside owning backend adapter |
| tar manifests/orchestration, DB/job retry/redo/requeue | Move to terrasync |
| per-backend `delete_dir_all_with_progress` | Replace with one `Namespace`-role recursive delete |
| per-backend integrity comparison over the storage enum | Replace with the `integrity` module over `ReadSource` + `Metadata` |

Migration is architecture-first and coordinated:

1. implement model, role, traversal, metadata, integrity, and transfer seams;
2. migrate terrasync snapshot/database, pure-scan, local-copy, and remote-session projections;
3. run the complete acceptance package against both repositories;
4. remove all legacy surfaces in the same migration before release.

There is no deprecation phase, compatibility adapter, old wire support, dual database codec,
or interval with two authoritative state machines.

## 14. Conformance

The architecture is implementable only together with the two YAML matrices and acceptance
gates named at the top. A change to a responsibility or semantic contract changes this
document; a change to a capability/cell changes the corresponding YAML; a change to evidence
changes `acceptance-gates.md`. No document duplicates another's authority.

### Local execution and automatic recovery

The accepted decision is [ADR-0001](../adr/0001-local-transfer-execution-and-recovery.md). Ordinary Local transfers default to automatic recovery: a 64 MiB destination checkpoint interval, with eligibility requiring a file strictly larger than that interval and more than one effective source chunk, selected once before payload I/O. Checkpointed uses this rule; a checkpoint boundary reached at end of file skips the periodic checkpoint and proceeds to final synchronization and publication. Eligible fresh stages are registered only at their first durable checkpoint. Smaller multi-chunk transfers still use inflight reads and writes without checkpoint registration. AtomicReplace skips new checkpoints and Local final durability barriers. See [ADR-0002](../adr/0002-auto-and-quick-policy.md).

Single-source-chunk transfers bypass the producer/channel but preserve source length checks, cancellation and atomic publication; final durability follows Checkpointed or AtomicReplace. Backend write ceilings independently govern splitting and concurrent writes. `Bytes` slices retain their allocations; Local does not concatenate source fragments to fill its maximum write size. Prepared stages reuse directory capabilities within an attempt.

### NFS execution and automatic recovery

Ordinary NFS transfers use the same fixed 64 MiB Checkpointed eligibility interval, independent of
negotiated protocol chunk sizes. Checkpointed sends UNSTABLE WRITEs while retaining their verifier evidence.
It COMMITs at the first eligible recovery boundary or at EOF, so files below the threshold pay only
the final COMMIT. Eligible copies register recovery only after the first interval is a contiguous
committed prefix and its per-stage checkpoint file has been atomically replaced. Every later 64 MiB
boundary replaces that record after its data barrier. Recovery trusts the record instead of stage
length and truncates any inflight tail before resume. Stage and checkpoint are hidden siblings of
the final file. The current stage token embeds an immutable, unique stage ID that remains present
across claim renames, so recovery derives the checkpoint from the single token carried by recovery
identity while isolating competing attempts for the same binding. NFS traversal omits the reserved
`.data-mover-` siblings.
Once recovery is active, bounded batch COMMITs may occur more frequently to release retry payloads,
but they do not advance the recorded recovery prefix. AtomicReplace uses bounded UNSTABLE WRITEs without COMMIT, then closes and atomically renames
the stage; it creates no recovery record and does not promise server-crash durability. Both modes
retain the negotiated NFS write ceiling and the common inflight backpressure.

The NFS write path distinguishes the requested stability from the server-reported commitment.
`FILE_SYNC` replies need no ordinary COMMIT and do not retain payload bytes for verifier replay.
`UNSTABLE` and `DATA_SYNC` replies retain payload bytes and count toward the bounded COMMIT batch.
All retained outcomes still pass through the backend's batch-completion primitive at recovery and
EOF boundaries so pNFS layout synchronization remains protocol-owned.

`TransferRequest::with_read_back_verification` selects `ReadBackVerification::Enabled` or `Disabled`. Outcomes carry that selection and `blake3: Option<[u8; 32]>`; `None` explicitly means destination read-back was not performed. The expert interface continues to require verified evidence.


Local/NFS artifact naming follow-up: new stages use the shared
`.data-mover-<destination-path-hash-16>-<uuid-32>.stage` base name in the final parent.
Checkpoints append `.checkpoint`; checkpoint update temporaries append `.tmp-<uuid-32>`.
Local retains its `.claim` file lock. NFS claim rename appends `.claim-<claim-id-32>`
to the stage base name, while its checkpoint name remains based on the unchanged base.
RecoveryIdentity still carries one current stage token. Only the unified naming format is
accepted for recovery; legacy stage names and the old centralized staging layout are rejected.


### HDFS source baseline metadata

Automatic copy observes HDFS mode and timestamps in the same stat that validates the described
source identity. A separate mode-only copy observation carries no fabricated numeric UID/GID
and leaves the existing metadata snapshot encoding unchanged. The metadata planner applies
mode to Local (Unix), NFS, and HDFS destinations while reporting owner/group loss and preserving
the destination principals. CIFS does not advertise POSIX mode conversion and reports that loss.
Compatible mtime is copied; atime and creation time remain excluded. Local mode application
uses permissions only; NFS uses a mode-only SETATTR, without reading or rewriting ownership.
