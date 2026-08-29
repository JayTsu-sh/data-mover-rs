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
- backend identity, capabilities, and typed unsupported/uncertified results;
- storage traversal and immutable observations;
- source streaming, staged writes, checkpoint validation, verification, and publication;
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

terrasync may persist an opaque snapshot and `RecoveryIdentity`. It must not parse or
mutate backend facts, staged paths, file handles, upload IDs, parts, offsets, hashes, or
checkpoint ranges.

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
role lending.

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
    async fn publish(&self, stage: &PreparedStage, request: PublishRequest) -> Result<PublicationEvidence, DestinationFailure>;
    async fn discard(&self, stage: PreparedStage) -> Result<(), DestinationFailure>;
}
```

The adapter decides ordered versus out-of-order writes, persistence barriers, staged naming,
and publication mechanics. `FinalDestination` is never a partial-write target.

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
no empty native adapter.

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
backend.

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

The default `Admission` order emits results in the sequence assigned by the backend enumerator.
Concurrent observation may finish out of order, but cannot change delivery order. Both the
admission window and item channel are bounded, so a slow consumer backpressures enumeration
without allowing the reorder buffer to grow without limit.

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
and hardlink topology is outside this architecture.

## 8. Transfer interfaces

Ordinary callers use one high-level operation:

```rust
pub async fn transfer(request: TransferRequest) -> Result<TransferOutcome, TransferFailure>;
```

`TransferRequest` contains neutral source/destination descriptors, `TransferIdentity`,
overwrite/publication requirements, recovery policy, verification policy, metadata policy,
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

## 9. Transfer state machine

```text
Created -> Preflighted -> Planned
        -> Prepared(Fresh | Resumable | Restarted) -> Transferring -> Verified -> Published
        -> CommitReady                           -> Verified -> Published
        -> AlreadyCommitted
        -> Completed | CommittedWithWarnings
```

`Cancelled` and `Failed` are structured terminal states attributed to their stage;
`FinalConflict` is a typed planning/preparation failure. Every transition emits neutral
facts but progress is not checkpoint truth. Only `CheckpointAdvanced` may carry a new opaque
recovery identity.

### Preflight

Preflight resolves role availability, instance capabilities, source identity, destination
policy, metadata semantics, verification guarantees, native applicability, and requested
QoS. A capability failure is typed and precedes remote mutation.

### Staging and transfer

The default path reads sequentially and may write out of order through bounded inflight
buffers. Buffer count and bytes are explicit. Queued, read, or gap-separated bytes are
progress only. A staged destination remains invisible as `FinalDestination`.

The Local streaming implementation lends both roles before source description or destination
mutation, selects the backend-neutral `Streaming` data path, and admits sequential ranges through
the shared chunk/byte/operation inflight runtime. Local range streams independently cap each
emitted chunk at 1 MiB. Completion at this stage requires a re-observed durable prefix equal to
the described source size; the staged file remains unpublished for verification and publication.

### Verification

Copy-time generic transfer computes source BLAKE3 during the initial complete sequential
read, then sequentially rereads the complete durable staged destination and compares its
BLAKE3 before publication. Recovery includes reused content in the complete verification.
The Local implementation performs both passes through bounded buffers, checks cancellation
between staged reads, and returns a recoverable unpublished stage on verification failure.
Backend-native success is evidence only when the adapter declares `NativeStorageGuarantee`;
size and ETag alone are insufficient. Standalone content validation compares two byte
streams and stops on the first mismatch or read failure, cancelling the other reader.

### Publication

Publication occurs only after required verification. Requests select a minimum acceptable
guarantee from `AtomicReplace`, `AtomicCreate`, `VerifiedNonAtomic`, and `BestAvailable`;
incapable destinations fail preflight and outcomes report the actual guarantee. Existing
destination policy is `Overwrite` (default), `VerifyOrSkip`, or `FailIfExists`.
`VerifyOrSkip` requires content-equivalence evidence, not path/time/size. Existing final
content remains unchanged until successful publication.
Local `Overwrite` publishes with a capability-confined atomic rename, while `FailIfExists` uses
an atomic create/link boundary. `VerifyOrSkip` hard-links the observed final into staging, hashes
that stable inode with cancellation checks, then atomically rebinds the verified inode to the
final path; a concurrent path replacement therefore cannot invalidate the equivalence evidence.
Publication failures explicitly distinguish pre-commit recoverable staged state from failures
after the final path changed. Post-commit cleanup or durability failures never claim that the
stage is recoverable or that `FinalDestination` is unchanged; they retain a separate idempotent
cleanup handle for staged artifacts. Temporary equivalence guards are removed on every
pre-commit exit and by discard/recovery cleanup after a process interruption.

### Cancellation and failure

Cancellation stops at a safe point and normally preserves a valid checkpoint. A discard
policy deletes only staged state whose ownership is proven. Failures carry lifecycle stage,
source/destination attribution, retry safety, recoverability, and failure scope.

Required metadata that can target staged content is applied before publication. Metadata
that exists only on `FinalDestination` follows publication. A required post-publication
failure returns committed data with `final_destination_changed = true`; best-effort failure
returns `CommittedWithWarnings`. Transfer never deletes its source and returns only a neutral
source-deletion-safety fact for terrasync policy.

## 10. Recovery contract

`RecoveryPolicy` is one of:

- `ResumeOrRestart` (default)
- `RequireResume`
- `Restart`

A checkpoint is valid only when the backend re-observes the staged state, binds it to the
same transfer/source/destination, independently observes reusable bytes or parts, and meets
the requested verification guarantee. `RecoveryIdentity` is versioned, integrity-checked,
opaque, and validated before any destructive action.

Backend mechanisms:

- Local/NFS/CIFS: persisted contiguous durable prefix; writes beyond a gap are not reusable;
- S3/DXN: service-enumerated multipart parts named by the validated upload identity;
- HDFS: deterministic request-bound partial and continuous verified tail append.

All destinations support restart. Resume is advertised only for combinations with reliable
source positioning and validated backend checkpoint truth. CIFS resume remains Uncertified
until smb-rs passes the FAS2750 gate. Unknown staged ownership is report-only and never
deleted automatically.

## 11. Source-only transfer QoS

QoS is a shared transfer-group source-read budget. Terrasync chooses the immutable group
policy; data-mover enforces it at actual source reads. It supports soft average bandwidth,
hard ceiling, bounded peak credit/duration, source-read IOPS, fairness, and cancellation-aware
waits. Historical constructors and unevidenced hot update are removed.

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
