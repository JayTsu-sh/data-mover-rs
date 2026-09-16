# Data Mover Storage Domain

This context describes the storage systems and transfer roles that data-mover presents through one common model.

## Language

**Storage backend**:
A supported storage system presented through data-mover's common set of storage operations.
_Avoid_: Protocol, driver, client

**First-class storage backend**:
A storage backend that can act as either the source or destination of transfers with every other supported backend and participates in the common core capabilities.
_Avoid_: Adapter, integration

**HDFS backend**:
The first-class storage backend that accesses Hadoop Distributed File System clusters through `hdfs-native`.
_Avoid_: HDFS protocol, HDFS adapter

**HDFS location**:
An `hdfs://` URL that identifies a Simple-mode user or Kerberos principal, a direct NameNode or logical NameService, and the backend's isolated root path. Kerberos keytab and credential-cache locations remain in the backend's `HdfsConfig`, not in the URL.
_Avoid_: HDFS connection string, embedding secret material in an HDFS URL

**Client-scoped HDFS credentials**:
Kerberos credential input owned by one HDFS backend instance. Source and destination backends in the same process may use different principals, keytabs, credential caches, clusters, or realms without changing process-global Kerberos environment variables.
_Avoid_: Pod credentials, process credentials, shared HDFS credentials

**Common storage semantics**:
The operations and entry facts that retain the same meaning across every participating storage backend; backend-specific concepts remain on their owning backend rather than being simulated with empty values.
_Avoid_: Unified backend API, lowest common denominator

**Storage capability**:
A behavior and guarantee that a connected storage backend instance truthfully offers through a role interface. Availability may depend on protocol version, endpoint behavior, configuration, and completed certification rather than only on backend kind.
_Avoid_: Storage type, operation switch, assumed support

**Observed entry**:
An immutable snapshot of a storage entry's common facts and the backend-specific facts requested by that enumeration. Facts requiring additional storage calls, such as ACLs, extended attributes, or tags, are captured independently only when requested; later operations do not re-fetch omitted facts from the source backend.
_Avoid_: Live entry, lazily enriched entry, partial entry requiring follow-up lookup

**Entry index**:
An application-owned query projection of selected observed-entry facts used for comparison, filtering, and statistics. It is derived from the same observation as the entry snapshot but is not authoritative for reconstructing backend facts or metadata observations.
_Avoid_: Entry snapshot, database-backed storage entry

**Entry snapshot**:
The versioned lossless representation of an observed entry used to reconstruct it without querying the source backend again. Data-mover owns its encoding and validation while consuming applications may persist or transport the envelope opaquely.
_Avoid_: Entry index, serialized database row

**EntryIdentityKey**:
A fixed-size opaque comparison projection derived by data-mover from a versioned source identity. Applications may index and compare the key but do not interpret it as a protocol handle or treat it as stronger than its reported identity strength.
_Avoid_: File handle, inode, source identity

**Observation plan**:
The caller's per-enumeration selection of optional entry facts. Facts that require additional storage calls are omitted by default and may be observed only when explicitly requested; an inline-only selection may retain facts already present in enumeration responses without authorizing extra storage I/O.
_Avoid_: Full metadata scan by default, lazy enrichment

**Metadata observation**:
An immutable record of a metadata item's value or the explicit reason no value was observed during an enumeration. It is a source fact, distinct from a caller's target metadata policy and from the target's application outcome.
_Avoid_: Optional metadata value, metadata plan, lazy metadata

**Metadata mapping**:
The explicit derivation of target metadata semantics from source metadata observations and target capabilities. Data-mover owns deterministic storage-semantic conversions and loss reporting; the consuming application selects preservation policy and supplies identity or product-specific mappings.
_Avoid_: Protocol-pair switch, silent metadata conversion

**Semantic loss**:
A specific source metadata meaning that cannot be represented exactly by the target storage capability, such as reduced timestamp precision or an unmappable ACL flag. It must be reported before target mutation and accepted explicitly by policy rather than silently discarded.
_Avoid_: Best available conversion, metadata warning without attribution

**Capability availability**:
The backend-reported status of a storage capability: supported, unsupported with a reason, or implemented but uncertified against its required acceptance gate.
_Avoid_: Boolean feature flag, runtime string error

**Transfer**:
A data-mover operation that streams bytes from one storage backend to another through a staged destination, applies the selected verification policy, and then publishes the destination using the strongest commit guarantee that backend truthfully exposes.
_Avoid_: Job, migration task, synchronization session

**TransferIdentity**:
A caller-provided stable identity for one logical transfer across recovery attempts. Data-mover treats it as opaque; it is neither an application job identity nor a container for backend recovery state.
_Avoid_: Job ID, upload ID, recovery token

**Transfer attempt**:
One execution of a logical transfer. Multiple attempts may share a TransferIdentity while retaining distinct outcomes and attempt identifiers.
_Avoid_: Job retry, transfer identity

**Transfer checkpoint**:
Protocol-confirmed evidence describing which staged destination bytes or parts can be reused after interruption. It is valid only when the staged destination can be re-observed, remains bound to the same transfer and source identity, and its reusable data can be verified independently of caller-reported progress. The storage backend owns the evidence; an application may persist only the stable identity needed to find it again.
_Avoid_: Job progress, trusting caller-reported byte counts

**Recovery identity**:
A versioned opaque value issued by data-mover that lets it locate and validate backend state for a later recovery attempt. Applications persist and return the value unchanged without parsing backend handles, paths, parts, offsets, or fingerprints from it.
_Avoid_: Resume context, missing intervals, application-owned upload ID

**Durable prefix**:
The longest contiguous byte range starting at zero that a file-like storage backend has confirmed through its required persistence barrier and can re-observe after reconnecting. Bytes written beyond a gap are progress, not checkpointed work.
_Avoid_: File length, highest written offset, queued bytes

**Resume**:
Continuing a transfer from reusable work established by a valid transfer checkpoint.
_Avoid_: Retry, redo, assuming every backend resumes by byte offset

**Restart upload**:
Discarding or isolating untrusted staged work and transferring the source again from the beginning when resumption is unavailable, invalid, or explicitly disabled.
_Avoid_: Resume from zero

**Staged destination**:
A backend-owned unpublished object, file, or protocol upload session used until a transfer can be verified and committed. An interrupted staged destination must not appear as the final destination.
_Avoid_: `.terrasync-part`, partial final file

**Ephemeral stage**:
An unpublished staged destination that currently has no reusable checkpoint or recovery identity. It preserves the selected verification and publication guarantees; interruption requires restart until a durable checkpoint and recovery registration have been established.
_Avoid_: Direct final write, small-file resume, durable zero-byte checkpoint

**FinalDestination**:
The caller-specified destination object or path that becomes externally visible when a transfer is published. It remains unchanged until publication succeeds.
_Avoid_: Final, final path, published destination

**Verification evidence**:
The proof supporting a transfer's content-integrity result. Evidence may come from comparing independently observed content or from a storage backend's declared guarantee for a native operation; size alone is not content-equivalence evidence.
_Avoid_: Checksum mode, assuming successful writes prove persisted content

**Content validation**:
A direct comparison of two byte streams that stops at the first mismatching offset or read failure. It is distinct from copy-time verification, which may require observing the complete transferred content before deciding equivalence.
_Avoid_: Background checksum, best-effort scan

**Commit guarantee**:
The backend capability that states how a verified staged destination becomes visible as the FinalDestination, including when atomic publication cannot be provided.
_Avoid_: Pretending every backend has atomic rename

**Transfer policy**:
Caller-selected choices such as whether valid staged work may be resumed or must be restarted. Job scheduling, persistence, redo, and requeue policies belong to the consuming application rather than this storage domain.
_Avoid_: Protocol capability

**Backend state**:
Facts owned and validated by a storage backend, such as a connection, protocol handle, staged object, multipart upload, or reusable range. Applications may retain an opaque identity for this state but do not interpret it.
_Avoid_: Job checkpoint, caller-reported completed ranges

**Backend-session failure**:
A failure that prevents a storage backend session from serving entries reliably, such as loss of connectivity, authentication, or access to the configured root. It terminates the affected scan or transfer session rather than being repeated as one failure per entry.
_Avoid_: Entry error, duplicating the same connection failure for every path

**Entry-operation failure**:
A failure scoped to one observed entry and one storage operation while the backend session remains usable. The failure identifies the entry and operation so the consuming application can apply its continue-or-stop policy.
_Avoid_: Backend outage, unscoped error string

**Transfer state**:
The lifecycle facts for one byte transfer, including source identity, staged destination, verification, publication, and any opaque recovery identity returned to the caller.
_Avoid_: Synchronization job, worker session

**Job state**:
Application-owned scheduling and persistence facts such as task identity, retry count, redo, requeue, worker assignment, and cross-process session progress.
_Avoid_: Transfer checkpoint, backend state

**Protocol retry**:
Replaying a transient backend request when the backend can prove that replay is safe within the current storage operation.
_Avoid_: Job retry

**Job retry**:
An application decision to run a transfer or synchronization task again after an attempt ends.
_Avoid_: Protocol retry

**Traversal observation stream**:
A bounded, cancellable stream of storage observations emitted as `Result<ObservedEntry, EntryOperationFailure>`. An entry-operation failure remains an item; a backend-session failure terminates the stream. Paging, broadcast fan-out, database generations, remote indexes, and wire completion markers are consumer-owned projections rather than storage traversal facts.
_Avoid_: File page, NDX event, synchronization scan generation

**ArchitectureReady**:
The acceptance state in which target responsibilities, interfaces, capability and transfer matrices, failure semantics, migration obligations, and executable release gates are specified completely enough for implementation to begin. It does not claim that the implementation has passed those gates.
_Avoid_: Release ready, implementation complete

**ReleaseReady**:
The acceptance state in which the implemented target architecture has passed its required automated contracts, real-environment profiles, consumer workflows, and performance guardrails and may be released.
_Avoid_: Architecture ready, design complete

**Validation profile**:
A concrete backend protocol or device environment against which support evidence is gathered. Several validation profiles may belong to one BackendKind when protocol versions or device behavior differ materially.
_Avoid_: Backend kind, capability

**Transfer QoS**:
An aggregate source-read budget shared by a group of concurrent transfer attempts. It limits client-mediated source bandwidth and source read operations; destination writes and server-internal native-copy traffic are outside its shaping scope. The consuming application chooses the policy and scope; data-mover enforces it at source storage I/O boundaries and reports when a native path is unshaped.
_Avoid_: Per-file independent limit, destination write limit, end-to-end native-copy bandwidth guarantee

**Automatic recovery**:
A transfer policy that establishes reusable progress only when the planned transfer justifies checkpoint work and the destination confirms a durable checkpoint. Before that transition, the attempt has no registered recovery state.
_Avoid_: Resume after every chunk, implicit recovery guarantee

**Read-back verification**:
An optional content-integrity check that independently reads the staged destination and compares its digest with the source digest before publication. A transfer that omits it reports that omission explicitly.
_Avoid_: Size-only content verification, successful write means verified
