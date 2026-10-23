# ADR-0008: Streaming traversal, scan checkpoints, and per-key object history

Status: accepted; P7 implementation and acceptance complete (2026-10-12), including C11b, C12,
legacy scan removal, C7c first-page recovery and Q4 block pairing. CI, available live matrices,
MinIO 100,000-version validation and scan cancel/SIGKILL resume passed. DXN-2 and StorageGRID
environment limitations remain recorded in P7 §14.3. Final C7c/Q4 acceptance and HDFS recovery
are recorded in P7 §14.4: healthy active/standby operation was restored, all seven scoped HDFS
product tests passed, and the isolated test root was removed. HDFS is no longer an environment
limitation.

Supersedes the traversal and entry-snapshot portions of ADR-0006 C22. The detailed decisions,
measurements and acceptance status are in [P7](../design/p7-streaming-listing.md). This ADR records
the contract; P7 §14.3–14.4 record which real-environment gates passed or remain unavailable.

## Context

Whole-directory enumeration delays the first entry and makes memory grow with directory width.
The former traversal order also couples consumers to sibling sorting and subtree events. S3
version-per-entry enumeration makes a key's identity, filtering, counts and history reconstruction
depend on a traversal mode. Consumers need explicit completeness evidence before deleting absent
entries, and durable progress that cannot skip data when a scanner restarts.

## Decisions

### Stream pages through one namespace role

`Namespace::list(ListRequest)` yields a bounded, cancellable `ListingStream`. Each page carries
children and child failures; `ListingEnd` separately reports whether enumeration finished. A
producer finishes in-flight protocol operations and closes its handles after cancellation or drop.
Backend adapters own pagination, retry classification and continuation validity.

Local, NFS, CIFS, HDFS and S3 current-object listings stream their pages. No backend ordering is
assumed. S3 histories are materialized per directory, capped at five million records, because a
key's records can be scattered across pages. Exceeding that cap fails the listing with `Capacity`;
it must not silently fall back to current objects or truncate history.

### Two orders, one set of entries and counts

`Unordered` is the default: workers keep local depth-first stacks and steal shallow pending work.
A directory's entry precedes everything below it. Directories may interleave; the segments of one
key remain consecutive and ordered. It emits no `DirectoryListed` events.

`Blocks` delivers a directory's children in backend listing order, then its `DirectoryListed`,
then visits subdirectories in raw UTF-8 name order. Block contents are not sorted. Only pending
subdirectories are retained for descent; prefetch and observation windows are bounded.

`pair_directory_blocks` pairs two closed blocks by raw child name with explicit directory mappings.
It borrows observations and history segments, preserves duplicate/type conflicts, and suppresses
absence when listing failures, filters, child failures or name semantics leave it uncertain. This
local result does not replace the session and ancestor-failure deletion gates below.

Both orders apply the same filter, depth, observation and counting contracts. Hidden artifacts
are excluded from `listed`. Every returned child satisfies:

```text
listed == emitted + filtered + failed_children + vanished
emitted == files + directories + symlinks + other
```

### Metadata observations and filter dependencies are explicit

`TraversalRequest.observation_plan` selects the existing five metadata families and their
`Omit / InlineOnly / BestEffort / Required` modes. `Metadata::observation_capabilities` exposes
instance-specific support without I/O; unknown support is checked at runtime, and a family
not applicable to the backend can be answered without a call. Known unsupported `Required`
requests fail before listing. Unsupported best-effort observations retain their state without
sending a futile request.

`TraversalFilter::required_metadata` declares families read by a decision. An omitted dependency
is a configuration refusal; filters never upgrade the caller's plan. The DSL maps `modified`
conditions to `Timestamps`, so callers explicitly select at least `InlineOnly`. Listed information
can settle a decision before unrelated observations. Pending dependencies wait for observation;
missing, unsupported, failed and not-applicable results are final states, not pending work.
The candidate exposes these states through `MetadataObservations`.

Only missing families authorized for additional calls are sent to the metadata role. Listed
values are merged with the result, so inline-only families cannot be populated accidentally by
an unrelated call. Required failures are child failures; a directory whose deferred observation
fails remains eligible for descent. Object histories retain their existing per-version failure
records. Symlink targets remain mandatory entry data read through `Namespace::read_link`.

### Completion and deletion safety are explicit

The session emits `TraversalItem::{Entry, Failure, DirectoryListed, Checkpoint}` and has a
separate terminal outcome. EOF alone proves nothing; only `Completed` permits deletion detection.
A listing failure excludes the whole previously known subtree from deletion; a child failure
excludes that entry. An unnamed child failure excludes all known direct children of that directory.
Consumers apply the same filter and depth bound to old data and handle destination name-folding
collisions as conflicts.

A vanished child is counted without emitting a failure. Throttling backs off within a bounded
budget, then fails at most its listing or child. Cancellation remains distinct from failure.
Backend session failures terminate the scan. A listing with no delivered children is `Unreadable`
when it fails; one that delivered children is `Interrupted`.

### Resume progress is conservative

With `Resumable::On`, checkpoints are emitted at the configured interval and on early termination.
The caller supplies a filter digest and persists a checkpoint only after committing every item
that precedes it. Replay is allowed; advancing beyond uncommitted output is forbidden.

Blocks checkpoints follow delivered blocks. Unordered checkpoints retain queued and active
directories. Resume re-lists the necessary directories and preserves failure exclusions. Backend,
root, filter, depth, order and the connected history capability form the fingerprint. The
filter's metadata dependencies and their observation modes also form the fingerprint. Modes for
unrelated metadata families, concurrency, buffering and checkpoint interval remain tunable.
A mismatch refuses resume. Oversized frontiers suppress checkpoint emission rather than
dropping pending work. The initial checkpoint format is v1, including these fingerprint rules.
The version field identifies the persisted format independently of the crate's release version;
future incompatible published formats require a new version and unknown versions are refused.

These scan checkpoints are independent of destination-resident transfer recovery (ADR-0006).

### History belongs to a key

S3 connects with one fixed history capability derived from `GetBucketVersioning`:

| Response | Listing |
|---|---|
| Enabled or Suspended | `ListObjectVersions` |
| Explicit refusal, including 403 | `ListObjectVersions` |
| Never versioned, unsupported versioning API, or NAS profile | `ListObjectsV2` |
| Transient network/timeout/408/429/5xx after SDK retries (except 501), or an undecodable HTTP 2xx answer | Refuse role connection: `Connectivity` / `Transient` |

An HTTP success status alone is not a versioning answer. The SDK can wrap an invalid successful
response in `ServiceError`; it still refuses connection rather than selecting a listing mode.
Successfully decoded `Enabled` and empty versioning configurations retain their normal behavior.

Every history key is an `ObservedEntry`, including a currently deleted key. `ObjectHistory`
records run oldest first in the server's original interleaved response order. Failed ordering
checks produce `HistoryOrder::Inferred` and a timestamp-based fallback with the latest record last.
The key has path-scoped identity; each `VersionRecord` retains its version-bound identity and
copy selector. Optional metadata is read per record; its failure stays on that record even under
`Required`. A delete marker performs no metadata read and has no copy selector.

More than 100,000 records are split into zero-based segments. Only the last is complete; a consumer
must receive all segments through it before accepting a whole history. Resume starts the key again
at segment zero. Each segment retains the whole key's maximum object size so transfer preflight
has a consistent answer. Filters and child/file counts run once per key before segmentation.
`bytes` is current content; `versions`, `delete_markers` and `all_versions_bytes` account for history.
Without history, `all_versions_bytes == bytes` and both record counts are zero.

`TraversalVersions`, `EntryVersion` and `Namespace::list_versions` are removed. Historical replay
iterates `history.records()` serially per key, uses `record.source()` for object versions and
`transfer::write_delete_marker` for markers. Destination marker coalescing and null-version
removal remain explicit results, not silent success.

### One entry-snapshot format during development

`EntrySnapshot` uses `DMES` plus version byte **7**, for entries with and without history. A
snapshot contains one complete history segment and its observations. v4, v5 and v6 are rejected
with `UnsupportedVersion`; the caller treats them as no previous observation and rescans.

Every format change increments the version byte, including an added field. Golden-byte tests pin
the current encoding and decoding. Before release, decide whether to decode N-1 or supply an
explicit terrasync migration/rescan procedure. This development policy must be reconsidered then.

Destination recovery pointer/binding formats are outside this policy. An incompatible change to
those formats must state “drain in-flight transfers before upgrading” in its commit because an
upgrade otherwise strands ongoing transfers.

## Consequences and acceptance

Consumers expand each history segment into per-version rows while storing its snapshot once per
`(path, segment)`. They persist output before scan progress, deduplicate replay, and use completion
and failure exclusions as their generation/deletion gate. These are terrasync follow-up changes,
not implicit behavior of this library.

Versioned buckets now always pay whole-directory history memory and any requested per-version
metadata I/O; the five-million-record cap is a deliberate limit. Current-object projection is a
consumer operation (`storage_role_operations --current`), not a cheaper alternate listing mode.

P7 §12 records performance acceptance for the two schedulers; §14 tracks CI, mutation tests,
real-environment matrices and the 100,000-version memory measurement. Legacy scan removal follows
those contracts and preserves a recoverable performance baseline in git history.
