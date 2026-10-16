# Legacy API removal

Status: complete; final local CI and the live checks recorded below passed.
The user requested removal of all legacy APIs and migration of their callers, with an
explicit check that the new path provides the corresponding optimized implementation.
Removing exports alone does not satisfy this work.

## Replacement inventory

| Old surface | Replacement | Deletion gate |
| --- | --- | --- |
| `StorageEnum`, `create_storage`, backend dispatch | `storage::connect_backend`, `Storage` and typed roles | Migrate examples, test fixtures and CLI configuration; preserve credential redaction and explicit HDFS configuration |
| `walkdir`, `walkdir_2`, NDX, HDFS recursive scanner | `StorageTraversalSource` | Already removed by P7; retain historical evidence |
| `copy_file`, per-backend read/write pipelines | `transfer::transfer` | Verify policy, cancellation, size guards, bounded positional I/O, metadata and native route coverage |
| Copy-and-delete source (`is_source_reserved = false`) | `transfer::move_file` | Verify and publish destination, recheck source binding, then delete; preserve published outcome on cleanup failure |
| `copy_file_resumable`, HDFS compatibility orchestration | Destination-resident recovery in `transfer::transfer` | Preserve source binding, durable checkpoints, cancellation and restart behavior; no caller-managed legacy stream handle |
| `resume_prepare`, `read_chunk_stream`, `write_chunk_stream`, `commit_chunk_stream` | `ExpertSourceSession`, `ExpertDestinationSession`, `WriteProgressObserver` | Split transport retains acknowledged range notifications; destination-recorded checkpoints remain recovery authority |
| S3 native copy / multipart wrappers | S3 role native endpoint and staged destination | Retain server-side copy, limits, checksums, profile behavior and directory-object refusal tests |
| Recursive directory creation | `storage::create_directory_all` | Retain NFS stale-parent recovery and typed errors |
| Recursive deletion and progress iterator | `storage::delete_tree`, namespace batch/subtree operations | Preserve S3 bulk current-key deletion, HDFS server-recursive deletion, hidden artifact cleanup, failure propagation and cancellation |
| NFS `remove_xattr` | Metadata role deletion of one named extended attribute | Preserve protocol-supported removal and typed errors |
| `get_metadata`, `set_metadata` | `Namespace::stat`, `Metadata`, metadata plans | Preserve supported ownership, mode, timestamp, ACL and xattr semantics; use listing facts when available |
| `rename`, `read_symlink` | `Namespace::rename`, `Namespace::read_link` | Migrate callers; preserve path confinement and protocol error classification |
| `create_symlink` | `Namespace::create_symlink(CreateSymlinkRequest)` | Local no-follow metadata and parent confinement; NFS fused SYMLINK attributes and bounded stale retry |
| `IntegrityCheck` | `integrity::compare` | Existing role comparison covers kind, size, mtime and streamed BLAKE3; check old mode/owner/group comparisons and exact timestamp option before deletion |
| `pack_files_to_tar`, `calculate_tar_size` | `tar_pack::pack(TarRequest)` and checked archive sizing | Bounded identity-bound reads, staged atomic publication, PAX long names/large values, headers and padding |
| `check_connectivity`, `probe_server_time` | `probes::{check_connectivity, probe_server_time}` | Preserve explicit probing semantics; connecting alone does not perform every old probe |
| `read_file_from`, `write_file_from_bytes` | `ReadSource` / staged destination sessions | Migrate bounded fixture helpers and preserve publication/cleanup behavior |
| `EntryEnum`, NAS/S3/HDFS compatibility entries | `SourceDescriptor`, `ObservedEntry`, metadata values | Remove compatibility models after protocol adapters and fixtures no longer depend on them |

## Optimization checks

Deletion must retain the new traversal's inline observations and page-driven scheduling,
NFS descent handles and cookie recovery, CIFS handle closure, S3 current/version listing
distinction, and HDFS lazy `getListing`. An application page is not necessarily one protocol
response. S3 history listing still gathers the directory before delivery; removal must not
claim that it is bounded like the current-object path.

Generic recursive deletion finishes listing each directory before mutating its children,
so a failed read preserves that directory. S3 explicitly opts into paged current-object
deletion and batches up to 100 keys per request; a later page failure can follow successful
earlier deletions. Both modes retain directories and ancestors whose children could not
be described. HDFS also exposes native recursive deletion with explicit storage-root opt-in.

For transfers, use the role engine's negotiated chunks, bounded in-flight reads/writes,
source QoS, native affinity, single-PUT threshold, checkpoint and publication policies.
Do not replace these with a whole-file buffer or a second per-backend dispatch pipeline.

Names such as Hyper's `client::legacy` and the RFC 1864 checksum describe third-party APIs
or wire protocols, not retired data-mover implementations. Historical design evidence is
also retained.

## Validation

Preserve meaningful behavior assertions on the replacement APIs. Retire tests of removed
wire representations only after their current recovery/transport replacement is covered.
Run formatting, architecture checks, strict clippy, library/integration/example checks and
local end-to-end tests. Record actual live-backend checks separately from environment-limited
checks; previous acceptance is not evidence for the changed revision.

The obsolete enum dispatch scanner and its baseline are retired with `StorageEnum`.
`make verify` retains the architecture dependency guard and role behavior tests;
backend capability differences are explicit, so requiring identical inherent methods
on every backend is no longer an appropriate gate.

### HDFS contract migration

The live contract uses an upstream `hdfs_native::Client` only for fixture seeding,
independent verification and confined cleanup. Operations under test use storage roles.
The root validator still rejects broad or escaped cleanup targets.

| Retained behavior | Replacement coverage |
| --- | --- |
| Explicit configuration and upstream ownership/async contracts | Four offline tests in `tests/hdfs_native_contract.rs` |
| Recursive filtering/depth and one-directory paging | `nightly_lab_streams_recursive_scan_with_depth_limit`, `nightly_lab_lists_exactly_one_directory` |
| Root lifecycle, rename, deletion and hidden artifacts | `nightly_lab_validates_factory_root_lifecycle`, `nightly_lab_namespace_rename_delete_and_artifacts`, `nightly_lab_confines_independent_roots_and_cleanup` |
| Positional ranges and negotiated chunks | `nightly_lab_reads_positional_ranges_and_common_stream` |
| Zero/small/large cross-protocol copies and integrity | `nightly_lab_cross_protocol_copy_and_integrity`, `nightly_lab_copies_between_s3_and_hdfs` |
| Policy boundaries and restart from a durable prefix | `tests/hdfs_native_contract/policy_runtime.rs` |
| Metadata and cancelled mutations | `nightly_lab_mutates_hdfs_native_metadata_safely`, policy runtime metadata test |
| TAR publication | `nightly_lab_role_tar_write` |
| Failure injection, cancellation and checkpoint ordering | `src/transfer/hdfs_tests.rs`, `src/transfer/hdfs_policy_tests.rs` |

Old `.part` names, serialized `StreamHandle` fields and legacy enum dispatch assertions
are not public contracts of the new engine. Their semantic replacements exercise
stage discovery, source binding, durable progress and publication; retaining assertions
about the removed wire representation would test an API that no longer exists.

Archive extensions follow the PAX byte-length record format. Non-UTF-8 link values
use `hdrcharset=BINARY` when placed in an extended header; this prevents a reader
from treating opaque Unix bytes as UTF-8. See the [vendor PAX format reference](https://docs.oracle.com/cd/F16636_01/html/E71066/pax-1.html).

### Progress and move semantics

`ExpertDestinationRequest::with_write_observer(WriteProgressObserver::new(...))`
reports acknowledged write ranges as `WriteProgress::Confirmed { offset, bytes }`.
These ranges can arrive out of order and do not by themselves prove crash durability
or publication. `Checkpoint { durable_prefix }` reports the destination-recorded
contiguous recovery prefix. A new attempt still rediscovers destination state;
notifications cannot authorize resume. Single-PUT buffering emits no confirmed-write
event until the service accepts the PUT.

`transfer::move_file` retains all transfer policies and QoS and forces read-back
verification. It deletes the source only after successful metadata application and
publication, with a fresh source identity/content check. If cleanup fails,
`MoveFailure::published` retains the completed transfer outcome. Callers must keep
the source quiescent and exclude destination aliases: backend namespaces do not
provide a universal atomic compare-and-unlink operation.

`tests/hdfs_architecture_contract.rs` retains traversal/overwrite, durable-prefix
reconnect and cancellation/resume cases. The old `DataChunk` writer reorder test is
replaced by `native_writer_crosses_block_boundary_through_bounded_transfer`, which
exercises the current engine across a multi-block HDFS payload and verifies read-back.

## Validation evidence (2026-10-12)

The checks below use this cleanup's working tree and freshly built artifacts. Live HDFS
test executables were selected from Cargo JSON output, transferred to the runner, and
SHA-256 checked before execution. Earlier P7 acceptance is not substituted for these runs.

| Check | Result |
| --- | --- |
| Final `make ci` | Passed formatting, architecture guard, harness contracts, strict all-target Clippy, workspace tests (1,747 passed, zero failed, 39 ignored), doctests, example builds and local end-to-end checks |
| Architecture guard self-tests | 15 passed |
| Capability/directed matrix validator | 8 profiles, 64 cells, 192 gates validated |
| Python matrix, evidence and lab harness contracts | 44 passed |
| Strict rustdoc | Passed with warnings denied |
| HDFS upstream dependency contract | Exact upstream `695fc47e9f88804e39bba1d30bf128e66da9b8d6` pin and hsync/lease source checks passed; four offline native contracts passed |
| FAS2750 NFSv4.1 and NFSv3 | Both end-to-end runners passed traversal, exports, directory lifecycle and mkdir permission contracts; v3 uses the established reserved-port configuration |
| Standard S3 transfer contract | Reconnect/recovery, publication, metadata, positional reads, cancellation, native copy and client-shaped fallback passed on a 72 MiB + 137-byte payload |
| S3 namespace operations | Rename preserved content, user metadata and tags and removed the source; 103-object role deletion passed with independent absence checks |
| HDFS live contracts | Four architecture cases and fourteen non-S3 native cases passed, including policy boundaries, durable-prefix reconnect, metadata, confinement and explicit native root deletion |
| Example move and resume | Same-path move refused without mutation; QoS move passed; fresh-process resume of 96 MiB + 123 bytes reused 73,652,786 bytes, verified the destination and removed the source |

The HDFS rerun corrected two test assumptions: `StoragePath` can carry backend-specific
spellings, so confinement is asserted on actual namespace operations; deletion counts files
and directories separately. Exact deletion events, sentinel preservation and hidden-artifact
cleanup remain asserted. The S3 contract now expects exactly two native part-copy requests
for its payload, matching the existing 64 MiB copy-part size. These fixes did not change
production semantics. Both copy examples now use `move_file` for `--delete-source`.

HDFS cleanup passed after both runs, with independent `FileNotFound` checks on both confined
roots. S3 cleanup independently confirmed no current objects, historical versions, delete
markers or incomplete uploads under each test prefix. No server configuration was changed.

For reproducibility, the first HDFS architecture executable was
`f7077c6d4005ec38007fa6a8d63c328bfd923e0278f03843247f3c3eaaf16edd` (SHA-256).
The native suite initially used
`3d65aed586766142912dd2f52bd66de9a30feccee2b5a2c9a797ca9029cd103d`; the two corrected
cases and cleanup were rerun with
`f930f6dc1e8b3d33bb0aa37c630164de232449028c519b0e05d3e49f36be0f52`.
Run-local logs are under `/tmp/dm-legacy-*`; they are not a committed artifact archive.
These runs establish behavior and route selection, not a new throughput or memory benchmark.

## Backend ownership consolidation (follow-up)

Concrete NFS, S3 and HDFS clients now live under
`src/storage/backends/<backend>/client.rs` and `client/`; CIFS connection and SDK
assembly live in `storage/backends/cifs/connection.rs`. The factory dispatches to
backend facades. The architecture guard rejects reintroduced root backend modules,
while allowing a named set of shared leaf helpers within backend implementations.

Callers import HDFS configuration and client construction, NFS export discovery,
and S3 bucket discovery from `data_mover::storage`. Windows ACL utilities belong
to Local and are exposed as `storage::windows_acl`; Local metadata roles use
capability-opened handles for security-descriptor observation and application.

Seven public usage examples remain in `examples/`. Lab contracts, benchmarks,
fault injection and resume tools live in `tests/lab/tools/`, retaining explicit
Cargo target names. Shared example setup is in `examples/support/`. The missing
`storage_small_benchmark` target used by nightly has been restored with its v1
measurement definitions, and the runner selects the executable reported by a
successful current Cargo build.

The live-server evidence above belongs to the preceding legacy-removal revision;
it does not certify this follow-up revision.


Follow-up local validation passed: the workspace suite (1,746 passed), the benchmark's
three tests, 68 Python harness tests, all 18 architecture-guard cases (the one fixture
captured before the ACL move was rerun after it), strict all-target Clippy, rustdoc
with warnings denied, all example builds, and the Local end-to-end runner. New read,
traverse and namespace examples were exercised on temporary directories; the benchmark
copied 100 files with independent byte comparison, and wrong counts/missing reads failed.

The final Windows GNU all-target compile passed. Native Windows execution was not
available locally; CI now runs the Windows ACL unit, observation and staged-publication
contracts on its Windows runner. The Linux suite does not execute these cfg-gated cases.

CI and lab commands now use the relocated test names and reject selections that execute
zero tests. Focused StorageGRID/DXN wire tests, HDFS poisoned-environment isolation, and
S3 contiguous-prefix/leftover recovery tests passed after the gate changes. The restored
multipart-rename regression uses the current private protocol and confines its fixtures;
DXN fixture keys now derive from the lab runner's scoped key. DXN was not contacted.

A fresh standard-S3 run of this follow-up passed the 20 MiB multipart rename regression,
the public 72 MiB + 137-byte architecture contract (including reconnect, native copy and
shaped fallback), namespace rename with content/metadata/tags preserved, and deletion of
103 objects. Independent cleanup confirmed no current objects, historical versions,
delete markers or incomplete uploads in the isolated test prefix. Run-local redacted logs
are under `/tmp/dm-consolidate-live-058e61a8512a`; no server configuration changed.
