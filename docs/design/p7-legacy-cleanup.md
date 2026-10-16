# P7 legacy scan cleanup

This is the inventory required by the final section of [P7](p7-streaming-listing.md).
The replacement is `StorageTraversalSource`, with `Unordered` or `Blocks` ordering; callers own
any transport-specific numbering. Existing transfer, metadata and deletion APIs remain in scope
only where they depend on a retired scanner.

## Baseline preserved before removal

P7 §12.8 records the accepted node1 / m1-source 53,368,420-entry, concurrency-64 comparison: four
alternating runs, legacy mean wall 996 s / CPU 254 s / peak RSS 16.5 MB, Unordered wall 0.999× /
CPU 1.04×. The historical report placed its binary at `/root/dm/p7-base/legacy` and logs under
`/root/dm-c10/logs` on node1. An SSH check on 2026-10-12 found both paths absent, so the raw
artifacts are no longer available there. The recorded measurements remain historical evidence,
not new runs on the current machine.

The complete pre-cleanup implementation is retained at `aafd463`; it contains the legacy
`examples/nfs_walkdir.rs` and all scanner implementations. Build that revision in a separate
checkout for later comparisons. Each deletion commit identifies its own last-containing parent.

## Removal blocks and retained contracts

| Block | Action | Replacement / retained behavior |
|---|---|---|
| `ndx_walk`, `dir_tree`, all `walkdir_2` methods and `WalkDirAsyncIterator2` | Delete | `Blocks` for directory-aligned delivery; consumers supply session numbering |
| NDX CLI/examples and reader-only tests | Delete or migrate | Local modified-time, artifact, S3 server-order and ONTAP marker-repair assertions remain on role traversal |
| NFS recursive deletion's `walk_listing` dependency | Migrate | Dedicated raw deletion traversal includes artifacts, propagates failures, and has no dependency on filtered scan APIs |
| Legacy `walkdir`, `WalkOptions`, scan message/iterator aliases, `walk_scheduler` | Delete | `TraversalRequest`, `TraversalItem`, explicit completion/failure evidence |
| HDFS `scan_recursive` and `HdfsScanEvent` | Delete | Role traversal and HDFS architecture contract |
| S3 `version_listing` | Delete | Keep `version_order`: role listing still needs raw XML ordering and ONTAP continuation repair |
| `NfsEnrich` | Remove scan-only abstraction | Preserve lookup ACL, owner/group, mode and identity fields directly from attributes |
| `async_receiver` | Migrate remaining deletion use | `DeleteDirIterator` retains shared `next(&self)` and Clone/Send/Sync semantics |
| Old `NamespaceRequest`, `NamespaceResult`, `execute` | Already removed by P7 C1 | Typed namespace methods and `ListRequest` |
| Scripts and current documentation | Update | Use role CLI, preserve real-environment boundaries and historical ADR evidence |

NFS deletion must not delegate through a role whose protocol deletion calls the same wrapper:
that creates recursion. Ordinary listings hide `.data-mover-*`; deletion's raw path must still
remove those artifacts. S3 `directory_objects::refuse_multipart` protects transfers and is retained
when legacy listing helpers are removed.

## Validation

C11b baseline and bug-fix validation are tracked in P7 §14. Cleanup revisions require library,
integration/example and clippy checks; meaningful behavior assertions stay on the replacement
interfaces rather than testing retired NDX numbering. Real-environment gates remain the matrices
in P7 §14, including NFS deletion artifacts and S3 directory-object writes.

The complete `make ci` gate passed at `757c6c9`, including strict all-feature clippy, workspace
tests, doctests, examples, local end-to-end checks and the Cargo artifact-selection regression.
Cleanup is complete. Current live acceptance, including MinIO 100,000-version validation and
actual scan cancel/SIGKILL resume, is recorded in P7 §14.3. DXN-2, StorageGRID and HDFS remained
environment-limited; historical passes are not substituted for unavailable current runs.

## Breaking API migration

The removals intentionally break scan consumers; the crate does not keep forwarding wrappers.

| Removed public surface | Consumer migration |
|---|---|
| `StorageEnum::{walkdir,walkdir_2}` and backend-specific methods, `HDFSStorage::scan_recursive` | Connect a role `Storage`, then `StorageTraversalSource::new(&storage)?.traverse(request)` |
| `WalkOptions` (crate root, `storage_options`, `storage_enum`) | `TraversalRequest`: filter adapter, depth, observation plan, concurrency and ordering |
| `WalkDirAsyncIterator`, `WalkDirAsyncIterator2`, `StorageEntryMessage`, `ErrorEvent`, `HdfsScanEvent` | `TraversalSession::next_item()` and `finish()`; inspect `TraversalItem::Failure` and completion totals |
| `dir_matches_date_filter`, `FilterExpression::has_matching_dir_date` | Use generic `DirDate` DSL matching through `DslTraversalFilter`; scanner packaging is retired |
| Public modules `ndx_walk`, `dir_tree`, `async_receiver`; `AsyncReceiver<T>` | `TraversalOrder::Blocks` for directory boundaries; session identifiers/numbering belong to consumers |
| Scan examples and `ndx-walk` CLI command | `storage_role_operations --backend <backend> traverse [--order blocks]` |

`DeleteDirIterator` remains exported at the crate root, now a concrete deletion-only receiver.
It retains `new(async_channel::Receiver<DeleteEvent>)`, `next(&self)`, `Clone`, `Send` and `Sync`;
clones consume events from the same channel. `DeleteEvent.error` reports background failures.
NFS raw deletion lists artifacts, drains issued RPCs before reporting failure, and retries transient
initial directory reads without replaying a partially processed directory. It never calls its own
role deletion wrapper recursively. Lookup still preserves ACL and owner fields from NFS attributes.

Traversal no longer implicitly enriches all metadata. Request timestamps with
`ObservationPlan::default().with_timestamps(ObservationMode::InlineOnly)` when the listing's
modified time is needed. S3 versioned buckets emit one key with its `ObjectHistory`; consume its
ordered records/segments rather than treating each version as another path entry.

Removal commits: `c43bb59` (NDX, last-containing parent `aafd463`), `61ce28a` (remaining
walkers, last-containing parent `b5dec4c`). Source and the historical performance measurements
remain available for comparison; the raw historical node1 binary/log paths did not survive.
