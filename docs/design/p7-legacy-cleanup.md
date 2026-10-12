# P7 legacy scan cleanup

This is the inventory required by the final section of [P7](p7-streaming-listing.md).
The replacement is `StorageTraversalSource`, with `Unordered` or `Blocks` ordering; callers own
any transport-specific numbering. Existing transfer, metadata and deletion APIs remain in scope
only where they depend on a retired scanner.

## Baseline preserved before removal

P7 §12.8 records the accepted node1 / m1-source 53,368,420-entry, concurrency-64 comparison: four
alternating runs, legacy mean wall 996 s / CPU 254 s / peak RSS 16.5 MB, Unordered wall 0.999× /
CPU 1.04×. The original legacy binary is `/root/dm/p7-base/legacy` on node1; run logs are under
`/root/dm-c10/logs`. These historical measurements are not new runs on the current machine.

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
