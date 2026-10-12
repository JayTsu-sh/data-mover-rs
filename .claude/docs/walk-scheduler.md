# Traversal scheduling

All backends share `StorageTraversalSource`; `TraversalRequest.order` selects the scheduler.

- `Unordered` (default): `src/traversal/storage/unordered.rs` and `unordered/{pool,worker,page_io}.rs`.
  Workers own LIFO directory stacks and steal shallow work. A directory entry precedes its descendants;
  there are no per-directory completion events. Consumers inspect session completion and listing failures.
- `Blocks`: `src/traversal/storage/{blocks,cursor,stream}.rs`. Children remain contiguous in listing order;
  the cursor then descends into child directories by name bytes. `DirectoryListed` carries outcome and counts.
- Both: bounded pages, typed child/listing failures, exact totals, cancellation and resumable checkpoints.
  Checkpoint positions are in `storage/resume.rs` and `resume/{positions,relist,blocks}.rs`.

When changing scheduling, run the traversal tests for both orders, accounting and resume behavior.
Use the role CLI and `.claude/skills/_shared/traversal_resume.sh` for real-backend matrices.
NFS cancellation must finish in-flight RPCs before releasing a task; CIFS enumeration must close handles.
For design/performance evidence and API migration see `docs/design/p7-streaming-listing.md` and
`docs/design/p7-legacy-cleanup.md`.
